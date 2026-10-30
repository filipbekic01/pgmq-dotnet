using System.Reflection;
using Npgsql;

namespace Pgmq;

/// <summary>How pgmq gets into the database.</summary>
public enum PgmqInstallMode
{
    /// <summary>
    /// The extension when the server has it (<c>pg_available_extensions</c>), the embedded SQL otherwise. A database
    /// that already has pgmq keeps the way it was installed.
    /// </summary>
    Auto,

    /// <summary>Only the extension. Fails on a server without it, or on a database with the SQL install.</summary>
    Extension,

    /// <summary>
    /// Only the embedded SQL, for managed servers without the extension (RDS, Azure, ...). Fails on a database with
    /// the extension.
    /// </summary>
    Sql,
}

/// <summary>How pgmq is installed in a database.</summary>
public enum PgmqInstallation
{
    /// <summary><c>create extension pgmq</c>; upgraded with <c>alter extension pgmq update</c>.</summary>
    Extension,

    /// <summary>
    /// The extension's SQL run directly, its version tracked in <c>pgmq.__pgmq_migrations</c> (the table the Rust
    /// client uses too); upgraded with the embedded upgrade scripts.
    /// </summary>
    Sql,
}

/// <summary>What <see cref="PgmqMigrator.MigrateAsync"/> found and did.</summary>
/// <param name="Installation">How pgmq is installed now.</param>
/// <param name="PreviousVersion">The version before; null when this run installed pgmq.</param>
/// <param name="Version">The version now.</param>
/// <param name="AppliedScripts">
/// What ran: <c>pgmq.sql</c> and upgrade scripts for the SQL install, <c>create extension</c> or
/// <c>alter extension</c> for the extension. Empty when pgmq was already current.
/// </param>
public sealed record PgmqMigrationResult(
    PgmqInstallation Installation,
    string? PreviousVersion,
    string Version,
    IReadOnlyList<string> AppliedScripts)
{
    /// <summary>Whether anything changed.</summary>
    public bool Changed => AppliedScripts.Count > 0;
}

/// <summary>
/// Creates the database and installs or upgrades pgmq in it, so an application needs no migrations of its own. Every
/// step is idempotent and serialized by an advisory lock (the Rust client's key), so every instance runs it on start.
/// <para>
/// pgmq ships as an extension, which managed servers often lack, and as plain SQL. Both are handled: the extension is
/// created and then updated to the server's default version; the SQL install runs the scripts embedded in this
/// package (<see cref="EmbeddedVersion"/>), recording each in <c>pgmq.__pgmq_migrations</c>, and later runs apply the
/// upgrade scripts from the recorded version on.
/// </para>
/// </summary>
/// <param name="dataSource">The database pgmq lives in.</param>
public sealed class PgmqMigrator(NpgsqlDataSource dataSource)
{
    /// <summary>The pgmq advisory lock key of the Rust client, so the two never install at once.</summary>
    internal const long ADVISORY_LOCK_KEY = long.MinValue + 4149;

    internal const string INIT_SCRIPT = "pgmq.sql";

    private const string RESOURCE_PREFIX = "Pgmq.Sql.";
    private const string INVALID_CATALOG_NAME = "3D000";
    private const string DUPLICATE_DATABASE = "42P04";
    private const string UNIQUE_VIOLATION = "23505";
    private const int SCRIPT_TIMEOUT_SECONDS = 300;

    private static readonly Lazy<IReadOnlyList<UpgradeScript>> Upgrades = new(LoadUpgradeScripts);

    /// <summary>The pgmq version the embedded <c>pgmq.sql</c> installs (from its <c>pgmq.control</c>).</summary>
    public static Version EmbeddedVersion { get; } = ReadEmbeddedVersion();

    /// <summary>
    /// Creates the database named in <paramref name="connectionString"/> when it does not exist, connecting to
    /// <paramref name="maintenanceDatabase"/> on the same server with the same credentials to do so. The user needs
    /// <c>CREATEDB</c> only when the database is missing. Another instance creating it first counts as success.
    /// </summary>
    /// <returns>True when this call created the database.</returns>
    public static async Task<bool> EnsureDatabaseAsync(string connectionString, string maintenanceDatabase = "postgres",
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(connectionString);
        ArgumentException.ThrowIfNullOrEmpty(maintenanceDatabase);

        NpgsqlConnectionStringBuilder target = new(connectionString);
        string database = target.Database
            ?? throw new ArgumentException("The connection string names no database.", nameof(connectionString));

        try
        {
            // Pooling off: a pooled connection to a database that is about to exist would keep the failure around.
            await using NpgsqlConnection probe = new(new NpgsqlConnectionStringBuilder(connectionString)
            { Pooling = false }.ConnectionString);
            await probe.OpenAsync(cancellationToken);
            return false;
        }
        catch (PostgresException ex) when (ex.SqlState == INVALID_CATALOG_NAME)
        {
            // Missing: create it below.
        }

        NpgsqlConnectionStringBuilder maintenance = new(connectionString)
        {
            Database = maintenanceDatabase,
            Pooling = false,
        };

        await using NpgsqlConnection connection = new(maintenance.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        // CREATE DATABASE takes no parameters, so the name goes in as a quoted identifier.
        await using NpgsqlCommand create = new($"create database {QuoteIdentifier(database)}", connection);
        try
        {
            await create.ExecuteNonQueryAsync(cancellationToken);
            return true;
        }
        catch (PostgresException ex) when (ex.SqlState is DUPLICATE_DATABASE or UNIQUE_VIOLATION)
        {
            return false;
        }
    }

    /// <summary>
    /// The installed pgmq version: the extension's, or the highest the SQL install recorded. Null when pgmq is not
    /// installed.
    /// </summary>
    public async Task<string?> GetInstalledVersionAsync(CancellationToken cancellationToken = default)
    {
        await using NpgsqlConnection connection = await dataSource.OpenConnectionAsync(cancellationToken);
        return (await ReadStateAsync(connection, null, cancellationToken)).InstalledVersion;
    }

    /// <summary>
    /// Installs pgmq, or brings it up to date, in one transaction under the advisory lock. Already current, it changes
    /// nothing. A database with pgmq objects but no version record (installed by hand from <c>pgmq.sql</c>) is refused
    /// rather than guessed at.
    /// </summary>
    public async Task<PgmqMigrationResult> MigrateAsync(PgmqInstallMode mode = PgmqInstallMode.Auto,
        CancellationToken cancellationToken = default)
    {
        await using NpgsqlConnection connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync(cancellationToken);

        await ExecuteAsync(connection, transaction, $"select pg_advisory_xact_lock({ADVISORY_LOCK_KEY})",
            cancellationToken);

        State state = await ReadStateAsync(connection, transaction, cancellationToken);
        PgmqMigrationResult result = state switch
        {
            { ExtensionVersion: not null } => await UpgradeExtensionAsync(connection, transaction, state, mode,
                cancellationToken),
            { HasMigrationsTable: true } => await MigrateSqlAsync(connection, transaction, state, mode,
                cancellationToken),
            { HasPgmqObjects: true } => throw new InvalidOperationException(
                "The pgmq schema exists but has no pgmq.__pgmq_migrations record, so its version is unknown. It " +
                "was probably installed by hand from pgmq.sql. Record its version there, or drop the schema."),
            _ when mode != PgmqInstallMode.Sql && state.AvailableExtensionVersion is not null =>
                await CreateExtensionAsync(connection, transaction, cancellationToken),
            _ when mode == PgmqInstallMode.Extension => throw new InvalidOperationException(
                "The pgmq extension is not available on this server. Use PgmqInstallMode.Auto or Sql to install " +
                "it as plain SQL."),
            _ => await MigrateSqlAsync(connection, transaction, state, mode, cancellationToken),
        };

        await transaction.CommitAsync(cancellationToken);
        return result;
    }

    private static async Task<PgmqMigrationResult> CreateExtensionAsync(NpgsqlConnection connection,
        NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        await ExecuteAsync(connection, transaction, "create extension if not exists pgmq cascade", cancellationToken);
        State after = await ReadStateAsync(connection, transaction, cancellationToken);
        return new(PgmqInstallation.Extension, null, after.ExtensionVersion!, ["create extension pgmq"]);
    }

    private static async Task<PgmqMigrationResult> UpgradeExtensionAsync(NpgsqlConnection connection,
        NpgsqlTransaction transaction, State state, PgmqInstallMode mode, CancellationToken cancellationToken)
    {
        if (mode == PgmqInstallMode.Sql)
        {
            throw new InvalidOperationException(
                $"pgmq {state.ExtensionVersion} is installed as an extension; PgmqInstallMode.Sql cannot manage it.");
        }

        string installed = state.ExtensionVersion!;
        if (state.AvailableExtensionVersion is not { } available
            || ParseVersion(available) <= ParseVersion(installed))
        {
            return new(PgmqInstallation.Extension, installed, installed, []);
        }

        await ExecuteAsync(connection, transaction, "alter extension pgmq update", cancellationToken);
        State after = await ReadStateAsync(connection, transaction, cancellationToken);
        return new(PgmqInstallation.Extension, installed, after.ExtensionVersion!, ["alter extension pgmq update"]);
    }

    private static async Task<PgmqMigrationResult> MigrateSqlAsync(NpgsqlConnection connection,
        NpgsqlTransaction transaction, State state, PgmqInstallMode mode, CancellationToken cancellationToken)
    {
        if (mode == PgmqInstallMode.Extension)
        {
            throw new InvalidOperationException(
                "pgmq is installed as plain SQL; PgmqInstallMode.Extension cannot manage it.");
        }

        // The Rust client's table, so a database either client installed stays upgradable by both.
        await ExecuteAsync(connection, transaction, """
            create schema if not exists pgmq;
            create table if not exists pgmq.__pgmq_migrations (
                name text primary key not null,
                version text not null,
                run_at timestamp with time zone not null default clock_timestamp());
            lock table pgmq.__pgmq_migrations in access exclusive mode;
            """, cancellationToken);

        Dictionary<string, string> applied = [];
        await using (NpgsqlCommand read = new("select name, version from pgmq.__pgmq_migrations", connection,
                         transaction))
        await using (NpgsqlDataReader reader = await read.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                applied[reader.GetString(0)] = reader.GetString(1);
            }
        }

        Version? installed = applied.Count == 0 ? null : applied.Values.Select(ParseVersion).Max();
        IReadOnlyList<PlannedScript> plan = Plan(installed, EmbeddedVersion, Upgrades.Value, applied.Keys);

        foreach (PlannedScript script in plan)
        {
            // Some upstream steps only bump the version and are empty; they are recorded all the same.
            string sql = ReadResource(script.Name);
            if (!string.IsNullOrWhiteSpace(sql))
            {
                await using NpgsqlCommand run = new(sql, connection, transaction);
                run.CommandTimeout = SCRIPT_TIMEOUT_SECONDS;
                await run.ExecuteNonQueryAsync(cancellationToken);
            }

            await using NpgsqlCommand record = new(
                "insert into pgmq.__pgmq_migrations (name, version) values ($1, $2)", connection, transaction);
            record.Parameters.AddWithValue(script.Name);
            record.Parameters.AddWithValue(script.To.ToString(3));
            await record.ExecuteNonQueryAsync(cancellationToken);
        }

        string version = (plan.Count > 0 ? plan[^1].To : installed!).ToString(3);
        return new(PgmqInstallation.Sql, installed?.ToString(3), version, [.. plan.Select(s => s.Name)]);
    }

    /// <summary>
    /// The scripts that take a SQL install from <paramref name="installed"/> to <paramref name="target"/>:
    /// <c>pgmq.sql</c> on an empty database, otherwise the chain of upgrade scripts. A version newer than the target
    /// is left alone; a gap in the chain throws.
    /// </summary>
    internal static IReadOnlyList<PlannedScript> Plan(Version? installed, Version target,
        IReadOnlyList<UpgradeScript> upgrades, IEnumerable<string> applied)
    {
        if (installed is null)
        {
            return [new(INIT_SCRIPT, target)];
        }

        HashSet<string> done = [.. applied];
        List<PlannedScript> plan = [];
        Version current = installed;

        while (current < target)
        {
            UpgradeScript next = upgrades.FirstOrDefault(u => u.From == current)
                ?? throw new InvalidOperationException(
                    $"No pgmq upgrade script from {current.ToString(3)} towards {target.ToString(3)}.");

            if (!done.Contains(next.Name))
            {
                plan.Add(new(next.Name, next.To));
            }

            current = next.To;
        }

        return plan;
    }

    /// <summary>Every embedded <c>pgmq--from--to.sql</c>, oldest first.</summary>
    internal static IReadOnlyList<UpgradeScript> LoadUpgradeScripts()
        => [.. typeof(PgmqMigrator).Assembly.GetManifestResourceNames()
            .Where(n => n.StartsWith(RESOURCE_PREFIX + "pgmq--", StringComparison.Ordinal))
            .Select(n => UpgradeScript.Parse(n[RESOURCE_PREFIX.Length..]))
            .OrderBy(u => u.From)];

    internal static string ReadResource(string name)
    {
        Assembly assembly = typeof(PgmqMigrator).Assembly;
        using Stream stream = assembly.GetManifestResourceStream(RESOURCE_PREFIX + name)
            ?? throw new InvalidOperationException($"The embedded pgmq script {name} is missing.");
        using StreamReader reader = new(stream);
        return reader.ReadToEnd();
    }

    private static Version ReadEmbeddedVersion()
    {
        foreach (string line in ReadResource("pgmq.control").Split('\n'))
        {
            string[] pair = line.Split('=', 2, StringSplitOptions.TrimEntries);
            if (pair is ["default_version", var value])
            {
                return ParseVersion(value.Trim('\''));
            }
        }

        throw new InvalidOperationException("The embedded pgmq.control has no default_version.");
    }

    private static async Task<State> ReadStateAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction,
        CancellationToken cancellationToken)
    {
        await using NpgsqlCommand command = new("""
            select
                (select extversion from pg_catalog.pg_extension where extname = 'pgmq'),
                (select default_version from pg_catalog.pg_available_extensions where name = 'pgmq'),
                to_regclass('pgmq.__pgmq_migrations') is not null,
                to_regclass('pgmq.meta') is not null
            """, connection, transaction);
        await using NpgsqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);

        State state = new(
            reader.IsDBNull(0) ? null : reader.GetString(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.GetBoolean(2),
            reader.GetBoolean(3));
        await reader.CloseAsync();

        if (state is { ExtensionVersion: null, HasMigrationsTable: true })
        {
            await using NpgsqlCommand version = new("select version from pgmq.__pgmq_migrations", connection,
                transaction);
            List<Version> versions = [];
            await using NpgsqlDataReader rows = await version.ExecuteReaderAsync(cancellationToken);
            while (await rows.ReadAsync(cancellationToken))
            {
                versions.Add(ParseVersion(rows.GetString(0)));
            }

            state = state with { SqlVersion = versions.Count == 0 ? null : versions.Max()!.ToString(3) };
        }

        return state;
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql,
        CancellationToken cancellationToken)
    {
        await using NpgsqlCommand command = new(sql, connection, transaction);
        command.CommandTimeout = SCRIPT_TIMEOUT_SECONDS;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    internal static Version ParseVersion(string value)
    {
        Version parsed = Version.Parse(value);
        return new(parsed.Major, parsed.Minor, Math.Max(parsed.Build, 0));
    }

    private static string QuoteIdentifier(string name) => "\"" + name.Replace("\"", "\"\"") + "\"";

    private sealed record State(
        string? ExtensionVersion,
        string? AvailableExtensionVersion,
        bool HasMigrationsTable,
        bool HasPgmqObjects,
        string? SqlVersion = null)
    {
        public string? InstalledVersion => ExtensionVersion ?? SqlVersion;
    }

    /// <summary>An upstream <c>pgmq--from--to.sql</c> upgrade script.</summary>
    internal sealed record UpgradeScript(string Name, Version From, Version To)
    {
        public static UpgradeScript Parse(string name)
        {
            string[] parts = name[..^".sql".Length].Split("--");
            if (parts is not ["pgmq", var from, var to])
            {
                throw new FormatException($"Not a pgmq upgrade script name: {name}");
            }

            return new(name, ParseVersion(from), ParseVersion(to));
        }
    }

    /// <summary>A script to run and the version it leaves pgmq at.</summary>
    internal sealed record PlannedScript(string Name, Version To);
}
