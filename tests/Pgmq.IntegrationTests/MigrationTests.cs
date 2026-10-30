using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Pgmq.Benchmarks;
using Pgmq.Hosting;

namespace Pgmq.IntegrationTests;

/// <summary>
///     Two servers: one with the pgmq extension, and plain PostgreSQL without it, as on a managed service. Each test
///     works in a database of its own that the setup creates. Needs a running Docker daemon.
/// </summary>
public sealed class MigrationServers : IAsyncLifetime
{
    private const string PlainImage = "postgres:18.6";
    private const string Password = "postgres";

    private readonly PgmqContainer _withExtension = new();

    private readonly IContainer _plain = new ContainerBuilder(PlainImage)
        .WithLogger(NullLogger.Instance)
        .WithEnvironment("POSTGRES_PASSWORD", Password)
        .WithPortBinding(5432, true)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilCommandIsCompleted("pg_isready", "-h", "127.0.0.1", "-U", "postgres"))
        .Build();

    /// <summary>A connection string to a database not yet created, on the server with the extension.</summary>
    public string NewOnExtensionServer() =>
        new NpgsqlConnectionStringBuilder(_withExtension.ConnectionString) { Database = NewName() }.ConnectionString;

    /// <summary>A connection string to a database not yet created, on the server without the extension.</summary>
    public string NewOnPlainServer() =>
        $"Host={_plain.Hostname};Port={_plain.GetMappedPublicPort(5432)};Username=postgres;Password={Password};" +
        $"Database={NewName()}";

    public async ValueTask InitializeAsync()
        => await Task.WhenAll(_withExtension.StartAsync(), _plain.StartAsync());

    public async ValueTask DisposeAsync()
    {
        await _withExtension.DisposeAsync();
        await _plain.DisposeAsync();
    }

    private static string NewName() => "transport_" + Guid.NewGuid().ToString("N")[..12];
}

public sealed class MigrationTests(MigrationServers servers) : IClassFixture<MigrationServers>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task AddPgmq_on_a_missing_database_Should_create_it_install_the_extension_and_its_queues()
    {
        string connectionString = servers.NewOnExtensionServer();

        await StartSetupAsync(connectionString);

        Assert.Equal("1.13.1", await ScalarAsync<string>(connectionString,
            "select extversion from pg_extension where extname = 'pgmq'"));
        Assert.True(await ScalarAsync<bool>(connectionString,
            "select exists (select 1 from pgmq.meta where queue_name = 'm_ping')"));

        // A second start finds everything current and changes nothing.
        await StartSetupAsync(connectionString);
        PgmqMigrationResult again = await MigratorOn(connectionString).MigrateAsync(cancellationToken: Ct);
        Assert.Equal(new(PgmqInstallation.Extension, "1.13.1", "1.13.1", []), again with { AppliedScripts = [] });
        Assert.False(again.Changed);
    }

    [Fact]
    public async Task Database_Should_set_pgmq_up_in_that_database_and_leave_the_connection_strings_alone()
    {
        string application = servers.NewOnExtensionServer();
        string transport = new NpgsqlConnectionStringBuilder(application)
        { Database = "own_" + Guid.NewGuid().ToString("N")[..12] }.ConnectionString;
        await PgmqMigrator.EnsureDatabaseAsync(application, cancellationToken: Ct);

        await StartSetupAsync(application, o => o.Database = new NpgsqlConnectionStringBuilder(transport).Database);

        Assert.Equal("1.13.1", await ScalarAsync<string>(transport,
            "select extversion from pg_extension where extname = 'pgmq'"));
        Assert.Equal(0L, await ScalarAsync<long>(application,
            "select count(*) from pg_namespace where nspname = 'pgmq'"));
    }

    [Fact]
    public async Task Without_the_extension_Should_install_the_embedded_sql_record_it_and_carry_messages()
    {
        string connectionString = servers.NewOnPlainServer();

        await StartSetupAsync(connectionString);

        Assert.Equal(0L, await ScalarAsync<long>(connectionString,
            "select count(*) from pg_extension where extname = 'pgmq'"));
        Assert.Equal("pgmq.sql=1.13.1", await ScalarAsync<string>(connectionString,
            "select string_agg(name || '=' || version, ',') from pgmq.__pgmq_migrations"));

        await using NpgsqlDataSource dataSource = NpgsqlDataSource.Create(connectionString);
        PgmqQueueManager queues = new(dataSource);
        Assert.Equal("1.13.1", await queues.GetVersionAsync(Ct));

        await ScalarAsync<long>(connectionString, "select * from pgmq.send('m_ping', '{\"n\": 1}')");
        Assert.Equal("""{"n": 1}""", await ScalarAsync<string>(connectionString,
            "select message::text from pgmq.read('m_ping', 30, 1)"));

        PgmqMigrationResult again = await MigratorOn(connectionString).MigrateAsync(cancellationToken: Ct);
        Assert.Equal(PgmqInstallation.Sql, again.Installation);
        Assert.False(again.Changed);
    }

    [Fact]
    public async Task An_older_sql_install_Should_be_upgraded_in_place_and_keep_its_queues_and_messages()
    {
        string connectionString = servers.NewOnPlainServer();
        await PgmqMigrator.EnsureDatabaseAsync(connectionString, cancellationToken: Ct);

        // pgmq 1.12.0 installed from SQL and recorded the way the Rust client and this library record it.
        string old = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Sql", "pgmq-1.12.0.sql"), Ct);
        await ExecuteAsync(connectionString, old);
        await ExecuteAsync(connectionString, """
            create table pgmq.__pgmq_migrations (name text primary key not null, version text not null,
                run_at timestamp with time zone not null default clock_timestamp());
            insert into pgmq.__pgmq_migrations (name, version) values ('pgmq.sql', '1.12.0');
            select pgmq.create('kept');
            select pgmq.send('kept', '{"n": 7}');
            """);

        PgmqMigrationResult result = await MigratorOn(connectionString).MigrateAsync(cancellationToken: Ct);

        Assert.Equal(PgmqInstallation.Sql, result.Installation);
        Assert.Equal("1.12.0", result.PreviousVersion);
        Assert.Equal("1.13.1", result.Version);
        Assert.Equal(["pgmq--1.12.0--1.13.0.sql", "pgmq--1.13.0--1.13.1.sql"], result.AppliedScripts);

        Assert.Equal(1L, await ScalarAsync<long>(connectionString, "select count(*) from pgmq.q_kept"));
        // A 1.13 function, absent in 1.12.
        Assert.True(await ScalarAsync<bool>(connectionString,
            "select exists (select 1 from pg_proc where proname = 'acquire_queue_lock')"));
    }

    [Fact]
    public async Task Several_instances_starting_together_Should_create_the_database_and_install_pgmq_once()
    {
        string connectionString = servers.NewOnPlainServer();

        bool[] created = await Task.WhenAll(Enumerable.Range(0, 4)
            .Select(_ => PgmqMigrator.EnsureDatabaseAsync(connectionString, cancellationToken: Ct)));
        Assert.Single(created, c => c);

        PgmqMigrationResult[] results = await Task.WhenAll(Enumerable.Range(0, 4)
            .Select(_ => MigratorOn(connectionString).MigrateAsync(cancellationToken: Ct)));

        Assert.Single(results, r => r.Changed);
        Assert.All(results, r => Assert.Equal("1.13.1", r.Version));
        Assert.Equal(1L, await ScalarAsync<long>(connectionString, "select count(*) from pgmq.__pgmq_migrations"));
    }

    [Fact]
    public async Task Extension_only_on_a_server_without_the_extension_Should_fail_and_install_nothing()
    {
        string connectionString = servers.NewOnPlainServer();
        await PgmqMigrator.EnsureDatabaseAsync(connectionString, cancellationToken: Ct);

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => MigratorOn(connectionString).MigrateAsync(PgmqInstallMode.Extension, Ct));

        Assert.Contains("not available", ex.Message);
        Assert.False(await ScalarAsync<bool>(connectionString,
            "select exists (select 1 from pg_namespace where nspname = 'pgmq')"));
    }

    [Fact]
    public async Task A_pgmq_schema_without_a_version_record_Should_be_refused_rather_than_guessed()
    {
        string connectionString = servers.NewOnPlainServer();
        await PgmqMigrator.EnsureDatabaseAsync(connectionString, cancellationToken: Ct);
        await ExecuteAsync(connectionString,
            await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Sql", "pgmq-1.12.0.sql"), Ct));

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => MigratorOn(connectionString).MigrateAsync(cancellationToken: Ct));

        Assert.Contains("__pgmq_migrations", ex.Message);
    }

    /// <summary>Runs Pgmq's start-up setup, as a host does, over one consumer, and requires it to succeed.</summary>
    private static async Task StartSetupAsync(string connectionString, Action<PgmqOptions>? configure = null)
    {
        ServiceCollection collection = new();
        collection.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        collection.AddPgmq(options =>
        {
            options.ConnectionString = connectionString;
            configure?.Invoke(options);
        });
        collection.AddPgmqConsumer<MigrationPingConsumer>();

        await using ServiceProvider services = collection.BuildServiceProvider(validateScopes: true);
        await services.GetServices<IHostedService>().OfType<QueueSetup>().Single().StartAsync(Ct);
        services.GetRequiredService<PgmqStatus>().EnsureReady();
    }

    private static PgmqMigrator MigratorOn(string connectionString)
        => new(NpgsqlDataSource.Create(connectionString));

    private static async Task ExecuteAsync(string connectionString, string sql)
    {
        await using NpgsqlConnection connection = new(connectionString);
        await connection.OpenAsync(Ct);
        await using NpgsqlCommand command = new(sql, connection);
        await command.ExecuteNonQueryAsync(Ct);
    }

    private static async Task<T> ScalarAsync<T>(string connectionString, string sql)
    {
        await using NpgsqlConnection connection = new(connectionString);
        await connection.OpenAsync(Ct);
        await using NpgsqlCommand command = new(sql, connection);
        return (T)(await command.ExecuteScalarAsync(Ct))!;
    }
}

public sealed record MigrationPing(int N);

[QueueName("m_ping")]
public sealed class MigrationPingConsumer : IPgmqConsumer<MigrationPing>
{
    public Task HandleAsync(PgmqConsumeContext<MigrationPing> context, CancellationToken cancellationToken)
        => Task.CompletedTask;
}
