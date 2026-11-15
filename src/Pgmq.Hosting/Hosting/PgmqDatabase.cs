using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Pgmq.Hosting;

/// <summary>
/// The data source Pgmq works through, kept under its own type so that it never clashes with an
/// <see cref="NpgsqlDataSource"/> the application registers. One Pgmq builds from a connection string is disposed with
/// it; one handed in through <see cref="PgmqOptions.DataSource"/> is left to its owner.
/// <para>
/// Beside it is an unpooled data source for the two connections that must never wait for a pool slot: the one that
/// LISTENs, and the lease renewals'. With a data source handed in, it is built from
/// <see cref="PgmqOptions.DedicatedConnectionString"/> when that is set. Otherwise it is built from that source's
/// connection string, which keeps the password only with <c>Persist Security Info=true</c>; those connections fall
/// back to the pool when they cannot connect that way.
/// </para>
/// </summary>
internal sealed class PgmqDatabase : IAsyncDisposable, IDisposable
{
    private readonly bool _owned;

    /// <param name="dataSource">The application's data source, or null to build one.</param>
    /// <param name="connectionString">What to build the data source from, when none is handed in.</param>
    /// <param name="dedicatedConnectionString">
    /// With a data source handed in: what to open the connections outside the pool with, instead of that source's own
    /// connection string.
    /// </param>
    public PgmqDatabase(NpgsqlDataSource? dataSource, string? connectionString,
        string? dedicatedConnectionString = null)
    {
        _owned = dataSource is null;
        DataSource = dataSource ?? Build(connectionString!);
        Server = ServerEndpoint.Of(DataSource);
        ListenerDataSource = Build(new NpgsqlConnectionStringBuilder(
            dedicatedConnectionString ?? connectionString ?? DataSource.ConnectionString)
        {
            Pooling = false,
        }.ConnectionString);
        ListenerIsDerived = dataSource is not null && dedicatedConnectionString is null;
    }

    public NpgsqlDataSource DataSource { get; }

    /// <summary>Unpooled: where the listener opens its one long-lived connection.</summary>
    public NpgsqlDataSource ListenerDataSource { get; }

    /// <summary>
    /// The listener's data source was built from a handed-in data source's connection string, which may have lost its
    /// password or anything else the application configured in code.
    /// </summary>
    public bool ListenerIsDerived { get; }

    /// <summary>Set once the unpooled source could not connect where the pool can: the pool is used from then on.</summary>
    private volatile bool _dedicatedOnPool;

    /// <summary>Whether the connections meant to be outside the pool are taken from it; see <see cref="OpenDedicatedAsync"/>.</summary>
    public bool DedicatedOnPool => _dedicatedOnPool;

    /// <summary>
    /// A connection of its own, outside the pool, for work that must not wait behind the application for a pool slot:
    /// the insert listener and the lease renewals. One built from a handed-in data source's connection string can lack
    /// the password (or a certificate, or anything else set in code); when it fails where the pool connects, these
    /// connections come from the pool from then on.
    /// </summary>
    public async Task<NpgsqlConnection> OpenDedicatedAsync(ILogger logger, CancellationToken cancellationToken)
    {
        if (_dedicatedOnPool)
        {
            return await DataSource.OpenConnectionAsync(cancellationToken);
        }

        try
        {
            return await ListenerDataSource.OpenConnectionAsync(cancellationToken);
        }
        catch (Exception ex) when (ListenerIsDerived && ex is not OperationCanceledException)
        {
            NpgsqlConnection pooled = await DataSource.OpenConnectionAsync(cancellationToken);

            // The pool hands out an idle connection untested. Only if it works is the unpooled source at fault and
            // not the database: while the database is away, both fail, and nothing is decided.
            try
            {
                await using NpgsqlCommand ping = new("select 1", pooled);
                await ping.ExecuteNonQueryAsync(cancellationToken);
            }
            catch
            {
                await pooled.DisposeAsync();
                throw;
            }

            _dedicatedOnPool = true;
            logger.LogWarning(ex, "Could not open a connection outside the pool from the data source's connection " +
                                  "string, which keeps the password only with Persist Security Info=true; the " +
                                  "insert listener and lease renewals hold two pooled connections instead, and " +
                                  "wait behind the application when the pool is used up. Set " +
                                  "PgmqOptions.DedicatedConnectionString to give them a connection string of " +
                                  "their own");
            return pooled;
        }
    }

    /// <summary>The database server, for the <c>server.*</c> tags of Pgmq's spans and metrics.</summary>
    public ServerEndpoint Server { get; }

    /// <summary>
    /// A data source whose commands are traced (when the application traces Npgsql) only inside a trace that is
    /// already running: a publish, or the handling of a message. The worker's polls and lease renewals run outside any
    /// trace and would otherwise each be a trace of their own, many a second.
    /// </summary>
    internal static NpgsqlDataSource Build(string connectionString)
    {
        NpgsqlDataSourceBuilder builder = new(connectionString);
        builder.ConfigureTracing(tracing => tracing
            .ConfigureCommandFilter(_ => Activity.Current is not null)
            .ConfigureBatchFilter(_ => Activity.Current is not null));
        return builder.Build();
    }

    public void Dispose()
    {
        ListenerDataSource.Dispose();
        if (_owned)
        {
            DataSource.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await ListenerDataSource.DisposeAsync();
        if (_owned)
        {
            await DataSource.DisposeAsync();
        }
    }
}
