using System.Diagnostics;
using Npgsql;

namespace Pgmq.Hosting;

/// <summary>
/// The data source Pgmq works through, kept under its own type so that it never clashes with an
/// <see cref="NpgsqlDataSource"/> the application registers. One Pgmq builds from a connection string is disposed with
/// it; one handed in through <see cref="PgmqOptions.DataSource"/> is left to its owner.
/// <para>
/// Beside it is an unpooled data source for the one LISTEN connection, so the listener never takes a pool slot and
/// never waits for one when it reconnects. With a data source handed in, it is built from that source's connection
/// string, which keeps the password only with <c>Persist Security Info=true</c>; the listener falls back to the pool
/// when it cannot connect that way.
/// </para>
/// </summary>
internal sealed class PgmqDatabase : IAsyncDisposable, IDisposable
{
    private readonly bool _owned;

    public PgmqDatabase(NpgsqlDataSource? dataSource, string? connectionString)
    {
        _owned = dataSource is null;
        DataSource = dataSource ?? Build(connectionString!);
        Server = ServerEndpoint.Of(DataSource);
        ListenerDataSource = Build(new NpgsqlConnectionStringBuilder(connectionString ?? DataSource.ConnectionString)
        {
            Pooling = false,
        }.ConnectionString);
        ListenerIsDerived = dataSource is not null;
    }

    public NpgsqlDataSource DataSource { get; }

    /// <summary>Unpooled: where the listener opens its one long-lived connection.</summary>
    public NpgsqlDataSource ListenerDataSource { get; }

    /// <summary>
    /// The listener's data source was built from a handed-in data source's connection string, which may have lost its
    /// password or anything else the application configured in code.
    /// </summary>
    public bool ListenerIsDerived { get; }

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
