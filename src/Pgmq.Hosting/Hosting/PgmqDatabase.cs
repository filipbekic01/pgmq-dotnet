using Npgsql;

namespace Pgmq.Hosting;

/// <summary>
/// The data source Pgmq works through, kept under its own type so that it never clashes with an
/// <see cref="NpgsqlDataSource"/> the application registers. One Pgmq builds from a connection string is disposed with
/// it; one handed in through <see cref="PgmqOptions.DataSource"/> is left to its owner.
/// </summary>
internal sealed class PgmqDatabase : IAsyncDisposable, IDisposable
{
    private readonly bool _owned;

    public PgmqDatabase(NpgsqlDataSource? dataSource, string? connectionString)
    {
        _owned = dataSource is null;
        DataSource = dataSource ?? NpgsqlDataSource.Create(connectionString!);
    }

    public NpgsqlDataSource DataSource { get; }

    public void Dispose()
    {
        if (_owned)
        {
            DataSource.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_owned)
        {
            await DataSource.DisposeAsync();
        }
    }
}
