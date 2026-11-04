using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace Pgmq.Hosting;

/// <summary>
/// Finds the idle queues that have work, so an idle queue never goes to the database on its own. In one statement for
/// every queue it asks which have a visible message, and wakes only those. Each queue is one index lookup on
/// <c>vt</c>, so many queues are still one cheap round trip on one connection.
/// <para>
/// It catches what insert notifications never report: a delayed message whose time came, a released message, one back
/// from a lapsed lease, and a lost or throttled notification. While every queue's notifications are live it sweeps
/// every <see cref="PgmqOptions.MaxIdleDelay"/>; while any is not, every <see cref="PgmqOptions.MinIdleDelay"/>,
/// doubling up to about a second while it finds nothing. <see cref="Nudge"/> makes it sweep at once.
/// </para>
/// <para>
/// The statement is built here, from the registered consumers, rather than installed as a function, so it needs no
/// migration and works with <see cref="PgmqOptions.AutoSetup"/> off.
/// </para>
/// </summary>
internal sealed class QueueSweeper(
    PgmqDatabase database,
    InsertListener listener,
    PgmqOptions options,
    ILogger<QueueSweeper> logger)
{
    /// <summary>The slowest sweep while some notifications are not live.</summary>
    private static readonly TimeSpan FastCeiling = TimeSpan.FromSeconds(1);

    private readonly SemaphoreSlim _nudged = new(0, 1);

    /// <summary>Sweeps at once, instead of after the current interval.</summary>
    public void Nudge()
    {
        try
        {
            _nudged.Release();
        }
        catch (SemaphoreFullException)
        {
            // Already nudged.
        }
    }

    public async Task RunAsync(IReadOnlyList<SweptQueue> queues, CancellationToken stoppingToken)
    {
        if (queues.Count == 0)
        {
            return;
        }

        string sql = Sql(queues.Select(q => q.Consumer).ToList());
        TimeSpan fastest = options.MinIdleDelay;
        TimeSpan slowest = TimeSpan.FromTicks(Math.Max(fastest.Ticks, Math.Min(FastCeiling.Ticks, options.MaxIdleDelay.Ticks)));
        TimeSpan fast = fastest;

        while (!stoppingToken.IsCancellationRequested)
        {
            int woken;
            try
            {
                woken = await SweepAsync(queues, sql, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                // The sweeper never ends while the host runs: without it, delayed messages would wait forever.
                logger.LogWarning(ex, "Looking for work on the queues failed; trying again");
                woken = 0;
            }

            TimeSpan wait;
            if (queues.All(q => listener.IsLive(q.Consumer.QueueName)))
            {
                wait = options.MaxIdleDelay;
                fast = fastest;
            }
            else
            {
                wait = fast;
                fast = woken > 0 ? fastest : TimeSpan.FromTicks(Math.Min(fast.Ticks * 2, slowest.Ticks));
            }

            try
            {
                await _nudged.WaitAsync(wait, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    /// <summary>One sweep: wakes every queue with a visible message, and says how many it woke.</summary>
    private async Task<int> SweepAsync(IReadOnlyList<SweptQueue> queues, string sql, CancellationToken cancellationToken)
    {
        long start = Stopwatch.GetTimestamp();
        string? errorType = null;
        List<SweptQueue> found;

        try
        {
            found = await FindAsync(queues, sql, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            errorType = PgmqTelemetry.ErrorType(ex);

            if (ex is not PostgresException)
            {
                // The database is out of reach; every queue's read would fail as well. Try again next time.
                logger.LogWarning(ex, "Looking for work on the queues failed; trying again");
                return 0;
            }

            // The database answered with an error, most likely one queue's table is gone. Check one queue at a time so
            // the others still get their work.
            logger.LogWarning(ex, "Looking for work on every queue at once failed; checking them one by one");
            found = await FindOneByOneAsync(queues, cancellationToken);
        }
        finally
        {
            RecordDuration(start, errorType);
        }

        // Woken only once the sweep's connection is back in the pool, so a woken read can take that one.
        foreach (SweptQueue queue in found)
        {
            queue.Wake();
        }

        PgmqTelemetry.SweepWoken.Add(found.Count);
        return found.Count;
    }

    private async Task<List<SweptQueue>> FindAsync(IReadOnlyList<SweptQueue> queues, string sql,
        CancellationToken cancellationToken)
    {
        List<SweptQueue> found = [];
        await using NpgsqlCommand command = database.DataSource.CreateCommand(sql);
        AddFilters(command, queues.Select(q => q.Consumer));

        await using NpgsqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            found.Add(queues[reader.GetInt32(0)]);
        }

        return found;
    }

    /// <summary>
    /// Checks each queue on its own. A queue whose check the database refuses is woken, so its own read reports what
    /// is wrong with it.
    /// </summary>
    private async Task<List<SweptQueue>> FindOneByOneAsync(IReadOnlyList<SweptQueue> queues,
        CancellationToken cancellationToken)
    {
        List<SweptQueue> found = [];
        await using NpgsqlConnection connection = await database.DataSource.OpenConnectionAsync(cancellationToken);

        foreach (SweptQueue queue in queues)
        {
            bool hasWork;
            try
            {
                await using NpgsqlCommand command = new($"select exists ({Visible(queue.Consumer, "$1")})", connection);
                AddFilters(command, [queue.Consumer]);
                hasWork = (bool)(await command.ExecuteScalarAsync(cancellationToken))!;
            }
            catch (PostgresException)
            {
                hasWork = true;
            }

            if (hasWork)
            {
                found.Add(queue);
            }
        }

        return found;
    }

    /// <summary>
    /// One branch per queue, each answering with the queue's index when it has a visible message:
    /// <code>
    /// select 0 where exists (select 1 from pgmq."q_orders" where vt &lt;= clock_timestamp())
    /// union all
    /// select 1 where exists (select 1 from pgmq."q_emails" where vt &lt;= clock_timestamp() and message @> $1::jsonb)
    /// </code>
    /// A consumer with a filter only counts a message its filter matches, so one it would never read does not wake it
    /// on every sweep. A partitioned queue is read through its parent table, as any other.
    /// </summary>
    internal static string Sql(IReadOnlyList<ConsumerRegistration> consumers)
    {
        StringBuilder sql = new();
        int parameter = 0;

        for (int i = 0; i < consumers.Count; i++)
        {
            if (i > 0)
            {
                sql.Append("\nunion all\n");
            }

            string? filter = consumers[i].Filter is null ? null : $"${++parameter}";
            sql.Append("select ").Append(i).Append(" where exists (").Append(Visible(consumers[i], filter)).Append(')');
        }

        return sql.ToString();
    }

    /// <summary>The visible messages of a queue (that its filter matches), as a subquery.</summary>
    private static string Visible(ConsumerRegistration consumer, string? filterParameter)
    {
        // pgmq's table for a queue is q_ and the queue name, lowercased; queue names are checked to be plain already.
        string table = PgmqMigrator.QuoteIdentifier(("q_" + consumer.QueueName).ToLowerInvariant());
        string filter = consumer.Filter is null || filterParameter is null
            ? ""
            : $" and message @> {filterParameter}::jsonb";
        return $"select 1 from pgmq.{table} where vt <= pg_catalog.clock_timestamp(){filter}";
    }

    private static void AddFilters(NpgsqlCommand command, IEnumerable<ConsumerRegistration> consumers)
    {
        foreach (ConsumerRegistration consumer in consumers)
        {
            if (consumer.Filter is not null)
            {
                command.Parameters.Add(new NpgsqlParameter { Value = consumer.Filter, NpgsqlDbType = NpgsqlDbType.Jsonb });
            }
        }
    }

    private void RecordDuration(long start, string? errorType)
    {
        if (!PgmqTelemetry.SweepDuration.Enabled)
        {
            return;
        }

        TagList tags = new() { { "messaging.system", PgmqTelemetry.System } };
        if (errorType is not null)
        {
            tags.Add("error.type", errorType);
        }

        if (database.Server.Address is not null)
        {
            tags.Add("server.address", database.Server.Address);
        }

        PgmqTelemetry.SweepDuration.Record(Stopwatch.GetElapsedTime(start).TotalSeconds, tags);
    }
}

/// <summary>A queue the sweeper looks at: its consumer, and how to wake its read loop.</summary>
internal sealed record SweptQueue(ConsumerRegistration Consumer, Action Wake);
