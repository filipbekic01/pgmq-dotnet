using Npgsql;
using NpgsqlTypes;

namespace Pgmq;

/// <summary><see cref="IPgmqQueueManager"/> calling the pgmq functions directly.</summary>
/// <param name="dataSource">The database with the pgmq extension.</param>
public sealed class PgmqQueueManager(NpgsqlDataSource dataSource) : IPgmqQueueManager
{
    /// <summary>The channel pgmq notifies when a message is inserted into <paramref name="queueName"/>.</summary>
    public static string InsertChannel(string queueName) => $"pgmq.q_{queueName}.INSERT";

    /// <inheritdoc />
    public async Task InitAsync(CancellationToken cancellationToken = default)
        => await ExecuteAsync("create extension if not exists pgmq cascade", [], cancellationToken);

    /// <summary>Creates the pg_partman extension that partitioned queues need, if it is missing.</summary>
    internal async Task InitPartmanAsync(CancellationToken cancellationToken = default)
        => await ExecuteAsync("create extension if not exists pg_partman", [], cancellationToken);

    /// <inheritdoc />
    public async Task<string?> GetVersionAsync(CancellationToken cancellationToken = default)
        => await ScalarAsync<string?>("select extversion from pg_catalog.pg_extension where extname = 'pgmq'", [],
            cancellationToken);

    /// <inheritdoc />
    public async Task CreateQueueAsync(string queueName, PgmqQueueOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        PgmqTopology.EnsureValidQueueName(queueName);
        (string sql, NpgsqlParameter[] parameters) = CreateCommand(queueName, options ?? PgmqQueueOptions.Standard);
        await ExecuteAsync(sql, parameters, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<bool> EnsureQueueAsync(string queueName, PgmqQueueOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        PgmqTopology.EnsureValidQueueName(queueName);
        (string create, NpgsqlParameter[] parameters) =
            CreateCommand(queueName, options ?? PgmqQueueOptions.Standard);

        // pgmq.create_partitioned fails on an existing queue, and two instances starting together would both see it
        // missing. A lock per queue name, held to the end of the transaction, serializes the check and the create.
        await using NpgsqlConnection connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync(cancellationToken);

        await using (NpgsqlCommand lockCommand = new(
                         "select pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('pgmq.create.' || $1))",
                         connection, transaction))
        {
            lockCommand.Parameters.Add(new NpgsqlParameter { Value = queueName });
            await lockCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (NpgsqlCommand exists = new("select exists (select 1 from pgmq.meta where queue_name = $1)",
                         connection, transaction))
        {
            exists.Parameters.Add(new NpgsqlParameter { Value = queueName });
            if ((bool)(await exists.ExecuteScalarAsync(cancellationToken))!)
            {
                await transaction.CommitAsync(cancellationToken);
                return false;
            }
        }

        await using (NpgsqlCommand command = new(create, connection, transaction))
        {
            command.Parameters.AddRange(parameters);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    /// <inheritdoc />
    public async Task<bool> DropQueueAsync(string queueName, CancellationToken cancellationToken = default)
        => await ScalarAsync<bool>("select pgmq.drop_queue($1)", [Text(queueName)], cancellationToken);

    /// <inheritdoc />
    public async Task<long> PurgeQueueAsync(string queueName, CancellationToken cancellationToken = default)
        => await ScalarAsync<long>("select pgmq.purge_queue($1)", [Text(queueName)], cancellationToken);

    /// <inheritdoc />
    public async Task<bool> QueueExistsAsync(string queueName, CancellationToken cancellationToken = default)
        => await ScalarAsync<bool>("select exists (select 1 from pgmq.meta where queue_name = $1)", [Text(queueName)],
            cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<PgmqQueueInfo>> ListQueuesAsync(CancellationToken cancellationToken = default)
        => QueryAsync("select queue_name, created_at, is_partitioned, is_unlogged from pgmq.list_queues()", [],
            r => new PgmqQueueInfo(r.GetString(0), r.GetFieldValue<DateTimeOffset>(1), r.GetBoolean(2), r.GetBoolean(3)),
            cancellationToken);

    /// <inheritdoc />
    public async Task<PgmqQueueMetrics> GetMetricsAsync(string queueName, CancellationToken cancellationToken = default)
        => (await QueryAsync($"select {MetricsColumns} from pgmq.metrics($1)", [Text(queueName)], ReadMetrics,
            cancellationToken)).Single();

    /// <inheritdoc />
    public Task<IReadOnlyList<PgmqQueueMetrics>> GetAllMetricsAsync(CancellationToken cancellationToken = default)
        => QueryAsync($"select {MetricsColumns} from pgmq.metrics_all()", [], ReadMetrics, cancellationToken);

    /// <inheritdoc />
    public async Task ConvertArchivePartitionedAsync(string queueName, string partitionInterval = "10000",
        string retentionInterval = "100000", int leadingPartition = 10, CancellationToken cancellationToken = default)
        => await ExecuteAsync("select pgmq.convert_archive_partitioned($1, $2, $3, $4)",
            [Text(queueName), Text(partitionInterval), Text(retentionInterval), Int(leadingPartition)],
            cancellationToken);

    /// <inheritdoc />
    public async Task BindTopicAsync(string pattern, string queueName, CancellationToken cancellationToken = default)
        => await ExecuteAsync("select pgmq.bind_topic($1, $2)", [Text(pattern), Text(queueName)], cancellationToken);

    /// <inheritdoc />
    public async Task<bool> UnbindTopicAsync(string pattern, string queueName,
        CancellationToken cancellationToken = default)
        => await ScalarAsync<bool>("select pgmq.unbind_topic($1, $2)", [Text(pattern), Text(queueName)],
            cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<PgmqTopicBinding>> ListTopicBindingsAsync(string? queueName = null,
        CancellationToken cancellationToken = default)
        => QueryAsync(
            queueName is null
                ? "select pattern, queue_name, bound_at, compiled_regex from pgmq.list_topic_bindings()"
                : "select pattern, queue_name, bound_at, compiled_regex from pgmq.list_topic_bindings($1)",
            queueName is null ? [] : [Text(queueName)],
            r => new PgmqTopicBinding(r.GetString(0), r.GetString(1), r.GetFieldValue<DateTimeOffset>(2),
                r.GetString(3)),
            cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<PgmqTopicRoute>> TestRoutingAsync(string routingKey,
        CancellationToken cancellationToken = default)
        => QueryAsync("select pattern, queue_name, compiled_regex from pgmq.test_routing($1)", [Text(routingKey)],
            r => new PgmqTopicRoute(r.GetString(0), r.GetString(1), r.GetString(2)), cancellationToken);

    /// <inheritdoc />
    public async Task EnableInsertNotificationsAsync(string queueName, TimeSpan? throttleInterval = null,
        CancellationToken cancellationToken = default)
        => await ExecuteAsync("select pgmq.enable_notify_insert($1, $2)",
            [Text(queueName), Int(Milliseconds(throttleInterval ?? TimeSpan.FromMilliseconds(250)))],
            cancellationToken);

    /// <summary>
    /// Turns insert notifications on with this throttle unless they already are, safely when several instances do it
    /// at once: <c>pgmq.enable_notify_insert</c> drops and recreates its trigger, which two callers at once would trip
    /// over.
    /// </summary>
    internal async Task EnsureInsertNotificationsAsync(string queueName, TimeSpan throttleInterval,
        CancellationToken cancellationToken = default)
    {
        int throttleMs = Milliseconds(throttleInterval);

        await using NpgsqlConnection connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync(cancellationToken);

        await using (NpgsqlCommand lockCommand = new(
                         "select pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('pgmq.notify.' || $1))",
                         connection, transaction))
        {
            lockCommand.Parameters.Add(Text(queueName));
            await lockCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (NpgsqlCommand current = new(
                         "select throttle_interval_ms from pgmq.list_notify_insert_throttles() where queue_name = $1",
                         connection, transaction))
        {
            current.Parameters.Add(Text(queueName));
            if (await current.ExecuteScalarAsync(cancellationToken) is int existing && existing == throttleMs)
            {
                await transaction.CommitAsync(cancellationToken);
                return;
            }
        }

        await using (NpgsqlCommand enable = new("select pgmq.enable_notify_insert($1, $2)", connection, transaction))
        {
            enable.Parameters.Add(Text(queueName));
            enable.Parameters.Add(Int(throttleMs));
            await enable.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async Task UpdateInsertNotificationsAsync(string queueName, TimeSpan throttleInterval,
        CancellationToken cancellationToken = default)
        => await ExecuteAsync("select pgmq.update_notify_insert($1, $2)",
            [Text(queueName), Int(Milliseconds(throttleInterval))], cancellationToken);

    /// <inheritdoc />
    public async Task DisableInsertNotificationsAsync(string queueName, CancellationToken cancellationToken = default)
        => await ExecuteAsync("select pgmq.disable_notify_insert($1)", [Text(queueName)], cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<PgmqInsertNotification>> ListInsertNotificationsAsync(
        CancellationToken cancellationToken = default)
        => QueryAsync(
            "select queue_name, throttle_interval_ms, last_notified_at from pgmq.list_notify_insert_throttles()", [],
            r => new PgmqInsertNotification(r.GetString(0), TimeSpan.FromMilliseconds(r.GetInt32(1)),
                r.GetFieldValue<DateTimeOffset>(2)),
            cancellationToken);

    /// <inheritdoc />
    public async Task CreateFifoIndexAsync(string queueName, CancellationToken cancellationToken = default)
        => await ExecuteAsync("select pgmq.create_fifo_index($1)", [Text(queueName)], cancellationToken);

    /// <inheritdoc />
    public async Task CreateFifoIndexesAsync(CancellationToken cancellationToken = default)
        => await ExecuteAsync("select pgmq.create_fifo_indexes_all()", [], cancellationToken);

    /// <inheritdoc />
    public async Task<bool> DeleteMessageAsync(string queueName, long messageId,
        CancellationToken cancellationToken = default)
        => await ScalarAsync<bool>("select pgmq.delete($1, $2)", [Text(queueName), Long(messageId)],
            cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<long>> DeleteMessagesAsync(string queueName, IReadOnlyCollection<long> messageIds,
        CancellationToken cancellationToken = default)
        => QueryAsync("select * from pgmq.delete($1, $2)", [Text(queueName), Longs(messageIds)],
            r => r.GetInt64(0), cancellationToken);

    /// <inheritdoc />
    public async Task<bool> ArchiveMessageAsync(string queueName, long messageId,
        CancellationToken cancellationToken = default)
        => await ScalarAsync<bool>("select pgmq.archive($1, $2)", [Text(queueName), Long(messageId)],
            cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<long>> ArchiveMessagesAsync(string queueName, IReadOnlyCollection<long> messageIds,
        CancellationToken cancellationToken = default)
        => QueryAsync("select * from pgmq.archive($1, $2)", [Text(queueName), Longs(messageIds)],
            r => r.GetInt64(0), cancellationToken);

    /// <inheritdoc />
    public async Task<bool> SetVisibilityTimeoutAsync(string queueName, long messageId, TimeSpan timeout,
        CancellationToken cancellationToken = default)
        => (await QueryAsync("select msg_id from pgmq.set_vt($1, $2, $3)",
            [Text(queueName), Long(messageId), Int(Seconds(timeout))], r => r.GetInt64(0), cancellationToken)).Count > 0;

    /// <inheritdoc />
    public Task<IReadOnlyList<long>> SetVisibilityTimeoutAsync(string queueName, IReadOnlyCollection<long> messageIds,
        TimeSpan timeout, CancellationToken cancellationToken = default)
        => QueryAsync("select msg_id from pgmq.set_vt($1, $2, $3)",
            [Text(queueName), Longs(messageIds), Int(Seconds(timeout))], r => r.GetInt64(0), cancellationToken);

    private const string MetricsColumns =
        "queue_name, queue_length, queue_visible_length, newest_msg_age_sec, oldest_msg_age_sec, total_messages, " +
        "scrape_time";

    private static PgmqQueueMetrics ReadMetrics(NpgsqlDataReader r)
        => new(
            r.GetString(0),
            r.GetInt64(1),
            r.GetInt64(2),
            r.IsDBNull(3) ? null : TimeSpan.FromSeconds(r.GetInt32(3)),
            r.IsDBNull(4) ? null : TimeSpan.FromSeconds(r.GetInt32(4)),
            r.GetInt64(5),
            r.GetFieldValue<DateTimeOffset>(6));

    private static (string Sql, NpgsqlParameter[] Parameters) CreateCommand(string queueName, PgmqQueueOptions options)
        => options.Type switch
        {
            PgmqQueueType.Standard => ("select pgmq.create($1)", [Text(queueName)]),
            PgmqQueueType.Unlogged => ("select pgmq.create_unlogged($1)", [Text(queueName)]),
            PgmqQueueType.Partitioned => ("select pgmq.create_partitioned($1, $2, $3, $4)",
            [
                Text(queueName), Text(options.PartitionInterval), Text(options.RetentionInterval), Int(options.Premake),
            ]),
            _ => throw new ArgumentOutOfRangeException(nameof(options), options.Type, "Unknown queue type."),
        };

    private static int Milliseconds(TimeSpan interval)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(interval, TimeSpan.Zero, nameof(interval));
        return (int)Math.Ceiling(interval.TotalMilliseconds);
    }

    private static NpgsqlParameter Text(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return new NpgsqlParameter { Value = value, NpgsqlDbType = NpgsqlDbType.Text };
    }

    private static int Seconds(TimeSpan timeout)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(timeout, TimeSpan.Zero, nameof(timeout));
        return (int)Math.Ceiling(timeout.TotalSeconds);
    }

    private static NpgsqlParameter Int(int value) => new() { Value = value, NpgsqlDbType = NpgsqlDbType.Integer };

    private static NpgsqlParameter Long(long value) => new() { Value = value, NpgsqlDbType = NpgsqlDbType.Bigint };

    private static NpgsqlParameter Longs(IReadOnlyCollection<long> values)
    {
        ArgumentNullException.ThrowIfNull(values);
        return new NpgsqlParameter { Value = values.ToArray(), NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint };
    }

    private async Task ExecuteAsync(string sql, NpgsqlParameter[] parameters, CancellationToken cancellationToken)
    {
        await using NpgsqlCommand command = dataSource.CreateCommand(sql);
        command.Parameters.AddRange(parameters);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task<T> ScalarAsync<T>(string sql, NpgsqlParameter[] parameters, CancellationToken cancellationToken)
    {
        await using NpgsqlCommand command = dataSource.CreateCommand(sql);
        command.Parameters.AddRange(parameters);
        object? value = await command.ExecuteScalarAsync(cancellationToken);
        return value is null or DBNull ? default! : (T)value;
    }

    private async Task<IReadOnlyList<T>> QueryAsync<T>(string sql, NpgsqlParameter[] parameters,
        Func<NpgsqlDataReader, T> read, CancellationToken cancellationToken)
    {
        await using NpgsqlCommand command = dataSource.CreateCommand(sql);
        command.Parameters.AddRange(parameters);
        await using NpgsqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken);

        List<T> rows = [];
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(read(reader));
        }

        return rows;
    }
}
