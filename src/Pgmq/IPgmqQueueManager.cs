using Npgsql;

namespace Pgmq;

/// <summary>
/// Manages queues and everything around them: creating and dropping queues, metrics, topic bindings, insert
/// notifications, FIFO indexes, and deleting, archiving or hiding messages by id. Each method is one pgmq function;
/// its summary names it. Pgmq.Hosting sets up the consumers' queues itself, so this is for what an application does
/// on its own.
/// </summary>
public interface IPgmqQueueManager
{
    /// <summary>
    /// Installs pgmq, or upgrades it, through <see cref="PgmqMigrator"/>: the extension where the server has it, the
    /// embedded SQL otherwise. Does nothing when pgmq is current.
    /// </summary>
    Task InitAsync(CancellationToken cancellationToken = default);

    /// <summary>The installed pgmq version, as an extension or as SQL; null when pgmq is not installed.</summary>
    Task<string?> GetVersionAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a queue: <c>pgmq.create</c>, <c>pgmq.create_unlogged</c> or <c>pgmq.create_partitioned</c>. A partitioned
    /// queue that already exists fails; use <see cref="EnsureQueueAsync"/> to create only what is missing.
    /// </summary>
    Task CreateQueueAsync(string queueName, PgmqQueueOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a queue unless it already exists, safely when several instances do it at once. An existing queue is
    /// left as it is, whatever its type.
    /// </summary>
    /// <returns>True when the queue was created.</returns>
    Task<bool> EnsureQueueAsync(string queueName, PgmqQueueOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Locks a queue until <paramref name="transaction"/> ends (<c>pgmq.acquire_queue_lock</c>). pgmq takes the same
    /// lock when it creates a partitioned queue, so this serializes your own work on a queue with that, and with
    /// other callers of this method.
    /// </summary>
    Task AcquireQueueLockAsync(string queueName, NpgsqlTransaction transaction,
        CancellationToken cancellationToken = default);

    /// <summary>Drops a queue and its archive (<c>pgmq.drop_queue</c>).</summary>
    /// <returns>True when the queue existed.</returns>
    Task<bool> DropQueueAsync(string queueName, CancellationToken cancellationToken = default);

    /// <summary>Deletes every message in a queue (<c>pgmq.purge_queue</c>).</summary>
    /// <returns>How many messages were deleted.</returns>
    Task<long> PurgeQueueAsync(string queueName, CancellationToken cancellationToken = default);

    /// <summary>Whether a queue exists.</summary>
    Task<bool> QueueExistsAsync(string queueName, CancellationToken cancellationToken = default);

    /// <summary>Every queue (<c>pgmq.list_queues</c>).</summary>
    Task<IReadOnlyList<PgmqQueueInfo>> ListQueuesAsync(CancellationToken cancellationToken = default);

    /// <summary>One queue's numbers (<c>pgmq.metrics</c>).</summary>
    Task<PgmqQueueMetrics> GetMetricsAsync(string queueName, CancellationToken cancellationToken = default);

    /// <summary>Every queue's numbers (<c>pgmq.metrics_all</c>).</summary>
    Task<IReadOnlyList<PgmqQueueMetrics>> GetAllMetricsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Turns a queue's archive into a partitioned table (<c>pgmq.convert_archive_partitioned</c>). Needs pg_partman.
    /// </summary>
    Task ConvertArchivePartitionedAsync(string queueName, string partitionInterval = "10000",
        string retentionInterval = "100000", int leadingPartition = 10, CancellationToken cancellationToken = default);

    /// <summary>
    /// Binds a topic pattern to a queue (<c>pgmq.bind_topic</c>), so a message published to a matching routing key
    /// reaches it. <c>*</c> matches one dot-separated word and <c>#</c> any number. Binding again changes nothing.
    /// </summary>
    Task BindTopicAsync(string pattern, string queueName, CancellationToken cancellationToken = default);

    /// <summary>Removes a topic binding (<c>pgmq.unbind_topic</c>).</summary>
    /// <returns>True when the binding existed.</returns>
    Task<bool> UnbindTopicAsync(string pattern, string queueName, CancellationToken cancellationToken = default);

    /// <summary>Every topic binding, or only those of one queue (<c>pgmq.list_topic_bindings</c>).</summary>
    Task<IReadOnlyList<PgmqTopicBinding>> ListTopicBindingsAsync(string? queueName = null,
        CancellationToken cancellationToken = default);

    /// <summary>The bindings a routing key matches, without sending anything (<c>pgmq.test_routing</c>).</summary>
    Task<IReadOnlyList<PgmqTopicRoute>> TestRoutingAsync(string routingKey,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Makes a queue send a notification on <see cref="PgmqInsertNotification.Channel"/> when a message is inserted, at
    /// most once per <paramref name="throttleInterval"/> (<c>pgmq.enable_notify_insert</c>).
    /// </summary>
    Task EnableInsertNotificationsAsync(string queueName, TimeSpan? throttleInterval = null,
        CancellationToken cancellationToken = default);

    /// <summary>Changes the throttle of a queue that already notifies (<c>pgmq.update_notify_insert</c>).</summary>
    Task UpdateInsertNotificationsAsync(string queueName, TimeSpan throttleInterval,
        CancellationToken cancellationToken = default);

    /// <summary>Stops a queue's insert notifications (<c>pgmq.disable_notify_insert</c>).</summary>
    Task DisableInsertNotificationsAsync(string queueName, CancellationToken cancellationToken = default);

    /// <summary>Every queue that notifies on insert (<c>pgmq.list_notify_insert_throttles</c>).</summary>
    Task<IReadOnlyList<PgmqInsertNotification>> ListInsertNotificationsAsync(
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Indexes a queue's FIFO group header, which speeds up grouped reads (<c>pgmq.create_fifo_index</c>). Idempotent.
    /// </summary>
    Task CreateFifoIndexAsync(string queueName, CancellationToken cancellationToken = default);

    /// <summary>Indexes the FIFO group header of every queue (<c>pgmq.create_fifo_indexes_all</c>).</summary>
    Task CreateFifoIndexesAsync(CancellationToken cancellationToken = default);

    /// <summary>Deletes one message (<c>pgmq.delete</c>).</summary>
    /// <returns>True when the message existed.</returns>
    Task<bool> DeleteMessageAsync(string queueName, long messageId, CancellationToken cancellationToken = default);

    /// <summary>Deletes several messages in one call (<c>pgmq.delete</c> with an array).</summary>
    /// <returns>The ids that existed and were deleted.</returns>
    Task<IReadOnlyList<long>> DeleteMessagesAsync(string queueName, IReadOnlyCollection<long> messageIds,
        CancellationToken cancellationToken = default);

    /// <summary>Moves one message to the queue's archive (<c>pgmq.archive</c>).</summary>
    /// <returns>True when the message existed.</returns>
    Task<bool> ArchiveMessageAsync(string queueName, long messageId, CancellationToken cancellationToken = default);

    /// <summary>Moves several messages to the queue's archive in one call (<c>pgmq.archive</c> with an array).</summary>
    /// <returns>The ids that existed and were archived.</returns>
    Task<IReadOnlyList<long>> ArchiveMessagesAsync(string queueName, IReadOnlyCollection<long> messageIds,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Hides one message for <paramref name="timeout"/> from now, rounded up to whole seconds; zero makes it visible at
    /// once (<c>pgmq.set_vt</c>).
    /// </summary>
    /// <returns>True when the message existed.</returns>
    Task<bool> SetVisibilityTimeoutAsync(string queueName, long messageId, TimeSpan timeout,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Hides several messages for <paramref name="timeout"/> from now in one call (<c>pgmq.set_vt</c> with an array).
    /// </summary>
    /// <returns>The ids that existed and were updated.</returns>
    Task<IReadOnlyList<long>> SetVisibilityTimeoutAsync(string queueName, IReadOnlyCollection<long> messageIds,
        TimeSpan timeout, CancellationToken cancellationToken = default);
}
