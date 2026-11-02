using Npgsql;

namespace Pgmq;

/// <summary>
/// Manages queues and everything around them: creating and dropping queues, metrics, topic bindings, insert
/// notifications, FIFO indexes, sending and reading raw messages, and deleting, archiving or hiding messages by id.
/// Each method is one pgmq function;
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

    /// <summary>
    /// Moves messages from <paramref name="sourceQueue"/>, or from its archive, onto <paramref name="targetQueue"/>
    /// in one statement (<c>pgmq.shovel</c>, Pgmq.NET's one addition to pgmq, created by the migrator). Each keeps its
    /// body and headers and arrives as a new message, visible at once, read zero times. From a queue only visible
    /// messages move: one being handled or delayed stays, and two shovels at once never move the same message. Either
    /// every selected message moves or none does. From the archive messages are moved, not copied: they leave it.
    /// <code>
    /// // Replay what failed: everything in orders' archive goes back onto orders.
    /// await queues.ShovelAsync("orders", "orders", fromArchive: true);
    /// </code>
    /// </summary>
    /// <param name="sourceQueue">The queue to take messages from.</param>
    /// <param name="targetQueue">The queue to put them on. It must exist.</param>
    /// <param name="fromArchive">Take them from the source's archive (<c>pgmq.a_{source}</c>) instead.</param>
    /// <param name="maxMessages">
    /// Move at most this many, oldest first: 1000 by default, so one call stays one modest transaction. Call it until
    /// it returns 0 to drain a queue, or pass null to move everything in one transaction.
    /// </param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>How many messages moved.</returns>
    Task<long> ShovelAsync(string sourceQueue, string targetQueue, bool fromArchive = false, int? maxMessages = 1000,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Moves the messages with <paramref name="messageIds"/> from <paramref name="sourceQueue"/>, or from its archive,
    /// onto <paramref name="targetQueue"/> in one statement, however many there are; see
    /// <see cref="ShovelAsync(string, string, bool, int?, CancellationToken)"/> for how they move. Ids that are not
    /// there, or (in the queue) not visible because they are being handled or delayed, are left out.
    /// <code>
    /// // Replay two archived failures.
    /// await queues.ShovelAsync("orders", "orders", [41, 42], fromArchive: true);
    /// </code>
    /// </summary>
    /// <param name="sourceQueue">The queue to take messages from.</param>
    /// <param name="targetQueue">The queue to put them on. It must exist.</param>
    /// <param name="messageIds">The <c>msg_id</c>s to move, in the queue or in its archive.</param>
    /// <param name="fromArchive">Take them from the source's archive (<c>pgmq.a_{source}</c>) instead.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>How many messages moved.</returns>
    Task<long> ShovelAsync(string sourceQueue, string targetQueue, IReadOnlyCollection<long> messageIds,
        bool fromArchive = false, CancellationToken cancellationToken = default);

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

    /// <summary>
    /// Writes raw JSON straight to one queue (<c>pgmq.send</c>), past the publisher's routing and headers: for queues
    /// shared with other languages or tools. The worker's consumers cannot handle such a message, since it lacks
    /// Pgmq's <c>message-type</c> header; publish through <see cref="IPgmqPublisher"/> for them.
    /// </summary>
    /// <param name="queueName">The queue.</param>
    /// <param name="message">The body, as JSON.</param>
    /// <param name="headers">The headers, as a JSON object, or null for none.</param>
    /// <param name="delay">How long it stays invisible, measured on the database's clock; null for none.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The new message's id.</returns>
    Task<long> SendAsync(string queueName, string message, string? headers = null, TimeSpan? delay = null,
        CancellationToken cancellationToken = default);

    /// <summary>Writes several raw JSON messages to one queue in one call (<c>pgmq.send_batch</c>); see <see cref="SendAsync"/>.</summary>
    /// <param name="queueName">The queue.</param>
    /// <param name="messages">The bodies, as JSON. At least one.</param>
    /// <param name="headers">One JSON object per message, or null for none.</param>
    /// <param name="delay">How long they stay invisible, measured on the database's clock; null for none.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>The new messages' ids, in the order of <paramref name="messages"/>.</returns>
    Task<IReadOnlyList<long>> SendBatchAsync(string queueName, IReadOnlyList<string> messages,
        IReadOnlyList<string>? headers = null, TimeSpan? delay = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads up to <paramref name="quantity"/> messages and hides them for <paramref name="visibilityTimeout"/>, rounded
    /// up to whole seconds (<c>pgmq.read</c>, the grouped reads, and their <c>_with_poll</c> forms). Delete or archive
    /// each one when done, or it comes back once the timeout passes. This is the raw read; consumers registered with
    /// Pgmq.Hosting are read by its worker.
    /// </summary>
    /// <returns>The messages, oldest first.</returns>
    Task<IReadOnlyList<PgmqMessage>> ReadAsync(string queueName, TimeSpan visibilityTimeout, int quantity = 1,
        PgmqReadOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>Reads and deletes up to <paramref name="quantity"/> messages in one step (<c>pgmq.pop</c>).</summary>
    /// <returns>The messages, oldest first. They are already gone from the queue.</returns>
    Task<IReadOnlyList<PgmqMessage>> PopAsync(string queueName, int quantity = 1,
        CancellationToken cancellationToken = default);
}
