namespace Pgmq;

/// <summary>How a queue's tables are stored.</summary>
public enum PgmqQueueType
{
    /// <summary>An ordinary logged table: <c>pgmq.create</c>.</summary>
    Standard,

    /// <summary>
    /// An unlogged table (<c>pgmq.create_unlogged</c>): faster writes, but its messages are lost on a crash and it is
    /// not replicated.
    /// </summary>
    Unlogged,

    /// <summary>
    /// A table partitioned by pg_partman (<c>pgmq.create_partitioned</c>), whose old partitions are dropped after the
    /// retention interval. Needs the pg_partman extension.
    /// </summary>
    Partitioned,
}

/// <summary>How to create a queue.</summary>
public sealed record PgmqQueueOptions
{
    /// <summary>A standard queue.</summary>
    public static readonly PgmqQueueOptions Standard = new();

    /// <summary>An unlogged queue.</summary>
    public static readonly PgmqQueueOptions Unlogged = new() { Type = PgmqQueueType.Unlogged };

    /// <summary>How the queue's tables are stored.</summary>
    public PgmqQueueType Type { get; init; } = PgmqQueueType.Standard;

    /// <summary>
    /// For a partitioned queue, the size of one partition: a number of messages (<c>"10000"</c>) or a time span
    /// (<c>"1 day"</c>).
    /// </summary>
    public string PartitionInterval { get; init; } = "10000";

    /// <summary>
    /// For a partitioned queue, how much is kept before old partitions are dropped, in the same unit as
    /// <see cref="PartitionInterval"/>.
    /// </summary>
    public string RetentionInterval { get; init; } = "100000";

    /// <summary>For a partitioned queue, how many partitions pg_partman creates ahead.</summary>
    public int Premake { get; init; } = 4;

    /// <summary>A partitioned queue.</summary>
    public static PgmqQueueOptions Partitioned(string partitionInterval = "10000", string retentionInterval = "100000",
        int premake = 4)
        => new()
        {
            Type = PgmqQueueType.Partitioned,
            PartitionInterval = partitionInterval,
            RetentionInterval = retentionInterval,
            Premake = premake,
        };
}

/// <summary>A queue as <c>pgmq.list_queues</c> reports it.</summary>
public sealed record PgmqQueueInfo(string QueueName, DateTimeOffset CreatedAt, bool IsPartitioned, bool IsUnlogged);

/// <summary>A queue's numbers, as <c>pgmq.metrics</c> reports them.</summary>
/// <param name="QueueName">The queue.</param>
/// <param name="QueueLength">Messages in the queue, visible or not.</param>
/// <param name="VisibleLength">Messages that can be read now.</param>
/// <param name="NewestMessageAge">Age of the newest message; null when the queue is empty.</param>
/// <param name="OldestMessageAge">Age of the oldest message; null when the queue is empty.</param>
/// <param name="TotalMessages">Messages ever sent to the queue.</param>
/// <param name="ScrapeTime">When the numbers were taken.</param>
/// <param name="DefaultPartitionLength">
/// Partitioned queues only, null otherwise: about how many messages (queue and archive) sit in the default partition.
/// Above zero means pg_partman maintenance is not keeping up and needs attention. An estimate from table statistics.
/// </param>
public sealed record PgmqQueueMetrics(
    string QueueName,
    long QueueLength,
    long VisibleLength,
    TimeSpan? NewestMessageAge,
    TimeSpan? OldestMessageAge,
    long TotalMessages,
    DateTimeOffset ScrapeTime,
    long? DefaultPartitionLength);

/// <summary>A topic pattern bound to a queue (<c>pgmq.bind_topic</c>).</summary>
public sealed record PgmqTopicBinding(string Pattern, string QueueName, DateTimeOffset BoundAt, string CompiledRegex);

/// <summary>A binding that a routing key matches (<c>pgmq.test_routing</c>).</summary>
public sealed record PgmqTopicRoute(string Pattern, string QueueName, string CompiledRegex);

/// <summary>A queue that sends a notification on insert (<c>pgmq.enable_notify_insert</c>).</summary>
/// <param name="QueueName">The queue.</param>
/// <param name="ThrottleInterval">The shortest time between two notifications.</param>
/// <param name="LastNotifiedAt">When the last notification went out.</param>
public sealed record PgmqInsertNotification(string QueueName, TimeSpan ThrottleInterval, DateTimeOffset LastNotifiedAt)
{
    /// <summary>The channel the notifications go to: <c>LISTEN "pgmq.q_{queue}.INSERT"</c>.</summary>
    public string Channel => PgmqQueueManager.InsertChannel(QueueName);
}
