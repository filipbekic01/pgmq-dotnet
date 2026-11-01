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

/// <summary>
/// A message as pgmq stores it (<c>pgmq.message_record</c>), read straight off a queue with
/// <see cref="IPgmqQueueManager.ReadAsync"/> or <see cref="IPgmqQueueManager.PopAsync"/>.
/// </summary>
/// <param name="MsgId">pgmq's id of the message in its queue.</param>
/// <param name="ReadCount">How many times it was read, this read included.</param>
/// <param name="EnqueuedAt">When it was sent.</param>
/// <param name="LastReadAt">When it was last read; null if never.</param>
/// <param name="VisibleAt">When it can be read again (its visibility timeout).</param>
/// <param name="Message">The JSON body.</param>
/// <param name="Headers">The JSON headers; null when it was sent without any.</param>
public sealed record PgmqMessage(
    long MsgId,
    int ReadCount,
    DateTimeOffset EnqueuedAt,
    DateTimeOffset? LastReadAt,
    DateTimeOffset VisibleAt,
    string? Message,
    string? Headers);

/// <summary>How <see cref="IPgmqQueueManager.ReadAsync"/> reads a queue.</summary>
public sealed record PgmqReadOptions
{
    /// <summary>Which pgmq read function to call: <c>read</c> or one of the grouped FIFO reads.</summary>
    public PgmqReadMode Mode { get; init; } = PgmqReadMode.Standard;

    /// <summary>
    /// A JSON object the body must contain (pgmq's experimental conditional read, <c>message @&gt; filter</c>), as in
    /// <c>{"region":"eu"}</c>. Standard mode only.
    /// </summary>
    public string? Filter { get; init; }

    /// <summary>
    /// Wait inside the database for up to this long, rounded up to whole seconds, when nothing can be read yet (the
    /// <c>_with_poll</c> functions). Null returns at once.
    /// </summary>
    public TimeSpan? PollTimeout { get; init; }

    /// <summary>While polling, how often the database checks the queue again.</summary>
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(100);
}

/// <summary>A queue that sends a notification on insert (<c>pgmq.enable_notify_insert</c>).</summary>
/// <param name="QueueName">The queue.</param>
/// <param name="ThrottleInterval">The shortest time between two notifications.</param>
/// <param name="LastNotifiedAt">When the last notification went out.</param>
public sealed record PgmqInsertNotification(string QueueName, TimeSpan ThrottleInterval, DateTimeOffset LastNotifiedAt)
{
    /// <summary>The channel the notifications go to: <c>LISTEN "pgmq.q_{queue}.INSERT"</c>.</summary>
    public string Channel => PgmqQueueManager.InsertChannel(QueueName);
}
