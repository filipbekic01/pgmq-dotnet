namespace Pgmq;

/// <summary>
/// Pins a consumer's queue. By default the name comes from the consumer's namespace and class name, so renaming or
/// moving it switches to a new, empty queue and strands what waits in the old one. Pin the current name before
/// renaming or moving a consumer.
/// </summary>
/// <param name="name">1 to 47 lowercase letters, digits and underscores.</param>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class QueueNameAttribute(string name) : Attribute
{
    /// <summary>The queue name.</summary>
    public string Name { get; } = name;
}

/// <summary>
/// How one consumer's queue runs. Unset values take the defaults in <see cref="PgmqOptions"/>; values passed to
/// <c>AddPgmqConsumer</c> win over these.
/// <code>
/// [ConsumerOptions(Concurrency = 1, TimeoutSeconds = 600)]
/// public sealed class RebuildFeedHandler : IPgmqConsumer&lt;RebuildFeed&gt; { ... }
/// </code>
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class ConsumerOptionsAttribute : Attribute
{
    /// <summary>
    /// How many messages of this queue one instance handles at once. Each instance applies it on its own, so a
    /// deployment runs up to this times its number of instances.
    /// </summary>
    public int Concurrency { get; init; }

    /// <summary>How long one message may run before its cancellation token fires and it is archived.</summary>
    public int TimeoutSeconds { get; init; }

    /// <summary>How the queue is read; see <see cref="PgmqReadMode"/>. Standard by default.</summary>
    public PgmqReadMode ReadMode { get; init; }

    /// <summary>How the queue's tables are stored when it is created; see <see cref="PgmqQueueType"/>.</summary>
    public PgmqQueueType QueueType { get; init; }

    /// <summary>For a partitioned queue, see <see cref="PgmqQueueOptions.PartitionInterval"/>.</summary>
    public string? PartitionInterval { get; init; }

    /// <summary>For a partitioned queue, see <see cref="PgmqQueueOptions.RetentionInterval"/>.</summary>
    public string? RetentionInterval { get; init; }

    /// <summary>
    /// Read with <c>pgmq.pop</c>, which deletes a message as it is read: each message is handled at most once, with no
    /// lease, retry or archive. A message whose consumer fails or times out is lost.
    /// </summary>
    public bool AtMostOnce { get; init; }

    /// <summary>
    /// A JSON object the message body must contain to be read (pgmq's experimental conditional read,
    /// <c>message @&gt; filter</c>), as in <c>{"region":"eu"}</c>. Messages that do not match stay in the queue.
    /// Standard read mode only.
    /// </summary>
    public string? Filter { get; init; }
}

/// <summary>
/// Binds a topic pattern to the consumer's queue at start-up (<c>pgmq.bind_topic</c>), so what
/// <see cref="IPgmqPublisher.PublishToTopicAsync{TMessage}(string, TMessage, CancellationToken)"/> sends to a matching
/// routing key reaches it. <c>*</c> matches one dot-separated word and <c>#</c> any number. A message arriving this way
/// still needs a type the consumer takes.
/// <code>
/// [TopicBinding("orders.*.created")]
/// public sealed class OrderAuditHandler : IPgmqConsumer&lt;OrderCreated&gt; { ... }
/// </code>
/// </summary>
/// <param name="pattern">The pattern, such as <c>orders.*</c> or <c>logs.#</c>.</param>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
public sealed class TopicBindingAttribute(string pattern) : Attribute
{
    /// <summary>The pattern.</summary>
    public string Pattern { get; } = pattern;
}

/// <summary>Per-consumer settings given at registration. Unset values fall back to the attributes, then the defaults.</summary>
public sealed class PgmqConsumerOptions
{
    /// <summary>The queue name; see <see cref="QueueNameAttribute"/>.</summary>
    public string? QueueName { get; set; }

    /// <summary>Messages handled at once per instance; see <see cref="ConsumerOptionsAttribute.Concurrency"/>.</summary>
    public int? Concurrency { get; set; }

    /// <summary>How long one message may run; see <see cref="ConsumerOptionsAttribute.TimeoutSeconds"/>.</summary>
    public TimeSpan? Timeout { get; set; }

    /// <summary>How the queue is read; see <see cref="PgmqReadMode"/>.</summary>
    public PgmqReadMode? ReadMode { get; set; }

    /// <summary>How the queue is created when it does not exist yet; see <see cref="PgmqQueueOptions"/>.</summary>
    public PgmqQueueOptions? Queue { get; set; }

    /// <summary>Handle each message at most once; see <see cref="ConsumerOptionsAttribute.AtMostOnce"/>.</summary>
    public bool? AtMostOnce { get; set; }

    /// <summary>Only read matching messages; see <see cref="ConsumerOptionsAttribute.Filter"/>.</summary>
    public string? Filter { get; set; }

    /// <summary>Topic patterns bound to the queue, besides any <see cref="TopicBindingAttribute"/>.</summary>
    public IList<string> TopicBindings { get; } = [];

    /// <summary>
    /// Wake this consumer's read loop through LISTEN/NOTIFY when a message arrives; see
    /// <see cref="PgmqOptions.UseInsertNotifications"/>.
    /// </summary>
    public bool? UseInsertNotifications { get; set; }
}
