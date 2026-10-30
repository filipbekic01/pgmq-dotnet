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
}
