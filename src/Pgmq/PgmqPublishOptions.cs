namespace Pgmq;

/// <summary>How to publish a message: when it becomes readable, its FIFO group, and headers of your own.</summary>
public sealed record PgmqPublishOptions
{
    /// <summary>Make the message readable no sooner than this from now. Do not set with <see cref="DeliverAt"/>.</summary>
    public TimeSpan? Delay { get; init; }

    /// <summary>Make the message readable no sooner than this. Do not set with <see cref="Delay"/>.</summary>
    public DateTimeOffset? DeliverAt { get; init; }

    /// <summary>
    /// The FIFO group (pgmq's <c>x-pgmq-group</c> header). A consumer reading its queue in a grouped
    /// <see cref="PgmqReadMode"/> handles the messages of one group one at a time, in the order they were sent, and
    /// different groups in parallel. Messages without a group share one default group.
    /// </summary>
    public string? GroupId { get; init; }

    /// <summary>
    /// Headers of your own, sent with the message and readable by its consumer. The names Pgmq uses itself
    /// (<c>message-type</c>, <c>message-id</c>, <c>redelivery-count</c>, <c>traceparent</c>, <c>tracestate</c> and
    /// <c>x-pgmq-group</c>) are refused.
    /// </summary>
    public IReadOnlyDictionary<string, string>? Headers { get; init; }

    /// <summary>The delay to send with, from either <see cref="Delay"/> or <see cref="DeliverAt"/>.</summary>
    internal TimeSpan? ResolveDelay()
    {
        if (Delay is not null && DeliverAt is not null)
        {
            throw new ArgumentException($"Set {nameof(Delay)} or {nameof(DeliverAt)}, not both.");
        }

        return Delay ?? (DeliverAt is { } at ? at - DateTimeOffset.UtcNow : null);
    }
}

/// <summary>
/// How a consumer reads its queue. The grouped modes are pgmq's FIFO reads: within one
/// <see cref="PgmqPublishOptions.GroupId"/> messages are handled one at a time and in order, and a group waits while
/// one of its messages is being handled or retried.
/// </summary>
public enum PgmqReadMode
{
    /// <summary>Oldest first, with no ordering between messages handled at once (<c>pgmq.read</c>).</summary>
    Standard,

    /// <summary>
    /// FIFO, filling a read from the group with the oldest message first, as SQS FIFO does (<c>pgmq.read_grouped</c>).
    /// </summary>
    Grouped,

    /// <summary>FIFO, taking messages from the groups in turn (<c>pgmq.read_grouped_rr</c>).</summary>
    GroupedRoundRobin,

    /// <summary>FIFO, taking only the oldest message of each group (<c>pgmq.read_grouped_head</c>).</summary>
    GroupedHead,
}
