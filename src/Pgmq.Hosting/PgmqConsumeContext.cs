using Pgmq.Hosting;

namespace Pgmq;

/// <summary>
/// What a consumer gets besides the message: what it arrived with, and ways to send more. Everything sent through a
/// context waits in an outbox until the consumer returns, and is dropped if it throws, so a retried consumer does not
/// send the same messages twice.
/// </summary>
public abstract class PgmqConsumeContext
{
    private readonly IPgmqPublisher? _publisher;
    private readonly SendBack? _sendBack;
    private readonly Outbox? _outbox;

    private protected PgmqConsumeContext(IPgmqPublisher? publisher, SendBack? sendBack, Outbox? outbox,
        Guid? messageId, int redeliveryCount)
    {
        _publisher = publisher;
        _sendBack = sendBack;
        _outbox = outbox;
        MessageId = messageId;
        RedeliveryCount = redeliveryCount;
    }

    /// <summary>The id the message was published with. It stays the same when the message is redelivered.</summary>
    public Guid? MessageId { get; }

    /// <summary>
    /// How many times this message came back before: once for every failed attempt, and once for every
    /// <see cref="PgmqConsumeContext{TMessage}.RedeliverAsync"/>. Zero on the first delivery.
    /// </summary>
    public int RedeliveryCount { get; }

    /// <summary>Publishes <paramref name="message"/> once this consumer succeeds.</summary>
    public Task PublishAsync<TOther>(TOther message, CancellationToken cancellationToken = default) where TOther : class
        => SendAsync(ct => Publisher.PublishAsync(message, ct), cancellationToken);

    /// <summary>Publishes <paramref name="message"/> as <paramref name="messageType"/> once this consumer succeeds.</summary>
    public Task PublishAsync(object message, Type messageType, CancellationToken cancellationToken = default)
        => SendAsync(ct => Publisher.PublishAsync(message, messageType, ct), cancellationToken);

    /// <summary>Publishes <paramref name="messages"/> once this consumer succeeds.</summary>
    public Task PublishBatchAsync<TOther>(IEnumerable<TOther> messages, CancellationToken cancellationToken = default)
        where TOther : class
    {
        TOther[] batch = [.. messages];
        return SendAsync(ct => Publisher.PublishBatchAsync(batch, ct), cancellationToken);
    }

    /// <summary>Publishes <paramref name="message"/> after <paramref name="delay"/>, once this consumer succeeds.</summary>
    public Task SchedulePublishAsync<TOther>(TOther message, TimeSpan delay, CancellationToken cancellationToken = default)
        where TOther : class
        => SendAsync(ct => Publisher.SchedulePublishAsync(message, delay, ct), cancellationToken);

    /// <summary>
    /// Sends <paramref name="message"/> to this consumer's own queue after <paramref name="delay"/>, without fanning it
    /// out to anyone else subscribed to its type, once this consumer succeeds.
    /// </summary>
    public Task SendToSelfAsync<TOther>(TOther message, TimeSpan delay, CancellationToken cancellationToken = default)
        where TOther : class
        => _sendBack is null
            ? SchedulePublishAsync(message, delay, cancellationToken)
            : SendAsync(ct => _sendBack(message, typeof(TOther), delay, 0, null, ct), cancellationToken);

    private protected Task SendBackAsync(object message, Type messageType, TimeSpan delay, int redeliveryCount)
        => _sendBack is null
            ? throw new InvalidOperationException("This message was not read from a queue, so it cannot go back to one.")
            : SendAsync(ct => _sendBack(message, messageType, delay, redeliveryCount, MessageId, ct), default);

    /// <summary>Holds the send in the outbox, or sends it now when there is none (a test calling a consumer).</summary>
    private Task SendAsync(Func<CancellationToken, Task> send, CancellationToken cancellationToken)
    {
        if (_outbox is null)
        {
            return send(cancellationToken);
        }

        _outbox.Add(send);
        return Task.CompletedTask;
    }

    private IPgmqPublisher Publisher
        => _publisher ?? throw new InvalidOperationException("This context was created without a publisher.");
}

/// <summary>The context of one <typeparamref name="TMessage"/>.</summary>
/// <typeparam name="TMessage">The type the consumer asked for.</typeparam>
public sealed class PgmqConsumeContext<TMessage> : PgmqConsumeContext where TMessage : class
{
    /// <summary>
    /// A context for calling a consumer directly, as a test does. Without an outbox, what the consumer sends goes out
    /// at once through <paramref name="publisher"/>.
    /// </summary>
    /// <param name="message">The message to hand the consumer.</param>
    /// <param name="publisher">Where the consumer's publishes go; null if it publishes nothing.</param>
    /// <param name="redeliveryCount">What <see cref="PgmqConsumeContext.RedeliveryCount"/> reports.</param>
    /// <param name="messageId">What <see cref="PgmqConsumeContext.MessageId"/> reports.</param>
    public PgmqConsumeContext(TMessage message, IPgmqPublisher? publisher = null, int redeliveryCount = 0,
        Guid? messageId = null)
        : this(message, publisher, sendBack: null, outbox: null, messageId, redeliveryCount)
    {
    }

    internal PgmqConsumeContext(TMessage message, IPgmqPublisher? publisher, SendBack? sendBack, Outbox? outbox,
        Guid? messageId, int redeliveryCount)
        : base(publisher, sendBack, outbox, messageId, redeliveryCount)
    {
        ArgumentNullException.ThrowIfNull(message);
        Message = message;
    }

    /// <summary>The message.</summary>
    public TMessage Message { get; }

    /// <summary>
    /// Puts this message back on this consumer's queue after <paramref name="delay"/>, with the same
    /// <see cref="PgmqConsumeContext.MessageId"/> and a <see cref="PgmqConsumeContext.RedeliveryCount"/> one higher.
    /// The current delivery then completes normally.
    /// </summary>
    public Task RedeliverAsync(TimeSpan delay)
        => SendBackAsync(Message, typeof(TMessage), delay, RedeliveryCount + 1);
}
