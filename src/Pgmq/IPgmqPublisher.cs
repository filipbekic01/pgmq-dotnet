namespace Pgmq;

/// <summary>
/// Publishes messages. A message is written to every queue subscribed to its type (see <see cref="PgmqTopology"/>);
/// with no subscriber it goes nowhere. Delivery is at least once, so consumers should be idempotent.
/// </summary>
public interface IPgmqPublisher
{
    /// <summary>Publishes <paramref name="message"/> as <typeparamref name="TMessage"/>.</summary>
    Task PublishAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default)
        where TMessage : class;

    /// <summary>
    /// Publishes <paramref name="message"/> as <paramref name="messageType"/>, for a message whose type is picked at
    /// run time. Publishing it generically would route it by its static type instead.
    /// </summary>
    Task PublishAsync(object message, Type messageType, CancellationToken cancellationToken = default);

    /// <summary>Publishes <paramref name="messages"/> with one write per subscribed queue.</summary>
    Task PublishBatchAsync<TMessage>(IEnumerable<TMessage> messages, CancellationToken cancellationToken = default)
        where TMessage : class;

    /// <summary>Publishes <paramref name="message"/> so that it can be read no sooner than <paramref name="delay"/> from now.</summary>
    Task SchedulePublishAsync<TMessage>(TMessage message, TimeSpan delay, CancellationToken cancellationToken = default)
        where TMessage : class;

    /// <summary>Publishes <paramref name="message"/> so that it can be read no sooner than <paramref name="deliverAt"/>.</summary>
    Task SchedulePublishAsync<TMessage>(TMessage message, DateTimeOffset deliverAt,
        CancellationToken cancellationToken = default) where TMessage : class;
}
