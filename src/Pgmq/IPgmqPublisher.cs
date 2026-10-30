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
    /// Publishes <paramref name="message"/> as <typeparamref name="TMessage"/>, with a delay, a FIFO group or headers
    /// of your own.
    /// </summary>
    Task PublishAsync<TMessage>(TMessage message, PgmqPublishOptions options,
        CancellationToken cancellationToken = default) where TMessage : class;

    /// <summary>
    /// Publishes <paramref name="message"/> as <paramref name="messageType"/>, for a message whose type is picked at
    /// run time. Publishing it generically would route it by its static type instead.
    /// </summary>
    Task PublishAsync(object message, Type messageType, CancellationToken cancellationToken = default);

    /// <summary>Publishes <paramref name="message"/> as <paramref name="messageType"/> with <paramref name="options"/>.</summary>
    Task PublishAsync(object message, Type messageType, PgmqPublishOptions options,
        CancellationToken cancellationToken = default);

    /// <summary>Publishes <paramref name="messages"/> with one write per subscribed queue.</summary>
    Task PublishBatchAsync<TMessage>(IEnumerable<TMessage> messages, CancellationToken cancellationToken = default)
        where TMessage : class;

    /// <summary>
    /// Publishes <paramref name="messages"/> with one write per subscribed queue, all with the same
    /// <paramref name="options"/>.
    /// </summary>
    Task PublishBatchAsync<TMessage>(IEnumerable<TMessage> messages, PgmqPublishOptions options,
        CancellationToken cancellationToken = default) where TMessage : class;

    /// <summary>Publishes <paramref name="message"/> so that it can be read no sooner than <paramref name="delay"/> from now.</summary>
    Task SchedulePublishAsync<TMessage>(TMessage message, TimeSpan delay, CancellationToken cancellationToken = default)
        where TMessage : class;

    /// <summary>Publishes <paramref name="message"/> so that it can be read no sooner than <paramref name="deliverAt"/>.</summary>
    Task SchedulePublishAsync<TMessage>(TMessage message, DateTimeOffset deliverAt,
        CancellationToken cancellationToken = default) where TMessage : class;

    /// <summary>
    /// Publishes <paramref name="message"/> to every queue with a topic binding that matches
    /// <paramref name="routingKey"/> (<c>pgmq.send_topic</c>), instead of to the queues subscribed to its type. Bind
    /// queues with <c>[TopicBinding]</c> on a consumer or <see cref="IPgmqQueueManager.BindTopicAsync"/>.
    /// </summary>
    /// <returns>How many queues the message was written to.</returns>
    Task<int> PublishToTopicAsync<TMessage>(string routingKey, TMessage message,
        CancellationToken cancellationToken = default) where TMessage : class;

    /// <summary>Publishes <paramref name="message"/> to a topic with <paramref name="options"/>.</summary>
    /// <returns>How many queues the message was written to.</returns>
    Task<int> PublishToTopicAsync<TMessage>(string routingKey, TMessage message, PgmqPublishOptions options,
        CancellationToken cancellationToken = default) where TMessage : class;

    /// <summary>
    /// Publishes <paramref name="messages"/> to every queue bound to a pattern matching <paramref name="routingKey"/>
    /// in one write (<c>pgmq.send_batch_topic</c>).
    /// </summary>
    /// <returns>How many queues the messages were written to.</returns>
    Task<int> PublishBatchToTopicAsync<TMessage>(string routingKey, IEnumerable<TMessage> messages,
        CancellationToken cancellationToken = default) where TMessage : class;

    /// <summary>Publishes <paramref name="messages"/> to a topic with <paramref name="options"/>.</summary>
    /// <returns>How many queues the messages were written to.</returns>
    Task<int> PublishBatchToTopicAsync<TMessage>(string routingKey, IEnumerable<TMessage> messages,
        PgmqPublishOptions options, CancellationToken cancellationToken = default) where TMessage : class;
}
