using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Npgmq;

namespace Pgmq;

/// <summary>
/// <see cref="IPgmqPublisher"/> on PGMQ: one copy of each message per subscribed queue, a delay being PGMQ's own
/// delayed send. The copies are separate writes, not one transaction.
/// </summary>
/// <param name="client">The PGMQ client to write with.</param>
/// <param name="topology">Which queues each message type goes to.</param>
/// <param name="logger">Notes messages that no queue is subscribed to.</param>
public sealed class PgmqPublisher(INpgmqClient client, PgmqTopology topology, ILogger<PgmqPublisher> logger)
    : IPgmqPublisher
{
    /// <inheritdoc />
    public Task PublishAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default)
        where TMessage : class
        => PublishAsync(message, typeof(TMessage), delay: null, cancellationToken);

    /// <inheritdoc />
    public Task PublishAsync(object message, Type messageType, CancellationToken cancellationToken = default)
        => PublishAsync(message, messageType, delay: null, cancellationToken);

    /// <inheritdoc />
    public async Task PublishBatchAsync<TMessage>(IEnumerable<TMessage> messages,
        CancellationToken cancellationToken = default) where TMessage : class
    {
        ArgumentNullException.ThrowIfNull(messages);

        TMessage[] batch = [.. messages];
        if (batch.Length == 0)
        {
            return;
        }

        using Activity? activity = PgmqTelemetry.StartSend(MessageNames.Of(typeof(TMessage)));
        activity?.SetTag("messaging.batch.message_count", batch.Length);

        string[] bodies = [.. batch.Select(m => MessageSerializer.Serialize(m, typeof(TMessage)))];

        foreach (string queue in QueuesFor(typeof(TMessage)))
        {
            IReadOnlyDictionary<string, object>[] headers =
                [.. batch.Select(_ => MessageHeaders.New(typeof(TMessage)).ToDictionary())];

            await client.SendBatchAsync(queue, bodies, headers, cancellationToken);
        }
    }

    /// <inheritdoc />
    public Task SchedulePublishAsync<TMessage>(TMessage message, TimeSpan delay,
        CancellationToken cancellationToken = default) where TMessage : class
        => PublishAsync(message, typeof(TMessage), delay, cancellationToken);

    /// <inheritdoc />
    public Task SchedulePublishAsync<TMessage>(TMessage message, DateTimeOffset deliverAt,
        CancellationToken cancellationToken = default) where TMessage : class
        => PublishAsync(message, typeof(TMessage), deliverAt - DateTimeOffset.UtcNow, cancellationToken);

    /// <summary>
    /// Writes straight to one queue, past the fan-out: how a consumer's message goes back to where it came from. A
    /// <paramref name="messageId"/> keeps the id it had, so the copy reads as the same message.
    /// </summary>
    internal async Task SendAsync(string queueName, object message, Type messageType, TimeSpan delay,
        int redeliveryCount, Guid? messageId, CancellationToken cancellationToken)
    {
        using Activity? activity = PgmqTelemetry.StartSend(MessageNames.Of(messageType), queueName);

        await SendAsync(queueName, MessageSerializer.Serialize(message, messageType),
            MessageHeaders.New(messageType, redeliveryCount, messageId), delay, cancellationToken);
    }

    private async Task PublishAsync(object message, Type messageType, TimeSpan? delay,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(messageType);

        using Activity? activity = PgmqTelemetry.StartSend(MessageNames.Of(messageType));

        string body = MessageSerializer.Serialize(message, messageType);

        foreach (string queue in QueuesFor(messageType))
        {
            await SendAsync(queue, body, MessageHeaders.New(messageType), delay, cancellationToken);
        }
    }

    private Task<long> SendAsync(string queueName, string body, MessageHeaders headers, TimeSpan? delay,
        CancellationToken cancellationToken)
        => delay is { } d && d > TimeSpan.Zero
            ? client.SendAsync(queueName, body, headers.ToDictionary(), DateTimeOffset.UtcNow + d, cancellationToken)
            : client.SendAsync(queueName, body, headers.ToDictionary(), cancellationToken);

    private IReadOnlyList<string> QueuesFor(Type messageType)
    {
        IReadOnlyList<string> queues = topology.QueuesFor(messageType);
        if (queues.Count == 0)
        {
            logger.LogDebug("No queue is subscribed to {MessageType}; the message goes nowhere", messageType.FullName);
        }

        return queues;
    }
}
