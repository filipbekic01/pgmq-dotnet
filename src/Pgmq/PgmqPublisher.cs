using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Npgmq;

namespace Pgmq;

/// <summary>
/// <see cref="IPgmqPublisher"/> on PGMQ: one copy of each message per subscribed queue, a delay being PGMQ's own
/// delayed send. The copies are separate writes, not one transaction. Topic publishes are one <c>pgmq.send_topic</c>
/// call, which writes to every bound queue in one statement.
/// </summary>
/// <param name="client">The PGMQ client to write with.</param>
/// <param name="topology">Which queues each message type goes to.</param>
/// <param name="logger">Notes messages that no queue is subscribed to.</param>
public sealed class PgmqPublisher(INpgmqClient client, PgmqTopology topology, ILogger<PgmqPublisher> logger)
    : IPgmqPublisher
{
    private static readonly PgmqPublishOptions NoOptions = new();

    /// <inheritdoc />
    public Task PublishAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default)
        where TMessage : class
        => PublishCoreAsync(message, typeof(TMessage), NoOptions, cancellationToken);

    /// <inheritdoc />
    public Task PublishAsync<TMessage>(TMessage message, PgmqPublishOptions options,
        CancellationToken cancellationToken = default) where TMessage : class
        => PublishCoreAsync(message, typeof(TMessage), options, cancellationToken);

    /// <inheritdoc />
    public Task PublishAsync(object message, Type messageType, CancellationToken cancellationToken = default)
        => PublishCoreAsync(message, messageType, NoOptions, cancellationToken);

    /// <inheritdoc />
    public Task PublishAsync(object message, Type messageType, PgmqPublishOptions options,
        CancellationToken cancellationToken = default)
        => PublishCoreAsync(message, messageType, options, cancellationToken);

    /// <inheritdoc />
    public Task PublishBatchAsync<TMessage>(IEnumerable<TMessage> messages,
        CancellationToken cancellationToken = default) where TMessage : class
        => PublishBatchAsync(messages, NoOptions, cancellationToken);

    /// <inheritdoc />
    public async Task PublishBatchAsync<TMessage>(IEnumerable<TMessage> messages, PgmqPublishOptions options,
        CancellationToken cancellationToken = default) where TMessage : class
    {
        ArgumentNullException.ThrowIfNull(messages);
        ArgumentNullException.ThrowIfNull(options);

        TMessage[] batch = [.. messages];
        if (batch.Length == 0)
        {
            return;
        }

        TimeSpan? delay = options.ResolveDelay();
        using Activity? activity = PgmqTelemetry.StartSend(MessageNames.Of(typeof(TMessage)));
        activity?.SetTag("messaging.batch.message_count", batch.Length);

        string[] bodies = [.. batch.Select(m => MessageSerializer.Serialize(m, typeof(TMessage)))];

        foreach (string queue in QueuesFor(typeof(TMessage)))
        {
            IReadOnlyDictionary<string, object>[] headers =
                [.. batch.Select(_ => NewHeaders(typeof(TMessage), options).ToDictionary())];

            await (delay is { } d && d > TimeSpan.Zero
                ? client.SendBatchAsync(queue, bodies, headers, DateTimeOffset.UtcNow + d, cancellationToken)
                : client.SendBatchAsync(queue, bodies, headers, cancellationToken));
        }
    }

    /// <inheritdoc />
    public Task SchedulePublishAsync<TMessage>(TMessage message, TimeSpan delay,
        CancellationToken cancellationToken = default) where TMessage : class
        => PublishCoreAsync(message, typeof(TMessage), new() { Delay = delay }, cancellationToken);

    /// <inheritdoc />
    public Task SchedulePublishAsync<TMessage>(TMessage message, DateTimeOffset deliverAt,
        CancellationToken cancellationToken = default) where TMessage : class
        => PublishCoreAsync(message, typeof(TMessage), new() { DeliverAt = deliverAt }, cancellationToken);

    /// <inheritdoc />
    public Task<int> PublishToTopicAsync<TMessage>(string routingKey, TMessage message,
        CancellationToken cancellationToken = default) where TMessage : class
        => PublishToTopicAsync(routingKey, message, NoOptions, cancellationToken);

    /// <inheritdoc />
    public async Task<int> PublishToTopicAsync<TMessage>(string routingKey, TMessage message,
        PgmqPublishOptions options, CancellationToken cancellationToken = default) where TMessage : class
    {
        ArgumentException.ThrowIfNullOrEmpty(routingKey);
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(options);

        int delaySeconds = Seconds(options.ResolveDelay());
        using Activity? activity = PgmqTelemetry.StartSend(MessageNames.Of(typeof(TMessage)), routingKey);

        return await client.SendTopicAsync(routingKey, MessageSerializer.Serialize(message, typeof(TMessage)),
            NewHeaders(typeof(TMessage), options).ToDictionary(), delaySeconds, cancellationToken);
    }

    /// <inheritdoc />
    public Task<int> PublishBatchToTopicAsync<TMessage>(string routingKey, IEnumerable<TMessage> messages,
        CancellationToken cancellationToken = default) where TMessage : class
        => PublishBatchToTopicAsync(routingKey, messages, NoOptions, cancellationToken);

    /// <inheritdoc />
    public async Task<int> PublishBatchToTopicAsync<TMessage>(string routingKey, IEnumerable<TMessage> messages,
        PgmqPublishOptions options, CancellationToken cancellationToken = default) where TMessage : class
    {
        ArgumentException.ThrowIfNullOrEmpty(routingKey);
        ArgumentNullException.ThrowIfNull(messages);
        ArgumentNullException.ThrowIfNull(options);

        TMessage[] batch = [.. messages];
        if (batch.Length == 0)
        {
            return 0;
        }

        int delaySeconds = Seconds(options.ResolveDelay());
        using Activity? activity = PgmqTelemetry.StartSend(MessageNames.Of(typeof(TMessage)), routingKey);
        activity?.SetTag("messaging.batch.message_count", batch.Length);

        string[] bodies = [.. batch.Select(m => MessageSerializer.Serialize(m, typeof(TMessage)))];
        IReadOnlyDictionary<string, object>[] headers =
            [.. batch.Select(_ => NewHeaders(typeof(TMessage), options).ToDictionary())];

        List<NpgmqSendTopicResult> sent =
            await client.SendBatchTopicAsync(routingKey, bodies, headers, delaySeconds, cancellationToken);

        return sent.Select(s => s.QueueName).Distinct(StringComparer.Ordinal).Count();
    }

    /// <summary>
    /// Writes straight to one queue, past the fan-out: how a consumer's message goes back to where it came from. A
    /// <paramref name="messageId"/> keeps the id it had, so the copy reads as the same message; the group and headers
    /// go with it.
    /// </summary>
    internal async Task SendAsync(string queueName, object message, Type messageType, TimeSpan delay,
        int redeliveryCount, Guid? messageId, string? groupId, IReadOnlyDictionary<string, string>? headers,
        CancellationToken cancellationToken)
    {
        using Activity? activity = PgmqTelemetry.StartSend(MessageNames.Of(messageType), queueName);

        await SendAsync(queueName, MessageSerializer.Serialize(message, messageType),
            MessageHeaders.New(messageType, redeliveryCount, messageId, groupId, headers), delay, cancellationToken);
    }

    private async Task PublishCoreAsync(object message, Type messageType, PgmqPublishOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(options);

        TimeSpan? delay = options.ResolveDelay();
        using Activity? activity = PgmqTelemetry.StartSend(MessageNames.Of(messageType));

        string body = MessageSerializer.Serialize(message, messageType);

        foreach (string queue in QueuesFor(messageType))
        {
            await SendAsync(queue, body, NewHeaders(messageType, options), delay, cancellationToken);
        }
    }

    private static MessageHeaders NewHeaders(Type messageType, PgmqPublishOptions options)
        => MessageHeaders.New(messageType, groupId: options.GroupId, custom: options.Headers);

    /// <summary>pgmq's topic sends take whole seconds; a part of one rounds up, so a message is never early.</summary>
    private static int Seconds(TimeSpan? delay)
        => delay is { } d && d > TimeSpan.Zero ? (int)Math.Ceiling(d.TotalSeconds) : 0;

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
