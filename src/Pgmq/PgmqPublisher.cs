using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Pgmq;

/// <summary>
/// <see cref="IPgmqPublisher"/> on PGMQ: one copy of each message per subscribed queue, a delay being PGMQ's own
/// delayed send. The copies are separate writes, not one transaction. Topic publishes are one <c>pgmq.send_topic</c>
/// call, which writes to every bound queue in one statement.
/// </summary>
public sealed class PgmqPublisher : IPgmqPublisher
{
    private readonly IPgmqSender _client;
    private readonly PgmqTopology _topology;
    private readonly ILogger<PgmqPublisher> _logger;
    private readonly ServerEndpoint _server;

    /// <param name="dataSource">The database with the pgmq extension.</param>
    /// <param name="topology">Which queues each message type goes to.</param>
    /// <param name="logger">Notes messages that no queue is subscribed to.</param>
    public PgmqPublisher(NpgsqlDataSource dataSource, PgmqTopology topology, ILogger<PgmqPublisher> logger)
        : this(new PgmqSender(dataSource ?? throw new ArgumentNullException(nameof(dataSource))), topology, logger,
            ServerEndpoint.Of(dataSource))
    {
    }

    internal PgmqPublisher(IPgmqSender client, PgmqTopology topology, ILogger<PgmqPublisher> logger,
        ServerEndpoint server = default)
    {
        _server = server;
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _topology = topology ?? throw new ArgumentNullException(nameof(topology));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

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
        string messageName = MessageNames.Of(typeof(TMessage));
        IReadOnlyList<string> queues = QueuesFor(typeof(TMessage));

        await TraceSendAsync(messageName, messageName, batch.Length, queues, async () =>
        {
            string[] bodies = [.. batch.Select(m => MessageSerializer.Serialize(m, typeof(TMessage)))];

            foreach (string queue in queues)
            {
                IReadOnlyDictionary<string, object>[] headers =
                    [.. batch.Select(_ => NewHeaders(typeof(TMessage), options).ToDictionary())];

                await _client.SendBatchAsync(queue, bodies, headers, DeliverAt(delay), cancellationToken);
                PgmqTelemetry.RecordSent(queue, batch.Length, _server);
            }

            return 0;
        });
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

        return await TraceSendAsync(routingKey, MessageNames.Of(typeof(TMessage)), 1, queues: null, async () =>
        {
            int sent = await _client.SendTopicAsync(routingKey, MessageSerializer.Serialize(message, typeof(TMessage)),
                NewHeaders(typeof(TMessage), options).ToDictionary(), delaySeconds, cancellationToken);

            // pgmq says how many queues matched, not which: count the copies under the routing key.
            PgmqTelemetry.RecordSent(routingKey, sent, _server);
            return sent;
        });
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

        return await TraceSendAsync(routingKey, MessageNames.Of(typeof(TMessage)), batch.Length, queues: null,
            async () =>
            {
                string[] bodies = [.. batch.Select(m => MessageSerializer.Serialize(m, typeof(TMessage)))];
                IReadOnlyDictionary<string, object>[] headers =
                    [.. batch.Select(_ => NewHeaders(typeof(TMessage), options).ToDictionary())];

                IReadOnlyList<string> sent =
                    await _client.SendBatchTopicAsync(routingKey, bodies, headers, delaySeconds, cancellationToken);

                foreach (IGrouping<string, string> queue in sent.GroupBy(q => q, StringComparer.Ordinal))
                {
                    PgmqTelemetry.RecordSent(queue.Key, queue.Count(), _server);
                }

                return sent.Distinct(StringComparer.Ordinal).Count();
            });
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
        await TraceSendAsync(queueName, MessageNames.Of(messageType), 1, queues: null, async () =>
        {
            await SendAsync(queueName, MessageSerializer.Serialize(message, messageType),
                MessageHeaders.New(messageType, redeliveryCount, messageId, groupId, headers), delay,
                cancellationToken);
            return 0;
        });
    }

    private async Task PublishCoreAsync(object message, Type messageType, PgmqPublishOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(options);

        TimeSpan? delay = options.ResolveDelay();
        string messageName = MessageNames.Of(messageType);
        IReadOnlyList<string> queues = QueuesFor(messageType);

        await TraceSendAsync(messageName, messageName, 1, queues, async () =>
        {
            string body = MessageSerializer.Serialize(message, messageType);

            foreach (string queue in queues)
            {
                await SendAsync(queue, body, NewHeaders(messageType, options), delay, cancellationToken);
            }

            return 0;
        });
    }

    /// <summary>
    /// Runs one send under its span and records its duration. The span is current while <paramref name="send"/>
    /// builds headers, so each message carries it as its parent. A fan-out names the message type as its destination
    /// and lists the queues it reached in <c>messaging.pgmq.queues</c>.
    /// </summary>
    private async Task<T> TraceSendAsync<T>(string destination, string messageType, int count,
        IReadOnlyList<string>? queues, Func<Task<T>> send)
    {
        long start = Stopwatch.GetTimestamp();
        using Activity? activity = PgmqTelemetry.StartSend(destination, messageType, _server);
        if (activity is not null)
        {
            if (count > 1)
            {
                activity.SetTag("messaging.batch.message_count", count);
            }

            if (queues is not null)
            {
                activity.SetTag("messaging.pgmq.queues", queues.ToArray());
            }
        }

        string? errorType = null;
        try
        {
            return await send();
        }
        catch (Exception ex)
        {
            errorType = PgmqTelemetry.ErrorType(ex);
            PgmqTelemetry.Fail(activity, errorType, ex);
            throw;
        }
        finally
        {
            PgmqTelemetry.RecordOperation("send", "send", destination, start, errorType, _server);
        }
    }

    private static MessageHeaders NewHeaders(Type messageType, PgmqPublishOptions options)
        => MessageHeaders.New(messageType, groupId: options.GroupId, custom: options.Headers);

    /// <summary>pgmq's topic sends take whole seconds; a part of one rounds up, so a message is never early.</summary>
    private static int Seconds(TimeSpan? delay)
        => delay is { } d && d > TimeSpan.Zero ? (int)Math.Ceiling(d.TotalSeconds) : 0;

    private async Task SendAsync(string queueName, string body, MessageHeaders headers, TimeSpan? delay,
        CancellationToken cancellationToken)
    {
        await _client.SendAsync(queueName, body, headers.ToDictionary(), DeliverAt(delay), cancellationToken);
        PgmqTelemetry.RecordSent(queueName, 1, _server);
    }

    /// <summary>When a delayed message becomes visible, or null to send it visible at once.</summary>
    private static DateTimeOffset? DeliverAt(TimeSpan? delay)
        => delay is { } d && d > TimeSpan.Zero ? DateTimeOffset.UtcNow + d : null;

    private IReadOnlyList<string> QueuesFor(Type messageType)
    {
        IReadOnlyList<string> queues = _topology.QueuesFor(messageType);
        if (queues.Count == 0)
        {
            _logger.LogDebug("No queue is subscribed to {MessageType}; the message goes nowhere", messageType.FullName);
        }

        return queues;
    }
}
