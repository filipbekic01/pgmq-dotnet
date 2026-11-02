using System.Collections.Concurrent;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace Pgmq.Hosting;

/// <summary>
/// Puts a message back on the queue it was read from, through <paramref name="publisher"/>, after a delay, with the
/// given redelivery count. A <paramref name="messageId"/> keeps the id it had, so the copy reads as the same message; a
/// group and headers go with it.
/// </summary>
internal delegate Task SendBack(IPgmqPublisher publisher, object message, Type messageType, TimeSpan delay, int redeliveryCount, Guid? messageId,
    string? groupId, IReadOnlyDictionary<string, string>? headers, CancellationToken cancellationToken);

/// <summary>
/// How a message reached its consumer: where to publish, how to send it back, the outbox its sends wait in, and the
/// group and headers it came with.
/// </summary>
internal sealed record Delivery(
    IPgmqPublisher Publisher,
    Guid? MessageId,
    int RedeliveryCount,
    Outbox Outbox,
    SendBack SendBack)
{
    public string? GroupId { get; init; }

    public IReadOnlyDictionary<string, string>? Headers { get; init; }
}

/// <summary>
/// Calls <see cref="IPgmqConsumer{TMessage}.HandleAsync"/> when the message type is only known at run time. The
/// consumer comes from the scope passed in, so it gets scoped services as a request handler would.
/// </summary>
internal static class ConsumerDispatcher
{
    private delegate Task Invoker(IServiceProvider services, Type consumerType, object message, Delivery delivery,
        CancellationToken cancellationToken);

    private static readonly MethodInfo InvokeMethod =
        typeof(ConsumerDispatcher).GetMethod(nameof(InvokeAsync), BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly ConcurrentDictionary<Type, Invoker> Invokers = new();

    public static Task DispatchAsync(IServiceProvider services, ConsumerRegistration consumer, object message,
        Type publishedType, Delivery delivery, CancellationToken cancellationToken)
    {
        // The consumer may take a base type of what was published: hand it over as the type it asked for.
        Type consumedType = consumer.MessageTypes.FirstOrDefault(m => m == publishedType)
                            ?? consumer.MessageTypes.FirstOrDefault(m => m.IsAssignableFrom(publishedType))
                            ?? throw new InvalidOperationException(
                                $"{consumer.ConsumerType.Name} does not consume {publishedType.FullName}.");

        Invoker invoker = Invokers.GetOrAdd(consumedType,
            t => InvokeMethod.MakeGenericMethod(t).CreateDelegate<Invoker>());

        return invoker(services, consumer.ConsumerType, message, delivery, cancellationToken);
    }

    private static Task InvokeAsync<TMessage>(IServiceProvider services, Type consumerType, object message,
        Delivery delivery, CancellationToken cancellationToken) where TMessage : class
    {
        IPgmqConsumer<TMessage> consumer = (IPgmqConsumer<TMessage>)services.GetRequiredService(consumerType);

        return consumer.HandleAsync(
            new PgmqConsumeContext<TMessage>((TMessage)message, delivery.Publisher, delivery.SendBack, delivery.Outbox,
                delivery.MessageId, delivery.RedeliveryCount, delivery.GroupId, delivery.Headers),
            cancellationToken);
    }
}

/// <summary>
/// What a consumer sends while it runs, held back until it succeeds. Each send is written against the publisher it is
/// flushed through, so the worker can flush it inside the transaction that deletes the message: then what the
/// consumer sent and the delete of its message commit together, or not at all.
/// </summary>
internal sealed class Outbox
{
    private readonly List<Func<IPgmqPublisher, CancellationToken, Task>> _pending = [];
    private readonly Lock _lock = new();

    public int Count
    {
        get
        {
            lock (_lock)
            {
                return _pending.Count;
            }
        }
    }

    public void Add(Func<IPgmqPublisher, CancellationToken, Task> send)
    {
        lock (_lock)
        {
            _pending.Add(send);
        }
    }

    /// <summary>
    /// Sends everything held, in order, through <paramref name="publisher"/>. Nothing leaves the outbox: a flush in a
    /// transaction that rolls back sent nothing, so the next flush sends it all again.
    /// </summary>
    public async Task FlushAsync(IPgmqPublisher publisher, CancellationToken cancellationToken)
    {
        Func<IPgmqPublisher, CancellationToken, Task>[] sends;
        lock (_lock)
        {
            sends = [.. _pending];
        }

        foreach (Func<IPgmqPublisher, CancellationToken, Task> send in sends)
        {
            await send(publisher, cancellationToken);
        }
    }
}
