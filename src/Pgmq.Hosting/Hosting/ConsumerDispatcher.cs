using System.Collections.Concurrent;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace Pgmq.Hosting;

/// <summary>
/// Puts a message back on the queue it was read from, after a delay, with the given redelivery count. A
/// <paramref name="messageId"/> keeps the id it had, so the copy reads as the same message.
/// </summary>
internal delegate Task SendBack(object message, Type messageType, TimeSpan delay, int redeliveryCount, Guid? messageId,
    CancellationToken cancellationToken);

/// <summary>How a message reached its consumer: where to publish, how to send it back, and the outbox its sends wait in.</summary>
internal sealed record Delivery(
    IPgmqPublisher Publisher,
    Guid? MessageId,
    int RedeliveryCount,
    Outbox Outbox,
    SendBack SendBack);

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
                delivery.MessageId, delivery.RedeliveryCount),
            cancellationToken);
    }
}

/// <summary>
/// What a consumer sends while it runs, held back until it succeeds. In memory only: it closes the gap between a
/// consumer's failure and its publishes, not a crash between the flush and the message's delete.
/// </summary>
internal sealed class Outbox
{
    private readonly Queue<Func<CancellationToken, Task>> _pending = new();
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

    public void Add(Func<CancellationToken, Task> send)
    {
        lock (_lock)
        {
            _pending.Enqueue(send);
        }
    }

    /// <summary>
    /// Sends everything held, in order. A send leaves the queue only once it went through, so calling this again after
    /// a failure resends only what had not gone out.
    /// </summary>
    public async Task FlushAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            Func<CancellationToken, Task>? next;
            lock (_lock)
            {
                if (!_pending.TryPeek(out next))
                {
                    return;
                }
            }

            await next(cancellationToken);

            lock (_lock)
            {
                _pending.Dequeue();
            }
        }
    }
}
