using System.Collections.Concurrent;
using System.Reflection;
using System.Text.RegularExpressions;

namespace Pgmq;

/// <summary>
/// Which queues receive which message types. Each subscription ties a message type to one queue; publishing a message
/// writes one copy to every queue subscribed to its type or to a type it derives from. That is how one message reaches
/// several independent consumers.
/// <para>
/// Pgmq.Hosting builds this from the registered consumers, so a publisher and a consumer that register the same
/// consumers agree on it without talking to each other. Subscribe directly only to publish to queues whose consumers
/// run in another application.
/// </para>
/// </summary>
public sealed partial class PgmqTopology
{
    /// <summary>PGMQ turns a queue into the table <c>pgmq.q_{name}</c>, which caps the name at this length.</summary>
    public const int MaxQueueNameLength = 47;

    private readonly Lock _lock = new();
    private readonly List<(Type MessageType, string QueueName)> _subscriptions = [];
    private readonly Dictionary<string, Type> _subscribedTypesByName = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<Type, string[]> _queuesByType = new();
    private readonly ConcurrentDictionary<string, Type?> _typesByName = new(StringComparer.Ordinal);

    /// <summary>Every queue with at least one subscription.</summary>
    public IReadOnlyCollection<string> QueueNames
    {
        get
        {
            lock (_lock)
            {
                return [.. _subscriptions.Select(s => s.QueueName).Distinct(StringComparer.Ordinal)];
            }
        }
    }

    /// <summary>
    /// Sends every message of <paramref name="messageType"/>, and of types deriving from it, to
    /// <paramref name="queueName"/>.
    /// </summary>
    /// <exception cref="ArgumentException">The queue name is not a valid PGMQ queue name.</exception>
    /// <exception cref="InvalidOperationException">
    /// Another subscribed type already travels under the same message name.
    /// </exception>
    public void Subscribe(Type messageType, string queueName)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        EnsureValidQueueName(queueName);

        string name = MessageNames.Of(messageType);

        lock (_lock)
        {
            if (_subscribedTypesByName.TryGetValue(name, out Type? existing) && existing != messageType)
            {
                throw new InvalidOperationException(
                    $"{existing.FullName} and {messageType.FullName} both travel as \"{name}\". " +
                    "Give one of them a different [MessageName].");
            }

            _subscribedTypesByName[name] = messageType;

            if (!_subscriptions.Contains((messageType, queueName)))
            {
                _subscriptions.Add((messageType, queueName));
            }

            _queuesByType.Clear();
            _typesByName.Clear();
        }
    }

    /// <summary>The queues a message published as <paramref name="messageType"/> is written to.</summary>
    public IReadOnlyList<string> QueuesFor(Type messageType)
        => _queuesByType.GetOrAdd(messageType, type =>
        {
            lock (_lock)
            {
                return
                [
                    .. _subscriptions
                        .Where(s => s.MessageType.IsAssignableFrom(type))
                        .Select(s => s.QueueName)
                        .Distinct(StringComparer.Ordinal)
                ];
            }
        });

    /// <summary>
    /// The type a message read off a queue was published as, from the name in its headers. Only subscribed types, and
    /// loaded types deriving from one, resolve: a name read from a queue never picks an arbitrary type to deserialize.
    /// </summary>
    public Type? ResolveMessageType(string messageName)
        => _typesByName.GetOrAdd(messageName, name =>
        {
            Type[] subscribed;
            lock (_lock)
            {
                if (_subscribedTypesByName.TryGetValue(name, out Type? exact))
                {
                    return exact;
                }

                subscribed = [.. _subscribedTypesByName.Values];
            }

            return AppDomain.CurrentDomain.GetAssemblies()
                .Where(a => !a.IsDynamic)
                .SelectMany(LoadableTypes)
                .FirstOrDefault(t => t is { IsAbstract: false, ContainsGenericParameters: false }
                                     && subscribed.Any(s => s.IsAssignableFrom(t))
                                     && MessageNames.Of(t) == name);
        });

    /// <summary>Throws when <paramref name="queueName"/> is not lowercase letters, digits and underscores, 1 to 47 long.</summary>
    /// <exception cref="ArgumentException">The name is not valid.</exception>
    public static void EnsureValidQueueName(string queueName)
    {
        if (string.IsNullOrEmpty(queueName) || queueName.Length > MaxQueueNameLength || !QueueNamePattern().IsMatch(queueName))
        {
            throw new ArgumentException(
                $"\"{queueName}\" is not a valid queue name: use 1 to {MaxQueueNameLength} lowercase letters, digits " +
                "and underscores.", nameof(queueName));
        }
    }

    private static IEnumerable<Type> LoadableTypes(Assembly assembly)
    {
        try
        {
            return assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException e)
        {
            return e.Types.OfType<Type>();
        }
    }

    [GeneratedRegex("^[a-z0-9_]+$")]
    private static partial Regex QueueNamePattern();
}
