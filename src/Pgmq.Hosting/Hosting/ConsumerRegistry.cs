using System.Reflection;
using System.Security.Cryptography;
using System.Text;

namespace Pgmq.Hosting;

/// <summary>A consumer as registered: its class, and what was said about it at registration.</summary>
internal sealed record ConsumerDescriptor(Type ConsumerType, PgmqConsumerOptions Options)
{
    public IReadOnlyList<Type> MessageTypes { get; } = MessageTypesOf(ConsumerType);

    public static IReadOnlyList<Type> MessageTypesOf(Type consumerType)
        =>
        [
            .. consumerType.GetInterfaces()
                .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IPgmqConsumer<>))
                .Select(i => i.GetGenericArguments()[0])
        ];
}

/// <summary>A consumer with everything resolved: its queue, the types it takes, its concurrency and timeout.</summary>
internal sealed record ConsumerRegistration(
    Type ConsumerType,
    string QueueName,
    IReadOnlyList<Type> MessageTypes,
    int Concurrency,
    TimeSpan Timeout);

/// <summary>Every registered consumer, resolved against the options, and the topology they make.</summary>
internal sealed class ConsumerRegistry
{
    public ConsumerRegistry(IEnumerable<ConsumerDescriptor> descriptors, PgmqOptions options)
    {
        Consumers =
        [
            .. descriptors
                .DistinctBy(d => d.ConsumerType)
                .Select(d => Resolve(d, options))
                .OrderBy(r => r.QueueName, StringComparer.Ordinal)
        ];

        string[] shared = [.. Consumers.GroupBy(c => c.QueueName).Where(g => g.Count() > 1)
            .Select(g => $"{g.Key} ({string.Join(", ", g.Select(c => c.ConsumerType.FullName))})")];
        if (shared.Length > 0)
        {
            throw new InvalidOperationException(
                $"Two consumers share a queue: {string.Join("; ", shared)}. Give one a different [QueueName].");
        }

        foreach (ConsumerRegistration consumer in Consumers)
        {
            foreach (Type messageType in consumer.MessageTypes)
            {
                Topology.Subscribe(messageType, consumer.QueueName);
            }
        }
    }

    public IReadOnlyList<ConsumerRegistration> Consumers { get; }

    public PgmqTopology Topology { get; } = new();

    /// <summary>
    /// The class's namespace and name, lowercased, with every other character an underscore. A name too long for
    /// PGMQ is cut and given a hash of the full name, so two cut names never collide.
    /// </summary>
    public static string DefaultQueueName(Type consumerType)
    {
        string fullName = consumerType.FullName ?? consumerType.Name;

        StringBuilder builder = new(fullName.Length);
        foreach (char c in fullName)
        {
            builder.Append(char.IsAsciiLetterOrDigit(c) ? char.ToLowerInvariant(c) : '_');
        }

        string name = builder.ToString();
        if (name.Length <= PgmqTopology.MaxQueueNameLength)
        {
            return name;
        }

        string hash = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(fullName)))[..6];
        return $"{name[..(PgmqTopology.MaxQueueNameLength - hash.Length - 1)]}_{hash}";
    }

    private static ConsumerRegistration Resolve(ConsumerDescriptor descriptor, PgmqOptions options)
    {
        Type type = descriptor.ConsumerType;
        ConsumerOptionsAttribute? attribute = type.GetCustomAttribute<ConsumerOptionsAttribute>();

        string queueName = descriptor.Options.QueueName
                           ?? type.GetCustomAttribute<QueueNameAttribute>()?.Name
                           ?? options.QueueNameFormatter?.Invoke(type)
                           ?? DefaultQueueName(type);
        PgmqTopology.EnsureValidQueueName(queueName);

        int concurrency = descriptor.Options.Concurrency
                          ?? (attribute?.Concurrency > 0 ? attribute.Concurrency : options.DefaultConcurrency);
        TimeSpan timeout = descriptor.Options.Timeout
                           ?? (attribute?.TimeoutSeconds > 0
                               ? TimeSpan.FromSeconds(attribute.TimeoutSeconds)
                               : options.DefaultTimeout);

        ArgumentOutOfRangeException.ThrowIfLessThan(concurrency, 1, $"{type.Name} concurrency");
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero, $"{type.Name} timeout");

        return new(type, queueName, descriptor.MessageTypes, concurrency, timeout);
    }
}
