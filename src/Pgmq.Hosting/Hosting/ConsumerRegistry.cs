using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

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

/// <summary>
/// A consumer with everything resolved: its queue and how it is created and read, the types it takes, its concurrency
/// and timeout.
/// </summary>
internal sealed record ConsumerRegistration(
    Type ConsumerType,
    string QueueName,
    IReadOnlyList<Type> MessageTypes,
    int Concurrency,
    TimeSpan Timeout)
{
    public PgmqReadMode ReadMode { get; init; }

    public PgmqQueueOptions Queue { get; init; } = PgmqQueueOptions.Standard;

    public bool AtMostOnce { get; init; }

    /// <summary>The JSON object a message must contain to be read, or null to read everything.</summary>
    public string? Filter { get; init; }

    public IReadOnlyList<string> TopicBindings { get; init; } = [];

    public bool UseInsertNotifications { get; init; }

    public bool IsGrouped => ReadMode != PgmqReadMode.Standard;
}

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

        PgmqReadMode readMode = descriptor.Options.ReadMode ?? attribute?.ReadMode ?? PgmqReadMode.Standard;
        bool atMostOnce = descriptor.Options.AtMostOnce ?? attribute?.AtMostOnce ?? false;
        string? filter = descriptor.Options.Filter ?? attribute?.Filter;

        if (atMostOnce && readMode != PgmqReadMode.Standard)
        {
            throw new InvalidOperationException(
                $"{type.Name} cannot be at-most-once and read in {readMode} mode: pgmq.pop has no grouped form.");
        }

        if (filter is not null)
        {
            if (readMode != PgmqReadMode.Standard || atMostOnce)
            {
                throw new InvalidOperationException(
                    $"{type.Name} has a filter, which pgmq supports only in the Standard read mode without AtMostOnce.");
            }

            filter = NormalizeFilter(type, filter);
        }

        string[] bindings =
        [
            .. type.GetCustomAttributes<TopicBindingAttribute>().Select(a => a.Pattern)
                .Concat(descriptor.Options.TopicBindings)
                .Distinct(StringComparer.Ordinal),
        ];
        foreach (string pattern in bindings)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(pattern, $"{type.Name} topic binding");
        }

        return new(type, queueName, descriptor.MessageTypes, concurrency, timeout)
        {
            ReadMode = readMode,
            Queue = descriptor.Options.Queue ?? QueueOptionsFrom(attribute),
            AtMostOnce = atMostOnce,
            Filter = filter,
            TopicBindings = bindings,
            UseInsertNotifications = descriptor.Options.UseInsertNotifications ?? options.UseInsertNotifications,
        };
    }

    private static PgmqQueueOptions QueueOptionsFrom(ConsumerOptionsAttribute? attribute)
        => attribute switch
        {
            null or { QueueType: PgmqQueueType.Standard } => PgmqQueueOptions.Standard,
            { QueueType: PgmqQueueType.Unlogged } => PgmqQueueOptions.Unlogged,
            _ => PgmqQueueOptions.Partitioned(
                attribute.PartitionInterval ?? PgmqQueueOptions.Standard.PartitionInterval,
                attribute.RetentionInterval ?? PgmqQueueOptions.Standard.RetentionInterval),
        };

    /// <summary>Checks that a filter is a JSON object.</summary>
    private static string NormalizeFilter(Type consumerType, string filter)
    {
        try
        {
            using JsonDocument document = JsonDocument.Parse(filter);
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                throw new InvalidOperationException($"The filter of {consumerType.Name} is not a JSON object.");
            }

            return document.RootElement.GetRawText();
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"The filter of {consumerType.Name} is not valid JSON.", ex);
        }
    }
}
