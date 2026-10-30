using System.Reflection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgmq;
using Pgmq;
using Pgmq.Hosting;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Registers Pgmq on the .NET Generic Host.</summary>
public static class PgmqServiceCollectionExtensions
{
    /// <summary>
    /// Adds the publisher (<see cref="IPgmqPublisher"/>), the queue manager (<see cref="IPgmqQueueManager"/>), the
    /// queue setup that runs at start, <see cref="PgmqStatus"/>, and, unless <see cref="PgmqOptions.RunConsumers"/> is
    /// off, the worker that runs the consumers registered with <see cref="AddPgmqConsumer{TConsumer}"/>. The pgmq
    /// client underneath (<see cref="INpgmqClient"/>) resolves too, for direct access to a queue.
    /// <code>
    /// services.AddPgmq(options => options.ConnectionString = connectionString);
    /// services.AddPgmqConsumer&lt;OrderCreatedHandler&gt;();
    /// </code>
    /// </summary>
    public static IServiceCollection AddPgmq(this IServiceCollection services, Action<PgmqOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        PgmqOptions options = new();
        configure(options);
        options.Validate();

        services.AddSingleton(options);

        // One pool for everything Pgmq does. A data source Pgmq builds is its own and disposed with the container;
        // one handed in belongs to the application.
        services.TryAddSingleton(_ => new PgmqDatabase(options.DataSource, options.ConnectionString));
        services.TryAddSingleton<INpgmqClient>(sp => new NpgmqClient(sp.GetRequiredService<PgmqDatabase>().DataSource));
        services.TryAddSingleton(sp => new PgmqQueueManager(sp.GetRequiredService<PgmqDatabase>().DataSource));
        services.TryAddSingleton<IPgmqQueueManager>(sp => sp.GetRequiredService<PgmqQueueManager>());
        services.TryAddSingleton(sp => new QueueReader(sp.GetRequiredService<PgmqDatabase>().DataSource, options));
        services.TryAddSingleton(sp => new InsertListener(sp.GetRequiredService<PgmqDatabase>().DataSource,
            sp.GetRequiredService<ILogger<InsertListener>>()));
        services.TryAddSingleton(sp => new ConsumerRegistry(sp.GetServices<ConsumerDescriptor>(), options));
        services.TryAddSingleton(sp => sp.GetRequiredService<ConsumerRegistry>().Topology);
        services.TryAddSingleton<PgmqStatus>();
        services.TryAddSingleton<PgmqPublisher>();
        services.TryAddSingleton<IPgmqPublisher>(sp => sp.GetRequiredService<PgmqPublisher>());

        // Setup first: hosted services start in order, so the worker knows whether Pgmq is there.
        services.AddHostedService<QueueSetup>();

        if (options.RunConsumers)
        {
            services.TryAddSingleton<ConsumerWorker>();
            services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<ConsumerWorker>());
        }

        return services;
    }

    /// <summary>
    /// Registers <typeparamref name="TConsumer"/> and gives it its own queue, subscribed to every message type it
    /// implements <see cref="IPgmqConsumer{TMessage}"/> for. It is resolved from a new scope for each message.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Settings that win over the consumer's attributes and the defaults.</param>
    public static IServiceCollection AddPgmqConsumer<TConsumer>(this IServiceCollection services,
        Action<PgmqConsumerOptions>? configure = null) where TConsumer : class
        => services.AddPgmqConsumer(typeof(TConsumer), configure);

    /// <summary>
    /// Registers <paramref name="consumerType"/>; see <see cref="AddPgmqConsumer{TConsumer}"/>.
    /// </summary>
    /// <exception cref="ArgumentException">The type does not implement <see cref="IPgmqConsumer{TMessage}"/>.</exception>
    public static IServiceCollection AddPgmqConsumer(this IServiceCollection services, Type consumerType,
        Action<PgmqConsumerOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(consumerType);

        if (consumerType is not { IsClass: true, IsAbstract: false, ContainsGenericParameters: false }
            || ConsumerDescriptor.MessageTypesOf(consumerType).Count == 0)
        {
            throw new ArgumentException(
                $"{consumerType.FullName} is not a concrete class implementing {typeof(IPgmqConsumer<>).Name}.",
                nameof(consumerType));
        }

        PgmqConsumerOptions consumerOptions = new();
        configure?.Invoke(consumerOptions);

        services.TryAddScoped(consumerType);
        services.AddSingleton(new ConsumerDescriptor(consumerType, consumerOptions));

        return services;
    }

    /// <summary>Registers every concrete <see cref="IPgmqConsumer{TMessage}"/> in <paramref name="assembly"/>.</summary>
    public static IServiceCollection AddPgmqConsumers(this IServiceCollection services, Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);

        foreach (Type type in assembly.GetTypes()
                     .Where(t => t is { IsClass: true, IsAbstract: false, ContainsGenericParameters: false }
                                 && ConsumerDescriptor.MessageTypesOf(t).Count > 0)
                     .OrderBy(t => t.FullName, StringComparer.Ordinal))
        {
            services.AddPgmqConsumer(type);
        }

        return services;
    }

    /// <summary>
    /// Adds a health check that reports Pgmq: not set up, or a queue no longer reading. The statuses come from
    /// <see cref="PgmqOptions.UnavailableHealthStatus"/> and <see cref="PgmqOptions.StalledHealthStatus"/>.
    /// </summary>
    public static IHealthChecksBuilder AddPgmq(this IHealthChecksBuilder builder, string name = "pgmq",
        IEnumerable<string>? tags = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddCheck<PgmqHealthCheck>(name, failureStatus: null, tags ?? []);
    }
}
