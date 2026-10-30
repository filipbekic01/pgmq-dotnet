using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Pgmq.Hosting;

namespace Pgmq.Benchmarks;

public enum LoadMode
{
    /// <summary>Every message is on its queue before the worker starts: how fast the worker empties full queues.</summary>
    Drain,

    /// <summary>
    /// The worker runs while messages are published one by one, as the app does: publish speed, end-to-end speed,
    /// and how long a message waits.
    /// </summary>
    Live,
}

/// <summary>What one run measured, and what went wrong in it. Every count but the times should be zero.</summary>
public sealed record LoadResult(
    LoadMode Mode,
    int Messages,
    int Queues,
    TimeSpan Publish,
    TimeSpan Total,
    TimeSpan LatencyP50,
    TimeSpan LatencyP95,
    TimeSpan LatencyP99,
    int Lost,
    int Duplicates,
    long LeftOnQueues,
    long Archived,
    int WarningsLogged,
    int ErrorsLogged)
{
    public double PublishPerSecond => Messages / Publish.TotalSeconds;

    /// <summary>Drain: how fast the worker emptied the queues. Live: first publish to last message handled.</summary>
    public double ConsumePerSecond => Messages / Total.TotalSeconds;

    public bool Clean => Lost == 0 && Duplicates == 0 && LeftOnQueues == 0 && Archived == 0 && ErrorsLogged == 0;
}

/// <summary>
/// Pushes <c>messages</c> through <c>queues</c> queues whose consumers do nothing, with the real publisher and worker
/// and default settings, against the PGMQ database at <c>connectionString</c>. Used by the benchmark for speed and by
/// the load tests for correctness; both run the same code.
/// </summary>
public sealed class LoadScenario(string connectionString, int messages, int queues, Action<PgmqOptions>? configure = null)
{
    public static readonly Type[] MessageTypes =
    [
        .. typeof(LoadMessage).Assembly.GetTypes()
            .Where(t => t is { IsAbstract: false } && t.IsSubclassOf(typeof(LoadMessage)))
            .OrderBy(t => t.Name, StringComparer.Ordinal)
    ];

    private static readonly MethodInfo PublishBatchMethod =
        typeof(LoadScenario).GetMethod(nameof(PublishBatchAsync), BindingFlags.NonPublic | BindingFlags.Static)!;

    /// <summary>How long a run may take before it is called stuck.</summary>
    public TimeSpan GiveUpAfter { get; init; } = TimeSpan.FromMinutes(10);

    /// <summary>Parallel publishers in <see cref="LoadMode.Live"/>, standing in for concurrent requests.</summary>
    public int Publishers { get; init; } = 64;

    /// <summary>Workers reading the same queues, as several pods do. Each gets its own services and pool.</summary>
    public int Instances { get; init; } = 1;

    public async Task<LoadResult> RunAsync(LoadMode mode, CancellationToken cancellationToken = default)
    {
        if (queues < 1 || queues > MessageTypes.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(queues), $"1 to {MessageTypes.Length} queues.");
        }

        Type[] messageTypes = MessageTypes[..queues];
        LoadTracker tracker = new(messages);
        LogCounter logs = new();

        ServiceProvider[] instances =
            [.. Enumerable.Range(0, Math.Max(1, Instances)).Select(_ => BuildServices(messageTypes, tracker, logs))];
        try
        {
            return await RunAsync(mode, instances, messageTypes, tracker, logs, cancellationToken);
        }
        finally
        {
            foreach (ServiceProvider instance in instances)
            {
                await instance.DisposeAsync();
            }
        }
    }

    private async Task<LoadResult> RunAsync(LoadMode mode, ServiceProvider[] instances, Type[] messageTypes,
        LoadTracker tracker, LogCounter logs, CancellationToken cancellationToken)
    {
        ServiceProvider services = instances[0];
        ConsumerRegistry registry = services.GetRequiredService<ConsumerRegistry>();
        PgmqPublisher bus = services.GetRequiredService<PgmqPublisher>();
        ConsumerWorker[] workers = [.. instances.Select(i => i.GetRequiredService<ConsumerWorker>())];

        // Every instance sets up as a host would; the calls are idempotent.
        foreach (ServiceProvider instance in instances)
        {
            await instance.GetServices<IHostedService>().OfType<QueueSetup>().Single().StartAsync(cancellationToken);
            instance.GetRequiredService<PgmqStatus>().EnsureReady();
        }

        await ResetAsync(registry);

        LoadMessage[][] perQueue = Split(messageTypes);
        Stopwatch publish = new();
        Stopwatch total = new();

        if (mode == LoadMode.Drain)
        {
            publish.Start();
            await Parallel.ForEachAsync(Enumerable.Range(0, queues),
                new ParallelOptions { MaxDegreeOfParallelism = 16, CancellationToken = cancellationToken },
                async (q, ct) => await PublishBatchesAsync(bus, messageTypes[q], perQueue[q], ct));
            publish.Stop();

            total.Start();
            await StartAsync(workers, cancellationToken);
        }
        else
        {
            await StartAsync(workers, cancellationToken);

            // Interleave the queues, so publishing does not fill one queue before the next.
            LoadMessage[] all = [.. Interleave(perQueue)];
            total.Start();
            publish.Start();
            await Parallel.ForEachAsync(all,
                new ParallelOptions { MaxDegreeOfParallelism = Publishers, CancellationToken = cancellationToken },
                async (message, ct) =>
                {
                    Stamp(message);
                    await bus.PublishAsync(message, message.GetType(), ct);
                });
            publish.Stop();
        }

        try
        {
            await tracker.AllSeen.WaitAsync(GiveUpAfter, cancellationToken);
        }
        catch (TimeoutException)
        {
            // Reported through Lost below, not thrown: the result says how far it got.
        }

        total.Stop();

        // The last deletes land just after the last consumer returns; give them a moment before counting.
        long left = await WaitForEmptyQueuesAsync(registry, TimeSpan.FromSeconds(30));

        using CancellationTokenSource stopTimeout = new(TimeSpan.FromSeconds(30));
        await Task.WhenAll(workers.Select(w => w.StopAsync(stopTimeout.Token)));

        long archived = await SumAsync(registry, "a");

        return new(mode, messages, queues, publish.Elapsed, total.Elapsed,
            tracker.Latency(50), tracker.Latency(95), tracker.Latency(99),
            tracker.Lost, tracker.Duplicates, left, archived, logs.Warnings, logs.Errors);
    }

    private static async Task StartAsync(ConsumerWorker[] workers, CancellationToken cancellationToken)
    {
        foreach (ConsumerWorker worker in workers)
        {
            await worker.StartAsync(cancellationToken);
        }
    }

    private ServiceProvider BuildServices(Type[] messageTypes, LoadTracker tracker, LogCounter logs)
    {
        ServiceCollection services = new();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning).AddProvider(logs));
        services.AddSingleton(tracker);
        services.AddPgmq(options =>
        {
            options.ConnectionString = connectionString;
            configure?.Invoke(options);
        });

        foreach (Type messageType in messageTypes)
        {
            services.AddPgmqConsumer(typeof(LoadConsumer<>).MakeGenericType(messageType));
        }

        return services.BuildServiceProvider(validateScopes: true);
    }

    /// <summary>The ids 0..messages-1, split evenly over the queues, each as its queue's message type.</summary>
    private LoadMessage[][] Split(Type[] messageTypes)
    {
        LoadMessage[][] perQueue = new LoadMessage[messageTypes.Length][];
        int baseCount = messages / messageTypes.Length;
        int extra = messages % messageTypes.Length;
        long id = 0;

        for (int q = 0; q < messageTypes.Length; q++)
        {
            int count = baseCount + (q < extra ? 1 : 0);
            perQueue[q] = new LoadMessage[count];
            for (int i = 0; i < count; i++)
            {
                LoadMessage message = (LoadMessage)Activator.CreateInstance(messageTypes[q])!;
                message.Id = id++;
                perQueue[q][i] = message;
            }
        }

        return perQueue;
    }

    private static IEnumerable<LoadMessage> Interleave(LoadMessage[][] perQueue)
    {
        int longest = perQueue.Max(q => q.Length);
        for (int i = 0; i < longest; i++)
        {
            foreach (LoadMessage[] queue in perQueue)
            {
                if (i < queue.Length)
                {
                    yield return queue[i];
                }
            }
        }
    }

    private static async Task PublishBatchesAsync(PgmqPublisher bus, Type messageType, LoadMessage[] messages,
        CancellationToken cancellationToken)
    {
        foreach (LoadMessage[] chunk in messages.Chunk(1000))
        {
            foreach (LoadMessage message in chunk)
            {
                Stamp(message);
            }

            await (Task)PublishBatchMethod.MakeGenericMethod(messageType).Invoke(null, [bus, chunk, cancellationToken])!;
        }
    }

    private static Task PublishBatchAsync<T>(PgmqPublisher bus, LoadMessage[] chunk,
        CancellationToken cancellationToken) where T : LoadMessage
        => bus.PublishBatchAsync(chunk.Cast<T>(), cancellationToken);

    private static void Stamp(LoadMessage message) => message.SentAt = Stopwatch.GetTimestamp();

    private async Task ResetAsync(ConsumerRegistry registry)
    {
        await using NpgsqlConnection connection = new(connectionString);
        await connection.OpenAsync();
        foreach (ConsumerRegistration consumer in registry.Consumers)
        {
            await using NpgsqlCommand command = new(
                $"delete from pgmq.q_{consumer.QueueName}; delete from pgmq.a_{consumer.QueueName};", connection);
            await command.ExecuteNonQueryAsync();
        }
    }

    private async Task<long> WaitForEmptyQueuesAsync(ConsumerRegistry registry, TimeSpan within)
    {
        Stopwatch clock = Stopwatch.StartNew();
        long left;
        while ((left = await SumAsync(registry, "q")) > 0 && clock.Elapsed < within)
        {
            await Task.Delay(100);
        }

        return left;
    }

    /// <summary>Rows across every queue table (<c>q</c>) or archive table (<c>a</c>).</summary>
    private async Task<long> SumAsync(ConsumerRegistry registry, string prefix)
    {
        string sql = string.Join(" + ", registry.Consumers.Select(c =>
            $"(select count(*) from pgmq.{prefix}_{c.QueueName})"));

        await using NpgsqlConnection connection = new(connectionString);
        await connection.OpenAsync();
        await using NpgsqlCommand command = new($"select {sql}", connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync());
    }

    /// <summary>Counts warnings and errors Pgmq logs, so a run that limps along is not called fast.</summary>
    private sealed class LogCounter : ILoggerProvider
    {
        private int _warnings;
        private int _errors;

        public int Warnings => Volatile.Read(ref _warnings);
        public int Errors => Volatile.Read(ref _errors);

        public ConcurrentQueue<string> FirstErrors { get; } = new();

        public ILogger CreateLogger(string categoryName) => new Counter(this);

        public void Dispose()
        {
        }

        private sealed class Counter(LogCounter owner) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                if (logLevel >= LogLevel.Error)
                {
                    Interlocked.Increment(ref owner._errors);
                    if (owner.FirstErrors.Count < 5)
                    {
                        owner.FirstErrors.Enqueue($"{formatter(state, exception)} {exception?.GetType().Name}: {exception?.Message}");
                        Console.Error.WriteLine($"[load error] {formatter(state, exception)} {exception?.Message}");
                    }
                }
                else if (logLevel == LogLevel.Warning)
                {
                    Interlocked.Increment(ref owner._warnings);
                }
            }
        }
    }
}
