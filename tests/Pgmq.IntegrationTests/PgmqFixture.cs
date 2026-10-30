using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgmq;
using Npgsql;
using Pgmq.Benchmarks;
using Pgmq.Hosting;

namespace Pgmq.IntegrationTests;

/// <summary>A throwaway PostgreSQL with PGMQ in Docker, shared by a test class. Needs a running Docker daemon.</summary>
public sealed class PgmqFixture : IAsyncLifetime
{
    private readonly PgmqContainer _container = new();
    private NpgsqlDataSource? _dataSource;

    public string ConnectionString => _container.ConnectionString;

    public async ValueTask InitializeAsync() => await _container.StartAsync();

    public async ValueTask DisposeAsync()
    {
        if (_dataSource is not null)
        {
            await _dataSource.DisposeAsync();
        }

        await _container.DisposeAsync();
    }

    /// <summary>Empties a queue and its archive, so a test starts from nothing whatever ran before it.</summary>
    public async Task ResetAsync(string queue)
    {
        await using NpgsqlConnection connection = new(ConnectionString);
        await connection.OpenAsync();
        await using NpgsqlCommand command = new($"delete from pgmq.q_{queue}; delete from pgmq.a_{queue};", connection);
        await command.ExecuteNonQueryAsync();
    }

    /// <summary>Messages waiting on <paramref name="queue"/>, visible or not.</summary>
    public Task<long> CountAsync(string queue) => ScalarAsync($"select count(*) from pgmq.q_{queue}");

    /// <summary>A queue manager on this database.</summary>
    public PgmqQueueManager Queues => new(_dataSource ??= NpgsqlDataSource.Create(ConnectionString));

    /// <summary>Runs <paramref name="sql"/> and returns its first value.</summary>
    public async Task<object?> ScalarObjectAsync(string sql)
    {
        await using NpgsqlConnection connection = new(ConnectionString);
        await connection.OpenAsync();
        await using NpgsqlCommand command = new(sql, connection);
        return await command.ExecuteScalarAsync();
    }

    /// <summary>Messages archived from <paramref name="queue"/>.</summary>
    public Task<long> ArchivedAsync(string queue) => ScalarAsync($"select count(*) from pgmq.a_{queue}");

    private async Task<long> ScalarAsync(string sql)
    {
        await using NpgsqlConnection connection = new(ConnectionString);
        await connection.OpenAsync();
        await using NpgsqlCommand command = new(sql, connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }
}

/// <summary>
///     The real publisher and worker over a chosen set of consumers, registered as an application would, with times
///     shortened so a test runs in seconds. Each harness is one instance; two over the same consumers share the queues.
/// </summary>
public sealed class TransportHarness : IAsyncDisposable
{
    /// <summary>The shortened times, readable by tests that wait on them.</summary>
    public static readonly PgmqOptions Fast = ApplyFast(new PgmqOptions());

    private readonly ServiceProvider _services;
    private bool _stopped;

    private TransportHarness(ServiceProvider services)
    {
        _services = services;
    }

    public IPgmqPublisher Bus => _services.GetRequiredService<IPgmqPublisher>();
    public PgmqStatus Status => _services.GetRequiredService<PgmqStatus>();
    public INpgmqClient Client => _services.GetRequiredService<INpgmqClient>();
    private ConsumerWorker Worker => _services.GetRequiredService<ConsumerWorker>();

    /// <param name="reset">Empty the queues first. False for a second instance joining queues already in use.</param>
    public static async Task<TransportHarness> StartAsync(PgmqFixture db, Probe probe, Type[] consumers,
        Action<PgmqOptions>? configure = null, bool reset = true)
    {
        ServiceCollection collection = new();
        collection.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        collection.AddSingleton(probe);
        collection.AddPgmq(options =>
        {
            options.ConnectionString = db.ConnectionString;
            ApplyFast(options);
            configure?.Invoke(options);
        });

        foreach (Type consumer in consumers)
        {
            collection.AddPgmqConsumer(consumer);
        }

        ServiceProvider services = collection.BuildServiceProvider(validateScopes: true);

        await services.GetServices<IHostedService>().OfType<QueueSetup>().Single().StartAsync(CancellationToken.None);
        services.GetRequiredService<PgmqStatus>().EnsureReady();

        if (reset)
        {
            foreach (string queue in services.GetRequiredService<PgmqStatus>().Queues)
            {
                await db.ResetAsync(queue);
            }
        }

        await services.GetRequiredService<ConsumerWorker>().StartAsync(CancellationToken.None);

        return new(services);
    }

    /// <summary>What a host does on shutdown: stop the worker and wait for its drain.</summary>
    public async Task StopAsync()
    {
        if (_stopped)
        {
            return;
        }

        _stopped = true;
        using CancellationTokenSource hostTimeout = new(TimeSpan.FromSeconds(30));
        await Worker.StopAsync(hostTimeout.Token);
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        await _services.DisposeAsync();
    }

    private static PgmqOptions ApplyFast(PgmqOptions options)
    {
        options.DefaultConcurrency = 4;
        options.DefaultTimeout = TimeSpan.FromSeconds(30);
        options.MaxAttempts = 3;
        options.Lease = TimeSpan.FromSeconds(2);
        options.RenewEvery = TimeSpan.FromMilliseconds(500);
        options.RetryBackoff = TimeSpan.FromSeconds(1);
        options.MaxRetryBackoff = TimeSpan.FromSeconds(1);
        options.AbandonAfter = TimeSpan.FromSeconds(1);
        options.DrainGrace = TimeSpan.FromMilliseconds(500);
        options.MinIdleDelay = TimeSpan.FromMilliseconds(50);
        options.MaxIdleDelay = TimeSpan.FromMilliseconds(200);
        options.StallAfter = TimeSpan.FromSeconds(5);
        return options;
    }
}

/// <summary>What the test consumers saw, and gates a test opens to let them finish.</summary>
public sealed class Probe
{
    private int _running;
    private int _maxRunning;

    public ConcurrentQueue<Delivered> Deliveries { get; } = new();

    public ConcurrentDictionary<string, TaskCompletionSource> Gates { get; } = new();

    public int MaxRunning => Volatile.Read(ref _maxRunning);

    public void Record(string consumer, object message, PgmqConsumeContext context)
        => Deliveries.Enqueue(new(consumer, message, context.RedeliveryCount, context.MessageId,
            Activity.Current?.TraceId.ToString(), context.GroupId, context.Headers));

    public IReadOnlyList<Delivered> Of(string consumer) => [.. Deliveries.Where(d => d.Consumer == consumer)];

    public Task Gate(string name) => Gates.GetOrAdd(name, _ => new(TaskCreationOptions.RunContinuationsAsynchronously)).Task;

    public void Open(string name) => Gates.GetOrAdd(name, _ => new(TaskCreationOptions.RunContinuationsAsynchronously)).TrySetResult();

    /// <summary>Counts a consumer as running for the duration of <paramref name="work"/>.</summary>
    public async Task RunningAsync(Func<Task> work)
    {
        int now = Interlocked.Increment(ref _running);
        int seen;
        while (now > (seen = Volatile.Read(ref _maxRunning)) && Interlocked.CompareExchange(ref _maxRunning, now, seen) != seen)
        {
        }

        try
        {
            await work();
        }
        finally
        {
            Interlocked.Decrement(ref _running);
        }
    }

    public sealed record Delivered(string Consumer, object Message, int RedeliveryCount, Guid? MessageId, string? TraceId,
        string? GroupId = null, IReadOnlyDictionary<string, string>? Headers = null);
}

public static class Eventually
{
    /// <summary>Waits until <paramref name="condition"/> holds, failing the test after <paramref name="within"/>.</summary>
    public static async Task HoldsAsync(Func<Task<bool>> condition, TimeSpan within, string what)
    {
        Stopwatch clock = Stopwatch.StartNew();
        while (!await condition())
        {
            if (clock.Elapsed > within)
            {
                Assert.Fail($"Not within {within}: {what}");
            }

            await Task.Delay(50);
        }
    }

    public static Task HoldsAsync(Func<bool> condition, TimeSpan within, string what)
        => HoldsAsync(() => Task.FromResult(condition()), within, what);
}
