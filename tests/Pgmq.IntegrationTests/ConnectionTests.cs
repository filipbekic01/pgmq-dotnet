using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Pgmq.Benchmarks;
using Pgmq.Hosting;

namespace Pgmq.IntegrationTests;

/// <summary>
///     How many connections a host holds, measured in <c>pg_stat_activity</c>: an idle host holds two (the listener
///     and the sweeper's) whatever the number of queues, and a busy one at most
///     <see cref="PgmqOptions.MaxConcurrentReads"/> plus the settles in flight plus those two. Each test's host has its
///     own application name, so no other connection is counted.
/// </summary>
[Trait("Category", "Connections")]
public sealed class ConnectionTests(PgmqFixture db) : IClassFixture<PgmqFixture>
{
    private const int Queues = 200;
    private const int MaxConcurrentReads = 4;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task An_idle_host_Should_hold_at_most_three_connections_whatever_the_number_of_queues()
    {
        using TelemetryCapture telemetry = new();
        await using Host host = await Host.StartAsync(db, Queues, messages: 0, o =>
        {
            // Sweep often, so a minute holds hundreds of sweeps.
            o.MinIdleDelay = TimeSpan.FromMilliseconds(50);
            o.MaxIdleDelay = TimeSpan.FromMilliseconds(200);
            o.StallAfter = TimeSpan.FromSeconds(2);
        });

        await Task.Delay(TimeSpan.FromSeconds(60), Ct);

        Assert.InRange(host.Peak, 1, 3);
        Assert.Equal(0, host.QueueNames.Sum(q => telemetry.Sum("pgmq.consumer.reads", q)));
        Assert.Empty(host.Status.StalledQueues);
    }

    [Fact]
    public async Task Starting_with_a_backlog_on_a_few_queues_Should_read_only_those()
    {
        const int Busy = 10;
        using TelemetryCapture telemetry = new();
        await using Host host = await Host.StartAsync(db, Queues, messages: Busy, o =>
        {
            o.MinIdleDelay = TimeSpan.FromMilliseconds(50);
            o.MaxIdleDelay = TimeSpan.FromMilliseconds(200);
        }, before: async (h, ct) =>
        {
            // One message on each of the first ten queues, before the worker starts.
            for (int i = 0; i < Busy; i++)
            {
                LoadMessage message = (LoadMessage)Activator.CreateInstance(h.MessageTypes[i])!;
                message.Id = i;
                await h.Publisher.PublishAsync(message, h.MessageTypes[i], ct);
            }
        });

        await host.Tracker.AllSeen.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        await Task.Delay(TimeSpan.FromSeconds(1), Ct);

        // Reads take turns on MaxConcurrentReads slots; the ten settles may overlap them.
        Assert.InRange(host.Peak, 1, MaxConcurrentReads + Busy + 2);
        Assert.All(host.QueueNames.Skip(Busy), q => Assert.Equal(0, telemetry.Sum("pgmq.consumer.reads", q)));
        Assert.All(host.QueueNames.Take(Busy), q => Assert.True(telemetry.Sum("pgmq.consumer.reads", q) > 0));
    }

    [Fact]
    public async Task A_lost_listener_Should_cost_no_burst_and_leave_messages_to_the_fast_sweep()
    {
        await using Host host = await Host.StartAsync(db, Queues, messages: 1, o =>
        {
            o.MinIdleDelay = TimeSpan.FromMilliseconds(200);
            o.MaxIdleDelay = TimeSpan.FromSeconds(5);
        });

        await Task.Delay(TimeSpan.FromSeconds(2), Ct);
        Assert.Equal(1L, await host.KillListenerAsync());

        // The listener reconnects after 5 seconds; this message arrives while it is down.
        await Task.Delay(TimeSpan.FromMilliseconds(300), Ct);
        LoadMessage message = (LoadMessage)Activator.CreateInstance(host.MessageTypes[42])!;
        Stopwatch clock = Stopwatch.StartNew();
        await host.Publisher.PublishAsync(message, host.MessageTypes[42], Ct);

        await host.Tracker.AllSeen.WaitAsync(TimeSpan.FromSeconds(1.5), Ct);
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(1.5), $"took {clock.Elapsed}");

        // Through the reconnect and its sweep.
        await Eventually.HoldsAsync(async () => await host.ListenersAsync() == 1, TimeSpan.FromSeconds(10),
            "the listener reconnected");
        await Task.Delay(TimeSpan.FromSeconds(1), Ct);

        Assert.InRange(host.Peak, 1, 3);
    }

    [Fact]
    public async Task A_delayed_message_on_an_idle_queue_Should_be_consumed_within_its_delay_and_one_sweep()
    {
        TimeSpan delay = TimeSpan.FromSeconds(3);
        TimeSpan sweep = TimeSpan.FromSeconds(2);
        await using Host host = await Host.StartAsync(db, Queues, messages: 1, o =>
        {
            o.MinIdleDelay = TimeSpan.FromMilliseconds(200);
            o.MaxIdleDelay = sweep;
        });

        await Eventually.HoldsAsync(async () => await host.ListenersAsync() == 1, TimeSpan.FromSeconds(10),
            "the listener is up");

        LoadMessage message = (LoadMessage)Activator.CreateInstance(host.MessageTypes[7])!;
        Stopwatch clock = Stopwatch.StartNew();
        await host.Publisher.PublishAsync(message, host.MessageTypes[7], new PgmqPublishOptions { Delay = delay },
            Ct);

        await host.Tracker.AllSeen.WaitAsync(delay + sweep + TimeSpan.FromSeconds(2), Ct);
        Assert.InRange(clock.Elapsed, delay, delay + sweep + TimeSpan.FromSeconds(0.5));
    }

    [Fact]
    public async Task A_busy_host_Should_read_through_at_most_MaxConcurrentReads_connections()
    {
        const int PerQueue = 50;
        await using Host host = await Host.StartAsync(db, Queues, messages: Queues * PerQueue, before: (h, ct) =>
            Parallel.ForEachAsync(Enumerable.Range(0, Queues * PerQueue),
                new ParallelOptions { MaxDegreeOfParallelism = 16, CancellationToken = ct },
                async (id, token) =>
                {
                    Type type = h.MessageTypes[id % Queues];
                    LoadMessage message = (LoadMessage)Activator.CreateInstance(type)!;
                    message.Id = id;
                    await h.Publisher.PublishAsync(message, type, token);
                }));

        await host.Tracker.AllSeen.WaitAsync(TimeSpan.FromMinutes(2), Ct);

        // Concurrency 1 on every queue: at most one settle per queue at a time.
        Assert.InRange(host.Peak, 1, MaxConcurrentReads + Queues + 2);
        Assert.Equal(0, host.Tracker.Duplicates);
    }

    [Fact]
    public async Task An_idle_queue_Should_never_be_reported_as_stalled()
    {
        await using Host host = await Host.StartAsync(db, 5, messages: 0, o =>
        {
            o.MaxIdleDelay = TimeSpan.FromSeconds(30);
            o.StallAfter = TimeSpan.FromSeconds(1);
        });

        for (int i = 0; i < 10; i++)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(500), Ct);
            Assert.Empty(host.Status.StalledQueues);
        }
    }

    /// <summary>
    ///     A host over the first <c>queues</c> benchmark consumers, each with its own short queue name, whose
    ///     connections carry their own application name, and whose peak connection count is sampled from the moment
    ///     its worker starts.
    /// </summary>
    private sealed class Host : IAsyncDisposable
    {
        private readonly ServiceProvider _services;
        private readonly string _connectionString;
        private readonly string _application;
        private readonly CancellationTokenSource _stop = new();
        private Task? _sampling;
        private int _peak;

        private Host(ServiceProvider services, string connectionString, string application, Type[] messageTypes,
            LoadTracker tracker)
        {
            _services = services;
            _connectionString = connectionString;
            _application = application;
            MessageTypes = messageTypes;
            Tracker = tracker;
        }

        public Type[] MessageTypes { get; }
        public LoadTracker Tracker { get; }
        public PgmqPublisher Publisher => _services.GetRequiredService<PgmqPublisher>();
        public PgmqStatus Status => _services.GetRequiredService<PgmqStatus>();
        public IReadOnlyList<string> QueueNames => [.. Status.Queues.Order(StringComparer.Ordinal)];

        /// <summary>The most connections this host held at once since its worker started.</summary>
        public int Peak => Volatile.Read(ref _peak);

        public static async Task<Host> StartAsync(PgmqFixture db, int queues, int messages,
            Action<PgmqOptions>? configure = null, Func<Host, CancellationToken, Task>? before = null)
        {
            string application = $"pgmq_conn_{Guid.NewGuid():N}"[..30];
            string connectionString = new NpgsqlConnectionStringBuilder(db.ConnectionString)
            {
                ApplicationName = application,
            }.ConnectionString;

            Type[] messageTypes = LoadScenario.MessageTypes[..queues];
            LoadTracker tracker = new(messages);

            ServiceCollection collection = new();
            collection.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
            collection.AddSingleton(tracker);
            collection.AddPgmq(o =>
            {
                o.ConnectionString = connectionString;
                o.MaxConcurrentReads = MaxConcurrentReads;
                o.QueueNameFormatter = t => "c_" + t.GetGenericArguments()[0].Name.ToLowerInvariant();
                o.InsertNotificationThrottle = TimeSpan.Zero;
                o.Lease = TimeSpan.FromSeconds(5);
                o.RenewEvery = TimeSpan.FromSeconds(1);
                o.DrainGrace = TimeSpan.FromMilliseconds(500);
                configure?.Invoke(o);
            });

            foreach (Type messageType in messageTypes)
            {
                collection.AddPgmqConsumer(typeof(LoadConsumer<>).MakeGenericType(messageType));
            }

            ServiceProvider services = collection.BuildServiceProvider(validateScopes: true);
            Host host = new(services, connectionString, application, messageTypes, tracker);

            await services.GetServices<IHostedService>().OfType<QueueSetup>().Single().StartAsync(Ct);
            host.Status.EnsureReady();

            foreach (string queue in host.QueueNames)
            {
                await db.ResetAsync(queue);
            }

            if (before is not null)
            {
                await before(host, Ct);
            }

            host._sampling = host.SampleAsync();
            await services.GetRequiredService<ConsumerWorker>().StartAsync(Ct);
            return host;
        }

        /// <summary>Ends this host's LISTEN connection, as a network failure would; how many it ended.</summary>
        public async Task<long> KillListenerAsync()
            => (long)(await ScalarAsync(
                "select count(pg_terminate_backend(pid)) from pg_stat_activity " +
                "where application_name = $1 and query ilike 'LISTEN %'"))!;

        /// <summary>This host's connections listening right now.</summary>
        public async Task<long> ListenersAsync()
            => (long)(await ScalarAsync(
                "select count(*) from pg_stat_activity where application_name = $1 and query ilike 'LISTEN %'"))!;

        public async ValueTask DisposeAsync()
        {
            TestContext.Current.TestOutputHelper?.WriteLine($"Peak connections: {Peak}");
            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(30));
            await _services.GetRequiredService<ConsumerWorker>().StopAsync(timeout.Token);
            await _stop.CancelAsync();
            if (_sampling is not null)
            {
                await _sampling;
            }

            await _services.DisposeAsync();
            _stop.Dispose();
        }

        private async Task SampleAsync()
        {
            // The sampler has an application name of its own, so it never counts itself.
            await using NpgsqlConnection connection = new(new NpgsqlConnectionStringBuilder(_connectionString)
            {
                ApplicationName = "pgmq_conn_sampler",
                Pooling = false,
            }.ConnectionString);
            await connection.OpenAsync();
            await using NpgsqlCommand count = new(
                "select count(*) from pg_stat_activity where application_name = $1", connection);
            count.Parameters.AddWithValue(_application);

            while (!_stop.IsCancellationRequested)
            {
                int now = (int)(long)(await count.ExecuteScalarAsync())!;
                if (now > Volatile.Read(ref _peak))
                {
                    Volatile.Write(ref _peak, now);
                }

                try
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(10), _stop.Token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }

        private async Task<object?> ScalarAsync(string sql)
        {
            await using NpgsqlConnection connection = new(new NpgsqlConnectionStringBuilder(_connectionString)
            {
                ApplicationName = "pgmq_conn_probe",
                Pooling = false,
            }.ConnectionString);
            await connection.OpenAsync();
            await using NpgsqlCommand command = new(sql, connection);
            command.Parameters.AddWithValue(_application);
            return await command.ExecuteScalarAsync();
        }
    }
}
