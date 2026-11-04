using System.Collections.Concurrent;

namespace Pgmq.IntegrationTests;

/// <summary>
///     Every pgmq read function the worker can call, against real PGMQ: the plain, grouped, round-robin and head reads,
///     and the conditional read, each woken by insert notifications and by the sweeper alone. Each case checks that what
///     comes back is handled as that read promises; which function the worker calls is checked in
///     <c>Pgmq.Hosting.Tests</c>. Needs a running Docker daemon.
/// </summary>
public sealed class ReadModeTests(PgmqFixture db) : IClassFixture<PgmqFixture>
{
    private static readonly TimeSpan Within = TimeSpan.FromSeconds(30);

    private readonly Probe _probe = new();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(PgmqReadMode.Grouped, true)]
    [InlineData(PgmqReadMode.Grouped, false)]
    [InlineData(PgmqReadMode.GroupedRoundRobin, true)]
    [InlineData(PgmqReadMode.GroupedRoundRobin, false)]
    [InlineData(PgmqReadMode.GroupedHead, true)]
    [InlineData(PgmqReadMode.GroupedHead, false)]
    public async Task A_grouped_queue_Should_handle_each_group_in_order_and_one_at_a_time(PgmqReadMode mode,
        bool notifications)
    {
        string queue = $"t_fifo_{mode.ToString().ToLowerInvariant()}_{(notifications ? "n" : "s")}";
        FifoConsumer.Reset();
        await using TransportHarness harness = await StartAsync(typeof(FifoConsumer), queue, notifications, c =>
        {
            c.ReadMode = mode;
            c.Concurrency = 6;
        });

        // Setup indexes the group header for every grouped queue.
        Assert.Equal(1L, await db.ScalarObjectAsync(
            $"select count(*) from pg_indexes where schemaname = 'pgmq' and indexname = 'q_{queue}_fifo_idx'"));

        for (int seq = 1; seq <= 5; seq++)
        {
            foreach (string group in new[] { "a", "b", "c" })
            {
                // b3 fails: it is archived, and the rest of b still comes after it, in order.
                await harness.Bus.PublishAsync(new FifoMessage(group, seq, Fail: group == "b" && seq == 3),
                    new PgmqPublishOptions { GroupId = group }, Ct);
            }
        }

        await Eventually.HoldsAsync(() => FifoConsumer.Done.Count == 14, Within, "every other message handled");
        await Eventually.HoldsAsync(async () => await db.ArchivedAsync(queue) == 1, Within, "b3 archived");

        foreach (string group in new[] { "a", "c" })
        {
            Assert.Equal([1, 2, 3, 4, 5], FifoConsumer.Done.Where(d => d.Group == group).Select(d => d.Seq));
        }

        Assert.Equal([1, 2, 4, 5], FifoConsumer.Done.Where(d => d.Group == "b").Select(d => d.Seq));
        Assert.Equal([1, 2, 3, 4, 5],
            _probe.Deliveries.Select(d => (FifoMessage)d.Message).Where(m => m.Group == "b").Select(m => m.Seq));

        Assert.Equal(0, FifoConsumer.Overlaps);
        Assert.True(FifoConsumer.MaxGroupsAtOnce > 1, "groups should run in parallel");
        Assert.All(_probe.Deliveries, d => Assert.Equal(((FifoMessage)d.Message).Group, d.GroupId));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_filtered_consumer_Should_read_only_the_messages_that_match(bool notifications)
    {
        string queue = $"t_filtered_{(notifications ? "notified" : "swept")}";
        await using TransportHarness harness = await StartAsync(typeof(FilteredConsumer), queue, notifications);

        await harness.Bus.PublishAsync(new FilteredMessage("us", 1), Ct);
        await harness.Bus.PublishAsync(new FilteredMessage("eu", 2), Ct);

        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 1, Within, "the match handled");
        await Task.Delay(TimeSpan.FromSeconds(1), Ct);
        Assert.Equal(2, Assert.IsType<FilteredMessage>(Assert.Single(_probe.Deliveries).Message).Id);
        Assert.Equal(1, await db.CountAsync(queue));
    }

    /// <summary>
    ///     Starts <paramref name="consumer"/> on <paramref name="queue"/>, woken by insert notifications, or by the
    ///     sweeper alone.
    /// </summary>
    private Task<TransportHarness> StartAsync(Type consumer, string queue, bool notifications = true,
        Action<PgmqConsumerOptions>? configure = null)
        => TransportHarness.StartAsync(db, _probe, [consumer],
            o => o.UseInsertNotifications = notifications,
            configureConsumers: c =>
            {
                c.QueueName = queue;
                configure?.Invoke(c);
            });

    public sealed record FifoMessage(string Group, int Seq, bool Fail);

    public sealed class FifoConsumer(Probe probe) : IPgmqConsumer<FifoMessage>
    {
        private static readonly ConcurrentDictionary<string, int> Running = new();
        private static int _groupsRunning;
        private static int _maxGroupsAtOnce;
        private static int _overlaps;

        public static ConcurrentQueue<(string Group, int Seq)> Done { get; private set; } = new();

        public static int Overlaps => Volatile.Read(ref _overlaps);

        public static int MaxGroupsAtOnce => Volatile.Read(ref _maxGroupsAtOnce);

        public static void Reset()
        {
            Running.Clear();
            Done = new();
            _groupsRunning = 0;
            _maxGroupsAtOnce = 0;
            _overlaps = 0;
        }

        public async Task HandleAsync(PgmqConsumeContext<FifoMessage> context, CancellationToken cancellationToken)
        {
            FifoMessage message = context.Message;
            probe.Record(nameof(FifoConsumer), message, context);

            if (Running.AddOrUpdate(message.Group, 1, (_, n) => n + 1) > 1)
            {
                Interlocked.Increment(ref _overlaps);
            }

            int groups = Interlocked.Increment(ref _groupsRunning);
            int seen;
            while (groups > (seen = Volatile.Read(ref _maxGroupsAtOnce))
                   && Interlocked.CompareExchange(ref _maxGroupsAtOnce, groups, seen) != seen)
            {
            }

            try
            {
                await Task.Delay(100, cancellationToken);

                if (message.Fail)
                {
                    throw new InvalidOperationException("failed");
                }

                Done.Enqueue((message.Group, message.Seq));
            }
            finally
            {
                Interlocked.Decrement(ref _groupsRunning);
                Running.AddOrUpdate(message.Group, 0, (_, n) => n - 1);
            }
        }
    }

    public sealed record FilteredMessage(string Region, int Id);

    [ConsumerOptions(Filter = """{"region":"eu"}""")]
    public sealed class FilteredConsumer(Probe probe) : IPgmqConsumer<FilteredMessage>
    {
        public Task HandleAsync(PgmqConsumeContext<FilteredMessage> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(FilteredConsumer), context.Message, context);
            return Task.CompletedTask;
        }
    }
}
