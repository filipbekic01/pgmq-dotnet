using System.Diagnostics;
using Pgmq.Hosting;

namespace Pgmq.IntegrationTests;

/// <summary>
///     The worker when things around it go wrong, against real PGMQ: a consumer that answers shutdown with an
///     exception of its own, a queue that cannot be written for a while, a lease that lapsed, messages that waited
///     behind a failed one, and an insert the notification throttle swallowed. Needs a running Docker daemon.
/// </summary>
public sealed class WorkerHardeningTests(PgmqFixture db) : IClassFixture<PgmqFixture>
{
    private static readonly TimeSpan Within = TimeSpan.FromSeconds(20);

    private readonly Probe _probe = new();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task On_shutdown_a_consumer_that_throws_its_own_exception_Should_go_back_to_the_queue_not_the_archive()
    {
        TransportHarness harness = await StartAsync(typeof(WrappingConsumer));
        await harness.Bus.PublishAsync(new Wrapped(), Ct);
        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 1, Within, "running");

        await harness.StopAsync();

        Assert.Equal(1, (await harness.Queues.GetMetricsAsync("t_wrapping", Ct)).VisibleLength);
        Assert.Equal(0, await db.ArchivedAsync("t_wrapping"));
        await harness.DisposeAsync();
    }

    [Fact]
    public async Task A_queue_that_cannot_be_written_for_a_while_Should_not_run_its_consumer_again()
    {
        await using TransportHarness harness = await StartAsync(typeof(OutageConsumer));
        await harness.Bus.PublishAsync(new Outage(), Ct);
        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 1, Within, "running");

        // The queue is out of reach while the consumer ends, and for longer than several leases: its delete fails,
        // and so does every renewal.
        await db.ScalarObjectAsync("alter table pgmq.q_t_outage rename to q_t_outage_away");
        _probe.Open("outage");
        await Task.Delay(TimeSpan.FromSeconds(8), Ct);
        await db.ScalarObjectAsync("alter table pgmq.q_t_outage_away rename to q_t_outage");

        await Eventually.HoldsAsync(async () => await db.CountAsync("t_outage") == 0, TimeSpan.FromSeconds(40),
            "deleted once the queue was back");
        await Task.Delay(TimeSpan.FromSeconds(1), Ct);
        Assert.Single(_probe.Deliveries);
        Assert.Equal(0, await db.ArchivedAsync("t_outage"));
    }

    [Fact]
    public async Task A_message_read_again_elsewhere_while_its_consumer_runs_Should_be_reported_as_a_lost_lease()
    {
        using TelemetryCapture telemetry = new();
        await using TransportHarness harness = await StartAsync(typeof(LostLeaseConsumer));
        await harness.Bus.PublishAsync(new LostLease(), Ct);
        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 1, Within, "running");

        // What another reader leaves on a message whose lease lapsed: one read more.
        await db.ScalarObjectAsync("update pgmq.q_t_lost_lease set read_ct = read_ct + 1");

        await Eventually.HoldsAsync(() => telemetry.Sum("pgmq.consumer.leases.lost", "t_lost_lease") == 1, Within,
            "reported");

        // Once, however many renewals follow.
        await Task.Delay(TransportHarness.Fast.RenewEvery * 4, Ct);
        Assert.Equal(1, telemetry.Sum("pgmq.consumer.leases.lost", "t_lost_lease"));
        _probe.Open("lost-lease");
    }

    [Fact]
    public async Task Messages_that_waited_behind_a_failed_one_Should_not_have_the_wait_counted_as_an_attempt()
    {
        await using (TransportHarness stopped = await StartAsync(typeof(BatchedGroupConsumer)))
        {
            await stopped.StopAsync();

            // All on the queue before the worker starts, so one read takes the whole group.
            for (int seq = 1; seq <= 4; seq++)
            {
                await stopped.Bus.PublishAsync(new BatchedStep(seq, Poison: seq <= 2),
                    new PgmqPublishOptions { GroupId = "g" }, Ct);
            }
        }

        await using TransportHarness harness =
            await TransportHarness.StartAsync(db, _probe, [typeof(BatchedGroupConsumer)], reset: false);

        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 4, Within, "every step tried");

        // Steps 3 and 4 went back to the queue twice, unhandled, when 1 and 2 failed ahead of them.
        Assert.Equal([1, 2, 3, 4], _probe.Deliveries.Select(d => ((BatchedStep)d.Message).Seq));
        Assert.All(_probe.Deliveries, d => Assert.Equal(0, d.RedeliveryCount));
        await Eventually.HoldsAsync(async () => await db.CountAsync("t_batched_group") == 0, Within, "queue empty");
        Assert.Equal(2, await db.ArchivedAsync("t_batched_group"));
    }

    [Fact]
    public async Task An_insert_inside_the_notification_throttle_Should_not_wait_for_the_next_regular_sweep()
    {
        TimeSpan sweep = TimeSpan.FromSeconds(30);
        await using TransportHarness harness = await TransportHarness.StartAsync(db, _probe, [typeof(ThrottledConsumer)],
            o =>
            {
                o.MaxIdleDelay = sweep;
                o.InsertNotificationThrottle = TimeSpan.FromSeconds(1);
            });

        // Past the sweeps of a starting host, so the first message is found by its notification alone.
        await Task.Delay(TimeSpan.FromSeconds(3), Ct);
        await harness.Bus.PublishAsync(new Throttled(1), Ct);
        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 1, TimeSpan.FromSeconds(5), "first handled");

        // Its queue is idle again, and pgmq sends no notification for this one: the last was under a second ago.
        await Task.Delay(TimeSpan.FromMilliseconds(200), Ct);
        Stopwatch clock = Stopwatch.StartNew();
        await harness.Bus.PublishAsync(new Throttled(2), Ct);

        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 2, TimeSpan.FromSeconds(10), "second handled");
        Assert.InRange(clock.Elapsed, TimeSpan.Zero, TimeSpan.FromSeconds(5));
    }

    private Task<TransportHarness> StartAsync(params Type[] consumers) => TransportHarness.StartAsync(db, _probe, consumers);

    public sealed record Wrapped;

    [QueueName("t_wrapping")]
    public sealed class WrappingConsumer(Probe probe) : IPgmqConsumer<Wrapped>
    {
        public async Task HandleAsync(PgmqConsumeContext<Wrapped> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(WrappingConsumer), context.Message, context);

            try
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
            catch (OperationCanceledException ex)
            {
                throw new InvalidOperationException("The call was cancelled.", ex);
            }
        }
    }

    public sealed record Outage;

    [QueueName("t_outage")]
    public sealed class OutageConsumer(Probe probe) : IPgmqConsumer<Outage>
    {
        public async Task HandleAsync(PgmqConsumeContext<Outage> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(OutageConsumer), context.Message, context);
            await probe.Gate("outage").WaitAsync(cancellationToken);
        }
    }

    public sealed record LostLease;

    [QueueName("t_lost_lease")]
    public sealed class LostLeaseConsumer(Probe probe) : IPgmqConsumer<LostLease>
    {
        public async Task HandleAsync(PgmqConsumeContext<LostLease> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(LostLeaseConsumer), context.Message, context);
            await probe.Gate("lost-lease").WaitAsync(cancellationToken);
        }
    }

    public sealed record BatchedStep(int Seq, bool Poison);

    [QueueName("t_batched_group")]
    [ConsumerOptions(ReadMode = PgmqReadMode.Grouped, Concurrency = 4)]
    public sealed class BatchedGroupConsumer(Probe probe) : IPgmqConsumer<BatchedStep>
    {
        public Task HandleAsync(PgmqConsumeContext<BatchedStep> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(BatchedGroupConsumer), context.Message, context);
            return context.Message.Poison
                ? throw new InvalidOperationException("permanent: archive it")
                : Task.CompletedTask;
        }
    }

    public sealed record Throttled(int Id);

    [QueueName("t_throttled")]
    public sealed class ThrottledConsumer(Probe probe) : IPgmqConsumer<Throttled>
    {
        public Task HandleAsync(PgmqConsumeContext<Throttled> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(ThrottledConsumer), context.Message, context);
            return Task.CompletedTask;
        }
    }
}
