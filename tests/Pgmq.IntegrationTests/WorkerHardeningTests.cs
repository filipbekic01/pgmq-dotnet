using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
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
    public async Task A_database_that_stops_answering_for_a_while_Should_be_survived_and_its_message_handled_once()
    {
        // Commands and connecting give up after two seconds, as they must to notice a server that no longer answers.
        await using TransportHarness harness = await TransportHarness.StartAsync(db, _probe, [typeof(FrozenConsumer)],
            o => o.ConnectionString = db.ConnectionString + ";Timeout=2;Command Timeout=2;Cancellation Timeout=1000");
        await harness.Bus.PublishAsync(new Frozen(), Ct);
        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 1, Within, "running");

        // Nothing answers for four leases, as when a network drops every packet. The consumer ends meanwhile: its
        // delete times out, and so do the renewals, the sweeps and the listener's ping. Two slots, so the queue is
        // read again as soon as the database answers. This shows the worker comes through it; it does not show how
        // long a write is tried for, because a server that was only frozen still runs the delete it was sent once it
        // goes on. The queue that cannot be written, above, shows that.
        Task frozen = db.FreezeAsync(TimeSpan.FromSeconds(8));
        await Task.Delay(TimeSpan.FromSeconds(1), Ct);
        _probe.Open("frozen");
        await frozen;

        await Eventually.HoldsAsync(async () => await db.CountAsync("t_frozen") == 0, TimeSpan.FromSeconds(60),
            "deleted once the database answered again");
        await Task.Delay(TimeSpan.FromSeconds(1), Ct);
        Assert.Single(_probe.Deliveries);
        Assert.Equal(0, await db.ArchivedAsync("t_frozen"));
        Assert.Empty(harness.Status.StalledQueues);
    }

    [Fact]
    public async Task A_pool_the_application_used_up_Should_not_keep_leases_from_being_renewed()
    {
        using TelemetryCapture telemetry = new();
        await using TransportHarness harness = await TransportHarness.StartAsync(db, _probe, [typeof(HoardingConsumer)],
            o => o.ConnectionString = db.ConnectionString + $";Maximum Pool Size={HoardingConsumer.PoolSize}");
        await harness.Bus.PublishAsync(new Hoarded(), Ct);

        // The consumer takes every connection of the pool and keeps them for several leases.
        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 1, Within, "running, with the pool used up");

        for (int i = 0; i < 10; i++)
        {
            await Task.Delay(TransportHarness.Fast.Lease / 2, Ct);
            Assert.Equal(0L, await db.ScalarObjectAsync(
                "select count(*) from pgmq.q_t_hoarding where vt <= clock_timestamp()"));
        }

        _probe.Open("hoarding");
        await Eventually.HoldsAsync(async () => await db.CountAsync("t_hoarding") == 0, Within, "settled");
        Assert.Single(_probe.Deliveries);
        Assert.Equal(0, telemetry.Sum("pgmq.consumer.leases.lost", "t_hoarding"));
        Assert.Equal(0, telemetry.Sum("pgmq.consumer.lease_renewal.failures", "t_hoarding"));
    }

    [Fact]
    public async Task A_handed_in_data_source_whose_connection_string_lost_its_password_Should_still_renew_leases()
    {
        using TelemetryCapture telemetry = new();
        await using NpgsqlDataSource dataSource = NpgsqlDataSource.Create(db.ConnectionString);

        // The data source does not hand out its password, so a connection outside its pool cannot be opened.
        Assert.DoesNotContain("Password", dataSource.ConnectionString, StringComparison.OrdinalIgnoreCase);

        await using TransportHarness harness = await TransportHarness.StartAsync(db, _probe, [typeof(SharedSourceConsumer)],
            o =>
            {
                o.ConnectionString = null;
                o.DataSource = dataSource;
            });
        await harness.Bus.PublishAsync(new SharedSource(), Ct);
        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 1, Within, "running");

        await Task.Delay(TransportHarness.Fast.Lease * 3, Ct);
        _probe.Open("shared-source");

        await Eventually.HoldsAsync(async () => await db.CountAsync("t_shared_source") == 0, Within, "settled");
        Assert.Single(_probe.Deliveries);
        Assert.Equal(0, telemetry.Sum("pgmq.consumer.leases.lost", "t_shared_source"));
    }

    [Fact]
    public async Task On_shutdown_a_release_that_fails_at_first_Should_be_tried_again_a_little_later()
    {
        // A lease long enough that only a release, never a lapsed lease, makes the message visible in this test.
        TransportHarness harness = await TransportHarness.StartAsync(db, _probe, [typeof(RetriedReleaseConsumer)],
            o => o.Lease = TimeSpan.FromSeconds(30));
        await harness.Bus.PublishAsync(new RetriedRelease(), Ct);
        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 1, Within, "running");

        // The queue is out of reach when the consumer is cancelled (after the drain grace), and back shortly after.
        await db.ScalarObjectAsync("alter table pgmq.q_t_retried_release rename to q_t_retried_release_away");
        Task back = Task.Run(async () =>
        {
            await Task.Delay(TransportHarness.Fast.DrainGrace + TimeSpan.FromMilliseconds(700));
            await db.ScalarObjectAsync("alter table pgmq.q_t_retried_release_away rename to q_t_retried_release");
        }, Ct);

        await harness.StopAsync();
        await back;

        Assert.Equal(1, (await harness.Queues.GetMetricsAsync("t_retried_release", Ct)).VisibleLength);
        Assert.Equal(0, await db.ArchivedAsync("t_retried_release"));
        await harness.DisposeAsync();
    }

    [Fact]
    public async Task A_group_whose_head_this_host_still_handles_Should_wait_for_it_when_its_lease_lapsed()
    {
        await using TransportHarness harness = await StartAsync(typeof(RereadGroupConsumer));
        PgmqPublishOptions group = new() { GroupId = "g" };
        await harness.Bus.PublishAsync(new RereadStep(1), group, Ct);
        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 1, Within, "the head runs");
        await harness.Bus.PublishAsync(new RereadStep(2), group, Ct);

        // The lease of the head lapsed: one read now takes both, the head included.
        await db.ScalarObjectAsync("update pgmq.q_t_reread_group set vt = clock_timestamp()");
        await Eventually.HoldsAsync(async () =>
                (long)(await db.ScalarObjectAsync("select count(*) from pgmq.q_t_reread_group where read_ct = 2"))! == 1,
            Within, "the head was read again");

        // The second step neither ran alongside the head nor had the read counted.
        await Task.Delay(TimeSpan.FromSeconds(1), Ct);
        Assert.Single(_probe.Deliveries);

        _probe.Open("reread-group");
        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 2, Within, "the second step ran after the head");
        Assert.Equal([1, 2], _probe.Deliveries.Select(d => ((RereadStep)d.Message).Seq));
        Assert.Equal(0, _probe.Deliveries.Last().RedeliveryCount);
        await Eventually.HoldsAsync(async () => await db.CountAsync("t_reread_group") == 0, Within, "queue empty");
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
    public async Task A_message_whose_lease_lapsed_Should_not_be_started_again_by_the_host_still_handling_it()
    {
        using TelemetryCapture telemetry = new();
        await using TransportHarness harness = await StartAsync(typeof(RereadConsumer));
        await harness.Bus.PublishAsync(new Reread(), Ct);
        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 1, Within, "running");

        // What a database that was away for longer than a lease leaves: the message visible again, a slot free.
        await db.ScalarObjectAsync("update pgmq.q_t_reread set vt = clock_timestamp()");

        await Eventually.HoldsAsync(() => telemetry.Sum("pgmq.consumer.leases.lost", "t_reread") == 1, Within,
            "read again, and reported");
        await Task.Delay(TimeSpan.FromSeconds(1), Ct);
        Assert.Single(_probe.Deliveries);

        _probe.Open("reread");
        await Eventually.HoldsAsync(async () => await db.CountAsync("t_reread") == 0, Within, "settled once");
        Assert.Single(_probe.Deliveries);
        Assert.Equal(1, telemetry.Sum("pgmq.consumer.leases.lost", "t_reread"));
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

    public sealed record Frozen;

    [QueueName("t_frozen")]
    [ConsumerOptions(Concurrency = 2)]
    public sealed class FrozenConsumer(Probe probe) : IPgmqConsumer<Frozen>
    {
        public async Task HandleAsync(PgmqConsumeContext<Frozen> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(FrozenConsumer), context.Message, context);
            await probe.Gate("frozen").WaitAsync(cancellationToken);
        }
    }

    public sealed record Hoarded;

    [QueueName("t_hoarding")]
    public sealed class HoardingConsumer(Probe probe, IServiceProvider services) : IPgmqConsumer<Hoarded>
    {
        public const int PoolSize = 3;

        public async Task HandleAsync(PgmqConsumeContext<Hoarded> context, CancellationToken cancellationToken)
        {
            NpgsqlDataSource pool = services.GetRequiredService<PgmqDatabase>().DataSource;
            List<NpgsqlConnection> held = [];
            try
            {
                for (int i = 0; i < PoolSize; i++)
                {
                    held.Add(await pool.OpenConnectionAsync(cancellationToken));
                }

                probe.Record(nameof(HoardingConsumer), context.Message, context);
                await probe.Gate("hoarding").WaitAsync(cancellationToken);
            }
            finally
            {
                foreach (NpgsqlConnection connection in held)
                {
                    await connection.DisposeAsync();
                }
            }
        }
    }

    public sealed record SharedSource;

    [QueueName("t_shared_source")]
    public sealed class SharedSourceConsumer(Probe probe) : IPgmqConsumer<SharedSource>
    {
        public async Task HandleAsync(PgmqConsumeContext<SharedSource> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(SharedSourceConsumer), context.Message, context);
            await probe.Gate("shared-source").WaitAsync(cancellationToken);
        }
    }

    public sealed record RetriedRelease;

    [QueueName("t_retried_release")]
    public sealed class RetriedReleaseConsumer(Probe probe) : IPgmqConsumer<RetriedRelease>
    {
        public async Task HandleAsync(PgmqConsumeContext<RetriedRelease> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(RetriedReleaseConsumer), context.Message, context);
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }
    }

    public sealed record RereadStep(int Seq);

    [QueueName("t_reread_group")]
    [ConsumerOptions(ReadMode = PgmqReadMode.Grouped, Concurrency = 3)]
    public sealed class RereadGroupConsumer(Probe probe) : IPgmqConsumer<RereadStep>
    {
        public async Task HandleAsync(PgmqConsumeContext<RereadStep> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(RereadGroupConsumer), context.Message, context);
            await probe.Gate("reread-group").WaitAsync(cancellationToken);
        }
    }

    public sealed record Reread;

    [QueueName("t_reread")]
    [ConsumerOptions(Concurrency = 2)]
    public sealed class RereadConsumer(Probe probe) : IPgmqConsumer<Reread>
    {
        public async Task HandleAsync(PgmqConsumeContext<Reread> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(RereadConsumer), context.Message, context);
            await probe.Gate("reread").WaitAsync(cancellationToken);
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
