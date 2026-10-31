using System.Diagnostics;

namespace Pgmq.IntegrationTests;

/// <summary>
///     How the transport behaves against real PGMQ: fan-out, retries, leases, per-queue concurrency and
///     independence, timeouts, the outbox, shutdown, delays, redelivery and tracing. Each test runs its own consumers
///     on its own queues. Needs a running Docker daemon.
/// </summary>
public sealed class TransportTests(PgmqFixture db) : IClassFixture<PgmqFixture>
{
    private static readonly TimeSpan Within = TimeSpan.FromSeconds(20);

    private readonly Probe _probe = new();

    [Fact]
    public async Task A_publish_Should_reach_every_consumer_of_its_type_once()
    {
        await using TransportHarness harness =
            await StartAsync(typeof(FanOutFirstConsumer), typeof(FanOutSecondConsumer));

        await harness.Bus.PublishAsync(new FanOutMessage(1), Ct);

        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 2, Within, "both consumers got it");
        Assert.Single(_probe.Of(nameof(FanOutFirstConsumer)));
        Assert.Single(_probe.Of(nameof(FanOutSecondConsumer)));
        await AssertEmptyAsync("t_fanout_first", "t_fanout_second");
    }

    [Fact]
    public async Task A_transient_failure_Should_be_retried_and_counted_until_it_succeeds()
    {
        await using TransportHarness harness = await StartAsync(typeof(FlakyConsumer));

        await harness.Bus.PublishAsync(new FlakyMessage(FailTimes: 2), Ct);

        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 3, Within, "three attempts");
        Assert.Equal([0, 1, 2], _probe.Deliveries.Select(d => d.RedeliveryCount));
        await Eventually.HoldsAsync(async () => await db.CountAsync("t_flaky") == 0, Within, "deleted after success");
        Assert.Equal(0, await db.ArchivedAsync("t_flaky"));
    }

    [Fact]
    public async Task A_transient_failure_Should_be_archived_once_attempts_run_out()
    {
        await using TransportHarness harness = await StartAsync(typeof(FlakyConsumer));

        await harness.Bus.PublishAsync(new FlakyMessage(FailTimes: 99), Ct);

        await Eventually.HoldsAsync(async () => await db.ArchivedAsync("t_flaky") == 1, Within, "archived");
        Assert.Equal(TransportHarness.Fast.MaxAttempts, _probe.Deliveries.Count);
    }

    [Fact]
    public async Task Any_other_failure_Should_be_archived_after_one_attempt()
    {
        await using TransportHarness harness = await StartAsync(typeof(BrokenConsumer));

        await harness.Bus.PublishAsync(new BrokenMessage(), Ct);

        await Eventually.HoldsAsync(async () => await db.ArchivedAsync("t_broken") == 1, Within, "archived");
        await Task.Delay(TransportHarness.Fast.Lease * 2, Ct);
        Assert.Single(_probe.Deliveries);
        Assert.Equal(0, await db.CountAsync("t_broken"));
    }

    [Fact]
    public async Task A_consumer_longer_than_its_lease_Should_keep_the_message_and_run_once()
    {
        // Two instances on the same queue: if the lease lapsed, the other would take the message.
        await using TransportHarness first = await StartAsync(typeof(SlowConsumer));
        await using TransportHarness second =
            await TransportHarness.StartAsync(db, _probe, [typeof(SlowConsumer)], reset: false);

        await first.Bus.PublishAsync(new SlowMessage(TimeSpan.FromSeconds(6)), Ct);

        await Eventually.HoldsAsync(async () => _probe.Deliveries.Count == 1 && await db.CountAsync("t_slow") == 0,
            Within, "handled and deleted");
        await Task.Delay(TransportHarness.Fast.Lease * 2, Ct);
        Assert.Single(_probe.Deliveries);
    }

    [Fact]
    public async Task A_queue_Should_run_no_more_than_its_concurrency_at_once()
    {
        await using TransportHarness harness = await StartAsync(typeof(LimitedConsumer));

        foreach (int i in Enumerable.Range(0, 8))
        {
            await harness.Bus.PublishAsync(new LimitedMessage(i), Ct);
        }

        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 8, Within, "all eight handled");
        Assert.Equal(2, _probe.MaxRunning);
    }

    [Fact]
    public async Task A_queue_with_every_slot_busy_Should_not_hold_up_another_queue()
    {
        await using TransportHarness harness = await StartAsync(typeof(BlockedConsumer), typeof(FreeConsumer));

        await harness.Bus.PublishAsync(new BlockedMessage(), Ct);
        await harness.Bus.PublishAsync(new BlockedMessage(), Ct);
        await Eventually.HoldsAsync(() => _probe.Of(nameof(BlockedConsumer)).Count == 1, Within, "blocked one runs");

        await harness.Bus.PublishAsync(new FreeMessage(), Ct);

        await Eventually.HoldsAsync(() => _probe.Of(nameof(FreeConsumer)).Count == 1, Within,
            "the other queue ran while the first was full");
        Assert.Single(_probe.Of(nameof(BlockedConsumer)));

        _probe.Open(nameof(BlockedConsumer));
        await Eventually.HoldsAsync(() => _probe.Of(nameof(BlockedConsumer)).Count == 2, Within, "second blocked one");
    }

    [Fact]
    public async Task A_consumer_past_its_timeout_Should_be_cancelled_archived_and_free_its_slot()
    {
        await using TransportHarness harness = await StartAsync(o => o.DefaultTimeout = TimeSpan.FromSeconds(1), typeof(HangingConsumer));

        await harness.Bus.PublishAsync(new HangingMessage(HonoursToken: true), Ct);
        await harness.Bus.PublishAsync(new HangingMessage(HonoursToken: true, Hang: false), Ct);

        await Eventually.HoldsAsync(async () => await db.ArchivedAsync("t_hanging") == 1, Within, "archived");
        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 2, Within, "the next one ran in the freed slot");
        await Eventually.HoldsAsync(async () => await db.CountAsync("t_hanging") == 0, Within, "queue empty");
    }

    [Fact]
    public async Task A_consumer_ignoring_its_timeout_Should_be_abandoned_and_free_its_slot()
    {
        await using TransportHarness harness = await StartAsync(o => o.DefaultTimeout = TimeSpan.FromSeconds(1), typeof(HangingConsumer));

        await harness.Bus.PublishAsync(new HangingMessage(HonoursToken: false), Ct);
        await harness.Bus.PublishAsync(new HangingMessage(HonoursToken: false, Hang: false), Ct);

        await Eventually.HoldsAsync(async () => await db.ArchivedAsync("t_hanging") == 1, Within, "abandoned");
        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 2, Within, "the next one ran in the freed slot");
    }

    [Fact]
    public async Task A_consumer_that_fails_Should_publish_nothing_and_one_that_succeeds_Should_publish()
    {
        await using TransportHarness harness =
            await StartAsync(typeof(PublishingConsumer), typeof(DownstreamConsumer));

        await harness.Bus.PublishAsync(new PublishingMessage("fails", Fail: true), Ct);
        await harness.Bus.PublishAsync(new PublishingMessage("succeeds", Fail: false), Ct);

        await Eventually.HoldsAsync(() => _probe.Of(nameof(DownstreamConsumer)).Count == 1, Within, "downstream ran");
        await Eventually.HoldsAsync(async () => await db.ArchivedAsync("t_publishing") == 1, Within, "failure archived");
        await Task.Delay(TimeSpan.FromSeconds(1), Ct);

        Assert.Equal("succeeds", Assert.IsType<DownstreamMessage>(
            Assert.Single(_probe.Of(nameof(DownstreamConsumer))).Message).From);
    }

    [Fact]
    public async Task On_shutdown_a_running_message_Should_go_straight_back_to_the_queue()
    {
        TransportHarness harness = await StartAsync(typeof(HangingConsumer));
        await harness.Bus.PublishAsync(new HangingMessage(HonoursToken: true), Ct);
        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 1, Within, "running");

        await harness.StopAsync();

        // Released, not left to its lease: it can be read again at once.
        Assert.Equal(1, (await harness.Queues.GetMetricsAsync("t_hanging", Ct)).VisibleLength);
        await harness.DisposeAsync();
    }

    [Fact]
    public async Task A_scheduled_publish_Should_arrive_after_its_delay_and_not_before()
    {
        await using TransportHarness harness = await StartAsync(typeof(FanOutFirstConsumer));

        await harness.Bus.SchedulePublishAsync(new FanOutMessage(2), TimeSpan.FromSeconds(3), Ct);

        await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);
        Assert.Empty(_probe.Deliveries);
        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 1, Within, "arrived after the delay");
    }

    [Fact]
    public async Task A_redelivery_Should_keep_the_message_id_and_count_up()
    {
        await using TransportHarness harness = await StartAsync(typeof(RedeliveringConsumer));

        await harness.Bus.PublishAsync(new RedeliveringMessage(), Ct);

        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 2, Within, "delivered twice");
        Probe.Delivered[] deliveries = [.. _probe.Deliveries];
        Assert.Equal([0, 1], deliveries.Select(d => d.RedeliveryCount));
        Assert.NotNull(deliveries[0].MessageId);
        Assert.Equal(deliveries[0].MessageId, deliveries[1].MessageId);
    }

    [Fact]
    public async Task A_consumer_Should_run_inside_the_trace_that_published_its_message()
    {
        using ActivitySource source = new("Pgmq.IntegrationTests");
        using ActivityListener listener = new()
        {
            ShouldListenTo = s => s.Name is PgmqTelemetry.SourceName or "Pgmq.IntegrationTests",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(listener);

        await using TransportHarness harness = await StartAsync(typeof(FanOutFirstConsumer));

        string traceId;
        using (Activity request = source.StartActivity("request")!)
        {
            traceId = request.TraceId.ToString();
            await harness.Bus.PublishAsync(new FanOutMessage(3), Ct);
        }

        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 1, Within, "handled");
        Assert.Equal(traceId, _probe.Deliveries.Single().TraceId);
    }

    [Fact]
    public async Task A_handled_message_Should_trace_send_process_and_delete_as_one_tree_and_be_measured()
    {
        using TelemetryCapture telemetry = new();
        await using TransportHarness harness = await StartAsync(typeof(FanOutFirstConsumer));

        await harness.Bus.PublishAsync(new FanOutMessage(4), Ct);

        await Eventually.HoldsAsync(() => telemetry.Spans("t_fanout_first").Count == 2, Within, "process and delete");
        Activity send = Assert.Single(telemetry.Spans(MessageNames.Of(typeof(FanOutMessage))));
        Activity process = telemetry.Spans("t_fanout_first").Single(a => a.Kind == ActivityKind.Consumer);
        Activity delete = telemetry.Spans("t_fanout_first").Single(a => a.Kind == ActivityKind.Client);

        Assert.Equal("process t_fanout_first", process.DisplayName);
        Assert.Equal(send.SpanId, process.ParentSpanId);
        Assert.Equal(nameof(FanOutFirstConsumer), process.GetTagItem("messaging.consumer.group.name"));
        Assert.Equal("success", process.GetTagItem("messaging.pgmq.outcome"));
        Assert.Equal(ActivityStatusCode.Unset, process.Status);
        Assert.Equal(new Npgsql.NpgsqlConnectionStringBuilder(db.ConnectionString).Host,
            process.GetTagItem("server.address"));

        Assert.Equal("delete t_fanout_first", delete.DisplayName);
        Assert.Equal(process.SpanId, delete.ParentSpanId);
        Assert.Equal("settle", delete.GetTagItem("messaging.operation.type"));

        Assert.Equal(1, telemetry.Sum("messaging.client.consumed.messages", "t_fanout_first"));
        Assert.Equal(1, telemetry.Sum("messaging.client.sent.messages", "t_fanout_first"));
        TelemetryCapture.Measured processed =
            Assert.Single(telemetry.Of("messaging.process.duration", "t_fanout_first"));
        Assert.Equal("success", processed.Tags["messaging.pgmq.outcome"]);
        Assert.Equal(0, telemetry.Sum("pgmq.consumer.messages.active", "t_fanout_first"));
        Assert.Contains(telemetry.Of("messaging.client.operation.duration", "t_fanout_first"),
            m => m.Tags["messaging.operation.name"] as string == "delete");
    }

    [Fact]
    public async Task A_failed_message_Should_report_its_outcome_and_error_type()
    {
        using TelemetryCapture telemetry = new();
        await using TransportHarness harness = await StartAsync(typeof(BrokenConsumer));

        await harness.Bus.PublishAsync(new BrokenMessage(), Ct);

        await Eventually.HoldsAsync(() => telemetry.Spans("t_broken").Count == 2, Within, "process and archive");
        Activity process = telemetry.Spans("t_broken").Single(a => a.Kind == ActivityKind.Consumer);
        Assert.Equal(ActivityStatusCode.Error, process.Status);
        Assert.Equal("archived", process.GetTagItem("messaging.pgmq.outcome"));
        Assert.Equal(typeof(InvalidOperationException).FullName, process.GetTagItem("error.type"));
        Assert.Contains(process.Events, e => e.Name == "exception");
        Assert.Contains(telemetry.Spans("t_broken"), a => a.DisplayName == "archive t_broken");

        TelemetryCapture.Measured processed = Assert.Single(telemetry.Of("messaging.process.duration", "t_broken"));
        Assert.Equal("archived", processed.Tags["messaging.pgmq.outcome"]);
        Assert.Equal(typeof(InvalidOperationException).FullName, processed.Tags["error.type"]);
    }

    [Fact]
    public async Task Queue_gauges_Should_report_depth_and_oldest_age_when_turned_on()
    {
        using TelemetryCapture telemetry = new();
        await using TransportHarness harness = await StartAsync(
            o => o.QueueMetricsInterval = TimeSpan.FromSeconds(1), typeof(FanOutFirstConsumer));

        await harness.Queues.CreateQueueAsync("t_gauged", cancellationToken: Ct);
        await harness.Queues.PurgeQueueAsync("t_gauged", Ct);
        await ((PgmqPublisher)harness.Bus).SendAsync("t_gauged", new FanOutMessage(5), typeof(FanOutMessage),
            TimeSpan.Zero, 0, null, null, null, Ct);
        await harness.StartHostedAsync<Pgmq.Hosting.QueueMetricsCollector>();

        await Eventually.HoldsAsync(() =>
        {
            telemetry.Collect();
            return telemetry.Of("pgmq.queue.messages", "t_gauged").Any(m => m.Value == 1);
        }, Within, "depth reported");

        Assert.Contains(telemetry.Of("pgmq.queue.messages.visible", "t_gauged"), m => m.Value == 1);
        Assert.Contains(telemetry.Of("pgmq.queue.oldest_message.age", "t_gauged"), m => m.Value >= 0);
    }

    [Fact]
    public async Task Every_read_loop_Should_stay_alive_while_running()
    {
        await using TransportHarness harness =
            await StartAsync(typeof(FanOutFirstConsumer), typeof(FanOutSecondConsumer), typeof(FlakyConsumer));

        await Task.Delay(TransportHarness.Fast.StallAfter + TimeSpan.FromSeconds(1), Ct);

        Assert.Empty(harness.Status.StalledQueues);
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private Task<TransportHarness> StartAsync(params Type[] consumers)
        => TransportHarness.StartAsync(db, _probe, consumers);

    private Task<TransportHarness> StartAsync(Action<PgmqOptions> configure, params Type[] consumers)
        => TransportHarness.StartAsync(db, _probe, consumers, configure);

    private async Task AssertEmptyAsync(params string[] queues)
    {
        foreach (string queue in queues)
        {
            await Eventually.HoldsAsync(async () => await db.CountAsync(queue) == 0, Within, $"{queue} empty");
        }
    }

    // Messages and consumers. Each test's queues are its own, pinned to short names.

    public sealed record FanOutMessage(int Id);

    [QueueName("t_fanout_first")]
    public sealed class FanOutFirstConsumer(Probe probe) : IPgmqConsumer<FanOutMessage>
    {
        public Task HandleAsync(PgmqConsumeContext<FanOutMessage> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(FanOutFirstConsumer), context.Message, context);
            return Task.CompletedTask;
        }
    }

    [QueueName("t_fanout_second")]
    public sealed class FanOutSecondConsumer(Probe probe) : IPgmqConsumer<FanOutMessage>
    {
        public Task HandleAsync(PgmqConsumeContext<FanOutMessage> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(FanOutSecondConsumer), context.Message, context);
            return Task.CompletedTask;
        }
    }

    public sealed record FlakyMessage(int FailTimes);

    [QueueName("t_flaky")]
    public sealed class FlakyConsumer(Probe probe) : IPgmqConsumer<FlakyMessage>
    {
        public Task HandleAsync(PgmqConsumeContext<FlakyMessage> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(FlakyConsumer), context.Message, context);
            return context.RedeliveryCount < context.Message.FailTimes
                ? throw new TimeoutException("transient")
                : Task.CompletedTask;
        }
    }

    public sealed record BrokenMessage;

    [QueueName("t_broken")]
    public sealed class BrokenConsumer(Probe probe) : IPgmqConsumer<BrokenMessage>
    {
        public Task HandleAsync(PgmqConsumeContext<BrokenMessage> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(BrokenConsumer), context.Message, context);
            throw new InvalidOperationException("a bug");
        }
    }

    public sealed record SlowMessage(TimeSpan Duration);

    [QueueName("t_slow")]
    public sealed class SlowConsumer(Probe probe) : IPgmqConsumer<SlowMessage>
    {
        public async Task HandleAsync(PgmqConsumeContext<SlowMessage> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(SlowConsumer), context.Message, context);
            await Task.Delay(context.Message.Duration, cancellationToken);
        }
    }

    public sealed record LimitedMessage(int Id);

    [QueueName("t_limited")]
    [ConsumerOptions(Concurrency = 2)]
    public sealed class LimitedConsumer(Probe probe) : IPgmqConsumer<LimitedMessage>
    {
        public Task HandleAsync(PgmqConsumeContext<LimitedMessage> context, CancellationToken cancellationToken)
            => probe.RunningAsync(async () =>
            {
                await Task.Delay(300, cancellationToken);
                probe.Record(nameof(LimitedConsumer), context.Message, context);
            });
    }

    public sealed record BlockedMessage;

    [QueueName("t_blocked")]
    [ConsumerOptions(Concurrency = 1)]
    public sealed class BlockedConsumer(Probe probe) : IPgmqConsumer<BlockedMessage>
    {
        public async Task HandleAsync(PgmqConsumeContext<BlockedMessage> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(BlockedConsumer), context.Message, context);
            await probe.Gate(nameof(BlockedConsumer)).WaitAsync(cancellationToken);
        }
    }

    public sealed record FreeMessage;

    [QueueName("t_free")]
    public sealed class FreeConsumer(Probe probe) : IPgmqConsumer<FreeMessage>
    {
        public Task HandleAsync(PgmqConsumeContext<FreeMessage> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(FreeConsumer), context.Message, context);
            return Task.CompletedTask;
        }
    }

    public sealed record HangingMessage(bool HonoursToken, bool Hang = true);

    [QueueName("t_hanging")]
    [ConsumerOptions(Concurrency = 1)]
    public sealed class HangingConsumer(Probe probe) : IPgmqConsumer<HangingMessage>
    {
        public async Task HandleAsync(PgmqConsumeContext<HangingMessage> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(HangingConsumer), context.Message, context);

            if (!context.Message.Hang)
            {
                return;
            }

            if (context.Message.HonoursToken)
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
            else
            {
                await Task.Delay(TimeSpan.FromSeconds(15), CancellationToken.None);
            }
        }
    }

    public sealed record PublishingMessage(string Name, bool Fail);

    public sealed record DownstreamMessage(string From);

    [QueueName("t_publishing")]
    public sealed class PublishingConsumer : IPgmqConsumer<PublishingMessage>
    {
        public async Task HandleAsync(PgmqConsumeContext<PublishingMessage> context, CancellationToken cancellationToken)
        {
            await context.PublishAsync(new DownstreamMessage(context.Message.Name));

            if (context.Message.Fail)
            {
                throw new InvalidOperationException("failed after publishing");
            }
        }
    }

    [QueueName("t_downstream")]
    public sealed class DownstreamConsumer(Probe probe) : IPgmqConsumer<DownstreamMessage>
    {
        public Task HandleAsync(PgmqConsumeContext<DownstreamMessage> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(DownstreamConsumer), context.Message, context);
            return Task.CompletedTask;
        }
    }

    public sealed record RedeliveringMessage;

    [QueueName("t_redelivering")]
    public sealed class RedeliveringConsumer(Probe probe) : IPgmqConsumer<RedeliveringMessage>
    {
        public Task HandleAsync(PgmqConsumeContext<RedeliveringMessage> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(RedeliveringConsumer), context.Message, context);
            return context.RedeliveryCount == 0
                ? context.RedeliverAsync(TimeSpan.FromMilliseconds(200))
                : Task.CompletedTask;
        }
    }
}
