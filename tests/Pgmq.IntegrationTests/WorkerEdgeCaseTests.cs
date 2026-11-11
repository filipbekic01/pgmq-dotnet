using Microsoft.Extensions.Diagnostics.HealthChecks;
using Npgsql;
using Pgmq.Hosting;

namespace Pgmq.IntegrationTests;

/// <summary>
///     The worker on the paths a happy publish never takes, against real PGMQ: messages from outside Pgmq, consumers
///     of base types and of several types, sending to itself, a permanent failure inside a FIFO group, a lost database
///     connection, and the health check while running. Needs a running Docker daemon.
/// </summary>
public sealed class WorkerEdgeCaseTests(PgmqFixture db) : IClassFixture<PgmqFixture>
{
    private static readonly TimeSpan Within = TimeSpan.FromSeconds(20);

    private readonly Probe _probe = new();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_message_sent_from_outside_Should_be_handled_when_it_names_its_type_and_archived_otherwise()
    {
        await using TransportHarness harness = await StartAsync(typeof(ForeignConsumer));

        // Another language's producer: only the message-type header is needed, with the name the type travels under.
        await harness.Queues.SendAsync("t_foreign", """{"orderId":7}""", """{"message-type":"foreign.order"}""",
            cancellationToken: Ct);
        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 1, Within, "the typed foreign message handled");
        Assert.Equal(7, Assert.IsType<ForeignOrder>(_probe.Deliveries.Single().Message).OrderId);

        // No headers: it can never be handled, so it is archived at once. A type nobody here consumes may be known to
        // a newer instance, so it is put back a few times first, and archived only after MaxAttempts reads.
        await harness.Queues.SendAsync("t_foreign", """{"orderId":8}""", cancellationToken: Ct);
        await harness.Queues.SendAsync("t_foreign", """{"orderId":9}""", """{"message-type":"nobody.knows"}""",
            cancellationToken: Ct);
        await Eventually.HoldsAsync(async () => await db.ArchivedAsync("t_foreign") == 1, Within,
            "the headerless one archived");
        Assert.Equal(1, await db.CountAsync("t_foreign"));
        await Eventually.HoldsAsync(async () => await db.ArchivedAsync("t_foreign") == 2, Within,
            "the unknown type archived after its attempts");
        Assert.Equal(0, await db.CountAsync("t_foreign"));
        Assert.Single(_probe.Deliveries);
    }

    [Fact]
    public async Task A_type_this_instance_does_not_know_Should_wait_for_an_instance_that_does()
    {
        // An old instance, during a rolling deploy, reads a message only the new version consumes.
        await using (TransportHarness old = await StartAsync(typeof(RollingOldConsumer)))
        {
            await old.Queues.SendAsync("t_rolling", """{"id":2}""", """{"message-type":"rolling.v2"}""",
                cancellationToken: Ct);
            await Eventually.HoldsAsync(async () =>
                    Convert.ToInt64(await db.ScalarObjectAsync("select count(*) from pgmq.q_t_rolling where read_ct > 0")) == 1,
                Within, "read by the old instance");
            await old.StopAsync();
        }

        Assert.Equal(0, await db.ArchivedAsync("t_rolling"));
        Assert.Empty(_probe.Deliveries);

        await using TransportHarness updated =
            await TransportHarness.StartAsync(db, _probe, [typeof(RollingNewConsumer)], reset: false);

        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 1, Within, "handled by the new instance");
        Assert.Equal(2, Assert.IsType<RollingV2>(_probe.Deliveries.Single().Message).Id);
        Assert.Equal(0, await db.ArchivedAsync("t_rolling"));
    }

    [Fact]
    public async Task A_message_that_cannot_be_handled_Should_not_hold_up_the_rest_of_its_group()
    {
        await using TransportHarness harness = await StartAsync(typeof(GroupedForeignConsumer));

        await harness.Queues.SendAsync("t_foreign_fifo", "{}", """{"x-pgmq-group":"a"}""", cancellationToken: Ct);
        await harness.Bus.PublishAsync(new GroupedOrder(1), new PgmqPublishOptions { GroupId = "a" }, Ct);

        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 1, Within, "the group went on");
        Assert.Equal(1, await db.ArchivedAsync("t_foreign_fifo"));
    }

    [Fact]
    public async Task A_permanent_failure_in_a_group_Should_archive_it_and_let_the_group_go_on_in_order()
    {
        await using TransportHarness harness = await StartAsync(typeof(PoisonedGroupConsumer));

        for (int seq = 1; seq <= 4; seq++)
        {
            await harness.Bus.PublishAsync(new GroupedStep(seq, Poison: seq == 2),
                new PgmqPublishOptions { GroupId = "g" }, Ct);
        }

        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 4, Within, "every step tried");
        Assert.Equal([1, 2, 3, 4], _probe.Deliveries.Select(d => ((GroupedStep)d.Message).Seq));
        await Eventually.HoldsAsync(async () => await db.CountAsync("t_poisoned_group") == 0, Within, "queue empty");
        Assert.Equal(1, await db.ArchivedAsync("t_poisoned_group"));
    }

    [Fact]
    public async Task A_consumer_of_a_base_type_Should_get_derived_messages_and_one_consumer_Should_take_several_types()
    {
        await using TransportHarness harness = await StartAsync(typeof(AnimalConsumer), typeof(DogAndNoteConsumer));

        await harness.Bus.PublishAsync(new Dog("rex"), Ct);
        await harness.Bus.PublishAsync(new Cat("tom"), Ct);
        await harness.Bus.PublishAsync(new Note("hi"), Ct);

        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 4, Within, "every copy handled");

        // A Cat has no consumer of its own: it still resolves, as a type deriving from a subscribed one.
        Assert.Equal(["Cat", "Dog"],
            _probe.Of(nameof(AnimalConsumer)).Select(d => d.Message.GetType().Name).Order(StringComparer.Ordinal));
        Assert.Equal(["Dog", "Note"],
            _probe.Of(nameof(DogAndNoteConsumer)).Select(d => d.Message.GetType().Name).Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task Sending_to_itself_Should_reach_only_its_own_queue()
    {
        await using TransportHarness harness = await StartAsync(typeof(SelfSendingConsumer), typeof(BystanderConsumer));

        await harness.Bus.PublishAsync(new Ping(), Ct);

        await Eventually.HoldsAsync(() => _probe.Of(nameof(SelfSendingConsumer)).Any(d => d.Message is Pong), Within,
            "the pong came back to the sender");
        await Task.Delay(TimeSpan.FromSeconds(1), Ct);
        Assert.Empty(_probe.Of(nameof(BystanderConsumer)));
    }

    [Fact]
    public async Task A_lost_database_connection_Should_be_survived_and_later_messages_handled()
    {
        await using TransportHarness harness = await StartAsync(typeof(SurvivorConsumer));
        await harness.Bus.PublishAsync(new Survivor(1), Ct);
        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 1, Within, "handled before the outage");

        // What a failover or a restarted server does to every open connection, the worker's included.
        await db.ScalarObjectAsync(
            "select count(pg_terminate_backend(pid)) from pg_stat_activity " +
            "where datname = current_database() and pid <> pg_backend_pid()");

        // Pooled connections the server cut fail on their next use; a caller retries a transient failure.
        await Eventually.HoldsAsync(async () =>
        {
            try
            {
                await harness.Bus.PublishAsync(new Survivor(2), Ct);
                return true;
            }
            catch (NpgsqlException)
            {
                return false;
            }
        }, Within, "published after the outage");

        await Eventually.HoldsAsync(() => _probe.Deliveries.Any(d => d.Message is Survivor { Id: 2 }), Within,
            "handled after the outage");
        Assert.Empty(harness.Status.StalledQueues);
    }

    [Fact]
    public async Task A_message_read_past_its_attempts_without_settling_Should_be_archived_without_running()
    {
        await using (TransportHarness stopped = await StartAsync(typeof(CrashingConsumer)))
        {
            await stopped.StopAsync();
            await stopped.Bus.PublishAsync(new Crashing(), Ct);
        }

        // What attempts that each took the process down leave behind: reads counted, nothing settled.
        await db.ScalarObjectAsync($"update pgmq.q_t_crashing set read_ct = {TransportHarness.Fast.MaxAttempts}");

        await using TransportHarness harness =
            await TransportHarness.StartAsync(db, _probe, [typeof(CrashingConsumer)], reset: false);

        await Eventually.HoldsAsync(async () => await db.ArchivedAsync("t_crashing") == 1, Within, "archived");
        Assert.Empty(_probe.Deliveries);
    }

    [Fact]
    public async Task The_health_check_Should_report_healthy_while_every_queue_reads()
    {
        await using TransportHarness harness = await StartAsync(typeof(SurvivorConsumer));

        HealthCheckResult result = await new PgmqHealthCheck(harness.Status, TransportHarness.Fast)
            .CheckHealthAsync(new HealthCheckContext(), Ct);

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    private Task<TransportHarness> StartAsync(params Type[] consumers) => TransportHarness.StartAsync(db, _probe, consumers);

    // Messages and consumers. Each test's queues are its own, pinned to short names.

    [MessageName("foreign.order")]
    public sealed record ForeignOrder(int OrderId);

    [QueueName("t_foreign")]
    public sealed class ForeignConsumer(Probe probe) : IPgmqConsumer<ForeignOrder>
    {
        public Task HandleAsync(PgmqConsumeContext<ForeignOrder> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(ForeignConsumer), context.Message, context);
            return Task.CompletedTask;
        }
    }

    [MessageName("rolling.v1")]
    public sealed record RollingV1(int Id);

    [MessageName("rolling.v2")]
    public sealed record RollingV2(int Id);

    [QueueName("t_rolling")]
    public sealed class RollingOldConsumer(Probe probe) : IPgmqConsumer<RollingV1>
    {
        public Task HandleAsync(PgmqConsumeContext<RollingV1> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(RollingOldConsumer), context.Message, context);
            return Task.CompletedTask;
        }
    }

    [QueueName("t_rolling")]
    public sealed class RollingNewConsumer(Probe probe) : IPgmqConsumer<RollingV1>, IPgmqConsumer<RollingV2>
    {
        public Task HandleAsync(PgmqConsumeContext<RollingV1> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(RollingNewConsumer), context.Message, context);
            return Task.CompletedTask;
        }

        public Task HandleAsync(PgmqConsumeContext<RollingV2> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(RollingNewConsumer), context.Message, context);
            return Task.CompletedTask;
        }
    }

    public sealed record GroupedOrder(int Id);

    [QueueName("t_foreign_fifo")]
    [ConsumerOptions(ReadMode = PgmqReadMode.Grouped)]
    public sealed class GroupedForeignConsumer(Probe probe) : IPgmqConsumer<GroupedOrder>
    {
        public Task HandleAsync(PgmqConsumeContext<GroupedOrder> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(GroupedForeignConsumer), context.Message, context);
            return Task.CompletedTask;
        }
    }

    public sealed record GroupedStep(int Seq, bool Poison);

    [QueueName("t_poisoned_group")]
    [ConsumerOptions(ReadMode = PgmqReadMode.Grouped)]
    public sealed class PoisonedGroupConsumer(Probe probe) : IPgmqConsumer<GroupedStep>
    {
        public Task HandleAsync(PgmqConsumeContext<GroupedStep> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(PoisonedGroupConsumer), context.Message, context);
            return context.Message.Poison
                ? throw new InvalidOperationException("permanent: archive it")
                : Task.CompletedTask;
        }
    }

    public abstract record Animal(string Name);

    public sealed record Dog(string Name) : Animal(Name);

    public sealed record Cat(string Name) : Animal(Name);

    public sealed record Note(string Text);

    [QueueName("t_animals")]
    public sealed class AnimalConsumer(Probe probe) : IPgmqConsumer<Animal>
    {
        public Task HandleAsync(PgmqConsumeContext<Animal> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(AnimalConsumer), context.Message, context);
            return Task.CompletedTask;
        }
    }

    [QueueName("t_dogs_and_notes")]
    public sealed class DogAndNoteConsumer(Probe probe) : IPgmqConsumer<Dog>, IPgmqConsumer<Note>
    {
        public Task HandleAsync(PgmqConsumeContext<Dog> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(DogAndNoteConsumer), context.Message, context);
            return Task.CompletedTask;
        }

        public Task HandleAsync(PgmqConsumeContext<Note> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(DogAndNoteConsumer), context.Message, context);
            return Task.CompletedTask;
        }
    }

    public sealed record Ping;

    public sealed record Pong;

    [QueueName("t_self_sender")]
    public sealed class SelfSendingConsumer(Probe probe) : IPgmqConsumer<Ping>, IPgmqConsumer<Pong>
    {
        public Task HandleAsync(PgmqConsumeContext<Ping> context, CancellationToken cancellationToken)
            => context.SendToSelfAsync(new Pong(), TimeSpan.FromMilliseconds(200), cancellationToken);

        public Task HandleAsync(PgmqConsumeContext<Pong> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(SelfSendingConsumer), context.Message, context);
            return Task.CompletedTask;
        }
    }

    [QueueName("t_self_bystander")]
    public sealed class BystanderConsumer(Probe probe) : IPgmqConsumer<Pong>
    {
        public Task HandleAsync(PgmqConsumeContext<Pong> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(BystanderConsumer), context.Message, context);
            return Task.CompletedTask;
        }
    }

    public sealed record Crashing;

    [QueueName("t_crashing")]
    public sealed class CrashingConsumer(Probe probe) : IPgmqConsumer<Crashing>
    {
        public Task HandleAsync(PgmqConsumeContext<Crashing> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(CrashingConsumer), context.Message, context);
            return Task.CompletedTask;
        }
    }

    public sealed record Survivor(int Id);

    [QueueName("t_survivor")]
    public sealed class SurvivorConsumer(Probe probe) : IPgmqConsumer<Survivor>
    {
        public Task HandleAsync(PgmqConsumeContext<Survivor> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(SurvivorConsumer), context.Message, context);
            return Task.CompletedTask;
        }
    }
}
