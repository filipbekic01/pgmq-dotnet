using Npgsql;

namespace Pgmq.IntegrationTests;

/// <summary>
///     The pgmq features beyond plain send and read, against real PGMQ: queue types, topics, insert notifications,
///     long polling, at-most-once reads, headers, and the queue manager. The read modes are in
///     <see cref="ReadModeTests"/>. Needs a running Docker daemon.
/// </summary>
public sealed class FeatureTests(PgmqFixture db) : IClassFixture<PgmqFixture>
{
    private static readonly TimeSpan Within = TimeSpan.FromSeconds(20);

    private readonly Probe _probe = new();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_consumer_Should_get_its_queue_created_as_the_type_it_asks_for()
    {
        await using TransportHarness harness =
            await StartAsync(typeof(UnloggedConsumer), typeof(PartitionedConsumer), typeof(TimePartitionedConsumer));

        IReadOnlyList<PgmqQueueInfo> queues = await db.Queues.ListQueuesAsync(Ct);

        Assert.Contains(queues, q => q is { QueueName: "t_unlogged", IsUnlogged: true, IsPartitioned: false });
        Assert.Contains(queues, q => q is { QueueName: "t_partitioned", IsPartitioned: true });
        Assert.Contains(queues, q => q is { QueueName: "t_partitioned_time", IsPartitioned: true });

        // A numeric interval partitions by message id, a time interval by when the message was sent.
        Assert.Null(await db.PartitionColumnAsync("q_t_unlogged"));
        Assert.Equal("msg_id", await db.PartitionColumnAsync("q_t_partitioned"));
        Assert.Equal("enqueued_at", await db.PartitionColumnAsync("q_t_partitioned_time"));

        await harness.Bus.PublishAsync(new TypedQueueMessage(1), Ct);
        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 3, Within, "every queue type delivered");
    }

    [Fact]
    public async Task A_topic_publish_Should_reach_the_queues_bound_to_a_matching_pattern()
    {
        await using TransportHarness harness = await StartAsync(typeof(TopicConsumer));

        int reached = await harness.Bus.PublishToTopicAsync("t.orders.created", new TopicMessage("one"), Ct);
        int batch = await harness.Bus.PublishBatchToTopicAsync("t.orders.updated",
            [new TopicMessage("two"), new TopicMessage("three")], Ct);
        int none = await harness.Bus.PublishToTopicAsync("t.payments.created", new TopicMessage("nobody"), Ct);

        Assert.Equal((1, 1, 0), (reached, batch, none));
        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 3, Within, "the bound queue got all three");
        Assert.Contains(await db.Queues.TestRoutingAsync("t.orders.anything", Ct), r => r.QueueName == "t_topic");
    }

    [Fact]
    public async Task An_insert_notification_Should_wake_an_idle_queue_before_its_next_poll()
    {
        await using TransportHarness harness = await StartAsync([typeof(NotifiedConsumer)], o =>
        {
            o.InsertNotificationThrottle = TimeSpan.Zero;
            o.LongPollTimeout = null;
            o.MinIdleDelay = TimeSpan.FromSeconds(15);
            o.MaxIdleDelay = TimeSpan.FromSeconds(15);
            o.StallAfter = TimeSpan.FromMinutes(1);
        });

        Assert.Contains(await db.Queues.ListInsertNotificationsAsync(Ct), n => n.QueueName == "t_notified");

        // Let the loop settle into its long idle wait.
        await Task.Delay(TimeSpan.FromSeconds(2), Ct);
        await harness.Bus.PublishAsync(new NotifiedMessage(), Ct);

        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 1, TimeSpan.FromSeconds(5),
            "woken well before the 15 second poll");
    }

    [Fact]
    public async Task Insert_notifications_Should_be_on_by_default_with_long_polling_as_the_fallback()
    {
        PgmqOptions defaults = new();

        Assert.True(defaults.UseInsertNotifications);
        Assert.NotNull(defaults.LongPollTimeout);

        await using TransportHarness harness = await StartAsync(typeof(FallbackConsumer));

        Assert.Contains(await db.Queues.ListInsertNotificationsAsync(Ct), n => n.QueueName == "t_fallback");
    }

    [Fact]
    public async Task Without_auto_setup_Should_create_nothing_and_fail_on_a_missing_queue()
    {
        await db.Queues.InitAsync(Ct);
        if ((await db.Queues.ListQueuesAsync(Ct)).Any(q => q.QueueName == "t_fallback"))
        {
            await db.Queues.DropQueueAsync("t_fallback", Ct);
        }

        await Assert.ThrowsAnyAsync<InvalidOperationException>(async () =>
        {
            await using TransportHarness harness = await StartAsync([typeof(FallbackConsumer)], o => o.AutoSetup = false);
        });

        Assert.DoesNotContain(await db.Queues.ListQueuesAsync(Ct), q => q.QueueName == "t_fallback");

        await db.Queues.CreateQueueAsync("t_fallback", cancellationToken: Ct);
        await using TransportHarness started = await StartAsync([typeof(FallbackConsumer)], o => o.AutoSetup = false);
        await started.Bus.PublishAsync(new FallbackMessage(), Ct);
        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 1, Within, "handled from the existing queue");
    }

    [Fact]
    public async Task A_lost_listener_Should_leave_the_queue_long_polling_until_it_reconnects()
    {
        await using TransportHarness harness = await StartAsync([typeof(FallbackConsumer)], o =>
        {
            o.InsertNotificationThrottle = TimeSpan.Zero;
            o.LongPollTimeout = TimeSpan.FromSeconds(3);
            o.MinIdleDelay = TimeSpan.FromMilliseconds(100);
            o.MaxIdleDelay = TimeSpan.FromSeconds(15);
            o.StallAfter = TimeSpan.FromMinutes(1);
        });

        // Let the listener come up, then cut its connection. It waits 5 seconds before reconnecting.
        await Task.Delay(TimeSpan.FromSeconds(2), Ct);
        object? killed = await db.ScalarObjectAsync(
            "select count(pg_terminate_backend(pid)) from pg_stat_activity " +
            "where query ilike 'LISTEN %t_fallback%' and pid <> pg_backend_pid()");
        Assert.Equal(1L, killed);

        await Task.Delay(TimeSpan.FromMilliseconds(500), Ct);
        await harness.Bus.PublishAsync(new FallbackMessage(), Ct);

        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 1, TimeSpan.FromSeconds(3),
            "picked up by a long poll, before the listener reconnects or the 15 second poll");
    }

    [Fact]
    public async Task A_long_poll_Should_pick_up_a_message_while_the_database_waits()
    {
        await using TransportHarness harness = await StartAsync([typeof(LongPollConsumer)], o =>
        {
            o.UseInsertNotifications = false;
            o.LongPollTimeout = TimeSpan.FromSeconds(3);
            o.MinIdleDelay = TimeSpan.FromMilliseconds(100);
            o.MaxIdleDelay = TimeSpan.FromSeconds(15);
        });

        await Task.Delay(TimeSpan.FromSeconds(1), Ct);
        await harness.Bus.PublishAsync(new LongPollMessage(), Ct);

        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 1, TimeSpan.FromSeconds(3),
            "picked up by the poll in progress");
    }

    [Fact]
    public async Task An_at_most_once_consumer_Should_never_see_a_message_twice_even_when_it_fails()
    {
        await using TransportHarness harness = await StartAsync(typeof(AtMostOnceConsumer));

        await harness.Bus.PublishAsync(new AtMostOnceMessage(), Ct);

        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 1, Within, "handled");
        await Task.Delay(TransportHarness.Fast.Lease * 2, Ct);
        Assert.Single(_probe.Deliveries);
        Assert.Equal(0, await db.CountAsync("t_at_most_once"));
        Assert.Equal(0, await db.ArchivedAsync("t_at_most_once"));
    }

    [Fact]
    public async Task Custom_headers_and_a_group_Should_reach_the_consumer_and_survive_a_redelivery()
    {
        await using TransportHarness harness = await StartAsync(typeof(HeadersConsumer));

        await harness.Bus.PublishAsync(new HeadersMessage(), new PgmqPublishOptions
        {
            GroupId = "g-1",
            Headers = new Dictionary<string, string> { ["tenant"] = "acme" },
        }, Ct);

        await Eventually.HoldsAsync(() => _probe.Deliveries.Count == 2, Within, "first delivery and redelivery");
        Assert.All(_probe.Deliveries, d =>
        {
            Assert.Equal("g-1", d.GroupId);
            Assert.Equal("acme", d.Headers?["tenant"]);
        });
    }

    [Fact]
    public async Task The_queue_manager_Should_create_inspect_and_drop_queues()
    {
        PgmqQueueManager queues = db.Queues;
        await queues.InitAsync(Ct);

        Assert.Equal("1.13.1", await queues.GetVersionAsync(Ct));

        Assert.True(await queues.EnsureQueueAsync("m_standard", cancellationToken: Ct));
        Assert.False(await queues.EnsureQueueAsync("m_standard", cancellationToken: Ct));
        await queues.InitPartmanAsync(Ct);
        Assert.True(await queues.EnsureQueueAsync("m_partitioned", PgmqQueueOptions.Partitioned(), Ct));
        Assert.False(await queues.EnsureQueueAsync("m_partitioned", PgmqQueueOptions.Partitioned(), Ct));
        await queues.CreateQueueAsync("m_unlogged", PgmqQueueOptions.Unlogged, Ct);

        IReadOnlyList<PgmqQueueInfo> listed = await queues.ListQueuesAsync(Ct);
        Assert.Contains(listed, q => q is { QueueName: "m_standard", IsPartitioned: false, IsUnlogged: false });
        Assert.Contains(listed, q => q is { QueueName: "m_partitioned", IsPartitioned: true });
        Assert.Contains(listed, q => q is { QueueName: "m_unlogged", IsUnlogged: true });

        await db.ScalarObjectAsync(
            """select count(*) from pgmq.send_batch('m_standard', array['{"a":1}', '{"a":2}', '{"a":3}']::jsonb[])""");

        PgmqQueueMetrics metrics = await queues.GetMetricsAsync("m_standard", Ct);
        Assert.Equal((3L, 3L), (metrics.QueueLength, metrics.VisibleLength));
        Assert.NotNull(metrics.OldestMessageAge);
        Assert.Null(metrics.DefaultPartitionLength);
        Assert.Equal(0, (await queues.GetMetricsAsync("m_partitioned", Ct)).DefaultPartitionLength);
        Assert.Contains(await queues.GetAllMetricsAsync(Ct), m => m.QueueName == "m_standard");

        Assert.Equal(3, await queues.PurgeQueueAsync("m_standard", Ct));
        Assert.Equal(0, (await queues.GetMetricsAsync("m_standard", Ct)).QueueLength);

        Assert.True(await queues.DropQueueAsync("m_unlogged", Ct));
        Assert.False(await queues.QueueExistsAsync("m_unlogged", Ct));
        Assert.True(await queues.QueueExistsAsync("m_standard", Ct));
    }

    [Fact]
    public async Task The_queue_manager_Should_delete_archive_and_hide_messages_by_id()
    {
        PgmqQueueManager queues = db.Queues;
        await queues.InitAsync(Ct);
        await queues.EnsureQueueAsync("m_messages", cancellationToken: Ct);
        await db.ResetAsync("m_messages");

        long[] ids = await db.SendAsync("m_messages", 6);

        Assert.True(await queues.SetVisibilityTimeoutAsync("m_messages", ids[0], TimeSpan.FromMinutes(5), Ct));
        Assert.Equal([ids[1], ids[2]],
            (await queues.SetVisibilityTimeoutAsync("m_messages", [ids[1], ids[2]], TimeSpan.FromMinutes(5), Ct))
            .Order());
        Assert.Equal(3, (await queues.GetMetricsAsync("m_messages", Ct)).VisibleLength);
        Assert.True(await queues.SetVisibilityTimeoutAsync("m_messages", ids[0], TimeSpan.Zero, Ct));
        Assert.Equal(4, (await queues.GetMetricsAsync("m_messages", Ct)).VisibleLength);

        Assert.True(await queues.DeleteMessageAsync("m_messages", ids[0], Ct));
        Assert.False(await queues.DeleteMessageAsync("m_messages", ids[0], Ct));
        Assert.Equal([ids[1]], await queues.DeleteMessagesAsync("m_messages", [ids[1], ids[0]], Ct));

        Assert.True(await queues.ArchiveMessageAsync("m_messages", ids[2], Ct));
        Assert.Equal([ids[3], ids[4]], (await queues.ArchiveMessagesAsync("m_messages", [ids[3], ids[4]], Ct)).Order());

        Assert.Equal(1, await db.CountAsync("m_messages"));
        Assert.Equal(3, await db.ArchivedAsync("m_messages"));
        Assert.False(await queues.SetVisibilityTimeoutAsync("m_messages", ids[0], TimeSpan.Zero, Ct));
    }

    [Fact]
    public async Task The_queue_manager_Should_hold_a_queue_lock_until_the_transaction_ends()
    {
        PgmqQueueManager queues = db.Queues;
        await queues.InitAsync(Ct);
        await queues.EnsureQueueAsync("m_lock", cancellationToken: Ct);

        await using NpgsqlConnection holder = new(db.ConnectionString);
        await holder.OpenAsync(Ct);
        await using NpgsqlTransaction held = await holder.BeginTransactionAsync(Ct);
        await queues.AcquireQueueLockAsync("m_lock", held, Ct);

        await using NpgsqlConnection other = new(db.ConnectionString);
        await other.OpenAsync(Ct);
        await using (NpgsqlTransaction blocked = await other.BeginTransactionAsync(Ct))
        {
            await using (NpgsqlCommand timeout = new("set local lock_timeout = '200ms'", other, blocked))
            {
                await timeout.ExecuteNonQueryAsync(Ct);
            }

            PostgresException ex = await Assert.ThrowsAsync<PostgresException>(
                () => queues.AcquireQueueLockAsync("m_lock", blocked, Ct));
            Assert.Equal(PostgresErrorCodes.LockNotAvailable, ex.SqlState);
        }

        await held.CommitAsync(Ct);

        await using NpgsqlTransaction after = await other.BeginTransactionAsync(Ct);
        await queues.AcquireQueueLockAsync("m_lock", after, Ct);
        await after.CommitAsync(Ct);
    }

    [Fact]
    public async Task The_queue_manager_Should_bind_topics_and_test_routing()
    {
        PgmqQueueManager queues = db.Queues;
        await queues.InitAsync(Ct);
        await queues.EnsureQueueAsync("m_topics", cancellationToken: Ct);

        await queues.BindTopicAsync("m.logs.#", "m_topics", Ct);
        await queues.BindTopicAsync("m.logs.#", "m_topics", Ct);

        Assert.Single(await queues.ListTopicBindingsAsync("m_topics", Ct));
        Assert.Contains(await queues.ListTopicBindingsAsync(cancellationToken: Ct), b => b.QueueName == "m_topics");
        Assert.Single(await queues.TestRoutingAsync("m.logs.app.error", Ct), r => r.QueueName == "m_topics");

        Assert.True(await queues.UnbindTopicAsync("m.logs.#", "m_topics", Ct));
        Assert.False(await queues.UnbindTopicAsync("m.logs.#", "m_topics", Ct));
        Assert.DoesNotContain(await queues.TestRoutingAsync("m.logs.app.error", Ct), r => r.QueueName == "m_topics");
    }

    [Fact]
    public async Task The_queue_manager_Should_turn_insert_notifications_on_change_them_and_off()
    {
        PgmqQueueManager queues = db.Queues;
        await queues.InitAsync(Ct);
        await queues.EnsureQueueAsync("m_notify", cancellationToken: Ct);

        await queues.EnableInsertNotificationsAsync("m_notify", TimeSpan.FromMilliseconds(100), Ct);
        Assert.Equal(TimeSpan.FromMilliseconds(100), await ThrottleAsync());

        await queues.UpdateInsertNotificationsAsync("m_notify", TimeSpan.FromMilliseconds(300), Ct);
        Assert.Equal(TimeSpan.FromMilliseconds(300), await ThrottleAsync());

        await queues.EnsureInsertNotificationsAsync("m_notify", TimeSpan.FromMilliseconds(300), Ct);
        Assert.Equal(TimeSpan.FromMilliseconds(300), await ThrottleAsync());

        await queues.DisableInsertNotificationsAsync("m_notify", Ct);
        Assert.Null(await ThrottleAsync());

        async Task<TimeSpan?> ThrottleAsync()
            => (await queues.ListInsertNotificationsAsync(Ct)).FirstOrDefault(n => n.QueueName == "m_notify")
                ?.ThrottleInterval;
    }

    [Fact]
    public async Task The_queue_manager_Should_index_fifo_headers_and_partition_an_archive()
    {
        PgmqQueueManager queues = db.Queues;
        await queues.InitAsync(Ct);
        await queues.EnsureQueueAsync("m_fifo", cancellationToken: Ct);
        await queues.EnsureQueueAsync("m_archive", cancellationToken: Ct);
        await queues.InitPartmanAsync(Ct);

        await queues.CreateFifoIndexAsync("m_fifo", Ct);
        await queues.CreateFifoIndexAsync("m_fifo", Ct);
        await queues.CreateFifoIndexesAsync(Ct);
        Assert.Equal(1L, await db.ScalarObjectAsync(
            "select count(*) from pg_indexes where schemaname = 'pgmq' and indexname = 'q_m_fifo_fifo_idx'"));

        await queues.ConvertArchivePartitionedAsync("m_archive", cancellationToken: Ct);
        Assert.Equal('p', await db.ScalarObjectAsync(
            "select c.relkind from pg_class c join pg_namespace n on n.oid = c.relnamespace " +
            "where n.nspname = 'pgmq' and c.relname = 'a_m_archive'"));
    }

    private Task<TransportHarness> StartAsync(params Type[] consumers) => TransportHarness.StartAsync(db, _probe, consumers);

    private Task<TransportHarness> StartAsync(Type[] consumers, Action<PgmqOptions> configure)
        => TransportHarness.StartAsync(db, _probe, consumers, configure);

    // Messages and consumers. Each test's queues are its own, pinned to short names.

    public sealed record TypedQueueMessage(int Id);

    [QueueName("t_unlogged")]
    [ConsumerOptions(QueueType = PgmqQueueType.Unlogged)]
    public sealed class UnloggedConsumer(Probe probe) : IPgmqConsumer<TypedQueueMessage>
    {
        public Task HandleAsync(PgmqConsumeContext<TypedQueueMessage> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(UnloggedConsumer), context.Message, context);
            return Task.CompletedTask;
        }
    }

    [QueueName("t_partitioned")]
    [ConsumerOptions(QueueType = PgmqQueueType.Partitioned, PartitionInterval = "1000", RetentionInterval = "10000")]
    public sealed class PartitionedConsumer(Probe probe) : IPgmqConsumer<TypedQueueMessage>
    {
        public Task HandleAsync(PgmqConsumeContext<TypedQueueMessage> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(PartitionedConsumer), context.Message, context);
            return Task.CompletedTask;
        }
    }

    [QueueName("t_partitioned_time")]
    [ConsumerOptions(QueueType = PgmqQueueType.Partitioned, PartitionInterval = "1 day", RetentionInterval = "7 days")]
    public sealed class TimePartitionedConsumer(Probe probe) : IPgmqConsumer<TypedQueueMessage>
    {
        public Task HandleAsync(PgmqConsumeContext<TypedQueueMessage> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(TimePartitionedConsumer), context.Message, context);
            return Task.CompletedTask;
        }
    }

    public sealed record TopicMessage(string Name);

    [QueueName("t_topic")]
    [TopicBinding("t.orders.*")]
    public sealed class TopicConsumer(Probe probe) : IPgmqConsumer<TopicMessage>
    {
        public Task HandleAsync(PgmqConsumeContext<TopicMessage> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(TopicConsumer), context.Message, context);
            return Task.CompletedTask;
        }
    }

    public sealed record NotifiedMessage;

    [QueueName("t_notified")]
    public sealed class NotifiedConsumer(Probe probe) : IPgmqConsumer<NotifiedMessage>
    {
        public Task HandleAsync(PgmqConsumeContext<NotifiedMessage> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(NotifiedConsumer), context.Message, context);
            return Task.CompletedTask;
        }
    }

    public sealed record FallbackMessage;

    [QueueName("t_fallback")]
    public sealed class FallbackConsumer(Probe probe) : IPgmqConsumer<FallbackMessage>
    {
        public Task HandleAsync(PgmqConsumeContext<FallbackMessage> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(FallbackConsumer), context.Message, context);
            return Task.CompletedTask;
        }
    }

    public sealed record LongPollMessage;

    [QueueName("t_long_poll")]
    public sealed class LongPollConsumer(Probe probe) : IPgmqConsumer<LongPollMessage>
    {
        public Task HandleAsync(PgmqConsumeContext<LongPollMessage> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(LongPollConsumer), context.Message, context);
            return Task.CompletedTask;
        }
    }

    public sealed record AtMostOnceMessage;

    [QueueName("t_at_most_once")]
    [ConsumerOptions(AtMostOnce = true)]
    public sealed class AtMostOnceConsumer(Probe probe) : IPgmqConsumer<AtMostOnceMessage>
    {
        public Task HandleAsync(PgmqConsumeContext<AtMostOnceMessage> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(AtMostOnceConsumer), context.Message, context);
            throw new TimeoutException("transient, but there is no second chance");
        }
    }

    public sealed record HeadersMessage;

    [QueueName("t_headers")]
    public sealed class HeadersConsumer(Probe probe) : IPgmqConsumer<HeadersMessage>
    {
        public Task HandleAsync(PgmqConsumeContext<HeadersMessage> context, CancellationToken cancellationToken)
        {
            probe.Record(nameof(HeadersConsumer), context.Message, context);
            return context.RedeliveryCount == 0
                ? context.RedeliverAsync(TimeSpan.FromMilliseconds(200))
                : Task.CompletedTask;
        }
    }
}
