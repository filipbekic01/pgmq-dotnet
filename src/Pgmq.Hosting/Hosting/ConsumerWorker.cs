using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Pgmq.Hosting;

/// <summary>
/// Runs every registered consumer. Each queue is independent: its own read loop, its own slots, its own lease
/// renewal. A message takes a slot when read and frees it when settled, and the loop reads again as soon as a slot is
/// free, so one slow message never holds up the others and a busy queue never holds up another queue.
/// <para>
/// <b>Lease.</b> A read message is hidden from other readers for <see cref="PgmqOptions.Lease"/>, renewed every
/// <see cref="PgmqOptions.RenewEvery"/> while its consumer runs. A process that dies gives its messages back within
/// one lease; a live one keeps them however long they take.
/// </para>
/// <para>
/// <b>Outcome.</b> The consumer runs under its timeout. Success flushes its outbox, then deletes the message. A
/// transient failure (<see cref="PgmqOptions.IsTransient"/>) makes it visible again after a backoff, up to
/// <see cref="PgmqOptions.MaxAttempts"/> reads; any other failure, a timeout, or the last attempt archives it to
/// <c>pgmq.a_{queue}</c>. The outcome is decided first and written to the queue after, with its own retries, so a
/// failed delete never runs a consumer again.
/// </para>
/// <para>
/// <b>Shutdown.</b> Reading stops at once. Running consumers get <see cref="PgmqOptions.DrainGrace"/> to finish, then
/// their token is cancelled and what stops on it goes straight back to the queue. One that still does not stop is
/// left to the host's shutdown timeout; its lease lapses and the message comes back.
/// </para>
/// <para>
/// <b>Survival.</b> A read loop that fails is logged and started again; it never ends while the host runs. Every loop
/// records when it last ran, and <see cref="PgmqStatus.StalledQueues"/> names those that went quiet.
/// </para>
/// <para>
/// <b>FIFO.</b> A queue read in a grouped <see cref="PgmqReadMode"/> may get several messages of one group in one
/// read. They run one after another, in order; when one does not succeed, the rest go back to the queue unhandled,
/// and pgmq holds the group until the failed one is settled.
/// </para>
/// <para>
/// <b>At most once.</b> A consumer with <see cref="ConsumerOptionsAttribute.AtMostOnce"/> reads with <c>pgmq.pop</c>:
/// the message is gone from the queue once read, so there is no lease to renew and nothing to settle.
/// </para>
/// <para>
/// <b>Waking.</b> An idle queue with live insert notifications (LISTEN/NOTIFY) waits for one, and still reads again
/// after <see cref="PgmqOptions.MaxIdleDelay"/> at the latest in case a notification was throttled or lost. When its
/// notifications are not live (turned off, not set up, or the listener is reconnecting) it long-polls in the database
/// for up to <see cref="PgmqOptions.LongPollTimeout"/>. A queue that cannot long-poll either (an at-most-once consumer,
/// or <see cref="PgmqOptions.LongPollTimeout"/> null) polls from here, backing off to
/// <see cref="PgmqOptions.MaxIdleDelay"/>.
/// </para>
/// </summary>
internal sealed class ConsumerWorker(
    PgmqQueueManager queues,
    QueueReader reader,
    InsertListener listener,
    ConsumerRegistry registry,
    PgmqPublisher publisher,
    IServiceScopeFactory scopeFactory,
    PgmqOptions options,
    PgmqStatus status,
    ILogger<ConsumerWorker> logger
) : BackgroundService
{
    private static readonly TimeSpan RestartDelay = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan BeatEvery = TimeSpan.FromSeconds(10);

    private static readonly TimeSpan[] SettleRetries =
        [TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5)];

    /// <summary>Cancelled <see cref="PgmqOptions.DrainGrace"/> after shutdown starts: the token running consumers see.</summary>
    private readonly CancellationTokenSource _abort = new();

    private readonly ConcurrentDictionary<string, QueueState> _queues = new();

    /// <summary>The host's stopping token, once running: a FIFO chain stops starting messages when it fires.</summary>
    private CancellationToken _stopping;

    private int LeaseSeconds => Math.Max(1, (int)Math.Ceiling(options.Lease.TotalSeconds));

    /// <summary>The queues whose read loop has not run for <see cref="PgmqOptions.StallAfter"/> while the host is up.</summary>
    public IReadOnlyList<string> Stalled()
    {
        long now = Stopwatch.GetTimestamp();
        return
        [
            .. _queues.Values
                .Where(q => !q.Stopped && Stopwatch.GetElapsedTime(Volatile.Read(ref q.LastBeat), now) > options.StallAfter)
                .Select(q => q.Consumer.QueueName)
                .Order(StringComparer.Ordinal)
        ];
    }

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        status.ReportStalledFrom(Stalled);
        return base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!status.IsReady)
        {
            logger.LogWarning("Pgmq is not available ({Failure}); its worker stays idle", status.Failure);
            return;
        }

        _stopping = stoppingToken;
        await using CancellationTokenRegistration onStop =
            stoppingToken.Register(() => _abort.CancelAfter(options.DrainGrace));

        foreach (ConsumerRegistration consumer in registry.Consumers)
        {
            _queues[consumer.QueueName] = new(consumer);
        }

        List<Task> running =
        [
            .. registry.Consumers.Select(consumer =>
                Task.Run(() => RunQueueAsync(_queues[consumer.QueueName], stoppingToken), CancellationToken.None)),
        ];

        Dictionary<string, Action> notified = registry.Consumers
            .Where(c => listener.IsEnabled(c.QueueName))
            .ToDictionary(c => c.QueueName, c => (Action)_queues[c.QueueName].Wake, StringComparer.Ordinal);
        if (notified.Count > 0)
        {
            running.Add(Task.Run(() => listener.RunAsync(notified, stoppingToken), CancellationToken.None));
        }

        await Task.WhenAll(running);
    }

    public override void Dispose()
    {
        _abort.Dispose();
        base.Dispose();
    }

    /// <summary>One queue for the life of the host: its read loop, kept alive, its lease renewal, and the drain.</summary>
    private async Task RunQueueAsync(QueueState state, CancellationToken stoppingToken)
    {
        ConsumerRegistration consumer = state.Consumer;
        using CancellationTokenSource renewalStop = new();
        Task renewal = RenewLeasesAsync(state, renewalStop.Token);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ReadLoopAsync(state, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                logger.LogCritical(ex, "Read loop for {Queue} failed; starting it again", consumer.QueueName);
                await DelayQuietlyAsync(RestartDelay, stoppingToken);
            }
        }

        // Shutting down: nothing new is read. Wait for what runs; it got the drain grace before its token cancels.
        state.Stopped = true;
        await Task.WhenAll(state.InFlight.Keys);
        await renewalStop.CancelAsync();
        await renewal;
    }

    private async Task ReadLoopAsync(QueueState state, CancellationToken stoppingToken)
    {
        string queue = state.Consumer.QueueName;
        TimeSpan idleDelay = options.MinIdleDelay;

        while (!stoppingToken.IsCancellationRequested)
        {
            state.Beat();

            // Wait for one free slot, then take every other one that is free, and read that many. While every slot is
            // busy the loop is still alive, so it keeps beating.
            while (!await state.Slots.WaitAsync(BeatEvery, stoppingToken))
            {
                state.Beat();
            }

            int free = 1;
            while (free < state.Consumer.Concurrency && state.Slots.Wait(0))
            {
                free++;
            }

            // Notifications first; long polling only while they are not live.
            bool longPoll = !listener.IsLive(queue) && reader.CanLongPoll(state.Consumer);

            List<QueueMessage> messages;
            try
            {
                messages = await reader.ReadAsync(state.Consumer, LeaseSeconds, free, longPoll, stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !stoppingToken.IsCancellationRequested)
            {
                state.Slots.Release(free);
                logger.LogError(ex, "Reading {Queue} failed; trying again in {Delay}", queue, options.MaxIdleDelay);
                await Task.Delay(options.MaxIdleDelay, stoppingToken);
                continue;
            }
            catch
            {
                state.Slots.Release(free);
                throw;
            }

            if (messages.Count < free)
            {
                state.Slots.Release(free - messages.Count);
            }

            if (messages.Count == 0)
            {
                if (longPoll)
                {
                    // The database already waited for a message.
                    continue;
                }

                if (await state.Woken.WaitAsync(idleDelay, stoppingToken))
                {
                    idleDelay = options.MinIdleDelay;
                }
                else
                {
                    idleDelay = TimeSpan.FromTicks(Math.Min(idleDelay.Ticks * 2, options.MaxIdleDelay.Ticks));
                }

                continue;
            }

            idleDelay = options.MinIdleDelay;

            if (state.Consumer.IsGrouped)
            {
                // One chain per FIFO group, in message order, so a group never runs two of its messages at once.
                foreach (IGrouping<string?, QueueMessage> group in messages.GroupBy(m => m.GroupId))
                {
                    Start(state, [.. group]);
                }
            }
            else
            {
                foreach (QueueMessage message in messages)
                {
                    Start(state, [message]);
                }
            }
        }
    }

    /// <summary>
    /// Runs messages one after another on their own task, freeing each one's slot when it is settled, whatever
    /// happened. Once one does not succeed, or the host is stopping, the rest go back to the queue unhandled.
    /// </summary>
    private void Start(QueueState state, QueueMessage[] chain)
    {
        if (!state.Consumer.AtMostOnce)
        {
            foreach (QueueMessage message in chain)
            {
                state.Leased[message.MsgId] = 0;
            }
        }

        Task handling = Task.Run(async () =>
        {
            int next = 0;
            bool carryOn = true;

            for (; next < chain.Length && carryOn; next++)
            {
                QueueMessage message = chain[next];
                try
                {
                    carryOn = await HandleAsync(state, message) == SettlementKind.Delete;
                }
                catch (Exception ex)
                {
                    // HandleAsync settles every outcome itself; reaching here is a bug in it. The lease lapses and
                    // the message comes back, so nothing is lost.
                    carryOn = false;
                    logger.LogCritical(ex, "Handling message {MsgId} on {Queue} failed unexpectedly", message.MsgId,
                        state.Consumer.QueueName);
                }
                finally
                {
                    state.Leased.TryRemove(message.MsgId, out _);
                    state.Slots.Release();
                }

                carryOn &= !_stopping.IsCancellationRequested;
            }

            for (; next < chain.Length; next++)
            {
                QueueMessage message = chain[next];
                state.Leased.TryRemove(message.MsgId, out _);
                await SettleAsync(state, message, Settlement.Release);
                state.Slots.Release();
            }
        }, CancellationToken.None);

        state.InFlight.TryAdd(handling, 0);
        handling.ContinueWith(done => state.InFlight.TryRemove(done, out _), TaskScheduler.Default);
    }

    /// <summary>Handles one message to its end, and says how it was settled.</summary>
    private async Task<SettlementKind> HandleAsync(QueueState state, QueueMessage message)
    {
        ConsumerRegistration consumer = state.Consumer;
        MessageHeaders? headers = MessageHeaders.From(message.Headers);
        Type? messageType = headers is null ? null : registry.Topology.ResolveMessageType(headers.MessageType);

        using IDisposable? scope = logger.BeginScope(new Dictionary<string, object?>
        {
            ["Queue"] = consumer.QueueName,
            ["PgmqMsgId"] = message.MsgId,
            ["MessageType"] = headers?.MessageType,
            ["MessageId"] = headers?.MessageId,
            ["Attempt"] = message.ReadCt,
        });

        if (headers is null || messageType is null || message.Body is null)
        {
            logger.LogError("Message on {Queue} has no known type ({MessageType}); archiving it",
                consumer.QueueName, headers?.MessageType);
            state.Leased.TryRemove(message.MsgId, out _);
            await SettleAsync(state, message, Settlement.Archive);

            // Archiving takes it out of its group, so the group's next message may run.
            return SettlementKind.Delete;
        }

        using Activity? activity =
            PgmqTelemetry.StartProcess(headers, consumer.QueueName, message.MsgId, message.ReadCt);

        Settlement settlement = await RunConsumerAsync(state, message, headers, messageType, activity);

        // Decided; from here only the queue is written. Stop renewing first, so a settle that cannot be written leaves
        // the message to come back within one lease rather than sit hidden.
        state.Leased.TryRemove(message.MsgId, out _);
        await SettleAsync(state, message, settlement);
        return settlement.Kind;
    }

    /// <summary>Runs the consumer under its deadline and flushes its outbox, and says what to do with the message.</summary>
    private async Task<Settlement> RunConsumerAsync(QueueState state, QueueMessage message,
        MessageHeaders headers, Type messageType, Activity? activity)
    {
        ConsumerRegistration consumer = state.Consumer;
        TimeSpan timeout = consumer.Timeout;
        using CancellationTokenSource deadline = CancellationTokenSource.CreateLinkedTokenSource(_abort.Token);
        deadline.CancelAfter(timeout);

        Outbox outbox = new();
        Task run = Task.Run(() => DispatchAsync(consumer, message, headers, messageType, outbox, deadline.Token),
            CancellationToken.None);

        try
        {
            // Normally the consumer returns, or throws once its token cancels. One that ignores the token is waited for
            // AbandonAfter longer, then left to finish on its own. On shutdown one that ignores it keeps the drain
            // waiting until the host gives up; its lease then lapses and the message comes back.
            await run.WaitAsync(timeout + options.AbandonAfter);
        }
        catch (TimeoutException ex) when (!run.IsCompleted)
        {
            _ = run.ContinueWith(t => _ = t.Exception, TaskContinuationOptions.OnlyOnFaulted);
            logger.LogError(ex, "{Consumer} ignored its timeout of {Timeout}; abandoned it and archiving the message",
                consumer.ConsumerType.Name, timeout);
            PgmqTelemetry.Fail(activity, ex, "abandoned");
            return Settlement.Archive;
        }
        catch (OperationCanceledException) when (_abort.IsCancellationRequested)
        {
            if (consumer.AtMostOnce)
            {
                logger.LogWarning("{Consumer} stopped for shutdown; its at-most-once message is lost",
                    consumer.ConsumerType.Name);
            }
            else
            {
                logger.LogInformation("{Consumer} stopped for shutdown; its message goes back to the queue",
                    consumer.ConsumerType.Name);
            }

            return Settlement.Release;
        }
        catch (OperationCanceledException ex) when (deadline.IsCancellationRequested)
        {
            logger.LogError(ex, "{Consumer} ran past its timeout of {Timeout}; archiving the message",
                consumer.ConsumerType.Name, timeout);
            PgmqTelemetry.Fail(activity, ex, "timeout");
            return Settlement.Archive;
        }
        catch (Exception ex)
        {
            return Failed(consumer, message, messageType, ex, activity);
        }

        try
        {
            await RetryAsync(outbox.FlushAsync, "flush the outbox of", state, message);
        }
        catch (Exception ex)
        {
            // The consumer's work is done but some of what it published is not out. Running it again is the only way
            // to send the rest, so treat it as transient; what already went out may then go twice.
            logger.LogError(ex, "{Consumer} succeeded but {Count} of its publishes did not go out",
                consumer.ConsumerType.Name, outbox.Count);
            PgmqTelemetry.Fail(activity, ex, "outbox");
            return message.ReadCt >= options.MaxAttempts
                ? Settlement.Archive
                : Settlement.RetryAfter(Backoff(message.ReadCt));
        }

        activity?.SetTag("messaging.outcome", "success");
        return Settlement.Delete;
    }

    private async Task DispatchAsync(ConsumerRegistration consumer, QueueMessage message,
        MessageHeaders headers, Type messageType, Outbox outbox, CancellationToken cancellationToken)
    {
        object body = MessageSerializer.Deserialize(message.Body!, messageType);

        // A failed attempt counts as a redelivery, as does an explicit RedeliverAsync (which writes a new message with
        // the count in its headers, so its reads start again from one).
        Delivery delivery = new(publisher, headers.MessageId,
            RedeliveryCount: headers.RedeliveryCount + Math.Max(0, message.ReadCt - 1),
            outbox,
            SendBack: (m, type, delay, count, messageId, groupId, custom, ct) =>
                publisher.SendAsync(consumer.QueueName, m, type, delay, count, messageId, groupId, custom, ct))
        {
            GroupId = headers.GroupId,
            Headers = headers.CustomHeaders,
        };

        await using AsyncServiceScope scope = scopeFactory.CreateAsyncScope();
        await ConsumerDispatcher.DispatchAsync(scope.ServiceProvider, consumer, body, messageType, delivery,
            cancellationToken);
    }

    private Settlement Failed(ConsumerRegistration consumer, QueueMessage message, Type messageType,
        Exception ex, Activity? activity)
    {
        if (consumer.AtMostOnce)
        {
            logger.LogError(ex, "{Consumer} failed at-most-once {MessageType}; the message is lost",
                consumer.ConsumerType.Name, messageType.Name);
            PgmqTelemetry.Fail(activity, ex, "lost");
            return Settlement.Archive;
        }

        if (!options.IsTransient(ex) || message.ReadCt >= options.MaxAttempts)
        {
            logger.LogError(ex, "{Consumer} failed {MessageType} (attempt {Attempt}); archiving the message",
                consumer.ConsumerType.Name, messageType.Name, message.ReadCt);
            PgmqTelemetry.Fail(activity, ex, "archived");
            return Settlement.Archive;
        }

        TimeSpan backoff = Backoff(message.ReadCt);
        logger.LogWarning(ex, "{Consumer} failed {MessageType} (attempt {Attempt}); retrying in {Delay}",
            consumer.ConsumerType.Name, messageType.Name, message.ReadCt, backoff);
        PgmqTelemetry.Fail(activity, ex, "retry");
        return Settlement.RetryAfter(backoff);
    }

    /// <summary>By default 30s, 2m, 8m, 30m.</summary>
    private TimeSpan Backoff(int attempt)
        => TimeSpan.FromTicks((long)Math.Min(options.RetryBackoff.Ticks * Math.Pow(4, Math.Max(0, attempt - 1)),
            options.MaxRetryBackoff.Ticks));

    /// <summary>Writes the outcome to the queue, retrying a failed write; a write that never lands leaves the lease to lapse.</summary>
    private async Task SettleAsync(QueueState state, QueueMessage message, Settlement settlement)
    {
        string queue = state.Consumer.QueueName;

        if (state.Consumer.AtMostOnce)
        {
            // pgmq.pop deleted it as it was read; there is nothing left to settle.
            return;
        }

        try
        {
            await RetryAsync(ct => settlement.Kind switch
            {
                SettlementKind.Delete => queues.DeleteMessageAsync(queue, message.MsgId, ct),
                SettlementKind.Archive => queues.ArchiveMessageAsync(queue, message.MsgId, ct),
                SettlementKind.Retry => queues.SetVisibilityTimeoutAsync(queue, message.MsgId,
                    TimeSpan.FromSeconds(Math.Max(1, (int)Math.Ceiling(settlement.Delay.TotalSeconds))), ct),
                SettlementKind.Release => queues.SetVisibilityTimeoutAsync(queue, message.MsgId, TimeSpan.Zero, ct),
                _ => Task.CompletedTask,
            }, settlement.Kind.ToString().ToLowerInvariant(), state, message);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Could not {Settlement} message {MsgId} on {Queue}; it comes back after the lease",
                settlement.Kind, message.MsgId, queue);
        }
    }

    private async Task RetryAsync(Func<CancellationToken, Task> write, string what, QueueState state,
        QueueMessage message)
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                await write(CancellationToken.None);
                return;
            }
            catch (Exception ex) when (attempt < SettleRetries.Length)
            {
                logger.LogWarning(ex, "Could not {What} message {MsgId} on {Queue}; trying again", what,
                    message.MsgId, state.Consumer.QueueName);
                await Task.Delay(SettleRetries[attempt]);
            }
        }
    }

    /// <summary>Keeps every running message of a queue hidden, one batch call per queue per renewal.</summary>
    private async Task RenewLeasesAsync(QueueState state, CancellationToken stop)
    {
        while (!stop.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(options.RenewEvery, stop);

                long[] leased = [.. state.Leased.Keys];
                if (leased.Length > 0)
                {
                    await queues.SetVisibilityTimeoutAsync(state.Consumer.QueueName, leased,
                        TimeSpan.FromSeconds(LeaseSeconds), CancellationToken.None);
                }
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                // One missed renewal is covered: the lease is three renewals long.
                logger.LogWarning(ex, "Could not renew leases on {Queue}", state.Consumer.QueueName);
            }
        }
    }

    private static async Task DelayQuietlyAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(delay, cancellationToken);
        }
        catch (OperationCanceledException)
        {
        }
    }

    private sealed class QueueState(ConsumerRegistration consumer)
    {
        public long LastBeat = Stopwatch.GetTimestamp();
        public volatile bool Stopped;

        public ConsumerRegistration Consumer { get; } = consumer;

        public SemaphoreSlim Slots { get; } = new(consumer.Concurrency, consumer.Concurrency);

        /// <summary>Messages whose lease is renewed: read, and not yet settled.</summary>
        public ConcurrentDictionary<long, byte> Leased { get; } = new();

        /// <summary>Every running handler, awaited on shutdown.</summary>
        public ConcurrentDictionary<Task, byte> InFlight { get; } = new();

        /// <summary>Released by an insert notification, to end an idle wait early.</summary>
        public SemaphoreSlim Woken { get; } = new(0, 1);

        public void Beat() => Volatile.Write(ref LastBeat, Stopwatch.GetTimestamp());

        public void Wake()
        {
            try
            {
                Woken.Release();
            }
            catch (SemaphoreFullException)
            {
                // Already woken; one wake-up is enough.
            }
        }
    }

    private enum SettlementKind
    {
        Delete,
        Archive,
        Retry,
        Release,
    }

    private readonly record struct Settlement(SettlementKind Kind, TimeSpan Delay)
    {
        public static readonly Settlement Delete = new(SettlementKind.Delete, TimeSpan.Zero);
        public static readonly Settlement Archive = new(SettlementKind.Archive, TimeSpan.Zero);
        public static readonly Settlement Release = new(SettlementKind.Release, TimeSpan.Zero);

        public static Settlement RetryAfter(TimeSpan delay) => new(SettlementKind.Retry, delay);
    }
}
