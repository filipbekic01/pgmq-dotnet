using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

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
/// <b>Outcome.</b> The consumer runs under its timeout. Success sends its outbox and deletes the message in one
/// transaction, so what it published goes out exactly when the message is done. A failure or a timeout archives it to
/// <c>pgmq.a_{queue}</c>; a consumer is never run again because it failed. A message read more than <see cref="PgmqOptions.MaxAttempts"/> times without settling (its
/// consumer keeps taking the process down) is archived without running. The outcome is decided first and written to the queue after, with its own retries
/// (some two minutes of them, the message keeping its lease meanwhile), so a failed delete does not run a consumer
/// again unless the database stays away longer than that.
/// </para>
/// <para>
/// <b>Shutdown.</b> Reading stops at once, and a message read but not yet started goes back to the queue. Running
/// consumers get <see cref="PgmqOptions.DrainGrace"/> to finish (less, if the host's shutdown timeout runs out first),
/// then their token is cancelled and what stops on it, with whatever exception, goes straight back to the queue. One
/// that still does not stop is left to the host's shutdown timeout; its lease lapses and the message comes back.
/// </para>
/// <para>
/// <b>Survival.</b> A read loop that fails is logged and started again; it never ends while the host runs. Every loop
/// records when it last ran, and <see cref="PgmqStatus.StalledQueues"/> names those that went quiet.
/// </para>
/// <para>
/// <b>FIFO.</b> A queue read in a grouped <see cref="PgmqReadMode"/> may get several messages of one group in one
/// read. They run one after another, in order; when one does not succeed, the rest go back to the queue unhandled
/// and without the read counting against their <see cref="PgmqOptions.MaxAttempts"/>, and pgmq holds the group until
/// the failed one is settled.
/// </para>
/// <para>
/// <b>At most once.</b> A consumer with <see cref="ConsumerOptionsAttribute.AtMostOnce"/> reads with <c>pgmq.pop</c>:
/// the message is gone from the queue once read, so there is no lease to renew and nothing to settle.
/// </para>
/// <para>
/// <b>Waking.</b> An idle queue never goes to the database on its own. Its read loop reads only when woken: by an
/// insert notification (LISTEN/NOTIFY), by the <see cref="QueueSweeper"/>, which finds every queue with a visible
/// message in one query, or because its last read found messages, so more are likely. Otherwise it waits. An idle host
/// therefore holds two connections (the listener and the sweeper's) whatever the number of queues, and a busy one
/// reads through at most <see cref="PgmqOptions.MaxConcurrentReads"/> connections at once, and renews its leases on
/// one more, outside the pool.
/// </para>
/// </summary>
internal sealed class ConsumerWorker(
    PgmqQueueManager queues,
    QueueReader reader,
    InsertListener listener,
    QueueSweeper sweeper,
    ConsumerRegistry registry,
    PgmqPublisher publisher,
    IServiceScopeFactory scopeFactory,
    PgmqOptions options,
    PgmqStatus status,
    PgmqDatabase database,
    ILogger<ConsumerWorker> logger
) : BackgroundService
{
    private static readonly TimeSpan RestartDelay = TimeSpan.FromSeconds(5);

    /// <summary>How long a stop the host gave up on still waits for cancelled consumers to put their messages back.</summary>
    private static readonly TimeSpan ReleaseWindow = TimeSpan.FromSeconds(5);

    /// <summary>The longest a message of a type this host does not know waits before it is read again.</summary>
    private static readonly TimeSpan MaxUnknownTypeDelay = TimeSpan.FromMinutes(5);

    /// <summary>
    /// The waits between tries of a queue write: some two minutes in all, long enough for a database failover. The
    /// message keeps its lease meanwhile, so nobody else reads it.
    /// </summary>
    private static readonly TimeSpan[] SettleRetries =
    [
        TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(10),
        TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30),
    ];

    /// <summary>How many of <see cref="SettleRetries"/> a write still gets once shutdown cancelled the consumers.</summary>
    private const int SettleRetriesOnShutdown = 3;

    /// <summary>Cancelled <see cref="PgmqOptions.DrainGrace"/> after shutdown starts: the token running consumers see.</summary>
    private readonly CancellationTokenSource _abort = new();

    private readonly ConcurrentDictionary<string, QueueState> _queues = new();

    /// <summary>
    /// Held by a lease renewal from its snapshot of leased messages to its write. A release waits for the renewal in
    /// flight, so a renewal that saw the message before it was settled never writes over the release.
    /// </summary>
    private readonly SemaphoreSlim _renewing = new(1, 1);

    /// <summary>The host's stopping token, once running: a FIFO chain stops starting messages when it fires.</summary>
    private CancellationToken _stopping;

    /// <summary>
    /// The lease renewals' own connection, outside the pool, so a renewal never waits behind the application for a pool
    /// slot while leases run out. Open only while messages are leased; used by the renewal task alone.
    /// </summary>
    private NpgsqlConnection? _renewalConnection;

    private int LeaseSeconds => Math.Max(1, (int)Math.Ceiling(options.Lease.TotalSeconds));

    /// <summary>
    /// How long one try of a renewal may take, to connect and to run: half a <see cref="PgmqOptions.RenewEvery"/>, so
    /// two tries on a database that does not answer still end within one.
    /// </summary>
    private int RenewTimeoutSeconds => Math.Max(1, (int)Math.Ceiling(options.RenewEvery.TotalSeconds / 2));

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

        // The listener and the sweeper come first. The read loops start waiting, and the first sweep wakes only the
        // queues that have messages: starting costs one query, not one read per queue.
        List<Task> background = [];

        Dictionary<string, Action> notified = registry.Consumers
            .Where(c => listener.IsEnabled(c.QueueName))
            .ToDictionary(c => c.QueueName, c => (Action)_queues[c.QueueName].Wake, StringComparer.Ordinal);
        if (notified.Count > 0)
        {
            background.Add(Task.Run(() => KeepRunningAsync("The insert listener",
                () => listener.RunAsync(notified, sweeper.Nudge, options.InsertNotificationThrottle, stoppingToken),
                stoppingToken), CancellationToken.None));
        }

        SweptQueue[] swept = [.. registry.Consumers.Select(c => new SweptQueue(c, _queues[c.QueueName].Wake))];
        background.Add(Task.Run(() => KeepRunningAsync("The sweeper", () => sweeper.RunAsync(swept, stoppingToken),
            stoppingToken), CancellationToken.None));

        using CancellationTokenSource renewalStop = new();
        Task renewal = Task.Run(() => KeepRunningAsync("Lease renewal", () => RenewLeasesAsync(renewalStop.Token),
            renewalStop.Token), CancellationToken.None);

        await Task.WhenAll(registry.Consumers.Select(consumer =>
            Task.Run(() => RunQueueAsync(_queues[consumer.QueueName], stoppingToken), CancellationToken.None)));

        // Every queue drained: nothing is leased any more.
        await renewalStop.CancelAsync();
        await renewal;
        await Task.WhenAll(background);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);

        // The base returns early only when the host's shutdown timeout ran out. If that came before the drain grace,
        // cancel the consumers now, so what stops on its token still goes back to the queue instead of waiting out its
        // lease.
        if (cancellationToken.IsCancellationRequested && ExecuteTask is { IsCompleted: false } running)
        {
            await _abort.CancelAsync();

            // Returning now would let the host dispose the data source, or exit, before those messages are written
            // back. Wait a little for the drain; a consumer that ignores its token is still left to its lease.
            try
            {
                await running.WaitAsync(ReleaseWindow, CancellationToken.None);
            }
            catch (TimeoutException)
            {
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    public override void Dispose()
    {
        // _abort is cancelled, not disposed: a consumer the host gave up on may still be running, and its token comes
        // from it. A callback a consumer registered on that token must not break the container's disposal.
        try
        {
            _abort.Cancel();
        }
        catch (AggregateException ex)
        {
            logger.LogWarning(ex, "A consumer's cancellation callback failed while the worker was disposed");
        }

        base.Dispose();
    }

    /// <summary>
    /// Runs one of the worker's background tasks until <paramref name="stop"/>, starting it again when it fails. Each
    /// of them handles its own failures, so this only catches what none of them foresaw; without it, such a failure
    /// would end the task unnoticed, and leases would stop being renewed or delayed messages stop being found.
    /// </summary>
    private async Task KeepRunningAsync(string what, Func<Task> run, CancellationToken stop)
    {
        while (true)
        {
            try
            {
                await run();
                return;
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                logger.LogCritical(ex, "{Task} failed; starting it again", what);
                PgmqTelemetry.BackgroundRestarts.Add(1, new KeyValuePair<string, object?>("pgmq.task", what));
            }

            await DelayQuietlyAsync(RestartDelay, stop);
            if (stop.IsCancellationRequested)
            {
                return;
            }
        }
    }

    /// <summary>One queue for the life of the host: its read loop, kept alive, and the drain.</summary>
    private async Task RunQueueAsync(QueueState state, CancellationToken stoppingToken)
    {
        ConsumerRegistration consumer = state.Consumer;
        bool restarted = false;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // A loop started again reads at once, in case it failed with work waiting.
                await ReadLoopAsync(state, readFirst: restarted, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                logger.LogCritical(ex, "Read loop for {Queue} failed; starting it again", consumer.QueueName);
                PgmqTelemetry.ReadLoopRestarts.Add(1,
                    PgmqTelemetry.ConsumerTags(consumer.QueueName, consumer.ConsumerType.Name));
                await DelayQuietlyAsync(RestartDelay, stoppingToken);
                restarted = true;
            }
        }

        // Shutting down: nothing new is read. Wait for what runs; it got the drain grace before its token cancels.
        state.Stopped = true;
        await Task.WhenAll(state.InFlight.Keys);
    }

    /// <summary>
    /// Reads only when there is reason to: woken by a notification or the sweeper, or right after a read that found
    /// messages. An idle queue waits here and costs the database nothing.
    /// </summary>
    private async Task ReadLoopAsync(QueueState state, bool readFirst, CancellationToken stoppingToken)
    {
        string queue = state.Consumer.QueueName;
        TimeSpan heartbeat = options.Heartbeat;
        bool readNext = readFirst;

        while (!stoppingToken.IsCancellationRequested)
        {
            state.Beat();

            // Nothing suggests work: wait to be woken. The loop is alive while it waits, so it keeps beating.
            if (!readNext)
            {
                while (!await state.Woken.WaitAsync(heartbeat, stoppingToken))
                {
                    state.Beat();
                }

                state.Beat();
            }

            // Wait for one free slot, then take every other one that is free, and read that many. While every slot is
            // busy the loop is still alive, so it keeps beating.
            while (!await state.Slots.WaitAsync(heartbeat, stoppingToken))
            {
                state.Beat();
            }

            int free = 1;
            while (free < state.Consumer.Concurrency && state.Slots.Wait(0))
            {
                free++;
            }

            List<QueueMessage> messages;
            try
            {
                messages = await reader.ReadAsync(state.Consumer, LeaseSeconds, free, state.Beat, stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !stoppingToken.IsCancellationRequested)
            {
                state.Slots.Release(free);
                logger.LogError(ex, "Reading {Queue} failed; trying again in {Delay}", queue, options.MaxIdleDelay);
                TagList failed = state.Tags;
                failed.Add("error.type", PgmqTelemetry.ErrorType(ex));
                PgmqTelemetry.ReadFailures.Add(1, failed);
                await Task.Delay(options.MaxIdleDelay, stoppingToken);
                readNext = true;
                continue;
            }
            catch
            {
                state.Slots.Release(free);
                throw;
            }

            PgmqTelemetry.Reads.Add(1, state.Tags);

            if (messages.Count < free)
            {
                state.Slots.Release(free - messages.Count);
            }

            // A read that found messages is likely followed by more: read again as soon as a slot is free. One that
            // found none waits to be woken.
            readNext = messages.Count > 0;
            if (messages.Count == 0)
            {
                continue;
            }

            if (PgmqTelemetry.ConsumedMessages.Enabled)
            {
                TagList consumed = state.Tags;
                consumed.Add("messaging.operation.name", state.Consumer.AtMostOnce ? "pop" : "read");
                PgmqTelemetry.ConsumedMessages.Add(messages.Count, consumed);
            }

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
    /// happened. Once one does not succeed, or the host is stopping, the rest go back to the queue unhandled. An
    /// at-most-once message is already off the queue, so it runs even while the host stops: its consumer gets the
    /// drain grace like any running one.
    /// </summary>
    private void Start(QueueState state, QueueMessage[] chain)
    {
        bool atMostOnce = state.Consumer.AtMostOnce;
        LeaseEntry?[] leases = new LeaseEntry?[chain.Length];
        if (!atMostOnce)
        {
            for (int i = 0; i < chain.Length; i++)
            {
                state.Leased[chain[i].MsgId] = leases[i] = new(chain[i].ReadCt);
            }
        }

        Task handling = Task.Run(async () =>
        {
            int next = 0;
            bool carryOn = true;

            // Checked before each message, so one read just before shutdown goes back instead of starting.
            for (; next < chain.Length && carryOn && (atMostOnce || !_stopping.IsCancellationRequested); next++)
            {
                QueueMessage message = chain[next];
                try
                {
                    carryOn = await HandleAsync(state, message, leases[next]) == SettlementKind.Delete;
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
                    Unlease(state, message, leases[next]);
                    state.Slots.Release();
                }
            }

            // Never handed to a consumer: back to the queue, with the read they were counted for taken back.
            for (; next < chain.Length; next++)
            {
                QueueMessage message = chain[next];
                try
                {
                    Unlease(state, message, leases[next]);
                    await SettleAsync(state, message, Settlement.Unstarted, leases[next]);
                }
                catch (Exception ex)
                {
                    logger.LogCritical(ex, "Putting message {MsgId} back on {Queue} failed unexpectedly",
                        message.MsgId, state.Consumer.QueueName);
                }
                finally
                {
                    state.Slots.Release();
                }
            }
        }, CancellationToken.None);

        state.InFlight.TryAdd(handling, 0);
        handling.ContinueWith(done => state.InFlight.TryRemove(done, out _), TaskScheduler.Default);
    }

    /// <summary>
    /// Stops renewing a message's lease. Only its own entry is removed: if the lease lapsed and this host read the
    /// message again meanwhile, the entry there now belongs to that read, which still needs it.
    /// </summary>
    private static void Unlease(QueueState state, QueueMessage message, LeaseEntry? lease)
    {
        if (lease is not null)
        {
            state.Leased.TryRemove(new KeyValuePair<long, LeaseEntry>(message.MsgId, lease));
        }
    }

    /// <summary>Handles one message to its end, and says how it was settled.</summary>
    private async Task<SettlementKind> HandleAsync(QueueState state, QueueMessage message, LeaseEntry? lease)
    {
        ConsumerRegistration consumer = state.Consumer;
        string consumerName = consumer.ConsumerType.Name;
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

        long start = Stopwatch.GetTimestamp();
        using Activity? activity = PgmqTelemetry.StartProcess(consumer.QueueName, consumerName, headers,
            message.MsgId, message.ReadCt, database.Server);

        bool known = headers is not null && messageType is not null && message.Body is not null;

        // Headers and a body, but a type this host cannot resolve: most likely an instance on a newer version published
        // it during a rolling deploy. It goes back to the queue for a while, so an instance that knows it can take it.
        bool unresolved = !known && headers is not null && message.Body is not null && !consumer.AtMostOnce;

        // A failed attempt is settled, and the last one archived, so a message is read more than MaxAttempts times only
        // when attempts never settled: its consumer kept taking the process down, or it never came back from one.
        bool exhausted = !consumer.AtMostOnce && message.ReadCt > options.MaxAttempts;
        Outcome outcome;

        PgmqTelemetry.ActiveMessages.Add(1, state.Tags);
        try
        {
            if (exhausted)
            {
                logger.LogError(
                    "Message {MsgId} on {Queue} was read {Reads} times without finishing, more than {MaxAttempts} " +
                    "attempts; its consumer may be crashing the process. Archiving it without running it",
                    message.MsgId, consumer.QueueName, message.ReadCt, options.MaxAttempts);
                outcome = Outcome.Archived("pgmq.attempts_exhausted");
            }
            else if (known)
            {
                outcome = await RunConsumerAsync(state, message, headers!, messageType!, lease);
            }
            else if (unresolved && message.ReadCt < options.MaxAttempts)
            {
                TimeSpan delay = UnknownTypeDelay(message.ReadCt);
                logger.LogWarning("Message on {Queue} has a type this host does not know ({MessageType}); putting it " +
                                  "back for {Delay} (read {Reads} of {MaxAttempts}) in case another instance knows it",
                    consumer.QueueName, headers!.MessageType, delay, message.ReadCt, options.MaxAttempts);
                outcome = Outcome.Deferred("pgmq.unknown_message_type", delay);
            }
            else
            {
                logger.LogError("Message on {Queue} has no known type ({MessageType}); archiving it",
                    consumer.QueueName, headers?.MessageType);
                outcome = Outcome.Archived("pgmq.unknown_message_type");
            }
        }
        finally
        {
            PgmqTelemetry.ActiveMessages.Add(-1, state.Tags);
        }

        activity?.SetTag("messaging.pgmq.outcome", outcome.Name);
        if (outcome.ErrorType is { } errorType)
        {
            PgmqTelemetry.Fail(activity, errorType, outcome.Error);
        }

        PgmqTelemetry.RecordProcess(consumer.QueueName, consumerName, outcome.Name, outcome.ErrorType, start);

        // Decided; from here only the queue is written. A delete or an archive keeps the lease while it is tried, so a
        // database that is away for a while does not hand the message to another reader, who would run its consumer
        // again; the caller ends the lease once this returns. A release ends it first, or a renewal would hide the
        // message again.
        if (outcome.Settlement.Kind == SettlementKind.Release)
        {
            Unlease(state, message, lease);
        }
        else if (lease is not null)
        {
            lease.Settling = true;
        }

        await SettleAsync(state, message, outcome.Settlement, lease);

        // An unknown or exhausted message that was archived is out of its group, so the group's next message may run.
        // One put back for later still holds its group, in order.
        return (known && !exhausted) || outcome.Settlement.Kind == SettlementKind.Release
            ? outcome.Settlement.Kind
            : SettlementKind.Delete;
    }

    /// <summary>
    /// How long a message of an unknown type waits before it is read again: one lease, doubling with each read, at most
    /// five minutes. With the default <see cref="PgmqOptions.MaxAttempts"/> it is archived some 7.5 minutes after it was
    /// first read, time enough for a rolling deploy to bring up an instance that knows it.
    /// </summary>
    private TimeSpan UnknownTypeDelay(int reads)
    {
        double factor = Math.Pow(2, Math.Clamp(reads - 1, 0, 20));
        return TimeSpan.FromTicks((long)Math.Min(options.Lease.Ticks * factor, MaxUnknownTypeDelay.Ticks));
    }

    /// <summary>Runs the consumer under its deadline and flushes its outbox, and says what to do with the message.</summary>
    private async Task<Outcome> RunConsumerAsync(QueueState state, QueueMessage message,
        MessageHeaders headers, Type messageType, LeaseEntry? lease)
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
            return Outcome.Archived("pgmq.abandoned", ex);
        }
        catch (Exception ex) when (_abort.IsCancellationRequested)
        {
            // Cancelled for shutdown. Not every consumer ends on that with an OperationCanceledException: one that
            // wraps its exceptions, or calls a library with an error of its own for a cancelled call, throws something
            // else. Whatever it threw after its token was cancelled says nothing about the message, so it is not
            // archived for it.
            if (consumer.AtMostOnce)
            {
                logger.LogWarning(ex, "{Consumer} stopped for shutdown; its at-most-once message is lost",
                    consumer.ConsumerType.Name);
                return Outcome.Lost("pgmq.shutdown");
            }

            logger.LogInformation(ex, "{Consumer} stopped for shutdown; its message goes back to the queue",
                consumer.ConsumerType.Name);
            return Outcome.Released;
        }
        catch (OperationCanceledException ex) when (deadline.IsCancellationRequested)
        {
            logger.LogError(ex, "{Consumer} ran past its timeout of {Timeout}; archiving the message",
                consumer.ConsumerType.Name, timeout);
            return Outcome.Archived("timeout", ex);
        }
        catch (Exception ex)
        {
            return Failed(consumer, message, messageType, ex);
        }

        if (outbox.Count == 0)
        {
            return Outcome.Success;
        }

        // The commit deletes the message: from here a renewal that finds it gone has not lost its lease.
        if (lease is not null)
        {
            lease.Settling = true;
        }

        bool deleted = false;
        try
        {
            // Each try is a whole new transaction, so retrying it runs no consumer and sends nothing twice: a failed
            // try rolled back everything it sent.
            await RetryAsync(async ct => deleted = await CommitAsync(state, message, outbox, ct),
                "commit the outbox of", state, message, PgmqTransientFailures.IsTransient);
        }
        catch (Exception ex)
        {
            // Nothing the consumer published went out, and its message is still on the queue.
            if (consumer.AtMostOnce)
            {
                logger.LogError(ex, "{Consumer} succeeded but its {Count} publishes could not be sent; the " +
                                    "at-most-once message is lost", consumer.ConsumerType.Name, outbox.Count);
                return Outcome.Lost(PgmqTelemetry.ErrorType(ex), ex);
            }

            // Running it again is the only way to send them, and a consumer is never run again.
            logger.LogError(ex, "{Consumer} succeeded but its {Count} publishes could not be sent; archiving the " +
                                "message", consumer.ConsumerType.Name, outbox.Count);
            return Outcome.Archived(PgmqTelemetry.ErrorType(ex), ex);
        }

        if (consumer.AtMostOnce || deleted)
        {
            return Outcome.Committed;
        }

        // Someone else settled the message first (its lease lapsed and another read finished it, or it was deleted by
        // hand), or a commit that seemed to fail went through. Its publishes then went out already, or belong to that
        // other read; these were rolled back.
        logger.LogWarning("{Consumer} succeeded, but message {MsgId} was no longer on {Queue}; its {Count} publishes " +
                          "were not sent", consumer.ConsumerType.Name, message.MsgId, consumer.QueueName, outbox.Count);
        return Outcome.Gone;
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
            // The outbox flushes through this worker's publisher, bound to its transaction: always a PgmqPublisher.
            SendBack: (p, m, type, delay, count, messageId, groupId, custom, ct) =>
                ((PgmqPublisher)p).SendAsync(consumer.QueueName, m, type, delay, count, messageId, groupId, custom, ct))
        {
            GroupId = headers.GroupId,
            Headers = headers.CustomHeaders,
        };

        await using AsyncServiceScope scope = scopeFactory.CreateAsyncScope();
        await ConsumerDispatcher.DispatchAsync(scope.ServiceProvider, consumer, body, messageType, delivery,
            cancellationToken);
    }

    private Outcome Failed(ConsumerRegistration consumer, QueueMessage message, Type messageType, Exception ex)
    {
        if (consumer.AtMostOnce)
        {
            logger.LogError(ex, "{Consumer} failed at-most-once {MessageType}; the message is lost",
                consumer.ConsumerType.Name, messageType.Name);
            return Outcome.Lost(PgmqTelemetry.ErrorType(ex), ex);
        }

        logger.LogError(ex, "{Consumer} failed {MessageType} (attempt {Attempt}); archiving the message",
            consumer.ConsumerType.Name, messageType.Name, message.ReadCt);
        return Outcome.Archived(PgmqTelemetry.ErrorType(ex), ex);
    }

    /// <summary>
    /// Sends the outbox and deletes the message in one transaction. An at-most-once message was deleted as it was read,
    /// so only the sends are in it.
    /// </summary>
    /// <returns>False when the message was no longer on the queue; the transaction then rolled back, sending nothing.</returns>
    private async Task<bool> CommitAsync(QueueState state, QueueMessage message, Outbox outbox,
        CancellationToken cancellationToken)
    {
        await using NpgsqlConnection connection = await database.DataSource.OpenConnectionAsync(cancellationToken);
        await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync(cancellationToken);

        await outbox.FlushAsync(publisher.InTransaction(transaction), cancellationToken);

        if (!state.Consumer.AtMostOnce && !await DeleteAsync(state, message, transaction, cancellationToken))
        {
            await transaction.RollbackAsync(cancellationToken);
            return false;
        }

        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    /// <summary>The delete of a message in its outbox's transaction, traced and measured as any other settle.</summary>
    private async Task<bool> DeleteAsync(QueueState state, QueueMessage message, NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        string queue = state.Consumer.QueueName;
        long start = Stopwatch.GetTimestamp();
        using Activity? activity = PgmqTelemetry.StartSettle("delete", queue, message.MsgId, database.Server);
        string? errorType = null;

        try
        {
            return await queues.DeleteMessageAsync(queue, message.MsgId, transaction, cancellationToken);
        }
        catch (Exception ex)
        {
            errorType = PgmqTelemetry.ErrorType(ex);
            PgmqTelemetry.Fail(activity, errorType, ex);
            throw;
        }
        finally
        {
            PgmqTelemetry.RecordOperation("settle", "delete", queue, start, errorType, database.Server);
        }
    }

    /// <summary>Writes the outcome to the queue, retrying a failed write; a write that never lands leaves the lease to lapse.</summary>
    private async Task SettleAsync(QueueState state, QueueMessage message, Settlement settlement, LeaseEntry? lease)
    {
        string queue = state.Consumer.QueueName;

        if (state.Consumer.AtMostOnce || settlement.Written)
        {
            // pgmq.pop deleted it as it was read, or the outbox's transaction already settled it.
            return;
        }

        string operation = settlement.Kind switch
        {
            SettlementKind.Delete => "delete",
            SettlementKind.Archive => "archive",
            _ => "set_vt",
        };

        long start = Stopwatch.GetTimestamp();
        using Activity? activity = PgmqTelemetry.StartSettle(operation, queue, message.MsgId, database.Server);
        string? errorType = null;

        try
        {
            bool found = true;
            await RetryAsync(async ct => found = settlement.Kind switch
            {
                SettlementKind.Delete => await queues.DeleteMessageAsync(queue, message.MsgId, ct),
                SettlementKind.Archive => await queues.ArchiveMessageAsync(queue, message.MsgId, ct),
                _ => await ReleaseAsync(queue, message.MsgId, settlement, ct),
            }, settlement.Kind.ToString().ToLowerInvariant(), state, message);

            if (!found)
            {
                ReportLostLease(state, message.MsgId, lease,
                    $"was no longer on the queue when it came to {operation} it");
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Could not {Settlement} message {MsgId} on {Queue}; it comes back after the lease",
                settlement.Kind, message.MsgId, queue);
            errorType = PgmqTelemetry.ErrorType(ex);
            PgmqTelemetry.Fail(activity, errorType, ex);

            TagList failed = state.Tags;
            failed.Add("messaging.operation.name", operation);
            failed.Add("error.type", errorType);
            PgmqTelemetry.SettleFailures.Add(1, failed);
        }
        finally
        {
            PgmqTelemetry.RecordOperation("settle", operation, queue, start, errorType, database.Server);
        }
    }

    /// <summary>
    /// Makes a message visible again after the settlement's delay. It is already out of the leased set, so the next
    /// renewal skips it; waiting out the one in flight keeps that renewal from hiding it again for a whole lease.
    /// </summary>
    /// <returns>False when the message was no longer on the queue.</returns>
    private async Task<bool> ReleaseAsync(string queue, long messageId, Settlement settlement,
        CancellationToken cancellationToken)
    {
        // Bounded: a renewal falling back queue by queue during an outage could hold it for minutes, and a release on
        // shutdown cannot wait that long. Past the bound the race is back, at worst hiding the message for a lease.
        if (await _renewing.WaitAsync(options.RenewEvery, cancellationToken))
        {
            _renewing.Release();
        }

        return settlement.Unread
            ? await queues.ReturnUnreadAsync(queue, messageId, cancellationToken)
            : await queues.SetVisibilityTimeoutAsync(queue, messageId, settlement.Delay, cancellationToken);
    }

    /// <summary>
    /// Says, once per message, that its lease lapsed while this host handled it: another reader read it again, and may
    /// have run its consumer too, or settled it.
    /// </summary>
    private void ReportLostLease(QueueState state, long messageId, LeaseEntry? lease, string what)
    {
        if (lease is not null && Interlocked.Exchange(ref lease.Reported, 1) == 1)
        {
            return;
        }

        logger.LogWarning("Message {MsgId} on {Queue} {What}: its lease lapsed, or it was changed by hand. Another " +
                          "reader may have handled it as well", messageId, state.Consumer.QueueName, what);
        PgmqTelemetry.LostLeases.Add(1, state.Tags);
    }

    /// <summary>Runs <paramref name="write"/>, trying again after a failure that <paramref name="retryIf"/> allows (any, by default).</summary>
    private async Task RetryAsync(Func<CancellationToken, Task> write, string what, QueueState state,
        QueueMessage message, Func<Exception, bool>? retryIf = null)
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                await write(CancellationToken.None);
                return;
            }
            catch (Exception ex) when (attempt < (_abort.IsCancellationRequested
                                           ? SettleRetriesOnShutdown
                                           : SettleRetries.Length)
                                       && (retryIf?.Invoke(ex) ?? true))
            {
                logger.LogWarning(ex, "Could not {What} message {MsgId} on {Queue}; trying again in {Delay}", what,
                    message.MsgId, state.Consumer.QueueName, SettleRetries[attempt]);

                // Cut short when shutdown cancels the consumers: the host is about to give up, so try once more now.
                await DelayQuietlyAsync(SettleRetries[attempt], _abort.Token);
            }
        }
    }

    /// <summary>
    /// Keeps every running message hidden: every <see cref="PgmqOptions.RenewEvery"/>, one round trip for all queues,
    /// a <c>set_vt</c> per queue with leased messages in one batch, on a connection of its own.
    /// </summary>
    private async Task RenewLeasesAsync(CancellationToken stop)
    {
        try
        {
            while (!stop.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(options.RenewEvery, stop);
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                await _renewing.WaitAsync(CancellationToken.None);
                try
                {
                    await RenewOnceAsync();
                }
                finally
                {
                    _renewing.Release();
                }
            }
        }
        finally
        {
            await CloseRenewalConnectionAsync();
        }
    }

    /// <summary>
    /// One renewal: every leased message of every queue, in one batch when it can. A try that fails on the connection
    /// is made once more on a new one; each is bounded (<see cref="RenewTimeoutSeconds"/>), so a database that does not
    /// answer never keeps the next renewal waiting for long.
    /// </summary>
    private async Task RenewOnceAsync()
    {
        List<(QueueState State, long[] Leased)> due =
        [
            .. _queues.Values
                .Select(q => (State: q, Leased: (long[])[.. q.Leased.Keys]))
                .Where(q => q.Leased.Length > 0),
        ];
        if (due.Count == 0)
        {
            // Nothing runs: an idle host holds no connection for renewals.
            await CloseRenewalConnectionAsync();
            return;
        }

        Exception? failure = null;
        for (int attempt = 0; attempt < 2; attempt++)
        {
            try
            {
                NpgsqlConnection connection = await RenewalConnectionAsync();
                try
                {
                    await RenewInOneBatchAsync(connection, due);
                }
                catch (PostgresException ex) when (connection.FullState == System.Data.ConnectionState.Open)
                {
                    // A batch is one transaction: one broken queue fails them all. Renew one queue at a time
                    // instead, so the others keep their leases.
                    logger.LogDebug(ex, "Renewing leases in one batch failed; renewing queue by queue");
                    await RenewQueueByQueueAsync(connection, due);
                }

                return;
            }
            catch (Exception ex)
            {
                // The connection, not a queue: every queue would fail on it. Try once on a new one.
                failure = ex;
                await CloseRenewalConnectionAsync();
            }
        }

        // One missed renewal is covered: the lease is three renewals long.
        logger.LogWarning(failure, "Could not renew the leases of {Count} queues", due.Count);
        string errorType = PgmqTelemetry.ErrorType(failure!);
        foreach ((QueueState state, _) in due)
        {
            TagList failed = state.Tags;
            failed.Add("error.type", errorType);
            PgmqTelemetry.LeaseRenewalFailures.Add(1, failed);
        }
    }

    private async Task RenewInOneBatchAsync(NpgsqlConnection connection, List<(QueueState State, long[] Leased)> due)
    {
        await using NpgsqlBatch batch = new(connection) { Timeout = RenewTimeoutSeconds };
        foreach ((QueueState state, long[] leased) in due)
        {
            NpgsqlBatchCommand command = new(RenewSql);
            AddRenewParameters(command.Parameters, state, leased);
            batch.BatchCommands.Add(command);
        }

        List<Dictionary<long, int>> renewed = new(due.Count);
        await using (NpgsqlDataReader reader = await batch.ExecuteReaderAsync(CancellationToken.None))
        {
            do
            {
                Dictionary<long, int> readCounts = [];
                while (await reader.ReadAsync(CancellationToken.None))
                {
                    readCounts[reader.GetInt64(0)] = reader.GetInt32(1);
                }

                renewed.Add(readCounts);
            } while (await reader.NextResultAsync(CancellationToken.None));
        }

        for (int i = 0; i < due.Count && i < renewed.Count; i++)
        {
            FindLostLeases(due[i].State, due[i].Leased, renewed[i]);
        }
    }

    /// <summary>Each queue on its own; one the database refuses is reported, and the others still renewed.</summary>
    private async Task RenewQueueByQueueAsync(NpgsqlConnection connection,
        List<(QueueState State, long[] Leased)> due)
    {
        foreach ((QueueState state, long[] leased) in due)
        {
            try
            {
                await using NpgsqlCommand command = new(RenewSql, connection) { CommandTimeout = RenewTimeoutSeconds };
                AddRenewParameters(command.Parameters, state, leased);

                Dictionary<long, int> readCounts = [];
                await using (NpgsqlDataReader reader = await command.ExecuteReaderAsync(CancellationToken.None))
                {
                    while (await reader.ReadAsync(CancellationToken.None))
                    {
                        readCounts[reader.GetInt64(0)] = reader.GetInt32(1);
                    }
                }

                FindLostLeases(state, leased, readCounts);
            }
            catch (PostgresException ex) when (connection.FullState == System.Data.ConnectionState.Open)
            {
                logger.LogWarning(ex, "Could not renew leases on {Queue}", state.Consumer.QueueName);
                TagList failed = state.Tags;
                failed.Add("error.type", PgmqTelemetry.ErrorType(ex));
                PgmqTelemetry.LeaseRenewalFailures.Add(1, failed);
            }
        }
    }

    private const string RenewSql = "select msg_id, read_ct from pgmq.set_vt($1, $2, $3)";

    private void AddRenewParameters(NpgsqlParameterCollection parameters, QueueState state, long[] leased)
    {
        parameters.Add(new() { Value = state.Consumer.QueueName, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        parameters.Add(new()
        {
            Value = leased,
            NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Bigint,
        });
        parameters.Add(new() { Value = LeaseSeconds, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
    }

    /// <summary>
    /// Compares what a renewal renewed with what it asked for. A message that is gone, or that was read more often
    /// than when this host read it, was taken by another reader after its lease lapsed. Nothing can undo that; it is
    /// reported, so lapsing leases are seen and not found out from duplicates.
    /// </summary>
    private void FindLostLeases(QueueState state, long[] asked, Dictionary<long, int> renewed)
    {
        foreach (long messageId in asked)
        {
            // Settled since the renewal took its snapshot, or being settled right now: its row may rightly be gone.
            if (!state.Leased.TryGetValue(messageId, out LeaseEntry? lease) || lease.Settling)
            {
                continue;
            }

            if (!renewed.TryGetValue(messageId, out int readCount))
            {
                ReportLostLease(state, messageId, lease, "is no longer on the queue while its consumer runs");
            }
            else if (readCount > lease.ReadCt)
            {
                ReportLostLease(state, messageId, lease,
                    $"was read again while its consumer runs (read {readCount}, was {lease.ReadCt})");
            }
        }
    }

    private async Task<NpgsqlConnection> RenewalConnectionAsync()
    {
        if (_renewalConnection is { FullState: System.Data.ConnectionState.Open } open)
        {
            return open;
        }

        await CloseRenewalConnectionAsync();

        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(RenewTimeoutSeconds));
        return _renewalConnection = await database.OpenDedicatedAsync(logger, timeout.Token);
    }

    private async Task CloseRenewalConnectionAsync()
    {
        if (Interlocked.Exchange(ref _renewalConnection, null) is not { } connection)
        {
            return;
        }

        try
        {
            await connection.DisposeAsync();
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Closing the lease renewal connection failed");
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

        /// <summary>The queue and consumer, as tags of the worker's metrics.</summary>
        public TagList Tags { get; } = PgmqTelemetry.ConsumerTags(consumer.QueueName, consumer.ConsumerType.Name);

        public SemaphoreSlim Slots { get; } = new(consumer.Concurrency, consumer.Concurrency);

        /// <summary>Messages whose lease is renewed: read, and not yet settled.</summary>
        public ConcurrentDictionary<long, LeaseEntry> Leased { get; } = new();

        /// <summary>Every running handler, awaited on shutdown.</summary>
        public ConcurrentDictionary<Task, byte> InFlight { get; } = new();

        /// <summary>Released by an insert notification or the sweeper, to end an idle wait.</summary>
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

    /// <summary>One read of a message whose lease this host renews.</summary>
    private sealed class LeaseEntry(int readCt)
    {
        /// <summary>1 once the loss of this lease was reported, so it is reported once.</summary>
        public int Reported;

        /// <summary>The message's read count as this host read it; a higher one means someone read it again.</summary>
        public int ReadCt { get; } = readCt;

        /// <summary>Its outcome is being written, so its row may be gone before the lease is ended.</summary>
        public volatile bool Settling;
    }

    private enum SettlementKind
    {
        Delete,
        Archive,
        Release,
    }

    /// <summary>
    /// What to write to the queue; nothing, when <c>Written</c> says the outbox's transaction already settled it. A
    /// release makes the message visible again after <c>Delay</c>; an <c>Unread</c> one also takes back the read the
    /// message was counted for, because it was never handed to a consumer.
    /// </summary>
    private readonly record struct Settlement(SettlementKind Kind, bool Written = false, TimeSpan Delay = default,
        bool Unread = false)
    {
        public static readonly Settlement Delete = new(SettlementKind.Delete);
        public static readonly Settlement Deleted = new(SettlementKind.Delete, Written: true);
        public static readonly Settlement Archive = new(SettlementKind.Archive);
        public static readonly Settlement Release = new(SettlementKind.Release);
        public static readonly Settlement Unstarted = new(SettlementKind.Release, Unread: true);
    }

    /// <summary>
    /// How handling a message ended: what to write to the queue, the <c>messaging.pgmq.outcome</c> it is reported
    /// under (success, archived, released, lost or gone), and, for a failure, its <c>error.type</c> and exception.
    /// </summary>
    private readonly record struct Outcome(Settlement Settlement, string Name, string? ErrorType, Exception? Error)
    {
        public static readonly Outcome Success = new(Settlement.Delete, "success", null, null);

        /// <summary>A success whose outbox went out in one transaction with the delete of its message.</summary>
        public static readonly Outcome Committed = new(Settlement.Deleted, "success", null, null);

        /// <summary>
        /// A success whose message was no longer on the queue when its outbox went to commit: nothing was sent, and
        /// nothing is left to settle.
        /// </summary>
        public static readonly Outcome Gone = new(Settlement.Deleted, "gone", null, null);
        public static readonly Outcome Released = new(Settlement.Release, "released", null, null);

        /// <summary>Put back on the queue unhandled, to be read again after <paramref name="delay"/>.</summary>
        public static Outcome Deferred(string errorType, TimeSpan delay)
            => new(new Settlement(SettlementKind.Release, Delay: delay), "released", errorType, null);

        public static Outcome Archived(string errorType, Exception? error = null)
            => new(Settlement.Archive, "archived", errorType, error);

        /// <summary>An at-most-once message that did not succeed: pgmq already deleted it, so nothing is written.</summary>
        public static Outcome Lost(string errorType, Exception? error = null)
            => new(Settlement.Archive, "lost", errorType, error);
    }
}
