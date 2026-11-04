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
/// consumer keeps taking the process down) is archived without running. The outcome is decided first and written to the queue after, with its own retries, so a
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
/// <b>Waking.</b> An idle queue never goes to the database on its own. Its read loop reads only when woken: by an
/// insert notification (LISTEN/NOTIFY), by the <see cref="QueueSweeper"/>, which finds every queue with a visible
/// message in one query, or because its last read found messages, so more are likely. Otherwise it waits. An idle host
/// therefore holds two connections (the listener and the sweeper's) whatever the number of queues, and a busy one
/// reads through at most <see cref="PgmqOptions.MaxConcurrentReads"/> connections at once.
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

        // The listener and the sweeper come first. The read loops start waiting, and the first sweep wakes only the
        // queues that have messages: starting costs one query, not one read per queue.
        List<Task> background = [];

        Dictionary<string, Action> notified = registry.Consumers
            .Where(c => listener.IsEnabled(c.QueueName))
            .ToDictionary(c => c.QueueName, c => (Action)_queues[c.QueueName].Wake, StringComparer.Ordinal);
        if (notified.Count > 0)
        {
            background.Add(Task.Run(() => listener.RunAsync(notified, sweeper.Nudge, stoppingToken),
                CancellationToken.None));
        }

        SweptQueue[] swept = [.. registry.Consumers.Select(c => new SweptQueue(c, _queues[c.QueueName].Wake))];
        background.Add(Task.Run(() => sweeper.RunAsync(swept, stoppingToken), CancellationToken.None));

        using CancellationTokenSource renewalStop = new();
        Task renewal = Task.Run(() => RenewLeasesAsync(renewalStop.Token), CancellationToken.None);

        await Task.WhenAll(registry.Consumers.Select(consumer =>
            Task.Run(() => RunQueueAsync(_queues[consumer.QueueName], stoppingToken), CancellationToken.None)));

        // Every queue drained: nothing is leased any more.
        await renewalStop.CancelAsync();
        await renewal;
        await Task.WhenAll(background);
    }

    public override void Dispose()
    {
        _abort.Dispose();
        base.Dispose();
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
                outcome = await RunConsumerAsync(state, message, headers!, messageType!);
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

        // Decided; from here only the queue is written. Stop renewing first, so a settle that cannot be written leaves
        // the message to come back within one lease rather than sit hidden.
        state.Leased.TryRemove(message.MsgId, out _);
        await SettleAsync(state, message, outcome.Settlement);

        // An unknown or exhausted message is archived, which takes it out of its group, so the group's next message may
        // run.
        return known && !exhausted ? outcome.Settlement.Kind : SettlementKind.Delete;
    }

    /// <summary>Runs the consumer under its deadline and flushes its outbox, and says what to do with the message.</summary>
    private async Task<Outcome> RunConsumerAsync(QueueState state, QueueMessage message,
        MessageHeaders headers, Type messageType)
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
        catch (OperationCanceledException) when (_abort.IsCancellationRequested)
        {
            if (consumer.AtMostOnce)
            {
                logger.LogWarning("{Consumer} stopped for shutdown; its at-most-once message is lost",
                    consumer.ConsumerType.Name);
                return Outcome.Lost("pgmq.shutdown");
            }

            logger.LogInformation("{Consumer} stopped for shutdown; its message goes back to the queue",
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
    private async Task SettleAsync(QueueState state, QueueMessage message, Settlement settlement)
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
            await RetryAsync(ct => settlement.Kind switch
            {
                SettlementKind.Delete => queues.DeleteMessageAsync(queue, message.MsgId, ct),
                SettlementKind.Archive => queues.ArchiveMessageAsync(queue, message.MsgId, ct),
                SettlementKind.Release => queues.SetVisibilityTimeoutAsync(queue, message.MsgId, TimeSpan.Zero, ct),
                _ => Task.CompletedTask,
            }, settlement.Kind.ToString().ToLowerInvariant(), state, message);
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
            catch (Exception ex) when (attempt < SettleRetries.Length && (retryIf?.Invoke(ex) ?? true))
            {
                logger.LogWarning(ex, "Could not {What} message {MsgId} on {Queue}; trying again", what,
                    message.MsgId, state.Consumer.QueueName);
                await Task.Delay(SettleRetries[attempt]);
            }
        }
    }

    /// <summary>
    /// Keeps every running message hidden: every <see cref="PgmqOptions.RenewEvery"/>, one round trip for all queues,
    /// a <c>set_vt</c> per queue with leased messages in one batch.
    /// </summary>
    private async Task RenewLeasesAsync(CancellationToken stop)
    {
        TimeSpan lease = TimeSpan.FromSeconds(LeaseSeconds);

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

            List<(QueueState State, long[] Leased)> due =
            [
                .. _queues.Values
                    .Select(q => (State: q, Leased: (long[])[.. q.Leased.Keys]))
                    .Where(q => q.Leased.Length > 0),
            ];
            if (due.Count == 0)
            {
                continue;
            }

            try
            {
                await using NpgsqlBatch batch = database.DataSource.CreateBatch();
                foreach ((QueueState state, long[] leased) in due)
                {
                    batch.BatchCommands.Add(new("select msg_id from pgmq.set_vt($1, $2, $3)")
                    {
                        Parameters =
                        {
                            new() { Value = state.Consumer.QueueName, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text },
                            new()
                            {
                                Value = leased,
                                NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Bigint,
                            },
                            new() { Value = LeaseSeconds, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer },
                        },
                    });
                }

                await batch.ExecuteNonQueryAsync(CancellationToken.None);
            }
            catch (Exception batchFailure)
            {
                // A batch is one transaction: one broken queue fails them all. Renew one queue at a time instead, so
                // the others keep their leases. One missed renewal is covered: the lease is three renewals long.
                logger.LogDebug(batchFailure, "Renewing leases in one batch failed; renewing queue by queue");
                foreach ((QueueState state, long[] leased) in due)
                {
                    try
                    {
                        await queues.SetVisibilityTimeoutAsync(state.Consumer.QueueName, leased, lease,
                            CancellationToken.None);
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex, "Could not renew leases on {Queue}", state.Consumer.QueueName);
                        TagList failed = state.Tags;
                        failed.Add("error.type", PgmqTelemetry.ErrorType(ex));
                        PgmqTelemetry.LeaseRenewalFailures.Add(1, failed);
                    }
                }
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

        /// <summary>The queue and consumer, as tags of the worker's metrics.</summary>
        public TagList Tags { get; } = PgmqTelemetry.ConsumerTags(consumer.QueueName, consumer.ConsumerType.Name);

        public SemaphoreSlim Slots { get; } = new(consumer.Concurrency, consumer.Concurrency);

        /// <summary>Messages whose lease is renewed: read, and not yet settled.</summary>
        public ConcurrentDictionary<long, byte> Leased { get; } = new();

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

    private enum SettlementKind
    {
        Delete,
        Archive,
        Release,
    }

    /// <summary>What to write to the queue; nothing, when <c>Written</c> says the outbox's transaction already settled it.</summary>
    private readonly record struct Settlement(SettlementKind Kind, bool Written = false)
    {
        public static readonly Settlement Delete = new(SettlementKind.Delete);
        public static readonly Settlement Deleted = new(SettlementKind.Delete, Written: true);
        public static readonly Settlement Archive = new(SettlementKind.Archive);
        public static readonly Settlement Release = new(SettlementKind.Release);
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

        public static Outcome Archived(string errorType, Exception? error = null)
            => new(Settlement.Archive, "archived", errorType, error);

        /// <summary>An at-most-once message that did not succeed: pgmq already deleted it, so nothing is written.</summary>
        public static Outcome Lost(string errorType, Exception? error = null)
            => new(Settlement.Archive, "lost", errorType, error);
    }
}
