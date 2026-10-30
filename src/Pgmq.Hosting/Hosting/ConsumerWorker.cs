using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgmq;

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
/// </summary>
internal sealed class ConsumerWorker(
    INpgmqClient client,
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

        await using CancellationTokenRegistration onStop =
            stoppingToken.Register(() => _abort.CancelAfter(options.DrainGrace));

        await Task.WhenAll(registry.Consumers.Select(consumer =>
            Task.Run(() => RunQueueAsync(consumer, stoppingToken), CancellationToken.None)));
    }

    public override void Dispose()
    {
        _abort.Dispose();
        base.Dispose();
    }

    /// <summary>One queue for the life of the host: its read loop, kept alive, its lease renewal, and the drain.</summary>
    private async Task RunQueueAsync(ConsumerRegistration consumer, CancellationToken stoppingToken)
    {
        QueueState state = _queues[consumer.QueueName] = new(consumer);
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

            List<NpgmqMessage<string>> messages;
            try
            {
                messages = await client.ReadBatchAsync<string>(queue, LeaseSeconds, free, stoppingToken);
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
                await Task.Delay(idleDelay, stoppingToken);
                idleDelay = TimeSpan.FromTicks(Math.Min(idleDelay.Ticks * 2, options.MaxIdleDelay.Ticks));
                continue;
            }

            idleDelay = options.MinIdleDelay;

            foreach (NpgmqMessage<string> message in messages)
            {
                Start(state, message);
            }
        }
    }

    /// <summary>Runs one message on its own, and frees its slot when it is settled, whatever happened.</summary>
    private void Start(QueueState state, NpgmqMessage<string> message)
    {
        state.Leased[message.MsgId] = 0;

        Task handling = Task.Run(async () =>
        {
            try
            {
                await HandleAsync(state, message);
            }
            catch (Exception ex)
            {
                // HandleAsync settles every outcome itself; reaching here is a bug in it. The lease lapses and the
                // message comes back, so nothing is lost.
                logger.LogCritical(ex, "Handling message {MsgId} on {Queue} failed unexpectedly", message.MsgId,
                    state.Consumer.QueueName);
            }
            finally
            {
                state.Leased.TryRemove(message.MsgId, out _);
            }
        }, CancellationToken.None);

        state.InFlight.TryAdd(handling, 0);
        handling.ContinueWith(done =>
        {
            state.InFlight.TryRemove(done, out _);
            state.Slots.Release();
        }, TaskScheduler.Default);
    }

    private async Task HandleAsync(QueueState state, NpgmqMessage<string> message)
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

        if (headers is null || messageType is null || message.Message is null)
        {
            logger.LogError("Message on {Queue} has no known type ({MessageType}); archiving it",
                consumer.QueueName, headers?.MessageType);
            state.Leased.TryRemove(message.MsgId, out _);
            await SettleAsync(state, message, Settlement.Archive);
            return;
        }

        using Activity? activity =
            PgmqTelemetry.StartProcess(headers, consumer.QueueName, message.MsgId, message.ReadCt);

        Settlement settlement = await RunConsumerAsync(state, message, headers, messageType, activity);

        // Decided; from here only the queue is written. Stop renewing first, so a settle that cannot be written leaves
        // the message to come back within one lease rather than sit hidden.
        state.Leased.TryRemove(message.MsgId, out _);
        await SettleAsync(state, message, settlement);
    }

    /// <summary>Runs the consumer under its deadline and flushes its outbox, and says what to do with the message.</summary>
    private async Task<Settlement> RunConsumerAsync(QueueState state, NpgmqMessage<string> message,
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
            logger.LogInformation("{Consumer} stopped for shutdown; its message goes back to the queue",
                consumer.ConsumerType.Name);
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

    private async Task DispatchAsync(ConsumerRegistration consumer, NpgmqMessage<string> message,
        MessageHeaders headers, Type messageType, Outbox outbox, CancellationToken cancellationToken)
    {
        object body = MessageSerializer.Deserialize(message.Message!, messageType);

        // A failed attempt counts as a redelivery, as does an explicit RedeliverAsync (which writes a new message with
        // the count in its headers, so its reads start again from one).
        Delivery delivery = new(publisher, headers.MessageId,
            RedeliveryCount: headers.RedeliveryCount + Math.Max(0, message.ReadCt - 1),
            outbox,
            SendBack: (m, type, delay, count, messageId, ct) =>
                publisher.SendAsync(consumer.QueueName, m, type, delay, count, messageId, ct));

        await using AsyncServiceScope scope = scopeFactory.CreateAsyncScope();
        await ConsumerDispatcher.DispatchAsync(scope.ServiceProvider, consumer, body, messageType, delivery,
            cancellationToken);
    }

    private Settlement Failed(ConsumerRegistration consumer, NpgmqMessage<string> message, Type messageType,
        Exception ex, Activity? activity)
    {
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
    private async Task SettleAsync(QueueState state, NpgmqMessage<string> message, Settlement settlement)
    {
        string queue = state.Consumer.QueueName;

        try
        {
            await RetryAsync(ct => settlement.Kind switch
            {
                SettlementKind.Delete => client.DeleteAsync(queue, message.MsgId, ct),
                SettlementKind.Archive => client.ArchiveAsync(queue, message.MsgId, ct),
                SettlementKind.Retry => client.SetVtAsync(queue, message.MsgId,
                    Math.Max(1, (int)Math.Ceiling(settlement.Delay.TotalSeconds)), ct),
                SettlementKind.Release => client.SetVtAsync(queue, message.MsgId, 0, ct),
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
        NpgmqMessage<string> message)
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
                    await client.SetVtBatchAsync(state.Consumer.QueueName, leased, LeaseSeconds, CancellationToken.None);
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

        public void Beat() => Volatile.Write(ref LastBeat, Stopwatch.GetTimestamp());
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
