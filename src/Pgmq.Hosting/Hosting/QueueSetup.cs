using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Pgmq.Hosting;

/// <summary>
/// Creates the pgmq extension and every consumer's queue before anything publishes or reads, and gives each queue what
/// its consumer asks for: its type when created, a FIFO index, topic bindings and insert notifications. All of it is
/// idempotent and safe to run from several instances at once, so every host runs it on start. A queue that already
/// exists keeps the type it has. With <see cref="PgmqOptions.AutoSetup"/> off it changes nothing and only checks that
/// the extension and the queues are there. A failure stops the host unless <see cref="PgmqOptions.FailOnSetupError"/> is off;
/// then it is logged and recorded in <see cref="PgmqStatus"/>.
/// </summary>
internal sealed class QueueSetup(
    PgmqQueueManager queues,
    ConsumerRegistry registry,
    InsertListener listener,
    PgmqOptions options,
    PgmqStatus status,
    ILogger<QueueSetup> logger
) : IHostedService
{
    private static readonly TimeSpan GiveUpAfter = TimeSpan.FromSeconds(30);

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        status.Queues = [.. registry.Consumers.Select(c => c.QueueName)];

        try
        {
            using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(GiveUpAfter);

            if (options.AutoSetup)
            {
                await SetUpAllAsync(timeout.Token);
            }
            else
            {
                await CheckAllAsync(timeout.Token);
            }

            status.MarkReady();
            logger.LogInformation("Pgmq is ready with {Queues} queues", registry.Consumers.Count);
        }
        catch (Exception ex) when (!options.FailOnSetupError && !cancellationToken.IsCancellationRequested)
        {
            status.MarkFailed($"{ex.GetType().Name}: {ex.Message}");
            logger.LogError(ex, "Pgmq could not be set up; its worker stays idle and the host runs without it");
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private async Task SetUpAllAsync(CancellationToken cancellationToken)
    {
        await queues.InitAsync(cancellationToken);

        if (registry.Consumers.Any(c => c.Queue.Type == PgmqQueueType.Partitioned))
        {
            await queues.InitPartmanAsync(cancellationToken);
        }

        IReadOnlyList<PgmqQueueInfo> existing = await queues.ListQueuesAsync(cancellationToken);

        foreach (ConsumerRegistration consumer in registry.Consumers)
        {
            await SetUpAsync(consumer, existing.FirstOrDefault(q => q.QueueName == consumer.QueueName),
                cancellationToken);
        }
    }

    /// <summary>Without auto setup: the extension and every queue must already exist.</summary>
    private async Task CheckAllAsync(CancellationToken cancellationToken)
    {
        if (await queues.GetVersionAsync(cancellationToken) is null)
        {
            throw new InvalidOperationException(
                $"The pgmq extension is not installed. Create it, or turn on {nameof(PgmqOptions.AutoSetup)}.");
        }

        HashSet<string> existing =
            [.. (await queues.ListQueuesAsync(cancellationToken)).Select(q => q.QueueName)];
        string[] missing = [.. registry.Consumers.Select(c => c.QueueName).Where(q => !existing.Contains(q))];
        if (missing.Length > 0)
        {
            throw new InvalidOperationException(
                $"Queues missing: {string.Join(", ", missing)}. Create them, or turn on {nameof(PgmqOptions.AutoSetup)}.");
        }

        HashSet<string> notified =
            [.. (await queues.ListInsertNotificationsAsync(cancellationToken)).Select(n => n.QueueName)];
        foreach (ConsumerRegistration consumer in registry.Consumers)
        {
            if (!consumer.UseInsertNotifications)
            {
                continue;
            }

            if (notified.Contains(consumer.QueueName))
            {
                listener.MarkEnabled(consumer.QueueName);
            }
            else
            {
                logger.LogInformation("Queue {Queue} has no insert notifications; it long-polls instead",
                    consumer.QueueName);
            }
        }
    }

    private async Task SetUpAsync(ConsumerRegistration consumer, PgmqQueueInfo? existing,
        CancellationToken cancellationToken)
    {
        string queue = consumer.QueueName;

        if (existing is null)
        {
            await queues.EnsureQueueAsync(queue, consumer.Queue, cancellationToken);
        }
        else if (TypeOf(existing) != consumer.Queue.Type)
        {
            logger.LogWarning(
                "Queue {Queue} exists as {Existing} but its consumer asks for {Wanted}; it stays {Existing}. Drop it " +
                "or move its messages to change it", queue, TypeOf(existing), consumer.Queue.Type, TypeOf(existing));
        }

        if (consumer.IsGrouped)
        {
            await queues.CreateFifoIndexAsync(queue, cancellationToken);
        }

        foreach (string pattern in consumer.TopicBindings)
        {
            await queues.BindTopicAsync(pattern, queue, cancellationToken);
        }

        if (consumer.UseInsertNotifications)
        {
            try
            {
                await queues.EnsureInsertNotificationsAsync(queue, options.InsertNotificationThrottle,
                    cancellationToken);
                listener.MarkEnabled(queue);
            }
            catch (PostgresException ex)
            {
                // Notifications only make waking faster, so a database that refuses them (no right to create the
                // trigger, say) leaves the queue on long polling rather than stopping the host.
                logger.LogWarning(ex, "Insert notifications for {Queue} could not be turned on; it long-polls instead",
                    queue);
            }
        }
    }

    private static PgmqQueueType TypeOf(PgmqQueueInfo queue)
        => queue.IsPartitioned ? PgmqQueueType.Partitioned
            : queue.IsUnlogged ? PgmqQueueType.Unlogged
            : PgmqQueueType.Standard;
}

/// <summary>
/// Reports Pgmq: <see cref="PgmqOptions.UnavailableHealthStatus"/> when it could not be set up, and
/// <see cref="PgmqOptions.StalledHealthStatus"/> when a queue's read loop has gone quiet.
/// </summary>
internal sealed class PgmqHealthCheck(PgmqStatus status, PgmqOptions options) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        if (!status.IsReady)
        {
            return Task.FromResult(new HealthCheckResult(options.UnavailableHealthStatus,
                $"Pgmq is not available: {status.Failure}"));
        }

        IReadOnlyList<string> stalled = status.StalledQueues;

        return Task.FromResult(stalled.Count == 0
            ? HealthCheckResult.Healthy()
            : new HealthCheckResult(options.StalledHealthStatus,
                $"Pgmq queues not reading: {string.Join(", ", stalled)}"));
    }
}
