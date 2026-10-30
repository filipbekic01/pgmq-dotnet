using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Pgmq.Hosting;

/// <summary>
/// Creates the pgmq extension and every consumer's queue before anything publishes or reads, and gives each queue what
/// its consumer asks for: its type when created, a FIFO index, topic bindings and insert notifications. All of it is
/// idempotent and safe to run from several instances at once, so every host runs it on start. A queue that already
/// exists keeps the type it has. A failure stops the host unless <see cref="PgmqOptions.FailOnSetupError"/> is off;
/// then it is logged and recorded in <see cref="PgmqStatus"/>.
/// </summary>
internal sealed class QueueSetup(
    PgmqQueueManager queues,
    ConsumerRegistry registry,
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

            await queues.InitAsync(timeout.Token);

            if (registry.Consumers.Any(c => c.Queue.Type == PgmqQueueType.Partitioned))
            {
                await queues.InitPartmanAsync(timeout.Token);
            }

            IReadOnlyList<PgmqQueueInfo> existing = await queues.ListQueuesAsync(timeout.Token);

            foreach (ConsumerRegistration consumer in registry.Consumers)
            {
                await SetUpAsync(consumer, existing.FirstOrDefault(q => q.QueueName == consumer.QueueName),
                    timeout.Token);
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
            await queues.EnsureInsertNotificationsAsync(queue, options.InsertNotificationThrottle, cancellationToken);
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
