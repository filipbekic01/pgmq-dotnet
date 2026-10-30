using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgmq;

namespace Pgmq.Hosting;

/// <summary>
/// Creates the pgmq extension and every consumer's queue before anything publishes or reads. Both are idempotent, so
/// every host and instance runs them on start. A failure stops the host unless
/// <see cref="PgmqOptions.FailOnSetupError"/> is off; then it is logged and recorded in <see cref="PgmqStatus"/>.
/// </summary>
internal sealed class QueueSetup(
    INpgmqClient client,
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

            await client.InitAsync(timeout.Token);

            foreach (ConsumerRegistration consumer in registry.Consumers)
            {
                await client.CreateQueueAsync(consumer.QueueName, timeout.Token);
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
