using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Pgmq.Hosting;

/// <summary>
/// Reports every queue's depth and the age of its oldest message as gauges on the <see cref="PgmqTelemetry.MeterName"/>
/// meter, from <c>pgmq.metrics_all</c> every <see cref="PgmqOptions.QueueMetricsInterval"/>. It queries only while the
/// application collects one of the gauges, and reports nothing when the last query failed rather than old numbers.
/// </summary>
internal sealed class QueueMetricsCollector : BackgroundService
{
    private readonly PgmqQueueManager _queues;
    private readonly PgmqStatus _status;
    private readonly TimeSpan _interval;
    private readonly ServerEndpoint _server;
    private readonly ILogger<QueueMetricsCollector> _logger;
    private readonly Meter _meter = new(PgmqTelemetry.MeterName, PgmqTelemetry.Version);
    private readonly Instrument[] _gauges;

    private volatile IReadOnlyList<PgmqQueueMetrics> _latest = [];

    public QueueMetricsCollector(PgmqQueueManager queues, PgmqStatus status, PgmqOptions options,
        PgmqDatabase database, ILogger<QueueMetricsCollector> logger)
    {
        _queues = queues;
        _status = status;
        _interval = options.QueueMetricsInterval ?? throw new InvalidOperationException(
            $"{nameof(PgmqOptions.QueueMetricsInterval)} is not set.");
        _server = database.Server;
        _logger = logger;

        _gauges =
        [
            _meter.CreateObservableGauge("pgmq.queue.messages", () => Measure(m => m.QueueLength), "{message}",
                "Messages in the queue, visible or not."),
            _meter.CreateObservableGauge("pgmq.queue.messages.visible", () => Measure(m => m.VisibleLength),
                "{message}", "Messages in the queue that can be read now."),
            _meter.CreateObservableGauge("pgmq.queue.oldest_message.age",
                () => Measure(m => m.OldestMessageAge?.TotalSeconds ?? 0), "s",
                "Age of the oldest message in the queue; 0 when it is empty."),
        ];
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_status.IsReady)
        {
            return;
        }

        using PeriodicTimer timer = new(_interval);
        do
        {
            if (!_gauges.Any(g => g.Enabled))
            {
                _latest = [];
                continue;
            }

            try
            {
                _latest = await _queues.GetAllMetricsAsync(stoppingToken);
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                _latest = [];
                _logger.LogWarning(ex, "Could not read queue metrics; trying again in {Interval}", _interval);
            }
        }
        while (await WaitAsync(timer, stoppingToken));
    }

    public override void Dispose()
    {
        _meter.Dispose();
        base.Dispose();
    }

    private static async Task<bool> WaitAsync(PeriodicTimer timer, CancellationToken stoppingToken)
    {
        try
        {
            return await timer.WaitForNextTickAsync(stoppingToken);
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    private IEnumerable<Measurement<T>> Measure<T>(Func<PgmqQueueMetrics, T> value) where T : struct
    {
        foreach (PgmqQueueMetrics queue in _latest)
        {
            TagList tags = new()
            {
                { "messaging.system", PgmqTelemetry.System },
                { "messaging.destination.name", queue.QueueName },
            };

            if (_server.Address is not null)
            {
                tags.Add("server.address", _server.Address);
            }

            yield return new(value(queue), tags);
        }
    }
}
