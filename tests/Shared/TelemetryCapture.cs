using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Pgmq.Testing;

/// <summary>
/// Records what Pgmq emits, as an application's OpenTelemetry setup would see it: every span of the Pgmq source and
/// every measurement of the Pgmq meter. Tests run in parallel and share the source and meter, so each test reads only
/// what was tagged with its own queue names.
/// </summary>
public sealed class TelemetryCapture : IDisposable
{
    private readonly ActivityListener _spans;
    private readonly MeterListener _meters = new();
    private readonly ConcurrentQueue<Activity> _activities = new();
    private readonly ConcurrentQueue<Measured> _measurements = new();

    public TelemetryCapture()
    {
        _spans = new()
        {
            ShouldListenTo = s => s.Name == PgmqTelemetry.SourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = _activities.Enqueue,
        };
        ActivitySource.AddActivityListener(_spans);

        _meters.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == PgmqTelemetry.MeterName)
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        _meters.SetMeasurementEventCallback<long>((i, v, tags, _) => Record(i, v, tags));
        _meters.SetMeasurementEventCallback<double>((i, v, tags, _) => Record(i, v, tags));
        _meters.Start();
    }

    /// <summary>Finished spans whose <c>messaging.destination.name</c> is <paramref name="destination"/>.</summary>
    public IReadOnlyList<Activity> Spans(string destination)
        => [.. _activities.Where(a => a.GetTagItem("messaging.destination.name") as string == destination)];

    /// <summary>Measurements of <paramref name="instrument"/> tagged with <paramref name="destination"/>.</summary>
    public IReadOnlyList<Measured> Of(string instrument, string destination)
        => [.. _measurements.Where(m => m.Instrument == instrument
                                        && m.Tags.TryGetValue("messaging.destination.name", out object? d)
                                        && d as string == destination)];

    public double Sum(string instrument, string destination) => Of(instrument, destination).Sum(m => m.Value);

    /// <summary>Asks observable instruments (gauges) for their values now.</summary>
    public void Collect() => _meters.RecordObservableInstruments();

    public void Dispose()
    {
        _spans.Dispose();
        _meters.Dispose();
    }

    private void Record(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        Dictionary<string, object?> copy = new(StringComparer.Ordinal);
        foreach (KeyValuePair<string, object?> tag in tags)
        {
            copy[tag.Key] = tag.Value;
        }

        _measurements.Enqueue(new(instrument.Name, value, copy));
    }

    public sealed record Measured(string Instrument, double Value, IReadOnlyDictionary<string, object?> Tags);
}
