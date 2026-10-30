using System.Diagnostics;


namespace Pgmq.Benchmarks;

/// <summary>A load message: its id, and when it was sent, so the consumer can tell how long it took to arrive.</summary>
public abstract class LoadMessage
{
    public long Id { get; set; }

    /// <summary><see cref="Stopwatch.GetTimestamp"/> at send. Sender and consumer share the process.</summary>
    public long SentAt { get; set; }
}

/// <summary>Does nothing but record that it saw the message, so the scenario measures the transport alone.</summary>
public sealed class LoadConsumer<T>(LoadTracker tracker) : IPgmqConsumer<T> where T : LoadMessage
{
    public Task HandleAsync(PgmqConsumeContext<T> context, CancellationToken cancellationToken)
    {
        tracker.Seen(context.Message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Every delivery, per message id: how many times each id arrived and how long it took. A count of zero is a lost
/// message, more than one a duplicate.
/// </summary>
public sealed class LoadTracker
{
    private readonly int[] _hits;
    private readonly long[] _latencyTicks;
    private readonly TaskCompletionSource _allSeen = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _distinct;
    private int _deliveries;

    public LoadTracker(int messages)
    {
        _hits = new int[messages];
        _latencyTicks = new long[messages];
        if (messages == 0)
        {
            _allSeen.SetResult();
        }
    }

    public int Expected => _hits.Length;
    public int Deliveries => Volatile.Read(ref _deliveries);
    public int Distinct => Volatile.Read(ref _distinct);
    public int Duplicates => Deliveries - Distinct;
    public int Lost => Expected - Distinct;

    /// <summary>Completes once every id arrived at least once.</summary>
    public Task AllSeen => _allSeen.Task;

    public void Seen(LoadMessage message)
    {
        long now = Stopwatch.GetTimestamp();
        Interlocked.Increment(ref _deliveries);

        if (Interlocked.Increment(ref _hits[message.Id]) != 1)
        {
            return;
        }

        _latencyTicks[message.Id] = now - message.SentAt;
        if (Interlocked.Increment(ref _distinct) == _hits.Length)
        {
            _allSeen.TrySetResult();
        }
    }

    /// <summary>Send-to-consume time at <paramref name="percentile"/> (0..100), over the messages that arrived.</summary>
    public TimeSpan Latency(double percentile)
    {
        long[] seen = [.. _latencyTicks.Where((_, i) => Volatile.Read(ref _hits[i]) > 0)];
        if (seen.Length == 0)
        {
            return TimeSpan.Zero;
        }

        Array.Sort(seen);
        int index = (int)Math.Clamp(Math.Ceiling(percentile / 100 * seen.Length) - 1, 0, seen.Length - 1);
        return Stopwatch.GetElapsedTime(0, seen[index]);
    }
}
