using System.Collections.Concurrent;

namespace Pgmq.Examples.WebApi;

/// <summary>One thing a consumer did.</summary>
public sealed record ActivityEntry(DateTimeOffset At, string Consumer, string Text);

/// <summary>The last few hundred things the consumers did, in memory, for GET /activity.</summary>
public sealed class ActivityFeed(TimeProvider time, ILogger<ActivityFeed> logger)
{
    private const int Capacity = 200;
    private readonly ConcurrentQueue<ActivityEntry> _entries = new();

    public void Add(string consumer, string text)
    {
        _entries.Enqueue(new ActivityEntry(time.GetUtcNow(), consumer, text));
        while (_entries.Count > Capacity && _entries.TryDequeue(out _))
        {
        }

        logger.LogInformation("{Consumer}: {Text}", consumer, text);
    }

    /// <summary>Newest first.</summary>
    public IEnumerable<ActivityEntry> Recent(int take) => _entries.Reverse().Take(take);

    public void Clear() => _entries.Clear();
}
