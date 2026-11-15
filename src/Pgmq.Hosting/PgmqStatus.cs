using System.Data.Common;
using System.Net;

namespace Pgmq;

/// <summary>Whether Pgmq came up in this host, which queues it serves, and which of them stopped reading.</summary>
public sealed class PgmqStatus
{
    private volatile string? _failure = "Not set up yet.";
    private Func<IReadOnlyList<string>>? _stalled;
    private Func<bool>? _dedicatedOnPool;

    /// <summary>True once the extension and every queue exist.</summary>
    public bool IsReady => _failure is null;

    /// <summary>Why Pgmq is not ready; null when it is.</summary>
    public string? Failure => _failure;

    /// <summary>The queues this host's consumers read.</summary>
    public IReadOnlyCollection<string> Queues { get; internal set; } = [];

    /// <summary>
    /// The queues that read nothing: their read loop has gone quiet for longer than
    /// <see cref="PgmqOptions.StallAfter"/>, or consumers that ignored their timeout and never ended hold every slot.
    /// </summary>
    public IReadOnlyList<string> StalledQueues => _stalled?.Invoke() ?? [];

    /// <summary>
    /// True once the insert listener and the lease renewals had to take their connections from the pool, because one
    /// outside it could not be opened from the <see cref="PgmqOptions.DataSource"/>'s connection string. They then
    /// hold two pool slots for good, and wait behind the application when the pool is used up. Set
    /// <see cref="PgmqOptions.DedicatedConnectionString"/> to give them a connection string that works.
    /// </summary>
    public bool DedicatedConnectionsUsePool => _dedicatedOnPool?.Invoke() ?? false;

    /// <summary>Throws when Pgmq is not ready.</summary>
    /// <exception cref="InvalidOperationException">Pgmq is not ready, with the reason.</exception>
    public void EnsureReady()
    {
        if (_failure is { } failure)
        {
            throw new InvalidOperationException($"Pgmq is not available: {failure}");
        }
    }

    internal void MarkReady() => _failure = null;

    internal void MarkFailed(string reason) => _failure = reason;

    internal void ReportStalledFrom(Func<IReadOnlyList<string>> stalled) => _stalled = stalled;

    internal void ReportDedicatedOnPoolFrom(Func<bool> dedicatedOnPool) => _dedicatedOnPool = dedicatedOnPool;
}

/// <summary>
/// The failures the worker retries its own queue writes on (the outbox commit), in place: they run no consumer.
/// Consumers themselves are never run again.
/// </summary>
internal static class PgmqTransientFailures
{
    /// <summary>
    /// True for a transient database error, a timeout, a broken HTTP connection, or an HTTP 408, 429, 5xx or 520
    /// response, found anywhere in the exception or its inner exceptions.
    /// </summary>
    public static bool IsTransient(Exception exception)
    {
        for (Exception? ex = exception; ex is not null; ex = ex.InnerException)
        {
            if (IsTransientItself(ex))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsTransientItself(Exception ex) => ex switch
    {
        DbException { IsTransient: true } => true,
        TimeoutException => true,
        HttpIOException => true,
        HttpRequestException { StatusCode: { } status } => status is HttpStatusCode.RequestTimeout
                                                               or HttpStatusCode.TooManyRequests
                                                               or >= HttpStatusCode.InternalServerError,
        _ => false,
    };
}
