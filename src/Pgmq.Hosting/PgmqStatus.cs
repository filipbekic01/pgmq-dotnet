using System.Data.Common;
using System.Net;

namespace Pgmq;

/// <summary>Whether Pgmq came up in this host, which queues it serves, and which of them stopped reading.</summary>
public sealed class PgmqStatus
{
    private volatile string? _failure = "Not set up yet.";
    private Func<IReadOnlyList<string>>? _stalled;

    /// <summary>True once the extension and every queue exist.</summary>
    public bool IsReady => _failure is null;

    /// <summary>Why Pgmq is not ready; null when it is.</summary>
    public string? Failure => _failure;

    /// <summary>The queues this host's consumers read.</summary>
    public IReadOnlyCollection<string> Queues { get; internal set; } = [];

    /// <summary>The queues whose read loop has gone quiet for longer than <see cref="PgmqOptions.StallAfter"/>.</summary>
    public IReadOnlyList<string> StalledQueues => _stalled?.Invoke() ?? [];

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
}

/// <summary>Which failures the worker tries again by default: those that say nothing about the message itself.</summary>
public static class PgmqTransientFailures
{
    /// <summary>
    /// True for a transient database error, a timeout, a broken HTTP connection, or an HTTP 408, 429, 5xx or 520
    /// response, found anywhere in the exception or its inner exceptions.
    /// <code>
    /// options.IsTransient = ex => PgmqTransientFailures.IsTransient(ex) || ex is LockTimeoutException;
    /// </code>
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
