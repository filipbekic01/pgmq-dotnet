using System.Diagnostics;

namespace Pgmq;

/// <summary>
/// Tracing for sending and handling messages, following the OpenTelemetry messaging conventions. A send span's
/// context travels in the message headers and the handling span starts as its child, so one trace runs from the code
/// that published a message through every consumer of it. Add <see cref="SourceName"/> to your tracer to see them.
/// </summary>
public static class PgmqTelemetry
{
    /// <summary>The <see cref="ActivitySource"/> name of every Pgmq span.</summary>
    public const string SourceName = "Pgmq";

    private static readonly ActivitySource Source = new(SourceName);

    internal static Activity? StartSend(string messageName, string? queueName = null)
        => Source.StartActivity($"send {messageName}", ActivityKind.Producer)
            ?.SetTag("messaging.system", "pgmq")
            .SetTag("messaging.operation.type", "send")
            .SetTag("messaging.message.type", messageName)
            .SetTag("messaging.destination.name", queueName);

    internal static Activity? StartProcess(MessageHeaders headers, string queueName, long msgId, int attempt)
    {
        ActivityContext.TryParse(headers.TraceParent, headers.TraceState, out ActivityContext parent);

        return Source.StartActivity($"process {headers.MessageType}", ActivityKind.Consumer, parent)
            ?.SetTag("messaging.system", "pgmq")
            .SetTag("messaging.operation.type", "process")
            .SetTag("messaging.message.type", headers.MessageType)
            .SetTag("messaging.destination.name", queueName)
            .SetTag("messaging.message.id", headers.MessageId?.ToString())
            .SetTag("messaging.pgmq.msg_id", msgId)
            .SetTag("messaging.pgmq.attempt", attempt);
    }

    internal static void Fail(Activity? activity, Exception exception, string outcome)
    {
        activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
        activity?.AddException(exception);
        activity?.SetTag("messaging.outcome", outcome);
    }
}
