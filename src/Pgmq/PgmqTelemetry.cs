using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Reflection;
using Npgsql;

namespace Pgmq;

/// <summary>
/// Pgmq's traces and metrics, following the OpenTelemetry messaging conventions. Pgmq only emits them, through the
/// .NET diagnostics APIs; it has no OpenTelemetry dependency and exports nothing. The application decides what is
/// collected, sampled and exported, by adding the source and the meter to its own OpenTelemetry setup:
/// <code>
/// builder.Services.AddOpenTelemetry()
///     .WithTracing(t => t.AddSource(PgmqTelemetry.SourceName))
///     .WithMetrics(m => m.AddMeter(PgmqTelemetry.MeterName));
/// </code>
/// A send span's context travels in the message headers and the handling span starts as its child, so one trace runs
/// from the code that published a message through every consumer of it.
/// </summary>
public static class PgmqTelemetry
{
    /// <summary>The <see cref="ActivitySource"/> name of every Pgmq span.</summary>
    public const string SourceName = "Pgmq";

    /// <summary>The <see cref="Meter"/> name of every Pgmq instrument.</summary>
    public const string MeterName = "Pgmq";

    internal const string System = "pgmq";

    /// <summary>The package version, without the commit hash.</summary>
    internal static readonly string? Version = typeof(PgmqTelemetry).Assembly
        .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0];

    private static readonly ActivitySource Source = new(SourceName, Version);
    private static readonly Meter Meter = new(MeterName, Version);

    /// <summary>The bucket boundaries the OpenTelemetry messaging conventions recommend for durations.</summary>
    private static readonly InstrumentAdvice<double> DurationAdvice = new()
    {
        HistogramBucketBoundaries = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10],
    };

    internal static readonly Counter<long> SentMessages = Meter.CreateCounter<long>(
        "messaging.client.sent.messages", "{message}", "Messages written to a queue.");

    internal static readonly Histogram<double> OperationDuration = Meter.CreateHistogram(
        "messaging.client.operation.duration", "s",
        "Duration of a send, or of settling a handled message (delete, archive, set_vt).", tags: null,
        advice: DurationAdvice);

    internal static readonly Counter<long> ConsumedMessages = Meter.CreateCounter<long>(
        "messaging.client.consumed.messages", "{message}", "Messages read off a queue by the worker.");

    internal static readonly Histogram<double> ProcessDuration = Meter.CreateHistogram(
        "messaging.process.duration", "s",
        "Duration of handling one message, by outcome (messaging.pgmq.outcome).", tags: null, advice: DurationAdvice);

    internal static readonly UpDownCounter<long> ActiveMessages = Meter.CreateUpDownCounter<long>(
        "pgmq.consumer.messages.active", "{message}", "Messages being handled right now.");

    internal static readonly Counter<long> ReadFailures = Meter.CreateCounter<long>(
        "pgmq.consumer.read.failures", "{failure}", "Reads of a queue that failed and will be tried again.");

    internal static readonly Counter<long> ReadLoopRestarts = Meter.CreateCounter<long>(
        "pgmq.consumer.read_loop.restarts", "{restart}", "Read loops that failed and were started again.");

    internal static readonly Counter<long> LeaseRenewalFailures = Meter.CreateCounter<long>(
        "pgmq.consumer.lease_renewal.failures", "{failure}", "Lease renewals that failed.");

    internal static readonly Counter<long> SettleFailures = Meter.CreateCounter<long>(
        "pgmq.consumer.settle.failures", "{failure}",
        "Outcomes that could not be written to the queue; the message comes back after its lease.");

    /// <summary>Starts a span for writing a message to <paramref name="destination"/>: a queue, a routing key, or a message type that fans out.</summary>
    internal static Activity? StartSend(string destination, string messageType, ServerEndpoint server)
        => Source.StartActivity($"send {destination}", ActivityKind.Producer)
            ?.SetTag("messaging.system", System)
            .SetTag("messaging.operation.type", "send")
            .SetTag("messaging.operation.name", "send")
            .SetTag("messaging.destination.name", destination)
            .SetTag("messaging.message.type", messageType)
            .SetTag("server.address", server.Address)
            .SetTag("server.port", server.Port);

    /// <summary>Starts the span of handling one message, as a child of the span that sent it.</summary>
    internal static Activity? StartProcess(string queueName, string consumer, MessageHeaders? headers, long msgId,
        int attempt, ServerEndpoint server)
    {
        ActivityContext.TryParse(headers?.TraceParent, headers?.TraceState, out ActivityContext parent);

        return Source.StartActivity($"process {queueName}", ActivityKind.Consumer, parent)
            ?.SetTag("messaging.system", System)
            .SetTag("messaging.operation.type", "process")
            .SetTag("messaging.operation.name", "process")
            .SetTag("messaging.destination.name", queueName)
            .SetTag("messaging.consumer.group.name", consumer)
            .SetTag("messaging.message.type", headers?.MessageType)
            .SetTag("messaging.message.id", headers?.MessageId?.ToString())
            .SetTag("messaging.pgmq.msg_id", msgId)
            .SetTag("messaging.pgmq.attempt", attempt)
            .SetTag("server.address", server.Address)
            .SetTag("server.port", server.Port);
    }

    /// <summary>Starts the span of writing a handled message's outcome: <c>delete</c>, <c>archive</c> or <c>set_vt</c>.</summary>
    internal static Activity? StartSettle(string operation, string queueName, long msgId, ServerEndpoint server)
        => Source.StartActivity($"{operation} {queueName}", ActivityKind.Client)
            ?.SetTag("messaging.system", System)
            .SetTag("messaging.operation.type", "settle")
            .SetTag("messaging.operation.name", operation)
            .SetTag("messaging.destination.name", queueName)
            .SetTag("messaging.pgmq.msg_id", msgId)
            .SetTag("server.address", server.Address)
            .SetTag("server.port", server.Port);

    /// <summary>The <c>error.type</c> of an exception: its full type name.</summary>
    internal static string ErrorType(Exception exception) => exception.GetType().FullName ?? exception.GetType().Name;

    /// <summary>Marks a span failed with <paramref name="errorType"/>, recording the exception when there is one.</summary>
    internal static void Fail(Activity? activity, string errorType, Exception? exception = null)
    {
        if (activity is null)
        {
            return;
        }

        activity.SetStatus(ActivityStatusCode.Error, exception?.Message ?? errorType);
        activity.SetTag("error.type", errorType);
        if (exception is not null)
        {
            activity.AddException(exception);
        }
    }

    /// <summary>Records one client operation (a send or a settle) in <c>messaging.client.operation.duration</c>.</summary>
    internal static void RecordOperation(string operationType, string operationName, string destination,
        long startTimestamp, string? errorType, ServerEndpoint server)
    {
        if (!OperationDuration.Enabled)
        {
            return;
        }

        TagList tags = new()
        {
            { "messaging.system", System },
            { "messaging.operation.type", operationType },
            { "messaging.operation.name", operationName },
            { "messaging.destination.name", destination },
        };
        AddServer(ref tags, server);
        AddError(ref tags, errorType);
        OperationDuration.Record(Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds, tags);
    }

    /// <summary>Counts messages written to one queue (or routing key) in <c>messaging.client.sent.messages</c>.</summary>
    internal static void RecordSent(string destination, long count, ServerEndpoint server)
    {
        if (!SentMessages.Enabled || count <= 0)
        {
            return;
        }

        TagList tags = new()
        {
            { "messaging.system", System },
            { "messaging.operation.name", "send" },
            { "messaging.destination.name", destination },
        };
        AddServer(ref tags, server);
        SentMessages.Add(count, tags);
    }

    /// <summary>The tags of a consumer's queue, shared by the worker's instruments.</summary>
    internal static TagList ConsumerTags(string queueName, string consumer) => new()
    {
        { "messaging.system", System },
        { "messaging.destination.name", queueName },
        { "messaging.consumer.group.name", consumer },
    };

    /// <summary>Records one handled message in <c>messaging.process.duration</c>.</summary>
    internal static void RecordProcess(string queueName, string consumer, string outcome, string? errorType,
        long startTimestamp)
    {
        if (!ProcessDuration.Enabled)
        {
            return;
        }

        TagList tags = ConsumerTags(queueName, consumer);
        tags.Add("messaging.operation.name", "process");
        tags.Add("messaging.pgmq.outcome", outcome);
        AddError(ref tags, errorType);
        ProcessDuration.Record(Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds, tags);
    }

    private static void AddServer(ref TagList tags, ServerEndpoint server)
    {
        if (server.Address is not null)
        {
            tags.Add("server.address", server.Address);
        }

        if (server.Port is not null)
        {
            tags.Add("server.port", server.Port);
        }
    }

    private static void AddError(ref TagList tags, string? errorType)
    {
        if (errorType is not null)
        {
            tags.Add("error.type", errorType);
        }
    }
}

/// <summary>The database server, as the <c>server.address</c> and <c>server.port</c> of spans and metrics.</summary>
internal readonly record struct ServerEndpoint(string? Address, int? Port)
{
    public static ServerEndpoint Of(NpgsqlDataSource dataSource)
    {
        try
        {
            NpgsqlConnectionStringBuilder builder = new(dataSource.ConnectionString);

            // Several hosts have a port each, or share one; name them all and leave the port out.
            return string.IsNullOrEmpty(builder.Host)
                ? default
                : new(builder.Host, builder.Host.Contains(',', StringComparison.Ordinal) ? null : builder.Port);
        }
        catch (ArgumentException)
        {
            return default;
        }
    }
}
