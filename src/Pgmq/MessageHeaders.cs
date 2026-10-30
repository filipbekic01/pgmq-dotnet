using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Pgmq;

/// <summary>
/// The pgmq headers every message carries. All values are written as strings, so they read back the same whatever
/// the JSON converter makes of them. <see cref="TraceParent"/> and <see cref="TraceState"/> are the W3C trace context
/// of the sender, so the consumer's span joins the trace that published the message.
/// </summary>
internal sealed record MessageHeaders(
    string MessageType,
    Guid? MessageId,
    int RedeliveryCount,
    string? TraceParent = null,
    string? TraceState = null)
{
    private const string MessageTypeKey = "message-type";
    private const string MessageIdKey = "message-id";
    private const string RedeliveryCountKey = "redelivery-count";
    private const string TraceParentKey = "traceparent";
    private const string TraceStateKey = "tracestate";

    /// <summary>Fresh headers for a message sent now, under the current trace.</summary>
    public static MessageHeaders New(Type messageType, int redeliveryCount = 0, Guid? messageId = null)
    {
        Activity? current = Activity.Current;
        bool w3c = current is { IdFormat: ActivityIdFormat.W3C };

        return new(MessageNames.Of(messageType), messageId ?? Guid.CreateVersion7(), redeliveryCount,
            w3c ? current!.Id : null, w3c ? current!.TraceStateString : null);
    }

    public IReadOnlyDictionary<string, object> ToDictionary()
    {
        Dictionary<string, object> headers = new()
        {
            [MessageTypeKey] = MessageType,
            [RedeliveryCountKey] = RedeliveryCount.ToString(CultureInfo.InvariantCulture),
        };

        if (MessageId is { } messageId)
        {
            headers[MessageIdKey] = messageId.ToString();
        }

        if (TraceParent is not null)
        {
            headers[TraceParentKey] = TraceParent;
        }

        if (TraceState is not null)
        {
            headers[TraceStateKey] = TraceState;
        }

        return headers;
    }

    /// <summary>The headers of a message read off a queue, or null when it carries no message type.</summary>
    public static MessageHeaders? From(IReadOnlyDictionary<string, object>? headers)
    {
        if (headers is null || Read(headers, MessageTypeKey) is not { } messageType)
        {
            return null;
        }

        return new(
            messageType,
            Guid.TryParse(Read(headers, MessageIdKey), out Guid messageId) ? messageId : null,
            int.TryParse(Read(headers, RedeliveryCountKey), CultureInfo.InvariantCulture, out int count) ? count : 0,
            Read(headers, TraceParentKey),
            Read(headers, TraceStateKey));
    }

    private static string? Read(IReadOnlyDictionary<string, object> headers, string key)
        => headers.TryGetValue(key, out object? value) ? Convert.ToString(value, CultureInfo.InvariantCulture) : null;
}

/// <summary>The name a message type travels under: <see cref="MessageNameAttribute"/>, or its full name.</summary>
internal static class MessageNames
{
    public static string Of(Type messageType)
        => messageType.GetCustomAttribute<MessageNameAttribute>()?.Name ?? messageType.FullName ?? messageType.Name;
}

/// <summary>The one JSON shape messages are written and read in.</summary>
internal static class MessageSerializer
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() },
    };

    public static string Serialize(object message, Type messageType)
        => JsonSerializer.Serialize(message, messageType, Options);

    public static object Deserialize(string json, Type messageType)
        => JsonSerializer.Deserialize(json, messageType, Options)
           ?? throw new JsonException($"A message of type {messageType.FullName} deserialized to null.");
}
