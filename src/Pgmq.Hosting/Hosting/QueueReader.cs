using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace Pgmq.Hosting;

/// <summary>A message as read off a queue: its pgmq id, how often it was read, its JSON body and its headers.</summary>
internal sealed record QueueMessage(long MsgId, int ReadCt, string? Body, IReadOnlyDictionary<string, object>? Headers)
{
    /// <summary>pgmq's FIFO group, or null for the default group.</summary>
    public string? GroupId => Headers?.TryGetValue(MessageHeaders.GroupKey, out object? group) == true
        ? group.ToString()
        : null;
}

/// <summary>
/// Reads a consumer's queue with the pgmq function its settings call for: <c>read</c>, the grouped FIFO reads,
/// <c>pop</c>, each with a conditional filter or a long poll where pgmq has one.
/// </summary>
internal sealed class QueueReader(NpgsqlDataSource dataSource, PgmqOptions options)
{
    private const string Columns = "msg_id, read_ct, message::text, headers::text";

    /// <summary>
    /// Reads up to <paramref name="quantity"/> messages. With <paramref name="longPoll"/>, which needs
    /// <see cref="CanLongPoll"/>, an empty queue is waited on inside the database for up to
    /// <see cref="PgmqOptions.LongPollTimeout"/>.
    /// </summary>
    public async Task<List<QueueMessage>> ReadAsync(ConsumerRegistration consumer, int leaseSeconds, int quantity,
        bool longPoll, CancellationToken cancellationToken)
    {
        longPoll = longPoll && CanLongPoll(consumer);
        await using NpgsqlCommand command = dataSource.CreateCommand(Sql(consumer, longPoll));
        command.Parameters.Add(new NpgsqlParameter { Value = consumer.QueueName, NpgsqlDbType = NpgsqlDbType.Text });

        if (!consumer.AtMostOnce)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = leaseSeconds, NpgsqlDbType = NpgsqlDbType.Integer });
        }

        command.Parameters.Add(new NpgsqlParameter { Value = quantity, NpgsqlDbType = NpgsqlDbType.Integer });

        if (longPoll)
        {
            int pollSeconds = (int)Math.Ceiling(options.LongPollTimeout!.Value.TotalSeconds);
            int intervalMs = Math.Max(10, (int)options.MinIdleDelay.TotalMilliseconds);
            command.Parameters.Add(new NpgsqlParameter { Value = pollSeconds, NpgsqlDbType = NpgsqlDbType.Integer });
            command.Parameters.Add(new NpgsqlParameter { Value = intervalMs, NpgsqlDbType = NpgsqlDbType.Integer });

            // The command must outlive the poll it asks the database for.
            command.CommandTimeout = Math.Max(command.CommandTimeout, pollSeconds + 30);
        }

        if (consumer.Filter is not null)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = consumer.Filter, NpgsqlDbType = NpgsqlDbType.Jsonb });
        }

        await using NpgsqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken);

        List<QueueMessage> messages = [];
        while (await reader.ReadAsync(cancellationToken))
        {
            messages.Add(new(
                reader.GetInt64(0),
                reader.GetInt32(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : ParseHeaders(reader.GetString(3))));
        }

        // pgmq returns a batch in no promised order; handle it oldest first.
        messages.Sort((a, b) => a.MsgId.CompareTo(b.MsgId));
        return messages;
    }

    /// <summary>
    /// Whether this consumer's reads can wait inside the database for a message: long polling is on, and the consumer
    /// does not read with <c>pop</c>, which pgmq cannot long-poll.
    /// </summary>
    public bool CanLongPoll(ConsumerRegistration consumer) => options.LongPollTimeout is not null && !consumer.AtMostOnce;

    private static string Sql(ConsumerRegistration consumer, bool poll)
    {
        if (consumer.AtMostOnce)
        {
            return $"select {Columns} from pgmq.pop($1, $2)";
        }

        string function = consumer.ReadMode switch
        {
            PgmqReadMode.Standard => "read",
            PgmqReadMode.Grouped => "read_grouped",
            PgmqReadMode.GroupedRoundRobin => "read_grouped_rr",
            PgmqReadMode.GroupedHead => "read_grouped_head",
            _ => throw new InvalidOperationException($"Unknown read mode {consumer.ReadMode}."),
        };

        string arguments = (poll, consumer.Filter is not null) switch
        {
            (false, false) => "$1, $2, $3",
            (false, true) => "$1, $2, $3, $4",
            (true, false) => "$1, $2, $3, $4, $5",
            (true, true) => "$1, $2, $3, $4, $5, $6",
        };

        return $"select {Columns} from pgmq.{function}{(poll ? "_with_poll" : "")}({arguments})";
    }

    /// <summary>Headers as the rest of the worker expects them: every value as the text it holds.</summary>
    private static Dictionary<string, object>? ParseHeaders(string json)
    {
        using JsonDocument document = JsonDocument.Parse(json);
        if (document.RootElement.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        Dictionary<string, object> headers = new(StringComparer.Ordinal);
        foreach (JsonProperty property in document.RootElement.EnumerateObject())
        {
            headers[property.Name] = property.Value.ValueKind == JsonValueKind.String
                ? property.Value.GetString()!
                : property.Value.GetRawText();
        }

        return headers;
    }
}
