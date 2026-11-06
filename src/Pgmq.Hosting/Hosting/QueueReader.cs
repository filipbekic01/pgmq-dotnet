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
/// Reads a consumer's queue with the pgmq function its settings call for: <c>read</c> (with a conditional filter), the
/// grouped FIFO reads, or <c>pop</c>. Every read in the host takes one of <see cref="PgmqOptions.MaxConcurrentReads"/>
/// slots, held only while it runs, so reads never use more connections than that however many queues are busy.
/// </summary>
internal sealed class QueueReader(NpgsqlDataSource dataSource, PgmqOptions options)
{
    private const string Columns = "msg_id, read_ct, message::text, headers::text";

    private readonly SemaphoreSlim _slots = new(options.MaxConcurrentReads, options.MaxConcurrentReads);

    /// <summary>
    /// Reads up to <paramref name="quantity"/> messages, once a read slot is free. While it waits for one it calls
    /// <paramref name="stillWaiting"/> every <see cref="PgmqOptions.Heartbeat"/>.
    /// </summary>
    public async Task<List<QueueMessage>> ReadAsync(ConsumerRegistration consumer, int leaseSeconds, int quantity,
        Action? stillWaiting, CancellationToken cancellationToken)
    {
        if (!_slots.Wait(0))
        {
            PgmqTelemetry.ReadsWaiting.Add(1);
            try
            {
                while (!await _slots.WaitAsync(options.Heartbeat, cancellationToken))
                {
                    stillWaiting?.Invoke();
                }
            }
            finally
            {
                PgmqTelemetry.ReadsWaiting.Add(-1);
            }
        }

        try
        {
            return await ReadNowAsync(consumer, leaseSeconds, quantity, cancellationToken);
        }
        finally
        {
            _slots.Release();
        }
    }

    private async Task<List<QueueMessage>> ReadNowAsync(ConsumerRegistration consumer, int leaseSeconds, int quantity,
        CancellationToken cancellationToken)
    {
        // Getting a connection can be cancelled; nothing has been read yet.
        await using NpgsqlConnection connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using NpgsqlCommand command = new(Sql(consumer), connection);
        command.Parameters.Add(new NpgsqlParameter { Value = consumer.QueueName, NpgsqlDbType = NpgsqlDbType.Text });

        if (!consumer.AtMostOnce)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = leaseSeconds, NpgsqlDbType = NpgsqlDbType.Integer });
        }

        command.Parameters.Add(new NpgsqlParameter { Value = quantity, NpgsqlDbType = NpgsqlDbType.Integer });

        if (consumer.Filter is not null)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = consumer.Filter, NpgsqlDbType = NpgsqlDbType.Jsonb });
        }

        // Once sent, the read is not cancelled: a pop cancelled after it ran would lose its messages, and a read would
        // leave them hidden for a lease with an attempt spent. It is one short query, and the caller handles what it
        // returns even while stopping.
        await using NpgsqlDataReader reader = await command.ExecuteReaderAsync(CancellationToken.None);

        List<QueueMessage> messages = [];
        while (await reader.ReadAsync(CancellationToken.None))
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

    /// <summary>The pgmq read this consumer's settings call for.</summary>
    internal static string Sql(ConsumerRegistration consumer)
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

        return consumer.Filter is null
            ? $"select {Columns} from pgmq.{function}($1, $2, $3)"
            : $"select {Columns} from pgmq.{function}($1, $2, $3, $4)";
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
