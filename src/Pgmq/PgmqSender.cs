using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace Pgmq;

/// <summary>The pgmq send functions the publisher writes with. An interface so the publisher can be tested alone.</summary>
internal interface IPgmqSender
{
    /// <summary><c>pgmq.send</c>: one message, visible at once or at <paramref name="deliverAt"/>.</summary>
    Task<long> SendAsync(string queueName, string body, IReadOnlyDictionary<string, object> headers,
        DateTimeOffset? deliverAt, CancellationToken cancellationToken);

    /// <summary><c>pgmq.send_batch</c>: several messages to one queue in one write.</summary>
    Task<IReadOnlyList<long>> SendBatchAsync(string queueName, IReadOnlyList<string> bodies,
        IReadOnlyList<IReadOnlyDictionary<string, object>> headers, DateTimeOffset? deliverAt,
        CancellationToken cancellationToken);

    /// <summary><c>pgmq.send_topic</c>: one message to every queue bound to a matching pattern.</summary>
    /// <returns>How many queues it reached.</returns>
    Task<int> SendTopicAsync(string routingKey, string body, IReadOnlyDictionary<string, object> headers,
        int delaySeconds, CancellationToken cancellationToken);

    /// <summary><c>pgmq.send_batch_topic</c>: several messages to every queue bound to a matching pattern.</summary>
    /// <returns>The queue each copy went to, one entry per copy.</returns>
    Task<IReadOnlyList<string>> SendBatchTopicAsync(string routingKey, IReadOnlyList<string> bodies,
        IReadOnlyList<IReadOnlyDictionary<string, object>> headers, int delaySeconds,
        CancellationToken cancellationToken);
}

/// <summary><see cref="IPgmqSender"/> calling the pgmq functions directly. Bodies are already JSON.</summary>
internal sealed class PgmqSender(NpgsqlDataSource dataSource) : IPgmqSender
{
    public async Task<long> SendAsync(string queueName, string body, IReadOnlyDictionary<string, object> headers,
        DateTimeOffset? deliverAt, CancellationToken cancellationToken)
    {
        await using NpgsqlCommand command = dataSource.CreateCommand(deliverAt is null
            ? "select * from pgmq.send($1, $2, $3)"
            : "select * from pgmq.send($1, $2, $3, $4)");
        command.Parameters.Add(Text(queueName));
        command.Parameters.Add(Jsonb(body));
        command.Parameters.Add(Jsonb(Serialize(headers)));
        if (deliverAt is { } at)
        {
            command.Parameters.Add(Timestamp(at));
        }

        return (long)(await command.ExecuteScalarAsync(cancellationToken))!;
    }

    public async Task<IReadOnlyList<long>> SendBatchAsync(string queueName, IReadOnlyList<string> bodies,
        IReadOnlyList<IReadOnlyDictionary<string, object>> headers, DateTimeOffset? deliverAt,
        CancellationToken cancellationToken)
    {
        await using NpgsqlCommand command = dataSource.CreateCommand(deliverAt is null
            ? "select * from pgmq.send_batch($1, $2, $3)"
            : "select * from pgmq.send_batch($1, $2, $3, $4)");
        command.Parameters.Add(Text(queueName));
        command.Parameters.Add(JsonbArray(bodies));
        command.Parameters.Add(JsonbArray([.. headers.Select(Serialize)]));
        if (deliverAt is { } at)
        {
            command.Parameters.Add(Timestamp(at));
        }

        List<long> ids = [];
        await using NpgsqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            ids.Add(reader.GetInt64(0));
        }

        return ids;
    }

    public async Task<int> SendTopicAsync(string routingKey, string body, IReadOnlyDictionary<string, object> headers,
        int delaySeconds, CancellationToken cancellationToken)
    {
        await using NpgsqlCommand command = dataSource.CreateCommand("select pgmq.send_topic($1, $2, $3, $4)");
        command.Parameters.Add(Text(routingKey));
        command.Parameters.Add(Jsonb(body));
        command.Parameters.Add(Jsonb(Serialize(headers)));
        command.Parameters.Add(Int(delaySeconds));

        return (int)(await command.ExecuteScalarAsync(cancellationToken))!;
    }

    public async Task<IReadOnlyList<string>> SendBatchTopicAsync(string routingKey, IReadOnlyList<string> bodies,
        IReadOnlyList<IReadOnlyDictionary<string, object>> headers, int delaySeconds,
        CancellationToken cancellationToken)
    {
        await using NpgsqlCommand command =
            dataSource.CreateCommand("select queue_name from pgmq.send_batch_topic($1, $2, $3, $4)");
        command.Parameters.Add(Text(routingKey));
        command.Parameters.Add(JsonbArray(bodies));
        command.Parameters.Add(JsonbArray([.. headers.Select(Serialize)]));
        command.Parameters.Add(Int(delaySeconds));

        List<string> queues = [];
        await using NpgsqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            queues.Add(reader.GetString(0));
        }

        return queues;
    }

    private static string Serialize(IReadOnlyDictionary<string, object> headers) => JsonSerializer.Serialize(headers);

    private static NpgsqlParameter Text(string value) => new() { Value = value, NpgsqlDbType = NpgsqlDbType.Text };

    private static NpgsqlParameter Int(int value) => new() { Value = value, NpgsqlDbType = NpgsqlDbType.Integer };

    private static NpgsqlParameter Jsonb(string json) => new() { Value = json, NpgsqlDbType = NpgsqlDbType.Jsonb };

    private static NpgsqlParameter JsonbArray(IReadOnlyList<string> json)
        => new() { Value = json.ToArray(), NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Jsonb };

    private static NpgsqlParameter Timestamp(DateTimeOffset at)
        => new() { Value = at.ToUniversalTime(), NpgsqlDbType = NpgsqlDbType.TimestampTz };
}
