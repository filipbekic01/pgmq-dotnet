using Npgsql;
using NpgsqlTypes;

namespace Pgmq.Hosting;

/// <summary>
/// Writes the deletes and archives of every queue in the host through one connection at a time. A message that is
/// done asks here and waits; whatever was asked for while a write was on its way goes out together in the next one, a
/// <c>pgmq.delete</c> or <c>pgmq.archive</c> per queue in one batch. So seventy consumers that finish at once cost
/// one or two round trips on one connection, not seventy connections, and a host that is not busy writes at once: a
/// write never waits for a timer.
/// </summary>
internal sealed class SettleBatcher
{
    private readonly Func<IReadOnlyList<SettleGroup>, Task> _write;
    private readonly Lock _lock = new();
    private List<Pending> _pending = [];
    private bool _writing;

    public SettleBatcher(NpgsqlDataSource dataSource)
        : this(groups => WriteAsync(dataSource, groups))
    {
    }

    /// <param name="write">
    /// Writes the groups, giving each its <see cref="SettleGroup.Settled"/> ids or its
    /// <see cref="SettleGroup.Failure"/>; throwing fails them all.
    /// </param>
    internal SettleBatcher(Func<IReadOnlyList<SettleGroup>, Task> write) => _write = write;

    /// <summary>Deletes a message.</summary>
    /// <returns>False when it was no longer on the queue.</returns>
    public Task<bool> DeleteAsync(string queue, long messageId) => EnqueueAsync(queue, messageId, archive: false);

    /// <summary>Moves a message to its queue's archive.</summary>
    /// <returns>False when it was no longer on the queue.</returns>
    public Task<bool> ArchiveAsync(string queue, long messageId) => EnqueueAsync(queue, messageId, archive: true);

    private Task<bool> EnqueueAsync(string queue, long messageId, bool archive)
    {
        Pending pending = new(queue, messageId, archive);
        bool start = false;

        lock (_lock)
        {
            _pending.Add(pending);
            if (!_writing)
            {
                _writing = start = true;
            }
        }

        if (start)
        {
            _ = Task.Run(WriteUntilEmptyAsync);
        }

        return pending.Done.Task;
    }

    /// <summary>The one writer: takes everything that waits, writes it, and ends when nothing waits any more.</summary>
    private async Task WriteUntilEmptyAsync()
    {
        while (true)
        {
            List<Pending> batch;
            lock (_lock)
            {
                if (_pending.Count == 0)
                {
                    _writing = false;
                    return;
                }

                batch = _pending;
                _pending = [];
            }

            List<(SettleGroup Group, List<Pending> Waiting)> groups =
            [
                .. batch
                    .GroupBy(p => (p.Queue, p.Archive))
                    .Select(g => (new SettleGroup(g.Key.Queue, g.Key.Archive, [.. g.Select(p => p.MessageId).Distinct()]),
                        g.ToList())),
            ];

            Exception? failure = null;
            try
            {
                await _write([.. groups.Select(g => g.Group)]);
            }
            catch (Exception ex)
            {
                failure = ex;
            }

            foreach ((SettleGroup group, List<Pending> waiting) in groups)
            {
                foreach (Pending pending in waiting)
                {
                    if ((failure ?? group.Failure) is { } ex)
                    {
                        pending.Done.TrySetException(ex);
                    }
                    else
                    {
                        pending.Done.TrySetResult(group.Settled?.Contains(pending.MessageId) == true);
                    }
                }
            }
        }
    }

    /// <summary>
    /// Every group in one batch, which is one round trip and one transaction. If the database refuses it, one broken
    /// queue failed them all: each group is then written on its own, so the others are still settled.
    /// </summary>
    private static async Task WriteAsync(NpgsqlDataSource dataSource, IReadOnlyList<SettleGroup> groups)
    {
        try
        {
            await using NpgsqlBatch batch = dataSource.CreateBatch();
            foreach (SettleGroup group in groups)
            {
                NpgsqlBatchCommand command = new(Sql(group));
                AddParameters(command.Parameters, group);
                batch.BatchCommands.Add(command);
            }

            List<HashSet<long>> settled = new(groups.Count);
            await using (NpgsqlDataReader reader = await batch.ExecuteReaderAsync(CancellationToken.None))
            {
                do
                {
                    settled.Add(await ReadIdsAsync(reader));
                } while (await reader.NextResultAsync(CancellationToken.None));
            }

            if (settled.Count != groups.Count)
            {
                throw new InvalidOperationException(
                    $"Settling {groups.Count} queues answered with {settled.Count} results.");
            }

            for (int i = 0; i < groups.Count; i++)
            {
                groups[i].Settled = settled[i];
            }
        }
        catch (PostgresException) when (groups.Count > 1)
        {
            foreach (SettleGroup group in groups)
            {
                try
                {
                    await using NpgsqlCommand command = dataSource.CreateCommand(Sql(group));
                    AddParameters(command.Parameters, group);
                    await using NpgsqlDataReader reader = await command.ExecuteReaderAsync(CancellationToken.None);
                    group.Settled = await ReadIdsAsync(reader);
                }
                catch (Exception ex)
                {
                    group.Failure = ex;
                }
            }
        }
    }

    private static string Sql(SettleGroup group)
        => group.Archive ? "select * from pgmq.archive($1, $2)" : "select * from pgmq.delete($1, $2)";

    private static void AddParameters(NpgsqlParameterCollection parameters, SettleGroup group)
    {
        parameters.Add(new() { Value = group.Queue, NpgsqlDbType = NpgsqlDbType.Text });
        parameters.Add(new() { Value = group.MessageIds, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint });
    }

    private static async Task<HashSet<long>> ReadIdsAsync(NpgsqlDataReader reader)
    {
        HashSet<long> ids = [];
        while (await reader.ReadAsync(CancellationToken.None))
        {
            ids.Add(reader.GetInt64(0));
        }

        return ids;
    }

    private sealed record Pending(string Queue, long MessageId, bool Archive)
    {
        public TaskCompletionSource<bool> Done { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}

/// <summary>The messages of one queue to delete, or to archive, in one statement.</summary>
internal sealed class SettleGroup(string queue, bool archive, long[] messageIds)
{
    public string Queue { get; } = queue;

    public bool Archive { get; } = archive;

    public long[] MessageIds { get; } = messageIds;

    /// <summary>The messages the statement found and settled; one missing here was no longer on the queue.</summary>
    public HashSet<long>? Settled { get; set; }

    /// <summary>Why this group could not be written, when the others could.</summary>
    public Exception? Failure { get; set; }
}
