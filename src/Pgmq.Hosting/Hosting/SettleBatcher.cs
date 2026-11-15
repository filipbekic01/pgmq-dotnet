using System.Collections.Concurrent;
using Npgsql;
using NpgsqlTypes;

namespace Pgmq.Hosting;

/// <summary>What to write to the queue for a message that needs no transaction of its own.</summary>
internal enum SettleKind
{
    /// <summary><c>pgmq.delete</c>.</summary>
    Delete,

    /// <summary><c>pgmq.archive</c>.</summary>
    Archive,

    /// <summary><c>pgmq.set_vt</c>: visible again after a delay.</summary>
    Release,

    /// <summary>Visible again at once, with the read it was counted for taken back.</summary>
    ReturnUnread,
}

/// <summary>
/// Writes the deletes, archives and releases of every queue in the host through one connection at a time. A message
/// that is done asks here and waits; whatever was asked for while a write was on its way goes out together in the next
/// one, a statement per queue and kind in one batch. So seventy consumers that finish at once cost one or two round
/// trips on one connection, not seventy connections, and a host that is not busy writes at once: a write never waits
/// for a timer.
/// <para>
/// A batch is one transaction, so one queue that cannot be written (its table locked, or gone) fails the batch it is
/// in. Every queue of a failed batch is then written apart, each on a writer and a connection of its own, until a
/// write of it succeeds: the queue at fault keeps failing alone, and the others are back in the shared batch after one
/// more write. Nothing is reported to the messages of a batch that failed for several queues at once; they are simply
/// written again, apart.
/// </para>
/// </summary>
internal sealed class SettleBatcher
{
    private readonly Func<IReadOnlyList<SettleGroup>, Task> _write;
    private readonly Lane _shared = new(null);

    /// <summary>The queues written apart from the shared batch, each by its own lane, until one write succeeds.</summary>
    private readonly ConcurrentDictionary<string, Lane> _apart = new(StringComparer.Ordinal);

    public SettleBatcher(NpgsqlDataSource dataSource)
        : this(groups => WriteAsync(dataSource, groups))
    {
    }

    /// <param name="write">
    /// Writes the groups as one, giving each its <see cref="SettleGroup.Settled"/> ids; throwing means none of them
    /// was written.
    /// </param>
    internal SettleBatcher(Func<IReadOnlyList<SettleGroup>, Task> write) => _write = write;

    /// <summary>Deletes a message.</summary>
    /// <returns>False when it was no longer on the queue.</returns>
    public Task<bool> DeleteAsync(string queue, long messageId) => EnqueueAsync(queue, messageId, SettleKind.Delete);

    /// <summary>Moves a message to its queue's archive.</summary>
    /// <returns>False when it was no longer on the queue.</returns>
    public Task<bool> ArchiveAsync(string queue, long messageId) => EnqueueAsync(queue, messageId, SettleKind.Archive);

    /// <summary>Makes a message visible again after <paramref name="delay"/>.</summary>
    /// <returns>False when it was no longer on the queue.</returns>
    public Task<bool> ReleaseAsync(string queue, long messageId, TimeSpan delay)
        => EnqueueAsync(queue, messageId, SettleKind.Release,
            delay > TimeSpan.Zero ? (int)Math.Ceiling(delay.TotalSeconds) : 0);

    /// <summary>
    /// Makes a message visible again that was read but never handed to a consumer, and takes back the read it was
    /// counted for, so a message that only waited behind another does not use up its attempts.
    /// </summary>
    /// <returns>False when it was no longer on the queue.</returns>
    public Task<bool> ReturnUnreadAsync(string queue, long messageId)
        => EnqueueAsync(queue, messageId, SettleKind.ReturnUnread);

    private Task<bool> EnqueueAsync(string queue, long messageId, SettleKind kind, int delaySeconds = 0)
    {
        Pending pending = new(queue, messageId, kind, delaySeconds);
        Route(pending);
        return pending.Done.Task;
    }

    private void Route(Pending pending)
        => Add(_apart.TryGetValue(pending.Queue, out Lane? lane) ? lane : _shared, pending);

    private void Add(Lane lane, Pending pending)
    {
        bool start = false;
        lock (lane.Lock)
        {
            lane.Pending.Add(pending);
            if (!lane.Writing)
            {
                lane.Writing = start = true;
            }
        }

        if (start)
        {
            _ = Task.Run(() => WriteUntilEmptyAsync(lane));
        }
    }

    /// <summary>A lane's one writer: takes everything that waits, writes it, and ends when nothing waits any more.</summary>
    private async Task WriteUntilEmptyAsync(Lane lane)
    {
        while (true)
        {
            List<Pending> batch;
            lock (lane.Lock)
            {
                if (lane.Pending.Count == 0)
                {
                    lane.Writing = false;
                    return;
                }

                batch = lane.Pending;
                lane.Pending = [];
            }

            try
            {
                await WriteBatchAsync(lane, batch);
            }
            catch (Exception ex)
            {
                // Whatever went wrong, nobody waits forever and the writer goes on.
                foreach (Pending pending in batch)
                {
                    pending.Done.TrySetException(ex);
                }
            }
        }
    }

    private async Task WriteBatchAsync(Lane lane, List<Pending> batch)
    {
        bool shared = lane.Queue is null;
        if (shared)
        {
            // A queue set apart since these were asked for is written by its own lane.
            List<Pending> stay = new(batch.Count);
            foreach (Pending pending in batch)
            {
                if (_apart.TryGetValue(pending.Queue, out Lane? apart))
                {
                    Add(apart, pending);
                }
                else
                {
                    stay.Add(pending);
                }
            }

            if (stay.Count == 0)
            {
                return;
            }

            batch = stay;
        }

        List<(SettleGroup Group, List<Pending> Waiting)> groups =
        [
            .. batch
                .GroupBy(p => (p.Queue, p.Kind, p.DelaySeconds))
                .Select(g => (
                    new SettleGroup(g.Key.Queue, g.Key.Kind, g.Key.DelaySeconds,
                        [.. g.Select(p => p.MessageId).Distinct()]),
                    g.ToList())),
        ];

        try
        {
            await _write([.. groups.Select(g => g.Group)]);
        }
        catch (Exception ex) when (shared)
        {
            // Which queue failed it cannot be told from here. Each is written apart from now on, until it succeeds.
            string[] queues = [.. batch.Select(p => p.Queue).Distinct(StringComparer.Ordinal)];
            foreach (string queue in queues)
            {
                _apart.TryAdd(queue, new(queue));
            }

            foreach (Pending pending in batch)
            {
                if (queues.Length == 1)
                {
                    // Only this queue was in it, so the failure is its own.
                    pending.Done.TrySetException(ex);
                }
                else
                {
                    pending.Rewritten = true;
                    Route(pending);
                }
            }

            return;
        }

        if (!shared)
        {
            // Written again: back to the shared batch. Whatever was routed to this lane meanwhile it still writes.
            _apart.TryRemove(new KeyValuePair<string, Lane>(lane.Queue!, lane));
        }

        foreach ((SettleGroup group, List<Pending> waiting) in groups)
        {
            foreach (Pending pending in waiting)
            {
                // A message missing on a second write may be gone because the first one did go through and only its
                // answer was lost; that is not a message someone else took.
                pending.Done.TrySetResult(pending.Rewritten || group.Settled?.Contains(pending.MessageId) == true);
            }
        }
    }

    /// <summary>Every group in one batch: one round trip, and one transaction.</summary>
    private static async Task WriteAsync(NpgsqlDataSource dataSource, IReadOnlyList<SettleGroup> groups)
    {
        await using NpgsqlBatch batch = dataSource.CreateBatch();
        foreach (SettleGroup group in groups)
        {
            NpgsqlBatchCommand command = new(Sql(group));
            if (group.Kind != SettleKind.ReturnUnread)
            {
                command.Parameters.Add(new() { Value = group.Queue, NpgsqlDbType = NpgsqlDbType.Text });
            }

            command.Parameters.Add(new()
            {
                Value = group.MessageIds,
                NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint,
            });

            if (group.Kind == SettleKind.Release)
            {
                command.Parameters.Add(new() { Value = group.DelaySeconds, NpgsqlDbType = NpgsqlDbType.Integer });
            }

            batch.BatchCommands.Add(command);
        }

        List<HashSet<long>> settled = new(groups.Count);
        await using (NpgsqlDataReader reader = await batch.ExecuteReaderAsync(CancellationToken.None))
        {
            do
            {
                HashSet<long> ids = [];
                while (await reader.ReadAsync(CancellationToken.None))
                {
                    ids.Add(reader.GetInt64(0));
                }

                settled.Add(ids);
            } while (await reader.NextResultAsync(CancellationToken.None));
        }

        if (settled.Count != groups.Count)
        {
            throw new InvalidOperationException($"Settling {groups.Count} groups answered with {settled.Count} results.");
        }

        for (int i = 0; i < groups.Count; i++)
        {
            groups[i].Settled = settled[i];
        }
    }

    /// <summary>The statement of one group; each answers with the ids it found.</summary>
    internal static string Sql(SettleGroup group)
    {
        switch (group.Kind)
        {
            case SettleKind.Delete:
                return "select * from pgmq.delete($1, $2)";
            case SettleKind.Archive:
                return "select * from pgmq.archive($1, $2)";
            case SettleKind.Release:
                return "select msg_id from pgmq.set_vt($1, $2, $3)";
            default:
                // pgmq has no function for this, so it writes the queue's table itself.
                PgmqTopology.EnsureValidQueueName(group.Queue);
                string table = PgmqMigrator.QuoteIdentifier(("q_" + group.Queue).ToLowerInvariant());
                return $"update pgmq.{table} set vt = pg_catalog.clock_timestamp(), " +
                       "read_ct = greatest(read_ct - 1, 0) where msg_id = any($1) returning msg_id";
        }
    }

    /// <summary>What waits to be written by one writer: the shared batch's, or one queue's while it is written apart.</summary>
    private sealed class Lane(string? queue)
    {
        /// <summary>The queue this lane writes apart; null for the shared batch.</summary>
        public string? Queue { get; } = queue;

        public Lock Lock { get; } = new();

        public List<Pending> Pending { get; set; } = [];

        public bool Writing { get; set; }
    }

    private sealed record Pending(string Queue, long MessageId, SettleKind Kind, int DelaySeconds)
    {
        public TaskCompletionSource<bool> Done { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>A write that carried it failed, and it is being written again.</summary>
        public bool Rewritten { get; set; }
    }
}

/// <summary>The messages of one queue to settle the same way, in one statement.</summary>
internal sealed class SettleGroup(string queue, SettleKind kind, int delaySeconds, long[] messageIds)
{
    public string Queue { get; } = queue;

    public SettleKind Kind { get; } = kind;

    /// <summary>For a <see cref="SettleKind.Release"/>: how long until the messages are visible again.</summary>
    public int DelaySeconds { get; } = delaySeconds;

    public long[] MessageIds { get; } = messageIds;

    /// <summary>The messages the statement found and settled; one missing here was no longer on the queue.</summary>
    public HashSet<long>? Settled { get; set; }
}
