using System.Collections.Concurrent;

namespace Pgmq.Hosting.Tests;

public sealed class SettleBatcherTests
{
    private static readonly TimeSpan Soon = TimeSpan.FromSeconds(5);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Settles_asked_for_during_a_write_Should_go_out_together_in_the_next_one()
    {
        List<IReadOnlyList<SettleGroup>> writes = [];
        TaskCompletionSource firstStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource letFirstEnd = new(TaskCreationOptions.RunContinuationsAsynchronously);

        SettleBatcher batcher = new(async groups =>
        {
            writes.Add(groups);
            if (writes.Count == 1)
            {
                firstStarted.SetResult();
                await letFirstEnd.Task;
            }

            SettleAll(groups);
        });

        Task<bool> first = batcher.DeleteAsync("orders", 1);
        await firstStarted.Task.WaitAsync(Soon, Ct);

        // Seventy-five consumers end while the first write is on its way.
        Task<bool>[] burst =
        [
            .. Enumerable.Range(2, 50).Select(id => batcher.DeleteAsync("orders", id)),
            .. Enumerable.Range(100, 15).Select(id => batcher.ArchiveAsync("orders", id)),
            .. Enumerable.Range(200, 5).Select(id => batcher.DeleteAsync("emails", id)),
            .. Enumerable.Range(300, 3).Select(id => batcher.ReleaseAsync("emails", id, TimeSpan.Zero)),
            .. Enumerable.Range(400, 2).Select(id => batcher.ReturnUnreadAsync("emails", id)),
        ];
        letFirstEnd.SetResult();

        Assert.True(await first.WaitAsync(Soon, Ct));
        Assert.All(await Task.WhenAll(burst).WaitAsync(Soon, Ct), Assert.True);

        // One write for the first, one for the rest: a statement per queue and kind.
        Assert.Equal(2, writes.Count);
        Assert.Equal(5, writes[1].Count);
        Assert.Equal(50, writes[1].Single(g => g is { Queue: "orders", Kind: SettleKind.Delete }).MessageIds.Length);
        Assert.Equal(15, writes[1].Single(g => g is { Queue: "orders", Kind: SettleKind.Archive }).MessageIds.Length);
        Assert.Equal(5, writes[1].Single(g => g is { Queue: "emails", Kind: SettleKind.Delete }).MessageIds.Length);
        Assert.Equal(3, writes[1].Single(g => g is { Queue: "emails", Kind: SettleKind.Release }).MessageIds.Length);
        Assert.Equal(2, writes[1].Single(g => g is { Queue: "emails", Kind: SettleKind.ReturnUnread }).MessageIds.Length);
    }

    [Fact]
    public async Task Releases_with_different_delays_Should_be_written_apart_and_round_up_to_whole_seconds()
    {
        ConcurrentBag<SettleGroup> written = [];
        SettleBatcher batcher = new(groups =>
        {
            foreach (SettleGroup group in groups)
            {
                written.Add(group);
            }

            SettleAll(groups);
            return Task.CompletedTask;
        });

        await Task.WhenAll(
            batcher.ReleaseAsync("orders", 1, TimeSpan.Zero),
            batcher.ReleaseAsync("orders", 2, TimeSpan.FromSeconds(1.2)),
            batcher.ReleaseAsync("orders", 3, TimeSpan.FromSeconds(-1))).WaitAsync(Soon, Ct);

        Assert.Equal([1L, 3L], written.Where(g => g.DelaySeconds == 0).SelectMany(g => g.MessageIds).Order());
        Assert.Equal([2L], written.Where(g => g.DelaySeconds == 2).SelectMany(g => g.MessageIds));
    }

    [Fact]
    public async Task A_message_the_write_did_not_find_Should_be_reported_as_gone()
    {
        SettleBatcher batcher = new(groups =>
        {
            foreach (SettleGroup group in groups)
            {
                group.Settled = [.. group.MessageIds.Where(id => id != 2)];
            }

            return Task.CompletedTask;
        });

        Assert.True(await batcher.DeleteAsync("orders", 1));
        Assert.False(await batcher.DeleteAsync("orders", 2));
    }

    [Fact]
    public async Task A_queue_that_cannot_be_written_Should_fail_alone_and_leave_the_others_settling()
    {
        // Any write with the broken queue in it fails as a whole, as a batch in one transaction does.
        int sharedWithBroken = 0;
        SettleBatcher batcher = new(groups =>
        {
            if (groups.Any(g => g.Queue == "broken"))
            {
                if (groups.Any(g => g.Queue != "broken"))
                {
                    Interlocked.Increment(ref sharedWithBroken);
                }

                throw new InvalidOperationException("no such queue");
            }

            SettleAll(groups);
            return Task.CompletedTask;
        });

        Task<bool>[] healthy = [batcher.DeleteAsync("orders", 1), batcher.ArchiveAsync("emails", 2)];
        Task<bool> broken = batcher.DeleteAsync("broken", 3);

        // The healthy queues are settled although they shared a failed write, and are told nothing of it.
        Assert.All(await Task.WhenAll(healthy).WaitAsync(Soon, Ct), Assert.True);
        await Assert.ThrowsAsync<InvalidOperationException>(() => broken.WaitAsync(Soon, Ct));

        // From here the broken queue is written apart: it keeps failing, and never takes the others down again.
        int before = Volatile.Read(ref sharedWithBroken);
        for (int i = 0; i < 20; i++)
        {
            Task<bool> good = batcher.DeleteAsync("orders", 100 + i);
            Task<bool> bad = batcher.DeleteAsync("broken", 200 + i);
            Assert.True(await good.WaitAsync(Soon, Ct));
            await Assert.ThrowsAsync<InvalidOperationException>(() => bad.WaitAsync(Soon, Ct));
        }

        Assert.Equal(before, Volatile.Read(ref sharedWithBroken));
    }

    [Fact]
    public async Task A_queue_whose_write_hangs_Should_not_hold_up_the_other_queues()
    {
        TaskCompletionSource stuck = new(TaskCreationOptions.RunContinuationsAsynchronously);
        bool first = true;
        SettleBatcher batcher = new(async groups =>
        {
            if (first)
            {
                // The first shared write fails, which sets every queue in it apart.
                first = false;
                throw new TimeoutException("the write timed out");
            }

            if (groups.Any(g => g.Queue == "locked"))
            {
                await stuck.Task;
            }

            SettleAll(groups);
        });

        Task<bool> locked = batcher.DeleteAsync("locked", 1);
        Task<bool> other = batcher.DeleteAsync("orders", 2);

        Assert.True(await other.WaitAsync(Soon, Ct));
        Assert.True(await batcher.DeleteAsync("orders", 3).WaitAsync(Soon, Ct));
        Assert.False(locked.IsCompleted);

        stuck.SetResult();
        Assert.True(await locked.WaitAsync(Soon, Ct));
    }

    [Fact]
    public async Task A_queue_written_apart_Should_rejoin_the_shared_batch_once_a_write_of_it_succeeds()
    {
        List<string[]> writes = [];
        bool fail = true;
        SettleBatcher batcher = new(groups =>
        {
            lock (writes)
            {
                writes.Add([.. groups.Select(g => g.Queue).Distinct().Order()]);
            }

            if (fail)
            {
                fail = false;
                throw new TimeoutException("the database is away");
            }

            SettleAll(groups);
            return Task.CompletedTask;
        });

        // One failed write for both, then each apart, both succeeding.
        Assert.All(await Task.WhenAll(batcher.DeleteAsync("emails", 1), batcher.DeleteAsync("orders", 2))
            .WaitAsync(Soon, Ct), Assert.True);

        Assert.All(await Task.WhenAll(batcher.DeleteAsync("emails", 3), batcher.DeleteAsync("orders", 4))
            .WaitAsync(Soon, Ct), Assert.True);

        lock (writes)
        {
            Assert.Contains(writes, w => w is ["emails"]);
            Assert.Contains(writes, w => w is ["orders"]);
        }
    }

    [Fact]
    public async Task A_message_missing_on_a_second_write_Should_not_be_reported_as_gone()
    {
        // The first write went through but its answer was lost; the second finds nothing left to delete.
        bool first = true;
        SettleBatcher batcher = new(groups =>
        {
            if (first)
            {
                first = false;
                throw new TimeoutException("the answer was lost");
            }

            foreach (SettleGroup group in groups)
            {
                group.Settled = [];
            }

            return Task.CompletedTask;
        });

        Assert.All(await Task.WhenAll(batcher.DeleteAsync("emails", 1), batcher.DeleteAsync("orders", 2))
            .WaitAsync(Soon, Ct), Assert.True);
    }

    [Fact]
    public async Task A_write_that_fails_Should_fail_what_it_carried_and_leave_the_next_one_working()
    {
        int writes = 0;
        SettleBatcher batcher = new(groups =>
        {
            if (Interlocked.Increment(ref writes) == 1)
            {
                throw new TimeoutException("the database is away");
            }

            SettleAll(groups);
            return Task.CompletedTask;
        });

        await Assert.ThrowsAsync<TimeoutException>(() => batcher.DeleteAsync("orders", 1));
        Assert.True(await batcher.DeleteAsync("orders", 1));
    }

    [Fact]
    public void An_unread_message_Should_go_back_through_the_queue_table_itself()
    {
        Assert.Contains("pgmq.\"q_orders\"", SettleBatcher.Sql(new("orders", SettleKind.ReturnUnread, 0, [1])));
        Assert.Throws<ArgumentException>(() => SettleBatcher.Sql(new("bad\"name", SettleKind.ReturnUnread, 0, [1])));
    }

    private static void SettleAll(IReadOnlyList<SettleGroup> groups)
    {
        foreach (SettleGroup group in groups)
        {
            group.Settled = [.. group.MessageIds];
        }
    }
}
