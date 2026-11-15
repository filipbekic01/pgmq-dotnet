namespace Pgmq.Hosting.Tests;

public sealed class SettleBatcherTests
{
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
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);

        // Seventy consumers finish while the first write is on its way.
        Task<bool>[] burst =
        [
            .. Enumerable.Range(2, 50).Select(id => batcher.DeleteAsync("orders", id)),
            .. Enumerable.Range(100, 15).Select(id => batcher.ArchiveAsync("orders", id)),
            .. Enumerable.Range(200, 5).Select(id => batcher.DeleteAsync("emails", id)),
        ];
        letFirstEnd.SetResult();

        Assert.True(await first.WaitAsync(TimeSpan.FromSeconds(5), Ct));
        Assert.All(await Task.WhenAll(burst).WaitAsync(TimeSpan.FromSeconds(5), Ct), Assert.True);

        // One write for the first, one for the seventy: a statement per queue and kind.
        Assert.Equal(2, writes.Count);
        Assert.Equal(3, writes[1].Count);
        Assert.Equal(50, writes[1].Single(g => g is { Queue: "orders", Archive: false }).MessageIds.Length);
        Assert.Equal(15, writes[1].Single(g => g is { Queue: "orders", Archive: true }).MessageIds.Length);
        Assert.Equal(5, writes[1].Single(g => g is { Queue: "emails", Archive: false }).MessageIds.Length);
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
    public async Task A_queue_that_cannot_be_written_Should_fail_only_its_own_messages()
    {
        SettleBatcher batcher = new(groups =>
        {
            foreach (SettleGroup group in groups)
            {
                if (group.Queue == "broken")
                {
                    group.Failure = new InvalidOperationException("no such queue");
                }
                else
                {
                    group.Settled = [.. group.MessageIds];
                }
            }

            return Task.CompletedTask;
        });

        Task<bool> good = batcher.DeleteAsync("orders", 1);
        Task<bool> bad = batcher.ArchiveAsync("broken", 2);
        Task<bool> alsoGood = batcher.ArchiveAsync("orders", 3);

        Assert.True(await good.WaitAsync(TimeSpan.FromSeconds(5), Ct));
        Assert.True(await alsoGood.WaitAsync(TimeSpan.FromSeconds(5), Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => bad.WaitAsync(TimeSpan.FromSeconds(5), Ct));
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

    private static void SettleAll(IReadOnlyList<SettleGroup> groups)
    {
        foreach (SettleGroup group in groups)
        {
            group.Settled = [.. group.MessageIds];
        }
    }
}
