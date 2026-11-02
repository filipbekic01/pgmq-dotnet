using Npgsql;

namespace Pgmq.IntegrationTests;

/// <summary>
///     <c>pgmq.shovel</c>, Pgmq.NET's one addition to pgmq, through <see cref="IPgmqQueueManager.ShovelAsync"/>: moving
///     messages from a queue or its archive onto another queue. Needs a running Docker daemon.
/// </summary>
public sealed class ShovelTests(PgmqFixture db) : IClassFixture<PgmqFixture>, IAsyncLifetime
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private PgmqQueueManager Queues => db.Queues;

    public async ValueTask InitializeAsync()
    {
        await using NpgsqlDataSource dataSource = NpgsqlDataSource.Create(db.ConnectionString);
        await new PgmqMigrator(dataSource).MigrateAsync(cancellationToken: Ct);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    [Fact]
    public async Task The_migrator_Should_create_the_shovel_in_pgmq_and_report_it_only_once()
    {
        await using NpgsqlDataSource dataSource = NpgsqlDataSource.Create(db.ConnectionString);
        PgmqMigrationResult again = await new PgmqMigrator(dataSource).MigrateAsync(cancellationToken: Ct);

        Assert.False(again.Changed);
        Assert.Equal(true, await db.ScalarObjectAsync(
            "select to_regprocedure('pgmq.shovel(text, text, boolean, integer)') is not null"));
    }

    [Fact]
    public async Task A_shovel_Should_move_every_visible_message_in_order_with_its_headers()
    {
        await NewQueuesAsync("s_from", "s_to");
        await Queues.SendAsync("s_from", """{"n": 1}""", """{"h": "one"}""", cancellationToken: Ct);
        await Queues.SendAsync("s_from", """{"n": 2}""", """{"h": "two"}""", cancellationToken: Ct);
        await Queues.SendAsync("s_from", """{"n": 3}""", cancellationToken: Ct);

        Assert.Equal(3, await Queues.ShovelAsync("s_from", "s_to", cancellationToken: Ct));

        Assert.Equal(0, await db.CountAsync("s_from"));
        IReadOnlyList<PgmqMessage> moved = await Queues.ReadAsync("s_to", TimeSpan.FromSeconds(30), 10, cancellationToken: Ct);
        Assert.Equal(["""{"n": 1}""", """{"n": 2}""", """{"n": 3}"""], moved.Select(m => m.Message));
        Assert.Equal(["""{"h": "one"}""", """{"h": "two"}""", null], moved.Select(m => m.Headers));
        Assert.All(moved, m => Assert.Equal(1, m.ReadCount));
    }

    [Fact]
    public async Task A_shovel_Should_leave_messages_being_handled_or_delayed_on_the_source()
    {
        await NewQueuesAsync("s_busy", "s_busy_to");
        await Queues.SendAsync("s_busy", """{"n": "leased"}""", cancellationToken: Ct);
        await Queues.SendAsync("s_busy", """{"n": "delayed"}""", delay: TimeSpan.FromMinutes(5), cancellationToken: Ct);
        await Queues.SendAsync("s_busy", """{"n": "free"}""", cancellationToken: Ct);
        await Queues.ReadAsync("s_busy", TimeSpan.FromMinutes(5), 1, cancellationToken: Ct); // leases the first

        Assert.Equal(1, await Queues.ShovelAsync("s_busy", "s_busy_to", cancellationToken: Ct));

        Assert.Equal(2, await db.CountAsync("s_busy"));
        Assert.Equal("""{"n": "free"}""",
            Assert.Single(await Queues.ReadAsync("s_busy_to", TimeSpan.FromSeconds(30), 10, cancellationToken: Ct)).Message);
    }

    [Fact]
    public async Task A_shovel_from_the_archive_Should_replay_archived_messages_onto_their_queue()
    {
        await NewQueuesAsync("s_replay");
        long first = await Queues.SendAsync("s_replay", """{"n": 1}""", cancellationToken: Ct);
        long second = await Queues.SendAsync("s_replay", """{"n": 2}""", cancellationToken: Ct);
        await Queues.ArchiveMessagesAsync("s_replay", [first, second], Ct);

        Assert.Equal(2, await Queues.ShovelAsync("s_replay", "s_replay", fromArchive: true, cancellationToken: Ct));

        Assert.Equal(0, await db.ArchivedAsync("s_replay"));
        Assert.Equal(2, await db.CountAsync("s_replay"));
    }

    [Fact]
    public async Task A_shovel_with_a_limit_Should_move_only_the_oldest_that_many()
    {
        await NewQueuesAsync("s_some", "s_some_to");
        foreach (int n in new[] { 1, 2, 3 })
        {
            await Queues.SendAsync("s_some", $$"""{"n": {{n}}}""", cancellationToken: Ct);
        }

        Assert.Equal(2, await Queues.ShovelAsync("s_some", "s_some_to", maxMessages: 2, cancellationToken: Ct));

        Assert.Equal("""{"n": 3}""",
            Assert.Single(await Queues.ReadAsync("s_some", TimeSpan.FromSeconds(30), 10, cancellationToken: Ct)).Message);
        Assert.Equal(2, await db.CountAsync("s_some_to"));
    }

    [Fact]
    public async Task A_shovel_to_a_missing_queue_Should_fail_and_move_nothing()
    {
        await NewQueuesAsync("s_nowhere");
        await Queues.SendAsync("s_nowhere", """{"n": 1}""", cancellationToken: Ct);

        await Assert.ThrowsAsync<PostgresException>(() => Queues.ShovelAsync("s_nowhere", "s_missing", cancellationToken: Ct));

        Assert.Equal(1, await db.CountAsync("s_nowhere"));
    }

    [Fact]
    public async Task A_shovel_of_a_queue_into_itself_Should_be_refused()
    {
        await NewQueuesAsync("s_self");

        await Assert.ThrowsAsync<PostgresException>(() => Queues.ShovelAsync("s_self", "s_self", cancellationToken: Ct));
    }

    private async Task NewQueuesAsync(params string[] names)
    {
        foreach (string name in names)
        {
            await Queues.EnsureQueueAsync(name, cancellationToken: Ct);
            await db.ResetAsync(name);
        }
    }
}
