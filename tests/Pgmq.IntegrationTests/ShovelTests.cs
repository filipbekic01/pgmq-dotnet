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
            "select to_regprocedure('pgmq.shovel(text, text, boolean, integer, bigint[], integer)') is not null"));
        Assert.Equal(1L, await db.ScalarObjectAsync(
            "select count(*) from pg_proc where proname = 'shovel' and pronamespace = 'pgmq'::regnamespace"));
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

        PostgresException ex = await Assert.ThrowsAsync<PostgresException>(
            () => Queues.ShovelAsync("s_nowhere", "s_missing", cancellationToken: Ct));

        Assert.Contains("target queue s_missing does not exist", ex.MessageText);
        Assert.Equal(1, await db.CountAsync("s_nowhere"));
    }

    [Fact]
    public async Task A_shovel_from_a_missing_queue_Should_fail_clearly()
    {
        await NewQueuesAsync("s_lonely_to");

        PostgresException ex = await Assert.ThrowsAsync<PostgresException>(
            () => Queues.ShovelAsync("s_gone", "s_lonely_to", cancellationToken: Ct));

        Assert.Contains("source queue s_gone does not exist", ex.MessageText);
    }

    [Fact]
    public async Task A_shovel_of_an_empty_queue_Should_move_nothing()
    {
        await NewQueuesAsync("s_empty", "s_empty_to");

        Assert.Equal(0, await Queues.ShovelAsync("s_empty", "s_empty_to", cancellationToken: Ct));
        Assert.Equal(0, await Queues.ShovelAsync("s_empty", "s_empty_to", fromArchive: true, cancellationToken: Ct));
    }

    [Fact]
    public async Task By_default_a_shovel_Should_move_1000_and_without_a_limit_every_message()
    {
        await NewQueuesAsync("s_many", "s_many_to");
        await db.SendAsync("s_many", 2500);

        Assert.Equal(1000, await Queues.ShovelAsync("s_many", "s_many_to", cancellationToken: Ct));
        Assert.Equal(1500, await db.CountAsync("s_many"));

        Assert.Equal(1500, await Queues.ShovelAsync("s_many", "s_many_to", maxMessages: null, cancellationToken: Ct));
        Assert.Equal(0, await db.CountAsync("s_many"));
        Assert.Equal(2500, await db.CountAsync("s_many_to"));
    }

    [Fact]
    public async Task Two_shovels_at_once_Should_never_move_the_same_message()
    {
        await NewQueuesAsync("s_race", "s_race_to");
        await db.SendAsync("s_race", 4);

        // The first holds the two oldest in an open transaction; the second skips them and takes the rest.
        await using NpgsqlConnection first = new(db.ConnectionString);
        await first.OpenAsync(Ct);
        await using NpgsqlTransaction transaction = await first.BeginTransactionAsync(Ct);
        await using (NpgsqlCommand shovel = new("select pgmq.shovel('s_race', 's_race_to', false, 2)", first, transaction))
        {
            Assert.Equal(2L, await shovel.ExecuteScalarAsync(Ct));
        }

        Assert.Equal(2, await Queues.ShovelAsync("s_race", "s_race_to", maxMessages: null, cancellationToken: Ct));
        await transaction.CommitAsync(Ct);

        Assert.Equal(0, await db.CountAsync("s_race"));
        Assert.Equal(4, await db.CountAsync("s_race_to"));
        Assert.Equal(4L, await db.ScalarObjectAsync("select count(distinct message) from pgmq.q_s_race_to"));
    }

    [Fact]
    public async Task A_shovel_in_a_transaction_that_rolls_back_Should_move_nothing()
    {
        await NewQueuesAsync("s_undo", "s_undo_to");
        await db.SendAsync("s_undo", 3);

        await using (NpgsqlConnection connection = new(db.ConnectionString))
        {
            await connection.OpenAsync(Ct);
            await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync(Ct);
            await using NpgsqlCommand shovel = new("select pgmq.shovel('s_undo', 's_undo_to')", connection, transaction);
            Assert.Equal(3L, await shovel.ExecuteScalarAsync(Ct));
            await transaction.RollbackAsync(Ct);
        }

        Assert.Equal(3, await db.CountAsync("s_undo"));
        Assert.Equal(0, await db.CountAsync("s_undo_to"));
    }

    [Fact]
    public async Task A_shovel_of_a_queue_into_itself_Should_be_refused()
    {
        await NewQueuesAsync("s_self");

        await Assert.ThrowsAsync<PostgresException>(() => Queues.ShovelAsync("s_self", "s_self", cancellationToken: Ct));
    }

    [Fact]
    public async Task A_shovel_by_id_Should_move_exactly_those_and_skip_unknown_or_leased_ones()
    {
        await NewQueuesAsync("s_ids", "s_ids_to");
        long[] ids = await db.SendAsync("s_ids", 4);
        await Queues.SetVisibilityTimeoutAsync("s_ids", ids[3], TimeSpan.FromMinutes(5), Ct); // as if being handled

        Assert.Equal(2, await Queues.ShovelAsync("s_ids", "s_ids_to", [ids[1], ids[2], ids[3], 999_999], cancellationToken: Ct));

        Assert.Equal([ids[0], ids[3]],
            ((long[])(await db.ScalarObjectAsync("select array_agg(msg_id order by msg_id) from pgmq.q_s_ids"))!));
        Assert.Equal(2, await db.CountAsync("s_ids_to"));
    }

    [Fact]
    public async Task A_shovel_by_id_from_the_archive_Should_restore_only_those()
    {
        await NewQueuesAsync("s_restore");
        long[] ids = await db.SendAsync("s_restore", 3);
        await Queues.ArchiveMessagesAsync("s_restore", ids, Ct);

        Assert.Equal(1, await Queues.ShovelAsync("s_restore", "s_restore", [ids[1]], fromArchive: true, cancellationToken: Ct));

        Assert.Equal(2, await db.ArchivedAsync("s_restore"));
        Assert.Equal(1, await db.CountAsync("s_restore"));
    }

    [Fact]
    public async Task A_shovel_by_id_Should_move_every_listed_message_past_the_default_limit()
    {
        await NewQueuesAsync("s_ids_many", "s_ids_many_to");
        long[] ids = await db.SendAsync("s_ids_many", 1200);

        Assert.Equal(1200, await Queues.ShovelAsync("s_ids_many", "s_ids_many_to", ids, cancellationToken: Ct));
        Assert.Equal(0, await db.CountAsync("s_ids_many"));
    }

    [Fact]
    public async Task Each_shovel_from_the_archive_Should_count_one_up_and_keep_the_other_headers()
    {
        await NewQueuesAsync("s_count");
        await Queues.SendAsync("s_count", """{"n": 1}""", """{"tenant": "acme"}""", cancellationToken: Ct);
        await Queues.SendAsync("s_count", """{"n": 2}""", cancellationToken: Ct); // no headers at all

        foreach (string expected in new[] { "1", "2" })
        {
            await ArchiveAllAsync("s_count");
            Assert.Equal(2, await Queues.ShovelAsync("s_count", "s_count", fromArchive: true, cancellationToken: Ct));
            Assert.Equal([expected, expected], await ShovelCountsAsync("q_s_count"));
        }

        Assert.Equal(
            ["""{"tenant": "acme", "shovel-count": "2"}""", """{"shovel-count": "2"}"""],
            (string[])(await db.ScalarObjectAsync(
                "select array_agg(headers::text order by msg_id) from pgmq.q_s_count"))!);
    }

    [Fact]
    public async Task A_shovel_from_the_archive_Should_count_from_zero_whatever_the_header_holds()
    {
        await NewQueuesAsync("s_odd");
        await Queues.SendAsync("s_odd", """{"n": 1}""", """{"shovel-count": 3}""", cancellationToken: Ct);
        await Queues.SendAsync("s_odd", """{"n": 2}""", """{"shovel-count": "many"}""", cancellationToken: Ct);
        await Queues.SendAsync("s_odd", """{"n": 3}""", """{"shovel-count": {"n": 1}}""", cancellationToken: Ct);
        await Queues.SendAsync("s_odd", """{"n": 4}""", """["not", "an object"]""", cancellationToken: Ct);
        await ArchiveAllAsync("s_odd");

        Assert.Equal(4, await Queues.ShovelAsync("s_odd", "s_odd", fromArchive: true, maxShovels: 4, cancellationToken: Ct));

        // A number counts as written, anything else as zero, and headers that are no object move as they are.
        Assert.Equal(
            ["""{"shovel-count": "4"}""", """{"shovel-count": "1"}""", """{"shovel-count": "1"}""", """["not", "an object"]"""],
            (string[])(await db.ScalarObjectAsync("select array_agg(headers::text order by msg_id) from pgmq.q_s_odd"))!);
    }

    [Fact]
    public async Task A_capped_shovel_Should_leave_messages_at_the_cap_in_the_archive_and_not_count_them()
    {
        await NewQueuesAsync("s_cap");
        long capped = await Queues.SendAsync("s_cap", """{"n": "capped"}""", """{"shovel-count": "2"}""", cancellationToken: Ct);
        await Queues.SendAsync("s_cap", """{"n": "once"}""", """{"shovel-count": "1"}""", cancellationToken: Ct);
        long over = await Queues.SendAsync("s_cap", """{"n": "over"}""", """{"shovel-count": "7"}""", cancellationToken: Ct);
        await Queues.SendAsync("s_cap", """{"n": "never"}""", cancellationToken: Ct);
        await Queues.SendAsync("s_cap", """{"n": "late"}""", cancellationToken: Ct);
        await ArchiveAllAsync("s_cap");

        // The oldest is at the cap: it is passed over, not counted against the two this call may move.
        Assert.Equal(2, await Queues.ShovelAsync("s_cap", "s_cap", fromArchive: true, maxMessages: 2, maxShovels: 2,
            cancellationToken: Ct));
        Assert.Equal(["2", "1"], await ShovelCountsAsync("q_s_cap"));

        Assert.Equal(1, await Queues.ShovelAsync("s_cap", "s_cap", fromArchive: true, maxShovels: 2, cancellationToken: Ct));
        Assert.Equal(0, await Queues.ShovelAsync("s_cap", "s_cap", fromArchive: true, maxShovels: 2, cancellationToken: Ct));

        // The two at the cap are still archived, the very rows they were.
        Assert.Equal([capped, over],
            (long[])(await db.ScalarObjectAsync("select array_agg(msg_id order by msg_id) from pgmq.a_s_cap"))!);
        Assert.Equal(["2", "7"], await ShovelCountsAsync("a_s_cap"));

        // Without a cap they move like any other.
        Assert.Equal(2, await Queues.ShovelAsync("s_cap", "s_cap", fromArchive: true, cancellationToken: Ct));
        Assert.Equal(0, await db.ArchivedAsync("s_cap"));
    }

    [Fact]
    public async Task A_capped_shovel_by_id_Should_skip_those_at_the_cap_and_without_a_cap_force_them()
    {
        await NewQueuesAsync("s_cap_ids");
        long capped = await Queues.SendAsync("s_cap_ids", """{"n": 1}""", """{"shovel-count": "3"}""", cancellationToken: Ct);
        long fresh = await Queues.SendAsync("s_cap_ids", """{"n": 2}""", cancellationToken: Ct);
        await ArchiveAllAsync("s_cap_ids");

        Assert.Equal(1, await Queues.ShovelAsync("s_cap_ids", "s_cap_ids", [capped, fresh], fromArchive: true,
            maxShovels: 3, cancellationToken: Ct));
        Assert.Equal(capped, await db.ScalarObjectAsync("select msg_id from pgmq.a_s_cap_ids"));

        Assert.Equal(1, await Queues.ShovelAsync("s_cap_ids", "s_cap_ids", [capped], fromArchive: true, cancellationToken: Ct));
        Assert.Equal(["1", "4"], await ShovelCountsAsync("q_s_cap_ids"));
    }

    [Fact]
    public async Task A_shovel_between_queues_Should_leave_the_count_alone_and_ignore_the_cap()
    {
        await NewQueuesAsync("s_plain", "s_plain_to");
        await Queues.SendAsync("s_plain", """{"n": 1}""", """{"shovel-count": "5", "tenant": "acme"}""", cancellationToken: Ct);
        await Queues.SendAsync("s_plain", """{"n": 2}""", cancellationToken: Ct);

        Assert.Equal(2, await Queues.ShovelAsync("s_plain", "s_plain_to", maxShovels: 1, cancellationToken: Ct));

        Assert.Equal(["""{"tenant": "acme", "shovel-count": "5"}""", null],
            (string?[])(await db.ScalarObjectAsync(
                "select array_agg(headers::text order by msg_id) from pgmq.q_s_plain_to"))!);
    }

    [Fact]
    public async Task A_cap_below_one_Should_be_refused()
    {
        await NewQueuesAsync("s_bad_cap");

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => Queues.ShovelAsync("s_bad_cap", "s_bad_cap", fromArchive: true, maxShovels: 0, cancellationToken: Ct));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => Queues.ShovelAsync("s_bad_cap", "s_bad_cap", [1], fromArchive: true, maxShovels: 0, cancellationToken: Ct));

        PostgresException ex = await Assert.ThrowsAsync<PostgresException>(
            () => db.ScalarObjectAsync("select pgmq.shovel('s_bad_cap', 's_bad_cap', true, max_shovels => 0)"));
        Assert.Contains("max_shovels must be at least 1", ex.MessageText);
    }

    [Fact]
    public async Task Two_capped_shovels_from_the_archive_at_once_Should_never_move_the_same_message()
    {
        await NewQueuesAsync("s_arace");
        await db.SendAsync("s_arace", 4);
        await ArchiveAllAsync("s_arace");

        await using NpgsqlConnection first = new(db.ConnectionString);
        await first.OpenAsync(Ct);
        await using NpgsqlTransaction transaction = await first.BeginTransactionAsync(Ct);
        await using (NpgsqlCommand shovel = new(
                         "select pgmq.shovel('s_arace', 's_arace', true, 2, max_shovels => 1)", first, transaction))
        {
            Assert.Equal(2L, await shovel.ExecuteScalarAsync(Ct));
        }

        Assert.Equal(2, await Queues.ShovelAsync("s_arace", "s_arace", fromArchive: true, maxMessages: null,
            maxShovels: 1, cancellationToken: Ct));
        await transaction.CommitAsync(Ct);

        Assert.Equal(0, await db.ArchivedAsync("s_arace"));
        Assert.Equal(4L, await db.ScalarObjectAsync("select count(distinct message) from pgmq.q_s_arace"));
        Assert.Equal(["1", "1", "1", "1"], await ShovelCountsAsync("q_s_arace"));
    }

    private async Task ArchiveAllAsync(string queue)
    {
        long[] ids = (long[])(await db.ScalarObjectAsync($"select array_agg(msg_id) from pgmq.q_{queue}"))!;
        await Queues.ArchiveMessagesAsync(queue, ids, Ct);
    }

    /// <summary>The shovel-count header of every row in <paramref name="table"/>, oldest first; null where it is absent.</summary>
    private async Task<string?[]> ShovelCountsAsync(string table)
        => (string?[])(await db.ScalarObjectAsync(
            $"select array_agg(headers ->> 'shovel-count' order by msg_id) from pgmq.{table}"))!;

    private async Task NewQueuesAsync(params string[] names)
    {
        foreach (string name in names)
        {
            await Queues.EnsureQueueAsync(name, cancellationToken: Ct);
            await db.ResetAsync(name);
        }
    }
}
