namespace Pgmq.Hosting.Tests;

public sealed class SweeperTests
{
    [Fact]
    public void The_sweep_Should_be_one_statement_with_a_branch_per_queue_and_a_parameter_per_filter()
    {
        ConsumerRegistration[] consumers =
        [
            Consumer("orders"),
            Consumer("eu_orders") with { Filter = """{"region":"eu"}""" },
            Consumer("emails"),
            Consumer("us_orders") with { Filter = """{"region":"us"}""" },
        ];

        string sql = QueueSweeper.Sql(consumers);

        Assert.Equal(
            """
            select 0 where exists (select 1 from pgmq."q_orders" where vt <= pg_catalog.clock_timestamp())
            union all
            select 1 where exists (select 1 from pgmq."q_eu_orders" where vt <= pg_catalog.clock_timestamp() and message @> $1::jsonb)
            union all
            select 2 where exists (select 1 from pgmq."q_emails" where vt <= pg_catalog.clock_timestamp())
            union all
            select 3 where exists (select 1 from pgmq."q_us_orders" where vt <= pg_catalog.clock_timestamp() and message @> $2::jsonb)
            """.ReplaceLineEndings("\n"),
            sql);
    }

    [Theory]
    [InlineData(PgmqReadMode.Standard, false, false, "pgmq.read($1, $2, $3)")]
    [InlineData(PgmqReadMode.Standard, true, false, "pgmq.read($1, $2, $3, $4)")]
    [InlineData(PgmqReadMode.Grouped, false, false, "pgmq.read_grouped($1, $2, $3)")]
    [InlineData(PgmqReadMode.GroupedRoundRobin, false, false, "pgmq.read_grouped_rr($1, $2, $3)")]
    [InlineData(PgmqReadMode.GroupedHead, false, false, "pgmq.read_grouped_head($1, $2, $3)")]
    [InlineData(PgmqReadMode.Standard, false, true, "pgmq.pop($1, $2)")]
    public void A_read_Should_call_the_pgmq_function_its_consumer_asks_for_and_never_long_poll(PgmqReadMode mode,
        bool filtered, bool atMostOnce, string call)
    {
        string sql = QueueReader.Sql(Consumer("orders") with
        {
            ReadMode = mode,
            Filter = filtered ? """{"region":"eu"}""" : null,
            AtMostOnce = atMostOnce,
        });

        Assert.EndsWith($"from {call}", sql);
        Assert.DoesNotContain("with_poll", sql);
    }

    [Fact]
    public void A_quiet_queue_Should_beat_often_enough_never_to_look_stalled()
    {
        Assert.Equal(TimeSpan.FromSeconds(10), new PgmqOptions().Heartbeat);
        Assert.Equal(TimeSpan.FromMilliseconds(500), new PgmqOptions { StallAfter = TimeSpan.FromSeconds(2) }.Heartbeat);
    }

    [Fact]
    public void MaxConcurrentReads_Should_be_at_least_one()
        => Assert.Throws<ArgumentOutOfRangeException>(() => new PgmqOptions
        {
            ConnectionString = "Host=h",
            MaxConcurrentReads = 0,
        }.Validate());

    private static ConsumerRegistration Consumer(string queue)
        => new(typeof(SweeperTests), queue, [typeof(string)], 1, TimeSpan.FromMinutes(1));
}
