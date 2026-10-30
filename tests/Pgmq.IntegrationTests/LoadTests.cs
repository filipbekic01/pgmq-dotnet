using Pgmq.Benchmarks;

namespace Pgmq.IntegrationTests;

/// <summary>
///     100,000 messages over 100 queues with the default settings, through the same scenario the benchmark
///     (<c>Pgmq.Benchmarks</c>) times. These assert behaviour, not speed: every message handled exactly once, nothing
///     archived or left on a queue, no error logged. Speed belongs to the benchmark, where a change is compared on one
///     machine. About a minute in all; needs Docker. Leave them out with <c>--filter-not-trait "Category=Load"</c>.
/// </summary>
[Trait("Category", "Load")]
public sealed class LoadTests(PgmqFixture db) : IClassFixture<PgmqFixture>
{
    private const int Messages = 100_000;
    private const int Queues = 100;

    [Fact]
    public async Task Full_queues_Should_be_drained_with_every_message_handled_exactly_once()
    {
        LoadResult result = await new LoadScenario(db.ConnectionString, Messages, Queues)
            .RunAsync(LoadMode.Drain, TestContext.Current.CancellationToken);

        AssertClean(result);
    }

    [Fact]
    public async Task Messages_published_while_consuming_Should_each_be_handled_exactly_once()
    {
        LoadResult result = await new LoadScenario(db.ConnectionString, Messages, Queues)
            .RunAsync(LoadMode.Live, TestContext.Current.CancellationToken);

        AssertClean(result);
    }

    [Fact]
    public async Task Two_instances_on_the_same_queues_Should_share_the_work_and_never_both_take_a_message()
    {
        LoadResult result = await new LoadScenario(db.ConnectionString, Messages, Queues) { Instances = 2 }
            .RunAsync(LoadMode.Live, TestContext.Current.CancellationToken);

        AssertClean(result);
    }

    private static void AssertClean(LoadResult result)
    {
        Assert.Equal(0, result.Lost);
        Assert.Equal(0, result.Duplicates);
        Assert.Equal(0, result.LeftOnQueues);
        Assert.Equal(0, result.Archived);
        Assert.Equal(0, result.ErrorsLogged);
    }
}
