using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Npgmq;

namespace Pgmq.Tests;

public sealed class PublisherTests
{
    private readonly Mock<INpgmqClient> _client = new(MockBehavior.Strict);
    private readonly PgmqTopology _topology = new();

    private PgmqPublisher Publisher => new(_client.Object, _topology, NullLogger<PgmqPublisher>.Instance);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_publish_Should_write_one_copy_to_each_subscribed_queue()
    {
        _topology.Subscribe(typeof(Ping), "first");
        _topology.Subscribe(typeof(Ping), "second");
        List<(string Queue, string Body, IReadOnlyDictionary<string, object> Headers)> sent = [];
        _client.Setup(c => c.SendAsync(It.IsAny<string>(), It.IsAny<string>(),
                It.IsAny<IReadOnlyDictionary<string, object>>(), It.IsAny<CancellationToken>()))
            .Callback((string q, string b, IReadOnlyDictionary<string, object> h, CancellationToken _) => sent.Add((q, b, h)))
            .ReturnsAsync(1);

        await Publisher.PublishAsync(new Ping(7), Ct);

        Assert.Equal(["first", "second"], sent.Select(s => s.Queue));
        Assert.All(sent, s => Assert.Equal("""{"id":7}""", s.Body));
        Assert.All(sent, s => Assert.Equal(typeof(Ping).FullName, s.Headers["message-type"]));
    }

    [Fact]
    public async Task A_message_nobody_subscribes_to_Should_write_nothing()
    {
        await Publisher.PublishAsync(new Ping(1), Ct);

        _client.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task A_scheduled_publish_Should_be_a_delayed_send()
    {
        _topology.Subscribe(typeof(Ping), "first");
        DateTimeOffset? deliverAt = null;
        _client.Setup(c => c.SendAsync("first", It.IsAny<string>(), It.IsAny<IReadOnlyDictionary<string, object>>(),
                It.IsAny<DateTimeOffset>(), It.IsAny<CancellationToken>()))
            .Callback((string _, string _, IReadOnlyDictionary<string, object> _, DateTimeOffset at, CancellationToken _) =>
                deliverAt = at)
            .ReturnsAsync(1);

        await Publisher.SchedulePublishAsync(new Ping(1), TimeSpan.FromMinutes(5), Ct);

        Assert.NotNull(deliverAt);
        Assert.InRange(deliverAt.Value - DateTimeOffset.UtcNow, TimeSpan.FromMinutes(4), TimeSpan.FromMinutes(5));
    }

    [Fact]
    public async Task A_batch_Should_be_one_write_per_queue()
    {
        _topology.Subscribe(typeof(Ping), "first");
        int writes = 0;
        _client.Setup(c => c.SendBatchAsync("first", It.Is<IReadOnlyList<string>>(b => b.Count == 3),
                It.Is<IReadOnlyList<IReadOnlyDictionary<string, object>>>(h => h.Count == 3), It.IsAny<CancellationToken>()))
            .Callback(() => writes++)
            .ReturnsAsync([1L, 2L, 3L]);

        await Publisher.PublishBatchAsync([new Ping(1), new Ping(2), new Ping(3)], Ct);

        Assert.Equal(1, writes);
    }

    [Fact]
    public async Task A_message_picked_at_run_time_Should_route_by_the_given_type()
    {
        _topology.Subscribe(typeof(Ping), "first");
        _client.Setup(c => c.SendAsync("first", It.IsAny<string>(), It.IsAny<IReadOnlyDictionary<string, object>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(1);

        object message = new Ping(1);
        await Publisher.PublishAsync(message, message.GetType(), Ct);

        _client.Verify(c => c.SendAsync("first", It.IsAny<string>(), It.IsAny<IReadOnlyDictionary<string, object>>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    public sealed record Ping(int Id);
}
