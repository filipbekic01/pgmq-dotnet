using Microsoft.Extensions.Logging.Abstractions;

namespace Pgmq.Tests;

public sealed class PublisherTests
{
    private readonly FakeSender _sender = new();
    private readonly PgmqTopology _topology = new();

    private PgmqPublisher Publisher => new(_sender, _topology, NullLogger<PgmqPublisher>.Instance);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_publish_Should_write_one_copy_to_each_subscribed_queue()
    {
        _topology.Subscribe(typeof(Ping), "first");
        _topology.Subscribe(typeof(Ping), "second");

        await Publisher.PublishAsync(new Ping(7), Ct);

        Assert.Equal(["first", "second"], _sender.Sends.Select(s => s.Queue));
        Assert.All(_sender.Sends, s => Assert.Equal("""{"id":7}""", s.Body));
        Assert.All(_sender.Sends, s => Assert.Equal(typeof(Ping).FullName, s.Headers["message-type"]));
        Assert.All(_sender.Sends, s => Assert.Null(s.DeliverAt));
    }

    [Fact]
    public async Task A_message_nobody_subscribes_to_Should_write_nothing()
    {
        await Publisher.PublishAsync(new Ping(1), Ct);

        Assert.Equal(0, _sender.Calls);
    }

    [Fact]
    public async Task A_scheduled_publish_Should_be_a_delayed_send()
    {
        _topology.Subscribe(typeof(Ping), "first");

        await Publisher.SchedulePublishAsync(new Ping(1), TimeSpan.FromMinutes(5), Ct);

        DateTimeOffset? deliverAt = Assert.Single(_sender.Sends).DeliverAt;
        Assert.NotNull(deliverAt);
        Assert.InRange(deliverAt.Value - DateTimeOffset.UtcNow, TimeSpan.FromMinutes(4), TimeSpan.FromMinutes(5));
    }

    [Fact]
    public async Task A_batch_Should_be_one_write_per_queue()
    {
        _topology.Subscribe(typeof(Ping), "first");

        await Publisher.PublishBatchAsync([new Ping(1), new Ping(2), new Ping(3)], Ct);

        (string queue, IReadOnlyList<string> bodies, IReadOnlyList<IReadOnlyDictionary<string, object>> headers) =
            Assert.Single(_sender.Batches);
        Assert.Equal("first", queue);
        Assert.Equal(3, bodies.Count);
        Assert.Equal(3, headers.Count);
        Assert.Equal(1, _sender.Calls);
    }

    [Fact]
    public async Task A_message_picked_at_run_time_Should_route_by_the_given_type()
    {
        _topology.Subscribe(typeof(Ping), "first");

        object message = new Ping(1);
        await Publisher.PublishAsync(message, message.GetType(), Ct);

        Assert.Equal("first", Assert.Single(_sender.Sends).Queue);
    }

    [Fact]
    public async Task A_publish_with_options_Should_carry_the_group_and_headers()
    {
        _topology.Subscribe(typeof(Ping), "first");

        await Publisher.PublishAsync(new Ping(1), new PgmqPublishOptions
        {
            GroupId = "customer-7",
            Headers = new Dictionary<string, string> { ["tenant"] = "acme" },
        }, Ct);

        IReadOnlyDictionary<string, object> headers = Assert.Single(_sender.Sends).Headers;
        Assert.Equal("customer-7", headers["x-pgmq-group"]);
        Assert.Equal("acme", headers["tenant"]);
        Assert.Equal(typeof(Ping).FullName, headers["message-type"]);
    }

    [Fact]
    public async Task A_header_Pgmq_uses_itself_Should_be_refused()
    {
        _topology.Subscribe(typeof(Ping), "first");

        await Assert.ThrowsAsync<ArgumentException>(() => Publisher.PublishAsync(new Ping(1), new PgmqPublishOptions
        {
            Headers = new Dictionary<string, string> { ["x-pgmq-group"] = "sneaky" },
        }, Ct));
    }

    [Fact]
    public async Task Both_a_delay_and_a_delivery_time_Should_be_refused()
        => await Assert.ThrowsAsync<ArgumentException>(() => Publisher.PublishAsync(new Ping(1),
            new PgmqPublishOptions { Delay = TimeSpan.FromSeconds(1), DeliverAt = DateTimeOffset.UtcNow }, Ct));

    [Fact]
    public async Task A_topic_publish_Should_be_one_send_topic_with_the_delay_rounded_up()
    {
        _sender.TopicQueues = 2;

        int queues = await Publisher.PublishToTopicAsync("orders.eu.created", new Ping(3),
            new PgmqPublishOptions { Delay = TimeSpan.FromMilliseconds(1500) }, Ct);

        (string routingKey, string body, int delay) = Assert.Single(_sender.TopicSends);
        Assert.Equal(("orders.eu.created", """{"id":3}""", 2), (routingKey, body, delay));
        Assert.Equal(2, queues);
    }

    [Fact]
    public async Task A_topic_batch_Should_count_the_queues_it_reached()
    {
        _sender.BatchTopicQueues = ["a", "a", "b", "b"];

        Assert.Equal(2, await Publisher.PublishBatchToTopicAsync("logs.app", [new Ping(1), new Ping(2)], Ct));
        Assert.Equal(1, _sender.Calls);
    }

    public sealed record Ping(int Id);

    /// <summary>Records what the publisher asks pgmq to write.</summary>
    private sealed class FakeSender : IPgmqSender
    {
        public List<(string Queue, string Body, IReadOnlyDictionary<string, object> Headers, DateTimeOffset? DeliverAt)>
            Sends { get; } = [];

        public List<(string Queue, IReadOnlyList<string> Bodies, IReadOnlyList<IReadOnlyDictionary<string, object>>
            Headers)> Batches { get; } = [];

        public List<(string RoutingKey, string Body, int Delay)> TopicSends { get; } = [];

        public int TopicQueues { get; set; }

        public IReadOnlyList<string> BatchTopicQueues { get; set; } = [];

        public int Calls { get; private set; }

        public Task<long> SendAsync(string queueName, string body, IReadOnlyDictionary<string, object> headers,
            DateTimeOffset? deliverAt, CancellationToken cancellationToken)
        {
            Calls++;
            Sends.Add((queueName, body, headers, deliverAt));
            return Task.FromResult((long)Sends.Count);
        }

        public Task<IReadOnlyList<long>> SendBatchAsync(string queueName, IReadOnlyList<string> bodies,
            IReadOnlyList<IReadOnlyDictionary<string, object>> headers, DateTimeOffset? deliverAt,
            CancellationToken cancellationToken)
        {
            Calls++;
            Batches.Add((queueName, bodies, headers));
            return Task.FromResult<IReadOnlyList<long>>([.. Enumerable.Range(1, bodies.Count).Select(i => (long)i)]);
        }

        public Task<int> SendTopicAsync(string routingKey, string body, IReadOnlyDictionary<string, object> headers,
            int delaySeconds, CancellationToken cancellationToken)
        {
            Calls++;
            TopicSends.Add((routingKey, body, delaySeconds));
            return Task.FromResult(TopicQueues);
        }

        public Task<IReadOnlyList<string>> SendBatchTopicAsync(string routingKey, IReadOnlyList<string> bodies,
            IReadOnlyList<IReadOnlyDictionary<string, object>> headers, int delaySeconds,
            CancellationToken cancellationToken)
        {
            Calls++;
            return Task.FromResult(BatchTopicQueues);
        }
    }
}
