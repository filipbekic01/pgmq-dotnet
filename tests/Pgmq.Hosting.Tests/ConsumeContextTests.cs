using System.Data.Common;
using System.Net;
using Moq;

namespace Pgmq.Hosting.Tests;

/// <summary>
/// What a consumer sends waits until it succeeds, so a retry does not send twice; and a flush goes through whatever
/// publisher it is handed, so the worker can run it in the transaction that deletes the message.
/// </summary>
public sealed class ConsumeContextTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Sends_from_a_queued_message_Should_wait_in_the_outbox()
    {
        Mock<IPgmqPublisher> publisher = new(MockBehavior.Strict);
        Outbox outbox = new();
        PgmqConsumeContext<Ping> context = new(new Ping(), publisher.Object,
            sendBack: (_, _, _, _, _, _, _, _, _) => Task.CompletedTask, outbox, Guid.NewGuid(), 0);

        await context.PublishAsync(new Ping(), Ct);
        await context.SchedulePublishAsync(new Ping(), TimeSpan.FromMinutes(1), Ct);
        await context.RedeliverAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(3, outbox.Count);
        publisher.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task A_redelivery_Should_keep_the_message_id_and_count_up()
    {
        Guid messageId = Guid.NewGuid();
        (int Count, Guid? Id)? sentBack = null;
        Outbox outbox = new();
        PgmqConsumeContext<Ping> context = new(new Ping(), Mock.Of<IPgmqPublisher>(),
            sendBack: (_, _, _, _, count, id, _, _, _) =>
            {
                sentBack = (count, id);
                return Task.CompletedTask;
            }, outbox, messageId, redeliveryCount: 2);

        await context.RedeliverAsync(TimeSpan.FromSeconds(1));
        await outbox.FlushAsync(Mock.Of<IPgmqPublisher>(), Ct);

        Assert.Equal((3, messageId), sentBack);
    }

    [Fact]
    public async Task A_redelivery_Should_keep_the_group_and_headers()
    {
        (string? Group, IReadOnlyDictionary<string, string>? Headers)? sentBack = null;
        Outbox outbox = new();
        Dictionary<string, string> headers = new() { ["tenant"] = "acme" };
        PgmqConsumeContext<Ping> context = new(new Ping(), Mock.Of<IPgmqPublisher>(),
            sendBack: (_, _, _, _, _, _, group, sent, _) =>
            {
                sentBack = (group, sent);
                return Task.CompletedTask;
            }, outbox, Guid.NewGuid(), redeliveryCount: 0, groupId: "customer-1", headers: headers);

        await context.RedeliverAsync(TimeSpan.FromSeconds(1));
        await outbox.FlushAsync(Mock.Of<IPgmqPublisher>(), Ct);

        Assert.Equal("customer-1", sentBack?.Group);
        Assert.Equal("acme", sentBack?.Headers?["tenant"]);
        Assert.Equal("customer-1", context.GroupId);
        Assert.Equal("acme", context.Headers["tenant"]);
    }

    [Fact]
    public async Task A_context_built_by_a_test_Should_send_at_once()
    {
        Mock<IPgmqPublisher> publisher = new();
        PgmqConsumeContext<Ping> context = new(new Ping(), publisher.Object);

        await context.PublishAsync(new Ping(), Ct);

        publisher.Verify(p => p.PublishAsync(It.IsAny<Ping>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task A_topic_publish_with_an_invalid_routing_key_Should_throw_in_the_consumer_and_queue_nothing()
    {
        Mock<IPgmqPublisher> publisher = new(MockBehavior.Strict);
        Outbox outbox = new();
        PgmqConsumeContext<Ping> context = new(new Ping(), publisher.Object,
            sendBack: (_, _, _, _, _, _, _, _, _) => Task.CompletedTask, outbox, Guid.NewGuid(), 0);

        await Assert.ThrowsAsync<ArgumentException>(() =>
            context.PublishToTopicAsync("notify.email.big spender", new Ping(), Ct));

        Assert.Equal(0, outbox.Count);
        publisher.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task A_flush_Should_send_through_the_publisher_it_is_handed()
    {
        Mock<IPgmqPublisher> held = new(MockBehavior.Strict);
        Mock<IPgmqPublisher> flushedThrough = new();
        Outbox outbox = new();
        PgmqConsumeContext<Ping> context = new(new Ping(), held.Object,
            sendBack: (_, _, _, _, _, _, _, _, _) => Task.CompletedTask, outbox, Guid.NewGuid(), 0);

        await context.PublishAsync(new Ping(), Ct);
        await outbox.FlushAsync(flushedThrough.Object, Ct);

        flushedThrough.Verify(p => p.PublishAsync(It.IsAny<Ping>(), It.IsAny<CancellationToken>()), Times.Once);
        held.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task A_flush_that_fails_Should_send_everything_again_next_time()
    {
        // The worker flushes in a transaction: one that fails rolled back what went out, so the next try sends it all.
        Outbox outbox = new();
        List<string> sent = [];
        bool failOnce = true;

        outbox.Add((_, _) =>
        {
            sent.Add("first");
            return Task.CompletedTask;
        });
        outbox.Add((_, _) =>
        {
            if (failOnce)
            {
                failOnce = false;
                throw new TimeoutException();
            }

            sent.Add("second");
            return Task.CompletedTask;
        });

        await Assert.ThrowsAsync<TimeoutException>(() => outbox.FlushAsync(Mock.Of<IPgmqPublisher>(), Ct));
        Assert.Equal(2, outbox.Count);

        await outbox.FlushAsync(Mock.Of<IPgmqPublisher>(), Ct);

        Assert.Equal(["first", "first", "second"], sent);
    }

    [Theory]
    [MemberData(nameof(Failures))]
    public void Only_failures_that_say_nothing_about_the_message_Should_be_transient(Exception exception, bool transient)
        => Assert.Equal(transient, PgmqTransientFailures.IsTransient(exception));

    public static TheoryData<Exception, bool> Failures() => new()
    {
        { new TimeoutException(), true },
        { new TransientDbException(), true },
        { new HttpRequestException("busy", null, HttpStatusCode.TooManyRequests), true },
        { new HttpRequestException("down", null, HttpStatusCode.ServiceUnavailable), true },
        { new InvalidOperationException("wrapped", new TimeoutException()), true },
        { new HttpRequestException("bad", null, HttpStatusCode.BadRequest), false },
        { new InvalidOperationException("a bug"), false },
        { new ArgumentException("bad input"), false },
    };

    public sealed record Ping;

    private sealed class TransientDbException : DbException
    {
        public override bool IsTransient => true;
    }
}
