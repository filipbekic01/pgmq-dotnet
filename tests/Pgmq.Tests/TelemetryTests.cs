using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;

namespace Pgmq.Tests;

public sealed class TelemetryTests : IDisposable
{
    private readonly TelemetryCapture _capture = new();
    private readonly PgmqTopology _topology = new();
    private readonly Sender _sender = new();

    private PgmqPublisher Publisher => new(_sender, _topology, NullLogger<PgmqPublisher>.Instance,
        new ServerEndpoint("db.example", 5432));

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public void Dispose() => _capture.Dispose();

    [Fact]
    public async Task A_fan_out_Should_be_one_send_span_that_every_copy_is_parented_to()
    {
        _topology.Subscribe(typeof(TelemetryPing), "tel_first");
        _topology.Subscribe(typeof(TelemetryPing), "tel_second");

        await Publisher.PublishAsync(new TelemetryPing(), Ct);

        string messageType = MessageNames.Of(typeof(TelemetryPing));
        Activity send = Assert.Single(_capture.Spans(messageType));
        Assert.Equal($"send {messageType}", send.DisplayName);
        Assert.Equal(ActivityKind.Producer, send.Kind);
        Assert.Equal("pgmq", send.GetTagItem("messaging.system"));
        Assert.Equal("send", send.GetTagItem("messaging.operation.name"));
        Assert.Equal(new[] { "tel_first", "tel_second" }, send.GetTagItem("messaging.pgmq.queues"));
        Assert.Equal("db.example", send.GetTagItem("server.address"));
        Assert.Equal(5432, send.GetTagItem("server.port"));
        Assert.All(_sender.Headers, h => Assert.Equal(send.Id, h["traceparent"]));
    }

    [Fact]
    public async Task A_send_Should_count_each_copy_under_its_queue_and_time_the_call()
    {
        _topology.Subscribe(typeof(TelemetryPing), "tel_count_a");
        _topology.Subscribe(typeof(TelemetryPing), "tel_count_b");

        await Publisher.PublishBatchAsync([new TelemetryPing(), new TelemetryPing()], Ct);

        Assert.Equal(2, _capture.Sum("messaging.client.sent.messages", "tel_count_a"));
        Assert.Equal(2, _capture.Sum("messaging.client.sent.messages", "tel_count_b"));

        TelemetryCapture.Measured duration =
            Assert.Single(_capture.Of("messaging.client.operation.duration", MessageNames.Of(typeof(TelemetryPing))));
        Assert.Equal("send", duration.Tags["messaging.operation.type"]);
        Assert.False(duration.Tags.ContainsKey("error.type"));
    }

    [Fact]
    public async Task A_failed_send_Should_mark_its_span_and_duration_with_the_error_type()
    {
        _topology.Subscribe(typeof(TelemetryPing), "tel_failing");
        _sender.Fail = true;

        await Assert.ThrowsAsync<InvalidOperationException>(() => Publisher.PublishAsync(new TelemetryPing(), Ct));

        string messageType = MessageNames.Of(typeof(TelemetryPing));
        Activity send = Assert.Single(_capture.Spans(messageType));
        Assert.Equal(ActivityStatusCode.Error, send.Status);
        Assert.Equal(typeof(InvalidOperationException).FullName, send.GetTagItem("error.type"));
        Assert.Contains(send.Events, e => e.Name == "exception");

        TelemetryCapture.Measured duration =
            Assert.Single(_capture.Of("messaging.client.operation.duration", messageType));
        Assert.Equal(typeof(InvalidOperationException).FullName, duration.Tags["error.type"]);
        Assert.Equal(0, _capture.Sum("messaging.client.sent.messages", "tel_failing"));
    }

    [Fact]
    public async Task Nothing_listening_Should_still_carry_the_callers_trace()
    {
        _capture.Dispose();
        _topology.Subscribe(typeof(TelemetryPing), "tel_quiet");

        using ActivitySource caller = new("Pgmq.Tests.Caller");
        using ActivityListener listener = new()
        {
            ShouldListenTo = s => s.Name == caller.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(listener);

        using (Activity request = caller.StartActivity("request")!)
        {
            await Publisher.PublishAsync(new TelemetryPing(), Ct);
            Assert.Equal(request.Id, Assert.Single(_sender.Headers)["traceparent"]);
        }
    }

    public sealed record TelemetryPing;

    private sealed class Sender : IPgmqSender
    {
        public bool Fail { get; set; }

        public ConcurrentQueue<IReadOnlyDictionary<string, object>> Headers { get; } = new();

        public Task<long> SendAsync(string queueName, string body, IReadOnlyDictionary<string, object> headers,
            DateTimeOffset? deliverAt, CancellationToken cancellationToken)
        {
            ThrowIfFailing();
            Headers.Enqueue(headers);
            return Task.FromResult(1L);
        }

        public Task<IReadOnlyList<long>> SendBatchAsync(string queueName, IReadOnlyList<string> bodies,
            IReadOnlyList<IReadOnlyDictionary<string, object>> headers, DateTimeOffset? deliverAt,
            CancellationToken cancellationToken)
        {
            ThrowIfFailing();
            foreach (IReadOnlyDictionary<string, object> h in headers)
            {
                Headers.Enqueue(h);
            }

            return Task.FromResult<IReadOnlyList<long>>([.. bodies.Select((_, i) => (long)i)]);
        }

        public Task<int> SendTopicAsync(string routingKey, string body, IReadOnlyDictionary<string, object> headers,
            int delaySeconds, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<IReadOnlyList<string>> SendBatchTopicAsync(string routingKey, IReadOnlyList<string> bodies,
            IReadOnlyList<IReadOnlyDictionary<string, object>> headers, int delaySeconds,
            CancellationToken cancellationToken)
            => throw new NotSupportedException();

        private void ThrowIfFailing()
        {
            if (Fail)
            {
                throw new InvalidOperationException("The database is gone.");
            }
        }
    }
}
