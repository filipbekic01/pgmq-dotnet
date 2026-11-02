using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Pgmq;
using Pgmq.Examples.WebApi;

// A Web API that publishes messages from HTTP endpoints and handles them in background consumers, in the same process.
// A worker service without HTTP sets Pgmq up the same way, on Host.CreateApplicationBuilder.
// Start PostgreSQL with pgmq, then the app:
//
//   docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres ghcr.io/pgmq/pg18-pgmq:v1.13.1
//   dotnet run --project examples/Pgmq.Examples.WebApi
//
// Then send the requests in Pgmq.Examples.WebApi.http, and watch GET /activity and GET /queues.

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<ActivityFeed>();

builder.Services.AddPgmq(options =>
{
    options.ConnectionString = builder.Configuration.GetConnectionString("Pgmq");
    options.Database = "example-transport"; // its own database, created on start

    // Queue depth gauges (pgmq.queue.messages and friends), off unless set.
    options.QueueMetricsInterval = TimeSpan.FromSeconds(30);
});
builder.Services.AddPgmqConsumers(typeof(Program).Assembly); // every IPgmqConsumer<T> in this project
builder.Services.AddHealthChecks().AddPgmq();

// Pgmq only emits telemetry; the app decides where it goes. Here: traces, metrics and logs to an OpenTelemetry
// collector, when OTEL_EXPORTER_OTLP_ENDPOINT (plus the other standard OTEL_* variables) names one, for example
// grafana/otel-lgtm:
//
//     docker run -d -p 3000:3000 -p 4318:4318 grafana/otel-lgtm
//     dotnet run --project examples/Pgmq.Examples.WebApi --launch-profile otlp
if (!string.IsNullOrWhiteSpace(builder.Configuration["OTEL_EXPORTER_OTLP_ENDPOINT"]))
{
    builder.Services.AddOpenTelemetry()
        .ConfigureResource(r => r.AddService("pgmq-example-webapi"))
        // The HTTP request span is the root, so a trace runs request -> send -> process -> the consumer's own sends.
        .WithTracing(t => t.AddAspNetCoreInstrumentation().AddSource(PgmqTelemetry.SourceName))
        .WithMetrics(m => m.AddMeter(PgmqTelemetry.MeterName))
        // While a message is handled, the log scope carries Queue, MessageType, MessageId and Attempt, so each line
        // can be found by the message-id header of an archived message.
        .WithLogging(_ => { }, o =>
        {
            o.IncludeScopes = true;
            o.IncludeFormattedMessage = true; // the rendered text, not the template, as the log body
        })
        .UseOtlpExporter();
}

WebApplication app = builder.Build();

app.MapGet("/", () => Results.Text("""
    Pgmq.NET example. Try:
      POST /orders                   place an order (a flaky gateway with "paymentFailures")
      POST /notifications            publish a notification by type
      POST /notifications/{key}      publish to a topic routing key, e.g. notify.email.ana
      GET  /topics/{key}             which queues a routing key reaches
      POST /ledger                   FIFO: ordered entries per account
      POST /reminders                deliver a message later
      GET  /activity                 what the consumers did, newest first
      GET  /queues                   queue depth and totals
      DELETE /queues/{name}          purge a queue
      GET  /health                   health check
    """));

// Type-based publish: every consumer of OrderPlaced gets its own copy.
app.MapPost("/orders", async (PlaceOrder request, IPgmqPublisher publisher, CancellationToken ct) =>
{
    // Check before publishing: a bad order caught here is a 400, not an archived message later.
    Dictionary<string, string[]> errors = [];
    if (string.IsNullOrWhiteSpace(request.Customer))
    {
        errors["customer"] = ["A customer is required."];
    }

    if (request.Total <= 0)
    {
        errors["total"] = ["The total must be more than 0."];
    }

    if (errors.Count > 0)
    {
        return Results.ValidationProblem(errors);
    }

    OrderPlaced order = new(Guid.CreateVersion7(), request.Customer!.Trim(), request.Total, request.PaymentFailures);
    await publisher.PublishAsync(order, ct);
    return Results.Accepted(value: order);
});

// Publish by type, with a custom header the consumers can read.
app.MapPost("/notifications", async (SendNotification request, IPgmqPublisher publisher, CancellationToken ct) =>
{
    await publisher.PublishAsync(new Notification(request.Text),
        new PgmqPublishOptions { Headers = new Dictionary<string, string> { ["x-source"] = "api" } }, ct);
    return Results.Accepted();
});

// Topic publish: reaches every queue bound to a matching pattern. Returns how many queues that was.
app.MapPost("/notifications/{routingKey}",
    async (string routingKey, SendNotification request, IPgmqPublisher publisher, CancellationToken ct) =>
    {
        int queues = await publisher.PublishToTopicAsync(routingKey, new Notification(request.Text), ct);
        return Results.Accepted(value: new { routingKey, queues });
    });

app.MapGet("/topics/{routingKey}", async (string routingKey, IPgmqQueueManager queues, CancellationToken ct) =>
    await queues.TestRoutingAsync(routingKey, ct));

// FIFO groups: the account is the group, so each account's entries are handled in order.
app.MapPost("/ledger", async (FillLedger request, IPgmqPublisher publisher, CancellationToken ct) =>
{
    for (int sequence = 1; sequence <= request.EntriesPerAccount; sequence++)
    {
        for (int account = 1; account <= request.Accounts; account++)
        {
            string id = $"ACC-{account}";
            LedgerEntry entry = new(id, sequence, Random.Shared.Next(-500, 500));
            await publisher.PublishAsync(entry, new PgmqPublishOptions { GroupId = id }, ct);
        }
    }

    return Results.Accepted(value: new { published = request.Accounts * request.EntriesPerAccount });
});

// Delayed delivery: the message sits invisible in the queue until the delay is up.
app.MapPost("/reminders", async (ScheduleReminder request, IPgmqPublisher publisher, TimeProvider time,
    CancellationToken ct) =>
{
    await publisher.SchedulePublishAsync(new Reminder(request.Text, time.GetUtcNow()),
        TimeSpan.FromSeconds(request.DelaySeconds), ct);
    return Results.Accepted(value: new { deliverIn = $"{request.DelaySeconds} s" });
});

app.MapGet("/activity", (ActivityFeed feed, int? take) => feed.Recent(take ?? 50));
app.MapDelete("/activity", (ActivityFeed feed) =>
{
    feed.Clear();
    return Results.NoContent();
});

app.MapGet("/queues", async (IPgmqQueueManager queues, CancellationToken ct) =>
    (await queues.GetAllMetricsAsync(ct)).Select(m => new
    {
        m.QueueName,
        m.QueueLength,
        m.VisibleLength,
        m.TotalMessages,
        OldestMessageAgeSeconds = m.OldestMessageAge?.TotalSeconds,
    }));

app.MapDelete("/queues/{name}", async (string name, IPgmqQueueManager queues, CancellationToken ct) =>
    Results.Ok(new { purged = await queues.PurgeQueueAsync(name, ct) }));

app.MapHealthChecks("/health");

app.Run();

public sealed record PlaceOrder(string? Customer, decimal Total, int PaymentFailures = 0);

public sealed record SendNotification(string Text);

public sealed record FillLedger(int Accounts = 3, int EntriesPerAccount = 5);

public sealed record ScheduleReminder(string Text, int DelaySeconds = 10);
