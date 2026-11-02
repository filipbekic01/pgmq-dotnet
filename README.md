# Pgmq.NET

|  | Pgmq.NET | [pgmq](https://github.com/pgmq/pgmq) |
|---|:---:|:---:|
| Every pgmq queue feature, one to one, with nothing added to or changed in its SQL | ✅ | ✅ |
| Background worker service that runs your consumers | ✅ | ❌ |
| OpenTelemetry traces and metrics out of the box | ✅ | ❌ |
| Shovel functionality via routines | ✅ | ❌ |

[pgmq](https://github.com/pgmq/pgmq) is the standard for message queues in PostgreSQL. Pgmq.NET uses it as it is and
adds a runtime service for .NET that consumes your messages for you.

## Run it

```bash
dotnet add package Pgmq.Hosting
```

```csharp
builder.Services.AddPgmq(o => o.ConnectionString = "Host=localhost;Username=postgres;Password=postgres");
builder.Services.AddPgmqConsumer<OrderCreatedHandler>();

// Optional: Pgmq's traces and metrics, through your app's own OpenTelemetry setup.
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource(PgmqTelemetry.SourceName))
    .WithMetrics(m => m.AddMeter(PgmqTelemetry.MeterName))
    .UseOtlpExporter();

public sealed record OrderCreated(Guid OrderId);

public sealed class OrderCreatedHandler : IPgmqConsumer<OrderCreated>
{
    public Task HandleAsync(PgmqConsumeContext<OrderCreated> context, CancellationToken ct)
    {
        Console.WriteLine($"Order {context.Message.OrderId}");
        return Task.CompletedTask;
    }
}

// Anywhere, via IPgmqPublisher:
await publisher.PublishAsync(new OrderCreated(Guid.NewGuid()));
```

A runnable example, a Web API that publishes and consumes in one process, to play with every feature:
[`examples/Pgmq.Examples.WebApi`](examples/Pgmq.Examples.WebApi). Behavior, all options and PGMQ feature support are
in [docs/guide.md](docs/guide.md).

## License

[MIT](LICENSE)
