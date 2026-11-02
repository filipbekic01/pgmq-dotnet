# Pgmq.NET

> Pgmq.NET follows the official [pgmq/pgmq](https://github.com/pgmq/pgmq) one to one: every queue feature is pgmq's
> own, with nothing added to or changed in its SQL. This library adds background workers on top of it, and that is all.

Publish/subscribe messaging for .NET, stored in PostgreSQL through the [PGMQ](https://github.com/pgmq/pgmq) extension.
No separate broker: your database is the queue.

You publish a typed message; every consumer of that type gets its own durable copy and handles it in a background
worker, with timeouts, archiving of failed messages and graceful shutdown. Consumers wake up instantly through PostgreSQL LISTEN/NOTIFY,
with long polling as the fallback.

## Run it

1. Start PostgreSQL with pgmq:

   ```bash
   docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres ghcr.io/pgmq/pg18-pgmq:v1.13.1
   ```

2. Add the package:

   ```bash
   dotnet add package Pgmq.Hosting --prerelease
   ```

3. Register Pgmq and a consumer, then publish:

   ```csharp
   builder.Services.AddPgmq(o => o.ConnectionString = "Host=localhost;Username=postgres;Password=postgres");
   builder.Services.AddPgmqConsumer<OrderCreatedHandler>();

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

## Retries: none

**A consumer that throws is never run again.** Its message is archived to `pgmq.a_<queue>` at once, where you can
inspect it, and the queue moves on.

Why: a library cannot know whether your consumer is idempotent, and most real consumers are not. A handler that
charged a card and then threw on its next line must not charge it again. Retrying the whole message replays every side
effect that already happened. The calls that fail for a passing reason are infrastructure calls (an HTTP API, a
database, a broker), so retry those inside the consumer, where you know what is safe to repeat, for example with
[Polly](https://github.com/App-vNext/Polly) or `Microsoft.Extensions.Http.Resilience`.

There is no option to turn consumer retries on. What the library still does for you, without running a consumer
twice:

- A failed delete or archive of a finished message is retried in place.
- What a consumer publishes through its context is sent only after it succeeds, in one transaction with the delete
  of its message, and a failed commit is retried in place.

Delivery is still at least once: a message whose process crashes mid-consumer, or whose consumer is cancelled by a
shutdown, comes back and runs again. Only `[ConsumerOptions(AtMostOnce = true)]` rules that out, at the cost of losing
the message instead.

## Setup on start

On start, Pgmq sets up its database, so it needs no migrations of its own. It creates the database if it is missing,
installs pgmq, and creates one queue per consumer. Every later start brings pgmq up to date. pgmq goes in as the
extension where the server has it, and as the extension's own SQL, embedded in the package, where it does not (RDS,
Azure and other managed servers). All of it is idempotent and safe from several instances at once.

- `o.CreateDatabase = false`: never create the database. It must already exist.
- `o.MaintenanceDatabase = "..."`: the database used to create it (`postgres` by default).
- `o.InstallMode = PgmqInstallMode.Extension` or `Sql`: allow only one way of installing pgmq.
- `o.AutoSetup = false`: manage the schema yourself. Pgmq then only checks that everything exists.

## Telemetry

Pgmq has no OpenTelemetry dependency. Add its source and meter to your app's OpenTelemetry setup, and your exporters
and sampling apply:

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource(PgmqTelemetry.SourceName))
    .WithMetrics(m => m.AddMeter(PgmqTelemetry.MeterName))
    .UseOtlpExporter(); // or any exporter you already use
```

Logs go through `ILogger`. For queue depth gauges, also set `o.QueueMetricsInterval = TimeSpan.FromSeconds(15)`.
Everything Pgmq emits is listed in [docs/guide.md](docs/guide.md#telemetry).

A runnable example, a Web API that publishes and consumes in one process, to play with every feature:
[`examples/Pgmq.Examples.WebApi`](examples/Pgmq.Examples.WebApi). Behavior, all options and PGMQ feature support are
in [docs/guide.md](docs/guide.md).

## License

[MIT](LICENSE)
