# Pgmq.NET

Strongly typed messaging for .NET on [PGMQ](https://github.com/pgmq/pgmq), the PostgreSQL message queue extension.
Publish a message, and every consumer subscribed to its type gets its own durable copy in PostgreSQL. A background
worker handles them with per-queue concurrency, leases, retries, timeouts and graceful shutdown.

The aim is a small, predictable alternative to full messaging frameworks such as MassTransit, Wolverine and Rebus:
publish and subscribe, nothing more. There are no sagas, workflow engines or middleware pipelines.

> **Status: early alpha (`0.1.0-alpha.1`), on NuGet.org as `Pgmq` and `Pgmq.Hosting`.** The features below are
> implemented and tested, including against a real PGMQ database. The public API may still change before 1.0. Pgmq.NET is an
> independent project, not affiliated with the PGMQ project.

## Packages

| Package | What it does |
|---|---|
| `Pgmq` | Publishing (`IPgmqPublisher`), the topology that decides which queues a message goes to, message headers, serialization and tracing. No hosting dependencies. |
| `Pgmq.Hosting` | Consumers (`IPgmqConsumer<T>`), the background worker, dependency injection, a health check and status reporting on the .NET Generic Host. References `Pgmq`. |

Both target .NET 10 and build on [Npgmq](https://github.com/brianpursley/Npgmq) and Npgsql.

```bash
dotnet add package Pgmq.Hosting --prerelease
```

## Quick start

You need PostgreSQL with the pgmq extension available, for example:

```bash
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres ghcr.io/pgmq/pg18-pgmq:v1.13.1
```

```csharp
builder.Services.AddPgmq(options => options.ConnectionString = connectionString);
builder.Services.AddPgmqConsumer<OrderCreatedHandler>();

public sealed record OrderCreated(Guid OrderId);

[QueueName("order_created")]
[ConsumerOptions(Concurrency = 4, TimeoutSeconds = 300)]
public sealed class OrderCreatedHandler(IBilling billing) : IPgmqConsumer<OrderCreated>
{
    public async Task HandleAsync(PgmqConsumeContext<OrderCreated> context, CancellationToken cancellationToken)
    {
        await billing.InvoiceAsync(context.Message.OrderId, cancellationToken);
        await context.PublishAsync(new InvoiceRequested(context.Message.OrderId), cancellationToken);
    }
}

// Anywhere else:
await publisher.PublishAsync(new OrderCreated(orderId), cancellationToken);
```

At start-up Pgmq creates the pgmq extension (if it can) and one queue per consumer. See
[`examples/Pgmq.Examples.Worker`](examples/Pgmq.Examples.Worker) for a runnable worker.

## How it behaves

- **Fan-out.** Each consumer has its own queue. A published message is written to the queue of every consumer of its
  type, or of a type it derives from.
- **At-least-once delivery.** A message is deleted only after its consumer returns. Consumers should be idempotent.
- **Independent queues.** Every queue has its own read loop and its own slots, so a full or slow queue never holds up
  another one. A slot frees as soon as its message is settled.
- **Leases.** A read message is hidden for `Lease` (30 s), renewed while its consumer runs. A long job keeps its
  message, and a crashed process gives it back within one lease.
- **Retries.** A failure that `IsTransient` accepts is retried after a backoff (30 s, 2 m, 8 m, 30 m), up to
  `MaxAttempts`. Any other failure, a timeout, or the last attempt archives the message to `pgmq.a_{queue}`.
- **Outbox.** What a consumer publishes through its context is sent only if it succeeds, so a retry does not send twice.
- **Timeouts.** A consumer past its timeout has its token cancelled and its message archived. One that ignores the
  token is abandoned, so its slot frees.
- **Graceful shutdown.** Reading stops, running consumers get `DrainGrace` to finish, and what stops on cancellation
  goes straight back to the queue.
- **Survival.** A failed read loop restarts itself. `PgmqStatus.StalledQueues` and the health check report a loop
  that went quiet.
- **Tracing.** Spans follow the OpenTelemetry messaging conventions under the `Pgmq` source. The trace continues from
  publisher to consumer through the message headers.

## Configuration

| Option | Default | |
|---|---|---|
| `ConnectionString` / `DataSource` | | One of the two is required. |
| `RunConsumers` | `true` | Off for a host that only publishes. |
| `FailOnSetupError` | `true` | Off to start without Pgmq when the database lacks it; the health check reports it. |
| `DefaultConcurrency` | `8` | Messages per queue at once, per instance. |
| `DefaultTimeout` | 1 hour | Per message. |
| `MaxAttempts` | `5` | |
| `IsTransient` | `PgmqTransientFailures.IsTransient` | Which failures are retried. |
| `Lease` / `RenewEvery` | 30 s / 10 s | |
| `RetryBackoff` / `MaxRetryBackoff` | 30 s / 30 min | |
| `DrainGrace` / `AbandonAfter` | 20 s / 30 s | |
| `MinIdleDelay` / `MaxIdleDelay` | 200 ms / 5 s | Polling of an empty queue. |
| `UnavailableHealthStatus` / `StalledHealthStatus` | `Unhealthy` | Use `Degraded` to keep a pod in service. |

A consumer's queue, concurrency and timeout come from `AddPgmqConsumer<T>(c => ...)`, then `[QueueName]` and
`[ConsumerOptions]`, then these defaults. Pin queue names with `[QueueName]` and message names with `[MessageName]`
before renaming or moving a class: the names live in the database.

Add `services.AddHealthChecks().AddPgmq()` for the health check, and the `Pgmq` source to your tracer for spans.

## Limitations

- **Polling, not push.** An idle queue picks up a new message within `MaxIdleDelay`. LISTEN/NOTIFY is not used yet.
- **Separate writes per queue.** Publishing to several queues is not one transaction, and there is no transactional
  outbox with your own database writes.
- **Archives grow.** Nothing trims `pgmq.a_*` tables.
- **No ordering guarantees** within a queue under concurrency.

## Repository

```
src/Pgmq                     core: publisher, topology, headers, tracing
src/Pgmq.Hosting             consumers, worker, dependency injection, health
tests/Pgmq.Tests             unit tests for the core
tests/Pgmq.Hosting.Tests     unit tests for hosting
tests/Pgmq.IntegrationTests  against a real PGMQ in Docker, including 100,000-message load tests
benchmarks/Pgmq.Benchmarks   throughput benchmark with a committed baseline
examples/Pgmq.Examples.Worker
docs/architecture.md
```

## Build and test

```bash
dotnet build -c Release
dotnet test --project tests/Pgmq.Tests -c Release
dotnet test --project tests/Pgmq.Hosting.Tests -c Release
dotnet test --project tests/Pgmq.IntegrationTests -c Release   # needs Docker
dotnet run -c Release --project benchmarks/Pgmq.Benchmarks     # needs Docker
```

The `Load` tests push 100,000 messages and take about a minute; leave them out with
`-- --filter-not-trait "Category=Load"`.

## License

[MIT](LICENSE)
