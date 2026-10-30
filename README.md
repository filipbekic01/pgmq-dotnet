# Pgmq.NET

Publish/subscribe messaging for .NET, stored in PostgreSQL through the [PGMQ](https://github.com/pgmq/pgmq) extension.
No separate broker: your database is the queue.

You publish a typed message; every consumer of that type gets its own durable copy and handles it in a background
worker, with retries, timeouts and graceful shutdown. Consumers wake up instantly through PostgreSQL LISTEN/NOTIFY,
with long polling as the fallback.

> Early alpha (`0.1.0-alpha.3`). The API may change before 1.0.

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

On start, Pgmq sets up the database, so it needs no migrations of its own. It creates the database named in the
connection string if it is missing, installs pgmq, and creates one queue per consumer. Every later start brings pgmq
up to date. pgmq goes in as the extension where the server has it, and as the extension's own SQL, embedded in the
package, where it does not (RDS, Azure and other managed servers). All of it is idempotent and safe from several
instances at once. To manage the schema yourself, set `o.AutoSetup = false`; Pgmq then only checks that everything
exists.

A runnable worker is in [`examples/Pgmq.Examples.Worker`](examples/Pgmq.Examples.Worker). Behavior, all options and
PGMQ feature support are in [docs/guide.md](docs/guide.md).

## License

[MIT](LICENSE)
