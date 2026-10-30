# Pgmq.NET

Publish/subscribe messaging for .NET, stored in PostgreSQL through the [PGMQ](https://github.com/pgmq/pgmq) extension.
No separate broker: your database is the queue.

You publish a typed message; every consumer of that type gets its own durable copy and handles it in a background
worker, with retries, timeouts and graceful shutdown. Consumers wake up instantly through PostgreSQL LISTEN/NOTIFY,
with long polling as the fallback.

> Early alpha (`0.1.0-alpha.4`). The API may change before 1.0.

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

## Where it lives

| | Default | Override |
|---|---|---|
| Database | The one in the connection string. Without `Database=` there, Npgsql uses the user name, so `postgres` in the example above. | `o.Database = "transport"`, or name it in `o.ConnectionString`. |
| Schema | `pgmq`: the queue registry `pgmq.meta`, every function, and one table pair per queue (`pgmq.q_<queue>` and `pgmq.a_<queue>`). The SQL install also adds `pgmq.__pgmq_migrations`. | Not possible: pgmq pins its schema. The extension cannot be relocated, and its SQL refers to `pgmq.` throughout. To keep pgmq apart from your own tables, give it its own database. |

`o.Database` lets an application pass its own connection string and still keep pgmq in a separate database:

```csharp
builder.Services.AddPgmq(o =>
{
    o.ConnectionString = builder.Configuration.GetConnectionString("App"); // server and credentials
    o.Database = "transport";                                             // pgmq's own database
});
```

With `o.DataSource` instead, the data source decides the database, and `o.Database` is refused.

## Setup on start

On start, Pgmq sets up its database, so it needs no migrations of its own. It creates the database if it is missing,
installs pgmq, and creates one queue per consumer. Every later start brings pgmq up to date. pgmq goes in as the
extension where the server has it, and as the extension's own SQL, embedded in the package, where it does not (RDS,
Azure and other managed servers). All of it is idempotent and safe from several instances at once.

- `o.CreateDatabase = false`: never create the database. It must already exist.
- `o.MaintenanceDatabase = "..."`: the database used to create it (`postgres` by default).
- `o.InstallMode = PgmqInstallMode.Extension` or `Sql`: allow only one way of installing pgmq.
- `o.AutoSetup = false`: manage the schema yourself. Pgmq then only checks that everything exists.

A runnable worker is in [`examples/Pgmq.Examples.Worker`](examples/Pgmq.Examples.Worker). Behavior, all options and
PGMQ feature support are in [docs/guide.md](docs/guide.md).

## License

[MIT](LICENSE)
