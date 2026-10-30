using Pgmq.Examples.Worker;

// A worker that publishes an OrderCreated every two seconds and handles it. Without a connection string it starts,
// says how to give it one, and does nothing else:
//
//   docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres ghcr.io/pgmq/pg18-pgmq:v1.13.1
//   dotnet run --project examples/Pgmq.Examples.Worker -- \
//     --ConnectionStrings:Pgmq "Host=localhost;Username=postgres;Password=postgres;Database=postgres"

HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

string? connectionString = builder.Configuration.GetConnectionString("Pgmq");

if (string.IsNullOrWhiteSpace(connectionString))
{
    builder.Services.AddHostedService<NoDatabaseNotice>();
}
else
{
    builder.Services.AddPgmq(options => options.ConnectionString = connectionString);
    builder.Services.AddPgmqConsumer<OrderCreatedHandler>();
    builder.Services.AddHostedService<OrderPublisher>();
}

await builder.Build().RunAsync();
