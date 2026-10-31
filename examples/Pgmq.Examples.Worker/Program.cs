using OpenTelemetry;
using OpenTelemetry.Resources;
using Pgmq;
using Pgmq.Examples.Worker;

// A worker that publishes an OrderCreated every two seconds and handles it. Without a connection string it starts,
// says how to give it one, and does nothing else:
//
//   docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres ghcr.io/pgmq/pg18-pgmq:v1.13.1
//   dotnet run --project examples/Pgmq.Examples.Worker -- \
//     --ConnectionStrings:Pgmq "Host=localhost;Username=postgres;Password=postgres;Database=postgres"
//
// To see its traces, metrics and logs, point it at an OpenTelemetry collector, for example the Aspire dashboard:
//
//   docker run -d -p 18888:18888 -p 4317:18889 mcr.microsoft.com/dotnet/aspire-dashboard
//   OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 dotnet run --project examples/Pgmq.Examples.Worker -- ...

HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

string? connectionString = builder.Configuration.GetConnectionString("Pgmq");

// Pgmq only emits telemetry; the application decides what to collect and where to send it. Here: Pgmq's spans and
// metrics plus the host's logs, to whatever OTLP endpoint the standard OTEL_* environment variables name.
if (!string.IsNullOrWhiteSpace(builder.Configuration["OTEL_EXPORTER_OTLP_ENDPOINT"]))
{
    builder.Services.AddOpenTelemetry()
        .ConfigureResource(r => r.AddService("pgmq-example-worker"))
        .WithTracing(t => t.AddSource(PgmqTelemetry.SourceName))
        .WithMetrics(m => m.AddMeter(PgmqTelemetry.MeterName))
        .WithLogging()
        .UseOtlpExporter();
}

if (string.IsNullOrWhiteSpace(connectionString))
{
    builder.Services.AddHostedService<NoDatabaseNotice>();
}
else
{
    builder.Services.AddPgmq(options =>
    {
        options.ConnectionString = connectionString;
        options.QueueMetricsInterval = TimeSpan.FromSeconds(15); // queue depth gauges
    });
    builder.Services.AddPgmqConsumer<OrderCreatedHandler>();
    builder.Services.AddHostedService<OrderPublisher>();
}

await builder.Build().RunAsync();
