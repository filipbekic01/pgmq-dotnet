namespace Pgmq.Examples.Worker;

/// <summary>The message: an order was placed.</summary>
public sealed record OrderCreated(Guid OrderId, decimal Total);

/// <summary>Handles each <see cref="OrderCreated"/>. Its queue is pinned so the class can be renamed later.</summary>
[QueueName("examples_order_created")]
[ConsumerOptions(Concurrency = 4)]
public sealed class OrderCreatedHandler(ILogger<OrderCreatedHandler> logger) : IPgmqConsumer<OrderCreated>
{
    public Task HandleAsync(PgmqConsumeContext<OrderCreated> context, CancellationToken cancellationToken)
    {
        logger.LogInformation("Order {OrderId} created, total {Total} (delivery {Attempt})",
            context.Message.OrderId, context.Message.Total, context.RedeliveryCount + 1);
        return Task.CompletedTask;
    }
}

/// <summary>Publishes an order every two seconds.</summary>
public sealed class OrderPublisher(IPgmqPublisher publisher) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using PeriodicTimer timer = new(TimeSpan.FromSeconds(2));
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            await publisher.PublishAsync(new OrderCreated(Guid.CreateVersion7(), Random.Shared.Next(10, 500)), stoppingToken);
        }
    }
}

/// <summary>Says how to point the example at a database, when it has none.</summary>
public sealed class NoDatabaseNotice(ILogger<NoDatabaseNotice> logger) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogWarning("No ConnectionStrings:Pgmq is set, so nothing is published or consumed. " +
                          "Point it at a PostgreSQL database with the pgmq extension to run the example.");
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
