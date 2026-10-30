using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Pgmq.Hosting.Tests;

public sealed class RegistrationTests
{
    private const string Unreachable = "Host=127.0.0.1;Port=1;Username=u;Password=p;Database=d;Timeout=2";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void Options_without_a_connection_Should_be_refused()
        => Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddPgmq(_ => { }));

    [Fact]
    public void Database_Should_replace_the_database_of_the_connection_string_and_keep_the_rest()
    {
        PgmqOptions options = new() { ConnectionString = Unreachable, Database = "transport" };

        Npgsql.NpgsqlConnectionStringBuilder effective = new(options.EffectiveConnectionString);

        Assert.Equal("transport", effective.Database);
        Assert.Equal("127.0.0.1", effective.Host);
        Assert.Equal("u", effective.Username);
        Assert.Equal(Unreachable, new PgmqOptions { ConnectionString = Unreachable }.EffectiveConnectionString);
    }

    [Fact]
    public void Database_with_a_data_source_Should_be_refused()
    {
        using Npgsql.NpgsqlDataSource dataSource = Npgsql.NpgsqlDataSource.Create(Unreachable);

        Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddPgmq(o =>
        {
            o.DataSource = dataSource;
            o.Database = "transport";
        }));
    }

    [Fact]
    public void A_class_that_consumes_nothing_Should_be_refused()
        => Assert.Throws<ArgumentException>(() => new ServiceCollection().AddPgmqConsumer<NotAConsumer>());

    [Fact]
    public void The_publisher_and_status_Should_resolve_from_the_registrations()
    {
        using ServiceProvider services = Build(o => o.ConnectionString = Unreachable, s => s.AddPgmqConsumer<FirstConsumer>());

        Assert.IsType<PgmqPublisher>(services.GetRequiredService<IPgmqPublisher>());
        Assert.NotNull(services.GetRequiredService<PgmqStatus>());
        Assert.Equal(["first_queue"], services.GetRequiredService<PgmqTopology>().QueuesFor(typeof(Ping)));
    }

    [Fact]
    public void A_publish_only_host_Should_know_the_queues_but_run_no_worker()
    {
        using ServiceProvider services = Build(o =>
        {
            o.ConnectionString = Unreachable;
            o.RunConsumers = false;
        }, s => s.AddPgmqConsumer<FirstConsumer>());

        Assert.DoesNotContain(services.GetServices<IHostedService>(), h => h is ConsumerWorker);
        Assert.Single(services.GetRequiredService<PgmqTopology>().QueuesFor(typeof(Ping)));
    }

    [Fact]
    public void Every_consumer_in_an_assembly_Should_be_registered()
    {
        using ServiceProvider services = Build(o => o.ConnectionString = Unreachable,
            s => s.AddPgmqConsumers(typeof(FirstConsumer).Assembly));

        Assert.Contains(services.GetRequiredService<ConsumerRegistry>().Consumers,
            c => c.ConsumerType == typeof(FirstConsumer));
    }

    [Fact]
    public void Queue_name_and_settings_Should_come_from_registration_then_attributes_then_defaults()
    {
        using ServiceProvider services = Build(o =>
        {
            o.ConnectionString = Unreachable;
            o.DefaultConcurrency = 3;
            o.DefaultTimeout = TimeSpan.FromMinutes(7);
        }, s => s
            .AddPgmqConsumer<FirstConsumer>()
            .AddPgmqConsumer<SecondConsumer>(c =>
            {
                c.QueueName = "overridden";
                c.Concurrency = 9;
            })
            .AddPgmqConsumer<PlainConsumer>());

        ConsumerRegistration[] consumers = [.. services.GetRequiredService<ConsumerRegistry>().Consumers];
        ConsumerRegistration first = consumers.Single(c => c.ConsumerType == typeof(FirstConsumer));
        ConsumerRegistration second = consumers.Single(c => c.ConsumerType == typeof(SecondConsumer));
        ConsumerRegistration plain = consumers.Single(c => c.ConsumerType == typeof(PlainConsumer));

        Assert.Equal(("first_queue", 2, TimeSpan.FromSeconds(90)), (first.QueueName, first.Concurrency, first.Timeout));
        Assert.Equal(("overridden", 9, TimeSpan.FromMinutes(7)), (second.QueueName, second.Concurrency, second.Timeout));
        Assert.Equal((3, TimeSpan.FromMinutes(7)), (plain.Concurrency, plain.Timeout));
        Assert.Equal(ConsumerRegistry.DefaultQueueName(typeof(PlainConsumer)), plain.QueueName);
    }

    [Fact]
    public void Queue_type_read_mode_bindings_and_notifications_Should_come_from_attributes_then_registration()
    {
        using ServiceProvider services = Build(o =>
        {
            o.ConnectionString = Unreachable;
            o.UseInsertNotifications = true;
        }, s => s
            .AddPgmqConsumer<FifoConsumer>()
            .AddPgmqConsumer<SecondConsumer>(c =>
            {
                c.Queue = PgmqQueueOptions.Partitioned("1 day", "7 days");
                c.Filter = """{ "region": "eu" }""";
                c.TopicBindings.Add("orders.#");
                c.UseInsertNotifications = false;
            }));

        ConsumerRegistration[] consumers = [.. services.GetRequiredService<ConsumerRegistry>().Consumers];
        ConsumerRegistration fifo = consumers.Single(c => c.ConsumerType == typeof(FifoConsumer));
        ConsumerRegistration second = consumers.Single(c => c.ConsumerType == typeof(SecondConsumer));

        Assert.Equal(PgmqReadMode.GroupedRoundRobin, fifo.ReadMode);
        Assert.Equal(PgmqQueueType.Unlogged, fifo.Queue.Type);
        Assert.Equal(["orders.*.created", "audit.#"], fifo.TopicBindings);
        Assert.True(fifo.UseInsertNotifications);

        Assert.Equal(PgmqQueueType.Partitioned, second.Queue.Type);
        Assert.Equal(("1 day", "7 days"), (second.Queue.PartitionInterval, second.Queue.RetentionInterval));
        Assert.Equal(["orders.#"], second.TopicBindings);
        Assert.False(second.UseInsertNotifications);
        Assert.Equal("""{ "region": "eu" }""", second.Filter);
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("[1, 2]")]
    public void A_filter_that_is_not_a_json_object_Should_be_refused(string filter)
    {
        using ServiceProvider services = Build(o => o.ConnectionString = Unreachable,
            s => s.AddPgmqConsumer<SecondConsumer>(c => c.Filter = filter));

        Assert.Throws<InvalidOperationException>(() => services.GetRequiredService<ConsumerRegistry>());
    }

    [Fact]
    public void Settings_pgmq_cannot_combine_Should_be_refused()
    {
        using ServiceProvider popGrouped = Build(o => o.ConnectionString = Unreachable,
            s => s.AddPgmqConsumer<FifoConsumer>(c => c.AtMostOnce = true));
        using ServiceProvider filterGrouped = Build(o => o.ConnectionString = Unreachable,
            s => s.AddPgmqConsumer<FifoConsumer>(c => c.Filter = "{}"));
        using ServiceProvider filterPop = Build(o => o.ConnectionString = Unreachable,
            s => s.AddPgmqConsumer<SecondConsumer>(c =>
            {
                c.AtMostOnce = true;
                c.Filter = "{}";
            }));

        Assert.Throws<InvalidOperationException>(() => popGrouped.GetRequiredService<ConsumerRegistry>());
        Assert.Throws<InvalidOperationException>(() => filterGrouped.GetRequiredService<ConsumerRegistry>());
        Assert.Throws<InvalidOperationException>(() => filterPop.GetRequiredService<ConsumerRegistry>());
    }

    [Fact]
    public void The_queue_manager_and_publisher_Should_resolve_from_the_registrations()
    {
        using ServiceProvider services = Build(o => o.ConnectionString = Unreachable, _ => { });

        Assert.IsType<PgmqQueueManager>(services.GetRequiredService<IPgmqQueueManager>());
        Assert.IsType<PgmqPublisher>(services.GetRequiredService<IPgmqPublisher>());
    }

    [Fact]
    public void A_queue_name_formatter_Should_name_consumers_without_a_pinned_name()
    {
        using ServiceProvider services = Build(o =>
        {
            o.ConnectionString = Unreachable;
            o.QueueNameFormatter = t => "custom_" + t.Name.ToLowerInvariant();
        }, s => s.AddPgmqConsumer<PlainConsumer>().AddPgmqConsumer<FirstConsumer>());

        string[] names = [.. services.GetRequiredService<ConsumerRegistry>().Consumers.Select(c => c.QueueName)];

        Assert.Equal(["custom_plainconsumer", "first_queue"], names);
    }

    [Fact]
    public void Two_consumers_on_one_queue_Should_be_refused()
    {
        using ServiceProvider services = Build(o => o.ConnectionString = Unreachable, s => s
            .AddPgmqConsumer<FirstConsumer>()
            .AddPgmqConsumer<PlainConsumer>(c => c.QueueName = "first_queue"));

        Assert.Throws<InvalidOperationException>(() => services.GetRequiredService<ConsumerRegistry>());
    }

    [Fact]
    public void A_derived_queue_name_Should_be_valid_and_stable_whatever_the_class_name()
    {
        string name = ConsumerRegistry.DefaultQueueName(typeof(AConsumerWithAVeryLongNameThatWillNotFitIntoAPgmqQueueName));

        PgmqTopology.EnsureValidQueueName(name);
        Assert.Equal(PgmqTopology.MaxQueueNameLength, name.Length);
        Assert.Equal(name, ConsumerRegistry.DefaultQueueName(typeof(AConsumerWithAVeryLongNameThatWillNotFitIntoAPgmqQueueName)));
    }

    [Fact]
    public async Task A_setup_failure_Should_stop_the_host_by_default()
    {
        await using ServiceProvider services = Build(o => o.ConnectionString = Unreachable,
            s => s.AddPgmqConsumer<FirstConsumer>());

        await Assert.ThrowsAnyAsync<Exception>(() => StartAsync(services));
    }

    [Fact]
    public async Task A_setup_failure_when_told_not_to_fail_Should_leave_the_host_running_and_report_it()
    {
        await using ServiceProvider services = Build(o =>
        {
            o.ConnectionString = Unreachable;
            o.FailOnSetupError = false;
            o.UnavailableHealthStatus = HealthStatus.Degraded;
        }, s => s.AddPgmqConsumer<FirstConsumer>().AddHealthChecks().AddPgmq());

        await StartAsync(services);

        PgmqStatus status = services.GetRequiredService<PgmqStatus>();
        Assert.False(status.IsReady);
        Assert.NotNull(status.Failure);
        Assert.Throws<InvalidOperationException>(status.EnsureReady);

        HealthReport health = await services.GetRequiredService<HealthCheckService>().CheckHealthAsync(Ct);
        Assert.Equal(HealthStatus.Degraded, health.Status);

        foreach (IHostedService hosted in services.GetServices<IHostedService>().Reverse())
        {
            await hosted.StopAsync(Ct);
        }
    }

    private static ServiceProvider Build(Action<PgmqOptions> configure, Action<IServiceCollection> register)
    {
        ServiceCollection services = new();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Critical));
        services.AddPgmq(configure);
        register(services);
        return services.BuildServiceProvider(validateScopes: true);
    }

    private static async Task StartAsync(IServiceProvider services)
    {
        foreach (IHostedService hosted in services.GetServices<IHostedService>())
        {
            await hosted.StartAsync(Ct);
        }
    }

    public sealed record Ping;

    public sealed class NotAConsumer;

    [QueueName("first_queue")]
    [ConsumerOptions(Concurrency = 2, TimeoutSeconds = 90)]
    public sealed class FirstConsumer : IPgmqConsumer<Ping>
    {
        public Task HandleAsync(PgmqConsumeContext<Ping> context, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    [QueueName("second_queue")]
    public sealed class SecondConsumer : IPgmqConsumer<Ping>
    {
        public Task HandleAsync(PgmqConsumeContext<Ping> context, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    [QueueName("fifo_queue")]
    [ConsumerOptions(ReadMode = PgmqReadMode.GroupedRoundRobin, QueueType = PgmqQueueType.Unlogged)]
    [TopicBinding("orders.*.created")]
    [TopicBinding("audit.#")]
    public sealed class FifoConsumer : IPgmqConsumer<Ping>
    {
        public Task HandleAsync(PgmqConsumeContext<Ping> context, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    public sealed class PlainConsumer : IPgmqConsumer<Ping>
    {
        public Task HandleAsync(PgmqConsumeContext<Ping> context, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    public sealed class AConsumerWithAVeryLongNameThatWillNotFitIntoAPgmqQueueName : IPgmqConsumer<Ping>
    {
        public Task HandleAsync(PgmqConsumeContext<Ping> context, CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
