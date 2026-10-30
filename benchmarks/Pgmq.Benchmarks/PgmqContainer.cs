using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Extensions.Logging.Abstractions;

namespace Pgmq.Benchmarks;

/// <summary>
/// A throwaway PostgreSQL with the pgmq extension in Docker, with room for a 200-connection pool. Needs a running
/// Docker daemon.
/// </summary>
public sealed class PgmqContainer : IAsyncDisposable
{
    public const string Image = "ghcr.io/pgmq/pg18-pgmq:v1.13.1";
    private const string Password = "postgres";

    private readonly IContainer _container = new ContainerBuilder(Image)
        .WithLogger(NullLogger.Instance)
        .WithEnvironment("POSTGRES_PASSWORD", Password)
        .WithPortBinding(5432, true)
        .WithCommand("postgres", "-c", "max_connections=500")
        .WithWaitStrategy(Wait.ForUnixContainer().UntilCommandIsCompleted("pg_isready", "-h", "127.0.0.1", "-U", "postgres"))
        .Build();

    /// <summary>A pool of up to 200 connections, as a busy production application might allow.</summary>
    public string ConnectionString =>
        $"Host={_container.Hostname};Port={_container.GetMappedPublicPort(5432)};Username=postgres;" +
        $"Password={Password};Database=postgres;Maximum Pool Size=200";

    public Task StartAsync(CancellationToken cancellationToken = default) => _container.StartAsync(cancellationToken);

    public ValueTask DisposeAsync() => _container.DisposeAsync();
}
