using Microsoft.Extensions.Diagnostics.HealthChecks;
using Npgsql;

namespace Pgmq;

/// <summary>
/// How Pgmq connects and how its worker behaves. One set per host; a consumer changes its own concurrency and timeout
/// with <see cref="ConsumerOptionsAttribute"/> or at registration. The defaults suit production; tests shorten the
/// times.
/// </summary>
public sealed class PgmqOptions
{
    /// <summary>The PostgreSQL database with the pgmq extension. Set this or <see cref="DataSource"/>.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>A data source to share with the rest of the application. Set this or <see cref="ConnectionString"/>.</summary>
    public NpgsqlDataSource? DataSource { get; set; }

    /// <summary>
    /// Run the registered consumers in this host. Turn off for a host that only publishes: it still knows every
    /// consumer's queue, so what it publishes reaches the hosts that consume.
    /// </summary>
    public bool RunConsumers { get; set; } = true;

    /// <summary>
    /// Stop the host from starting when the pgmq extension or the queues cannot be set up. Turn off to run without
    /// Pgmq instead: the failure is logged, the worker stays idle, and <see cref="PgmqStatus"/> and the health check
    /// report it.
    /// </summary>
    public bool FailOnSetupError { get; set; } = true;

    /// <summary>Messages one queue handles at once, per instance, unless its consumer says otherwise.</summary>
    public int DefaultConcurrency { get; set; } = 8;

    /// <summary>How long one message may run, unless its consumer says otherwise.</summary>
    public TimeSpan DefaultTimeout { get; set; } = TimeSpan.FromHours(1);

    /// <summary>Reads of one message before a transient failure is given up on and the message archived.</summary>
    public int MaxAttempts { get; set; } = 5;

    /// <summary>
    /// Which failures are worth another attempt. Anything else archives the message at once: a handler that charged a
    /// card should not run again because a later line threw. Extend <see cref="PgmqTransientFailures.IsTransient"/>
    /// rather than replace it.
    /// </summary>
    public Func<Exception, bool> IsTransient { get; set; } = PgmqTransientFailures.IsTransient;

    /// <summary>
    /// How long a read message stays hidden from other readers. It is renewed while the consumer runs, so this only
    /// bounds how long a message of a crashed process waits to come back.
    /// </summary>
    public TimeSpan Lease { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How often running messages' leases are renewed. A third of <see cref="Lease"/> survives one missed renewal.</summary>
    public TimeSpan RenewEvery { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>The first retry delay after a transient failure; each further one is four times longer.</summary>
    public TimeSpan RetryBackoff { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>The longest retry delay.</summary>
    public TimeSpan MaxRetryBackoff { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>How long a consumer that ignores its cancelled token is waited for before it is abandoned.</summary>
    public TimeSpan AbandonAfter { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How long running consumers may finish after shutdown starts, before their token is cancelled. Keep it below the
    /// host's shutdown timeout.
    /// </summary>
    public TimeSpan DrainGrace { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>The first poll delay on an empty queue; it doubles up to <see cref="MaxIdleDelay"/>.</summary>
    public TimeSpan MinIdleDelay { get; set; } = TimeSpan.FromMilliseconds(200);

    /// <summary>The longest poll delay on an empty queue, and so the longest wait for a new message.</summary>
    public TimeSpan MaxIdleDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Wake an idle read loop as soon as a message is inserted, through PostgreSQL LISTEN/NOTIFY
    /// (<c>pgmq.enable_notify_insert</c>), rather than at its next poll. Polling stays as the fallback, so a missed
    /// notification only costs up to <see cref="MaxIdleDelay"/>. Uses one extra connection per host. A consumer can
    /// turn it on or off for itself at registration.
    /// </summary>
    public bool UseInsertNotifications { get; set; }

    /// <summary>The shortest time between two insert notifications of one queue.</summary>
    public TimeSpan InsertNotificationThrottle { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Long-poll an empty queue inside PostgreSQL (pgmq's <c>read_with_poll</c> family) for up to this long, checking
    /// every <see cref="MinIdleDelay"/>, instead of polling from the application. Holds a connection per queue while it
    /// waits. Null, the default, polls from the application. Not used by <see cref="ConsumerOptionsAttribute.AtMostOnce"/>
    /// consumers, which pgmq cannot long-poll.
    /// </summary>
    public TimeSpan? LongPollTimeout { get; set; }

    /// <summary>A queue's read loop silent this long counts as stalled.</summary>
    public TimeSpan StallAfter { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// What the health check reports while Pgmq could not be set up. Degraded keeps a pod in service while it is
    /// reported.
    /// </summary>
    public HealthStatus UnavailableHealthStatus { get; set; } = HealthStatus.Unhealthy;

    /// <summary>
    /// What the health check reports while a read loop is stalled. Unhealthy lets a liveness probe restart the process.
    /// </summary>
    public HealthStatus StalledHealthStatus { get; set; } = HealthStatus.Unhealthy;

    /// <summary>The queue name of a consumer without <see cref="QueueNameAttribute"/>. Null uses the default derivation.</summary>
    public Func<Type, string>? QueueNameFormatter { get; set; }

    internal void Validate()
    {
        if (ConnectionString is null == (DataSource is null))
        {
            throw new InvalidOperationException($"Set exactly one of {nameof(ConnectionString)} and {nameof(DataSource)}.");
        }

        ArgumentOutOfRangeException.ThrowIfLessThan(DefaultConcurrency, 1, nameof(DefaultConcurrency));
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxAttempts, 1, nameof(MaxAttempts));
        ArgumentOutOfRangeException.ThrowIfLessThan(Lease, TimeSpan.FromSeconds(1), nameof(Lease));
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(RenewEvery, Lease, nameof(RenewEvery));
        ArgumentNullException.ThrowIfNull(IsTransient, nameof(IsTransient));
        ArgumentOutOfRangeException.ThrowIfLessThan(InsertNotificationThrottle, TimeSpan.Zero,
            nameof(InsertNotificationThrottle));

        if (LongPollTimeout is { } longPoll)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(longPoll, TimeSpan.FromSeconds(1), nameof(LongPollTimeout));
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(longPoll, StallAfter, nameof(LongPollTimeout));
        }
    }
}
