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
    /// <summary>
    /// The PostgreSQL server, credentials and database Pgmq uses. Set this or <see cref="DataSource"/>. Pgmq works in
    /// the database it names, or in <see cref="Database"/> when that is set.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// The database to use instead of the one in <see cref="ConnectionString"/>, so an application can hand over its
    /// own connection string and still keep pgmq apart, for example <c>"transport"</c>. Null uses the connection
    /// string's. Created on start when missing (<see cref="CreateDatabase"/>). Not with <see cref="DataSource"/>.
    /// </summary>
    public string? Database { get; set; }

    /// <summary>A data source to share with the rest of the application. Set this or <see cref="ConnectionString"/>.</summary>
    public NpgsqlDataSource? DataSource { get; set; }

    /// <summary>
    /// Run the registered consumers in this host. Turn off for a host that only publishes: it still knows every
    /// consumer's queue, so what it publishes reaches the hosts that consume.
    /// </summary>
    public bool RunConsumers { get; set; } = true;

    /// <summary>
    /// Set up the database on start, so it needs no migrations of its own: create the database if it is missing
    /// (<see cref="CreateDatabase"/>), install or upgrade pgmq (<see cref="InstallMode"/>; pg_partman too for
    /// partitioned queues), then create every consumer's queue, its FIFO index, topic bindings and insert notification
    /// trigger. All of it is idempotent and safe from several instances at once, so every start runs it and keeps the
    /// schema current. Turn off when the database is managed elsewhere: Pgmq then only checks that pgmq and every
    /// queue exist, and uses the insert notifications that are already there.
    /// </summary>
    public bool AutoSetup { get; set; } = true;

    /// <summary>
    /// With <see cref="AutoSetup"/>, create the database named in <see cref="ConnectionString"/> when it does not
    /// exist, connecting to <see cref="MaintenanceDatabase"/> to do so; the user then needs <c>CREATEDB</c>. Needs
    /// <see cref="ConnectionString"/>: a <see cref="DataSource"/> does not hand out its password, so with one the
    /// database must already exist.
    /// </summary>
    public bool CreateDatabase { get; set; } = true;

    /// <summary>The database <see cref="CreateDatabase"/> connects to when the target is missing.</summary>
    public string MaintenanceDatabase { get; set; } = "postgres";

    /// <summary>
    /// How <see cref="AutoSetup"/> installs pgmq: as the extension where the server has it, as the embedded SQL
    /// otherwise (<see cref="PgmqInstallMode.Auto"/>), or one of the two only.
    /// </summary>
    public PgmqInstallMode InstallMode { get; set; } = PgmqInstallMode.Auto;

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

    /// <summary>
    /// The first poll delay on an empty queue that polls from the application; it doubles up to
    /// <see cref="MaxIdleDelay"/>. Also how often a long poll checks the queue inside the database.
    /// </summary>
    public TimeSpan MinIdleDelay { get; set; } = TimeSpan.FromMilliseconds(200);

    /// <summary>
    /// The longest poll delay on an empty queue that polls from the application. A queue waiting for an insert
    /// notification reads again after this long at the latest, in case a notification was throttled or lost.
    /// </summary>
    public TimeSpan MaxIdleDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Wake an idle read loop as soon as a message is inserted, through PostgreSQL LISTEN/NOTIFY
    /// (<c>pgmq.enable_notify_insert</c>). On by default. While notifications are not live (the listener connection is
    /// down or reconnecting, or the database refused to create the trigger), the queue falls back to
    /// <see cref="LongPollTimeout"/>. Uses one extra connection per host. A consumer can turn it on or off for itself at
    /// registration.
    /// </summary>
    public bool UseInsertNotifications { get; set; } = true;

    /// <summary>The shortest time between two insert notifications of one queue.</summary>
    public TimeSpan InsertNotificationThrottle { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// The fallback when insert notifications are not live for a queue: long-poll it inside PostgreSQL (pgmq's
    /// <c>read_with_poll</c> family) for up to this long, checking every <see cref="MinIdleDelay"/>. Holds a connection
    /// per queue while it waits. Null polls from the application instead, backing off to <see cref="MaxIdleDelay"/>.
    /// Not used by <see cref="ConsumerOptionsAttribute.AtMostOnce"/> consumers, which pgmq cannot long-poll. Must be
    /// shorter than <see cref="StallAfter"/>.
    /// </summary>
    public TimeSpan? LongPollTimeout { get; set; } = TimeSpan.FromSeconds(5);

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

    /// <summary><see cref="ConnectionString"/> with <see cref="Database"/> applied; null with a data source.</summary>
    internal string? EffectiveConnectionString => ConnectionString is null || Database is null
        ? ConnectionString
        : new NpgsqlConnectionStringBuilder(ConnectionString) { Database = Database }.ConnectionString;

    internal void Validate()
    {
        if (ConnectionString is null == (DataSource is null))
        {
            throw new InvalidOperationException($"Set exactly one of {nameof(ConnectionString)} and {nameof(DataSource)}.");
        }

        if (Database is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(Database, nameof(Database));
            if (DataSource is not null)
            {
                throw new InvalidOperationException(
                    $"{nameof(Database)} needs {nameof(ConnectionString)}; a {nameof(DataSource)} names its own database.");
            }
        }

        ArgumentException.ThrowIfNullOrEmpty(MaintenanceDatabase, nameof(MaintenanceDatabase));
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
