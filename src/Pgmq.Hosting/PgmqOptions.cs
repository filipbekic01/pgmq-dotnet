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

    /// <summary>
    /// Messages one queue handles at once, per instance, unless its consumer sets
    /// <see cref="ConsumerOptionsAttribute.Concurrency"/>. One by default: a handler never runs alongside itself unless
    /// it asks to, and load stays one message per queue however many queues there are. Change it for every queue at
    /// setup, for example to one per processor:
    /// <code>
    /// builder.Services.AddPgmq(o => o.DefaultConcurrency = Environment.ProcessorCount);
    /// </code>
    /// </summary>
    public int DefaultConcurrency { get; set; } = 1;

    /// <summary>How long one message may run, unless its consumer says otherwise.</summary>
    public TimeSpan DefaultTimeout { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// Reads of one message before it is archived without running. A consumer that throws is archived at once and never
    /// run again, so a message is read more than once only when an earlier read never finished: the process crashed or
    /// shut down while it ran. One read more than this many times (its consumer keeps taking the process down) is
    /// archived without running. Keep it above 1, so a message that went back to the queue on shutdown still runs. A
    /// message that was read but never handed to its consumer (it waited behind another of its FIFO group, or the host
    /// was stopping) goes back without that read counted.
    /// </summary>
    public int MaxAttempts { get; set; } = 5;

    /// <summary>
    /// How long a read message stays hidden from other readers. It is renewed while the consumer runs, so this only
    /// bounds how long a message of a crashed process waits to come back.
    /// </summary>
    public TimeSpan Lease { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How often running messages' leases are renewed. A third of <see cref="Lease"/> survives one missed renewal.</summary>
    public TimeSpan RenewEvery { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>How long a consumer that ignores its cancelled token is waited for before it is abandoned.</summary>
    public TimeSpan AbandonAfter { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How long running consumers may finish after shutdown starts, before their token is cancelled. Keep it below the
    /// host's shutdown timeout, so a cancelled consumer has time to put its message back. If the host's timeout runs out
    /// first, consumers are cancelled then.
    /// </summary>
    public TimeSpan DrainGrace { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>
    /// How often the sweeper looks for work while some queue's insert notifications are not live (the listener is
    /// down or reconnecting, notifications are off, or the database refused the trigger): first after this long, then
    /// doubling up to about 1 s (never above <see cref="MaxIdleDelay"/>). Latency stays near a second on one
    /// connection, however many queues there are.
    /// </summary>
    public TimeSpan MinIdleDelay { get; set; } = TimeSpan.FromMilliseconds(200);

    /// <summary>
    /// How often the sweeper looks for work while insert notifications are live. An idle queue never reads on its
    /// own: one sweeper asks, in one query for every queue, which have a visible message, and wakes only those. This
    /// catches what notifications never report: a delayed message whose time came, a released message, a message back
    /// from a lapsed lease, a lost or throttled notification. A delayed message is picked up within this long of its
    /// due time.
    /// </summary>
    public TimeSpan MaxIdleDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Reads, across every queue, that run at once in this host. A busy queue holds a slot only for its read, not
    /// while its messages run, so busy queues take turns and idle ones never ask. Settles, outbox commits and lease
    /// renewals do not count against it. A host then uses at most this many connections for reads, one for the
    /// listener, one for the sweeper, one for lease renewals while messages run, and one per message settling at that
    /// moment, whatever the pool allows.
    /// <para>
    /// One by default: the fewest connections. Each read is one round trip, so with many busy queues of low concurrency
    /// (each read then fetches only a message or two) this caps how fast the host reads. Raise it for throughput:
    /// </para>
    /// <code>
    /// builder.Services.AddPgmq(o => o.MaxConcurrentReads = 16);
    /// </code>
    /// </summary>
    public int MaxConcurrentReads { get; set; } = 1;

    /// <summary>
    /// Wake an idle read loop as soon as a message is inserted, through PostgreSQL LISTEN/NOTIFY
    /// (<c>pgmq.enable_notify_insert</c>). On by default. Uses one extra connection per host, opened outside the pool
    /// so it never takes a pool slot. While notifications are not live (the listener is down or reconnecting, or the
    /// database refused the trigger), the sweeper looks for work every <see cref="MinIdleDelay"/> to about 1 s instead
    /// of every <see cref="MaxIdleDelay"/>. A consumer can turn it on or off for itself at registration.
    /// </summary>
    public bool UseInsertNotifications { get; set; } = true;

    /// <summary>
    /// The shortest time between two insert notifications of one queue. pgmq sends none for an insert inside that
    /// time, so the worker sweeps once when it ends, and a message inserted just after another is still found then.
    /// </summary>
    public TimeSpan InsertNotificationThrottle { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Ignored. Queues no longer long-poll, which held a connection per queue: the sweeper finds work for every queue
    /// on one connection (see <see cref="MinIdleDelay"/>). Kept so that code setting it still compiles.
    /// </summary>
    [Obsolete("Ignored: idle queues no longer long-poll. The sweeper finds work for every queue on one connection; " +
              "see MinIdleDelay and MaxIdleDelay. This property will be removed in 3.0.")]
    public TimeSpan? LongPollTimeout { get; set; }

    /// <summary>
    /// How often to read every queue's depth and oldest message age (<c>pgmq.metrics_all</c>) for the
    /// <c>pgmq.queue.*</c> gauges of the <see cref="PgmqTelemetry.MeterName"/> meter. Off (null) by default: the query
    /// counts each queue's rows, which costs on large queues, and every instance runs it. Turn it on in one host, or in
    /// all of them and read the gauges as a maximum. Even when on, it queries only while the application collects the
    /// gauges.
    /// </summary>
    public TimeSpan? QueueMetricsInterval { get; set; }

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

    /// <summary>
    /// How often a waiting read loop reports that it is alive: often enough that a quiet queue is never taken for a
    /// stalled one.
    /// </summary>
    internal TimeSpan Heartbeat => TimeSpan.FromTicks(Math.Clamp(StallAfter.Ticks / 4,
        TimeSpan.FromMilliseconds(10).Ticks, TimeSpan.FromSeconds(10).Ticks));

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
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(RenewEvery, TimeSpan.Zero, nameof(RenewEvery));
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(RenewEvery, Lease, nameof(RenewEvery));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(DefaultTimeout, TimeSpan.Zero, nameof(DefaultTimeout));
        ArgumentOutOfRangeException.ThrowIfLessThan(AbandonAfter, TimeSpan.Zero, nameof(AbandonAfter));
        if (DrainGrace != Timeout.InfiniteTimeSpan)
        {
            // Infinite never cancels running consumers: they are left to the host's shutdown timeout.
            ArgumentOutOfRangeException.ThrowIfLessThan(DrainGrace, TimeSpan.Zero, nameof(DrainGrace));
        }
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(MaxIdleDelay, TimeSpan.Zero, nameof(MaxIdleDelay));
        ArgumentOutOfRangeException.ThrowIfLessThan(InsertNotificationThrottle, TimeSpan.Zero,
            nameof(InsertNotificationThrottle));

        if (QueueMetricsInterval is { } metricsInterval)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(metricsInterval, TimeSpan.FromSeconds(1),
                nameof(QueueMetricsInterval));
        }

        ArgumentOutOfRangeException.ThrowIfLessThan(MaxConcurrentReads, 1, nameof(MaxConcurrentReads));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(MinIdleDelay, TimeSpan.Zero, nameof(MinIdleDelay));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(StallAfter, TimeSpan.Zero, nameof(StallAfter));
    }
}
