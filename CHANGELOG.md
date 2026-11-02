# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- A shovel: `IPgmqQueueManager.ShovelAsync` moves messages from a queue, or from its archive, onto another queue in
  one statement, keeping their bodies and headers. Replay what failed with
  `ShovelAsync("orders", "orders", fromArchive: true)`. It runs `pgmq_dotnet.shovel`, Pgmq.NET's first routine of its
  own, which the migrator installs after pgmq in the new `pgmq_dotnet` schema and records in
  `pgmq_dotnet.__migrations`; pgmq's own schema is left exactly as upstream ships it.

## [2.0.0] - 2026-11-02

### Changed

- **Breaking:** `PgmqOptions.DefaultConcurrency` defaults to 1, down from 8. A queue handles one message at a time
  per instance unless its consumer sets `[ConsumerOptions(Concurrency = n)]`, so a handler never runs alongside itself
  without asking to, and load no longer grows eight-fold with the number of queues. Change it for every queue at
  setup, for example `AddPgmq(o => o.DefaultConcurrency = Environment.ProcessorCount)`.
- What a consumer publishes through its context now goes out in one transaction with the delete of its message. Its
  publishes and its "done" commit together or not at all, so a send that fails partway, or a crash between the sends
  and the delete, no longer sends part of the outbox or sends it twice. A failed commit is retried as a whole. An
  at-most-once consumer's publishes are one transaction too.
- A consumer whose message is no longer on the queue when it finishes (another read finished it after its lease
  lapsed, or the queue was purged) has its publishes rolled back, with the new outcome `gone`.
- An at-most-once consumer whose publishes cannot be sent reports `lost` instead of `archived`.

### Removed

- **Breaking:** consumer retries. `PgmqOptions.IsTransient`, `PgmqOptions.RetryBackoff`,
  `PgmqOptions.MaxRetryBackoff` and the public `PgmqTransientFailures` class are gone, along with the `retry`
  outcome. A consumer that throws or times out is archived at once and never run again; retry infrastructure calls
  inside the consumer instead. `MaxAttempts` stays, as the guard against a message whose consumer keeps taking the
  process down. The library still retries its own queue writes (delete, archive, outbox commit) in place, which never
  runs a consumer.

## [1.0.0] - 2026-11-02

### Changed

- **Breaking:** consumers are no longer retried by default. `PgmqOptions.IsTransient` defaults to the new
  `PgmqTransientFailures.Never`, so a consumer that throws has its message archived at once instead of running again,
  which is unsafe for a consumer that is not idempotent. Opt back in with
  `options.IsTransient = PgmqTransientFailures.IsTransient`. A failed outbox flush is still retried in place, which
  runs no consumer.
- The README says what the default retry policy is and why.
- The README opens by saying Pgmq.NET follows pgmq one to one and adds workers on top. Its "Where it lives" section
  is removed.
- The Web API example runs on the library's defaults: `PaymentHandler` retries its flaky gateway call itself instead
  of having Pgmq run the whole message again.

### Removed

- `examples/Pgmq.Examples.Worker`. The Web API example publishes and consumes in one process and covers everything it
  showed.

### Added

- `examples/Pgmq.Examples.WebApi`: a Web API to try every feature from HTTP requests, with its own Postgres in Docker.
  Its traces, metrics and logs go to an OTLP collector when `OTEL_EXPORTER_OTLP_ENDPOINT` is set (launch profile
  `otlp`).

### Fixed

- A topic publish with an invalid routing key (a space, a wildcard, an empty word) throws `ArgumentException` before
  sending, with pgmq's own rules. Inside a consumer it used to fail only when the outbox flushed, after the consumer
  succeeded, so the consumer ran again until `MaxAttempts` and resent its other publishes each time.
- A consumer whose outbox fails to flush for a reason `IsTransient` rejects is archived at once, instead of running
  again and resending what already went out.

## [0.1.0-beta.2] - 2026-11-01

### Added

- Direct queue access on `IPgmqQueueManager`, as raw JSON: `SendAsync` and `SendBatchAsync` (`pgmq.send`,
  `pgmq.send_batch`), `ReadAsync` with `PgmqReadOptions` for the grouped modes, a filter and a long poll (`pgmq.read`,
  the grouped reads and their `_with_poll` forms), and `PopAsync` (`pgmq.pop`). Messages come back as `PgmqMessage`.

### Fixed

- A message whose consumer keeps taking the process down is no longer read forever: once it was read more than
  `MaxAttempts` times without finishing, the worker archives it without running it (`error.type`
  `pgmq.attempts_exhausted`).

## [0.1.0-beta.1] - 2026-10-31

### Added

- Metrics on the `Pgmq` meter (`PgmqTelemetry.MeterName`), following the OpenTelemetry messaging conventions:
  `messaging.client.sent.messages`, `messaging.client.operation.duration`, `messaging.client.consumed.messages` and
  `messaging.process.duration` (tagged with the outcome), plus worker health counters (`pgmq.consumer.*`).
- Queue depth and oldest message age as `pgmq.queue.*` gauges, off until `PgmqOptions.QueueMetricsInterval` is set.
- Spans for settling a handled message (`delete`, `archive`, `set_vt`), as children of its `process` span.
- Spans carry `error.type`, `messaging.operation.name`, `messaging.consumer.group.name` (the consumer class),
  `server.address` and `server.port`. A failed publish now marks its span as failed.
- A message of unknown type gets a `process` span too.
- The data source Pgmq builds traces Npgsql commands only inside a running trace, so the worker's polls no longer
  start a trace each when the application traces Npgsql.

### Changed

- Span names follow `{operation} {destination}`: `process {queue}` instead of `process {message type}`. A fan-out
  publish is `send {message type}` and lists its queues in `messaging.pgmq.queues`.
- The outcome tag is `messaging.pgmq.outcome` instead of `messaging.outcome`, with the values `success`, `retry`,
  `archived`, `released` and `lost`. Why a message failed is in `error.type` (`timeout`, `pgmq.abandoned`, ...).
- The `Pgmq` activity source has the package version.

## [0.1.0-alpha.4] - 2026-10-30

### Added

- `PgmqOptions.Database`: the database to use instead of the one in `ConnectionString`, so an application can pass
  its own connection string and keep pgmq in a database of its own. It is created on start when missing.
- README: which database and schema Pgmq uses, and how to override them. The `pgmq` schema is fixed by pgmq.

## [0.1.0-alpha.3] - 2026-10-30

### Added

- Pgmq sets up its own database, so applications need no migrations. On start, `AutoSetup` creates the database when
  it is missing (`CreateDatabase`, `MaintenanceDatabase`), installs pgmq, and upgrades it on every later start. All of
  it is idempotent and serialized across instances.
- pgmq without the extension: where the server lacks it (managed services), pgmq is installed from its own SQL,
  embedded in the package (1.13.1), and upgraded with the upstream upgrade scripts. The version is tracked in
  `pgmq.__pgmq_migrations`, compatible with the Rust client. `PgmqOptions.InstallMode` picks `Auto`, `Extension` or
  `Sql`.
- `PgmqMigrator` for the same steps outside the host: `EnsureDatabaseAsync`, `MigrateAsync`,
  `GetInstalledVersionAsync`.

### Changed

- `IPgmqQueueManager.InitAsync` installs or upgrades pgmq through `PgmqMigrator` instead of only running
  `create extension if not exists pgmq`. `GetVersionAsync` also reports a SQL install.

## [0.1.0-alpha.2] - 2026-10-30

### Added

- Support for every function of the PGMQ 1.13 SQL API; the README has a table of what maps where.
- `IPgmqQueueManager` / `PgmqQueueManager`: create (standard, unlogged, partitioned), ensure, drop, purge, list and
  check queues; metrics; topic bindings and routing tests; insert notifications; FIFO indexes; archive partitioning;
  the extension version; deleting, archiving and hiding messages by id, one or many at a time;
  `AcquireQueueLockAsync` (`pgmq.acquire_queue_lock`) to lock a queue for a transaction.
- `PgmqQueueMetrics.DefaultPartitionLength`: how many messages of a partitioned queue sit in its default partition,
  which means pg_partman maintenance is failing.
- `PgmqPublishOptions` with a delay or delivery time, a FIFO `GroupId` and custom `Headers`, and `PublishToTopicAsync`
  / `PublishBatchToTopicAsync` for topic routing.
- Consumers: `ReadMode` (`Grouped`, `GroupedRoundRobin`, `GroupedHead` FIFO reads, in order per group),
  `QueueType` with partition settings, `AtMostOnce` (`pgmq.pop`), `Filter` (conditional read) and `[TopicBinding]`.
  `context.GroupId` and `context.Headers` reach the consumer and survive a redelivery.
- `PgmqOptions.UseInsertNotifications` wakes idle queues through LISTEN/NOTIFY, on by default.
  `PgmqOptions.LongPollTimeout` (5 s by default) long-polls in the database whenever notifications are not live.
- `PgmqOptions.AutoSetup` (on by default) creates the extension, queues and triggers on start; off only checks that
  they exist.

### Changed

- The README is a short introduction; the details moved to `docs/guide.md`.
- The publisher, the worker and the queue manager share one `NpgsqlDataSource`.
- Pgmq calls pgmq's SQL functions directly and no longer depends on Npgmq. `PgmqPublisher` takes an
  `NpgsqlDataSource`.

### Removed

- `INpgmqClient` is no longer registered. Its message operations are on `IPgmqQueueManager`: `DeleteMessageAsync`,
  `DeleteMessagesAsync`, `ArchiveMessageAsync`, `ArchiveMessagesAsync` and `SetVisibilityTimeoutAsync`.

## [0.1.0-alpha.1] - 2026-10-30

### Added

- Repository scaffolding: solution, central package management, CI, documentation, and a manually run workflow
  that publishes the packages to NuGet.org.
- `Pgmq`: `IPgmqPublisher` with fan-out to subscribed queues, delayed publishing and batches; `PgmqTopology`;
  `[MessageName]`; message headers with W3C trace context; the `Pgmq` activity source.
- `Pgmq.Hosting`: `AddPgmq`, `AddPgmqConsumer<T>` and `AddPgmqConsumers`; `IPgmqConsumer<T>`; a worker with
  independent per-queue concurrency, renewed leases, retries with backoff for transient failures, archiving, timeouts,
  an outbox for publishes from consumers, graceful shutdown and self-restarting read loops; `[QueueName]` and
  `[ConsumerOptions]`; `PgmqStatus` and a health check.
- Integration tests against PGMQ in Docker, including 100,000-message load tests, and a throughput benchmark.
