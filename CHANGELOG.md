# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0-alpha.2] - 2026-10-30

### Added

- Support for every function of the PGMQ 1.13 SQL API; the README has a table of what maps where.
- `IPgmqQueueManager` / `PgmqQueueManager`: create (standard, unlogged, partitioned), ensure, drop, purge, list and
  check queues; metrics; topic bindings and routing tests; insert notifications; FIFO indexes; archive partitioning;
  the extension version; deleting, archiving and hiding messages by id, one or many at a time.
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
