# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

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
