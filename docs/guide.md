# Pgmq.NET guide

Everything the [README](../README.md) leaves out: behavior, PGMQ feature support, options and limitations.

## How it behaves

- **Fan-out.** Each consumer has its own queue. A published message is written to the queue of every consumer of its
  type, or of a type it derives from.
- **At-least-once delivery.** A message is deleted only after its consumer returns. Consumers should be idempotent.
- **Independent queues.** Every queue has its own read loop and its own slots, so a full or slow queue never holds up
  another one. A slot frees as soon as its message is settled.
- **Leases.** A read message is hidden for `Lease` (30 s), renewed while its consumer runs. A long job keeps its
  message, and a crashed process gives it back within one lease.
- **Retries.** A failure that `IsTransient` accepts is retried after a backoff (30 s, 2 m, 8 m, 30 m), up to
  `MaxAttempts`. Any other failure, a timeout, or the last attempt archives the message to `pgmq.a_{queue}`.
- **Outbox.** What a consumer publishes through its context is sent only if it succeeds, so a retry does not send twice.
- **Timeouts.** A consumer past its timeout has its token cancelled and its message archived. One that ignores the
  token is abandoned, so its slot frees.
- **Graceful shutdown.** Reading stops, running consumers get `DrainGrace` to finish, and what stops on cancellation
  goes straight back to the queue.
- **Survival.** A failed read loop restarts itself. `PgmqStatus.StalledQueues` and the health check report a loop
  that went quiet.
- **Telemetry.** Traces, metrics and logs through the .NET diagnostics APIs, following the OpenTelemetry messaging
  conventions. The trace continues from publisher to consumer through the message headers. Your application's
  OpenTelemetry setup decides what is collected and where it goes; see [Telemetry](#telemetry).

## PGMQ feature support

Every function in the [PGMQ 1.13 SQL API](https://pgmq.github.io/pgmq/latest/api/sql/functions/) and where to find
it. "Worker" means the consumer worker in `Pgmq.Hosting` uses it for you. Pgmq calls these functions itself, with
Npgsql and nothing else in between.

| PGMQ feature | Function | Supported | How |
|---|---|:---:|---|
| **Sending** | | | |
| Send a message | `send` | ✅ | `IPgmqPublisher.PublishAsync` |
| Send with headers | `send(…, headers)` | ✅ | Pgmq's own headers on every message; yours with `PgmqPublishOptions.Headers` |
| Delayed send (seconds or timestamp) | `send(…, delay)` | ✅ | `SchedulePublishAsync`, or `PgmqPublishOptions.Delay` / `DeliverAt` |
| Send a batch | `send_batch` | ✅ | `PublishBatchAsync`, one write per queue |
| **Topic routing** | | | |
| Bind a pattern to a queue | `bind_topic` | ✅ | `[TopicBinding("orders.*")]` on a consumer, or `IPgmqQueueManager.BindTopicAsync` |
| Unbind a pattern | `unbind_topic` | ✅ | `IPgmqQueueManager.UnbindTopicAsync` |
| List bindings | `list_topic_bindings` | ✅ | `IPgmqQueueManager.ListTopicBindingsAsync` |
| Test a routing key | `test_routing` | ✅ | `IPgmqQueueManager.TestRoutingAsync` |
| Send to a topic | `send_topic` | ✅ | `IPgmqPublisher.PublishToTopicAsync` |
| Send a batch to a topic | `send_batch_topic` | ✅ | `IPgmqPublisher.PublishBatchToTopicAsync` |
| **Reading** | | | |
| Read with a visibility timeout | `read` | ✅ | Worker, renewing the lease while the consumer runs |
| Conditional read (experimental) | `read(…, conditional)` | ✅ | `[ConsumerOptions(Filter = """{"region":"eu"}""")]` |
| Long poll | `read_with_poll` | ✅ | Worker, while notifications are not live (`PgmqOptions.LongPollTimeout`) |
| FIFO, fill from the oldest group | `read_grouped`, `read_grouped_with_poll` | ✅ | `ReadMode = PgmqReadMode.Grouped`, group with `PgmqPublishOptions.GroupId` |
| FIFO, round robin across groups | `read_grouped_rr`, `read_grouped_rr_with_poll` | ✅ | `ReadMode = PgmqReadMode.GroupedRoundRobin` |
| FIFO, head of each group | `read_grouped_head`, `read_grouped_head_with_poll` | ✅ | `ReadMode = PgmqReadMode.GroupedHead` |
| Read and delete (at most once) | `pop` | ✅ | `[ConsumerOptions(AtMostOnce = true)]` |
| **Deleting and archiving** | | | |
| Delete a message | `delete` | ✅ | Worker, when a consumer succeeds; or `IPgmqQueueManager.DeleteMessageAsync` |
| Delete a batch | `delete(…, msg_ids[])` | ✅ | `IPgmqQueueManager.DeleteMessagesAsync` |
| Archive a message | `archive` | ✅ | Worker, on a permanent failure, timeout or the last attempt; or `IPgmqQueueManager.ArchiveMessageAsync` |
| Archive a batch | `archive(…, msg_ids[])` | ✅ | `IPgmqQueueManager.ArchiveMessagesAsync` |
| Purge a queue | `purge_queue` | ✅ | `IPgmqQueueManager.PurgeQueueAsync` |
| **Queue management** | | | |
| Create a queue | `create`, `create_non_partitioned` | ✅ | Worker setup for every consumer, or `IPgmqQueueManager.CreateQueueAsync` / `EnsureQueueAsync` |
| Create an unlogged queue | `create_unlogged` | ✅ | `[ConsumerOptions(QueueType = PgmqQueueType.Unlogged)]`, or `PgmqQueueOptions.Unlogged` |
| Create a partitioned queue (pg_partman) | `create_partitioned` | ✅ | `QueueType = PgmqQueueType.Partitioned`, or `PgmqQueueOptions.Partitioned(...)` |
| Partition an archive (pg_partman) | `convert_archive_partitioned` | ✅ | `IPgmqQueueManager.ConvertArchivePartitionedAsync` |
| Drop a queue | `drop_queue` | ✅ | `IPgmqQueueManager.DropQueueAsync` |
| Detach an archive | `detach_archive` | ➖ | A deprecated no-op in PGMQ, so left out |
| **Utilities** | | | |
| Set the visibility timeout | `set_vt` (one or many) | ✅ | Worker for leases, retries and releases; or `IPgmqQueueManager.SetVisibilityTimeoutAsync` |
| List queues | `list_queues` | ✅ | `IPgmqQueueManager.ListQueuesAsync` |
| Metrics for a queue / all queues | `metrics`, `metrics_all` | ✅ | `IPgmqQueueManager.GetMetricsAsync` / `GetAllMetricsAsync`, with `DefaultPartitionLength` to spot failing pg_partman maintenance |
| Lock a queue for a transaction | `acquire_queue_lock` | ✅ | `EnsureQueueAsync` uses it; or `IPgmqQueueManager.AcquireQueueLockAsync(queue, transaction)` |
| Notify on insert (LISTEN/NOTIFY) | `enable_notify_insert` | ✅ | Worker, on by default (`PgmqOptions.UseInsertNotifications`); or `IPgmqQueueManager.EnableInsertNotificationsAsync` |
| Change or stop notifications | `update_notify_insert`, `disable_notify_insert` | ✅ | `IPgmqQueueManager.UpdateInsertNotificationsAsync` / `DisableInsertNotificationsAsync` |
| List notification throttles | `list_notify_insert_throttles` | ✅ | `IPgmqQueueManager.ListInsertNotificationsAsync` |
| FIFO indexes | `create_fifo_index`, `create_fifo_indexes_all` | ✅ | Worker setup for grouped consumers, or `IPgmqQueueManager.CreateFifoIndexAsync` / `CreateFifoIndexesAsync` |
| Extension version | | ✅ | `IPgmqQueueManager.GetVersionAsync` (extension or SQL install) |

✅ supported · ➖ not needed

## Using PGMQ features

### FIFO groups

Messages of one group are handled one at a time, in the order they were published. Different groups run in
parallel, up to the consumer's concurrency. When a message fails and is retried, the rest of its group waits for it.

```csharp
[ConsumerOptions(ReadMode = PgmqReadMode.Grouped, Concurrency = 8)]
public sealed class LedgerHandler : IPgmqConsumer<LedgerEntry> { ... }

await publisher.PublishAsync(entry, new PgmqPublishOptions { GroupId = entry.AccountId.ToString() });
```

### Topics

Topic routing works next to the type-based fan-out. A consumer binds patterns to its queue (`*` is one word, `#` is
any number), and a publish to a routing key reaches every queue whose pattern matches.

```csharp
[TopicBinding("orders.*.created")]
public sealed class OrderAuditHandler : IPgmqConsumer<OrderCreated> { ... }

int queues = await publisher.PublishToTopicAsync("orders.eu.created", new OrderCreated(orderId));
```

### Queue types, filters and at-most-once

```csharp
// Alternatives: a consumer has one [ConsumerOptions], and these settings combine in it.
[ConsumerOptions(QueueType = PgmqQueueType.Unlogged)]          // fast, but lost on a crash
[ConsumerOptions(QueueType = PgmqQueueType.Partitioned,        // needs pg_partman; old partitions are dropped
                 PartitionInterval = "1 day", RetentionInterval = "7 days")]
[ConsumerOptions(Filter = """{"region":"eu"}""")]              // reads only bodies that contain this JSON
[ConsumerOptions(AtMostOnce = true)]                           // pgmq.pop: no retries, a failure loses the message
```

A queue gets its type when it is created. An existing queue keeps its type, and a warning is logged if the consumer
asks for a different one. Filters match the JSON body as it is serialized, with camelCase property names.

### Waking up: notifications and long polling

By default each queue sends a PostgreSQL notification on insert (LISTEN/NOTIFY), which wakes the idle consumer at
once over one extra connection per host. A consumer still reads again after `MaxIdleDelay` in case a notification was
throttled. While notifications are not live (the listener is reconnecting, or the database refused the trigger), the
queue long-polls in the database instead (`read_with_poll`, up to `LongPollTimeout`). Set `LongPollTimeout = null` to
poll from the application instead, or `UseInsertNotifications = false` to rely on long polling alone.

### Headers

`PgmqPublishOptions.Headers` travel with the message and reach the consumer as `context.Headers`, next to
`context.GroupId`. Redelivered messages keep both.

### Queue management

`IPgmqQueueManager` resolves from dependency injection, or create a `new PgmqQueueManager(dataSource)` without hosting:

```csharp
PgmqQueueMetrics metrics = await queues.GetMetricsAsync("order_created");
await queues.PurgeQueueAsync("order_created");
await queues.BindTopicAsync("logs.#", "log_sink");
```

## Database setup

With `AutoSetup` on (the default), every start runs the same steps. Each is idempotent, so a start that finds
everything current changes nothing:

1. **Database.** If the database in `ConnectionString` does not exist, it is created through `MaintenanceDatabase`.
   Two instances racing to create it both succeed.
2. **pgmq.** Under an advisory lock, in one transaction (`PgmqMigrator`):
   - Installed as an extension: `alter extension pgmq update` when the server has a newer version.
   - Installed as SQL: the embedded upgrade scripts from the recorded version on, each recorded in
     `pgmq.__pgmq_migrations`.
   - Not installed: `create extension pgmq` where the server has it, otherwise the embedded `pgmq.sql`
     (`InstallMode` narrows this).
   - A `pgmq` schema with no version record is refused rather than guessed at.
3. **Queues.** Each consumer's queue, FIFO index, topic bindings and insert notification trigger.

The SQL install follows what upstream calls a versioned SQL-only install. It uses the same table and lock key as the
Rust client, so a database installed by either one can be upgraded by both. The embedded scripts are pgmq's own, at
the version in `PgmqMigrator.EmbeddedVersion`. `PgmqMigrator` can also be used without the host: call
`EnsureDatabaseAsync` and `MigrateAsync` from a deploy step.

## Configuration

| Option | Default | |
|---|---|---|
| `ConnectionString` / `DataSource` | | One of the two is required. |
| `Database` | | The database to use instead of the connection string's, e.g. `"transport"`. Not with `DataSource`. |
| `AutoSetup` | `true` | Create the database, install or upgrade pgmq, and create queues and triggers on start. Off to only check they exist. |
| `CreateDatabase` | `true` | With `AutoSetup`, create the database in `ConnectionString` when missing (needs `CREATEDB`). Not with `DataSource`. |
| `MaintenanceDatabase` | `"postgres"` | Where `CreateDatabase` connects to create it. |
| `InstallMode` | `Auto` | `Auto`: the extension where available, the embedded SQL otherwise. `Extension` or `Sql` for one only. |
| `RunConsumers` | `true` | Off for a host that only publishes. |
| `FailOnSetupError` | `true` | Off to start without Pgmq when the database lacks it; the health check reports it. |
| `DefaultConcurrency` | `8` | Messages per queue at once, per instance. |
| `DefaultTimeout` | 1 hour | Per message. |
| `MaxAttempts` | `5` | |
| `IsTransient` | `PgmqTransientFailures.IsTransient` | Which failures are retried. |
| `Lease` / `RenewEvery` | 30 s / 10 s | |
| `RetryBackoff` / `MaxRetryBackoff` | 30 s / 30 min | |
| `DrainGrace` / `AbandonAfter` | 20 s / 30 s | |
| `MinIdleDelay` / `MaxIdleDelay` | 200 ms / 5 s | Polling of an empty queue; `MaxIdleDelay` is also the safety re-read while waiting for a notification. |
| `UseInsertNotifications` / `InsertNotificationThrottle` | `true` / 250 ms | Wake idle queues through LISTEN/NOTIFY. |
| `LongPollTimeout` | 5 s | Long-poll empty queues in the database while notifications are not live. `null` polls from the application. |
| `QueueMetricsInterval` | `null` | How often to read queue depth and oldest message age for the `pgmq.queue.*` gauges. Off by default. |
| `UnavailableHealthStatus` / `StalledHealthStatus` | `Unhealthy` | Use `Degraded` to keep a pod in service. |

A consumer's queue, concurrency, timeout, read mode, queue type, filter and at-most-once setting come from
`AddPgmqConsumer<T>(c => ...)`, then `[QueueName]`, `[ConsumerOptions]` and `[TopicBinding]`, then these defaults. Pin queue names with `[QueueName]` and message names with `[MessageName]`
before renaming or moving a class: the names live in the database.

Add `services.AddHealthChecks().AddPgmq()` for the health check.

## Telemetry

Pgmq emits traces and metrics through `System.Diagnostics` (`ActivitySource` and `Meter`, both named `Pgmq`) and logs
through `ILogger`. It references no OpenTelemetry package and exports nothing. Your application's OpenTelemetry setup
decides what is collected, how it is sampled and where it goes. Until the application listens, no spans are created
and no measurements are taken.

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource(PgmqTelemetry.SourceName))
    .WithMetrics(m => m.AddMeter(PgmqTelemetry.MeterName))
    .UseOtlpExporter();
```

### Spans

| Span | Kind | When |
|---|---|---|
| `send {destination}` | Producer | Each publish. `{destination}` is the queue, the routing key, or, for a fan-out, the message type; a fan-out lists the queues it reached in `messaging.pgmq.queues`. |
| `process {queue}` | Consumer | Handling one message, as a child of the `send` span that wrote it. Your consumer's own spans and logs are inside it. |
| `delete` / `archive` / `set_vt {queue}` | Client | Writing the outcome back to the queue, as a child of `process`. |

Spans carry the standard `messaging.*` attributes (`system`, `operation.type`, `operation.name`, `destination.name`,
`consumer.group.name` (the consumer class), `message.type`, `message.id`), `server.address` and `server.port`, plus
`messaging.pgmq.msg_id`, `messaging.pgmq.attempt` and `messaging.pgmq.outcome`. A failure sets the span's status to
error, sets `error.type`, and records the exception.

The trace context travels in the `traceparent` and `tracestate` headers of each message. When the application does
not listen to `Pgmq`, the headers carry the application's own current span instead, such as the incoming HTTP request,
so a trace still continues into the consumer.

### Metrics

| Instrument | Type | Unit | Attributes |
|---|---|---|---|
| `messaging.client.sent.messages` | Counter | `{message}` | queue (or routing key) |
| `messaging.client.operation.duration` | Histogram | `s` | `send` per publish; `delete` / `archive` / `set_vt` per settle; `error.type` on failure |
| `messaging.client.consumed.messages` | Counter | `{message}` | queue, consumer, `read` or `pop` |
| `messaging.process.duration` | Histogram | `s` | queue, consumer, `messaging.pgmq.outcome`, `error.type` on failure |
| `pgmq.consumer.messages.active` | UpDownCounter | `{message}` | queue, consumer |
| `pgmq.consumer.read.failures` | Counter | `{failure}` | queue, consumer, `error.type` |
| `pgmq.consumer.read_loop.restarts` | Counter | `{restart}` | queue, consumer |
| `pgmq.consumer.lease_renewal.failures` | Counter | `{failure}` | queue, consumer, `error.type` |
| `pgmq.consumer.settle.failures` | Counter | `{failure}` | queue, consumer, operation, `error.type` |
| `pgmq.queue.messages` | Gauge | `{message}` | queue; needs `QueueMetricsInterval` |
| `pgmq.queue.messages.visible` | Gauge | `{message}` | queue; needs `QueueMetricsInterval` |
| `pgmq.queue.oldest_message.age` | Gauge | `s` | queue; needs `QueueMetricsInterval` |

`messaging.pgmq.outcome` is what happened to a handled message: `success`, `retry`, `archived`, `released` (shutdown;
it goes back to the queue) or `lost` (an at-most-once message that did not succeed). `error.type` says why: the
exception's type, `timeout`, `pgmq.abandoned`, `pgmq.shutdown` or `pgmq.unknown_message_type`. Histograms suggest the
bucket boundaries the OpenTelemetry messaging conventions recommend; your views can override them.

The queue gauges come from `pgmq.metrics_all`, which counts the rows of every queue in the database. That query costs
more on large queues, and every instance runs it, so the gauges are off until `QueueMetricsInterval` is set. Even then
they query only while the application collects them.

### Logs

Logs go through `ILogger` with the categories `Pgmq.*`, so your log levels, filters and providers apply. While a
message is handled, its log scope holds `Queue`, `PgmqMsgId`, `MessageType`, `MessageId` and `Attempt`, and the
`process` span is current, so the logs carry its trace and span ids.

### Npgsql spans

If you also trace Npgsql (`AddNpgsql()`), the data source Pgmq builds from `ConnectionString` traces a command only
inside a trace that is already running, such as a publish or a handled message. The worker's polls and lease renewals
run outside any trace, many times a second, and would otherwise each become a trace of their own. A data source you
pass in through `DataSource` is yours to configure; to get the same behavior:

```csharp
NpgsqlDataSourceBuilder builder = new(connectionString);
builder.ConfigureTracing(t => t
    .ConfigureCommandFilter(_ => Activity.Current is not null)
    .ConfigureBatchFilter(_ => Activity.Current is not null));
```

## Limitations

- **Separate writes per queue.** Publishing to several queues is not one transaction, and there is no transactional
  outbox with your own database writes. A topic publish is one statement.
- **Archives grow** on standard and unlogged queues. Partitioned queues drop old partitions; `ConvertArchivePartitionedAsync`
  partitions an existing archive.
- **No ordering guarantees** within a queue under concurrency, except per group in the grouped read modes.
- **PGMQ 1.13 or later** for every feature here; older versions lack the FIFO, topic and notification functions.
