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
- **Tracing.** Spans follow the OpenTelemetry messaging conventions under the `Pgmq` source. The trace continues from
  publisher to consumer through the message headers.

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
| Extension version | | ✅ | `IPgmqQueueManager.GetVersionAsync` |

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

## Configuration

| Option | Default | |
|---|---|---|
| `ConnectionString` / `DataSource` | | One of the two is required. |
| `AutoSetup` | `true` | Create the extension, queues and triggers on start. Off to only check they exist. |
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
| `UnavailableHealthStatus` / `StalledHealthStatus` | `Unhealthy` | Use `Degraded` to keep a pod in service. |

A consumer's queue, concurrency, timeout, read mode, queue type, filter and at-most-once setting come from
`AddPgmqConsumer<T>(c => ...)`, then `[QueueName]`, `[ConsumerOptions]` and `[TopicBinding]`, then these defaults. Pin queue names with `[QueueName]` and message names with `[MessageName]`
before renaming or moving a class: the names live in the database.

Add `services.AddHealthChecks().AddPgmq()` for the health check, and the `Pgmq` source to your tracer for spans.

## Limitations

- **Separate writes per queue.** Publishing to several queues is not one transaction, and there is no transactional
  outbox with your own database writes. A topic publish is one statement.
- **Archives grow** on standard and unlogged queues. Partitioned queues drop old partitions; `ConvertArchivePartitionedAsync`
  partitions an existing archive.
- **No ordering guarantees** within a queue under concurrency, except per group in the grouped read modes.
- **PGMQ 1.13 or later** for every feature here; older versions lack the FIFO, topic and notification functions.
