# Architecture

## Principles

- **PostgreSQL is the source of truth.** Messages live in PGMQ tables beside the application's own data. There is no
  broker to run, and a message survives anything the database survives.
- **PGMQ does storage and delivery.** Pgmq.NET adds typing, routing, and a worker; it does not reimplement queueing.
- **At-least-once delivery, idempotent handlers.** A message is deleted only after its consumer returns, so a crash at
  the wrong moment can deliver it again. Handlers must tolerate that.
- **Bounded concurrency.** Every queue handles at most its concurrency at once per instance, and reads no more than
  it has free slots for, so a burst waits in PostgreSQL rather than in memory.
- **Minimal surface.** Publish and subscribe. No sagas, workflow engines, or middleware pipelines.

## Core and hosting

`Pgmq` (core) owns everything a publisher needs:

- `IPgmqPublisher` / `PgmqPublisher`: serialize a message and write it, with headers, to every queue its type is
  subscribed to.
- `PgmqTopology`: subscriptions from message types to queue names, and resolving a message type from the name in its
  headers. Only subscribed types and types deriving from them resolve, so a value read from the database never picks
  an arbitrary type to deserialize.
- Message headers: message type, message id, redelivery count, and the W3C trace context.
- `PgmqTelemetry`: the `Pgmq` activity source.
- `IPgmqQueueManager` / `PgmqQueueManager`: the rest of the pgmq API (queue types, metrics, topics, notifications,
  FIFO indexes), called as SQL over an `NpgsqlDataSource`.

`Pgmq.Hosting` owns everything that runs in a host:

- `AddPgmq`, `AddPgmqConsumer<T>`, `AddPgmqConsumers(assembly)`: registration. The consumers make the topology, so a
  publish-only host that registers the same consumers (`RunConsumers = false`) routes the same way without consuming.
- Queue setup at start: the extension and one queue per consumer, both idempotent.
- The worker, `PgmqStatus` and the health check.

The core never references the hosting package.

## Durable fan-out

Each consumer is a separate logical subscription with its own queue. Publishing writes one copy per subscribed queue,
so each consumer retries, fails and archives independently of the others. The copies are separate writes.

## The worker

One read loop per queue, each with its own semaphore of slots:

1. Wait for a free slot, take every free slot, and read that many messages with a short visibility timeout (the
   lease).
2. Run each message on its own task. While it runs, one call per queue renews the leases of everything running.
3. Decide the outcome first, then write it to the queue with its own retries: delete on success; on a transient
   failure set the visibility timeout to a backoff; otherwise archive. A failed write is never mistaken for a failed
   consumer.
4. Free the slot.

An empty queue backs off its polling from `MinIdleDelay` to `MaxIdleDelay`. With insert notifications on, one
connection per host LISTENs to every such queue and a notification ends the wait; with `LongPollTimeout` the read
itself waits in the database. A loop that fails restarts itself and reports a heartbeat, which `PgmqStatus` and the
health check read.

Each consumer's settings pick the pgmq read function: `read` (with an optional conditional filter), one of the
grouped FIFO reads, or `pop` for at-most-once. A grouped read can return several messages of one group; they run one
after another on one task, and when one does not succeed the rest are released unhandled, so a group never runs out of
order. pgmq itself holds back a group while one of its messages is invisible.

Timeouts cancel the consumer's token; a consumer that ignores it is abandoned after `AbandonAfter` so its slot frees.
On shutdown the loops stop reading, running consumers get `DrainGrace`, and a message that stops on cancellation is
made visible again at once.

## Intentions

Not implemented yet, and open to design:

- **Transactional publishing** together with the application's own writes.
- **Archive retention** for queues that are not partitioned.
- **Metrics** for queue depth, age of the oldest message, and outcomes.
