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
- `PgmqTelemetry`: the `Pgmq` activity source and meter, and the instruments the publisher and the worker record to.
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

1. Wait for a reason to read: an insert notification, the sweeper, or a previous read that found messages. An idle
   loop waits here without touching the database.
2. Wait for a free slot, take every free slot, and read that many messages with a short visibility timeout (the
   lease). The read itself takes one of `MaxConcurrentReads` slots shared by the whole host, held only while it runs.
3. Run each message on its own task. While it runs, one batched call for every queue renews the leases of everything
   running.
4. Decide the outcome first, then write it to the queue with its own retries: delete on success (in one
   transaction with what the consumer published, when it published anything); archive on a failure or a timeout.
   A consumer is never run again because it failed. A failed
   write is never mistaken for a failed consumer.
5. Free the slot.

Nothing an idle queue does grows with the number of queues. One connection per host LISTENs to every queue, outside
the pool, and a notification wakes that queue's loop. One sweeper asks, in a single query for every queue, which have
a visible message, and wakes only those: every `MaxIdleDelay` while notifications are live, to catch delayed messages
whose time came, released messages, lapsed leases and lost notifications; every `MinIdleDelay` to about a second while
they are not. The listener and the sweeper start before the read loops, which start waiting, so starting the host is one
query, and a listener that comes back runs one sweep rather than waking every queue. An idle host holds two
connections; a busy one at most `MaxConcurrentReads` plus the settles in flight plus those two.

A loop that fails restarts itself and reports a heartbeat, which `PgmqStatus` and the health check read.

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
