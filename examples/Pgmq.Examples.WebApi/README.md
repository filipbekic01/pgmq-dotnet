# Pgmq.Examples.WebApi

A small Web API to play with Pgmq.NET: HTTP endpoints publish messages, background consumers handle them, and
`GET /activity` shows what the consumers did.

## Run it

From this folder:

```bash
docker compose up -d --wait
dotnet run
```

Postgres with pgmq runs on port **5433**, so it stays clear of any Postgres you already have on 5432. On start, Pgmq
creates the `pgmq_example` database, installs pgmq and creates one queue per consumer. The API listens on
<http://localhost:5080>.

Then open [`Pgmq.Examples.WebApi.http`](Pgmq.Examples.WebApi.http) in Rider, Visual Studio or VS Code (REST Client)
and send the requests one by one, checking `GET /activity` in between. Or with curl:

```bash
curl -X POST localhost:5080/orders -H 'Content-Type: application/json' -d '{"customer":"Ana","total":120}'
curl localhost:5080/activity
```

To start over with an empty database: `docker compose down -v`.

## What it shows

| Try | Consumer | Feature |
|---|---|---|
| `POST /orders` | `OrderPlacedHandler` | Publish by type. The handler publishes a `PaymentRequested` and a topic notification through its context: an outbox that sends only if the handler succeeds. |
| `POST /orders` with `"paymentFailures": 2` | `PaymentHandler` | A transient failure (`TimeoutException`) is retried with backoff. |
| `"paymentFailures": 5` | `PaymentHandler` | Retries run out after `MaxAttempts` (4 here) and the message is archived. |
| `"total": 50000` | `PaymentHandler` | Any other exception archives the message straight away. |
| `POST /notifications` | `EmailNotifier`, `NotificationAudit` | Fan-out: every consumer of a type gets its own copy. Plus a custom header. |
| `POST /notifications/{routingKey}` | same | Topic routing: `notify.email.*` and `notify.#`. Returns how many queues matched. |
| `GET /topics/{routingKey}` | | Which queues a routing key would reach. |
| `POST /ledger` | `LedgerHandler` | FIFO groups: each account's entries are handled in order, accounts in parallel. |
| `POST /reminders` | `ReminderHandler` | Delayed delivery. |
| `GET /queues`, `DELETE /queues/{name}` | | Queue metrics, purge. |
| `GET /health` | | The Pgmq health check. |

Archived messages stay in Postgres, one archive table per queue:

```bash
docker compose exec postgres psql -U postgres -d pgmq_example -c "select msg_id, read_ct, message from pgmq.a_example_payments"
```

## Where to look

- [`Program.cs`](Program.cs): `AddPgmq`, `AddPgmqConsumers` and the endpoints.
- [`Messages.cs`](Messages.cs): the message records.
- [`Consumers.cs`](Consumers.cs): one consumer per feature, with `[QueueName]`, `[ConsumerOptions]` and `[TopicBinding]`.
