# Pgmq.Examples.WebApi

A small Web API to play with Pgmq.NET: HTTP endpoints publish messages, background consumers in the same process
handle them, and `GET /activity` shows what the consumers did. It runs on the library's defaults, retries included.
A worker service without HTTP sets Pgmq up the same way, on `Host.CreateApplicationBuilder`.

## Run it

From this folder:

```bash
docker compose up -d --wait
dotnet run
```

Postgres with pgmq runs on port **5432**. If you already run Postgres with pgmq there, skip `docker compose up`: the
app uses it as is. On start, Pgmq creates the `example-transport` database, installs pgmq and creates one queue per consumer. The API listens on
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
| `POST /orders` with `"paymentFailures": 2` | `PaymentHandler` | The library's default retry policy: none. The handler retries its flaky gateway call itself, and the 3rd call succeeds. |
| `"paymentFailures": 5` | `PaymentHandler` | The gateway fails 3 times, the handler throws, and the message is archived. Pgmq does not run it again. |
| `"total": 50000` | `PaymentHandler` | Any exception archives the message straight away. |
| `POST /notifications` | `EmailNotifier`, `NotificationAudit` | Fan-out: every consumer of a type gets its own copy. Plus a custom header. |
| `POST /notifications/{routingKey}` | same | Topic routing: `notify.email.*` and `notify.#`. Returns how many queues matched. |
| `GET /topics/{routingKey}` | | Which queues a routing key would reach. |
| `POST /ledger` | `LedgerHandler` | FIFO groups: each account's entries are handled in order, accounts in parallel. |
| `POST /reminders` | `ReminderHandler` | Delayed delivery. |
| `GET /queues`, `DELETE /queues/{name}` | | Queue metrics, purge. |
| `GET /health` | | The Pgmq health check. |

Archived messages stay in Postgres, one archive table per queue:

```bash
docker compose exec postgres psql -U postgres -d example-transport -c "select msg_id, read_ct, message from pgmq.a_example_payments"
```

## Telemetry

Run with the `otlp` launch profile to send traces, metrics and logs to an OpenTelemetry collector on
<http://localhost:4318>, for example [grafana/otel-lgtm](https://github.com/grafana/docker-otel-lgtm):

```bash
docker run -d -p 3000:3000 -p 4318:4318 grafana/otel-lgtm
dotnet run --launch-profile otlp
```

One `POST /orders` makes a single trace: the HTTP request, the `send`, the `process` of each consumer, the sends made
from inside a consumer, and each `delete`. Without the profile (no `OTEL_EXPORTER_OTLP_ENDPOINT`), the app exports no
telemetry.

## Where to look

- [`Program.cs`](Program.cs): `AddPgmq`, `AddPgmqConsumers` and the endpoints.
- [`Messages.cs`](Messages.cs): the message records.
- [`Consumers.cs`](Consumers.cs): one consumer per feature, with `[QueueName]`, `[ConsumerOptions]` and `[TopicBinding]`.
