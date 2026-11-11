# Real-Time Event-Driven Distributed Streaming Platform

An event-driven distributed messaging platform written in Go.
It uses RabbitMQ and Azure Service Bus for reliable asynchronous communication between microservices,
the Transactional Outbox Pattern backed by PostgreSQL to prevent message loss,
and Prometheus, Grafana, Loki and Promtail for observability.

## Architecture

```
 client ──POST /orders──▶ [order-service :8080]
                               │  ONE transaction: orders + outbox (+ idempotency_keys)
                               ▼
                          PostgreSQL ◀──── FOR UPDATE SKIP LOCKED ──── [outbox-relay :8081]
                                                                            │ publisher confirms
                                         ┌──────────────────────────────────┴───────────────┐
                                         ▼                                                  ▼
                           RabbitMQ  exchange "orders.events" (topic)          Azure Service Bus topic "order-events"
                                         │ routing key = event type              (duplicate detection on MessageId)
                     ┌───────────────────┴──────────────────┐                           │
                     ▼                                      ▼                           ▼
     quorum queue notification.order-events     exclusive queue per gateway     subscription "integration"
     (x-delivery-limit=5 → .dlq)                            │                           │
                     ▼                                      ▼                           ▼
     [notification-service :8082]             [stream-gateway :8083]         [servicebus-listener]
     inbox table → exactly-once effect        SSE ──▶ browser (GET /)         stands in for an external system
```

| Service | Role | Port |
|---|---|---|
| `order-service` | REST API; writes order + event atomically | 8080 |
| `outbox-relay` | Moves outbox rows to the broker(s), at-least-once | 8081 (`/healthz`) |
| `notification-service` | Idempotent consumer, retry + DLQ | 8082 (`/notifications`) |
| `stream-gateway` | Real-time fan-out to browsers over SSE | 8083 (`/`, `/events`, `/stats`) |
| `servicebus-listener` | Reads the Azure Service Bus subscription | — |

**Delivery guarantees, end to end**

| Hop | Guarantee | Mechanism |
|---|---|---|
| API → database | atomic | order + event in one transaction |
| outbox → broker | at-least-once | mark published only after the broker confirms |
| broker → notification-service | effectively exactly-once | inbox table in the same transaction as the side effect |
| broker → browser | at-most-once (best effort) | live view; the database is the source of truth |

## Quick Start

```bash
make up            # Postgres + RabbitMQ (management UI: http://localhost:15672, guest/guest)
make migrate-up    # apply database migrations

# one terminal each
make run-order
make run-relay
make run-notification
make run-gateway   # open http://localhost:8083 to watch orders arrive live

curl -i -X POST localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-1' \
  -d '{"customer_id":"c-1","amount_cents":1999,"currency":"EUR"}'

curl 'localhost:8082/notifications?customer_id=c-1'
```

With Azure Service Bus (emulator, needs ~2 GB of extra Docker disk/RAM):

```bash
make up-azure          # adds the Service Bus emulator + its SQL backend
make run-relay-azure   # publishes to RabbitMQ AND Service Bus
make run-sb-listener   # prints events from the "integration" subscription
```

## Testing

```bash
make test               # unit tests, no Docker needed
make test-integration   # integration tests against throwaway Postgres + RabbitMQ (testcontainers)
```

## Roadmap

| Step | Topic | Status |
|---|---|---|
| 0 | Repo skeleton, docker-compose, config, health check | ✅ |
| 1 | order-service and Transactional Outbox (write side) | ✅ |
| 2 | outbox-relay → RabbitMQ (publisher confirms, retry) | ✅ |
| 3 | Idempotent consumer, inbox table, DLQ | ✅ |
| 4 | stream-gateway (SSE) | ✅ |
| 5 | Azure Service Bus adapter | 🚧 code complete, emulator run pending |
| 6 | Prometheus metrics | ⏳ |
| 7 | Grafana dashboard provisioning | ⏳ |
| 8 | Centralized logging with Loki + Promtail | ⏳ |
| 9 | Chaos test: proof of zero message loss | ⏳ |

---

## Step 1: order-service and Transactional Outbox (Write Side)

The order and its event are written in the **same Postgres transaction**.
Publishing to RabbitMQ is not part of this step; that is Step 2.

### Why?

Without an outbox, the typical flow looks like this:

```go
db.Insert(order)       // ✅ committed
rabbit.Publish(event)  // ❌ network drops → order exists, event lost = data loss
```

Reversing the order causes the opposite problem: the event is published but the order is never saved.
The fix is to write the event to the database as well. Both commit together or neither does.
A separate process (the relay) moves the event to the broker afterwards.

### Step 1.1: Set up migrations (golang-migrate) and sqlc

**Migrations — [golang-migrate](https://github.com/golang-migrate/migrate)**

- Keep migrations under `migrations/` as `NNNNNN_name.up.sql` / `NNNNNN_name.down.sql` pairs
- Every `up` must have a working `down`
- Makefile targets: `make migrate-up`, `make migrate-down`, `make migrate-new name=<name>`
- Migrations are embedded (`migrations/embed.go`) and applied from Go via `internal/db.MigrateUp`
  using the `iofs` source and the `pgx5://` driver; integration tests use this to build their schema

**Queries — [sqlc](https://sqlc.dev)**

- `sqlc.yaml` at the repo root, engine `postgresql`, `sql_package: pgx/v5`
- sqlc reads the schema directly from `migrations/` (it skips `.down.sql` files)
- Queries live in `internal/order/queries.sql`, generated code goes to `internal/order/store/`
- `uuid` columns map to `github.com/google/uuid.UUID` instead of the default `pgtype.UUID`
- Makefile target: `make sqlc`; generated code is committed so the project builds without sqlc installed

```yaml
version: "2"
sql:
  - engine: postgresql
    schema: migrations
    queries: internal/order/queries.sql
    gen:
      go:
        package: store
        out: internal/order/store
        sql_package: pgx/v5
        emit_json_tags: true
        overrides:
          - db_type: uuid
            go_type: github.com/google/uuid.UUID
          - db_type: timestamptz
            go_type: time.Time
          - db_type: timestamptz
            nullable: true
            go_type:
              type: time.Time
              pointer: true
```

Verify with `make sqlc && go build ./...`.

### Step 1.2: `orders` table

```sql
CREATE TABLE orders (
    id           UUID PRIMARY KEY,
    customer_id  TEXT        NOT NULL,
    amount_cents BIGINT      NOT NULL CHECK (amount_cents > 0),
    currency     CHAR(3)     NOT NULL,
    status       TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### Step 1.3: `outbox` table

```sql
CREATE TABLE outbox (
    id             UUID PRIMARY KEY,          -- event_id, used as the idempotency key by consumers
    aggregate_type TEXT        NOT NULL,      -- "order"
    aggregate_id   UUID        NOT NULL,
    event_type     TEXT        NOT NULL,      -- "order.created"
    payload        JSONB       NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ NULL,          -- NULL = not yet published
    attempts       INT         NOT NULL DEFAULT 0,
    last_error     TEXT        NULL
);

-- The relay only scans unpublished events
CREATE INDEX idx_outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;
```

### Step 1.4: Domain model and event envelope

`store.Order` (generated by sqlc) is a database row. This step defines the **domain model**,
where business rules live, and the **event envelope** that goes to the broker.
Plain Go: no HTTP, no SQL.

```
internal/order/domain.go   # Order, Status, NewOrder + validation errors
internal/order/events.go   # order.created payload and Order → outbox.Event mapping
internal/outbox/event.go   # generic Event envelope shared by all event types
```

**Domain model**

```go
type Order struct {
    ID          uuid.UUID
    CustomerID  string
    AmountCents int64
    Currency    string
    Status      Status
    CreatedAt   time.Time
}

func NewOrder(customerID string, amountCents int64, currency string) (Order, error)
```

`NewOrder` is the only way to build a valid order:

- `customerID` must not be empty
- `amountCents > 0`
- `currency` is a 3-letter ISO code, normalized to upper case (`eur` → `EUR`)
- The ID is generated by the application, not the database, so the order and its event are fully
  built before a transaction is opened (see [Why application-generated IDs?](#why-application-generated-ids))
- IDs are **UUIDv7** (`uuid.NewV7()`): time-ordered, so inserts append to the B-tree index
  instead of splitting random pages like UUIDv4 does
- Validation failures are sentinel errors (`ErrInvalidCustomer`, `ErrInvalidAmount`, `ErrInvalidCurrency`),
  wrapped by `ErrValidation`, so the HTTP layer can map them to `400` with `errors.Is`

**Event envelope**

```go
type Event struct {
    ID            uuid.UUID        // outbox.id, the consumer's idempotency key
    Type          string           // "order.created"
    AggregateType string           // "order"
    AggregateID   uuid.UUID
    OccurredAt    time.Time
    Payload       json.RawMessage
}
```

Each event type has its own payload struct (e.g. `OrderCreatedPayload`). The event contract is
decoupled from the table schema, so a schema change does not break consumers.

#### Why application-generated IDs?

- **Shorter transactions:** the aggregate and its event (including JSON marshalling) are built before
  `BEGIN`, so the transaction contains nothing but two inserts
- **Globally unique:** events leave this service; consumers can use the ID as a key in their own stores,
  which a per-table `SERIAL` cannot guarantee
- **Retry-safe:** a retried write with the same ID hits the primary key instead of creating a duplicate
- **Not enumerable:** sequential IDs leak business volume and invite IDOR attacks
- **Testable:** the domain can be tested without a database

**Dependency direction:** `domain.go` and `events.go` use neither `store` nor `pgx`, and `store` never
imports `order`. The service layer (Step 1.6) is the only place that maps between the two.

**sqlc override:** `timestamptz` maps to `time.Time` (and `*time.Time` when nullable) instead of
`pgtype.Timestamptz`, which keeps the domain ↔ store mapping trivial.

**Acceptance criteria**

- [x] `NewOrder` returns the correct error for every invalid input (table-driven tests)
- [x] `order.created` events carry the order's ID as `AggregateID` and a valid JSON payload
- [x] `go test ./internal/...` passes without a database

### Step 1.5: sqlc queries

```
internal/order/queries.sql   # source queries
internal/order/store/        # generated by sqlc (do not edit by hand)
```

```sql
-- name: CreateOrder :one
INSERT INTO orders (id, customer_id, amount_cents, currency, status, created_at)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING *;

-- name: GetOrder :one
SELECT * FROM orders WHERE id = $1;

-- name: InsertOutboxEvent :exec
INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload, created_at)
VALUES ($1, $2, $3, $4, $5, $6);
```

`created_at` is passed explicitly so the stored timestamps match the domain object and the
event's `OccurredAt` exactly; the column `DEFAULT now()` is only a fallback.

- Run `make sqlc` and use the generated `store.Queries` type
- `store.New(pool).WithTx(tx)` binds the same queries to a transaction, which is how the outbox insert shares the order's transaction

### Step 1.6: Service (transaction boundary)

```
internal/order/service.go
```

This is the heart of the outbox pattern: the order and its event are written in **one transaction**.
Either both commit or neither does.

```go
func (s *Service) Create(ctx context.Context, in CreateInput) (Order, error) {
    o, err := NewOrder(...)        // validation, no DB
    ev, err := o.CreatedEvent()    // event fully built, no DB

    tx, err := s.db.Begin(ctx)
    defer tx.Rollback(ctx)         // no-op after Commit, undoes everything on error or panic

    q := s.q.WithTx(tx)
    q.CreateOrder(ctx, ...)        // 1. the order
    q.InsertOutboxEvent(ctx, ...)  // 2. its event, same transaction
    return o, tx.Commit(ctx)
}
```

- **Small dependency:** the service depends on a `DB` interface (`store.DBTX` + `Begin`), which
  `*pgxpool.Pool` satisfies
- **Mapping:** `store.Order` → domain `Order` happens only here (`toDomain`)
- **Error translation:** `pgx.ErrNoRows` becomes the domain error `ErrNotFound`, so the HTTP layer never sees `pgx`
- **No transaction for invalid input:** validation fails before `Begin`

**Acceptance criteria**

- [x] A successful `Create` leaves one row in `orders` and one in `outbox`, with `published_at IS NULL`
- [x] If the outbox insert fails, `orders` stays empty
- [x] Invalid input never opens a transaction
- [x] `Get` returns `ErrNotFound` for an unknown ID

### Step 1.7: HTTP handlers

```
internal/httpserver/json.go      # WriteJSON / WriteError helpers
internal/order/handler.go        # POST /orders, GET /orders/{id}
internal/order/handler_test.go   # httptest + fake service, no database
cmd/order-service/main.go        # wires pool → Service → Handler
```

**API**

| Method | Path | Success | Errors |
|---|---|---|---|
| `POST` | `/orders` | `201` + order, `Location: /orders/{id}` | `400` invalid JSON / validation, `500` |
| `GET` | `/orders/{id}` | `200` + order | `400` malformed UUID, `404` not found, `500` |

```bash
curl -i -X POST localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"c-1","amount_cents":1999,"currency":"EUR"}'
```

Errors share one shape: `{"error": "<message>"}`.

**Design notes**

- The handler depends on a small `OrderService` interface (`Create`, `Get`), so it is unit-tested
  with a fake; the real transaction logic is covered by the integration tests in Step 1.9
- Request bodies are capped (`http.MaxBytesReader`, 1 MB) and unknown JSON fields are rejected
- Error mapping uses `errors.Is`: `ErrValidation` → `400`, `ErrNotFound` → `404`, anything else → `500`
- `500` responses return a generic message; the real error is only logged, never leaked to the client
- Routing uses the Go 1.22+ standard `http.ServeMux` patterns (`POST /orders`, `GET /orders/{id}`), no framework

**Acceptance criteria**

- [x] `POST /orders` with a valid body → `201`, a `Location` header, and one row in `orders` and `outbox`
- [x] Invalid body (negative amount, bad currency, malformed JSON, unknown field) → `400`, nothing written
- [x] `GET /orders/{id}` → `200` for an existing order, `404` for an unknown one, `400` for a non-UUID
- [x] `go test ./internal/...` passes without a database

### Step 1.8: Idempotency-Key

Clients (or proxies) retry on timeouts. Without protection, a retry after a lost response creates a
second order **and a second event**. With an `Idempotency-Key` header, a retry returns the original order.

```
migrations/000003_create_idempotency_keys_table.*.sql   # key → request_hash, order_id
```

```sql
CREATE TABLE idempotency_keys (
    key          TEXT PRIMARY KEY,
    request_hash TEXT        NOT NULL,   -- sha256 of the normalized request
    order_id     UUID        NOT NULL REFERENCES orders (id),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**Behavior**

| Request | Response |
|---|---|
| New key | `201`, order created |
| Same key, same body | `201` with the **original** order, `Idempotent-Replayed: true` |
| Same key, different body | `422 Unprocessable Entity` |
| Key longer than 255 chars | `400` |
| No key | Normal create, no protection |

The request hash is computed from the **normalized** input, so `"eur"` and `"EUR"` count as the same request.

**How it works — two paths**

1. **Fast path (sequential retry):** look up the key before opening a transaction; if it exists, return the stored order.
2. **Slow path (concurrent retries):** both requests pass the lookup and race. The key is inserted
   in the same transaction as the order and event with `ON CONFLICT (key) DO NOTHING`. The unique index makes
   the second transaction **wait** for the first; once the first commits, the second sees `0 rows affected`,
   rolls back its own order + event, and returns the winner's order.

The key lives in the same transaction as the order and event, so a key can never point to an order
that was rolled back, and a committed order can never be missing its key.

**Not in scope (yet):** keys never expire. A production system would delete keys older than ~24 h.

### Step 1.9: Integration tests

```
internal/order/integration_test.go   # //go:build integration
```

- `TestMain` starts `postgres:16-alpine` with `testcontainers-go`, applies migrations with
  `db.MigrateUp`, and shares one pool across tests; each test truncates the tables first
- Behind the `integration` build tag, so `make test` stays fast and needs no Docker

| Test | Proves |
|---|---|
| `TestCreate_WritesOrderAndOutboxInOneTransaction` | One order + one `order.created` event, `published_at IS NULL`, payload matches, `Create` == `Get` |
| `TestCreate_RollsBackOrderWhenOutboxInsertFails` | A trigger makes the outbox insert fail → `orders` stays empty |
| `TestCreate_InvalidInputWritesNothing` | Validation fails before any write |
| `TestGet_UnknownIDReturnsNotFound` | `pgx.ErrNoRows` → `ErrNotFound` |
| `TestCreate_IdempotencyKeyReplaysOriginalOrder` | Retry with same key → same order, still 1 order / 1 event |
| `TestCreate_IdempotencyKeyReuseWithDifferentBodyFails` | Same key, different body → `ErrIdempotencyKeyReuse` |
| `TestCreate_ConcurrentRequestsWithSameKeyCreateOneOrder` | 20 simultaneous requests, same key → exactly 1 order, 1 event, 19 replays |
| `TestHTTP_CreateWithIdempotencyKeyEndToEnd` | Real HTTP server + real DB, replay header and `Location` |
| `TestMigrations_DownThenUp` | Every `up` migration has a working `down` |

The rollback test uses a real failure inside Postgres rather than a mock, so it proves the
transaction boundary itself, not just that our code calls `Rollback`.

### Step 1 Acceptance Criteria

- [x] After `POST /orders`, there is one row in `orders` and one in `outbox`, with `published_at IS NULL`
- [x] If the outbox insert fails, `orders` stays empty because the rollback works
- [x] Invalid input (negative amount, etc.) → `400`, nothing is written to the database
- [x] Retrying with the same `Idempotency-Key` never creates a second order or event, even concurrently
- [x] `make test` and `make test-integration` pass

---

## Step 2: outbox-relay → RabbitMQ

A separate process moves events from the `outbox` table to the broker.

```
cmd/outbox-relay/main.go
internal/relay/relay.go             # poll loop, batch processing, backoff
internal/relay/queries.sql          # FetchUnpublished (SKIP LOCKED), MarkPublished, MarkFailed
internal/broker/broker.go           # Publisher interface, Multi publisher, ErrPermanent
internal/broker/rabbitmq/publisher.go
internal/broker/rabbitmq/topology.go
```

### How one batch works

```
BEGIN
  SELECT … FROM outbox WHERE published_at IS NULL
  ORDER BY created_at, id LIMIT 100
  FOR UPDATE SKIP LOCKED                 -- other relay instances skip these rows
  for each event (in order):
      publish → wait for publisher confirm
      on failure: attempts += 1, last_error = …, STOP the batch
  UPDATE outbox SET published_at = now() WHERE id = ANY(<confirmed ids>)
COMMIT
```

- **Publisher confirms:** `Publish` returns only after RabbitMQ acks the message. Messages are
  `persistent` and go to **durable quorum queues**, so an ack means the message is on disk (replicated in a cluster).
- **`mandatory` + `NotifyReturn`:** a message no queue is bound to is returned instead of silently dropped,
  and counts as a failure. The relay also declares `notification.order-events` itself, so events published
  before the consumer ever started are kept, not lost.
- **Stop at first failure:** later events are not published ahead of an earlier one.
- **Backoff:** a failed batch waits 500 ms → 1 s → 2 s … up to 30 s; a full batch is followed immediately by the next.
- **Shutdown safety:** confirmed IDs are marked with a context detached from cancellation
  (`context.WithoutCancel`), so a SIGTERM mid-batch does not cause those events to be re-sent.
- **Reconnect:** the publisher reconnects lazily on the next publish; a broker outage only fails batches.
- **Message format:** body = the full event envelope as JSON; `MessageId` = event ID, `Type` = event type,
  routing key = event type (`order.created`).

### Why at-least-once and not exactly-once?

If the relay crashes after the broker confirmed but before `COMMIT`, the row is still unpublished and is sent
again on restart. Exactly-once *delivery* across a database and a broker is impossible without a distributed
transaction; instead, consumers make the *effect* exactly-once (Step 3).

### Trade-offs

- The transaction (and row locks) stays open while publishing. Batches are small and publishes are fast, so
  this is acceptable; the alternative ("claim with a lease column, publish outside the transaction") adds complexity.
- With several relay instances, order is guaranteed within a batch but not across instances.
  If strict per-aggregate ordering is required: one relay, or partition by `aggregate_id`.
- A message that can never be published (e.g. too large) blocks the outbox (head-of-line blocking);
  `attempts`/`last_error` make it visible, and Step 6 will alert on it.
- Polling adds up to `RELAY_POLL_INTERVAL` latency. Postgres `LISTEN/NOTIFY` or CDC (Debezium) could remove it.

### Acceptance criteria

- [x] Events are published in order and marked `published_at` only after the broker confirms
- [x] With RabbitMQ down, `POST /orders` still returns `201`; events wait in the outbox; `/healthz` reports `degraded`
- [x] When RabbitMQ returns, the relay drains the outbox with no manual action
- [x] 4 relay instances × 60 events → each event published exactly once (`SKIP LOCKED`)

---

## Step 3: Idempotent consumer, inbox table, DLQ

```
cmd/notification-service/main.go
internal/broker/rabbitmq/consumer.go     # reconnecting consumer, ack / nack / dead-letter
internal/notification/handler.go         # inbox + side effect in one transaction
internal/notification/queries.sql
migrations/000004_create_notification_tables.*.sql   # processed_messages (inbox), notifications
```

### Inbox pattern (idempotent consumer)

```
BEGIN
  INSERT INTO processed_messages (consumer, message_id) VALUES ('notification-service', <event id>)
  ON CONFLICT DO NOTHING                    -- 0 rows → duplicate → ACK and skip
  INSERT INTO notifications …              -- the side effect
COMMIT
ACK                                         -- only after commit
```

- The inbox row and the side effect commit together: a failure rolls both back, so the retry is not
  mistaken for a duplicate.
- **Ack after commit:** a crash between commit and ack causes a redelivery, which the inbox catches.
- The inbox is keyed by `(consumer, message_id)`, so each consumer of the same event deduplicates independently.
- Unknown event types are recorded and acked, so a new producer event never blocks this consumer.

### Retry and dead-lettering

```
orders.events (topic) ──order.*──▶ notification.order-events   quorum queue, x-delivery-limit = 5
                                         │ rejected / limit exceeded
                                         ▼
                                   orders.dlx (direct) ──▶ notification.order-events.dlq
```

| Handler result | Action | Outcome |
|---|---|---|
| `nil` | `Ack` | done |
| wraps `broker.ErrPermanent` (malformed JSON, missing ID) | `Nack(requeue=false)` | straight to the DLQ |
| any other error (DB down, …) | wait `RetryDelay`, `Nack(requeue=true)` | retried; after 5 redeliveries RabbitMQ dead-letters it |

The delivery limit is enforced by RabbitMQ itself (quorum queue `x-delivery-limit`), so retry counting
survives consumer restarts and needs no extra code or retry queues.

- **Prefetch (QoS) = 10:** bounds in-flight unacked messages per consumer.
- **Reconnect:** the consumer re-dials with exponential backoff and re-declares its topology.
- **Chaos hook:** `NOTIFY_FAIL_CUSTOMER_ID=<id>` makes that customer's events fail, to demo retry → DLQ.

### Acceptance criteria

- [x] The same event delivered 3 times produces exactly 1 notification
- [x] A failed handling leaves no inbox row; the retry succeeds
- [x] A malformed message goes to the DLQ immediately
- [x] A persistently failing message is retried, then dead-lettered (6 deliveries total)
- [x] Consumers survive a RabbitMQ restart and resume

---

## Step 4: stream-gateway (Server-Sent Events)

```
cmd/stream-gateway/main.go
internal/gateway/hub.go          # client registry, non-blocking broadcast
internal/gateway/sse.go          # GET /events, GET /stats, GET / (demo page)
internal/gateway/static/index.html
```

- **Fan-out:** each gateway instance declares its own **exclusive, auto-delete** queue bound to `order.*`.
  Every instance sees every event, and nothing piles up while no gateway is running.
- **SSE over WebSocket:** traffic is one-way (server → browser). SSE is plain HTTP, works through proxies,
  and `EventSource` reconnects automatically. WebSocket would add a protocol upgrade for no benefit here.
- **Filtering:** `GET /events?customer_id=alice` streams only that customer's events.
- **Slow clients:** each client has a 32-message buffer; when full, messages to *that* client are dropped
  (counted in `/stats`). One slow browser can never block the consumer or other clients.
- **Heartbeat:** a `: ping` comment every 15 s keeps idle proxies from closing the connection.
- **Graceful shutdown:** the hub closes every client channel on SIGTERM, so open streams end and
  `http.Server.Shutdown` does not wait for them forever.
- **Guarantee:** at-most-once. The stream is a live view; a reconnecting client reloads state from the API.
  Not implemented: `Last-Event-ID` replay.

### Acceptance criteria

- [x] `POST /orders` → the event appears at `http://localhost:8083` within the relay poll interval
- [x] Customer filter, slow-client dropping and shutdown are unit-tested; the full path is integration-tested

---

## Step 5: Azure Service Bus adapter

```
internal/broker/servicebus/servicebus.go   # Publisher + Listen (subscription receiver)
cmd/servicebus-listener/main.go
deploy/servicebus/config.json              # emulator: topic "order-events", subscription "integration"
```

The relay depends only on `broker.Publisher`. `PUBLISHERS=rabbitmq,servicebus` wraps both in a
`broker.Multi`: an event is marked published only when **every** broker accepted it.

- **RabbitMQ** carries internal service-to-service events; **Service Bus** carries integration events
  for external consumers (simulated by `servicebus-listener`).
- **Duplicates across brokers:** if Service Bus fails after RabbitMQ succeeded, the whole event is retried,
  so RabbitMQ sees it twice (handled by the inbox) and Service Bus deduplicates by `MessageId`
  (`RequiresDuplicateDetection: true`, 10-minute window).
- **Listener:** `CompleteMessage` on success, `AbandonMessage` on failure (redelivered; dead-lettered after
  `MaxDeliveryCount: 5`), `DeadLetterMessage` for malformed bodies.
- **Local emulator:** the official Service Bus emulator in Docker Compose (profile `azure`), connected with
  `UseDevelopmentEmulator=true`. Its AMQP port is mapped to `5673` because `5672` is RabbitMQ.

### Acceptance criteria

- [x] `PUBLISHERS=rabbitmq` (default) and `PUBLISHERS=rabbitmq,servicebus` both build and run the same relay
- [ ] With `make up-azure`, an order reaches `servicebus-listener` *(pending a local emulator run)*
