# Real-Time Event-Driven Distributed Streaming Platform

An event-driven distributed messaging platform written in Go.
It uses RabbitMQ and Azure Service Bus for reliable asynchronous communication between microservices,
the Transactional Outbox Pattern backed by PostgreSQL to prevent message loss,
and Prometheus, Grafana, Loki and Promtail for observability.

## Architecture

```
[order-service] --HTTP--> Postgres (orders + outbox, SINGLE transaction)
                                │
                        [outbox-relay]  (polling, FOR UPDATE SKIP LOCKED)
                                │
                 ┌──────────────┴──────────────┐
             RabbitMQ                    Azure Service Bus (emulator)
       (internal service events)        (external integration events)
                 │
     ┌───────────┴───────────┐
[notification-svc]     [stream-gateway] --WebSocket/SSE--> client
 (idempotent consumer)

Prometheus + Grafana + Loki + Promtail → provisioned with Docker Compose
```

## Quick Start

```bash
make up            # start Postgres
make migrate-up    # apply database migrations
make run-order     # run order-service (:8080)

curl -i -X POST localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-1' \
  -d '{"customer_id":"c-1","amount_cents":1999,"currency":"EUR"}'
```

## Testing

```bash
make test               # unit tests, no Docker needed
make test-integration   # integration tests against a throwaway Postgres (testcontainers, needs Docker)
```

## Roadmap

| Step | Topic | Status |
|---|---|---|
| 0 | Repo skeleton, docker-compose, config, health check | ✅ |
| 1 | order-service and Transactional Outbox (write side) | ✅ |
| 2 | outbox-relay → RabbitMQ (publisher confirms, retry) | ⏳ |
| 3 | Idempotent consumer, inbox table, DLQ | ⏳ |
| 4 | stream-gateway (WebSocket/SSE) | ⏳ |
| 5 | Azure Service Bus adapter | ⏳ |
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
