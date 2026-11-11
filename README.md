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
make up          # start Postgres
make run-order   # run order-service (:8080)
curl localhost:8080/healthz
```

## Roadmap

| Step | Topic | Status |
|---|---|---|
| 0 | Repo skeleton, docker-compose, config, health check | ✅ |
| 1 | order-service and Transactional Outbox (write side) | 🚧 |
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
- Optionally, embed migrations with `embed` + the `iofs` source and run them on startup

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

- [ ] `NewOrder` returns the correct error for every invalid input (table-driven tests)
- [ ] `order.created` events carry the order's ID as `AggregateID` and a valid JSON payload
- [ ] `go test ./internal/...` passes without a database

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

- [ ] A successful `Create` leaves one row in `orders` and one in `outbox`, with `published_at IS NULL`
- [ ] If the outbox insert fails, `orders` stays empty
- [ ] Invalid input never opens a transaction
- [ ] `Get` returns `ErrNotFound` for an unknown ID

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

- [ ] `POST /orders` with a valid body → `201`, a `Location` header, and one row in `orders` and `outbox`
- [ ] Invalid body (negative amount, bad currency, malformed JSON, unknown field) → `400`, nothing written
- [ ] `GET /orders/{id}` → `200` for an existing order, `404` for an unknown one, `400` for a non-UUID
- [ ] `go test ./internal/...` passes without a database

### Step 1.8 (optional): Idempotency-Key

- If a client retries the same request with the same `Idempotency-Key` header, no second order is created

### Step 1.9: Integration tests

- Service tests against a real Postgres using `testcontainers-go`, with migrations applied by golang-migrate at test start

### Acceptance Criteria

- [ ] After `POST /orders`, there is one row in `orders` and one in `outbox`, with `published_at IS NULL`
- [ ] If the outbox insert fails (forced in a test), `orders` stays empty because the rollback works
- [ ] Invalid input (negative amount, etc.) → `400`, nothing is written to the database
- [ ] Integration tests pass with `make test`
