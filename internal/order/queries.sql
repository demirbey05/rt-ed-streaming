-- name: CreateOrder :one
INSERT INTO orders (id, customer_id, amount_cents, currency, status, created_at)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING *;

-- name: GetOrder :one
SELECT * FROM orders
WHERE id = $1;

-- name: InsertOutboxEvent :exec
INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload, created_at)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: InsertIdempotencyKey :execrows
-- Returns 0 rows affected when the key already exists (or is being
-- inserted by a concurrent transaction that then commits).
INSERT INTO idempotency_keys (key, request_hash, order_id)
VALUES ($1, $2, $3)
ON CONFLICT (key) DO NOTHING;

-- name: GetIdempotencyKey :one
SELECT * FROM idempotency_keys
WHERE key = $1;
