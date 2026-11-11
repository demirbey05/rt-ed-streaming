-- name: CreateOrder :one
INSERT INTO orders (id, customer_id, amount_cents, currency, status)
VALUES ($1, $2, $3, $4, $5)
RETURNING *;

-- name: GetOrder :one
SELECT * FROM orders
WHERE id = $1;

-- name: InsertOutboxEvent :exec
INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload)
VALUES ($1, $2, $3, $4, $5);
