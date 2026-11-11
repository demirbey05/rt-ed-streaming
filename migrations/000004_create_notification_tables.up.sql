-- Inbox: one row per (consumer, message) that has been fully processed.
-- Inserting here in the same transaction as the side effect makes the
-- consumer idempotent under at-least-once delivery.
CREATE TABLE processed_messages (
    consumer     TEXT        NOT NULL,
    message_id   UUID        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, message_id)
);

-- The side effect of notification-service: a notification per new order.
CREATE TABLE notifications (
    id          UUID PRIMARY KEY,
    event_id    UUID        NOT NULL,
    order_id    UUID        NOT NULL,
    customer_id TEXT        NOT NULL,
    message     TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_notifications_customer ON notifications (customer_id, created_at DESC);
