CREATE TABLE outbox (
    id             UUID PRIMARY KEY,          -- event_id, used as the idempotency key by consumers
    aggregate_type TEXT        NOT NULL,      -- e.g. "order"
    aggregate_id   UUID        NOT NULL,
    event_type     TEXT        NOT NULL,      -- e.g. "order.created"
    payload        JSONB       NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ NULL,          -- NULL = not yet published
    attempts       INT         NOT NULL DEFAULT 0,
    last_error     TEXT        NULL
);

-- The relay only scans unpublished events, oldest first.
CREATE INDEX idx_outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;
