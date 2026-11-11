-- Maps a client-supplied Idempotency-Key to the order it created.
-- request_hash detects the same key being reused with a different body.
CREATE TABLE idempotency_keys (
    key          TEXT PRIMARY KEY,
    request_hash TEXT        NOT NULL,
    order_id     UUID        NOT NULL REFERENCES orders (id),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
