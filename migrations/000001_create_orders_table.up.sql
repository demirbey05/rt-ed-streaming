CREATE TABLE orders (
    id           UUID PRIMARY KEY,
    customer_id  TEXT        NOT NULL,
    amount_cents BIGINT      NOT NULL CHECK (amount_cents > 0),
    currency     CHAR(3)     NOT NULL,
    status       TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
