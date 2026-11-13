-- Lesson 01 baseline schema. Runs once, when the PostgreSQL container starts fresh.
CREATE TABLE IF NOT EXISTS orders (
    id            BIGSERIAL PRIMARY KEY,
    customer_id   BIGINT        NOT NULL,
    restaurant_id BIGINT        NOT NULL,
    total_amount  NUMERIC(10,2) NOT NULL,
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    idempotency_key VARCHAR(128),
    status TEXT NOT NULL DEFAULT 'placed',
    status_version BIGINT NOT NULL DEFAULT 0,
    UNIQUE (customer_id, idempotency_key)
);

CREATE INDEX orders_customer_id_id ON orders (customer_id, id DESC);

CREATE TABLE order_status_events (
    order_id BIGINT NOT NULL REFERENCES orders(id),
    version BIGINT NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY (order_id, version)
);
