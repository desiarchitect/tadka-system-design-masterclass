-- Lesson 01 baseline schema. Runs once, when the PostgreSQL container starts fresh.
CREATE TABLE IF NOT EXISTS orders (
    id            BIGSERIAL PRIMARY KEY,
    customer_id   BIGINT        NOT NULL,
    restaurant_id BIGINT        NOT NULL,
    total_amount  NUMERIC(10,2) NOT NULL,
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);
