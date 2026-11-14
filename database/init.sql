-- Lesson 10 schema: Orders module owns public.orders, public.order_status_events, and public.order_items.
-- Catalog module owns catalog.restaurants and catalog.menu_items.
-- Intentionally no cross-schema foreign key between order_items.menu_item_id and catalog.menu_items.
CREATE TABLE IF NOT EXISTS orders (
    id              BIGSERIAL PRIMARY KEY,
    customer_id     BIGINT        NOT NULL,
    restaurant_id   BIGINT        NOT NULL,
    total_amount    NUMERIC(10,2) NOT NULL,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    idempotency_key VARCHAR(128),
    status          TEXT          NOT NULL DEFAULT 'placed',
    status_version  BIGINT        NOT NULL DEFAULT 0,
    UNIQUE (customer_id, idempotency_key)
);

CREATE INDEX orders_customer_id_id ON orders (customer_id, id DESC);

CREATE TABLE order_status_events (
    order_id BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    version  BIGINT NOT NULL,
    status   TEXT   NOT NULL,
    PRIMARY KEY (order_id, version)
);

CREATE TABLE IF NOT EXISTS order_items (
    order_id            BIGINT        NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    menu_item_id        BIGINT        NOT NULL,
    item_name_snapshot  TEXT          NOT NULL,
    unit_price_snapshot NUMERIC(10,2) NOT NULL,
    quantity            INT           NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (order_id, menu_item_id)
);

CREATE SCHEMA IF NOT EXISTS catalog;

CREATE TABLE IF NOT EXISTS catalog.restaurants (
    id      BIGINT  PRIMARY KEY,
    name    TEXT    NOT NULL,
    is_open BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS catalog.menu_items (
    id            BIGINT        PRIMARY KEY,
    restaurant_id BIGINT        NOT NULL REFERENCES catalog.restaurants(id),
    name          TEXT          NOT NULL,
    price         NUMERIC(10,2) NOT NULL CHECK (price > 0),
    is_available  BOOLEAN       NOT NULL DEFAULT TRUE,
    version       BIGINT        NOT NULL DEFAULT 1
);

INSERT INTO catalog.restaurants (id, name, is_open) VALUES
    (7, 'Tadka Indiranagar', TRUE),
    (8, 'Tadka Koramangala', FALSE);

INSERT INTO catalog.menu_items (id, restaurant_id, name, price, is_available, version) VALUES
    (101, 7, 'Paneer Tikka', 240.00, TRUE, 1),
    (102, 7, 'Masala Dosa', 120.00, TRUE, 1),
    (103, 7, 'Filter Coffee', 60.00, FALSE, 1),
    (201, 8, 'Veg Biryani', 220.00, TRUE, 1);

-- Lesson 14: flash deal inventory row for MVCC inspection and lost-update reproduction.
CREATE TABLE IF NOT EXISTS catalog.flash_deals (
    menu_item_id    BIGINT PRIMARY KEY REFERENCES catalog.menu_items(id),
    title           TEXT   NOT NULL,
    remaining_stock INT    NOT NULL CHECK (remaining_stock >= 0),
    version         BIGINT NOT NULL DEFAULT 1
);

INSERT INTO catalog.flash_deals (menu_item_id, title, remaining_stock, version) VALUES
    (101, 'Paneer Tikka Lunch Flash Deal', 5, 1)
ON CONFLICT (menu_item_id) DO NOTHING;

-- Lesson 12: seed 25,000 historical orders across 20 restaurants so Seq Scan vs B+ Tree Index Scan
-- vs Covering Index-Only Scan differences are clearly visible in EXPLAIN (ANALYZE, BUFFERS).
INSERT INTO orders (customer_id, restaurant_id, total_amount, created_at, status, status_version)
SELECT
    1000 + (i % 500),
    1 + (i % 20),
    ROUND((120 + (i % 480))::numeric, 2),
    NOW() - (i * INTERVAL '1 minute'),
    CASE (((i - 1) / 20) % 4)
        WHEN 0 THEN 'placed'
        WHEN 1 THEN 'preparing'
        WHEN 2 THEN 'ready'
        ELSE 'delivered'
    END,
    (((i - 1) / 20) % 4)
FROM generate_series(1, 25000) AS s(i);

-- Populate pg_statistic and the PostgreSQL visibility map (all_visible) from clean startup.
VACUUM ANALYZE orders;

-- Lesson 15: Distributed ID B-tree index locality benchmark tables (UUIDv4 vs UUIDv7 vs Snowflake INT8)
CREATE EXTENSION IF NOT EXISTS pgstattuple;

CREATE TABLE IF NOT EXISTS id_bench_uuidv4 (
    id         UUID        PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload    TEXT        NOT NULL
);

CREATE TABLE IF NOT EXISTS id_bench_uuidv7 (
    id         UUID        PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload    TEXT        NOT NULL
);

CREATE TABLE IF NOT EXISTS id_bench_snowflake (
    id         BIGINT      PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload    TEXT        NOT NULL
);

-- Lesson 18: 4 logical shards inside PostgreSQL to prove hot-partition skew, write salting & scatter-gather routing.
CREATE SCHEMA IF NOT EXISTS shard_0;
CREATE SCHEMA IF NOT EXISTS shard_1;
CREATE SCHEMA IF NOT EXISTS shard_2;
CREATE SCHEMA IF NOT EXISTS shard_3;

CREATE TABLE IF NOT EXISTS shard_0.sharded_orders (
    order_id      BIGINT        PRIMARY KEY,
    customer_id   BIGINT        NOT NULL,
    restaurant_id BIGINT        NOT NULL,
    shard_key     TEXT          NOT NULL,
    total_amount  NUMERIC(10,2) NOT NULL,
    status        TEXT          NOT NULL,
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS shard_0_customer_idx ON shard_0.sharded_orders (customer_id, created_at DESC, order_id DESC);
CREATE INDEX IF NOT EXISTS shard_0_restaurant_idx ON shard_0.sharded_orders (restaurant_id, created_at DESC, order_id DESC);

CREATE TABLE IF NOT EXISTS shard_1.sharded_orders (
    order_id      BIGINT        PRIMARY KEY,
    customer_id   BIGINT        NOT NULL,
    restaurant_id BIGINT        NOT NULL,
    shard_key     TEXT          NOT NULL,
    total_amount  NUMERIC(10,2) NOT NULL,
    status        TEXT          NOT NULL,
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS shard_1_customer_idx ON shard_1.sharded_orders (customer_id, created_at DESC, order_id DESC);
CREATE INDEX IF NOT EXISTS shard_1_restaurant_idx ON shard_1.sharded_orders (restaurant_id, created_at DESC, order_id DESC);

CREATE TABLE IF NOT EXISTS shard_2.sharded_orders (
    order_id      BIGINT        PRIMARY KEY,
    customer_id   BIGINT        NOT NULL,
    restaurant_id BIGINT        NOT NULL,
    shard_key     TEXT          NOT NULL,
    total_amount  NUMERIC(10,2) NOT NULL,
    status        TEXT          NOT NULL,
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS shard_2_customer_idx ON shard_2.sharded_orders (customer_id, created_at DESC, order_id DESC);
CREATE INDEX IF NOT EXISTS shard_2_restaurant_idx ON shard_2.sharded_orders (restaurant_id, created_at DESC, order_id DESC);

CREATE TABLE IF NOT EXISTS shard_3.sharded_orders (
    order_id      BIGINT        PRIMARY KEY,
    customer_id   BIGINT        NOT NULL,
    restaurant_id BIGINT        NOT NULL,
    shard_key     TEXT          NOT NULL,
    total_amount  NUMERIC(10,2) NOT NULL,
    status        TEXT          NOT NULL,
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS shard_3_customer_idx ON shard_3.sharded_orders (customer_id, created_at DESC, order_id DESC);
CREATE INDEX IF NOT EXISTS shard_3_restaurant_idx ON shard_3.sharded_orders (restaurant_id, created_at DESC, order_id DESC);

-- Lesson 32: Reliable messaging (idempotent consumer ledger + DLQ audit table)
CREATE TABLE IF NOT EXISTS consumer_loyalty_ledger (
    customer_id     BIGINT        PRIMARY KEY,
    points_balance  INT           NOT NULL DEFAULT 0,
    credit_count    INT           NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS processed_consumer_events (
    event_id        VARCHAR(128)  NOT NULL,
    consumer_group  VARCHAR(128)  NOT NULL,
    order_id        BIGINT        NOT NULL,
    customer_id     BIGINT        NOT NULL,
    points_awarded  INT           NOT NULL,
    processed_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, consumer_group)
);

CREATE TABLE IF NOT EXISTS consumer_dlq_records (
    id              BIGSERIAL     PRIMARY KEY,
    event_id        VARCHAR(128)  NOT NULL UNIQUE,
    original_topic  VARCHAR(128)  NOT NULL,
    dlq_topic       VARCHAR(128)  NOT NULL,
    partition_id    INT           NOT NULL,
    offset_value    BIGINT        NOT NULL,
    attempt_count   INT           NOT NULL,
    failure_reason  TEXT          NOT NULL,
    raw_payload     TEXT          NOT NULL,
    routed_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);
