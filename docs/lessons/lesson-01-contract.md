# Lesson 01 contract: Inspect & Break, the Order API under a traffic spike

Branch: `lesson-01-baseline`. This file is language-neutral. Every implementation of this lab, in any language, must satisfy it, so that the same k6 script, the same database and the same reading of the logs work for all of them.

## What the experiment proves

An API can become slow because requests wait for a database connection, even when the database command itself has not become slower.

## Fixed parts (shared, do not copy or change)

- Database: `docker compose up -d --wait`. PostgreSQL 16, `localhost:5433`, database `tadka`, user `tadka`, password `tadka_local`.
- Schema: `database/init.sql`, one `orders` table.
- Load: `load-tests/order-spike.js`.
- Monitor and reset: `scripts/`.

## API

| Item | Requirement |
|---|---|
| Port | `5080` |
| Instances | Exactly one process |
| `GET /health` | `200` with any small body |
| `POST /orders` | JSON body `{"customerId": long, "restaurantId": long, "totalAmount": decimal}` |
| Validation | All three must be positive, else `400` before touching the database |
| Success | `201` with `{"id": <new order id>}` |
| Pool failure | `503`, and the error is logged |

## Database work for one order

Exactly one SQL statement, run on one pooled connection:

```sql
INSERT INTO orders (customer_id, restaurant_id, total_amount)
SELECT $1, $2, $3
FROM pg_sleep($4)
RETURNING id;
```

`$4` is the hold time in **seconds** (for example `0.2`). Use your driver's parameter syntax (`?`, `$1`, `%s`, named parameters). Do not split the sleep and the insert into two round trips, and do not wrap them in an explicit transaction.

`pg_sleep` keeps the connection busy without using database CPU. It is an artificial delay so the pool's behaviour is visible. It is not a simulation of a slow production query.

## Settings (all explicit, none left to defaults)

| Setting | Value | How it is changed |
|---|---|---|
| Maximum pool size | `10` | Config file |
| Pool acquire timeout | `15` seconds: how long a request may wait for a free pooled connection before failing | Config file |
| Hold time | `200` ms by default | Environment variable `HOLD_MS` (the .NET reference also accepts `Demo__HoldMs`). Run 4 uses `20`. |
| Minimum or idle connections | `0` (pool starts empty) | Config file |

Library equivalents, as a starting point only. **Check each against the documentation for the version you install,** because names, units and behaviour change between versions:

| Stack | Pool size | Acquire timeout | Notes |
|---|---|---|---|
| .NET, Npgsql (reference) | `Maximum Pool Size=10` | `Timeout=15` (seconds) | Connection string |
| Java, HikariCP | `maximumPoolSize=10` | `connectionTimeout=15000` (milliseconds) | Also set `minimumIdle=0` |
| Node.js, `pg` Pool | `max: 10` | `connectionTimeoutMillis: 15000` | Confirm it applies to requests queued for a busy pool in your version |
| Python, `psycopg_pool` | `max_size=10` | `timeout=15` (seconds) on the pool or on `connection()` | Set `min_size=0` |
| Go, `pgxpool` | `MaxConns=10` | No pool setting: use a 15-second context deadline on `Acquire` | Set `MinConns=0` |

## Timers (the most important part)

Two timers per request, never one:

```
start wait timer
    acquire a connection from the pool
stop wait timer        -> wait_ms
start db timer
    execute the one SQL statement
stop db timer          -> db_ms
release the connection to the pool (always, also on error)
```

- `wait_ms` must not include validation, JSON parsing or the SQL.
- `db_ms` must not include waiting for the pool.
- If the pool times out, log `wait_ms` as the time waited until the failure and `db_ms` as `-`.
- Use a monotonic clock (not wall-clock time).
- If the client disconnects, the server still finishes the order. Do not cancel the database work on client disconnect.

## Log format

One line per request on standard output, with these key=value fields in this order:

```
order request_id=<id> status=<code> wait_ms=<int or -> db_ms=<int or -> total_ms=<int>
```

On failure add `error=<type>: <message>`. A timestamp prefix is fine. At startup, print one line with the settings in use:

```
demo_config max_pool_size=10 pool_timeout_s=15 hold_ms=200
```

## The runs

| Run | Command | Hold |
|---|---|---|
| 1 | `k6 run -e RATE=20 -e DURATION=10s load-tests/order-spike.js` | 200 ms |
| 2 | `k6 run -e RATE=40 -e DURATION=10s load-tests/order-spike.js` | 200 ms |
| 3 | `k6 run -e RATE=100 -e DURATION=30s load-tests/order-spike.js` | 200 ms |
| 4 | `k6 run -e RATE=100 -e DURATION=15s load-tests/order-spike.js` | 20 ms, after restarting the API |

Theory to compare against (theoretical, pool-bound, under the stated assumptions):

```
10 connections × (1000 ms / 200 ms) = 50 operations per second
10 connections × (1000 ms / 20 ms)  = 500 operations per second
```

## A port is done when

- [ ] All shared files are reused unchanged.
- [ ] Pool size, acquire timeout, minimum connections and hold time are set explicitly and printed at startup.
- [ ] `wait_ms` and `db_ms` follow the timer boundaries above.
- [ ] Connections are always released, including on exceptions.
- [ ] All four runs have been executed, and the report states what was measured, not what was expected.
- [ ] Any difference from the reference behaviour is explained, not hidden.
