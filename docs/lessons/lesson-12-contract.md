# Lesson 12: database indexing and EXPLAIN ANALYZE

This branch inherits Lesson 11 (`Tadka.OrderApi`, `Tadka.GatewayBff`, Catalog/Orders boundaries, idempotency, cursor pages and SSE replay) and seeds 25,000 historical rows into `orders` so query planner behaviour is visible and measurable with `EXPLAIN (ANALYZE, BUFFERS)`.

## Setup

.NET 10 SDK, Docker with PostgreSQL 16 and Node 22+ are required. Stop any earlier lesson database and API first so ports `5080`, `5081` and `5433` are free. Use a fresh database (`docker compose -p tadka-lesson12 down -v` before starting if needed).

```text
docker compose -p tadka-lesson12 up -d --wait
dotnet run -c Release --project src/Tadka.OrderApi --launch-profile demo
```

In a second terminal:

```text
node scripts/verify-api-contracts.mjs
node scripts/verify-realtime.mjs
node scripts/verify-modular-boundaries.mjs
node scripts/verify-indexing.mjs
```

All commands work in bash and PowerShell.

## Endpoints

| Method & route | Purpose |
|---|---|
| `POST /diagnostics/indexing/mode` | Switch index mode on `orders`: `none` (no kitchen index), `composite` (`(restaurant_id, status, created_at DESC)`), or `covering` (`(restaurant_id, status, created_at DESC) INCLUDE (id, customer_id, total_amount)` + `VACUUM ANALYZE`). Returns `tableBytes` and `indexesBytes`. |
| `POST /diagnostics/indexing/reseed` | Restores the 25,000 baseline historical rows if `orders` was truncated. |
| `GET /orders/kitchen-feed?restaurantId=7&status=preparing&limit=20&explain=true` | Queries recent orders for one restaurant and status, returning `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` scan telemetry (`nodeType`, `indexName`, `actualRows`, `rowsRemovedByFilter`, `heapFetches`, `sharedHitBlocks`, `sharedReadBlocks`, `executionTimeMs`). |
| `GET /orders/status-scan?status=preparing&limit=20&explain=true` | Negative control for the leftmost-prefix rule: queries `WHERE status = $1 ORDER BY created_at DESC LIMIT $2` without `restaurant_id`. |

## What the assertions prove

`scripts/verify-indexing.mjs` verifies:

1. With `mode: "none"`, `kitchen-feed` performs a `Seq Scan` and filters out thousands of non-matching rows (`rowsRemovedByFilter > 1000`).
2. With `mode: "composite"`, `kitchen-feed` uses `Index Scan` on `orders_restaurant_status_created_idx` with `rowsRemovedByFilter === 0`.
3. With `mode: "covering"` (plus `VACUUM ANALYZE` updating the visibility map), `kitchen-feed` uses `Index Only Scan` on `orders_restaurant_status_covering_idx` with `heapFetches === 0` and `rowsRemovedByFilter === 0`.
4. Under `mode: "covering"`, omitting the leftmost key column (`status-scan` without `restaurant_id`) yields no narrow prefix `Index Cond` seek (`indexCond === null`), and `indexesBytes` strictly exceeds the `none` baseline (storage and write-maintenance cost).

## Limits and cleanup

The 25,000-row dataset fits easily inside PostgreSQL shared buffers, so `executionTimeMs` reflects in-memory page reads rather than cold spinning-disk I/O. Focus on buffer block counts, `rowsRemovedByFilter`, `heapFetches`, `indexCond`, and `indexesBytes` rather than wall-clock milliseconds alone.

Stop the API with Ctrl+C and clean up this lab's database:

```text
docker compose -p tadka-lesson12 down -v
```
