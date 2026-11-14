# Lesson 23: caching strategies and invalidation

This branch inherits the `Tadka.OrderApi` modular monolith (`Catalog`, `Orders`) and adds Redis 7 (`redis:7.4-alpine` on `127.0.0.1:6379`) in front of `Catalog` menu reads to demonstrate Cache-Aside (`MISS` -> `HIT`), stale reads when invalidation is skipped, cache invalidation by key deletion, and Write-Through updates.

## Setup

.NET 10 SDK, Docker (PostgreSQL 16 + Redis 7.4) and Node 22+ are required. Stop any earlier lesson containers or API processes first so ports `5080`, `5433` and `6379` are free.

```text
docker compose -p tadka-lesson23 up -d --wait
dotnet run -c Release --project src/Tadka.OrderApi --launch-profile demo
```

In a second terminal:

```text
node scripts/verify-api-contracts.mjs
node scripts/verify-realtime.mjs
node scripts/verify-modular-boundaries.mjs
node scripts/verify-caching-strategies.mjs
```

All commands work in bash and PowerShell.

## Redis configuration

`docker-compose.yml` configures Redis with `64mb` max memory and `allkeys-lru` eviction (`--save "" --appendonly no`) so Redis acts purely as a bounded volatile cache in front of PostgreSQL. `Caching:MenuTtlSeconds` in `appsettings.json` is set explicitly to `30` seconds.

## Endpoints

| Method & route | Purpose |
|---|---|
| `GET /catalog/restaurants/{id}/menu?strategy=cache-aside\|bypass` | Reads menu via Redis key `tadka:menu:{id}` (`cache-aside`, default) or directly from PostgreSQL (`bypass`). Returns `X-Cache` (`HIT`, `MISS`, `BYPASS`, `FALLBACK`), `X-Cache-Key`, and `X-Cache-Ttl-Seconds`. |
| `PATCH /catalog/restaurants/{id}/items/{itemId}?invalidation=delete\|none\|write-through` | Updates PostgreSQL, then deletes `tadka:menu:{id}` (`delete`, default), leaves Redis untouched (`none`, negative control), or eagerly writes fresh menu JSON to Redis (`write-through`). Returns `X-Cache-Invalidation`. |
| `POST /diagnostics/cache/reset` | Clears `tadka:*` Redis keys, zeroes counters, and restores item `101` to `240.00`. |
| `GET /diagnostics/cache/stats?restaurantId=7` | Returns Redis key presence, remaining TTL, hit/miss/dbQuery/invalidation/writeThrough counters, `cachedMenu`, and `dbMenu`. |

## What the assertions prove

`scripts/verify-caching-strategies.mjs` verifies:

1. Cold read after reset produces `X-Cache: MISS` (`dbQueries === 1`, `cacheMisses === 1`).
2. Three repeat reads return `X-Cache: HIT` with zero additional PostgreSQL queries (`dbQueries === 1`, `cacheHits === 3`, `ttlRemainingSeconds > 0`).
3. Negative control (`invalidation=none`): updating item `101` to `290.00` updates PostgreSQL while Redis continues serving stale `240.00` on `X-Cache: HIT` until TTL expiry.
4. Cache-Aside invalidation (`invalidation=delete`): deletes `tadka:menu:7` (`keyExists === false`, `invalidations === 1`); the next read misses and repopulates `290.00`, then hits cleanly.
5. Write-Through (`invalidation=write-through`): updating item `101` to `260.00` writes fresh JSON to Redis immediately (`keyExists === true`, `writeThroughUpdates === 1`), so the very next read returns `X-Cache: HIT` with `260.00`.

## Limits and cleanup

Menu reads and writes here run against a single API process and single Redis node without Redis persistence. Distributed single-flight coalescing, cache stampede, and TTL jitter are added in Lesson 24.

Stop the API with Ctrl+C and remove only this lab's containers:

```text
docker compose -p tadka-lesson23 down -v
```
