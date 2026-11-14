# Lesson 24: cache stampede, single-flight and TTL jitter

This branch inherits Lesson 23 (PostgreSQL 16 + Redis 7.4 Cache-Aside) and reproduces a **cache stampede** when a hot key expires under concurrent traffic, then fixes it with **in-process single-flight request coalescing** and **randomized TTL jitter**.

## Setup

.NET 10 SDK, Docker (PostgreSQL 16 + Redis 7.4) and Node 22+ are required. Stop any earlier lesson containers or API processes first.

```text
docker compose -p tadka-lesson24 up -d --wait
dotnet run -c Release --project src/Tadka.OrderApi --launch-profile demo
```

In a second terminal:

```text
node scripts/verify-api-contracts.mjs
node scripts/verify-realtime.mjs
node scripts/verify-modular-boundaries.mjs
node scripts/verify-caching-strategies.mjs
node scripts/verify-cache-stampede.mjs
```

All commands work in bash and PowerShell.

## Endpoints

| Method & route | Purpose |
|---|---|
| `GET /catalog/restaurants/{id}/menu-stampede?mode=naive\|single-flight&dbDelayMs=60&baseTtlSeconds=30&jitterSeconds=6` | Reads `tadka:menu:{id}` in Redis. On miss, applies a disclosed `dbDelayMs` (`0..500` ms) so concurrent loopback calls overlap. `naive` lets every caller hit PostgreSQL (`X-Stampede-Role: uncoordinated-miss`). `single-flight` elects one leader (`X-Stampede-Role: leader`, `X-Cache: MISS`) that populates Redis with `baseTtlSeconds + random(0..jitterSeconds)` while concurrent callers wait (`X-Stampede-Role: coalesced`, `X-Cache: COALESCED`). |
| `POST /diagnostics/cache/ttl-jitter-sample?count=12&baseTtlSeconds=30&jitterSeconds=10` | Writes `count` fixed-TTL keys and `count` jittered-TTL keys to Redis, reads back `PTTL`, and compares spread in milliseconds. |

## What the assertions prove

`scripts/verify-cache-stampede.mjs` verifies:

1. Negative control (`mode=naive`): 20 concurrent requests after cache reset produce `dbQueries === 20` (all 20 miss Redis and query PostgreSQL).
2. Single-flight coalescing (`mode=single-flight`): 20 concurrent requests after cache reset produce `dbQueries === 1`, `coalescedRequests === 19`, exactly `1` response with `X-Stampede-Role: leader`, and `19` responses with `X-Stampede-Role: coalesced`.
3. Subsequent read hits Redis (`X-Cache: HIT`) with `dbQueries === 1`.
4. `ttl-jitter-sample` produces `jitteredSpreadMs > fixedSpreadMs` and `distinctJitteredSeconds >= 3`.

## Limits and cleanup

`dbDelayMs` (default `60` ms) is a disclosed artificial delay so concurrent requests on localhost overlap deterministically. Single-flight here is **in-process** inside one `Tadka.OrderApi` instance; across `N` API replicas, each replica allows at most `1` leader per expired key (`N` total queries instead of `N * C`), whereas cross-pod single-flight requires distributed leases or probabilistic early expiration.

Stop the API with Ctrl+C and remove only this lab's containers:

```text
docker compose -p tadka-lesson24 down -v
```
