# Lesson 26: HTTP caching, ETag and edge caching

This branch inherits Lesson 24 (`Tadka.OrderApi` with Redis Cache-Aside + Single-Flight on `:5080`, and `Tadka.GatewayBff` on `:5081`) and adds HTTP caching semantics (`Cache-Control`, strong `ETag`, `If-None-Match` -> `304 Not Modified`), strict `private, no-store` headers on personal order receipts, and a simulated CDN/edge cache inside `Tadka.GatewayBff`.

## Setup

.NET 10 SDK, Docker (PostgreSQL 16 + Redis 7.4) and Node 22+ are required. Stop any earlier lesson containers or API processes first.

```text
docker compose -p tadka-lesson26 up -d --wait
dotnet run -c Release --project src/Tadka.OrderApi --launch-profile demo
```

In a second terminal:

```text
dotnet run -c Release --project src/Tadka.GatewayBff --launch-profile demo
```

In a third terminal:

```text
node scripts/verify-api-contracts.mjs
node scripts/verify-realtime.mjs
node scripts/verify-modular-boundaries.mjs
node scripts/verify-gateway-bff.mjs
node scripts/verify-caching-strategies.mjs
node scripts/verify-cache-stampede.mjs
node scripts/verify-http-edge-caching.mjs
```

All commands work in bash and PowerShell.

## HTTP cache contract

| Endpoint | Cache-Control & ETag behaviour |
|---|---|
| `GET /catalog/restaurants/{id}/menu` | Sets `Cache-Control: public, max-age=15, stale-while-revalidate=30`, `Vary: Accept-Encoding`, and a deterministic strong `ETag`. When `If-None-Match` matches, returns `304 Not Modified` with 0 body bytes. |
| `GET /orders/{id}/receipt` | Sets `Cache-Control: private, no-store` so personal receipts are never cached by shared proxies or CDNs. |
| `GET /api/catalog/restaurants/{id}/menu` (Gateway `:5081`) | Forwards `If-None-Match` upstream and preserves `ETag`, `Cache-Control`, `Vary`, `X-Cache` and `304` empty responses. |
| `GET /api/edge/catalog/restaurants/{id}/menu` (Gateway `:5081`) | Simulates an HTTP edge cache in `GatewayBff`: first read returns `X-Edge-Cache: MISS`, repeat reads within 15s return `X-Edge-Cache: HIT` without calling `OrderApi`, `?revalidate=true` validates with `If-None-Match` (`REVALIDATED` on `304`, `REFRESHED` on `200`). |
| `POST /diagnostics/edge-cache/reset`, `GET /diagnostics/edge-cache/stats` | Clears and inspects edge cache counters (`edgeHits`, `edgeMisses`, `edgeRevalidated`, `edgeRefreshed`). |

## What the assertions prove

`scripts/verify-http-edge-caching.mjs` verifies:

1. `GET /catalog/restaurants/7/menu` returns `200 OK` with `Cache-Control: public, max-age=15, stale-while-revalidate=30` and strong `ETag`.
2. Conditional `GET` with matching `If-None-Match` returns `304 Not Modified` and 0 body bytes.
3. Updating item `101` to `290.00` changes the `ETag`; conditional `GET` with the old `ETag` returns `200 OK` with the new `ETag` and `290.00`.
4. Through `Tadka.GatewayBff` (`:5081`), `GET /api/catalog/restaurants/7/menu` with `If-None-Match` returns `304`, and `/api/edge/catalog/restaurants/7/menu` transitions cleanly through `MISS` -> `HIT` -> `REVALIDATED` -> `REFRESHED`.
5. `GET /orders/{id}/receipt` returns `Cache-Control: private, no-store`.

## Limits and cleanup

The edge cache in `Tadka.GatewayBff` is an in-memory simulator for local inspection rather than a globally distributed CDN PoP network. CDN PoP vs Origin Shield topology and the stacked TTL arithmetic trap are covered on the board in Lesson 26 Video 2.

Stop both APIs with Ctrl+C and remove only this lab's containers:

```text
docker compose -p tadka-lesson26 down -v
```
