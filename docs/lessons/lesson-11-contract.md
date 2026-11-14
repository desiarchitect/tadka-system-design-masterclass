# Lesson 11: API Gateway and Backend-for-Frontend (BFF)

This branch inherits Lesson 10 (`Tadka.OrderApi` with isolated `Catalog` and `Orders` modules on `:5080`) and adds a separate edge process `Tadka.GatewayBff` on `:5081`. `Tadka.GatewayBff` holds **zero database references** and talks only over HTTP to `Tadka.OrderApi`. Service discovery is covered on the board in Video 2 and not simulated as a live multi-node cluster here.

## Setup

.NET 10 SDK, Docker with PostgreSQL 16 and Node 22+ are required. Stop any previous lab containers or API processes first.

```text
docker compose -p tadka-lesson11 up -d --wait
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
```

All commands work in bash and PowerShell.

## Edge contract (`:5081`)

Every `/api/*` and `/bff/*` route enforces:

- `X-Tadka-Client` must be `mobile-app` or `partner-web` (`400` Problem Details if missing/unknown). This is a local demo caller marker, not cryptographic authentication.
- `X-Correlation-Id` is propagated to `Tadka.OrderApi` (or generated if missing) and echoed in the response header.
- Internal diagnostic routes (`/api/orders/{id}/naive-join-receipt`) are blocked with `403 Forbidden`.

| Route | Caller rule | Behaviour |
|---|---|---|
| `GET /api/catalog/restaurants/{id}/menu` | Any valid client | Gateway pass-through to `OrderApi`. Forwards `X-Demo-Catalog-Failure` in `Demo`. |
| `POST /api/orders/checkout` | Any valid client | Gateway pass-through to `OrderApi`, preserving `Idempotency-Key` and `Idempotency-Replayed`. |
| `PATCH /api/orders/{id}/status` | `partner-web` only (`403` for `mobile-app`) | Edge caller check before forwarding status mutation. |
| `GET /bff/mobile/orders/{id}/tracking` | Any valid client | Aggregates receipt, status and restaurant name into a compact card (`itemCount`, `restaurantName`, `catalogDegraded`). Query `mode=resilient` (default) degrades Catalog failure to `200` with `restaurantName: null`; `mode=strict` returns `502`. |
| `GET /bff/partner/orders/{id}` | `partner-web` only (`403` for `mobile-app`) | Aggregates full itemized order receipt, customer ID, status and restaurant metadata. |

## What the assertions prove

`scripts/verify-gateway-bff.mjs` verifies:

1. Static check: `src/Tadka.GatewayBff` contains no `Npgsql` package reference or SQL queries.
2. Edge policy: missing `X-Tadka-Client` returns `400`; internal `naive-join-receipt` returns `403`.
3. Role policy: `mobile-app` calling `PATCH /api/orders/{id}/status` or `GET /bff/partner/orders/{id}` receives `403`, while `partner-web` succeeds (`200`).
4. Correlation and idempotency: `POST /api/orders/checkout` echoes custom `X-Correlation-Id` and replays safely on retry.
5. Client shaping: mobile tracking returns compact `itemCount` without itemized rows; partner order returns full `items`.
6. Negative control vs resilient degradation: under `X-Demo-Catalog-Failure: true`, `mode=strict` fails with `502`, whereas `mode=resilient` returns `200` with `totalAmount`, `status`, `restaurantName: null` and `catalogDegraded: true`.

## Limits and cleanup

`Upstream:OrderApiBaseUrl` is static (`http://localhost:5080`) in `appsettings.json`. Dynamic service registration, health-based routing and graceful connection draining are covered on the board in Lesson 11 Video 2 and implemented in later lessons.

Stop both APIs with Ctrl+C and remove only this lab's database:

```text
docker compose -p tadka-lesson11 down
```
