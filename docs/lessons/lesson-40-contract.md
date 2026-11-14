# Lesson 40: structured logs, RED & USE metrics, and W3C distributed tracing

This branch inherits Lesson 38/39 (`lesson-38-overload-protection`) and adds W3C `traceparent` propagation plus `Modules/Observability/` across `Tadka.GatewayBff` and `Tadka.OrderApi` to trace a live request from edge gateway to downstream API, Redis cache lookup, and PostgreSQL order insert (`4` correlated spans), expose bounded-cardinality RED & USE metrics, mask PII (Luhn 13-19 card digits and 12-digit KYC IDs), and shed `DEBUG` log storms while keeping `WARN`/`ERROR`.

## Setup

.NET 10 SDK, Docker (PostgreSQL 16 + Redis 7.4) and Node 22+ are required. Stop any earlier lesson containers or API processes first.

```text
docker compose -p tadka-lesson40 up -d --wait
dotnet run -c Release --project src/Tadka.OrderApi --launch-profile demo
```

In a second terminal (for cumulative verification including load balancing):

```text
dotnet run -c Release --project src/Tadka.OrderApi --no-launch-profile -- --urls http://127.0.0.1:5082 --environment Demo --Instance:Id=node-b
```

In a third terminal:

```text
dotnet run -c Release --project src/Tadka.GatewayBff --launch-profile demo
```

In a fourth terminal, run the cumulative verification suite:

```text
node scripts/verify-api-contracts.mjs
node scripts/verify-realtime.mjs
node scripts/verify-modular-boundaries.mjs
node scripts/verify-gateway-bff.mjs
node scripts/verify-caching-strategies.mjs
node scripts/verify-cache-stampede.mjs
node scripts/verify-http-edge-caching.mjs
node scripts/verify-load-balancing.mjs
node scripts/verify-resilience.mjs
node scripts/verify-overload-protection.mjs
node scripts/verify-observability.mjs
```

All commands work in bash and PowerShell.

## Endpoints

| Method & route | Purpose |
|---|---|
| `POST /api/observability/trace-order` (GatewayBff `:5081`, requires `X-Tadka-Client: mobile-app`) | Starts root W3C `Activity` (`gateway.http.post_trace_order`), propagates `traceparent` (`00-<traceId>-<spanId>-01`) to `OrderApi`, and ingests gateway span into trace collector. |
| `POST /observability/orders/trace-checkout` (OrderApi `:5080`) | Continues parent `traceparent` context with child spans `orderapi.checkout.process`, `redis.cache.menu_lookup`, and `postgres.db.insert_order`, recording RED & USE metrics and correlated logs. |
| `GET /diagnostics/observability/traces/{traceId}` | Returns full 4-span distributed trace tree and correlated structured JSON logs for one `traceId`. |
| `GET /diagnostics/observability/metrics` | Returns RED (`requestsTotal`, `errorsTotal`, latency percentiles) + USE (`dbPool` active/peak connections, `maxPoolSize`, `saturationWaitCount`) + cardinality guard proving `order_id` and `trace_id` are excluded from metric labels. |
| `POST /diagnostics/observability/log-storm-demo` | Masks Luhn 13-19 card numbers and 12-digit `[2-9]\d{11}` Aadhaar-like numbers while keeping `orderId` unmasked, and drops `DEBUG` / samples `INFO` while preserving `WARN`/`ERROR`. |
| `POST /diagnostics/observability/reset` | Resets trace buffers, structured logs, and RED/USE counters. |

## What the assertions prove

`scripts/verify-observability.mjs` verifies:

1. **End-to-end W3C trace tree (`traceparent`):** calling `GatewayBff` produces a `32`-hex `TraceId` and `4` linked spans (`gateway.http.post_trace_order` -> `orderapi.checkout.process` -> `redis.cache.menu_lookup` & `postgres.db.insert_order`) plus an `INFO` log correlated by `traceId` and `orderId`.
2. **Error span correlation:** `simulateFail=true` returns HTTP `500`, stamps `status = "error"` on `gateway`, `orderapi.checkout.process`, and `postgres.db.insert_order` spans, and attaches an `ERROR` log with matching `traceId`.
3. **Bounded metric cardinality + RED/USE:** firing `15` unique `customerId`/`orderId`/`traceId` checkouts keeps `uniqueSeriesCount <= 3` grouped only by `(service, route, method, status_class)` while `rejectedHighCardinalityTagCount` proves `order_id`/`trace_id` never enter metric series keys.
4. **PII masking + storm shedding:** `DEBUG` logs drop under `dropDebug=true`, `WARN`/`ERROR` survive at 100%, raw Luhn card digits and 12-digit KYC numbers are masked to last 4 digits, and `orderId` remains queryable.

## Limits and cleanup

`simulateFail`, `slowDbMs`, and `log-storm-demo` events are disclosed diagnostic controls for deterministic local verification on one host. Stop all API processes with Ctrl+C and remove only this lab's containers:

```text
docker compose -p tadka-lesson40 down -v
```
