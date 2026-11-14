# Lesson 38 & 39: distributed rate limiting, failure policies, bulkheads and load shedding

This branch inherits Lesson 37 and adds `Modules/OverloadProtection/` inside `Tadka.OrderApi` (serving both Lesson 38 Rate Limiter and Lesson 39 Bulkheads & Load Shedding) to compare non-atomic TOCTOU rate-limit races against single-script Redis Lua Token Buckets (`TIME`), Redis failure fallbacks (`fail-closed`, `fail-open`, `local-fallback`), shared semaphore pool exhaustion against isolated per-lane Bulkheads, and priority-tiered Load Shedding (`503` + `Retry-After`).

## Setup

.NET 10 SDK, Docker (PostgreSQL 16 + Redis 7.4) and Node 22+ are required. Stop any earlier lesson containers or API processes first.

```text
docker compose -p tadka-lesson38 up -d --wait
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
```

All commands work in bash and PowerShell.

## Endpoints

| Method & route | Purpose |
|---|---|
| `POST /diagnostics/overload/rate-limit-config` | Configures Token Bucket `capacity`, `refillPerSecond`, `mode` (`naive-non-atomic\|atomic-lua`), disclosed `raceWindowMs`, `simulateRedisDown`, `failurePolicy` (`fail-closed\|fail-open\|local-fallback`), and `localCapacity`. |
| `POST /overload/rate-limit/check` | Executes a rate-limit check, stamping `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`, `Retry-After` on `429`, and `X-RateLimit-Fallback` when Redis is unavailable. |
| `POST /diagnostics/overload/bulkhead-config` | Switches `isolationMode` (`shared\|bulkhead`) and concurrency caps (`sharedMaxConcurrency`, `analyticsMaxConcurrency`, `checkoutMaxConcurrency`). |
| `POST /overload/bulkhead/work` | Simulates `analytics` or `checkout` work for `workMs` inside either the shared pool or isolated lane semaphores. |
| `POST /diagnostics/overload/shedding-config`, `POST /overload/shedding/request` | Configures `simulatedLoadPercent` thresholds and enforces priority-based shedding (`critical-checkout`, `normal-browse`, `low-recommendations`), returning `503` with `X-Load-Shed: true` and `Retry-After`. |
| `GET /diagnostics/overload/status` | Returns rate-limiter, bulkhead, and load-shedding configuration and counters. |

## What the assertions prove

`scripts/verify-overload-protection.mjs` verifies:

1. **Rate limiter TOCTOU negative control vs atomic Lua:** 10 concurrent checks on `capacity=5` in `naive-non-atomic` mode admit `> 5` requests, whereas `atomic-lua` using Redis `TIME` admits exact `5` (`200 OK`) and rejects exact `5` (`429 Too Many Requests` with `Retry-After`).
2. **Failure policy matrix (`simulateRedisDown=true`):** `fail-closed` returns `503` (`rejected_fail_closed`), `fail-open` returns `200` (`allowed_fail_open`), and `local-fallback` admits up to `localCapacity=3` then returns `429` (`rejected_local_fallback`).
3. **Bulkhead isolation:** in `shared` mode (4 slots), 4 concurrent slow `analytics` calls starve 3 concurrent `checkout` calls (`0/3` succeed, `503 shared_pool_exhausted`); in `bulkhead` mode (`analyticsMaxConcurrency=2`, `checkoutMaxConcurrency=4`), 4 slow `analytics` calls reject 2 excess analytics calls while **3/3 `checkout` calls succeed**.
4. **Priority load shedding:** at `simulatedLoadPercent=88` (`shedLowThresholdPercent=80`, `shedNormalThresholdPercent=95`), `low-recommendations` is shed (`503`, `X-Load-Shed: true`), while `normal-browse` and `critical-checkout` succeed; at `97`, `normal-browse` sheds (`503`) while `critical-checkout` stays `200 OK`.

## Limits and cleanup

`raceWindowMs`, `workMs`, `simulateRedisDown`, and `simulatedLoadPercent` are disclosed synthetic controls so race leaks, pool starvation, Redis outage policies, and overload shedding execute deterministically on one host. Stop all API processes with Ctrl+C and remove only this lab's containers:

```text
docker compose -p tadka-lesson38 down -v
```
