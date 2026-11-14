# Lesson 37: timeouts, deadlines, retry amplification and circuit breakers

This branch inherits Lesson 27 (`Tadka.OrderApi` + `Tadka.GatewayBff` + PostgreSQL 16 + Redis 7.4) and adds `Modules/Resilience/` inside `Tadka.OrderApi` to reproduce zombie downstream work, multi-hop retry amplification, retry-budget throttling, full-jitter backoff spread, and the Circuit Breaker state machine (`closed -> open -> half-open -> closed`).

## Setup

.NET 10 SDK, Docker (PostgreSQL 16 + Redis 7.4) and Node 22+ are required. Stop any earlier lesson containers or API processes first.

```text
docker compose -p tadka-lesson37 up -d --wait
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
```

All commands work in bash and PowerShell.

## Endpoints

| Method & route | Purpose |
|---|---|
| `POST /diagnostics/resilience/downstream-config`, `GET /diagnostics/resilience/status` | Configures downstream mode (`healthy\|slow\|failing`), disclosed `delayMs`, `perTryTimeoutMs`, `overallDeadlineMs`, `maxRetries`, `retryBudgetPercent`, `circuitBreakerFailureThreshold`, `circuitBreakerOpenMs`, and inspects counters + circuit state. |
| `POST /resilience/orders/quote-with-deadline` | Compares uncoordinated execution (`enforceDeadline=false`, causing zombie downstream work past caller budget) against deadline budget enforcement (`enforceDeadline=true`, honoring `X-Deadline-Remaining-Ms` and aborting with `504 Gateway Timeout`). |
| `POST /resilience/orders/dispatch-probe` | Compares `naive-amplified` multi-hop immediate retries (`perHopAttempts ^ hops`, e.g., `9x` downstream amplification) against `resilient` single-layer retries bounded by overall deadline, full-jitter backoff, and retry-budget tokens. |
| `POST /diagnostics/resilience/jitter-sample` | Samples fixed exponential ceiling (`spread=0`) vs Full Jitter (`random(0, min(capMs, baseMs * 2^attempt))`). |
| `POST /resilience/orders/protected-call` | Executes a call guarded by a `closed -> open -> half-open -> closed` Circuit Breaker and stamps `X-Circuit-State`. |

## What the assertions prove

`scripts/verify-resilience.mjs` verifies:

1. **Deadline propagation:** ignoring the remaining caller budget (`enforceDeadline=false`) wastes downstream work (`wastedWorkMs > 0`), whereas `enforceDeadline=true` aborts at the remaining budget (`504`, `wastedWorkMs === 0`) and immediately rejects expired budgets (`actualWorkMs === 0`).
2. **Retry amplification vs retry budget:** under `mode=failing`, 10 calls in `naive-amplified` (2 hops x 3 attempts) generate `90` downstream attempts (`9x` amplification), while `resilient` mode with `retryBudgetPercent=20` blocks excess retries (`retriesBlockedByBudget >= 10`) and keeps total downstream attempts bounded (`< 20`).
3. **Full jitter spread:** sampling attempt 3 gives `fixedSpreadMs === 0` vs `fullJitterSpreadMs > 0` across `>= 4` distinct values.
4. **Circuit breaker lifecycle:** 3 consecutive failures trip `closed -> open`, the next 5 calls fast-fail (`503`, `X-Circuit-State: open`, `downstreamCalled === false`, `0` new downstream attempts), a failed probe after `circuitBreakerOpenMs` transitions `open -> half-open -> open`, and a healthy probe transitions `half-open -> closed`.

## Limits and cleanup

`delayMs`, `downstreamDelayMs`, and `mode=failing|slow` are disclosed in-process downstream fault controls so cascading-failure mechanics run deterministically on localhost without deploying external chaos proxies. Stop all API processes with Ctrl+C and remove only this lab's containers:

```text
docker compose -p tadka-lesson37 down -v
```
