# Lesson 27: Load balancing, active health checks and graceful draining

This branch inherits Lesson 26 and turns `Tadka.GatewayBff` (`:5081`) into an L7 HTTP load balancer over two `Tadka.OrderApi` instances (`node-a` on `:5080` and `node-b` on `:5082`), demonstrating Round-Robin, Least-Requests under slow upstream nodes, unprobed failure routing, active readiness probing (`/health/ready`), and graceful in-flight draining.

## Setup

.NET 10 SDK, Docker (PostgreSQL 16 + Redis 7.4) and Node 22+ are required. Stop any earlier lesson containers or API processes first.

```text
docker compose -p tadka-lesson27 up -d --wait
dotnet run -c Release --project src/Tadka.OrderApi --launch-profile demo
```

In a second terminal, start the second `OrderApi` node on port `5082` with instance ID `node-b`:

```text
dotnet run -c Release --project src/Tadka.OrderApi --no-launch-profile -- --urls http://127.0.0.1:5082 --environment Demo --Instance:Id=node-b
```

In a third terminal, start `Tadka.GatewayBff`:

```text
dotnet run -c Release --project src/Tadka.GatewayBff --launch-profile demo
```

In a fourth terminal, run the verification suite:

```text
node scripts/verify-api-contracts.mjs
node scripts/verify-realtime.mjs
node scripts/verify-modular-boundaries.mjs
node scripts/verify-gateway-bff.mjs
node scripts/verify-caching-strategies.mjs
node scripts/verify-cache-stampede.mjs
node scripts/verify-http-edge-caching.mjs
node scripts/verify-load-balancing.mjs
```

All commands work in bash and PowerShell.

## Load balancing & instance lifecycle endpoints

| Component & route | Purpose |
|---|---|
| `GET /health`, `GET /health/ready` (`OrderApi`) | Readiness probe: returns `200 OK` when `state=healthy`, or `503 Service Unavailable` when `draining` or `unhealthy`. |
| `GET /health/live` (`OrderApi`) | Liveness probe: returns `200 OK` when `healthy` or `draining` (so orchestrators do not kill draining pods prematurely), and `503` when `unhealthy`. |
| `POST /diagnostics/instance/state` (`OrderApi`) | Transitions `state` (`healthy\|draining\|unhealthy`), sets disclosed `slowMs` (`0..2000`), or resets request counters. |
| `GET /diagnostics/instance/in-flight-demo?workMs=350` (`OrderApi`) | Holds an active request for `workMs` milliseconds and reports `stateAtCompletion` so graceful draining is observable. |
| `POST /diagnostics/lb/config`, `POST /diagnostics/lb/probe`, `GET /diagnostics/lb/status` (`GatewayBff`) | Switches `algorithm` (`round-robin\|least-requests`), toggles `healthCheckEnabled`, runs active `/health/ready` probes, and inspects pool counters. |
| `GET /api/lb/catalog/restaurants/{id}/menu`, `GET /api/catalog/restaurants/{id}/menu` (`GatewayBff`) | Routes requests across configured `OrderApiNodes` and stamps `X-Upstream-Instance`, `X-Upstream-Url`, and `X-LB-Algorithm`. |

## What the assertions prove

`scripts/verify-load-balancing.mjs` verifies:

1. **Round-Robin:** 8 requests across healthy `node-a` and `node-b` split evenly (`4` to `node-a`, `4` to `node-b`).
2. **Least-Requests:** when `node-b` has `slowMs=150` and `node-a` has `slowMs=0`, concurrent worker traffic routes strictly more requests to fast `node-a` than slow `node-b`.
3. **Negative control:** disabling active health checks (`healthCheckEnabled=false`) while `node-b` is `unhealthy` causes gateway calls routed to `node-b` to fail with `502`/`503`.
4. **Active health checks:** enabling health checks and probing removes `node-b`; 100% of requests return `200 OK` from `node-a`.
5. **Graceful draining:** transitioning `node-b` to `draining` while `/diagnostics/instance/in-flight-demo` is mid-flight makes `/health/ready` return `503` (so LB routes new traffic 100% to `node-a`) while `/health/live` stays `200` and the in-flight request completes `200 OK` with `stateAtCompletion="draining"`.

## Limits and cleanup

`slowMs` is a disclosed synthetic delay so single-machine concurrency deterministically skews active in-flight counts between two loopback nodes. Routing state lives in memory inside one `Tadka.GatewayBff` instance (`nodes` from `Upstream:OrderApiNodes` or `ORDER_API_NODES`).

Stop both `OrderApi` instances and `GatewayBff` with Ctrl+C, then clean up containers:

```text
docker compose -p tadka-lesson27 down -v
```