# Lesson 45: Tadka v1.0 integrated production architecture

This branch unifies Module 5 (`lesson-36-upi-payment-workflow`) and Module 6 (`lesson-40-observability`) into a single runnable Tadka v1.0 production stack (`Modules/TadkaV1/` alongside `Modules/Catalog`, `Orders`, `Messaging`, `OutboxSaga`, `Payments`, `Resilience`, `OverloadProtection`, `Observability`, `Replication`, `Sharding`, `Indexing`, and `Tadka.GatewayBff`).

## Setup

.NET 10 SDK, Docker (PostgreSQL 16 + Redis 7.4 + Apache Kafka 3.8 KRaft) and Node 22+ are required. Stop any earlier lesson containers or API processes first.

```text
docker compose -p tadka-lesson45 up -d --wait
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

In a fourth terminal, run all 16 cumulative verification scripts:

```text
node scripts/verify-api-contracts.mjs
node scripts/verify-realtime.mjs
node scripts/verify-modular-boundaries.mjs
node scripts/verify-gateway-bff.mjs
node scripts/verify-caching-strategies.mjs
node scripts/verify-cache-stampede.mjs
node scripts/verify-http-edge-caching.mjs
node scripts/verify-load-balancing.mjs
node scripts/verify-kafka-events.mjs
node scripts/verify-reliable-consumers.mjs
node scripts/verify-outbox-saga.mjs
node scripts/verify-upi-payment-workflow.mjs
node scripts/verify-resilience.mjs
node scripts/verify-overload-protection.mjs
node scripts/verify-observability.mjs
node scripts/verify-tadka-v1-production.mjs
```

All commands work in bash and PowerShell.

## Endpoints

| Method & route | Purpose |
|---|---|
| `GET /api/v1/tadka/architecture` (GatewayBff `:5081`, requires `X-Tadka-Client: mobile-app`) | Returns live health probes for PostgreSQL, Redis, Kafka, pool size, all 7 Tadka architectural evolution stages, and live `v1` order/outbox/ledger counters. |
| `POST /api/v1/tadka/orders/checkout` (GatewayBff `:5081`, requires `X-Tadka-Client`, optional `Idempotency-Key` & `traceparent`) | End-to-end production checkout: load shedding + rate limit + caller deadline guard -> Redis Single-Flight catalog lookup -> atomic PG transaction (idempotency check `201`/`200`/`409` + order + item snapshot + status + payment intent + 4-leg zero-sum ledger + outbox row) -> Kafka `tadka.order.outbox` publish -> idempotent loyalty consumer -> 5-span W3C trace tree & PII-masked structured log. |
| `GET /v1/tadka/architecture`, `POST /v1/tadka/orders/checkout` (OrderApi `:5080`) | Direct upstream endpoints called by GatewayBff and local drills. |
| `POST /diagnostics/v1/chaos-drill` (`mode=unprotected` vs `mode=defended`) | Side-by-side empirical drill across 4 controls: 15-way cold-cache stampede, duplicate webhook delivery on payment intent, expired caller deadline, and 90% simulated load priority shedding. |
| `POST /diagnostics/v1/outbox-flush` | Flushes pending `evt-v1-*` outbox rows to Kafka and runs 2x consumer deliveries per event to prove `processed_consumer_events` suppresses duplicates. |
| `POST /diagnostics/v1/reset` | Clears only `v1` lab/drill orders, intents, ledger rows, outbox events, consumer dedup rows, and cache/overload state without touching baseline seed data. |

## What the assertions prove

`scripts/verify-tadka-v1-production.mjs` verifies:

1. **Unified architecture manifest:** Gateway BFF rejects missing `X-Tadka-Client` (`400`) and exposes `GET /api/v1/tadka/architecture` with healthy PostgreSQL, Redis, and Kafka plus all `7` stages.
2. **Idempotent end-to-end checkout (`201` -> `200` -> `409`):** first call with an `Idempotency-Key` returns `201` (`Idempotency-Status: CREATED`), writes `4` zero-sum ledger entries (`netSignedSum === 0`), publishes the transactional outbox event to Kafka, credits loyalty once, and masks raw card/KYC digits in the structured log; exact replay returns `200` (`Idempotency-Status: REPLAYED`) with identical `orderId` and `intentRef`; altered payload with the same key returns `409 Conflict` (`IDEMPOTENCY_PAYLOAD_MISMATCH`).
3. **5-span W3C trace tree:** `GET /diagnostics/observability/traces/{traceId}` returns `5` linked spans (`gateway.v1.post_checkout` -> `orderapi.v1.checkout` -> `redis.v1.menu.singleflight`, `postgres.v1.tx.checkout`, `kafka.v1.outbox.publish`) plus a PII-masked structured log.
4. **Deferred outbox & duplicate consumer suppression:** `pauseOutboxRelay=true` leaves the event pending in PostgreSQL (`outboxPublished === false`); calling `/diagnostics/v1/outbox-flush` publishes to Kafka and deliberately consumes twice, confirming `newlyCreditedConsumers >= 1`, `duplicateConsumersSuppressed === newlyCreditedConsumers`, and `pendingRemaining === 0`.
5. **Negative control vs defended stack (`chaos-drill`):** `mode=unprotected` reproduces `15` DB queries on cold stampede, `2x` double-debit (`8` ledger rows, `doubleDebited: true`), non-zero wasted DB work after expired caller deadline (`wastedWorkMs >= 40`), and `0` low-priority requests shed at `90%` load; `mode=defended` collapses the stampede to `1` DB query (`14` coalesced), keeps `ledgerRowCount === 4` with `netSignedSum === 0`, fast-fails at `wastedWorkMs === 0`, and sheds all `5` low-priority calls while admitting `100%` of `3` critical checkouts.
6. **Edge overload & deadline guards on live checkout route:** low-priority shedding returns `503`, expired `callerBudgetMs < gatewaySpentMs` fast-fails with `504`, and `/diagnostics/observability/metrics` confirms `rejectedHighCardinalityTagCount === 0` after `v1` calls.

## Limits and cleanup

`chaos-drill`, `pauseOutboxRelay`, and `callerBudgetMs`/`gatewaySpentMs` simulation fields are disclosed diagnostic controls for repeatable single-host verification. Stop all API processes with Ctrl+C and remove only this lab's containers:

```text
docker compose -p tadka-lesson45 down -v
```
