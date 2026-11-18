# Lesson 32: Reliable messaging, idempotent consumers and dead-letter queues

This branch inherits Lesson 31 and adds idempotent Kafka consumer deduplication backed by PostgreSQL (`processed_consumer_events` + `consumer_loyalty_ledger`) alongside bounded retry with exponential backoff and routing of unprocessable poison messages onto `tadka.order.reliable.dlq` (`consumer_dlq_records`).

## Setup

.NET 10 SDK, Docker (PostgreSQL 16 + Redis 7.4 + Apache Kafka 3.8.0) and Node 22+ are required.

```text
docker compose -p tadka-lesson32 up -d --wait
dotnet run -c Release --project src/Tadka.OrderApi --launch-profile demo
```

In a second terminal, run the verification suite (after starting `Tadka.GatewayBff` if running the inherited Gateway suites):

```text
node scripts/verify-api-contracts.mjs
node scripts/verify-realtime.mjs
node scripts/verify-modular-boundaries.mjs
node scripts/verify-caching-strategies.mjs
node scripts/verify-cache-stampede.mjs
node scripts/verify-http-edge-caching.mjs
node scripts/verify-kafka-events.mjs
node scripts/verify-reliable-consumers.mjs
```

All commands work in bash and PowerShell.

## Reliable consumer lab endpoints

| Route | Purpose |
|---|---|
| `POST /diagnostics/kafka/reliable/reset` | Truncates `consumer_loyalty_ledger`, `processed_consumer_events`, `consumer_dlq_records` and recreates `tadka.order.reliable` + `tadka.order.reliable.dlq`. |
| `POST /diagnostics/kafka/reliable/duplicate-delivery?mode=naive\|idempotent&customerId=501&orderId=9001&points=50&deliveries=3` | Publishes 3 duplicate loyalty events (`eventId = evt-loyalty-order-9001`). `naive` credits `150` points (`3` credits); `idempotent` executes transactional `INSERT ... ON CONFLICT DO NOTHING` and credits `50` points (`1` credit, `2` suppressed). |
| `POST /diagnostics/kafka/reliable/poison-pipeline?policy=block-on-poison\|retry-then-dlq&maxAttempts=3` | Publishes `[evt-valid-1, evt-poison-2, evt-valid-3, evt-valid-4]`. `block-on-poison` halts behind the poison message leaving `2` valid events blocked; `retry-then-dlq` makes `3` attempts with `2` waits between them (`backoffScheduleMs`: `100, 200 ms`), moves `evt-poison-2` to `tadka.order.reliable.dlq` with failure headers + `consumer_dlq_records`, and processes all `3` valid events (`601, 603, 604`). |

## Limits and cleanup

The 1-partition layout for `tadka.order.reliable` isolates strict partition head-of-line blocking deterministically on a single broker. Retry timing (waits of `100 ms` then `200 ms` between three attempts; each extra attempt doubles the next wait) is compressed for fast classroom execution; production consumers typically pair longer retries or retry topics with alerting on DLQ growth.

```text
docker compose -p tadka-lesson32 down -v
```
