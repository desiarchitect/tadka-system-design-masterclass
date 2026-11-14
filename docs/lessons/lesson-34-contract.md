# Lesson 34 & 35: Dual-write reproduction, Transactional Outbox & Orchestrated Saga

This branch inherits Lesson 32 and adds PostgreSQL `outbox_events` (indexed cleanly by partial index `idx_outbox_unpublished WHERE published_at IS NULL`) and `saga_executions`, proving why direct DB+Kafka writes diverge on failure, how an in-transaction outbox + `FOR UPDATE SKIP LOCKED` relay solves dual writes with at-least-once delivery + idempotent sinks, and how orchestrated backward compensations recover partial multi-step workflows safely.

## Setup

.NET 10 SDK, Docker (PostgreSQL 16 + Redis 7.4 + Apache Kafka 3.8.0) and Node 22+ are required.

```text
docker compose -p tadka-lesson34 up -d --wait
dotnet run -c Release --project src/Tadka.OrderApi --launch-profile demo
```

In a second terminal, run the verification suite:

```text
node scripts/verify-outbox-saga.mjs
```

All commands work in bash and PowerShell.

## Outbox & Saga lab endpoints

| Route | Purpose |
|---|---|
| `POST /diagnostics/outbox/reset` | Clears lab tables/orders, restores `catalog.flash_deals` item `101` (`remaining_stock = 5`), and recreates topic `tadka.order.outbox`. |
| `POST /diagnostics/outbox/dual-write?mode=db-first-broker-fails\|broker-first-db-fails\|transactional-outbox` | Reproduces ghost orders (`orderInDatabase = true, eventInKafka = false`), phantom events (`orderInDatabase = false, eventInKafka = true`), and atomic DB+Outbox persistence (`outboxRowInDatabase = true, outboxPublished = false`). |
| `POST /diagnostics/outbox/relay?simulateCrashAfterPublishBeforeMark=false\|true` | Polls unpublished rows with `FOR UPDATE SKIP LOCKED`, publishes to `tadka.order.outbox`, simulates relay crash before `published_at = NOW()` when `true`, and verifies downstream consumer idempotency (`uniqueDownstreamEffects = 1` across `2` Kafka deliveries). |
| `POST /diagnostics/saga/execute?scenario=happy-path\|kitchen-rejected-uncompensated\|kitchen-rejected-compensated` | Demonstrates flash-deal reserve -> payment capture -> kitchen accept (`remainingStock = 4`), uncompensated kitchen rejection (`remainingStock = 4` leaked + unrefunded), and idempotent reverse compensation (`remainingStock = 5` even after compensation replay). |

## Limits and cleanup

The outbox relay runs via explicit endpoint invocation rather than a continuous background poller so relay timing and post-publish crashes stay deterministic under camera control. Production teams can pair either this polling relay or Debezium WAL log-tailing CDC (covered conceptually on the board) with the same idempotent downstream sinks.

```text
docker compose -p tadka-lesson34 down -v
```
