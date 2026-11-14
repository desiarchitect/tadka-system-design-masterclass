# Lesson 31: Kafka partitions, consumer groups and rebalancing

This branch inherits Lesson 27 and adds a single-node Apache Kafka 3.8.0 (KRaft) broker alongside PostgreSQL 16 and Redis 7.4, demonstrating partition-key ordering, independent consumer groups, idle-consumer ceiling (`consumers > partitions`), and offset-commit behaviour during partition rebalancing.

## Setup

.NET 10 SDK, Docker (PostgreSQL 16 + Redis 7.4 + Apache Kafka 3.8.0) and Node 22+ are required. Stop any earlier lesson containers or API processes first.

```text
docker compose -p tadka-lesson31 up -d --wait
dotnet run -c Release --project src/Tadka.OrderApi --launch-profile demo
```

In a second terminal, run the verification suite:

```text
node scripts/verify-api-contracts.mjs
node scripts/verify-realtime.mjs
node scripts/verify-modular-boundaries.mjs
node scripts/verify-caching-strategies.mjs
node scripts/verify-cache-stampede.mjs
node scripts/verify-http-edge-caching.mjs
node scripts/verify-kafka-events.mjs
```

All commands work in bash and PowerShell.

## Kafka lab endpoints

| Route | Purpose |
|---|---|
| `POST /diagnostics/kafka/reset` | Recreates topic `tadka.order.events` with `4` partitions (`ReplicationFactor = 1`). |
| `POST /diagnostics/kafka/publish-lifecycle?keyStrategy=unkeyed\|order-id&orderId=9001` | Publishes 4 lifecycle events (`placed`, `preparing`, `ready`, `delivered`) across `4` partitions (`unkeyed` negative control) or hashed onto `1` partition (`order-id`). |
| `POST /diagnostics/kafka/consumer-group-layout?consumerCount=1\|2\|4\|5` | Launches real Kafka consumers in `kitchen-group` (`consumerCount` members) and `analytics-group` (`1` member), reporting broker partition assignments and delivered counts. |
| `POST /diagnostics/kafka/rebalance-simulation?commitMode=uncommitted-on-revoke\|cooperative-commit-on-revoke` | Reproduces duplicate replay (`5` processed for `4` events) when `consumer-1` revokes without committing, versus exact handoff (`4` processed, `0` duplicates) when committing in `PartitionsRevokedHandler`. |

## What the assertions prove

`scripts/verify-kafka-events.mjs` verifies:

1. **Negative control (`unkeyed`):** publishing 4 lifecycle events across partitions `0..3` yields `partitionsUsed = 4` and `singlePartitionOrdered = false`.
2. **Partition-key ordering (`order-id`):** publishing with key `order-9001` lands all 4 events on `1` partition with strictly increasing offsets `0, 1, 2, 3`.
3. **Consumer group scaling & idle limit:** with `4` partitions, `consumerCount = 2` assigns `2` partitions each, `consumerCount = 5` leaves `1` idle consumer (`activeConsumerCount = 4`, `idleConsumerCount = 1`), and independent `analytics-group` reads `100%` of the topic without competing with `kitchen-group`.
4. **Rebalance duplicate vs cooperative commit:** revoking without committing replays offset `0` (`totalProcessedEvents = 5`, `duplicateDeliveries = 1`); committing in the revocation callback gives `totalProcessedEvents = 4`, `duplicateDeliveries = 0`.

## Limits and cleanup

This lab uses a single-node Kafka KRaft broker (`ReplicationFactor = 1`, `min.insync.replicas = 1`) on `127.0.0.1:9092` for reproducible local experiments. Multi-broker ISR durability and multi-pod network partitions are covered on the board. For `unkeyed` negative control, partitions `0..3` are assigned explicitly so librdkafka's sticky batcher does not group all 4 rapid local messages onto one partition.

Stop `OrderApi` with Ctrl+C, then remove containers and volumes:

```text
docker compose -p tadka-lesson31 down -v
```
