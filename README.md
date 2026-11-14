# Tadka: System Design Masterclass

The pre-built case study for the Desi Architect System Design Masterclass.
Check out a lesson branch, inspect its contract, reproduce the problem and compare
the change. This is teaching code, not a production food-ordering service.

## Available branches

Each branch carries its own instructions. Do not mix the schema or commands from
different lessons.

| Branch | What you inspect | Start here after switching |
|---|---|---|
| `lesson-01-baseline` | Order API, PostgreSQL and application-side connection-pool wait | `docs/lessons/lesson-01-contract.md` |
| `lesson-05-networking` | HTTP/1.1 and HTTP/2 negotiation, fresh versus reused connections | `docs/lessons/lesson-05-contract.md` |
| `lesson-07-api-contracts` | Database-backed idempotency, lost responses and cursor pages | `docs/lessons/lesson-07-contract.md` |
| `lesson-08-realtime-sse` | Persisted status versions, SSE framing and reconnect replay | `docs/lessons/lesson-08-contract.md` |
| `lesson-10-modular-boundaries` | Explicit Catalog/Orders module contracts, schema ownership and receipt snapshots | `docs/lessons/lesson-10-contract.md` |
| `lesson-11-api-gateway-bff` | Edge Gateway routing, caller role checks, correlation IDs and resilient BFF shaping | `docs/lessons/lesson-11-contract.md` |
| `lesson-12-database-indexing` | 25k order seed, Seq Scan vs Composite Index vs Covering Index Only Scan (`EXPLAIN (ANALYZE, BUFFERS)`) | `docs/lessons/lesson-12-contract.md` |
| `lesson-14-concurrency-mvcc` | PostgreSQL MVCC tuple inspection (`ctid`, `xmin`, `xmax`), lost update bug and Optimistic CAS vs `FOR UPDATE` | `docs/lessons/lesson-14-contract.md` |
| `lesson-15-distributed-ids` | 64-bit Snowflake with clock-rollback guard and `pgstatindex` locality (UUIDv4 vs monotonic UUIDv7 vs Snowflake) | `docs/lessons/lesson-15-contract.md` |
| `lesson-16-read-replicas` | 2-container PostgreSQL streaming replication (`5433` primary, `5434` replica), WAL replay pause lag and LSN RYOW routing | `docs/lessons/lesson-16-contract.md` |
| `lesson-18-sharding` | 4 logical shards (`shard_0..shard_3`), celebrity hot-partition skew, write salting and scatter-gather routing | `docs/lessons/lesson-18-contract.md` |
| `lesson-23-caching-strategies` | Redis 7.4 (`allkeys-lru`, `64mb`) Cache-Aside menu reads, stale bug (`invalidation=none`), delete invalidation & write-through | `docs/lessons/lesson-23-contract.md` |
| `lesson-24-cache-stampede` | 20-request cache stampede reproduction vs in-process single-flight request coalescing and TTL jitter | `docs/lessons/lesson-24-contract.md` |
| `lesson-26-http-edge-caching` | Strong SHA256 `ETag`, `If-None-Match` (`304`), `Cache-Control`, `private, no-store` receipts & gateway edge cache simulator | `docs/lessons/lesson-26-contract.md` |
| `lesson-27-load-balancing` | 2-node `OrderApi` cluster (`:5080`, `:5082`), L7 round-robin & least-requests LB, `/health/ready` vs `/health/live` & draining | `docs/lessons/lesson-27-contract.md` |
| `lesson-31-kafka-events` | Kafka 3.8 KRaft (`127.0.0.1:9092`), 4-partition `tadka.order.events`, per-key ordering, 2 vs 5 consumer group rebalance & commits | `docs/lessons/lesson-31-contract.md` |
| `lesson-32-reliable-consumers` | Transactional `processed_consumer_events` deduplication vs `3x` credit, poison-pill backoff (`100/200/400` ms) to `tadka.order.reliable.dlq` | `docs/lessons/lesson-32-contract.md` |
| `lesson-34-outbox-saga` | Dual-write failures (`Ghost Order`/`Phantom Event`), `outbox_events` (`idx_outbox_unpublished`, `FOR UPDATE SKIP LOCKED`), pre-mark crash recovery & idempotent Saga compensations | `docs/lessons/lesson-34-contract.md` |
| `lesson-36-upi-payment-workflow` | Capstone UPI intent idempotency (`201`/`200`/`409`), constant-time HMAC webhooks, `FOR UPDATE` monotonic state machine, 4-leg integer-paise double-entry ledger & `/payments/upi/reconcile` | `docs/lessons/lesson-36-contract.md` |
| `lesson-37-resilience` | Layered timeouts & deadline propagation, `k^N` retry amplification vs 10% retry budget, full-jitter backoff, and circuit breaker state machine | `docs/lessons/lesson-37-contract.md` |
| `lesson-38-overload-protection` | Atomic Redis Lua token bucket (`TIME`), fail-open/fail-closed/local-fallback policies, isolated semaphore bulkheads, and priority load shedding (Lessons 38 & 39) | `docs/lessons/lesson-38-contract.md` |
| `lesson-40-observability` | W3C `traceparent` 4-span trace tree across `GatewayBff`, `OrderApi`, Redis & PostgreSQL, bounded-cardinality RED/USE metrics, PII masking, and storm log shedding | `docs/lessons/lesson-40-contract.md` |

Lessons 02, 03, 04, 06, 09, 13, 17, 19, 20, 21, 22, 25, 28, 29, 30, 33, 41 and 42 are board or whiteboard case-study lessons and do not need separate code branches (Lesson 35 shares `lesson-34-outbox-saga`; Lesson 39 shares `lesson-38-overload-protection`).
Later module branches (Module 7+) are not published yet.

## Get a lesson

```text
git clone https://github.com/desiarchitect/tadka-system-design-masterclass.git
cd tadka-system-design-masterclass
git fetch origin
git switch lesson-05-networking
dotnet run -c Release --project src/Tadka.NetworkingLab
```

The networking probe needs .NET 10 only. It starts and stops its own loopback
HTTPS server and asserts connection reuse. It does not need PostgreSQL or Docker.
Request timings are observations from your machine, not promised benchmarks.
HTTP/3 is explained in the lesson but is not implemented in the probe.

For Lessons 07, 08, 10, 11, 12, 14, 15, 16, 18, 23, 24, 26, 27, 31, 32, 34, 36, 37, 38, 39 and 40, install .NET 10, Docker and Node 22+, switch to the branch,
then follow its contract. Run one lesson API/database at a time because they share
ports 5080 and 5433 (Lessons 11, 26, 27, 37, 38 and 40 also use port 5081 for `Tadka.GatewayBff`; Lessons 16 and 18 run `postgres-replica` on port 5434; Lessons 23, 24, 26, 27, 31, 32, 34, 36, 37, 38 and 40 run Redis on port 6379; Lessons 27, 37, 38 and 40 also run a second `OrderApi` node on port 5082; Lessons 31, 32, 34 and 36 run `apache/kafka:3.8.0` KRaft on port 9092). Stop only that lesson's Compose project before switching.
The databases and caches are disposable. API-only restart preserves PostgreSQL data; database teardown
resets it.

The setup-check command for the Start Here video is still planned. `main` is an
index, not a runnable application. Do not record that setup check as available.

## Evolution and limits

Networking and API contracts each start from the baseline. The SSE branch builds
on API contracts. Module 2 (`lesson-10-modular-boundaries` and `lesson-11-api-gateway-bff`)
builds on `lesson-08-realtime-sse` by splitting `Catalog` and `Orders` inside the modular
monolith and adding `Tadka.GatewayBff` in front of `Tadka.OrderApi`. Module 3
(`lesson-12-database-indexing` through `lesson-18-sharding`) builds cumulatively on
`lesson-11-api-gateway-bff` inside `Tadka.OrderApi`, adding B+ Tree indexing diagnostics,
MVCC/flash-deal concurrency modes, distributed ID generation, streaming replication on port 5434,
and 4-shard routing. Module 4 (`lesson-23-caching-strategies` through `lesson-27-load-balancing`)
builds forward from `lesson-18-sharding`, adding `redis:7.4-alpine` (`allkeys-lru`),
in-process single-flight and TTL jitter, HTTP strong `ETag` and `Cache-Control` semantics,
a simulated gateway edge cache, and 2-node L7 load balancing with active readiness checks
and graceful draining. Module 5 (`lesson-31-kafka-events` through `lesson-36-upi-payment-workflow`)
builds forward from `lesson-27-load-balancing`, adding single-broker Apache Kafka 3.8.0 KRaft
(`127.0.0.1:9092`), 4-partition topics and `CooperativeSticky` consumer groups, transactional SQL
deduplication and Dead Letter Queue (`tadka.order.reliable.dlq`), `outbox_events` with partial
index and `FOR UPDATE SKIP LOCKED` relay, idempotent Flash Deal Saga compensations, and an
end-to-end UPI payment workflow (`payment_intents`, constant-time HMAC-SHA256 webhooks, 4-leg
integer-paise double-entry `ledger_entries` summing to `0`, and status reconciliation). Module 6
(`lesson-37-resilience`, `lesson-38-overload-protection` and `lesson-40-observability`) builds forward
from `lesson-27-load-balancing`, adding layered deadlines and zombie-work abort, retry amplification vs
10% retry budgets and full jitter, circuit breakers, atomic Redis Lua token buckets (`TIME`) with failure
policies, semaphore bulkheads, priority load shedding, W3C `traceparent` tracing across `GatewayBff`,
`OrderApi`, Redis and PostgreSQL, bounded-cardinality RED/USE metrics, PII masking and storm log shedding.
Networking does not silently change the Order API.

Lessons 07, 08, 10, 11, 12, 14, 15, 16, 18, 23, 24, 26, 27, 31, 32, 34, 36, 37, 38 and 40 keep normal checkout/read paths free of hidden artificial sleep (Lesson 24 exposes a disclosed `dbDelayMs` parameter on `/menu-stampede` so concurrent misses overlap on localhost, Lesson 27 exposes a disclosed `slowMs` fault parameter on `/diagnostics/instance/state`, Lessons 31 to 36 expose explicit `/diagnostics/*` and `/payments/upi/reconcile` simulation parameters, and Lessons 37 to 40 expose disclosed `/diagnostics/resilience/*`, `/diagnostics/overload/*` and `/diagnostics/observability/*` fault/load controls for local failure reproduction).
Their assertion scripts first reproduce unsafe or broken behaviour, then verify the fix.
The labs have been run repeatedly from clean state; a timed recording
rehearsal is still required on the instructor's machine.

All new listeners bind to loopback. Customer IDs and `X-Tadka-Client` headers in
the labs are caller supplied; there is no production authentication or authorization.
An order ID, cursor, client marker or SSE event ID is not a security credential.
Do not expose these labs publicly.

Module 0 to 4 and Module 6 labs require PostgreSQL (and Redis in Modules 4 and 6) with no message broker; Module 5
(`lesson-31-kafka-events` through `lesson-36-upi-payment-workflow`) runs a single-broker Apache
Kafka 3.8.0 KRaft container alongside PostgreSQL and Redis via Docker Compose. No Kubernetes
cluster or browser UI is required by these Module 0 to 6 labs. Follow each branch's contract for
measurement boundaries, cleanup and what the experiment does not prove.
