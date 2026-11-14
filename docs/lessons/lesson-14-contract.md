# Lesson 14: concurrency, MVCC and locking

This branch inherits Lesson 12 (indexing lab, 25,000-row orders seed, Catalog/Orders boundaries, idempotency, cursor pages and SSE replay) and adds `catalog.flash_deals` inside `Modules/Catalog` to inspect PostgreSQL MVCC tuple updates (`xmin`, `xmax`, `ctid`), reproduce a read-modify-write Lost Update, and compare Optimistic Concurrency Control (`version` CAS) against Pessimistic Locking (`SELECT ... FOR UPDATE`).

## Setup

.NET 10 SDK, Docker with PostgreSQL 16 and Node 22+ are required. Stop any previous lab containers or API processes first.

```text
docker compose -p tadka-lesson14 up -d --wait
dotnet run -c Release --project src/Tadka.OrderApi --launch-profile demo
```

In a second terminal:

```text
node scripts/verify-api-contracts.mjs
node scripts/verify-realtime.mjs
node scripts/verify-modular-boundaries.mjs
node scripts/verify-indexing.mjs
node scripts/verify-concurrency-mvcc.mjs
```

All commands work in bash and PowerShell.

## Endpoints (`Modules/Catalog`)

| Method & route | Purpose |
|---|---|
| `POST /catalog/deals/{menuItemId}/reset` | Resets `remaining_stock` (default `5`) and sets `version = 1`. |
| `GET /catalog/deals/{menuItemId}/mvcc-inspect` | Returns `remainingStock`, `version`, and PostgreSQL tuple fields `xmin`, `xmax`, `ctid`, plus table `liveTuples` and `deadTuples`. |
| `POST /catalog/deals/{menuItemId}/claim?mode=unsafe|optimistic|pessimistic` | Claims 1 unit under the chosen concurrency strategy. `unsafe` and `optimistic` apply a disclosed 25 ms concurrency alignment delay between read and write. `pessimistic` locks the row with `SELECT ... FOR UPDATE`. |

## What the assertions prove

`scripts/verify-concurrency-mvcc.mjs` verifies:

1. MVCC tuple versioning: claiming 1 unit updates PostgreSQL physical tuple pointer `ctid` and transaction ID `xmin` on `catalog.flash_deals`.
2. Negative control (`mode=unsafe`): 10 concurrent claims on `remainingStock = 5` reproduce a Lost Update (`claimedCount > 5` or `5 - finalRemainingStock < claimedCount`).
3. Optimistic Concurrency Control (`mode=optimistic`): 10 concurrent claims on `remainingStock = 5` never oversell (`claimedCount + finalRemainingStock === 5`), lose gracefully with `409` (`code === 'optimistic_version_conflict'`), and bounded client retries cleanly claim all 5 remaining units (`finalRemainingStock === 0`, total winners `5`).
4. Pessimistic Locking (`mode=pessimistic`): 10 concurrent claims on `remainingStock = 5` serialize on `FOR UPDATE` inside PostgreSQL, yielding 5 winners (`200`), 5 `out_of_stock` (`409`), and `finalRemainingStock === 0` in one pass.

## Limits and cleanup

`unsafe` and `optimistic` include a disclosed 25 ms read-to-write delay (`concurrencyWindowAppliedMs: 25`) so loopback HTTP callers overlap deterministically. In production, network round trips, GC pauses and business checks create this window naturally.

Stop the API with Ctrl+C and clean up this lab's database:

```text
docker compose -p tadka-lesson14 down -v
```
