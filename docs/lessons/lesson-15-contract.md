# Lesson 15 Contract: Distributed ID Generation & B-Tree Index Locality

Branch: `lesson-15-distributed-ids`

## Endpoints
- `GET /diagnostics/ids/sample?count=5`: Generates `count` (1..50) sample IDs across UUIDv4 (`Guid.NewGuid()`), UUIDv7 (`Guid.CreateVersion7()`), and 64-bit Snowflake (`1 | 41 | 10 | 12` bits), including `idString` (lossless above JS 53-bit `Number.MAX_SAFE_INTEGER`) and decoded Snowflake components.
- `POST /diagnostics/ids/clock-rollback-check?rollbackMs=150`: Exercises the Snowflake clock-rollback guard using an isolated injectable clock, returning `blocked: true` and `code: "clock_moved_backwards"`.
- `POST /diagnostics/ids/benchmark?rows=25000`: Truncates `id_bench_uuidv4`, `id_bench_uuidv7`, and `id_bench_snowflake`, inserts `rows` (1000..50000) generated IDs (RFC 9562 Method 1 monotonic UUIDv7 vs random UUIDv4 vs Snowflake) into each, runs `VACUUM ANALYZE`, and inspects the primary-key B-tree with `pgstatindex(...)`.

## Assertions (`scripts/verify-distributed-ids.mjs`)
1. Snowflake IDs in a sample burst are strictly increasing (`id[i] > id[i - 1]`) and decode back to `nodeId = 7`.
2. Simulated backward NTP clock drift triggers `clock_moved_backwards` instead of emitting duplicate IDs.
3. On 25,000 inserted rows, random `uuidv4` shows noticeably lower `avgLeafDensityPct`, higher `leafFragmentationPct`, and more `leafPages` than time-ordered `uuidv7`.
4. 64-bit `snowflake` (`BIGINT`) keeps ~90% `avgLeafDensityPct`, near-zero fragmentation, and a smaller `indexSizeBytes` than 128-bit UUID primary keys.
