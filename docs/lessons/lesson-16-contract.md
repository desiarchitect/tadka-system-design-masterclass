# Lesson 16: Read Replicas, Streaming Replication & Replication Lag

Branch: `lesson-16-read-replicas`

## Infrastructure
- `postgres`: PostgreSQL 16 Primary on `127.0.0.1:5433`, running with `wal_level=replica`, `max_wal_senders=10`, `max_replication_slots=10`, `hot_standby=on`.
- `postgres-replica`: PostgreSQL 16 Hot Standby initialized with `pg_basebackup -Fp -Xs -P -R` on `127.0.0.1:5434`.

## Endpoints
- `GET /diagnostics/replication/status`: Reports Primary `pg_current_wal_lsn()`, `pg_stat_replication` peers, Replica `pg_is_in_recovery()`, WAL receive/replay LSN, replay pause state, and replay byte lag (`pg_wal_lsn_diff`).
- `POST /diagnostics/replication/replay` (`{ paused: true | false }`): Pauses or resumes WAL replay on the replica via `pg_wal_replay_pause()` / `pg_wal_replay_resume()`.
- `POST /diagnostics/replication/readonly-check`: Attempts an `INSERT` directly on the replica connection, asserting PostgreSQL SQLSTATE `25006` (`read_only_sql_transaction`).
- `POST /orders/with-lsn`: Inserts an order on Primary and returns post-commit `commitLsn` (also in `X-Commit-Lsn` header).
- `PATCH /orders/{id}/status-with-lsn`: Updates order status on Primary and returns updated row + `commitLsn` (`X-Commit-Lsn` header).
- `GET /orders/{id}/read-your-writes?routing=replica-only|ryow` (plus `minLsn` or `X-Min-Lsn`):
  - `routing=replica-only`: Routes blindly to the replica, exposing stale reads or missing rows while WAL replay lags behind `minLsn`.
  - `routing=ryow`: Compares Replica `pg_last_wal_replay_lsn()` against `minLsn`; routes to `replica` when caught up or falls back to `primary-fallback` when lagging, then returns to `replica` once caught up.
