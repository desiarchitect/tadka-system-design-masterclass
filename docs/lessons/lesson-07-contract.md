# Lesson 07: idempotency and pagination

This branch evolves Lesson 01. It removes the artificial `pg_sleep` from the create path. The inherited Lesson 01 traffic runs and timing explanations are not the contract for this branch. No broker, cache, ORM or authentication is added.

## Setup

.NET 10 SDK, Docker with PostgreSQL 16, Node 22+ for assertions. Use a fresh database for this branch's schema. Run only one lesson API/database at a time.

```text
docker compose -p tadka-lesson07 up -d --wait
dotnet run -c Release --project src/Tadka.OrderApi --launch-profile demo
```

In another terminal:

```text
node scripts/verify-api-contracts.mjs
```

Both command blocks work in bash and PowerShell. Use `BASE_URL` to override the test target if needed. The supplied profile binds the API to localhost:5080. SQL is loopback-only on port 5433. `GET /health` checks the process, not database readiness.

## API contract

`POST /orders` accepts `{customerId,restaurantId,totalAmount}`. IDs must be positive; amount must fit NUMERIC(10,2) without rounding. A valid create returns `201`, `{id}`, and `Location: /orders/{id}`. GET that URL returns the persisted order, or `404`. Errors use Problem Details.

The optional `Idempotency-Key` header is 1..128 ASCII letters, digits or hyphens. No header is intentionally unsafe, for the negative control. With a key, identity is `(customer_id, key)` and payload equality compares restaurant and amount numerically. A matching replay returns the same status, resource ID and Location, plus `Idempotency-Replayed: true`. Different payload returns `409`. Invalid input returns `400`. A database outage returns `503`; a lost connection means outcome is unknown, not necessarily failed.

One PostgreSQL row stores both order and key. The unique constraint serializes conflicting inserts; `INSERT ... ON CONFLICT DO NOTHING`, followed by a fresh READ COMMITTED statement, reads the committed winner. Commit precedes the HTTP response. No in-memory key map or check-then-insert race is used. Cancelling before commit can roll back; a cancellation after commit cannot undo it. Retry using the same key.

In the `Demo` environment only, `X-Demo-Drop-Response: true` aborts a first response **after commit**. It is controlled fault injection, not a network emulator. Replay does not drop the response again.

`GET /orders?customerId=1&limit=2` returns `{items,nextCursor}`. Default ordering is unique immutable `id DESC`, not timestamp/commit order. The cursor is an inspectable `customerId:lastId` boundary. The next request sends it unchanged as `cursor`. This is not a security token. Changed customer scope, invalid cursor, limit outside 1..100 or mixed pagination modes return `400`.

For comparison: `pagination=offset&offset=2`. A concurrent new high-ID row shifts offsets but does not shift a cursor's boundary. Cursor pages are not a database snapshot. An earlier allocated ID committed late can appear on a later page; deletion removes rows. Stable export snapshots and query-plan performance belong to later lessons. The supporting `(customer_id,id DESC)` index exists, but this lab makes no timing claim.

## What the assertions prove

They first reproduce duplicate unprotected creates and offset drift, then verify matching concurrent retries, payload conflict, customer separation, validation, a response lost after commit, a resolvable Location and non-overlapping cursor traversal.

Stop and restart **only the API**, not PostgreSQL, then repeat `demo-order-1` from `orders.http`: same ID and replay header should remain. This is the restart check. `docker compose down` is a data reset, not an API restart.

## Limits and cleanup

No authentication: customerId is supplied by the caller. This is loopback teaching code, not an access-control design. Production binds scope to authenticated identity, hashes/normalizes the full semantic payload, defines key retention, protects stored sensitive data and coordinates any external effects separately. This branch retains keys for the life of each order, does not delete orders and does not promise exactly-once payments. Cursor format would normally be opaque and integrity-protected if tampering matters.

Stop API with Ctrl+C. `docker compose -p tadka-lesson07 down` removes this disposable database; no named volume is configured. Do not stop or reset another lesson's project.

## References

- https://www.rfc-editor.org/rfc/rfc9110#section-9.2.2
- https://www.postgresql.org/docs/16/sql-insert.html
- https://www.postgresql.org/docs/16/transaction-iso.html
- https://www.postgresql.org/docs/16/queries-limit.html
