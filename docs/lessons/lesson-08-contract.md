# Lesson 08: live order status

This branch inherits Lesson 07, including idempotency and pages. It adds a status workflow to the same API and PostgreSQL. There is no broker, browser UI, WebSocket implementation or hosted background job.

## Run

Use .NET 10, Docker and Node 22+. Stop the previous lesson API/database first. Use a fresh database for the new schema.

```text
docker compose -p tadka-lesson08 up -d --wait
dotnet run -c Release --project src/Tadka.OrderApi --launch-profile demo
```

In another terminal:

```text
node scripts/verify-api-contracts.mjs
node scripts/verify-realtime.mjs
```

Commands work in bash and PowerShell. No database connection is held while a subscriber waits.

## Endpoints

Create an order with the inherited `POST /orders`. New orders have status `placed`, version 0.

| Endpoint | Contract |
|---|---|
| `GET /orders/{id}/status` | `{orderId,status,version}` or `404` |
| `PATCH /orders/{id}/status` | Body `{"status":"preparing"}`. Then `ready`, then `delivered`. Unknown status `400`, skipped/backward transition `409`, missing order `404`. Repeating current status returns it without a new event. |
| `GET /orders/{id}/events` | UTF-8 SSE. No cursor: current snapshot. `Last-Event-ID: N`: ordered persisted events after version N for that order. Bad/negative cursor `400`; future cursor `409`; unknown order `404`. |

Example in PowerShell (`curl.exe`) or bash (`curl`):

```text
curl.exe -N http://localhost:5080/orders/ACTUAL_ID/events
curl.exe -X PATCH -H "Content-Type: application/json" -d "{\"status\":\"preparing\"}" http://localhost:5080/orders/ACTUAL_ID/status
curl.exe -N -H "Last-Event-ID: 1" http://localhost:5080/orders/ACTUAL_ID/events
```

Shell quoting differs. Prefer the `.http` requests or PowerShell `Invoke-RestMethod -Body '{"status":"preparing"}'` to avoid native-argument quoting differences. Replace ACTUAL_ID with the returned ID. The Node runner avoids shell-specific JSON quoting entirely.

## Mechanism

A status change locks the order row, updates its status/version and appends the same version to `order_status_events` in one PostgreSQL transaction. Versions are per order. The stream queries a bounded batch of at most 100 events after its cursor every 500 ms, then releases the connection before waiting. It writes named `order-status` events with `id`, JSON `data` and a blank line, then flushes. Initial snapshots use event name `snapshot`.

The initial stream sends `retry: 1000`. Comments every 10 seconds are heartbeat traffic, not business events or delivery acknowledgements. Cache-Control is `no-cache, no-transform`; `X-Accel-Buffering: no` is an advisory proxy hint, not a guarantee. A proxy may still buffer or time out.

Disconnect cancels pending queries/delays and logs `sse_disconnected`. Database/transport failures after headers are logged and abort the stream, not replaced with a fake success frame. Before headers, database failures use the inherited `503` Problem Details handler.

## Assertions and limits

The runner checks initial state, live events, two updates while disconnected, ordered replay with a cursor, and the negative control: reconnecting without a cursor only returns the current snapshot. It also checks duplicate status, invalid IDs/cursors and rejected backward transitions.

Repeat with PostgreSQL kept running and only the API restarted: a saved order ID and Last-Event-ID should replay its retained rows. Reconnecting is not proof of exactly-once UI handling. A real client applies only versions newer than its last applied version.

All history is retained until this lab database is reset. A production design needs retention limits and a defined reset/snapshot response for an expired cursor. IDs belong to one order's stream; do not reuse them for another order. Order deletion is not supported. No auth is implemented: IDs are not access-control tokens. Production must authorize both status reads and mutations before exposing this endpoint.

This is **server-side database polling behind SSE**, not a scalable fan-out service. Idle subscriber count grows the database query rate. Flushing does not prove a browser rendered the event, and this lab does not reproduce mobile networks, proxy fleets or WebSocket scaling.

Stop API with Ctrl+C. `docker compose -p tadka-lesson08 down` removes this disposable database only.

## References

- https://html.spec.whatwg.org/multipage/server-sent-events.html
- https://www.postgresql.org/docs/16/explicit-locking.html
