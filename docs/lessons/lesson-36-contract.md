# Lesson 36: Capstone UPI payment gateway, ledger & webhook reconciliation

This branch inherits Lesson 34 and implements a complete end-to-end payment integrity layer on `Tadka.OrderApi`: idempotent payment-intent creation (`201 Created`, `200 REPLAYED`, `409` payload mismatch), constant-time HMAC-SHA256 webhook verification (`401` on tampered payload/signature), monotonic row-locked payment state machine (`PENDING -> CAPTURED | FAILED | EXPIRED`), balanced integer-paise 4-leg double-entry ledger (`SUM(signed_amount) = 0`), atomic `orders.status` + `outbox_events` capture commit, and server-side PSP status inquiry reconciliation for dropped webhooks.

## Setup

.NET 10 SDK, Docker (PostgreSQL 16 + Redis 7.4 + Apache Kafka 3.8.0) and Node 22+ are required.

```text
docker compose -p tadka-lesson36 up -d --wait
dotnet run -c Release --project src/Tadka.OrderApi --launch-profile demo
```

In a second terminal, run the verification suite:

```text
node scripts/verify-upi-payment-workflow.mjs
```

All commands work in bash and PowerShell.

## UPI Payment workflow endpoints

| Route | Purpose |
|---|---|
| `POST /payments/upi/reset` | Resets `payment_webhook_events`, `ledger_entries`, `payment_intents`, `evt-upi-*` outbox rows, and demo orders `>= 95000`. |
| `POST /payments/upi/intents` (`Idempotency-Key` header) | Creates `PENDING` intent (`201 Created`), replays matching request hash (`200 REPLAYED`), or rejects mutated payload/amount (`409 IDEMPOTENCY_PAYLOAD_MISMATCH`). |
| `POST /payments/upi/sign-webhook` | Demo helper returning exact UTF-8 `rawBody` and HMAC-SHA256 `X-Tadka-Signature`. |
| `POST /payments/upi/webhooks?mode=safe\|naive` | Constant-time HMAC verification (`401` on invalid signature). `naive` double-posts 4+4=8 ledger entries on duplicate delivery; `safe` uses `FOR UPDATE` + monotonic state checks (`TRANSITION_APPLIED`, `DUPLICATE_IGNORED`, `TERMINAL_STATE_GUARDED`), writes 4 balanced ledger entries (`+45000`, `-43938`, `-900`, `-162`, net `0`), marks `orders.status = 'paid'`, and inserts a `PaymentCaptured` `outbox_events` row atomically. |
| `POST /payments/upi/reconcile` | Queries `PENDING` intents missing webhooks and applies the exact same transactional capture + balanced ledger + outbox commit via PSP status inquiry recovery. |
| `GET /payments/upi/intents/{intentRef}/audit` | Returns full intent state, order status, ledger lines, debit/credit/net sums (`netSignedSum = 0`), outbox events, and webhook history. |

## Limits and cleanup

PSP status inquiry responses during `/payments/upi/reconcile` are simulated locally (`pspOutcome = CAPTURED | FAILED`) rather than calling a live bank switch, and the webhook signing helper runs inside the demo API so students can reproduce HMAC tamper rejection locally without configuring an external merchant dashboard. Ledger values use signed integer `BIGINT` paise exclusively (no floating-point currency math).

```text
docker compose -p tadka-lesson36 down -v
```
