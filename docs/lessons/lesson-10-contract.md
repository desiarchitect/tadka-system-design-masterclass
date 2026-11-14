# Lesson 10: modular boundaries and data ownership

This branch inherits Lesson 08 (`orders`, `order_status_events`, idempotency, cursor pages and SSE replay) and splits the modular monolith into two explicit modules inside `Tadka.OrderApi`: **Catalog** and **Orders**. No separate microservice process, message broker, cache or authentication is added.

## Setup

.NET 10 SDK, Docker with PostgreSQL 16 and Node 22+ are required. Stop any earlier lesson database and API first so ports `5080` and `5433` are free. Use a fresh database for this branch's updated schema.

```text
docker compose -p tadka-lesson10 up -d --wait
dotnet run -c Release --project src/Tadka.OrderApi --launch-profile demo
```

In another terminal:

```text
node scripts/verify-api-contracts.mjs
node scripts/verify-realtime.mjs
node scripts/verify-modular-boundaries.mjs
```

All commands work in bash and PowerShell.

## Schema ownership and module boundary

| Module | PostgreSQL tables | Production owner |
|---|---|---|
| `Catalog` (`Modules/Catalog`) | `catalog.restaurants`, `catalog.menu_items` | `CatalogModule` (`ICatalogModule`) |
| `Orders` (`Modules/Orders` + legacy `Data`) | `public.orders`, `public.order_items`, `public.order_status_events` | `OrderCheckoutService`, `OrderStore`, `OrderStatusService` |

There is intentionally **no foreign key** from `order_items.menu_item_id` to `catalog.menu_items(id)`. `Orders` owns point-in-time snapshots (`item_name_snapshot`, `unit_price_snapshot`), while `Catalog` owns live restaurant availability and menu pricing.

Seeded restaurants and items in `database/init.sql`:

- Restaurant `7` (`Tadka Indiranagar`, open): item `101` (`Paneer Tikka`, `240.00`, available), item `102` (`Masala Dosa`, `120.00`, available), item `103` (`Filter Coffee`, `60.00`, unavailable).
- Restaurant `8` (`Tadka Koramangala`, closed): item `201` (`Veg Biryani`, `220.00`, available).

## Endpoints

Inherited `/orders` endpoints from Lessons 07 and 08 remain unchanged.

| Endpoint | Contract |
|---|---|
| `GET /catalog/restaurants/{id}/menu` | Returns live restaurant status and menu items (`200`), or `404`. In `Demo`, `X-Demo-Catalog-Failure: true` injects a `503` for resilience testing. |
| `PATCH /catalog/restaurants/{id}/items/{itemId}` | Updates `price` and/or `isAvailable` and increments `version` (`200`), or `404`/`400`. |
| `POST /orders/checkout` | Body `{customerId, restaurantId, items: [{menuItemId, quantity}]}` with optional `Idempotency-Key`. Calls `ICatalogModule.QuoteItemsAsync`, enforces open restaurant, item ownership and availability (`409` on violation, `404` on unknown restaurant), then commits `orders` + `order_items` snapshots in one transaction. |
| `GET /orders/{id}/receipt` | Reads only `orders` and `order_items` snapshots (`200`), or `404`. Never touches `catalog.*`. |
| `GET /orders/{id}/naive-join-receipt` | **Demo negative control only** (`404` outside `Demo`). Intentionally joins `order_items` to `catalog.menu_items` so learners can observe total drift after a catalog price update. |

## What the assertions prove

`scripts/verify-modular-boundaries.mjs` runs:

1. A static code check verifying that `catalog.` SQL queries only exist inside `Modules/Catalog/` and the single documented `GetNaiveJoinReceiptAsync` negative control.
2. Checkout of `2 x Paneer Tikka` (`101`) at `240.00` (`480.00` total), followed by a catalog price update to `290.00`.
3. Negative control: `GET /orders/{id}/naive-join-receipt` shows `liveJoinedTotal = 580.00` and `mismatchWithCommittedTotal = true`.
4. Snapshot receipt: `GET /orders/{id}/receipt` stays `totalAmount = 480.00` with `unitPrice = 240.00`.
5. Domain invariants: unavailable item `103`, closed restaurant `8`, cross-restaurant item `201` at restaurant `7` all return `409`; missing restaurant `999` returns `404`.
6. Concurrent idempotency: 8 parallel `POST /orders/checkout` calls with the same key create 1 order; reusing the key with different items returns `409`.

## Limits and cleanup

Inside a single process, `ICatalogModule` is an in-process interface rather than an RPC hop, so database failure isolation between modules is still shared-cluster unless separate schemas/users or separate databases are enforced. `POST /orders/checkout` checks quote prices before inserting; an item price updated concurrently during checkout could differ from a preview shown earlier on screen unless a quote token or price expectation is checked.

Stop the API with Ctrl+C. Remove only this lab's database with:

```text
docker compose -p tadka-lesson10 down
```
