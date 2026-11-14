import assert from 'node:assert/strict';
import { randomInt, randomUUID } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

const base = process.env.BASE_URL ?? 'http://localhost:5080';

// 1. Static module boundary check: no production Orders/Data code may query catalog.* tables.
function collectCsFiles(dir) {
  const results = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory() && entry.name !== 'bin' && entry.name !== 'obj') {
      results.push(...collectCsFiles(full));
    } else if (entry.isFile() && entry.name.endsWith('.cs')) {
      results.push(full);
    }
  }
  return results;
}

const apiRoot = path.resolve('src/Tadka.OrderApi');
const csFiles = collectCsFiles(apiRoot);
const forbiddenCatalogSql = /\bcatalog\.(restaurants|menu_items)\b/i;
for (const file of csFiles) {
  const rel = path.relative(apiRoot, file).replaceAll('\\', '/');
  if (rel.startsWith('Modules/Catalog/')) continue;
  const content = fs.readFileSync(file, 'utf8');
  if (rel === 'Modules/Orders/OrderCheckoutService.cs') {
    const stripped = content.replace(
      /public async Task<NaiveJoinReceiptResponse\?> GetNaiveJoinReceiptAsync[\s\S]*?(?=private async Task<CheckoutResult\?> TryFindExistingAsync)/,
      '',
    );
    assert.equal(forbiddenCatalogSql.test(stripped), false, `${rel} outside GetNaiveJoinReceiptAsync must not query catalog.*`);
  } else {
    assert.equal(forbiddenCatalogSql.test(content), false, `${rel} must not query catalog.* directly`);
  }
}

async function patchItemPrice(restaurantId, itemId, price) {
  const res = await fetch(`${base}/catalog/restaurants/${restaurantId}/items/${itemId}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ price }),
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(res.status, 200);
  return res.json();
}

// Ensure baseline price 240.00 before test starts.
await patchItemPrice(7, 101, 240.00);

const customerId = randomInt(1, 2 ** 47);
const key = randomUUID();

try {
  // 2. Inspect menu and place checkout order (2 x Paneer Tikka @ 240.00 = 480.00).
  const menuRes = await fetch(`${base}/catalog/restaurants/7/menu`, { signal: AbortSignal.timeout(10000) });
  assert.equal(menuRes.status, 200);
  const menu = await menuRes.json();
  assert.equal(menu.restaurantName, 'Tadka Indiranagar');
  assert.equal(menu.isOpen, true);

  const checkoutRes = await fetch(`${base}/orders/checkout`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
    body: JSON.stringify({ customerId, restaurantId: 7, items: [{ menuItemId: 101, quantity: 2 }] }),
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(checkoutRes.status, 201);
  assert.equal(checkoutRes.headers.get('idempotency-replayed'), 'false');
  const created = await checkoutRes.json();
  assert.equal(created.totalAmount, 480);

  // 3. Mutate Catalog price to 290.00.
  const updatedItem = await patchItemPrice(7, 101, 290.00);
  assert.equal(updatedItem.price, 290);

  // 4. Negative control: naive cross-schema JOIN drifts to 580.00.
  const naiveRes = await fetch(`${base}/orders/${created.id}/naive-join-receipt`, { signal: AbortSignal.timeout(10000) });
  assert.equal(naiveRes.status, 200);
  const naive = await naiveRes.json();
  assert.equal(naive.committedOrderTotal, 480);
  assert.equal(naive.liveJoinedTotal, 580);
  assert.equal(naive.mismatchWithCommittedTotal, true);

  // 5. Proper module ownership: receipt uses Orders snapshot (480.00, unitPrice 240.00).
  const receiptRes = await fetch(`${base}/orders/${created.id}/receipt`, { signal: AbortSignal.timeout(10000) });
  assert.equal(receiptRes.status, 200);
  const receipt = await receiptRes.json();
  assert.equal(receipt.totalAmount, 480);
  assert.equal(receipt.items.length, 1);
  assert.equal(receipt.items[0].unitPrice, 240);
  assert.equal(receipt.items[0].lineTotal, 480);

  // 6. Retry with same Idempotency-Key after price change must still replay original committed order (480.00).
  const replayRes = await fetch(`${base}/orders/checkout`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
    body: JSON.stringify({ customerId, restaurantId: 7, items: [{ menuItemId: 101, quantity: 2 }] }),
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(replayRes.status, 201);
  assert.equal(replayRes.headers.get('idempotency-replayed'), 'true');
  const replayed = await replayRes.json();
  assert.equal(replayed.id, created.id);
  assert.equal(replayed.totalAmount, 480);

  // 7. Catalog invariant checks through module boundary.
  const unavailable = await fetch(`${base}/orders/checkout`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ customerId, restaurantId: 7, items: [{ menuItemId: 103, quantity: 1 }] }),
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(unavailable.status, 409);

  const closedRestaurant = await fetch(`${base}/orders/checkout`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ customerId, restaurantId: 8, items: [{ menuItemId: 201, quantity: 1 }] }),
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(closedRestaurant.status, 409);

  const wrongRestaurantItem = await fetch(`${base}/orders/checkout`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ customerId, restaurantId: 7, items: [{ menuItemId: 201, quantity: 1 }] }),
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(wrongRestaurantItem.status, 409);

  const missingRestaurant = await fetch(`${base}/orders/checkout`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ customerId, restaurantId: 999, items: [{ menuItemId: 101, quantity: 1 }] }),
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(missingRestaurant.status, 404);

  // 8. Concurrent checkout idempotency + conflict on mismatched items.
  const concurrentKey = `${key}-concurrent`;
  const concurrent = await Promise.all(
    Array.from({ length: 8 }, () =>
      fetch(`${base}/orders/checkout`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Idempotency-Key': concurrentKey },
        body: JSON.stringify({ customerId, restaurantId: 7, items: [{ menuItemId: 102, quantity: 3 }] }),
        signal: AbortSignal.timeout(10000),
      }),
    ),
  );
  const concurrentIds = new Set();
  for (const res of concurrent) {
    assert.equal(res.status, 201);
    concurrentIds.add((await res.json()).id);
  }
  assert.equal(concurrentIds.size, 1);

  const conflictRes = await fetch(`${base}/orders/checkout`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': concurrentKey },
    body: JSON.stringify({ customerId, restaurantId: 7, items: [{ menuItemId: 102, quantity: 4 }] }),
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(conflictRes.status, 409);

  console.log(`Assertions passed: static boundary check, snapshot vs naive-join drift (480 vs 580), catalog invariants, replay after price change, and concurrent checkout idempotency. orderId=${created.id}`);
} finally {
  // Always restore Paneer Tikka price to 240.00 so subsequent runs stay clean.
  await patchItemPrice(7, 101, 240.00);
}
