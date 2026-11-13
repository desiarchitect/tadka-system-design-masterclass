import assert from 'node:assert/strict';
import { randomInt, randomUUID } from 'node:crypto';

const base = process.env.BASE_URL ?? 'http://localhost:5080';
const customerId = randomInt(1, 2 ** 47);
const order = { customerId, restaurantId: 7, totalAmount: 249.50 };
const prefix = randomUUID();
async function create(key, body = order, extra = {}) {
  return fetch(`${base}/orders`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...(key ? { 'Idempotency-Key': key } : {}), ...extra },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(10000),
  });
}
async function page(query = '') {
  const response = await fetch(`${base}/orders?customerId=${customerId}&limit=2${query}`, { signal: AbortSignal.timeout(10000) });
  assert.equal(response.status, 200);
  return response.json();
}

// Negative control: an unprotected retry produces two orders.
const unsafe1 = await create();
const unsafe2 = await create();
assert.equal(unsafe1.status, 201);
assert.equal(unsafe2.status, 201);
assert.notEqual((await unsafe1.json()).id, (await unsafe2.json()).id);

const responses = await Promise.all(Array.from({ length: 10 }, () => create(prefix)));
const ids = [];
for (const response of responses) {
  assert.equal(response.status, 201);
  assert.ok(response.headers.get('location'));
  ids.push((await response.json()).id);
}
assert.equal(new Set(ids).size, 1, 'concurrent matching retries must create one order');
const replay = await create(prefix);
assert.equal(replay.headers.get('idempotency-replayed'), 'true');
assert.equal((await replay.json()).id, ids[0]);
assert.equal((await create(prefix, { ...order, totalAmount: 250 })).status, 409);
const otherCustomer = await create(prefix, { ...order, customerId: customerId + 1 });
assert.equal(otherCustomer.status, 201);
assert.notEqual((await otherCustomer.json()).id, ids[0]);
assert.equal((await create('bad key')).status, 400);
assert.equal((await create(`${prefix}-precision`, { ...order, totalAmount: 1.001 })).status, 400);

const lostKey = `${prefix}-lost`;
await assert.rejects(() => create(lostKey, order, { 'X-Demo-Drop-Response': 'true' }), 'run API in Demo environment');
const recovered = await create(lostKey);
assert.equal(recovered.status, 201);
assert.equal(recovered.headers.get('idempotency-replayed'), 'true', 'the dropped response followed a real commit');
const recoveredId = (await recovered.json()).id;
const resource = await fetch(`${base}/orders/${recoveredId}`);
assert.equal(resource.status, 200);
assert.equal((await resource.json()).customerId, customerId);

await create(`${prefix}-page-a`);
await create(`${prefix}-page-b`);
const first = await page();
assert.equal(first.items.length, 2);
assert.ok(first.nextCursor);
await create(`${prefix}-page-new`);
const shifted = await page('&pagination=offset&offset=2');
assert.ok(shifted.items.some(item => item.id === first.items[1].id), 'offset negative control must repeat a boundary row');
const next = await page(`&cursor=${encodeURIComponent(first.nextCursor)}`);
assert.ok(next.items.every(item => item.id < first.items[1].id));
assert.ok(next.items.every(item => !first.items.some(old => old.id === item.id)));
assert.equal((await fetch(`${base}/orders?customerId=${customerId}&cursor=invalid`)).status, 400);
assert.equal((await fetch(`${base}/orders?customerId=${customerId + 1}&cursor=${first.nextCursor}`)).status, 400);
assert.equal((await fetch(`${base}/orders?customerId=${customerId}&limit=101`)).status, 400);
assert.equal((await fetch(`${base}/orders?customerId=${customerId}&offset=1`)).status, 400);
console.log(`Assertions passed: unsafe duplicates, concurrent replay, payload conflict, customer scope, post-commit disconnect, offset drift and cursor traversal. customerId=${customerId}`);
