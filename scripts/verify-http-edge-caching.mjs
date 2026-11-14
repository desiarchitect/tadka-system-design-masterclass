import assert from 'node:assert/strict';
import { randomInt, randomUUID } from 'node:crypto';

const apiBase = process.env.BASE_URL ?? 'http://localhost:5080';
const gwBase = process.env.GW_URL ?? 'http://localhost:5081';

async function resetAll() {
  const r1 = await fetch(`${apiBase}/diagnostics/cache/reset`, {
    method: 'POST',
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(r1.status, 200);
  const r2 = await fetch(`${gwBase}/diagnostics/edge-cache/reset`, {
    method: 'POST',
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(r2.status, 200);
}

async function patchItem(price) {
  const res = await fetch(`${apiBase}/catalog/restaurants/7/items/101`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ price }),
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(res.status, 200);
  return res.json();
}

try {
  await resetAll();

  // 1. Origin GET returns Cache-Control public + strong ETag + JSON body
  const firstRes = await fetch(`${apiBase}/catalog/restaurants/7/menu`, {
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(firstRes.status, 200);
  const cc = firstRes.headers.get('cache-control');
  const etag1 = firstRes.headers.get('etag');
  const firstBody = await firstRes.text();
  assert.match(cc ?? '', /public/i);
  assert.match(cc ?? '', /max-age=15/i);
  assert.match(cc ?? '', /stale-while-revalidate=30/i);
  assert.ok(etag1 && etag1.startsWith('"menu-7-v'), `expected strong ETag, got ${etag1}`);
  assert.ok(firstBody.length > 100);

  // 2. Conditional GET with matching If-None-Match returns 304 Not Modified and 0 body bytes
  const notModRes = await fetch(`${apiBase}/catalog/restaurants/7/menu`, {
    headers: { 'If-None-Match': etag1 },
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(notModRes.status, 304);
  const notModBytes = await notModRes.text();
  assert.equal(notModBytes.length, 0, '304 Not Modified must have empty body');
  assert.equal(notModRes.headers.get('etag'), etag1);

  // 3. Mutate item 101 to 290.00 -> stale ETag returns 200 OK with new ETag and updated price
  await patchItem(290.00);
  const staleRes = await fetch(`${apiBase}/catalog/restaurants/7/menu`, {
    headers: { 'If-None-Match': etag1 },
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(staleRes.status, 200);
  const etag2 = staleRes.headers.get('etag');
  assert.notEqual(etag2, etag1, 'ETag must change after menu mutation');
  const staleJson = await staleRes.json();
  assert.equal(staleJson.items.find(i => i.id === 101).price, 290);

  // 4a. Gateway pass-through forwards If-None-Match and returns 304
  const gw304 = await fetch(`${gwBase}/api/catalog/restaurants/7/menu`, {
    headers: {
      'X-Tadka-Client': 'mobile-app',
      'If-None-Match': etag2,
    },
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(gw304.status, 304);
  assert.equal((await gw304.text()).length, 0);

  // 4b. Gateway Edge cache transitions: MISS -> HIT -> REVALIDATED -> REFRESHED
  const edgeMiss = await fetch(`${gwBase}/api/edge/catalog/restaurants/7/menu`, {
    headers: { 'X-Tadka-Client': 'mobile-app' },
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(edgeMiss.status, 200);
  assert.equal(edgeMiss.headers.get('x-edge-cache'), 'MISS');

  const edgeHit = await fetch(`${gwBase}/api/edge/catalog/restaurants/7/menu`, {
    headers: { 'X-Tadka-Client': 'mobile-app' },
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(edgeHit.status, 200);
  assert.equal(edgeHit.headers.get('x-edge-cache'), 'HIT');

  const edgeReval = await fetch(`${gwBase}/api/edge/catalog/restaurants/7/menu?revalidate=true`, {
    headers: { 'X-Tadka-Client': 'mobile-app' },
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(edgeReval.status, 200);
  assert.equal(edgeReval.headers.get('x-edge-cache'), 'REVALIDATED');

  await patchItem(255.00);
  const edgeRefresh = await fetch(`${gwBase}/api/edge/catalog/restaurants/7/menu?revalidate=true`, {
    headers: { 'X-Tadka-Client': 'mobile-app' },
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(edgeRefresh.status, 200);
  assert.equal(edgeRefresh.headers.get('x-edge-cache'), 'REFRESHED');
  const refreshedJson = await edgeRefresh.json();
  assert.equal(refreshedJson.items.find(i => i.id === 101).price, 255);

  const edgeStatsRes = await fetch(`${gwBase}/diagnostics/edge-cache/stats`, {
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(edgeStatsRes.status, 200);
  const edgeStats = await edgeStatsRes.json();
  assert.equal(edgeStats.edgeMisses, 1);
  assert.equal(edgeStats.edgeHits, 1);
  assert.equal(edgeStats.edgeRevalidated, 1);
  assert.equal(edgeStats.edgeRefreshed, 1);

  // 5. Personal order receipt returns private, no-store
  const checkoutRes = await fetch(`${apiBase}/orders/checkout`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': randomUUID() },
    body: JSON.stringify({
      customerId: randomInt(1, 2 ** 47),
      restaurantId: 7,
      items: [{ menuItemId: 102, quantity: 1 }],
    }),
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(checkoutRes.status, 201);
  const created = await checkoutRes.json();
  const receiptRes = await fetch(`${apiBase}/orders/${created.id}/receipt`, {
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(receiptRes.status, 200);
  const receiptCc = receiptRes.headers.get('cache-control') ?? '';
  assert.match(receiptCc, /private/i);
  assert.match(receiptCc, /no-store/i);

  console.log('Assertions passed: public Cache-Control + strong ETag, 304 empty body, ETag invalidation on update, Gateway edge MISS->HIT->REVALIDATED->REFRESHED, and private no-store on receipts.');
} finally {
  await resetAll();
}
