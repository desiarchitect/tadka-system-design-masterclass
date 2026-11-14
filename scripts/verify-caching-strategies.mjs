import assert from 'node:assert/strict';

const base = process.env.BASE_URL ?? 'http://localhost:5080';

async function resetCache() {
  const res = await fetch(`${base}/diagnostics/cache/reset`, {
    method: 'POST',
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(res.status, 200);
}

async function getStats(restaurantId = 7) {
  const res = await fetch(`${base}/diagnostics/cache/stats?restaurantId=${restaurantId}`, {
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(res.status, 200);
  return res.json();
}

async function getMenu(restaurantId = 7, query = '') {
  const res = await fetch(`${base}/catalog/restaurants/${restaurantId}/menu${query}`, {
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(res.status, 200);
  return {
    xCache: res.headers.get('x-cache'),
    xCacheKey: res.headers.get('x-cache-key'),
    xCacheTtl: res.headers.get('x-cache-ttl-seconds'),
    body: await res.json(),
  };
}

async function patchItemPrice(price, invalidation) {
  const query = invalidation ? `?invalidation=${invalidation}` : '';
  const res = await fetch(`${base}/catalog/restaurants/7/items/101${query}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ price }),
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(res.status, 200);
  return {
    invalidationHeader: res.headers.get('x-cache-invalidation'),
    body: await res.json(),
  };
}

function itemPrice(menu, itemId = 101) {
  const item = menu.items.find(i => i.id === itemId);
  assert.ok(item, `menu item ${itemId} must exist`);
  return item.price;
}

try {
  // 1. Reset and verify cold read -> MISS (dbQueries = 1)
  await resetCache();
  const cold = await getMenu(7);
  assert.equal(cold.xCache, 'MISS');
  assert.equal(cold.xCacheKey, 'tadka:menu:7');
  assert.equal(itemPrice(cold.body), 240);

  let stats = await getStats(7);
  assert.equal(stats.dbQueries, 1);
  assert.equal(stats.cacheMisses, 1);
  assert.equal(stats.cacheHits, 0);
  assert.equal(stats.keyExists, true);

  // 2. Three subsequent reads -> HIT (dbQueries stays 1, cacheHits = 3)
  for (let i = 0; i < 3; i++) {
    const hit = await getMenu(7);
    assert.equal(hit.xCache, 'HIT');
    assert.equal(itemPrice(hit.body), 240);
  }
  stats = await getStats(7);
  assert.equal(stats.dbQueries, 1);
  assert.equal(stats.cacheHits, 3);
  assert.ok(stats.ttlRemainingSeconds > 0 && stats.ttlRemainingSeconds <= 30);

  // 3. Negative control: invalidation=none leaves stale 240.00 in Redis while DB has 290.00
  const skipped = await patchItemPrice(290.00, 'none');
  assert.equal(skipped.invalidationHeader, 'skipped');
  assert.equal(skipped.body.price, 290);

  const staleRead = await getMenu(7);
  assert.equal(staleRead.xCache, 'HIT');
  assert.equal(itemPrice(staleRead.body), 240, 'skipped invalidation serves stale price from Redis');

  stats = await getStats(7);
  assert.equal(itemPrice(stats.cachedMenu), 240);
  assert.equal(itemPrice(stats.dbMenu), 290);

  // 4. Cache-Aside invalidation (delete): wipes key, next read MISSES with fresh 290.00, then HITS
  const deleted = await patchItemPrice(290.00, 'delete');
  assert.equal(deleted.invalidationHeader, 'deleted');
  stats = await getStats(7);
  assert.equal(stats.keyExists, false);
  assert.equal(stats.invalidations, 1);

  const afterInvalidateMiss = await getMenu(7);
  assert.equal(afterInvalidateMiss.xCache, 'MISS');
  assert.equal(itemPrice(afterInvalidateMiss.body), 290);

  const afterInvalidateHit = await getMenu(7);
  assert.equal(afterInvalidateHit.xCache, 'HIT');
  assert.equal(itemPrice(afterInvalidateHit.body), 290);

  // 5. Write-Through: updates DB and Redis together, so the very next GET is HIT with 260.00
  const writeThrough = await patchItemPrice(260.00, 'write-through');
  assert.equal(writeThrough.invalidationHeader, 'write-through');

  stats = await getStats(7);
  assert.equal(stats.keyExists, true);
  assert.equal(stats.writeThroughUpdates, 1);
  assert.equal(itemPrice(stats.cachedMenu), 260);
  assert.equal(itemPrice(stats.dbMenu), 260);

  const immediateHit = await getMenu(7);
  assert.equal(immediateHit.xCache, 'HIT');
  assert.equal(itemPrice(immediateHit.body), 260);

  console.log('Assertions passed: cache-aside MISS->HIT, negative control stale hit, delete invalidation, and write-through immediate warm.');
} finally {
  await resetCache();
}
