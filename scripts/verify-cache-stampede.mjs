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

try {
  // 1. Negative control: 20 concurrent misses in mode=naive trigger 20 PostgreSQL queries
  await resetCache();
  const naiveResponses = await Promise.all(
    Array.from({ length: 20 }, () =>
      fetch(`${base}/catalog/restaurants/7/menu-stampede?mode=naive&dbDelayMs=60`, {
        signal: AbortSignal.timeout(10000),
      }),
    ),
  );
  for (const res of naiveResponses) {
    assert.equal(res.status, 200);
    assert.equal(res.headers.get('x-stampede-role'), 'uncoordinated-miss');
  }
  let stats = await getStats(7);
  assert.equal(stats.dbQueries, 20, 'naive stampede must send all 20 callers to PostgreSQL');
  assert.equal(stats.coalescedRequests, 0);

  // 2. Single-flight fix: 20 concurrent misses trigger 1 DB query (1 leader + 19 coalesced)
  await resetCache();
  const sfResponses = await Promise.all(
    Array.from({ length: 20 }, () =>
      fetch(`${base}/catalog/restaurants/7/menu-stampede?mode=single-flight&dbDelayMs=60&baseTtlSeconds=30&jitterSeconds=6`, {
        signal: AbortSignal.timeout(10000),
      }),
    ),
  );

  let leaders = 0;
  let coalesced = 0;
  for (const res of sfResponses) {
    assert.equal(res.status, 200);
    const role = res.headers.get('x-stampede-role');
    if (role === 'leader') leaders++;
    else if (role === 'coalesced') coalesced++;
  }
  assert.equal(leaders, 1, 'single-flight must elect exactly 1 leader');
  assert.equal(coalesced, 19, 'single-flight must coalesce 19 followers');

  stats = await getStats(7);
  assert.equal(stats.dbQueries, 1, 'only 1 PostgreSQL query must execute across 20 concurrent requests');
  assert.equal(stats.coalescedRequests, 19);

  // 3. Subsequent read hits Redis directly without extra DB queries
  const afterRes = await fetch(`${base}/catalog/restaurants/7/menu-stampede?mode=single-flight&dbDelayMs=60`, {
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(afterRes.status, 200);
  assert.equal(afterRes.headers.get('x-cache'), 'HIT');
  stats = await getStats(7);
  assert.equal(stats.dbQueries, 1);

  // 4. TTL Jitter spreads expiry times compared to fixed batch TTL
  const jitterRes = await fetch(`${base}/diagnostics/cache/ttl-jitter-sample?count=12&baseTtlSeconds=30&jitterSeconds=10`, {
    method: 'POST',
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(jitterRes.status, 200);
  const jitter = await jitterRes.json();
  assert.ok(jitter.jitteredSpreadMs > jitter.fixedSpreadMs, 'jittered TTL spread must exceed fixed TTL spread');
  assert.ok(jitter.distinctJitteredSeconds >= 3, 'jittered TTLs must spread across multiple distinct seconds');

  console.log(`Assertions passed: naive stampede (20 DB queries), single-flight coalescing (1 DB query, 19 coalesced), and TTL jitter spread (${jitter.jitteredSpreadMs} ms vs fixed ${jitter.fixedSpreadMs} ms).`);
} finally {
  await resetCache();
}
