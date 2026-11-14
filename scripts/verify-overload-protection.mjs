import assert from 'node:assert/strict';

const base = process.env.BASE_URL ?? 'http://127.0.0.1:5080';

async function postJson(path, body) {
  const res = await fetch(`${base}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(10000),
  });
  return { status: res.status, headers: res.headers, json: await res.json() };
}

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

try {
  // 1. Negative control: naive GET + delay + SET over-admits under concurrency
  await postJson('/diagnostics/overload/rate-limit-config', {
    capacity: 5,
    refillPerSecond: 0.1,
    mode: 'naive-non-atomic',
    raceWindowMs: 25,
    simulateRedisDown: false,
    resetState: true,
  });

  const naiveBatch = await Promise.all(
    Array.from({ length: 10 }, () =>
      postJson('/overload/rate-limit/check', { key: 'user:naive', endpoint: 'catalog-browse', cost: 1 }),
    ),
  );
  const naiveAllowed = naiveBatch.filter(r => r.status === 200).length;
  assert.ok(naiveAllowed > 5, `naive non-atomic check must leak > 5 concurrent requests, got ${naiveAllowed}`);

  // 2. Atomic Redis Lua Token Bucket: exact 5 allowed and exact 5 rejected with 429
  await postJson('/diagnostics/overload/rate-limit-config', {
    capacity: 5,
    refillPerSecond: 0.1,
    mode: 'atomic-lua',
    simulateRedisDown: false,
    resetState: true,
  });

  const luaBatch = await Promise.all(
    Array.from({ length: 10 }, () =>
      postJson('/overload/rate-limit/check', { key: 'user:lua', endpoint: 'catalog-browse', cost: 1 }),
    ),
  );
  const luaAllowed = luaBatch.filter(r => r.status === 200).length;
  const luaRejected = luaBatch.filter(r => r.status === 429);
  assert.equal(luaAllowed, 5, `atomic Lua token bucket must allow exact 5 requests, got ${luaAllowed}`);
  assert.equal(luaRejected.length, 5, `atomic Lua token bucket must reject exact 5 requests, got ${luaRejected.length}`);
  for (const r of luaRejected) {
    assert.ok(r.headers.get('retry-after'), '429 must include Retry-After header');
    assert.equal(r.headers.get('x-ratelimit-limit'), '5');
  }

  // 3. Failure policies when Redis is down
  await postJson('/diagnostics/overload/rate-limit-config', {
    simulateRedisDown: true,
    failurePolicy: 'fail-closed',
    localCapacity: 3,
    refillPerSecond: 0.1,
    resetState: true,
  });

  const closedRes = await postJson('/overload/rate-limit/check', { key: 'user:1', endpoint: 'login-otp' });
  assert.equal(closedRes.status, 503);
  assert.equal(closedRes.json.decision, 'rejected_fail_closed');
  assert.equal(closedRes.headers.get('x-ratelimit-fallback'), 'fail-closed');

  const openRes = await postJson('/overload/rate-limit/check', {
    key: 'user:1',
    endpoint: 'catalog-browse',
    failurePolicy: 'fail-open',
  });
  assert.equal(openRes.status, 200);
  assert.equal(openRes.json.decision, 'allowed_fail_open');
  assert.equal(openRes.headers.get('x-ratelimit-fallback'), 'fail-open');

  await postJson('/diagnostics/overload/rate-limit-config', {
    simulateRedisDown: true,
    failurePolicy: 'local-fallback',
    localCapacity: 3,
    refillPerSecond: 0.05,
    resetState: true,
  });
  const localBatch = [];
  for (let i = 0; i < 5; i++) {
    localBatch.push(await postJson('/overload/rate-limit/check', { key: 'user:local', endpoint: 'checkout' }));
  }
  const localAllowed = localBatch.filter(r => r.status === 200).length;
  const localRejected = localBatch.filter(r => r.status === 429).length;
  assert.equal(localAllowed, 3, 'local-fallback bucket must allow localCapacity=3 requests');
  assert.equal(localRejected, 2, 'local-fallback bucket must rate limit excess requests');

  // 4. Bulkhead isolation: shared pool starvation vs isolated semaphores
  await postJson('/diagnostics/overload/bulkhead-config', {
    isolationMode: 'shared',
    sharedMaxConcurrency: 4,
    analyticsMaxConcurrency: 2,
    checkoutMaxConcurrency: 4,
    resetCounters: true,
  });

  const sharedAnalyticsPromise = Promise.all(
    Array.from({ length: 4 }, () => postJson('/overload/bulkhead/work', { lane: 'analytics', workMs: 350 })),
  );
  await sleep(60);
  const sharedCheckouts = await Promise.all(
    Array.from({ length: 3 }, () => postJson('/overload/bulkhead/work', { lane: 'checkout', workMs: 20 })),
  );
  assert.equal(sharedCheckouts.filter(r => r.status === 200).length, 0, 'shared pool saturated by analytics must starve checkout');
  assert.ok(sharedCheckouts.every(r => r.status === 503 && r.json.reason === 'shared_pool_exhausted'));
  await sharedAnalyticsPromise;

  await postJson('/diagnostics/overload/bulkhead-config', {
    isolationMode: 'bulkhead',
    analyticsMaxConcurrency: 2,
    checkoutMaxConcurrency: 4,
    resetCounters: true,
  });

  const isolatedAnalyticsPromise = Promise.all(
    Array.from({ length: 4 }, () => postJson('/overload/bulkhead/work', { lane: 'analytics', workMs: 350 })),
  );
  await sleep(60);
  const isolatedCheckouts = await Promise.all(
    Array.from({ length: 3 }, () => postJson('/overload/bulkhead/work', { lane: 'checkout', workMs: 20 })),
  );
  const isolatedAnalytics = await isolatedAnalyticsPromise;

  assert.equal(isolatedAnalytics.filter(r => r.status === 200).length, 2, 'analytics bulkhead must admit 2');
  assert.equal(isolatedAnalytics.filter(r => r.status === 503).length, 2, 'analytics bulkhead must reject 2 excess analytics calls');
  assert.equal(isolatedCheckouts.filter(r => r.status === 200).length, 3, 'checkout bulkhead must admit 3/3 checkouts unaffected');

  // 5. Priority load shedding
  await postJson('/diagnostics/overload/shedding-config', {
    enabled: true,
    simulatedLoadPercent: 88,
    shedLowThresholdPercent: 80,
    shedNormalThresholdPercent: 95,
    resetCounters: true,
  });

  const lowShed = await postJson('/overload/shedding/request', { priority: 'low-recommendations' });
  assert.equal(lowShed.status, 503);
  assert.equal(lowShed.headers.get('x-load-shed'), 'true');
  assert.equal(lowShed.headers.get('retry-after'), '2');

  const normalOk = await postJson('/overload/shedding/request', { priority: 'normal-browse' });
  assert.equal(normalOk.status, 200);
  assert.equal(normalOk.headers.get('x-load-shed'), 'false');

  const criticalOk = await postJson('/overload/shedding/request', { priority: 'critical-checkout' });
  assert.equal(criticalOk.status, 200);

  await postJson('/diagnostics/overload/shedding-config', { simulatedLoadPercent: 97 });
  const normalShed = await postJson('/overload/shedding/request', { priority: 'normal-browse' });
  assert.equal(normalShed.status, 503);
  const criticalStillOk = await postJson('/overload/shedding/request', { priority: 'critical-checkout' });
  assert.equal(criticalStillOk.status, 200);

  console.log(
    `Assertions passed: rate limiter race (${naiveAllowed}/10 leaked) vs Lua atomic (5/5), failure policies (fail-closed/open/local-3), bulkhead isolation (0/3 shared vs 3/3 isolated checkout), and priority load shedding.`,
  );
} finally {
  await postJson('/diagnostics/overload/rate-limit-config', {
    simulateRedisDown: false,
    mode: 'atomic-lua',
    resetState: true,
  }).catch(() => {});
  await postJson('/diagnostics/overload/shedding-config', {
    simulatedLoadPercent: 0,
    resetCounters: true,
  }).catch(() => {});
}
