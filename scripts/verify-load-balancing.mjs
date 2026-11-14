import assert from 'node:assert/strict';

const nodeABase = process.env.NODE_A_URL ?? 'http://127.0.0.1:5080';
const nodeBBase = process.env.NODE_B_URL ?? 'http://127.0.0.1:5082';
const gwBase = process.env.GW_URL ?? 'http://127.0.0.1:5081';

async function setNodeState(baseUrl, body) {
  const res = await fetch(`${baseUrl}/diagnostics/instance/state`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(res.status, 200);
  return res.json();
}

async function configureLb(body) {
  const res = await fetch(`${gwBase}/diagnostics/lb/config`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(res.status, 200);
  return res.json();
}

async function probeLb() {
  const res = await fetch(`${gwBase}/diagnostics/lb/probe`, {
    method: 'POST',
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(res.status, 200);
  return res.json();
}

async function lbMenuRequest() {
  const res = await fetch(`${gwBase}/api/lb/catalog/restaurants/7/menu`, {
    headers: { 'X-Tadka-Client': 'mobile-app' },
    signal: AbortSignal.timeout(10000),
  });
  await res.text();
  return {
    status: res.status,
    instance: res.headers.get('x-upstream-instance'),
    algorithm: res.headers.get('x-lb-algorithm'),
  };
}

try {
  // Baseline: both nodes healthy, fast, counters clean
  await setNodeState(nodeABase, { state: 'healthy', slowMs: 0, resetCounters: true });
  await setNodeState(nodeBBase, { state: 'healthy', slowMs: 0, resetCounters: true });
  await configureLb({ algorithm: 'round-robin', healthCheckEnabled: true, resetStats: true });
  const initialStatus = await probeLb();
  assert.equal(initialStatus.nodes.filter(n => n.healthy).length, 2, 'expected both node-a and node-b healthy');

  // 1. Round-Robin: 8 sequential requests split evenly 4 and 4
  const rrCounts = { 'node-a': 0, 'node-b': 0 };
  for (let i = 0; i < 8; i++) {
    const r = await lbMenuRequest();
    assert.equal(r.status, 200);
    assert.equal(r.algorithm, 'round-robin');
    rrCounts[r.instance] = (rrCounts[r.instance] ?? 0) + 1;
  }
  assert.equal(rrCounts['node-a'], 4, `expected 4 requests on node-a, got ${JSON.stringify(rrCounts)}`);
  assert.equal(rrCounts['node-b'], 4, `expected 4 requests on node-b, got ${JSON.stringify(rrCounts)}`);

  // 2. Least-Requests: node-b slows down (150 ms), node-a stays fast (0 ms)
  await setNodeState(nodeBBase, { slowMs: 150 });
  await configureLb({ algorithm: 'least-requests', healthCheckEnabled: true, resetStats: true });

  const lrCounts = { 'node-a': 0, 'node-b': 0 };
  async function runWorker(iterations) {
    for (let i = 0; i < iterations; i++) {
      const r = await lbMenuRequest();
      assert.equal(r.status, 200);
      assert.equal(r.algorithm, 'least-requests');
      lrCounts[r.instance] = (lrCounts[r.instance] ?? 0) + 1;
    }
  }
  await Promise.all([runWorker(6), runWorker(6), runWorker(6)]);
  assert.ok(
    lrCounts['node-a'] > lrCounts['node-b'],
    `least-requests must favor fast node-a over slow node-b, got ${JSON.stringify(lrCounts)}`,
  );

  // 3. Negative control: unprobed unhealthy node causes client-visible failures (502/503)
  await setNodeState(nodeBBase, { state: 'unhealthy', slowMs: 0 });
  await configureLb({ algorithm: 'round-robin', healthCheckEnabled: false, resetStats: true });
  let failedRequests = 0;
  for (let i = 0; i < 6; i++) {
    const r = await lbMenuRequest();
    if (r.status === 502 || r.status === 503) failedRequests++;
  }
  assert.ok(failedRequests >= 2, `negative control expected >=2 failures without health checks, got ${failedRequests}`);

  // 4. Active health check isolates unhealthy node-b and restores 100% 200 OK via node-a
  await configureLb({ algorithm: 'round-robin', healthCheckEnabled: true, resetStats: true });
  const afterProbe = await probeLb();
  const nodeBStatus = afterProbe.nodes.find(n => n.instanceId === 'node-b');
  assert.equal(nodeBStatus?.healthy, false, 'node-b must be marked unhealthy after /health/ready returns 503');

  for (let i = 0; i < 6; i++) {
    const r = await lbMenuRequest();
    assert.equal(r.status, 200);
    assert.equal(r.instance, 'node-a');
  }

  // 5. Graceful draining: stop new traffic via /health/ready (503) while in-flight request completes 200 OK
  await setNodeState(nodeBBase, { state: 'healthy', slowMs: 0 });
  await configureLb({ algorithm: 'round-robin', healthCheckEnabled: true, resetStats: true });
  await probeLb();

  const inFlightPromise = fetch(`${nodeBBase}/diagnostics/instance/in-flight-demo?workMs=400`, {
    signal: AbortSignal.timeout(10000),
  });

  await new Promise(resolve => setTimeout(resolve, 80));
  await setNodeState(nodeBBase, { state: 'draining' });

  const readyRes = await fetch(`${nodeBBase}/health/ready`, { signal: AbortSignal.timeout(5000) });
  assert.equal(readyRes.status, 503, 'draining node /health/ready must return 503');
  const liveRes = await fetch(`${nodeBBase}/health/live`, { signal: AbortSignal.timeout(5000) });
  assert.equal(liveRes.status, 200, 'draining node /health/live must stay 200');

  await probeLb();
  for (let i = 0; i < 4; i++) {
    const r = await lbMenuRequest();
    assert.equal(r.status, 200);
    assert.equal(r.instance, 'node-a', 'new traffic must avoid draining node-b');
  }

  const inFlightRes = await inFlightPromise;
  assert.equal(inFlightRes.status, 200, 'in-flight request on draining node-b must complete 200 OK');
  const inFlightJson = await inFlightRes.json();
  assert.equal(inFlightJson.instanceId, 'node-b');
  assert.equal(inFlightJson.stateAtCompletion, 'draining');

  console.log(
    `Assertions passed: round-robin (4/4), least-requests (${lrCounts['node-a']} vs ${lrCounts['node-b']}), unprobed failures (${failedRequests}), active probe isolation, and graceful draining completion.`,
  );
} finally {
  await setNodeState(nodeABase, { state: 'healthy', slowMs: 0, resetCounters: true }).catch(() => {});
  await setNodeState(nodeBBase, { state: 'healthy', slowMs: 0, resetCounters: true }).catch(() => {});
  await configureLb({ algorithm: 'round-robin', healthCheckEnabled: true, resetStats: true }).catch(() => {});
  await probeLb().catch(() => {});
}