import assert from 'node:assert/strict';

const base = process.env.BASE_URL ?? 'http://127.0.0.1:5080';

async function configResilience(body) {
  const res = await fetch(`${base}/diagnostics/resilience/downstream-config`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(res.status, 200);
  return res.json();
}

async function getResilienceStatus() {
  const res = await fetch(`${base}/diagnostics/resilience/status`, {
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(res.status, 200);
  return res.json();
}

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

try {
  // 1. Deadline propagation: negative control wastes work vs enforced budget aborts cleanly
  await configResilience({ mode: 'healthy', delayMs: 0, resetCounters: true });

  const zombieRes = await fetch(`${base}/resilience/orders/quote-with-deadline`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      callerBudgetMs: 200,
      gatewaySpentMs: 120,
      downstreamDelayMs: 250,
      enforceDeadline: false,
    }),
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(zombieRes.status, 200);
  const zombie = await zombieRes.json();
  assert.equal(zombie.deadlineExceeded, true);
  assert.ok(zombie.wastedWorkMs > 0, `negative control must show wasted work past 80ms budget, got ${JSON.stringify(zombie)}`);

  const enforcedRes = await fetch(`${base}/resilience/orders/quote-with-deadline`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      callerBudgetMs: 200,
      gatewaySpentMs: 120,
      downstreamDelayMs: 250,
      enforceDeadline: true,
    }),
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(enforcedRes.status, 504);
  const enforced = await enforcedRes.json();
  assert.equal(enforced.reason, 'deadline_exceeded');
  assert.equal(enforced.wastedWorkMs, 0);
  assert.ok(enforced.actualWorkMs < 200, `enforced deadline must abort near 80ms, got ${enforced.actualWorkMs}ms`);

  const expiredRes = await fetch(`${base}/resilience/orders/quote-with-deadline`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Deadline-Remaining-Ms': '0',
    },
    body: JSON.stringify({
      callerBudgetMs: 200,
      gatewaySpentMs: 200,
      downstreamDelayMs: 250,
      enforceDeadline: true,
    }),
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(expiredRes.status, 504);
  const expired = await expiredRes.json();
  assert.equal(expired.actualWorkMs, 0, 'expired budget must reject immediately without downstream call');

  // 2. Retry amplification (9x) vs Retry budget + Full Jitter
  await configResilience({
    mode: 'failing',
    delayMs: 0,
    perTryTimeoutMs: 200,
    overallDeadlineMs: 1200,
    maxRetries: 2,
    retryBudgetPercent: 20,
    resetCounters: true,
  });

  for (let i = 0; i < 10; i++) {
    const r = await fetch(`${base}/resilience/orders/dispatch-probe`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ mode: 'naive-amplified', hops: 2, perHopAttempts: 3 }),
      signal: AbortSignal.timeout(10000),
    });
    assert.equal(r.status, 502);
    const body = await r.json();
    assert.equal(body.attemptsThisCall, 9);
  }
  const naiveStatus = await getResilienceStatus();
  assert.equal(naiveStatus.downstreamAttempts, 90, '10 calls x 2 hops x 3 attempts must produce 90 downstream attempts');

  await configResilience({
    mode: 'failing',
    delayMs: 0,
    perTryTimeoutMs: 200,
    overallDeadlineMs: 1200,
    maxRetries: 2,
    retryBudgetPercent: 20,
    resetCounters: true,
  });

  for (let i = 0; i < 10; i++) {
    const r = await fetch(`${base}/resilience/orders/dispatch-probe`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ mode: 'resilient', baseBackoffMs: 5, capBackoffMs: 20 }),
      signal: AbortSignal.timeout(10000),
    });
    assert.equal(r.status, 503);
  }
  const resilientStatus = await getResilienceStatus();
  assert.ok(
    resilientStatus.retriesBlockedByBudget >= 10,
    `retry budget must block excess retries, got ${JSON.stringify(resilientStatus)}`,
  );
  assert.ok(
    resilientStatus.downstreamAttempts < 20,
    `resilient attempts must stay bounded (<20 vs naive 90), got ${resilientStatus.downstreamAttempts}`,
  );

  // 3. Full Jitter backoff distribution check
  const jitterRes = await fetch(`${base}/diagnostics/resilience/jitter-sample?baseMs=50&capMs=1000&attempt=3&samples=12`, {
    method: 'POST',
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(jitterRes.status, 200);
  const jitter = await jitterRes.json();
  assert.equal(jitter.fixedSpreadMs, 0);
  assert.ok(jitter.fullJitterSpreadMs > 0);
  assert.ok(jitter.distinctFullJitterValues >= 4);

  // 4. Circuit Breaker state transitions: closed -> open -> half-open -> open -> half-open -> closed
  await configResilience({
    mode: 'failing',
    delayMs: 0,
    perTryTimeoutMs: 200,
    circuitBreakerFailureThreshold: 3,
    circuitBreakerOpenMs: 280,
    resetCounters: true,
  });

  for (let i = 0; i < 3; i++) {
    const r = await fetch(`${base}/resilience/orders/protected-call`, {
      method: 'POST',
      signal: AbortSignal.timeout(10000),
    });
    assert.equal(r.status, 503);
    const body = await r.json();
    assert.equal(body.downstreamCalled, true);
    if (i < 2) {
      assert.equal(body.circuitState, 'closed');
    } else {
      assert.equal(body.circuitState, 'open', '3rd failure must trip circuit breaker to open');
    }
  }

  for (let i = 0; i < 5; i++) {
    const r = await fetch(`${base}/resilience/orders/protected-call`, {
      method: 'POST',
      signal: AbortSignal.timeout(10000),
    });
    assert.equal(r.status, 503);
    assert.equal(r.headers.get('x-circuit-state'), 'open');
    const body = await r.json();
    assert.equal(body.fastFailed, true);
    assert.equal(body.downstreamCalled, false);
  }

  const openStatus = await getResilienceStatus();
  assert.equal(openStatus.downstreamAttempts, 3, 'open circuit must stop downstream attempts completely');
  assert.equal(openStatus.circuitFastFails, 5);

  // Wait past open duration while downstream is still failing -> half-open probe fails and re-opens
  await sleep(340);
  const failedProbeRes = await fetch(`${base}/resilience/orders/protected-call`, {
    method: 'POST',
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(failedProbeRes.status, 503);
  const failedProbe = await failedProbeRes.json();
  assert.equal(failedProbe.previousCircuitState, 'half-open');
  assert.equal(failedProbe.circuitState, 'open');
  assert.equal(failedProbe.downstreamCalled, true);

  // Recover downstream, wait past open duration -> half-open probe succeeds and closes breaker
  await configResilience({ mode: 'healthy', delayMs: 0 });
  await sleep(340);
  const recoveredProbeRes = await fetch(`${base}/resilience/orders/protected-call`, {
    method: 'POST',
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(recoveredProbeRes.status, 200);
  const recoveredProbe = await recoveredProbeRes.json();
  assert.equal(recoveredProbe.previousCircuitState, 'half-open');
  assert.equal(recoveredProbe.circuitState, 'closed');
  assert.equal(recoveredProbe.ok, true);

  const steadyRes = await fetch(`${base}/resilience/orders/protected-call`, {
    method: 'POST',
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(steadyRes.status, 200);
  assert.equal(steadyRes.headers.get('x-circuit-state'), 'closed');

  console.log(
    `Assertions passed: deadline propagation (zombie ${zombie.wastedWorkMs}ms vs 504 abort), retry amplification (90 -> ${resilientStatus.downstreamAttempts} attempts with ${resilientStatus.retriesBlockedByBudget} blocked), full jitter spread (${jitter.fullJitterSpreadMs}ms), and circuit breaker closed->open->half-open->closed lifecycle.`,
  );
} finally {
  await configResilience({ mode: 'healthy', delayMs: 0, resetCounters: true }).catch(() => {});
}
