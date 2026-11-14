import assert from 'node:assert/strict';

const base = process.env.BASE_URL ?? 'http://127.0.0.1:5080';
const gw = process.env.GW_URL ?? 'http://127.0.0.1:5081';

async function postJson(url, body = {}, extraHeaders = {}) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...extraHeaders },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(15000),
  });
  return { status: res.status, headers: res.headers, json: await res.json() };
}

async function getJson(url, extraHeaders = {}) {
  const res = await fetch(url, {
    headers: extraHeaders,
    signal: AbortSignal.timeout(15000),
  });
  return { status: res.status, headers: res.headers, json: await res.json() };
}

try {
  // 0. Reset V1 lab state and check Gateway client enforcement + architecture probe
  const resetRes = await postJson(`${base}/diagnostics/v1/reset`, {});
  assert.equal(resetRes.status, 200);
  assert.equal(resetRes.json.status, 'RESET');

  const metricsBefore = await getJson(`${base}/diagnostics/observability/metrics`);
  assert.equal(metricsBefore.status, 200);
  const rejectedBefore = metricsBefore.json.cardinalityGuard.rejectedHighCardinalityTagCount;

  const noClientArch = await getJson(`${gw}/api/v1/tadka/architecture`);
  assert.equal(noClientArch.status, 400, 'GatewayBff must reject missing X-Tadka-Client on /api/v1/*');

  const archRes = await getJson(`${gw}/api/v1/tadka/architecture`, { 'X-Tadka-Client': 'mobile-app' });
  assert.equal(archRes.status, 200, `Expected healthy architecture probe, got ${JSON.stringify(archRes.json)}`);
  const arch = archRes.json.upstream;
  assert.equal(arch.allDependenciesHealthy, true, 'Postgres, Redis, and Kafka must all be healthy');
  assert.equal(arch.stages.length, 7, 'Tadka v1.0 manifest must report all 7 evolution stages');
  assert.equal(arch.infrastructure.postgres.ok, true);
  assert.equal(arch.infrastructure.redis.ok, true);
  assert.equal(arch.infrastructure.kafka.ok, true);

  // 1. End-to-end checkout (201 CREATED -> 200 REPLAYED -> 409 IDEMPOTENCY_PAYLOAD_MISMATCH)
  const idemKey = `v1-checkout-${Date.now()}`;
  const checkoutPayload = {
    customerId: 1091,
    restaurantId: 7,
    menuItemId: 101,
    quantity: 2,
    payerVpa: 'ananya.sharma@okhdfcbank',
    cardHint: '4111-1111-1111-1111',
    kycHint: '987654321012',
  };

  const first = await postJson(`${gw}/api/v1/tadka/orders/checkout`, checkoutPayload, {
    'X-Tadka-Client': 'mobile-app',
    'Idempotency-Key': idemKey,
  });
  assert.equal(first.status, 201, `Expected 201 CREATED, got ${first.status}: ${JSON.stringify(first.json)}`);
  assert.equal(first.headers.get('idempotency-status'), 'CREATED');

  const traceId = first.headers.get('x-trace-id');
  const traceparent = first.headers.get('traceparent');
  assert.match(traceId ?? '', /^[0-9a-f]{32}$/);
  assert.match(traceparent ?? '', /^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/);

  const up1 = first.json.upstream;
  assert.ok(up1.orderId > 0);
  assert.ok(up1.intentRef.startsWith('v1_intent_'));
  assert.equal(up1.amountPaise, 48000);
  assert.equal(up1.ledgerEntriesCount, 4);
  assert.equal(up1.ledgerNetSignedSum, 0);
  assert.equal(up1.outboxPublished, true);
  assert.equal(up1.loyaltyPointsCredited, 48);
  assert.ok(up1.piiMaskedLogPreview.includes('************1111'), `Expected masked card last-4, got: ${up1.piiMaskedLogPreview}`);
  assert.ok(up1.piiMaskedLogPreview.includes('********1012'), `Expected masked KYC last-4, got: ${up1.piiMaskedLogPreview}`);
  assert.ok(!up1.piiMaskedLogPreview.includes('4111-1111'), 'Raw PAN prefix must be masked');
  assert.ok(!up1.piiMaskedLogPreview.includes('98765432'), 'Raw 12-digit KYC ID must be masked');

  // Replay with identical payload & key -> 200 REPLAYED, same orderId and intentRef
  const replay = await postJson(`${gw}/api/v1/tadka/orders/checkout`, checkoutPayload, {
    'X-Tadka-Client': 'mobile-app',
    'Idempotency-Key': idemKey,
  });
  assert.equal(replay.status, 200, `Expected 200 REPLAYED, got ${replay.status}`);
  assert.equal(replay.headers.get('idempotency-status'), 'REPLAYED');
  assert.equal(replay.json.upstream.orderId, up1.orderId);
  assert.equal(replay.json.upstream.intentRef, up1.intentRef);
  assert.equal(replay.json.upstream.ledgerEntriesCount, 4);
  assert.equal(replay.json.upstream.ledgerNetSignedSum, 0);

  // Conflict: reuse same Idempotency-Key with different quantity -> 409
  const conflict = await postJson(
    `${gw}/api/v1/tadka/orders/checkout`,
    { ...checkoutPayload, quantity: 3 },
    { 'X-Tadka-Client': 'mobile-app', 'Idempotency-Key': idemKey },
  );
  assert.equal(conflict.status, 409);
  assert.equal(conflict.json.upstream.errorCode, 'IDEMPOTENCY_PAYLOAD_MISMATCH');

  // 2. Verify 5-span distributed W3C trace tree across GatewayBff -> OrderApi -> Redis -> PostgreSQL -> Kafka
  const traceRes = await getJson(`${base}/diagnostics/observability/traces/${traceId}`);
  assert.equal(traceRes.status, 200);
  const trace = traceRes.json;
  assert.equal(trace.spanCount, 5, `Expected 5 spans (gateway, orderapi, redis, postgres, kafka), got ${trace.spanCount}`);

  const gwSpan = trace.spans.find(s => s.operation === 'gateway.v1.post_checkout');
  const apiSpan = trace.spans.find(s => s.operation === 'orderapi.v1.checkout');
  const redisSpan = trace.spans.find(s => s.operation === 'redis.v1.menu.singleflight');
  const dbSpan = trace.spans.find(s => s.operation === 'postgres.v1.tx.checkout');
  const kafkaSpan = trace.spans.find(s => s.operation === 'kafka.v1.outbox.publish');
  assert.ok(gwSpan && apiSpan && redisSpan && dbSpan && kafkaSpan, 'All 5 trace spans must exist');
  assert.equal(apiSpan.parentSpanId, gwSpan.spanId);
  assert.equal(redisSpan.parentSpanId, apiSpan.spanId);
  assert.equal(dbSpan.parentSpanId, apiSpan.spanId);
  assert.equal(kafkaSpan.parentSpanId, apiSpan.spanId);
  assert.ok(trace.structuredLogs.some(l => l.traceId === traceId && l.piiMasked === true));

  // 3. Deferred outbox relay + duplicate consumer suppression via flush
  const paused = await postJson(
    `${gw}/api/v1/tadka/orders/checkout`,
    { ...checkoutPayload, customerId: 1092, quantity: 1, pauseOutboxRelay: true },
    { 'X-Tadka-Client': 'mobile-app', 'Idempotency-Key': `${idemKey}-paused` },
  );
  assert.equal(paused.status, 201);
  assert.equal(paused.json.upstream.outboxPublished, false);
  assert.equal(paused.json.upstream.loyaltyPointsCredited, 0);

  const flushRes = await postJson(`${base}/diagnostics/v1/outbox-flush`, {});
  assert.equal(flushRes.status, 200);
  assert.equal(flushRes.json.publishedCount, 1);
  assert.equal(flushRes.json.newlyCreditedConsumers, 1);
  assert.equal(flushRes.json.duplicateConsumersSuppressed, 1);
  assert.equal(flushRes.json.pendingRemaining, 0);

  // 4. Negative control vs Defended Chaos Drill
  const unprotected = await postJson(`${base}/diagnostics/v1/chaos-drill`, { mode: 'unprotected' });
  assert.equal(unprotected.status, 200);
  const u = unprotected.json;
  assert.equal(u.stampedeCheck.dbQueriesExecuted, 15, 'Unprotected cold cache must hit DB 15 times');
  assert.equal(u.webhookDoubleDebitCheck.doubleDebited, true, 'Unprotected duplicate webhook must double-debit ledger');
  assert.equal(u.webhookDoubleDebitCheck.ledgerRowCount, 8);
  assert.ok(u.deadlineWasteCheck.wastedWorkMs >= 40, `Expected wasted downstream work without deadline propagation, got ${u.deadlineWasteCheck.wastedWorkMs}`);
  assert.equal(u.loadSheddingCheck.lowPriorityShed, 0);

  const defended = await postJson(`${base}/diagnostics/v1/chaos-drill`, { mode: 'defended' });
  assert.equal(defended.status, 200);
  const d = defended.json;
  assert.equal(d.stampedeCheck.dbQueriesExecuted, 1, 'Defended single-flight must execute exactly 1 DB query');
  assert.equal(d.stampedeCheck.coalescedReads, 14, 'Defended single-flight must coalesce 14 waiters');
  assert.equal(d.webhookDoubleDebitCheck.doubleDebited, false, 'Defended FOR UPDATE guard must prevent double debit');
  assert.equal(d.webhookDoubleDebitCheck.ledgerRowCount, 4);
  assert.equal(d.webhookDoubleDebitCheck.netSignedSum, 0);
  assert.equal(d.deadlineWasteCheck.statusCode, 504);
  assert.equal(d.deadlineWasteCheck.wastedWorkMs, 0, 'Defended deadline propagation must abort before wasting DB time');
  assert.equal(d.loadSheddingCheck.lowPriorityShed, 5);
  assert.equal(d.loadSheddingCheck.criticalCheckoutAdmitted, 3);

  // 5. Edge caller deadline enforcement on live checkout route
  const expiredDeadline = await postJson(
    `${gw}/api/v1/tadka/orders/checkout`,
    { ...checkoutPayload, customerId: 1093, callerBudgetMs: 120, gatewaySpentMs: 150 },
    { 'X-Tadka-Client': 'mobile-app', 'Idempotency-Key': `${idemKey}-expired` },
  );
  assert.equal(expiredDeadline.status, 504);
  assert.equal(expiredDeadline.json.upstream.errorCode, 'DEADLINE_EXCEEDED');

  // 6. Clean reset check leaves seed baseline intact and deletes v1 rows
  const finalReset = await postJson(`${base}/diagnostics/v1/reset`, {});
  assert.equal(finalReset.status, 200);
  assert.ok(finalReset.json.deletedV1Orders >= 2);
  assert.ok(finalReset.json.deletedV1OutboxEvents >= 2);

  const metricsAfter = await getJson(`${base}/diagnostics/observability/metrics`);
  assert.equal(metricsAfter.status, 200);
  assert.equal(
    metricsAfter.json.cardinalityGuard.rejectedHighCardinalityTagCount,
    rejectedBefore,
    'V1 checkout path must never emit forbidden high-cardinality metric tags',
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        orderId: up1.orderId,
        intentRef: up1.intentRef,
        traceId,
        traceSpanCount: trace.spanCount,
        unprotectedStampedeDbQueries: u.stampedeCheck.dbQueriesExecuted,
        defendedStampedeDbQueries: d.stampedeCheck.dbQueriesExecuted,
        defendedCoalescedReads: d.stampedeCheck.coalescedReads,
        unprotectedLedgerRows: u.webhookDoubleDebitCheck.ledgerRowCount,
        defendedLedgerRows: d.webhookDoubleDebitCheck.ledgerRowCount,
        unprotectedWastedWorkMs: u.deadlineWasteCheck.wastedWorkMs,
        defendedWastedWorkMs: d.deadlineWasteCheck.wastedWorkMs,
        outboxFlushDuplicatesSuppressed: flushRes.json.duplicateConsumersSuppressed,
      },
      null,
      2,
    ),
  );
} catch (error) {
  console.error('verify-tadka-v1-production failed:', error);
  process.exitCode = 1;
}
