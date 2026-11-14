import assert from 'node:assert/strict';

const base = process.env.BASE_URL ?? 'http://127.0.0.1:5080';
const gw = process.env.GW_URL ?? 'http://127.0.0.1:5081';

async function postJson(url, body = {}, extraHeaders = {}) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...extraHeaders },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(10000),
  });
  return { status: res.status, headers: res.headers, json: await res.json() };
}

async function getJson(url) {
  const res = await fetch(url, { signal: AbortSignal.timeout(10000) });
  return { status: res.status, headers: res.headers, json: await res.json() };
}

try {
  // 0. Reset observability state
  const resetRes = await postJson(`${base}/diagnostics/observability/reset`, {});
  assert.equal(resetRes.status, 200);

  // 1. Distributed W3C trace tree: GatewayBff -> OrderApi -> Redis -> PostgreSQL (4 spans)
  const okTrace = await postJson(`${gw}/api/observability/trace-order`, {
    customerId: 1042,
    restaurantId: 7,
    simulateFail: false,
  }, { 'X-Tadka-Client': 'mobile-app' });
  assert.equal(okTrace.status, 200, `traced gateway order failed: ${JSON.stringify(okTrace.json)}`);

  const traceId = okTrace.headers.get('x-trace-id');
  const traceparent = okTrace.headers.get('traceparent');
  assert.match(traceId ?? '', /^[0-9a-f]{32}$/, 'x-trace-id header must be 32-hex W3C TraceId');
  assert.match(traceparent ?? '', /^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/, 'traceparent header must follow W3C format');

  const viewRes = await getJson(`${base}/diagnostics/observability/traces/${traceId}`);
  assert.equal(viewRes.status, 200);
  const traceView = viewRes.json;
  assert.equal(traceView.spanCount, 4, `expected 4 correlated spans, got ${traceView.spanCount}`);

  const gwSpan = traceView.spans.find(s => s.operation === 'gateway.http.post_trace_order');
  const apiSpan = traceView.spans.find(s => s.operation === 'orderapi.checkout.process');
  const redisSpan = traceView.spans.find(s => s.operation === 'redis.cache.menu_lookup');
  const dbSpan = traceView.spans.find(s => s.operation === 'postgres.db.insert_order');

  assert.ok(gwSpan, 'missing gateway root span');
  assert.ok(apiSpan, 'missing orderapi process span');
  assert.ok(redisSpan, 'missing redis lookup span');
  assert.ok(dbSpan, 'missing postgres insert span');

  assert.equal(gwSpan.parentSpanId, '0000000000000000');
  assert.equal(apiSpan.parentSpanId, gwSpan.spanId);
  assert.equal(redisSpan.parentSpanId, apiSpan.spanId);
  assert.equal(dbSpan.parentSpanId, apiSpan.spanId);
  assert.equal(traceView.structuredLogs.length, 1);
  assert.equal(traceView.structuredLogs[0].traceId, traceId);
  assert.ok(traceView.structuredLogs[0].orderId, 'structured INFO log must preserve orderId');

  // 2. Failure trace correlation
  const failRes = await postJson(`${gw}/api/observability/trace-order`, {
    customerId: 1042,
    restaurantId: 7,
    simulateFail: true,
  }, { 'X-Tadka-Client': 'mobile-app' });
  assert.equal(failRes.status, 500);
  const failTraceId = failRes.headers.get('x-trace-id');
  assert.match(failTraceId ?? '', /^[0-9a-f]{32}$/);

  const failView = (await getJson(`${base}/diagnostics/observability/traces/${failTraceId}`)).json;
  const errorSpans = failView.spans.filter(s => s.status === 'error');
  assert.ok(errorSpans.length >= 3, `expected gateway, orderapi, and db error spans, got ${errorSpans.length}`);
  assert.ok(failView.structuredLogs.some(l => l.level === 'ERROR' && l.traceId === failTraceId));

  // 3. RED + USE metrics + high-cardinality guard across 15 unique customer/order/trace IDs
  await Promise.all(
    Array.from({ length: 15 }, (_, idx) =>
      postJson(`${base}/observability/orders/trace-checkout`, {
        customerId: 2001 + idx,
        restaurantId: 7,
        slowDbMs: idx < 4 ? 80 : 10,
      }),
    ),
  );

  const metricsRes = await getJson(`${base}/diagnostics/observability/metrics`);
  assert.equal(metricsRes.status, 200);
  const metrics = metricsRes.json;

  assert.ok(metrics.red.requestsTotal >= 17, `expected >= 17 RED requests, got ${metrics.red.requestsTotal}`);
  assert.equal(metrics.red.errorsTotal, 1);
  assert.ok(metrics.red.durationMs.p99 >= metrics.red.durationMs.p95);
  assert.ok(metrics.red.durationMs.p95 >= metrics.red.durationMs.p50);
  assert.ok(metrics.red.durationMs.p50 > 0);
  assert.ok(
    metrics.cardinalityGuard.uniqueSeriesCount <= 3,
    `bounded series count expected <= 3 despite 15+ unique orders, got ${metrics.cardinalityGuard.uniqueSeriesCount}`,
  );
  assert.ok(metrics.cardinalityGuard.rejectedHighCardinalityTagCount >= 34);
  assert.ok(!metrics.cardinalityGuard.allowedLabelKeys.includes('order_id'));
  assert.ok(!metrics.cardinalityGuard.allowedLabelKeys.includes('trace_id'));
  assert.ok(metrics.use.dbPool.maxPoolSize > 0);
  assert.ok(metrics.use.dbPool.peakActiveConnections >= 2);

  // 4. PII masking (Luhn card + 12-digit KYC) + storm log shedding
  const stormEvents = [
    { level: 'DEBUG', traceId, orderId: 'ORD-9001', message: 'Polling cache hit item 101' },
    { level: 'DEBUG', traceId, orderId: 'ORD-9002', message: 'Polling cache hit item 102' },
    { level: 'DEBUG', traceId, orderId: 'ORD-9003', message: 'Polling cache hit item 103' },
    { level: 'DEBUG', traceId, orderId: 'ORD-9004', message: 'Polling cache hit item 104' },
    { level: 'INFO', traceId, orderId: 'ORD-9005', message: 'Authorized card 4111-1111-1111-1111' },
    { level: 'INFO', traceId, orderId: 'ORD-9006', message: 'Authorized card 5500000000000004' },
    { level: 'WARN', traceId, orderId: 'ORD-9007', message: 'KYC check slow for id 234567890123' },
    { level: 'ERROR', traceId, orderId: 'ORD-9008', message: 'Checkout timeout card 4111111111111111 id 345678901234' },
  ];

  const storm1 = await postJson(`${base}/diagnostics/observability/log-storm-demo`, {
    dropDebug: true,
    sampleInfoPercent: 100,
    events: stormEvents,
  });
  assert.equal(storm1.status, 200);
  assert.equal(storm1.json.droppedByLevel.DEBUG, 4);
  assert.equal(storm1.json.droppedByLevel.WARN, 0);
  assert.equal(storm1.json.droppedByLevel.ERROR, 0);
  assert.equal(storm1.json.emittedCount, 4);
  assert.equal(storm1.json.maskedFieldCount, 5);

  const combinedMessages = storm1.json.emittedLogs.map(l => l.message).join('\n');
  assert.ok(!combinedMessages.includes('4111-1111-1111-1111'));
  assert.ok(!combinedMessages.includes('4111111111111111'));
  assert.ok(!combinedMessages.includes('5500000000000004'));
  assert.ok(!combinedMessages.includes('234567890123'));
  assert.ok(!combinedMessages.includes('345678901234'));
  assert.ok(combinedMessages.includes('1111'));
  assert.ok(combinedMessages.includes('0123'));
  assert.ok(storm1.json.emittedLogs.some(l => l.orderId === 'ORD-9008' && l.level === 'ERROR'));

  const storm2 = await postJson(`${base}/diagnostics/observability/log-storm-demo`, {
    dropDebug: true,
    sampleInfoPercent: 0,
    events: stormEvents,
  });
  assert.equal(storm2.status, 200);
  assert.equal(storm2.json.droppedByLevel.DEBUG, 4);
  assert.equal(storm2.json.droppedByLevel.INFO, 2);
  assert.equal(storm2.json.emittedCount, 2);

  console.log(
    `Assertions passed: 4-span W3C trace (${traceId.slice(0, 8)}...), error span correlation, bounded RED/USE metrics (series=${metrics.cardinalityGuard.uniqueSeriesCount}), PII masking (${storm1.json.maskedFieldCount} fields), and DEBUG/INFO log shedding.`,
  );
} catch (error) {
  console.error('verify-observability failed:', error);
  process.exit(1);
}
