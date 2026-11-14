import assert from 'node:assert/strict';
import { randomInt, randomUUID } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

const gwBase = process.env.GW_URL ?? 'http://localhost:5081';

// 1. Static check: Gateway/BFF must contain zero database drivers or SQL statements.
const gwRoot = path.resolve('src/Tadka.GatewayBff');
const csproj = fs.readFileSync(path.join(gwRoot, 'Tadka.GatewayBff.csproj'), 'utf8');
assert.equal(/Npgsql|EntityFramework|Dapper/i.test(csproj), false, 'GatewayBff must not reference database drivers');

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

for (const file of collectCsFiles(gwRoot)) {
  const content = fs.readFileSync(file, 'utf8');
  assert.equal(/\b(Npgsql|SELECT\s+.+\s+FROM|INSERT\s+INTO)\b/i.test(content), false, `${file} must not execute SQL`);
}

// 2. Missing X-Tadka-Client header -> 400
const missingClient = await fetch(`${gwBase}/api/catalog/restaurants/7/menu`, { signal: AbortSignal.timeout(10000) });
assert.equal(missingClient.status, 400);

// 3. Blocked internal diagnostic route -> 403
const blockedInternal = await fetch(`${gwBase}/api/orders/1/naive-join-receipt`, {
  headers: { 'X-Tadka-Client': 'partner-web' },
  signal: AbortSignal.timeout(10000),
});
assert.equal(blockedInternal.status, 403);

// 4. Checkout through gateway with correlation ID + idempotent retry
const customerId = randomInt(1, 2 ** 47);
const key = randomUUID();
const correlationId = `corr-${randomUUID()}`;
const checkoutBody = JSON.stringify({ customerId, restaurantId: 7, items: [{ menuItemId: 101, quantity: 2 }] });

const createRes = await fetch(`${gwBase}/api/orders/checkout`, {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'X-Tadka-Client': 'mobile-app',
    'X-Correlation-Id': correlationId,
    'Idempotency-Key': key,
  },
  body: checkoutBody,
  signal: AbortSignal.timeout(10000),
});
assert.equal(createRes.status, 201);
assert.equal(createRes.headers.get('x-correlation-id'), correlationId);
assert.equal(createRes.headers.get('idempotency-replayed'), 'false');
const created = await createRes.json();
assert.equal(created.totalAmount, 480);

const replayRes = await fetch(`${gwBase}/api/orders/checkout`, {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'X-Tadka-Client': 'mobile-app',
    'X-Correlation-Id': correlationId,
    'Idempotency-Key': key,
  },
  body: checkoutBody,
  signal: AbortSignal.timeout(10000),
});
assert.equal(replayRes.status, 201);
assert.equal(replayRes.headers.get('idempotency-replayed'), 'true');
assert.equal((await replayRes.json()).id, created.id);

// 5. Caller role enforcement on status mutation and partner BFF
const mobilePatch = await fetch(`${gwBase}/api/orders/${created.id}/status`, {
  method: 'PATCH',
  headers: { 'Content-Type': 'application/json', 'X-Tadka-Client': 'mobile-app' },
  body: JSON.stringify({ status: 'preparing' }),
  signal: AbortSignal.timeout(10000),
});
assert.equal(mobilePatch.status, 403);

const mobilePartnerBff = await fetch(`${gwBase}/bff/partner/orders/${created.id}`, {
  headers: { 'X-Tadka-Client': 'mobile-app' },
  signal: AbortSignal.timeout(10000),
});
assert.equal(mobilePartnerBff.status, 403);

const partnerPatch = await fetch(`${gwBase}/api/orders/${created.id}/status`, {
  method: 'PATCH',
  headers: { 'Content-Type': 'application/json', 'X-Tadka-Client': 'partner-web' },
  body: JSON.stringify({ status: 'preparing' }),
  signal: AbortSignal.timeout(10000),
});
assert.equal(partnerPatch.status, 200);

// 6. Client shaping: compact Mobile BFF vs full Partner BFF
const mobileTrackingRes = await fetch(`${gwBase}/bff/mobile/orders/${created.id}/tracking`, {
  headers: { 'X-Tadka-Client': 'mobile-app', 'X-Correlation-Id': correlationId },
  signal: AbortSignal.timeout(10000),
});
assert.equal(mobileTrackingRes.status, 200);
assert.equal(mobileTrackingRes.headers.get('x-correlation-id'), correlationId);
const mobileTracking = await mobileTrackingRes.json();
assert.equal(mobileTracking.orderId, created.id);
assert.equal(mobileTracking.status, 'preparing');
assert.equal(mobileTracking.totalAmount, 480);
assert.equal(mobileTracking.itemCount, 2);
assert.equal(mobileTracking.restaurantName, 'Tadka Indiranagar');
assert.equal(mobileTracking.catalogDegraded, false);
assert.equal('items' in mobileTracking, false, 'mobile tracking payload should omit full line items');

const partnerOrderRes = await fetch(`${gwBase}/bff/partner/orders/${created.id}`, {
  headers: { 'X-Tadka-Client': 'partner-web' },
  signal: AbortSignal.timeout(10000),
});
assert.equal(partnerOrderRes.status, 200);
const partnerOrder = await partnerOrderRes.json();
assert.equal(partnerOrder.customerId, customerId);
assert.equal(partnerOrder.restaurantOpen, true);
assert.equal(partnerOrder.items.length, 1);
assert.equal(partnerOrder.items[0].itemName, 'Paneer Tikka');

// 7. Negative control (mode=strict) vs resilient graceful degradation under Catalog failure
const strictRes = await fetch(`${gwBase}/bff/mobile/orders/${created.id}/tracking?mode=strict`, {
  headers: { 'X-Tadka-Client': 'mobile-app', 'X-Demo-Catalog-Failure': 'true' },
  signal: AbortSignal.timeout(10000),
});
assert.equal(strictRes.status, 502);

const resilientRes = await fetch(`${gwBase}/bff/mobile/orders/${created.id}/tracking`, {
  headers: { 'X-Tadka-Client': 'mobile-app', 'X-Demo-Catalog-Failure': 'true' },
  signal: AbortSignal.timeout(10000),
});
assert.equal(resilientRes.status, 200);
const degraded = await resilientRes.json();
assert.equal(degraded.orderId, created.id);
assert.equal(degraded.status, 'preparing');
assert.equal(degraded.totalAmount, 480);
assert.equal(degraded.restaurantName, null);
assert.equal(degraded.catalogDegraded, true);

console.log(`Assertions passed: zero-SQL gateway check, edge caller enforcement, blocked internal route, correlation ID, mobile vs partner shaping, and strict (502) vs resilient (200 degraded) catalog failure. orderId=${created.id}`);
