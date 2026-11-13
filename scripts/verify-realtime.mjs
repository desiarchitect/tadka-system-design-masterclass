import assert from 'node:assert/strict';
import { randomInt, randomUUID } from 'node:crypto';

const base = process.env.BASE_URL ?? 'http://localhost:5080';
const created = await fetch(`${base}/orders`, {
  method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': randomUUID() },
  body: JSON.stringify({ customerId: randomInt(1, 2 ** 47), restaurantId: 7, totalAmount: 249.50 }),
  signal: AbortSignal.timeout(10000),
});
assert.equal(created.status, 201);
const { id } = await created.json();
const url = `${base}/orders/${id}`;
async function change(status) {
  const response = await fetch(`${url}/status`, {
    method: 'PATCH', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ status }), signal: AbortSignal.timeout(10000),
  });
  assert.equal(response.status, 200);
  return response.json();
}
async function open(lastId) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(new Error('SSE check exceeded 15 seconds')), 15000);
  let reader;
  try {
    const response = await fetch(`${url}/events`, {
      headers: lastId === undefined ? {} : { 'Last-Event-ID': String(lastId) },
      signal: controller.signal,
    });
    assert.equal(response.status, 200);
    assert.match(response.headers.get('content-type'), /text\/event-stream/);
    reader = response.body.getReader();
  } catch (error) {
    clearTimeout(timeout);
    controller.abort();
    throw error;
  }
  const decoder = new TextDecoder();
  let buffer = '';
  return {
    async next() {
      while (true) {
        const boundary = buffer.indexOf('\n\n');
        if (boundary >= 0) {
          const frame = buffer.slice(0, boundary);
          buffer = buffer.slice(boundary + 2);
          const lines = frame.split('\n');
          const data = lines.find(line => line.startsWith('data: '));
          if (!data) continue;
          return {
            id: lines.find(line => line.startsWith('id: '))?.slice(4),
            event: lines.find(line => line.startsWith('event: '))?.slice(7),
            data: JSON.parse(data.slice(6)),
          };
        }
        const { done, value } = await reader.read();
        assert.equal(done, false, 'stream ended before the expected event');
        buffer += decoder.decode(value, { stream: true });
      }
    },
    close() { clearTimeout(timeout); controller.abort(); },
  };
}

let stream = await open();
try {
  const snapshot = await stream.next();
  assert.equal(snapshot.event, 'snapshot');
  assert.equal(snapshot.id, '0');
  assert.equal(snapshot.data.status, 'placed');
  await change('preparing');
  const event = await stream.next();
  assert.equal(event.event, 'order-status');
  assert.equal(event.id, '1');
  assert.equal(event.data.status, 'preparing');
} finally { stream.close(); }

// Disconnect while two transitions occur; Last-Event-ID must recover both in order.
await change('ready');
await change('delivered');
stream = await open(1);
try {
  const ready = await stream.next();
  const delivered = await stream.next();
  assert.equal(ready.id, '2');
  assert.equal(ready.data.status, 'ready');
  assert.equal(delivered.id, '3');
  assert.equal(delivered.data.status, 'delivered');
} finally { stream.close(); }
assert.equal((await change('delivered')).version, 3, 'same status must not create another event');
const snapshotOnly = await open();
try {
  const latest = await snapshotOnly.next();
  assert.equal(latest.event, 'snapshot');
  assert.equal(latest.id, '3', 'without a cursor, reconnect returns current state, not missing history');
} finally { snapshotOnly.close(); }
assert.equal((await fetch(`${url}/events`, { headers: { 'Last-Event-ID': 'bad' } })).status, 400);
assert.equal((await fetch(`${url}/events`, { headers: { 'Last-Event-ID': '-1' } })).status, 400);
assert.equal((await fetch(`${url}/events`, { headers: { 'Last-Event-ID': '4' } })).status, 409);
assert.equal((await fetch(`${base}/orders/9223372036854775807/events`)).status, 404);
assert.equal((await fetch(`${url}/status`, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: '{"status":"placed"}' })).status, 409);
const status = await fetch(`${url}/status`);
assert.equal((await status.json()).version, 3);
console.log(`Assertions passed: snapshot, live framing, disconnect replay, snapshot-only negative control, duplicate status and invalid cursors. orderId=${id}`);
