import assert from 'node:assert/strict';

const base = process.env.BASE_URL ?? 'http://127.0.0.1:5080';

async function postJson(path) {
  const res = await fetch(`${base}${path}`, {
    method: 'POST',
    signal: AbortSignal.timeout(30000),
  });
  const body = await res.json();
  assert.equal(res.status, 200, `${path} returned ${res.status}: ${JSON.stringify(body)}`);
  return body;
}

try {
  // 1. Reset topic cleanly with 4 partitions
  const reset = await postJson('/diagnostics/kafka/reset');
  assert.equal(reset.topic, 'tadka.order.events');
  assert.equal(reset.partitionCount, 4);

  // 2. Negative control: unkeyed lifecycle events spread across 4 partitions (no single-partition order)
  const unkeyed = await postJson('/diagnostics/kafka/publish-lifecycle?keyStrategy=unkeyed&orderId=9001');
  assert.equal(unkeyed.partitionsUsed, 4, `expected 4 partitions used for unkeyed, got ${unkeyed.partitionsUsed}`);
  assert.equal(unkeyed.singlePartitionOrdered, false);
  assert.equal(unkeyed.events.length, 4);

  // 3. Keyed lifecycle: reset then publish with key=order-9001 -> all 4 on 1 partition with offsets 0..3
  await postJson('/diagnostics/kafka/reset');
  const keyed = await postJson('/diagnostics/kafka/publish-lifecycle?keyStrategy=order-id&orderId=9001');
  assert.equal(keyed.partitionsUsed, 1, `expected 1 partition for keyed lifecycle, got ${keyed.partitionsUsed}`);
  assert.equal(keyed.singlePartitionOrdered, true);
  assert.deepEqual(
    keyed.events.map(e => e.offset),
    [0, 1, 2, 3],
    `expected contiguous offsets [0,1,2,3], got ${JSON.stringify(keyed.events)}`,
  );

  // 4. Consumer groups: 2 consumers -> 2 active (2+2 partitions); independent analytics-group gets all 4
  const layout2 = await postJson('/diagnostics/kafka/consumer-group-layout?consumerCount=2');
  assert.equal(layout2.totalTopicMessages, 4);
  assert.equal(layout2.kitchenGroup.activeConsumerCount, 2);
  assert.equal(layout2.kitchenGroup.idleConsumerCount, 0);
  assert.equal(layout2.kitchenGroup.totalDelivered, 4);
  assert.equal(layout2.analyticsGroup.totalDelivered, 4);

  // 5. Over-provisioned group: 5 consumers on 4 partitions -> 4 active, 1 idle
  const layout5 = await postJson('/diagnostics/kafka/consumer-group-layout?consumerCount=5');
  assert.equal(layout5.kitchenGroup.consumerCount, 5);
  assert.equal(layout5.kitchenGroup.activeConsumerCount, 4);
  assert.equal(layout5.kitchenGroup.idleConsumerCount, 1);
  assert.equal(layout5.kitchenGroup.totalDelivered, 4);

  // 6. Negative control rebalance: revoke without commit replays offset 0 (5 processed, 1 duplicate)
  const rebBad = await postJson('/diagnostics/kafka/rebalance-simulation?commitMode=uncommitted-on-revoke');
  assert.equal(rebBad.publishedCount, 4);
  assert.equal(rebBad.totalProcessedEvents, 5, `expected 5 total processed on uncommitted revoke, got ${rebBad.totalProcessedEvents}`);
  assert.equal(rebBad.duplicateDeliveries, 1);

  // 7. Cooperative commit-on-revoke: zero duplicates, 4 processed
  const rebGood = await postJson('/diagnostics/kafka/rebalance-simulation?commitMode=cooperative-commit-on-revoke');
  assert.equal(rebGood.publishedCount, 4);
  assert.equal(rebGood.totalProcessedEvents, 4, `expected 4 total processed on commit-on-revoke, got ${rebGood.totalProcessedEvents}`);
  assert.equal(rebGood.duplicateDeliveries, 0);

  console.log(
    `Assertions passed: unkeyed (${unkeyed.partitionsUsed} partitions), keyed (1 partition offsets 0..3), 5 consumers (4 active, 1 idle), independent analytics-group (4 delivered), rebalance duplicate (5 vs 4).`,
  );
} finally {
  await postJson('/diagnostics/kafka/reset').catch(() => {});
}
