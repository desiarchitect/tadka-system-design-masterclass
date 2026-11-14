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
  // 1. Clean reset
  const reset = await postJson('/diagnostics/outbox/reset');
  assert.equal(reset.status, 'RESET');
  assert.equal(reset.flashDealStock, 5);
  assert.equal(reset.outboxPendingCount, 0);

  // 2. Negative control A: DB commits first, then broker fails -> ghost order with missing Kafka event
  const dbFirst = await postJson('/diagnostics/outbox/dual-write?mode=db-first-broker-fails');
  assert.equal(dbFirst.orderInDatabase, true);
  assert.equal(dbFirst.outboxRowInDatabase, false);
  assert.equal(dbFirst.eventInKafka, false);
  assert.equal(dbFirst.inconsistency, 'GHOST_ORDER_NO_EVENT');

  // 3. Negative control B: Broker publishes first, then DB rolls back -> phantom Kafka event with missing order
  const brokerFirst = await postJson('/diagnostics/outbox/dual-write?mode=broker-first-db-fails');
  assert.equal(brokerFirst.orderInDatabase, false);
  assert.equal(brokerFirst.outboxRowInDatabase, false);
  assert.equal(brokerFirst.eventInKafka, true);
  assert.equal(brokerFirst.inconsistency, 'PHANTOM_EVENT_NO_ORDER');

  // 4. Transactional Outbox: single DB commit persists order + unpublished outbox row atomically
  const atomicOutbox = await postJson('/diagnostics/outbox/dual-write?mode=transactional-outbox');
  assert.equal(atomicOutbox.orderInDatabase, true);
  assert.equal(atomicOutbox.outboxRowInDatabase, true);
  assert.equal(atomicOutbox.outboxPublished, false);
  assert.equal(atomicOutbox.eventInKafka, false);
  assert.equal(atomicOutbox.inconsistency, 'NONE_PENDING_RELAY');

  // 5. Outbox Relay crash simulation: publishes to Kafka once, crashes before marking published_at = NOW()
  const relayCrash = await postJson('/diagnostics/outbox/relay?simulateCrashAfterPublishBeforeMark=true');
  assert.equal(relayCrash.relayedCount, 1);
  assert.equal(relayCrash.pendingOutboxRemaining, 1, 'row must stay pending after pre-mark crash');
  assert.equal(relayCrash.kafkaMessageCountForRelayedEvents, 1);
  assert.equal(relayCrash.uniqueDownstreamEffects, 1);
  assert.equal(relayCrash.loyaltyPointsAwarded, 25);

  // 6. Normal Relay recovery: re-publishes the still-pending row -> 2 copies on Kafka, still 1 downstream effect!
  const relayRecover = await postJson('/diagnostics/outbox/relay?simulateCrashAfterPublishBeforeMark=false');
  assert.equal(relayRecover.relayedCount, 1);
  assert.equal(relayRecover.pendingOutboxRemaining, 0);
  assert.equal(relayRecover.kafkaMessageCountForRelayedEvents, 2, 'relay crash recovery produces at-least-once duplicate on Kafka');
  assert.equal(relayRecover.uniqueDownstreamEffects, 1, 'idempotent consumer must apply effect once');
  assert.ok(relayRecover.duplicatesSuppressed >= 1);
  assert.equal(relayRecover.loyaltyPointsAwarded, 25);

  // 7. Saga happy path: 5 -> 4 stock, payment captured, kitchen accepted, order confirmed
  const sagaHappy = await postJson('/diagnostics/saga/execute?scenario=happy-path');
  assert.equal(sagaHappy.initialStock, 5);
  assert.equal(sagaHappy.remainingStock, 4);
  assert.equal(sagaHappy.paymentCaptured, true);
  assert.equal(sagaHappy.paymentRefunded, false);
  assert.equal(sagaHappy.kitchenAccepted, true);
  assert.equal(sagaHappy.currentState, 'COMPLETED');
  assert.equal(sagaHappy.orderStatus, 'confirmed');

  // 8. Negative control Saga: kitchen rejects without compensation -> leaked stock (4) & unrefunded payment
  const sagaLeaked = await postJson('/diagnostics/saga/execute?scenario=kitchen-rejected-uncompensated');
  assert.equal(sagaLeaked.remainingStock, 4);
  assert.equal(sagaLeaked.paymentCaptured, true);
  assert.equal(sagaLeaked.paymentRefunded, false);
  assert.equal(sagaLeaked.inventoryCompensated, false);
  assert.equal(sagaLeaked.currentState, 'FAILED_UNCOMPENSATED');
  assert.equal(sagaLeaked.inconsistency, 'STOCK_LEAKED_AND_PAYMENT_UNREFUNDED');

  // 9. Compensated Saga + idempotent compensation replay: refunds payment, releases stock 4 -> 5, stays 5 on replay
  const sagaComp = await postJson('/diagnostics/saga/execute?scenario=kitchen-rejected-compensated');
  assert.equal(sagaComp.initialStock, 5);
  assert.equal(sagaComp.remainingStock, 5);
  assert.equal(sagaComp.stockAfterReplay, 5, 'replaying compensation must never increment stock to 6');
  assert.equal(sagaComp.inventoryCompensated, true);
  assert.equal(sagaComp.paymentCaptured, true);
  assert.equal(sagaComp.paymentRefunded, true);
  assert.equal(sagaComp.compensationAttempts, 2);
  assert.equal(sagaComp.currentState, 'COMPENSATED');
  assert.equal(sagaComp.orderStatus, 'cancelled_compensated');

  console.log(
    `Assertions passed: ghost order (${dbFirst.orderId}), phantom event (${brokerFirst.orderId}), outbox relay crash recovery (2 Kafka copies -> 1 effect / ${relayRecover.loyaltyPointsAwarded} pts), saga leaked (${sagaLeaked.remainingStock}) vs idempotent compensated (${sagaComp.stockAfterReplay}, attempts=${sagaComp.compensationAttempts}).`,
  );
} finally {
  await postJson('/diagnostics/outbox/reset').catch(() => {});
}
