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
  // 1. Clean reset of reliable consumer tables and topics
  const reset = await postJson('/diagnostics/kafka/reliable/reset');
  assert.equal(reset.status, 'RESET');
  assert.equal(reset.reliableTopic, 'tadka.order.reliable');
  assert.equal(reset.dlqTopic, 'tadka.order.reliable.dlq');

  // 2. Negative control: naive consumer applies all 3 duplicate deliveries (50 * 3 = 150 points)
  const naiveDup = await postJson(
    '/diagnostics/kafka/reliable/duplicate-delivery?mode=naive&customerId=501&orderId=9001&points=50&deliveries=3',
  );
  assert.equal(naiveDup.deliveriesPublished, 3);
  assert.equal(naiveDup.deliveriesConsumed, 3);
  assert.equal(naiveDup.pointsBalance, 150, `expected naive over-credit 150, got ${naiveDup.pointsBalance}`);
  assert.equal(naiveDup.creditCount, 3);
  assert.equal(naiveDup.duplicateCreditsApplied, 2);
  assert.equal(naiveDup.processedEventRowCount, 0);

  // 3. Idempotent consumer: same 3 deliveries collapse into 1 atomic credit (50 points)
  const idempDup = await postJson(
    '/diagnostics/kafka/reliable/duplicate-delivery?mode=idempotent&customerId=501&orderId=9001&points=50&deliveries=3',
  );
  assert.equal(idempDup.deliveriesPublished, 3);
  assert.equal(idempDup.deliveriesConsumed, 3);
  assert.equal(idempDup.pointsBalance, 50, `expected idempotent balance 50, got ${idempDup.pointsBalance}`);
  assert.equal(idempDup.creditCount, 1);
  assert.equal(idempDup.duplicatesSuppressed, 2);
  assert.equal(idempDup.duplicateCreditsApplied, 0);
  assert.equal(idempDup.processedEventRowCount, 1);

  // 4. Negative control poison pill: block-on-poison halts behind evt-poison-2 and blocks 2 valid events
  const blocked = await postJson(
    '/diagnostics/kafka/reliable/poison-pipeline?policy=block-on-poison&maxAttempts=3',
  );
  assert.equal(blocked.totalPublished, 4);
  assert.equal(blocked.validProcessedCount, 1);
  assert.equal(blocked.headOfLineBlockedCount, 2);
  assert.equal(blocked.blockedBehindEventId, 'evt-poison-2');
  assert.equal(blocked.attemptCountOnPoison, 3);
  assert.equal(blocked.dlqRoutedCount, 0);
  assert.deepEqual(blocked.unprocessedValidEvents, ['evt-valid-3', 'evt-valid-4']);

  // 5. Positive control poison pill: retry 3 times then route to Kafka DLQ + PG audit table; valid events flow
  const dlqRun = await postJson(
    '/diagnostics/kafka/reliable/poison-pipeline?policy=retry-then-dlq&maxAttempts=3',
  );
  assert.equal(dlqRun.totalPublished, 4);
  assert.equal(dlqRun.validProcessedCount, 3);
  assert.equal(dlqRun.headOfLineBlockedCount, 0);
  assert.equal(dlqRun.attemptCountOnPoison, 3);
  assert.deepEqual(dlqRun.backoffScheduleMs, [100, 200, 400]);
  assert.equal(dlqRun.dlqRoutedCount, 1);
  assert.equal(dlqRun.dlqTopic, 'tadka.order.reliable.dlq');
  assert.equal(dlqRun.dlqMessageVerifiedOnKafka, true);
  assert.equal(dlqRun.dlqRecord.eventId, 'evt-poison-2');
  assert.equal(dlqRun.dlqRecord.originalPartition, 0);
  assert.equal(dlqRun.dlqRecord.originalOffset, 1);
  assert.equal(dlqRun.dlqRecord.attemptCount, 3);
  assert.equal(dlqRun.dlqRecord.failureReason, 'NEGATIVE_POINTS_SCHEMA_VIOLATION');
  assert.deepEqual(dlqRun.processedCustomerIds, [601, 603, 604]);

  console.log(
    `Assertions passed: naive duplicate (${naiveDup.pointsBalance} pts) vs idempotent (${idempDup.pointsBalance} pts, ${idempDup.duplicatesSuppressed} suppressed); poison block (${blocked.headOfLineBlockedCount} blocked) vs retry+DLQ (${dlqRun.validProcessedCount} valid, DLQ verified=${dlqRun.dlqMessageVerifiedOnKafka}).`,
  );
} finally {
  await postJson('/diagnostics/kafka/reliable/reset').catch(() => {});
}
