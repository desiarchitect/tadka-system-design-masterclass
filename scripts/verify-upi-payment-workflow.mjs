import assert from 'node:assert/strict';

const base = process.env.BASE_URL ?? 'http://127.0.0.1:5080';

async function requestJson(path, { method = 'POST', headers = {}, body } = {}) {
  const res = await fetch(`${base}${path}`, {
    method,
    headers,
    body,
    signal: AbortSignal.timeout(30000),
  });
  const text = await res.text();
  const parsed = text ? JSON.parse(text) : null;
  return { status: res.status, headers: res.headers, body: parsed };
}

try {
  // 1. Clean reset
  const reset = await requestJson('/payments/upi/reset');
  assert.equal(reset.status, 200);
  assert.equal(reset.body.status, 'RESET');

  // 2. Payment Intent Idempotency: 201 Created -> 200 Replayed -> 409 Conflict on mutated amount
  const intentPayload = JSON.stringify({
    orderId: 95001,
    customerId: 801,
    amountPaise: 45000,
    payerVpa: 'deepak@okaxis',
  });
  const createRes = await requestJson('/payments/upi/intents', {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': 'upi-idem-95001',
    },
    body: intentPayload,
  });
  assert.equal(createRes.status, 201);
  assert.equal(createRes.headers.get('idempotency-status'), 'CREATED');
  assert.equal(createRes.body.intentRef, 'upi_intent_95001');
  assert.equal(createRes.body.status, 'PENDING');

  const replayRes = await requestJson('/payments/upi/intents', {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': 'upi-idem-95001',
    },
    body: intentPayload,
  });
  assert.equal(replayRes.status, 200);
  assert.equal(replayRes.headers.get('idempotency-status'), 'REPLAYED');
  assert.equal(replayRes.body.intentRef, 'upi_intent_95001');

  const conflictRes = await requestJson('/payments/upi/intents', {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': 'upi-idem-95001',
    },
    body: JSON.stringify({
      orderId: 95001,
      customerId: 801,
      amountPaise: 99000,
      payerVpa: 'deepak@okaxis',
    }),
  });
  assert.equal(conflictRes.status, 409);
  assert.equal(conflictRes.body.error, 'IDEMPOTENCY_PAYLOAD_MISMATCH');

  // 3. Forged / tampered HMAC webhook rejected with 401
  const validSignNaive = await requestJson('/payments/upi/sign-webhook', {
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      webhookId: 'wh-naive-1',
      intentRef: 'upi_intent_95001',
      status: 'CAPTURED',
      pspRrn: 'RRN431200000001',
      amountPaise: 45000,
    }),
  });
  assert.equal(validSignNaive.status, 200);

  const tamperedWebhook = await requestJson('/payments/upi/webhooks?mode=safe', {
    headers: {
      'Content-Type': 'application/json',
      'X-Tadka-Signature': 'deadbeef0000invalid',
    },
    body: validSignNaive.body.rawBody,
  });
  assert.equal(tamperedWebhook.status, 401);
  assert.equal(tamperedWebhook.body.error, 'INVALID_WEBHOOK_SIGNATURE');

  // 4. Negative control (mode=naive): delivering CAPTURED webhook twice double-posts ledger entries (8 rows / 90,000 debit)
  const naiveWh1 = await requestJson('/payments/upi/webhooks?mode=naive', {
    headers: {
      'Content-Type': 'application/json',
      'X-Tadka-Signature': validSignNaive.body.signature,
    },
    body: validSignNaive.body.rawBody,
  });
  assert.equal(naiveWh1.status, 200);
  assert.equal(naiveWh1.body.ledgerEntryCount, 4);

  const naiveWh2 = await requestJson('/payments/upi/webhooks?mode=naive', {
    headers: {
      'Content-Type': 'application/json',
      'X-Tadka-Signature': validSignNaive.body.signature,
    },
    body: validSignNaive.body.rawBody,
  });
  assert.equal(naiveWh2.status, 200);
  assert.equal(naiveWh2.body.ledgerEntryCount, 8, 'naive mode double-posts 8 ledger entries');
  assert.equal(naiveWh2.body.totalDebitPaise, 90000, 'naive duplicate doubles total debit to 90000 paise');

  // 5. Positive control (mode=safe) on fresh order 95002: atomic 4-leg balanced ledger + duplicate ignored + terminal FAILED guarded
  const createSafe = await requestJson('/payments/upi/intents', {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': 'upi-idem-95002',
    },
    body: JSON.stringify({
      orderId: 95002,
      customerId: 802,
      amountPaise: 45000,
      payerVpa: 'ananya@okhdfc',
    }),
  });
  assert.equal(createSafe.status, 201);

  const signedSafeCapture = await requestJson('/payments/upi/sign-webhook', {
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      webhookId: 'wh-safe-cap-1',
      intentRef: 'upi_intent_95002',
      status: 'CAPTURED',
      pspRrn: 'RRN431200000002',
      amountPaise: 45000,
    }),
  });

  const safeWh1 = await requestJson('/payments/upi/webhooks?mode=safe', {
    headers: {
      'Content-Type': 'application/json',
      'X-Tadka-Signature': signedSafeCapture.body.signature,
    },
    body: signedSafeCapture.body.rawBody,
  });
  assert.equal(safeWh1.status, 200);
  assert.equal(safeWh1.body.transitionOutcome, 'TRANSITION_APPLIED');
  assert.equal(safeWh1.body.status, 'CAPTURED');
  assert.equal(safeWh1.body.ledgerEntryCount, 4);
  assert.equal(safeWh1.body.totalDebitPaise, 45000);
  assert.equal(safeWh1.body.totalCreditPaise, 45000);
  assert.equal(safeWh1.body.netSignedSum, 0);

  // Duplicate CAPTURED delivery ignored safely
  const safeWh2 = await requestJson('/payments/upi/webhooks?mode=safe', {
    headers: {
      'Content-Type': 'application/json',
      'X-Tadka-Signature': signedSafeCapture.body.signature,
    },
    body: signedSafeCapture.body.rawBody,
  });
  assert.equal(safeWh2.status, 200);
  assert.equal(safeWh2.body.transitionOutcome, 'DUPLICATE_IGNORED');
  assert.equal(safeWh2.body.ledgerEntryCount, 4);
  assert.equal(safeWh2.body.totalDebitPaise, 45000);

  // Out-of-order late FAILED webhook guarded (never overwrites CAPTURED)
  const signedLateFail = await requestJson('/payments/upi/sign-webhook', {
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      webhookId: 'wh-safe-late-fail',
      intentRef: 'upi_intent_95002',
      status: 'FAILED',
      pspRrn: 'RRN431200000002',
      amountPaise: 45000,
    }),
  });
  const lateFailRes = await requestJson('/payments/upi/webhooks?mode=safe', {
    headers: {
      'Content-Type': 'application/json',
      'X-Tadka-Signature': signedLateFail.body.signature,
    },
    body: signedLateFail.body.rawBody,
  });
  assert.equal(lateFailRes.status, 200);
  assert.equal(lateFailRes.body.transitionOutcome, 'TERMINAL_STATE_GUARDED');
  assert.equal(lateFailRes.body.status, 'CAPTURED');

  // Verify audit proves balanced ledger + order 'paid' + outbox event created in same tx
  const auditSafe = await requestJson('/payments/upi/intents/upi_intent_95002/audit', { method: 'GET' });
  assert.equal(auditSafe.status, 200);
  assert.equal(auditSafe.body.orderStatus, 'paid');
  assert.equal(auditSafe.body.netSignedSum, 0);
  assert.equal(auditSafe.body.ledgerEntries.length, 4);
  assert.equal(auditSafe.body.outboxEvents.length, 1);
  assert.equal(auditSafe.body.outboxEvents[0].eventType, 'PaymentCaptured');
  assert.equal(auditSafe.body.webhookEvents.length, 3);

  // 6. Dropped webhook recovery: order 95003 stays PENDING until reconciliation inquiry runs
  const createPending = await requestJson('/payments/upi/intents', {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': 'upi-idem-95003',
    },
    body: JSON.stringify({
      orderId: 95003,
      customerId: 803,
      amountPaise: 45000,
      payerVpa: 'rohit@okicici',
    }),
  });
  assert.equal(createPending.status, 201);

  const reconRes = await requestJson('/payments/upi/reconcile');
  assert.equal(reconRes.status, 200);
  assert.equal(reconRes.body.scannedPendingCount, 1);
  assert.equal(reconRes.body.reconciledCount, 1);
  assert.equal(reconRes.body.reconciledIntents[0].intentRef, 'upi_intent_95003');
  assert.equal(reconRes.body.reconciledIntents[0].newStatus, 'CAPTURED');
  assert.equal(reconRes.body.reconciledIntents[0].ledgerEntryCount, 4);
  assert.equal(reconRes.body.reconciledIntents[0].netSignedSum, 0);

  const auditReconciled = await requestJson('/payments/upi/intents/upi_intent_95003/audit', { method: 'GET' });
  assert.equal(auditReconciled.status, 200);
  assert.equal(auditReconciled.body.intent.status, 'CAPTURED');
  assert.equal(auditReconciled.body.orderStatus, 'paid');
  assert.equal(auditReconciled.body.netSignedSum, 0);
  assert.equal(auditReconciled.body.outboxEvents.length, 1);

  console.log(
    `Assertions passed: idempotency (201/200/409), HMAC 401 reject, naive duplicate (8 rows/${naiveWh2.body.totalDebitPaise}p) vs safe (4 rows/${safeWh2.body.totalDebitPaise}p, netSigned=0), late FAILED guarded, and reconciliation recovery (upi_intent_95003 -> CAPTURED).`,
  );
} finally {
  await requestJson('/payments/upi/reset').catch(() => {});
}
