import assert from 'node:assert/strict';

const base = process.env.BASE_URL ?? 'http://localhost:5080';

async function toggleReplay(paused) {
  const res = await fetch(`${base}/diagnostics/replication/replay`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ paused }),
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(res.status, 200);
  return res.json();
}

async function getRepStatus() {
  const res = await fetch(`${base}/diagnostics/replication/status`, {
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(res.status, 200);
  return res.json();
}

try {
  // 1. Streaming replication is active and replica rejects direct writes (25006)
  const initialStatus = await getRepStatus();
  assert.equal(initialStatus.replicaInRecovery, true, 'Replica must be in recovery mode');
  assert.ok(initialStatus.connectedReplicas.length >= 1, 'Primary must report streaming replica in pg_stat_replication');

  const roRes = await fetch(`${base}/diagnostics/replication/readonly-check`, {
    method: 'POST',
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(roRes.status, 200);
  const ro = await roRes.json();
  assert.equal(ro.blocked, true);
  assert.equal(ro.sqlState, '25006');

  // 2. Create order on primary while replay is active, then wait briefly until replicated
  const createRes = await fetch(`${base}/orders/with-lsn`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ customerId: 8801, restaurantId: 7, totalAmount: 420.50 }),
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(createRes.status, 201);
  const created = await createRes.json();
  const createCommitLsn = createRes.headers.get('x-commit-lsn');
  assert.ok(createCommitLsn, 'Expected X-Commit-Lsn header on write');

  let caughtUp = false;
  for (let i = 0; i < 30; i++) {
    const readSynced = await fetch(
      `${base}/orders/${created.id}/read-your-writes?routing=ryow&minLsn=${encodeURIComponent(created.commitLsn)}`,
      { signal: AbortSignal.timeout(10000) },
    );
    if (readSynced.status === 200) {
      const body = await readSynced.json();
      if (body.servedFrom === 'replica' && body.status === 'placed') {
        caughtUp = true;
        break;
      }
    }
    await new Promise(r => setTimeout(r, 100));
  }
  assert.ok(caughtUp, 'Expected replica to catch up and serve newly created order directly');

  // 3. Deterministic replication lag: pause WAL replay, mutate order on primary -> expose stale read on replica-only, RYOW falls back to primary
  const pausedState = await toggleReplay(true);
  assert.equal(pausedState.replicaWalReplayPaused, true);

  const patchRes = await fetch(`${base}/orders/${created.id}/status-with-lsn`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ status: 'preparing' }),
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(patchRes.status, 200);
  const updated = await patchRes.json();
  assert.equal(updated.status, 'preparing');

  // Confirm primary actually has WAL bytes ahead of paused replica
  const laggedStatus = await getRepStatus();
  assert.ok(laggedStatus.replayLagBytes > 0, `Expected replayLagBytes > 0 while paused (got ${laggedStatus.replayLagBytes})`);

  // Negative control: routing=replica-only sees stale status 'placed'
  const staleRes = await fetch(
    `${base}/orders/${created.id}/read-your-writes?routing=replica-only`,
    {
      headers: { 'X-Min-Lsn': updated.commitLsn },
      signal: AbortSignal.timeout(10000),
    },
  );
  assert.equal(staleRes.status, 200);
  const stale = await staleRes.json();
  assert.equal(stale.servedFrom, 'replica');
  assert.equal(stale.status, 'placed', 'Paused replica must still see stale placed status');
  assert.equal(stale.staleRelativeToMinLsn, true);

  // RYOW routing detects replicaReplayLsn < X-Min-Lsn and routes to primary-fallback
  const ryowRes = await fetch(
    `${base}/orders/${created.id}/read-your-writes?routing=ryow`,
    {
      headers: { 'X-Min-Lsn': updated.commitLsn },
      signal: AbortSignal.timeout(10000),
    },
  );
  assert.equal(ryowRes.status, 200);
  const ryow = await ryowRes.json();
  assert.equal(ryow.servedFrom, 'primary-fallback');
  assert.equal(ryow.reason, 'replica_behind_min_lsn');
  assert.equal(ryow.status, 'preparing');

  // 4. Resume WAL replay -> replica catches up and RYOW routes back to replica with fresh 'preparing' status
  const resumedState = await toggleReplay(false);
  assert.equal(resumedState.replicaWalReplayPaused, false);

  let afterResume = null;
  for (let i = 0; i < 30; i++) {
    const r = await fetch(
      `${base}/orders/${created.id}/read-your-writes?routing=ryow`,
      {
        headers: { 'X-Min-Lsn': updated.commitLsn },
        signal: AbortSignal.timeout(10000),
      },
    );
    const body = await r.json();
    if (body.servedFrom === 'replica' && body.status === 'preparing') {
      afterResume = body;
      break;
    }
    await new Promise(r => setTimeout(r, 100));
  }
  assert.ok(afterResume, 'Expected RYOW routing to return to replica once WAL replay catches up');
  assert.equal(afterResume.reason, 'replica_at_or_ahead_of_min_lsn');
  assert.equal(afterResume.staleRelativeToMinLsn, false);

  console.log(
    `Assertions passed: readOnlySqlState=${ro.sqlState}, lagBytesWhilePaused=${laggedStatus.replayLagBytes}, staleReplicaStatus=${stale.status}, ryowFallbackStatus=${ryow.status} (${ryow.servedFrom}), resumedReplicaStatus=${afterResume.status} (${afterResume.servedFrom}).`,
  );
} finally {
  // Best effort resume in case test aborted mid-pause
  await fetch(`${base}/diagnostics/replication/replay`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ paused: false }),
    signal: AbortSignal.timeout(5000),
  }).catch(() => {});
}
