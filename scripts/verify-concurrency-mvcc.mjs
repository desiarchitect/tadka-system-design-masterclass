import assert from 'node:assert/strict';

const base = process.env.BASE_URL ?? 'http://localhost:5080';

async function resetDeal(stock = 5) {
  const res = await fetch(`${base}/catalog/deals/101/reset`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ remainingStock: stock }),
    signal: AbortSignal.timeout(10000),
  });
  assert.equal(res.status, 200);
  return res.json();
}

async function inspectDeal() {
  const res = await fetch(`${base}/catalog/deals/101/mvcc-inspect`, { signal: AbortSignal.timeout(10000) });
  assert.equal(res.status, 200);
  return res.json();
}

async function claimOnce(mode) {
  const res = await fetch(`${base}/catalog/deals/101/claim?mode=${mode}`, {
    method: 'POST',
    signal: AbortSignal.timeout(15000),
  });
  const body = await res.json();
  return { status: res.status, body };
}

// 1. MVCC tuple proof: updating the row creates a new ctid and xmin in PostgreSQL.
const before = await resetDeal(5);
const singleClaim = await claimOnce('pessimistic');
assert.equal(singleClaim.status, 200);
const after = await inspectDeal();
assert.equal(after.remainingStock, 4);
assert.notEqual(after.ctid, before.ctid, 'MVCC UPDATE must allocate a new physical tuple version (ctid)');
assert.notEqual(after.xmin, before.xmin, 'MVCC UPDATE must stamp a newer transaction xmin');

// 2. Negative control (mode=unsafe): 10 parallel claims against 5 units lose updates.
await resetDeal(5);
const unsafeResults = await Promise.all(Array.from({ length: 10 }, () => claimOnce('unsafe')));
const unsafeWinners = unsafeResults.filter(r => r.status === 200).length;
const afterUnsafe = await inspectDeal();
const actualDecremented = 5 - afterUnsafe.remainingStock;
assert.ok(
  unsafeWinners > 5 || actualDecremented < unsafeWinners,
  `Expected unsafe concurrent updates to lose updates (winners=${unsafeWinners}, decremented=${actualDecremented})`,
);

// 3. Optimistic Concurrency Control: 10 concurrent claims never oversell; bounded retries claim all 5.
await resetDeal(5);
const optBatch = await Promise.all(Array.from({ length: 10 }, () => claimOnce('optimistic')));
const optWinnersPass1 = optBatch.filter(r => r.status === 200).length;
const optVersionConflicts = optBatch.filter(r => r.status === 409 && r.body.code === 'optimistic_version_conflict').length;
const afterOptPass1 = await inspectDeal();
assert.equal(
  optWinnersPass1 + afterOptPass1.remainingStock,
  5,
  'Optimistic CAS must never oversell stock',
);
assert.ok(optVersionConflicts > 0, 'Concurrent optimistic contention must surface optimistic_version_conflict');

let totalOptWinners = optWinnersPass1;
const losers = optBatch.filter(r => r.status === 409 && r.body.code === 'optimistic_version_conflict');
for (let i = 0; i < losers.length; i++) {
  for (let attempt = 0; attempt < 10; attempt++) {
    const retry = await claimOnce('optimistic');
    if (retry.status === 200) {
      totalOptWinners++;
      break;
    }
    if (retry.body.code === 'out_of_stock') break;
  }
}
const afterOptRetries = await inspectDeal();
assert.equal(totalOptWinners, 5, 'Bounded optimistic retries must fulfill exactly 5 deals');
assert.equal(afterOptRetries.remainingStock, 0);

// 4. Pessimistic Locking (SELECT ... FOR UPDATE): 10 concurrent claims yield 5 winners and 5 out_of_stock in one pass.
await resetDeal(5);
const pessBatch = await Promise.all(Array.from({ length: 10 }, () => claimOnce('pessimistic')));
const pessWinners = pessBatch.filter(r => r.status === 200).length;
const pessOutOfStock = pessBatch.filter(r => r.status === 409 && r.body.code === 'out_of_stock').length;
const afterPess = await inspectDeal();
assert.equal(pessWinners, 5);
assert.equal(pessOutOfStock, 5);
assert.equal(afterPess.remainingStock, 0);

console.log(
  `Assertions passed: MVCC tuple shift (${before.ctid} -> ${after.ctid}), unsafe lost update (winners=${unsafeWinners}, stockLeft=${afterUnsafe.remainingStock}), optimistic CAS (pass1Winners=${optWinnersPass1}, conflicts=${optVersionConflicts}, totalAfterRetries=${totalOptWinners}), pessimistic FOR UPDATE (winners=${pessWinners}, outOfStock=${pessOutOfStock}).`,
);
