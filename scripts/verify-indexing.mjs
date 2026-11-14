import assert from 'node:assert/strict';

const base = process.env.BASE_URL ?? 'http://localhost:5080';

async function setMode(mode) {
  const res = await fetch(`${base}/diagnostics/indexing/mode`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ mode }),
    signal: AbortSignal.timeout(15000),
  });
  assert.equal(res.status, 200, `Expected 200 switching mode to ${mode}`);
  return res.json();
}

async function fetchJson(path) {
  const res = await fetch(`${base}${path}`, { signal: AbortSignal.timeout(15000) });
  assert.equal(res.status, 200, `Expected 200 for ${path}`);
  return res.json();
}

// Ensure baseline 25,000 historical rows are present.
const reseedRes = await fetch(`${base}/diagnostics/indexing/reseed`, {
  method: 'POST',
  signal: AbortSignal.timeout(15000),
});
assert.equal(reseedRes.status, 200);
const reseed = await reseedRes.json();
assert.ok(reseed.estimatedRows >= 25000, `Expected >= 25000 orders, got ${reseed.estimatedRows}`);

// 1. Negative control: no kitchen-feed index -> Seq Scan filtering ~24k rows.
const noneMode = await setMode('none');
const seqFeed = await fetchJson('/orders/kitchen-feed?restaurantId=7&status=preparing&limit=20&explain=true');
assert.equal(seqFeed.items.length, 20);
assert.equal(seqFeed.plan.nodeType, 'Seq Scan');
assert.ok(
  seqFeed.plan.rowsRemovedByFilter > 1000,
  `Expected Seq Scan to filter > 1000 rows, got ${seqFeed.plan.rowsRemovedByFilter}`,
);

// 2. Composite index -> Index Scan on orders_restaurant_status_created_idx with 0 rows removed by filter.
const compositeMode = await setMode('composite');
assert.ok(
  compositeMode.indexesBytes > noneMode.indexesBytes,
  'Adding composite index must increase total index bytes on orders',
);
const compositeFeed = await fetchJson('/orders/kitchen-feed?restaurantId=7&status=preparing&limit=20&explain=true');
assert.equal(compositeFeed.items.length, 20);
assert.equal(compositeFeed.plan.nodeType, 'Index Scan');
assert.equal(compositeFeed.plan.indexName, 'orders_restaurant_status_created_idx');
assert.equal(compositeFeed.plan.rowsRemovedByFilter, 0);

// 3. Covering index + VACUUM ANALYZE -> Index Only Scan with heapFetches = 0.
const coveringMode = await setMode('covering');
assert.ok(
  coveringMode.indexesBytes > compositeMode.indexesBytes,
  'Covering index with INCLUDE columns must use more index bytes than composite index',
);
const coveringFeed = await fetchJson('/orders/kitchen-feed?restaurantId=7&status=preparing&limit=20&explain=true');
assert.equal(coveringFeed.items.length, 20);
assert.equal(coveringFeed.plan.nodeType, 'Index Only Scan');
assert.equal(coveringFeed.plan.indexName, 'orders_restaurant_status_covering_idx');
assert.equal(coveringFeed.plan.rowsRemovedByFilter, 0);
assert.equal(coveringFeed.plan.heapFetches, 0, 'Visibility map after VACUUM should yield zero heap fetches');

// 4. Leftmost-prefix negative control: querying without restaurantId cannot narrow via Index Cond.
const statusScan = await fetchJson('/orders/status-scan?status=preparing&limit=20&explain=true');
assert.equal(statusScan.items.length, 20);
assert.equal(
  statusScan.plan.indexCond,
  null,
  `Expected no narrow Index Cond when leading key restaurant_id is omitted, got ${statusScan.plan.indexCond}`,
);
assert.ok(
  statusScan.plan.rowsRemovedByFilter > 1000,
  `Expected status-only scan to filter > 1000 non-matching rows, got ${statusScan.plan.rowsRemovedByFilter}`,
);

console.log(
  `Assertions passed: Seq Scan (${seqFeed.plan.rowsRemovedByFilter} filtered, ${seqFeed.plan.sharedHitBlocks} hit blocks) -> Composite Index Scan (${compositeFeed.plan.sharedHitBlocks} hit blocks) -> Covering Index Only Scan (heapFetches=${coveringFeed.plan.heapFetches}, ${coveringFeed.plan.sharedHitBlocks} hit blocks) and leftmost-prefix guard verified.`,
);
