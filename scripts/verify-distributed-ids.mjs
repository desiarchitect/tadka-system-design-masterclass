import assert from 'node:assert/strict';

const base = process.env.BASE_URL ?? 'http://localhost:5080';

// 1. Sample generation + Snowflake bit layout decoding + monotonic ordering
const sampleRes = await fetch(`${base}/diagnostics/ids/sample?count=10`, {
  signal: AbortSignal.timeout(10000),
});
assert.equal(sampleRes.status, 200);
const sample = await sampleRes.json();
assert.equal(sample.count, 10);
assert.equal(sample.snowflakes.length, 10);

for (let i = 0; i < sample.snowflakes.length; i++) {
  assert.equal(sample.snowflakes[i].nodeId, 7);
  const currentId = BigInt(sample.snowflakes[i].idString);
  assert.ok(currentId > 0n, 'Snowflake sign bit must remain 0');
  if (i > 0) {
    const prevId = BigInt(sample.snowflakes[i - 1].idString);
    assert.ok(
      currentId > prevId,
      `Snowflake IDs must be strictly increasing (${currentId} > ${prevId})`,
    );
  }
}

// UUIDv7 48-bit millisecond timestamp prefix (first 13 chars "xxxxxxxx-xxxx") is monotonic
const v7Prefixes = sample.uuidV7.map(u => u.slice(0, 13));
assert.deepEqual(v7Prefixes, [...v7Prefixes].sort(), 'UUIDv7 timestamp prefix must be monotonically ordered');

// 2. Clock rollback guard
const clockRes = await fetch(`${base}/diagnostics/ids/clock-rollback-check?rollbackMs=150`, {
  method: 'POST',
  signal: AbortSignal.timeout(10000),
});
assert.equal(clockRes.status, 200);
const clock = await clockRes.json();
assert.equal(clock.blocked, true);
assert.equal(clock.code, 'clock_moved_backwards');
assert.equal(clock.detectedDriftMs, 150);

// 3. B-tree index locality benchmark (pgstatindex on 25,000 rows)
const benchRes = await fetch(`${base}/diagnostics/ids/benchmark?rows=25000`, {
  method: 'POST',
  signal: AbortSignal.timeout(60000),
});
assert.equal(benchRes.status, 200);
const bench = await benchRes.json();
assert.equal(bench.rowsPerTable, 25000);

assert.ok(
  bench.uuidV4.avgLeafDensityPct < bench.uuidV7.avgLeafDensityPct,
  `Expected UUIDv4 leaf density (${bench.uuidV4.avgLeafDensityPct}%) < UUIDv7 (${bench.uuidV7.avgLeafDensityPct}%)`,
);
assert.ok(
  bench.uuidV4.leafFragmentationPct > bench.uuidV7.leafFragmentationPct,
  `Expected UUIDv4 fragmentation (${bench.uuidV4.leafFragmentationPct}%) > UUIDv7 (${bench.uuidV7.leafFragmentationPct}%)`,
);
assert.ok(
  bench.uuidV4.leafPages > bench.uuidV7.leafPages,
  `Expected UUIDv4 leaf pages (${bench.uuidV4.leafPages}) > UUIDv7 (${bench.uuidV7.leafPages})`,
);
assert.ok(
  bench.snowflake.indexSizeBytes < bench.uuidV7.indexSizeBytes,
  `Expected 8-byte Snowflake index (${bench.snowflake.indexSizeBytes}B) < 16-byte UUIDv7 (${bench.uuidV7.indexSizeBytes}B)`,
);

console.log(
  `Assertions passed: UUIDv4(density=${bench.uuidV4.avgLeafDensityPct}%, frag=${bench.uuidV4.leafFragmentationPct}%, leafPages=${bench.uuidV4.leafPages}, size=${bench.uuidV4.indexSizeBytes}B) vs UUIDv7(density=${bench.uuidV7.avgLeafDensityPct}%, frag=${bench.uuidV7.leafFragmentationPct}%, leafPages=${bench.uuidV7.leafPages}, size=${bench.uuidV7.indexSizeBytes}B) vs Snowflake(density=${bench.snowflake.avgLeafDensityPct}%, frag=${bench.snowflake.leafFragmentationPct}%, leafPages=${bench.snowflake.leafPages}, size=${bench.snowflake.indexSizeBytes}B).`,
);
