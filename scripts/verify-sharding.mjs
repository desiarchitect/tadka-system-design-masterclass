import assert from 'node:assert/strict';

const base = process.env.BASE_URL ?? 'http://localhost:5080';

async function simulate(strategy, totalOrders = 400, hotRestaurantShare = 0.80) {
  const res = await fetch(`${base}/sharding/simulate-workload`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ strategy, totalOrders, hotRestaurantShare }),
    signal: AbortSignal.timeout(15000),
  });
  assert.equal(res.status, 200);
  return res.json();
}

// 1. Hot-partition negative control: sharding by restaurant_id while restaurant 7 receives 80% of traffic creates severe skew
const skewed = await simulate('by-restaurant', 400, 0.80);
assert.equal(skewed.shards.length, 4);
assert.ok(skewed.maxShardPct >= 75, `Expected hot shard >= 75% under by-restaurant skew (got ${skewed.maxShardPct}%)`);
assert.ok(skewed.skewRatio >= 3.0, `Expected skewRatio >= 3.0 (got ${skewed.skewRatio})`);

// Scatter-gather on customer query when sharded by restaurant_id touches all 4 shards
const scatterCustRes = await fetch(`${base}/sharding/customer-orders?customerId=1042&strategy=by-restaurant&limit=10`, {
  signal: AbortSignal.timeout(10000),
});
assert.equal(scatterCustRes.status, 200);
const scatterCust = await scatterCustRes.json();
assert.equal(scatterCust.shardsTouched, 4);
assert.equal(scatterCust.returnedCount, 10);

// 2. Sharding by customer_id eliminates celebrity-restaurant hot partition and routes customer queries to 1 shard
const balancedByCust = await simulate('by-customer', 400, 0.80);
assert.ok(
  balancedByCust.maxShardPct <= 35,
  `Expected balanced shards <= 35% max under by-customer (got ${balancedByCust.maxShardPct}%)`,
);
assert.ok(balancedByCust.skewRatio < 1.5, `Expected skewRatio < 1.5 (got ${balancedByCust.skewRatio})`);

const routedCustRes = await fetch(`${base}/sharding/customer-orders?customerId=1042&strategy=by-customer&limit=10`, {
  signal: AbortSignal.timeout(10000),
});
assert.equal(routedCustRes.status, 200);
const routedCust = await routedCustRes.json();
assert.equal(routedCust.shardsTouched, 1);
assert.equal(routedCust.returnedCount, 10);
// Verify single-shard result matches descending time order
for (let i = 1; i < routedCust.items.length; i++) {
  assert.ok(
    new Date(routedCust.items[i - 1].createdAt) >= new Date(routedCust.items[i].createdAt),
    'Customer orders must be sorted newest first',
  );
}

// 3. Salted restaurant key (rest:7#salt:0..3) balances celebrity writes evenly across all 4 shards at the cost of 4-shard restaurant read fan-out
const salted = await simulate('salted-restaurant', 400, 0.80);
assert.ok(
  salted.maxShardPct <= 30,
  `Expected salted-restaurant to spread celebrity writes <= 30% max per shard (got ${salted.maxShardPct}%)`,
);
const saltedRestRes = await fetch(`${base}/sharding/restaurant-orders?restaurantId=7&strategy=salted-restaurant&limit=15`, {
  signal: AbortSignal.timeout(10000),
});
assert.equal(saltedRestRes.status, 200);
const saltedRest = await saltedRestRes.json();
assert.equal(saltedRest.shardsTouched, 4);
assert.equal(saltedRest.returnedCount, 15);
const distinctShardsHit = new Set(saltedRest.items.map(i => i.shardSchema));
assert.ok(distinctShardsHit.size >= 2, 'Salted celebrity restaurant orders must span multiple shards');

console.log(
  `Assertions passed: by-restaurant skew(max=${skewed.maxShardPct}%, ratio=${skewed.skewRatio}, custShards=${scatterCust.shardsTouched}), by-customer balanced(max=${balancedByCust.maxShardPct}%, ratio=${balancedByCust.skewRatio}, custShards=${routedCust.shardsTouched}), salted-restaurant balanced(max=${salted.maxShardPct}%, ratio=${salted.skewRatio}, restShards=${saltedRest.shardsTouched}).`,
);
