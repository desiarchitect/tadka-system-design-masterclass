# Lesson 18 Contract: Partitioning, Sharding Keys, Hot Partitions & Scatter-Gather

Branch: `lesson-18-sharding`

## Schema Topology
- Four logical PostgreSQL schemas (`shard_0`, `shard_1`, `shard_2`, `shard_3`), each owning an independent `sharded_orders` table using Snowflake 64-bit primary keys (`order_id`) and indexes on `(customer_id, created_at DESC, order_id DESC)` and `(restaurant_id, created_at DESC, order_id DESC)`.

## Endpoints
- `POST /sharding/reset`: Truncates all 4 shard tables.
- `POST /sharding/simulate-workload`: Simulates `totalOrders` (default 400) where `hotRestaurantShare` (default `0.80`) hits celebrity restaurant `7`, routed using `by-restaurant`, `by-customer`, or `salted-restaurant`, returning per-shard counts and skew ratios.
- `GET /sharding/customer-orders?customerId=1042&strategy=...&limit=20`: Fetches a customer's recent orders. Uses 1-shard point routing under `by-customer` (`shardsTouched = 1`) vs full scatter-gather merge-sort across all 4 shards under `by-restaurant` or `salted-restaurant` (`shardsTouched = 4`).
- `GET /sharding/restaurant-orders?restaurantId=7&strategy=...&limit=20`: Fetches a restaurant's recent orders. Uses 1-shard point routing under `by-restaurant` (`shardsTouched = 1`) vs 4-shard scatter-gather merge-sort under `by-customer` or `salted-restaurant` (`shardsTouched = 4`).
