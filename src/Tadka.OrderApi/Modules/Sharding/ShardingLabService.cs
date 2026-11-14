using Npgsql;
using NpgsqlTypes;
using Tadka.OrderApi.Modules.DistributedIds;

namespace Tadka.OrderApi.Modules.Sharding;

public sealed record ShardBucketCount(int Shard, string Schema, long Count, double SharePct);

public sealed record ShardingSimulationResponse(
    string Strategy,
    int TotalOrders,
    long HotRestaurantId,
    double HotRestaurantShare,
    double MaxShardPct,
    double SkewRatio,
    IReadOnlyList<ShardBucketCount> Shards);

public sealed record ShardedOrderRow(
    string OrderIdString,
    long CustomerId,
    long RestaurantId,
    string ShardKey,
    string ShardSchema,
    decimal TotalAmount,
    string Status,
    DateTimeOffset CreatedAt);

public sealed record ShardedQueryResponse(
    string QueryKind,
    string Strategy,
    long KeyId,
    int ShardsTouched,
    IReadOnlyList<string> TargetedSchemas,
    int ReturnedCount,
    IReadOnlyList<ShardedOrderRow> Items);

public sealed class ShardingLabService(NpgsqlDataSource db, SnowflakeIdGenerator snowflake)
{
    private sealed record PendingShardedOrder(
        long OrderId,
        long CustomerId,
        long RestaurantId,
        string ShardKey,
        decimal TotalAmount,
        string Status,
        DateTimeOffset CreatedAt);

    public async Task ResetAsync(CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            TRUNCATE TABLE
                shard_0.sharded_orders,
                shard_1.sharded_orders,
                shard_2.sharded_orders,
                shard_3.sharded_orders;
            """;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<ShardingSimulationResponse> SimulateWorkloadAsync(
        string strategy,
        int totalOrders,
        double hotRestaurantShare,
        CancellationToken ct)
    {
        await ResetAsync(ct);

        const long hotRestaurantId = 7;
        var hotCount = (int)Math.Round(totalOrders * hotRestaurantShare);
        var otherRestaurants = Enumerable.Range(1, 20).Where(r => r != hotRestaurantId).ToArray();
        var perShard = Enumerable.Range(0, ShardRouter.ShardCount).Select(_ => new List<PendingShardedOrder>()).ToArray();
        var baseTime = DateTimeOffset.UtcNow.AddMinutes(-totalOrders);

        for (var i = 0; i < totalOrders; i++)
        {
            var isHot = i < hotCount;
            var restaurantId = isHot ? hotRestaurantId : otherRestaurants[i % otherRestaurants.Length];
            // Include customer 1042 across diverse restaurants so scatter-gather merges multi-shard hits.
            var customerId = i % 10 == 0 ? 1042L : 1000L + (i % 80);
            var (shard, shardKey) = ShardRouter.Resolve(strategy, customerId, restaurantId, i);
            perShard[shard].Add(new PendingShardedOrder(
                OrderId: snowflake.NextId(),
                CustomerId: customerId,
                RestaurantId: restaurantId,
                ShardKey: shardKey,
                TotalAmount: 180.00m + (i % 350),
                Status: "placed",
                CreatedAt: baseTime.AddSeconds(i)));
        }

        await using var conn = await db.OpenConnectionAsync(ct);
        for (var s = 0; s < ShardRouter.ShardCount; s++)
        {
            if (perShard[s].Count == 0)
                continue;

            var schema = ShardRouter.SchemaForShard(s);
            await InsertIntoShardAsync(conn, schema, perShard[s], ct);
        }

        var counts = new List<ShardBucketCount>(ShardRouter.ShardCount);
        for (var s = 0; s < ShardRouter.ShardCount; s++)
        {
            var schema = ShardRouter.SchemaForShard(s);
            await using var countCmd = conn.CreateCommand();
            countCmd.CommandText = $"SELECT COUNT(*) FROM {schema}.sharded_orders;";
            var c = (long)(await countCmd.ExecuteScalarAsync(ct))!;
            var pct = Math.Round(c * 100.0 / totalOrders, 2);
            counts.Add(new ShardBucketCount(s, schema, c, pct));
        }

        var avgCount = totalOrders / (double)ShardRouter.ShardCount;
        var maxCount = counts.Max(x => x.Count);
        var maxPct = counts.Max(x => x.SharePct);
        var skewRatio = Math.Round(maxCount / avgCount, 2);

        return new ShardingSimulationResponse(
            strategy,
            totalOrders,
            hotRestaurantId,
            hotRestaurantShare,
            maxPct,
            skewRatio,
            counts);
    }

    public async Task<ShardedQueryResponse> GetCustomerOrdersAsync(
        long customerId,
        string strategy,
        int limit,
        CancellationToken ct)
    {
        var targetedSchemas = strategy == "by-customer"
            ? new[] { ShardRouter.SchemaForShard(ShardRouter.RouteCustomer(customerId)) }
            : Enumerable.Range(0, ShardRouter.ShardCount).Select(ShardRouter.SchemaForShard).ToArray();

        var items = await QueryShardsAsync(targetedSchemas, "customer_id", customerId, limit, ct);
        return new ShardedQueryResponse(
            QueryKind: "customer-orders",
            Strategy: strategy,
            KeyId: customerId,
            ShardsTouched: targetedSchemas.Length,
            TargetedSchemas: targetedSchemas,
            ReturnedCount: items.Count,
            Items: items);
    }

    public async Task<ShardedQueryResponse> GetRestaurantOrdersAsync(
        long restaurantId,
        string strategy,
        int limit,
        CancellationToken ct)
    {
        var targetedSchemas = strategy == "by-restaurant"
            ? new[] { ShardRouter.SchemaForShard(ShardRouter.RouteRestaurant(restaurantId)) }
            : Enumerable.Range(0, ShardRouter.ShardCount).Select(ShardRouter.SchemaForShard).ToArray();

        var items = await QueryShardsAsync(targetedSchemas, "restaurant_id", restaurantId, limit, ct);
        return new ShardedQueryResponse(
            QueryKind: "restaurant-orders",
            Strategy: strategy,
            KeyId: restaurantId,
            ShardsTouched: targetedSchemas.Length,
            TargetedSchemas: targetedSchemas,
            ReturnedCount: items.Count,
            Items: items);
    }

    private static async Task InsertIntoShardAsync(
        NpgsqlConnection conn,
        string schema,
        IReadOnlyList<PendingShardedOrder> rows,
        CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            INSERT INTO {schema}.sharded_orders
                (order_id, customer_id, restaurant_id, shard_key, total_amount, status, created_at)
            SELECT oid, cid, rid, skey, amt, st, ts
            FROM UNNEST(@oids, @cids, @rids, @skeys, @amts, @sts, @tss)
                AS t(oid, cid, rid, skey, amt, st, ts);
            """;
        cmd.Parameters.Add(new NpgsqlParameter<long[]>("oids", NpgsqlDbType.Array | NpgsqlDbType.Bigint) { TypedValue = rows.Select(r => r.OrderId).ToArray() });
        cmd.Parameters.Add(new NpgsqlParameter<long[]>("cids", NpgsqlDbType.Array | NpgsqlDbType.Bigint) { TypedValue = rows.Select(r => r.CustomerId).ToArray() });
        cmd.Parameters.Add(new NpgsqlParameter<long[]>("rids", NpgsqlDbType.Array | NpgsqlDbType.Bigint) { TypedValue = rows.Select(r => r.RestaurantId).ToArray() });
        cmd.Parameters.Add(new NpgsqlParameter<string[]>("skeys", NpgsqlDbType.Array | NpgsqlDbType.Text) { TypedValue = rows.Select(r => r.ShardKey).ToArray() });
        cmd.Parameters.Add(new NpgsqlParameter<decimal[]>("amts", NpgsqlDbType.Array | NpgsqlDbType.Numeric) { TypedValue = rows.Select(r => r.TotalAmount).ToArray() });
        cmd.Parameters.Add(new NpgsqlParameter<string[]>("sts", NpgsqlDbType.Array | NpgsqlDbType.Text) { TypedValue = rows.Select(r => r.Status).ToArray() });
        cmd.Parameters.Add(new NpgsqlParameter<DateTimeOffset[]>("tss", NpgsqlDbType.Array | NpgsqlDbType.TimestampTz) { TypedValue = rows.Select(r => r.CreatedAt).ToArray() });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task<List<ShardedOrderRow>> QueryShardsAsync(
        string[] schemas,
        string filterColumn,
        long filterValue,
        int limit,
        CancellationToken ct)
    {
        var merged = new List<ShardedOrderRow>();
        await using var conn = await db.OpenConnectionAsync(ct);

        foreach (var schema in schemas)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                SELECT order_id, customer_id, restaurant_id, shard_key, total_amount, status, created_at
                FROM {schema}.sharded_orders
                WHERE {filterColumn} = @filterValue
                ORDER BY created_at DESC, order_id DESC
                LIMIT @limit;
                """;
            cmd.Parameters.AddWithValue("filterValue", filterValue);
            cmd.Parameters.AddWithValue("limit", limit);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                merged.Add(new ShardedOrderRow(
                    OrderIdString: reader.GetInt64(0).ToString(),
                    CustomerId: reader.GetInt64(1),
                    RestaurantId: reader.GetInt64(2),
                    ShardKey: reader.GetString(3),
                    ShardSchema: schema,
                    TotalAmount: reader.GetDecimal(4),
                    Status: reader.GetString(5),
                    CreatedAt: reader.GetFieldValue<DateTimeOffset>(6)));
            }
        }

        // Cross-shard merge-sort across scattered results
        return merged
            .OrderByDescending(r => r.CreatedAt)
            .ThenByDescending(r => long.Parse(r.OrderIdString))
            .Take(limit)
            .ToList();
    }
}
