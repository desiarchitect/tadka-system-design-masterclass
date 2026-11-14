using System.Text.Json;
using Npgsql;

namespace Tadka.OrderApi.Modules.Indexing;

public sealed record IndexingModeResponse(
    string Mode,
    IReadOnlyList<string> ActiveIndexes,
    long TableBytes,
    long IndexesBytes,
    long EstimatedRows);

public sealed record KitchenFeedItem(
    long Id,
    long CustomerId,
    decimal TotalAmount,
    DateTimeOffset CreatedAt);

public sealed record ExplainPlanSummary(
    string NodeType,
    string? IndexName,
    string? IndexCond,
    string? Filter,
    string? SortMethod,
    long ActualRows,
    long RowsRemovedByFilter,
    long HeapFetches,
    long SharedHitBlocks,
    long SharedReadBlocks,
    double ExecutionTimeMs);

public sealed record KitchenFeedResponse(
    IReadOnlyList<KitchenFeedItem> Items,
    ExplainPlanSummary? Plan);

public sealed class IndexingLabService(NpgsqlDataSource dataSource)
{
    private const string CompositeIndexName = "orders_restaurant_status_created_idx";
    private const string CoveringIndexName = "orders_restaurant_status_covering_idx";

    public async Task<IndexingModeResponse> SetModeAsync(string mode, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        switch (mode)
        {
            case "none":
                await ExecuteSqlAsync(connection, $"DROP INDEX IF EXISTS {CompositeIndexName};", ct);
                await ExecuteSqlAsync(connection, $"DROP INDEX IF EXISTS {CoveringIndexName};", ct);
                await ExecuteSqlAsync(connection, "ANALYZE orders;", ct);
                break;

            case "composite":
                await ExecuteSqlAsync(connection, $"DROP INDEX IF EXISTS {CoveringIndexName};", ct);
                await ExecuteSqlAsync(connection, $"""
                    CREATE INDEX IF NOT EXISTS {CompositeIndexName}
                    ON orders (restaurant_id, status, created_at DESC);
                    """, ct);
                await ExecuteSqlAsync(connection, "ANALYZE orders;", ct);
                break;

            case "covering":
                await ExecuteSqlAsync(connection, $"DROP INDEX IF EXISTS {CompositeIndexName};", ct);
                await ExecuteSqlAsync(connection, $"""
                    CREATE INDEX IF NOT EXISTS {CoveringIndexName}
                    ON orders (restaurant_id, status, created_at DESC)
                    INCLUDE (id, customer_id, total_amount);
                    """, ct);
                // Visibility map must mark heap pages all-visible so Index-Only Scan avoids heap fetches.
                await ExecuteSqlAsync(connection, "VACUUM ANALYZE orders;", ct);
                break;

            default:
                throw new ArgumentException($"Unsupported mode '{mode}'.", nameof(mode));
        }

        return await InspectIndexesAsync(connection, mode, ct);
    }

    public async Task<IndexingModeResponse> ReseedAsync(CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        await using var countCmd = new NpgsqlCommand("SELECT COUNT(*) FROM orders;", connection);
        var currentCount = Convert.ToInt64(await countCmd.ExecuteScalarAsync(ct));

        if (currentCount < 25000)
        {
            await ExecuteSqlAsync(connection, """
                INSERT INTO orders (customer_id, restaurant_id, total_amount, created_at, status, status_version)
                SELECT
                    1000 + (i % 500),
                    1 + (i % 20),
                    ROUND((120 + (i % 480))::numeric, 2),
                    NOW() - (i * INTERVAL '1 minute'),
                    CASE (((i - 1) / 20) % 4)
                        WHEN 0 THEN 'placed'
                        WHEN 1 THEN 'preparing'
                        WHEN 2 THEN 'ready'
                        ELSE 'delivered'
                    END,
                    (((i - 1) / 20) % 4)
                FROM generate_series(1, 25000) AS s(i);
                """, ct);
            await ExecuteSqlAsync(connection, "VACUUM ANALYZE orders;", ct);
        }

        var current = await InspectIndexesAsync(connection, "current", ct);
        var inferredMode = current.ActiveIndexes.Contains(CoveringIndexName)
            ? "covering"
            : current.ActiveIndexes.Contains(CompositeIndexName)
                ? "composite"
                : "none";
        return current with { Mode = inferredMode };
    }

    public async Task<KitchenFeedResponse> GetKitchenFeedAsync(
        long restaurantId,
        string status,
        int limit,
        bool includeExplain,
        CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        var items = new List<KitchenFeedItem>();
        const string sql = """
            SELECT id, customer_id, total_amount, created_at
            FROM orders
            WHERE restaurant_id = $1 AND status = $2
            ORDER BY created_at DESC
            LIMIT $3;
            """;

        await using (var cmd = new NpgsqlCommand(sql, connection))
        {
            cmd.Parameters.AddWithValue(restaurantId);
            cmd.Parameters.AddWithValue(status);
            cmd.Parameters.AddWithValue(limit);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                items.Add(new KitchenFeedItem(
                    reader.GetInt64(0),
                    reader.GetInt64(1),
                    reader.GetDecimal(2),
                    reader.GetFieldValue<DateTimeOffset>(3)));
            }
        }

        ExplainPlanSummary? plan = null;
        if (includeExplain)
        {
            plan = await RunExplainAsync(
                connection,
                $"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}",
                cmd =>
                {
                    cmd.Parameters.AddWithValue(restaurantId);
                    cmd.Parameters.AddWithValue(status);
                    cmd.Parameters.AddWithValue(limit);
                },
                ct);
        }

        return new KitchenFeedResponse(items, plan);
    }

    public async Task<KitchenFeedResponse> GetStatusScanAsync(
        string status,
        int limit,
        bool includeExplain,
        CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        var items = new List<KitchenFeedItem>();
        const string sql = """
            SELECT id, customer_id, total_amount, created_at
            FROM orders
            WHERE status = $1
            ORDER BY created_at DESC
            LIMIT $2;
            """;

        await using (var cmd = new NpgsqlCommand(sql, connection))
        {
            cmd.Parameters.AddWithValue(status);
            cmd.Parameters.AddWithValue(limit);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                items.Add(new KitchenFeedItem(
                    reader.GetInt64(0),
                    reader.GetInt64(1),
                    reader.GetDecimal(2),
                    reader.GetFieldValue<DateTimeOffset>(3)));
            }
        }

        ExplainPlanSummary? plan = null;
        if (includeExplain)
        {
            plan = await RunExplainAsync(
                connection,
                $"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}",
                cmd =>
                {
                    cmd.Parameters.AddWithValue(status);
                    cmd.Parameters.AddWithValue(limit);
                },
                ct);
        }

        return new KitchenFeedResponse(items, plan);
    }

    private static async Task<IndexingModeResponse> InspectIndexesAsync(
        NpgsqlConnection connection,
        string mode,
        CancellationToken ct)
    {
        var indexes = new List<string>();
        await using (var idxCmd = new NpgsqlCommand("""
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public' AND tablename = 'orders'
            ORDER BY indexname;
            """, connection))
        await using (var reader = await idxCmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
                indexes.Add(reader.GetString(0));
        }

        await using var sizeCmd = new NpgsqlCommand("""
            SELECT
                pg_relation_size('orders'),
                pg_indexes_size('orders'),
                (SELECT COUNT(*) FROM orders);
            """, connection);
        await using var sizeReader = await sizeCmd.ExecuteReaderAsync(ct);
        await sizeReader.ReadAsync(ct);

        return new IndexingModeResponse(
            mode,
            indexes,
            sizeReader.GetInt64(0),
            sizeReader.GetInt64(1),
            sizeReader.GetInt64(2));
    }

    private static async Task<ExplainPlanSummary> RunExplainAsync(
        NpgsqlConnection connection,
        string explainSql,
        Action<NpgsqlCommand> bindParameters,
        CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(explainSql, connection);
        bindParameters(cmd);
        var jsonText = (string?)await cmd.ExecuteScalarAsync(ct)
            ?? throw new InvalidOperationException("EXPLAIN returned no JSON payload.");

        using var doc = JsonDocument.Parse(jsonText);
        var root = doc.RootElement[0];
        var execMs = root.TryGetProperty("Execution Time", out var execProp) ? execProp.GetDouble() : 0.0;
        var topPlan = root.GetProperty("Plan");

        var scanNode = FindPrimaryScanNode(topPlan) ?? topPlan;
        var sortNode = FindNodeByType(topPlan, "Sort");

        var nodeType = scanNode.GetProperty("Node Type").GetString() ?? "Unknown";
        var indexName = scanNode.TryGetProperty("Index Name", out var idxProp) ? idxProp.GetString() : null;
        var indexCond = scanNode.TryGetProperty("Index Cond", out var condProp) ? condProp.GetString() : null;
        var filter = scanNode.TryGetProperty("Filter", out var filterProp) ? filterProp.GetString() : null;
        var sortMethod = sortNode?.TryGetProperty("Sort Method", out var sortProp) == true ? sortProp.GetString() : null;
        var actualRows = scanNode.TryGetProperty("Actual Rows", out var rowsProp) ? rowsProp.GetInt64() : 0L;
        var removed = scanNode.TryGetProperty("Rows Removed by Filter", out var remProp) ? remProp.GetInt64() : 0L;
        var heapFetches = scanNode.TryGetProperty("Heap Fetches", out var heapProp) ? heapProp.GetInt64() : 0L;
        var sharedHit = topPlan.TryGetProperty("Shared Hit Blocks", out var hitProp) ? hitProp.GetInt64() : 0L;
        var sharedRead = topPlan.TryGetProperty("Shared Read Blocks", out var readProp) ? readProp.GetInt64() : 0L;

        return new ExplainPlanSummary(
            nodeType,
            indexName,
            indexCond,
            filter,
            sortMethod,
            actualRows,
            removed,
            heapFetches,
            sharedHit,
            sharedRead,
            execMs);
    }

    private static JsonElement? FindPrimaryScanNode(JsonElement node)
    {
        if (node.TryGetProperty("Node Type", out var typeProp))
        {
            var type = typeProp.GetString();
            if (type is "Index Only Scan" or "Index Scan" or "Bitmap Heap Scan" or "Seq Scan")
                return node;
        }

        if (node.TryGetProperty("Plans", out var children) && children.ValueKind == JsonValueKind.Array)
        {
            foreach (var child in children.EnumerateArray())
            {
                var found = FindPrimaryScanNode(child);
                if (found.HasValue)
                    return found;
            }
        }

        return null;
    }

    private static JsonElement? FindNodeByType(JsonElement node, string targetType)
    {
        if (node.TryGetProperty("Node Type", out var typeProp) && typeProp.GetString() == targetType)
            return node;

        if (node.TryGetProperty("Plans", out var children) && children.ValueKind == JsonValueKind.Array)
        {
            foreach (var child in children.EnumerateArray())
            {
                var found = FindNodeByType(child, targetType);
                if (found.HasValue)
                    return found;
            }
        }

        return null;
    }

    private static async Task ExecuteSqlAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, connection);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
