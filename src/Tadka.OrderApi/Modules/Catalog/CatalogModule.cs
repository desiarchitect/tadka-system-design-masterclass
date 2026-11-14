using Npgsql;

namespace Tadka.OrderApi.Modules.Catalog;

/// <summary>Owns all production queries to the catalog PostgreSQL schema.</summary>
public sealed class CatalogModule(NpgsqlDataSource dataSource) : ICatalogModule
{
    public async Task<CatalogMenuResponse?> GetMenuAsync(long restaurantId, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        string? restaurantName = null;
        var isOpen = false;
        await using (var restaurantCmd = new NpgsqlCommand("""
            SELECT name, is_open
            FROM catalog.restaurants
            WHERE id = $1;
            """, connection))
        {
            restaurantCmd.Parameters.AddWithValue(restaurantId);
            await using var reader = await restaurantCmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
                return null;
            restaurantName = reader.GetString(0);
            isOpen = reader.GetBoolean(1);
        }

        var items = new List<CatalogMenuItem>();
        await using (var itemsCmd = new NpgsqlCommand("""
            SELECT id, restaurant_id, name, price, is_available, version
            FROM catalog.menu_items
            WHERE restaurant_id = $1
            ORDER BY id;
            """, connection))
        {
            itemsCmd.Parameters.AddWithValue(restaurantId);
            await using var reader = await itemsCmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                items.Add(new CatalogMenuItem(
                    reader.GetInt64(0),
                    reader.GetInt64(1),
                    reader.GetString(2),
                    reader.GetDecimal(3),
                    reader.GetBoolean(4),
                    reader.GetInt64(5)));
            }
        }

        return new CatalogMenuResponse(restaurantId, restaurantName!, isOpen, items);
    }

    public async Task<CatalogQuoteResult> QuoteItemsAsync(
        long restaurantId,
        IReadOnlyList<RequestedOrderItem> items,
        CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        string? restaurantName = null;
        var isOpen = false;
        await using (var restaurantCmd = new NpgsqlCommand("""
            SELECT name, is_open
            FROM catalog.restaurants
            WHERE id = $1;
            """, connection))
        {
            restaurantCmd.Parameters.AddWithValue(restaurantId);
            await using var reader = await restaurantCmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
                return new(false, false, "Restaurant not found.", null, 0m, []);
            restaurantName = reader.GetString(0);
            isOpen = reader.GetBoolean(1);
        }

        if (!isOpen)
            return new(true, true, $"Restaurant '{restaurantName}' is currently closed.", restaurantName, 0m, []);

        var requestedIds = items.Select(i => i.MenuItemId).ToArray();
        var byId = new Dictionary<long, (long RestaurantId, string Name, decimal Price, bool IsAvailable)>();

        await using (var itemsCmd = new NpgsqlCommand("""
            SELECT id, restaurant_id, name, price, is_available
            FROM catalog.menu_items
            WHERE id = ANY($1);
            """, connection))
        {
            itemsCmd.Parameters.AddWithValue(requestedIds);
            await using var reader = await itemsCmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                byId[reader.GetInt64(0)] = (
                    reader.GetInt64(1),
                    reader.GetString(2),
                    reader.GetDecimal(3),
                    reader.GetBoolean(4));
            }
        }

        var quoted = new List<QuotedOrderItem>(items.Count);
        decimal total = 0m;

        foreach (var req in items)
        {
            if (!byId.TryGetValue(req.MenuItemId, out var row))
                return new(true, true, $"Menu item {req.MenuItemId} does not exist.", restaurantName, 0m, []);
            if (row.RestaurantId != restaurantId)
                return new(true, true, $"Menu item {req.MenuItemId} does not belong to restaurant {restaurantId}.", restaurantName, 0m, []);
            if (!row.IsAvailable)
                return new(true, true, $"Menu item '{row.Name}' is currently unavailable.", restaurantName, 0m, []);

            var lineTotal = row.Price * req.Quantity;
            total += lineTotal;
            quoted.Add(new QuotedOrderItem(req.MenuItemId, row.Name, row.Price, req.Quantity, lineTotal));
        }

        return new(true, false, null, restaurantName, total, quoted);
    }

    public async Task<CatalogMenuItem?> UpdateItemAsync(
        long restaurantId,
        long itemId,
        decimal? price,
        bool? isAvailable,
        CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand("""
            UPDATE catalog.menu_items
            SET price = COALESCE($3, price),
                is_available = COALESCE($4, is_available),
                version = version + 1
            WHERE restaurant_id = $1 AND id = $2
            RETURNING id, restaurant_id, name, price, is_available, version;
            """);
        command.Parameters.AddWithValue(restaurantId);
        command.Parameters.AddWithValue(itemId);
        command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Numeric, (object?)price ?? DBNull.Value);
        command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Boolean, (object?)isAvailable ?? DBNull.Value);

        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return null;

        return new CatalogMenuItem(
            reader.GetInt64(0),
            reader.GetInt64(1),
            reader.GetString(2),
            reader.GetDecimal(3),
            reader.GetBoolean(4),
            reader.GetInt64(5));
    }
}
