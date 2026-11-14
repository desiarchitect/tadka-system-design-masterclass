using Npgsql;
using Tadka.OrderApi.Modules.Catalog;

namespace Tadka.OrderApi.Modules.Orders;

public sealed record CheckoutRequest(long CustomerId, long RestaurantId, IReadOnlyList<RequestedOrderItem> Items);
public sealed record CheckoutResponse(long Id, decimal TotalAmount, bool Replayed);
public sealed record OrderReceiptLine(long MenuItemId, string ItemName, decimal UnitPrice, int Quantity, decimal LineTotal);

public sealed record OrderReceiptResponse(
    long OrderId,
    long CustomerId,
    long RestaurantId,
    string Status,
    long StatusVersion,
    decimal TotalAmount,
    IReadOnlyList<OrderReceiptLine> Items);

public sealed record NaiveJoinReceiptResponse(
    long OrderId,
    decimal CommittedOrderTotal,
    decimal LiveJoinedTotal,
    bool MismatchWithCommittedTotal,
    IReadOnlyList<OrderReceiptLine> Items);

public sealed record CheckoutResult(
    long Id,
    decimal TotalAmount,
    bool Replayed,
    bool Conflict,
    bool RestaurantFound,
    string? ErrorReason);

public interface IOrderCheckoutService
{
    Task<CheckoutResult> CheckoutAsync(long customerId, long restaurantId, IReadOnlyList<RequestedOrderItem> items, string? key, CancellationToken ct);
    Task<OrderReceiptResponse?> GetReceiptAsync(long orderId, CancellationToken ct);
    Task<NaiveJoinReceiptResponse?> GetNaiveJoinReceiptAsync(long orderId, CancellationToken ct);
}

/// <summary>Owns public.orders and public.order_items; requests menu quotes through ICatalogModule.</summary>
public sealed class OrderCheckoutService(NpgsqlDataSource dataSource, ICatalogModule catalog) : IOrderCheckoutService
{
    public async Task<CheckoutResult> CheckoutAsync(
        long customerId,
        long restaurantId,
        IReadOnlyList<RequestedOrderItem> items,
        string? key,
        CancellationToken ct)
    {
        var normalized = items.OrderBy(i => i.MenuItemId).ToArray();

        if (key is not null)
        {
            var existing = await TryFindExistingAsync(customerId, restaurantId, normalized, key, ct);
            if (existing is not null)
                return existing;
        }

        // Cross-module call goes through ICatalogModule contract, never a cross-schema SQL join.
        var quote = await catalog.QuoteItemsAsync(restaurantId, normalized, ct);
        if (!quote.RestaurantFound)
            return new(0, 0m, false, false, false, quote.ErrorReason);
        if (quote.Conflict)
            return new(0, 0m, false, true, true, quote.ErrorReason);

        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);

        await using var insertOrder = new NpgsqlCommand("""
            INSERT INTO orders (customer_id, restaurant_id, total_amount, idempotency_key)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (customer_id, idempotency_key) DO NOTHING
            RETURNING id;
            """, connection, transaction);
        insertOrder.Parameters.AddWithValue(customerId);
        insertOrder.Parameters.AddWithValue(restaurantId);
        insertOrder.Parameters.AddWithValue(quote.TotalAmount);
        insertOrder.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Text, (object?)key ?? DBNull.Value);

        var inserted = await insertOrder.ExecuteScalarAsync(ct);
        if (inserted is long orderId)
        {
            foreach (var line in quote.Items)
            {
                await using var insertItem = new NpgsqlCommand("""
                    INSERT INTO order_items (order_id, menu_item_id, item_name_snapshot, unit_price_snapshot, quantity)
                    VALUES ($1, $2, $3, $4, $5);
                    """, connection, transaction);
                insertItem.Parameters.AddWithValue(orderId);
                insertItem.Parameters.AddWithValue(line.MenuItemId);
                insertItem.Parameters.AddWithValue(line.Name);
                insertItem.Parameters.AddWithValue(line.UnitPrice);
                insertItem.Parameters.AddWithValue(line.Quantity);
                await insertItem.ExecuteNonQueryAsync(ct);
            }

            await transaction.CommitAsync(ct);
            return new(orderId, quote.TotalAmount, false, false, true, null);
        }

        // READ COMMITTED gives this statement a fresh snapshot after the concurrent winner commits.
        var winner = await ReadWinnerInTransactionAsync(connection, transaction, customerId, restaurantId, normalized, key!, ct);
        await transaction.CommitAsync(ct);
        return winner;
    }

    public async Task<OrderReceiptResponse?> GetReceiptAsync(long orderId, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        OrderReceiptResponse? header = null;
        await using (var orderCmd = new NpgsqlCommand("""
            SELECT id, customer_id, restaurant_id, status, status_version, total_amount
            FROM orders
            WHERE id = $1;
            """, connection))
        {
            orderCmd.Parameters.AddWithValue(orderId);
            await using var reader = await orderCmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
                return null;
            header = new OrderReceiptResponse(
                reader.GetInt64(0),
                reader.GetInt64(1),
                reader.GetInt64(2),
                reader.GetString(3),
                reader.GetInt64(4),
                reader.GetDecimal(5),
                []);
        }

        var items = new List<OrderReceiptLine>();
        await using (var itemsCmd = new NpgsqlCommand("""
            SELECT menu_item_id, item_name_snapshot, unit_price_snapshot, quantity
            FROM order_items
            WHERE order_id = $1
            ORDER BY menu_item_id;
            """, connection))
        {
            itemsCmd.Parameters.AddWithValue(orderId);
            await using var reader = await itemsCmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var unitPrice = reader.GetDecimal(2);
                var qty = reader.GetInt32(3);
                items.Add(new OrderReceiptLine(
                    reader.GetInt64(0),
                    reader.GetString(1),
                    unitPrice,
                    qty,
                    unitPrice * qty));
            }
        }

        return header! with { Items = items };
    }

    /// <summary>
    /// Demo negative control only: intentionally joins across the catalog schema boundary
    /// to show receipt drift when live menu prices change after order placement.
    /// </summary>
    public async Task<NaiveJoinReceiptResponse?> GetNaiveJoinReceiptAsync(long orderId, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        decimal committedTotal;
        await using (var orderCmd = new NpgsqlCommand("SELECT total_amount FROM orders WHERE id = $1;", connection))
        {
            orderCmd.Parameters.AddWithValue(orderId);
            var scalar = await orderCmd.ExecuteScalarAsync(ct);
            if (scalar is null or DBNull)
                return null;
            committedTotal = (decimal)scalar;
        }

        var items = new List<OrderReceiptLine>();
        decimal liveTotal = 0m;
        await using (var joinCmd = new NpgsqlCommand("""
            SELECT oi.menu_item_id, m.name, m.price, oi.quantity
            FROM order_items oi
            JOIN catalog.menu_items m ON m.id = oi.menu_item_id
            WHERE oi.order_id = $1
            ORDER BY oi.menu_item_id;
            """, connection))
        {
            joinCmd.Parameters.AddWithValue(orderId);
            await using var reader = await joinCmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var price = reader.GetDecimal(2);
                var qty = reader.GetInt32(3);
                var lineTotal = price * qty;
                liveTotal += lineTotal;
                items.Add(new OrderReceiptLine(
                    reader.GetInt64(0),
                    reader.GetString(1),
                    price,
                    qty,
                    lineTotal));
            }
        }

        return new NaiveJoinReceiptResponse(
            orderId,
            committedTotal,
            liveTotal,
            liveTotal != committedTotal,
            items);
    }

    private async Task<CheckoutResult?> TryFindExistingAsync(
        long customerId,
        long restaurantId,
        RequestedOrderItem[] normalized,
        string key,
        CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        return await ReadWinnerCoreAsync(connection, null, customerId, restaurantId, normalized, key, ct);
    }

    private static async Task<CheckoutResult> ReadWinnerInTransactionAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long customerId,
        long restaurantId,
        RequestedOrderItem[] normalized,
        string key,
        CancellationToken ct)
    {
        var result = await ReadWinnerCoreAsync(connection, transaction, customerId, restaurantId, normalized, key, ct);
        return result ?? throw new InvalidOperationException("The winning idempotency record is missing.");
    }

    private static async Task<CheckoutResult?> ReadWinnerCoreAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        long customerId,
        long restaurantId,
        RequestedOrderItem[] normalized,
        string key,
        CancellationToken ct)
    {
        long existingId;
        long existingRestaurantId;
        decimal existingTotal;

        await using (var orderCmd = new NpgsqlCommand("""
            SELECT id, restaurant_id, total_amount
            FROM orders
            WHERE customer_id = $1 AND idempotency_key = $2;
            """, connection, transaction))
        {
            orderCmd.Parameters.AddWithValue(customerId);
            orderCmd.Parameters.AddWithValue(key);
            await using var reader = await orderCmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
                return null;
            existingId = reader.GetInt64(0);
            existingRestaurantId = reader.GetInt64(1);
            existingTotal = reader.GetDecimal(2);
        }

        var existingItems = new List<RequestedOrderItem>();
        await using (var itemsCmd = new NpgsqlCommand("""
            SELECT menu_item_id, quantity
            FROM order_items
            WHERE order_id = $1
            ORDER BY menu_item_id;
            """, connection, transaction))
        {
            itemsCmd.Parameters.AddWithValue(existingId);
            await using var reader = await itemsCmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                existingItems.Add(new RequestedOrderItem(reader.GetInt64(0), reader.GetInt32(1)));
        }

        var same = existingRestaurantId == restaurantId && existingItems.SequenceEqual(normalized);
        return new CheckoutResult(
            existingId,
            existingTotal,
            true,
            !same,
            true,
            same ? null : "This key was already used with a different order.");
    }
}
