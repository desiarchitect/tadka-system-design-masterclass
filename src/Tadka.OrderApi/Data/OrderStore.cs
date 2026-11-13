using Npgsql;

namespace Tadka.OrderApi.Data;

public sealed record NewOrder(long CustomerId, long RestaurantId, decimal TotalAmount);
public sealed record CreateOrderResult(long Id, bool Replayed, bool Conflict);

/// <summary>One order in a bounded customer-scoped page.</summary>
public sealed record OrderResponse(long Id, long CustomerId, long RestaurantId, decimal TotalAmount, DateTimeOffset CreatedAt);
/// <summary>A page and an optional cursor for the next older page.</summary>
public sealed record OrderListResponse(IReadOnlyList<OrderResponse> Items, string? NextCursor);

public interface IOrderService
{
    Task<CreateOrderResult> CreateAsync(NewOrder order, string? key, CancellationToken ct);
    Task<OrderResponse?> GetAsync(long id, CancellationToken ct);
    Task<OrderListResponse> ListAsync(long customerId, int limit, long? before, int offset, CancellationToken ct);
}

public sealed class OrderStore(NpgsqlDataSource dataSource) : IOrderService
{
    public async Task<CreateOrderResult> CreateAsync(NewOrder order, string? key, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await using var insert = new NpgsqlCommand("""
            INSERT INTO orders (customer_id, restaurant_id, total_amount, idempotency_key)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (customer_id, idempotency_key) DO NOTHING
            RETURNING id;
            """, connection, transaction);
        insert.Parameters.AddWithValue(order.CustomerId);
        insert.Parameters.AddWithValue(order.RestaurantId);
        insert.Parameters.AddWithValue(order.TotalAmount);
        insert.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Text, (object?)key ?? DBNull.Value);
        var inserted = await insert.ExecuteScalarAsync(ct);
        if (inserted is long id)
        {
            await transaction.CommitAsync(ct);
            return new(id, false, false);
        }

        // READ COMMITTED gives this next statement a fresh snapshot after a concurrent winner commits.
        await using var existing = new NpgsqlCommand("""
            SELECT id, restaurant_id, total_amount FROM orders
            WHERE customer_id = $1 AND idempotency_key = $2;
            """, connection, transaction);
        existing.Parameters.AddWithValue(order.CustomerId);
        existing.Parameters.AddWithValue(key!);
        await using var reader = await existing.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            throw new InvalidOperationException("The winning idempotency record is missing.");
        var result = new CreateOrderResult(reader.GetInt64(0), true,
            reader.GetInt64(1) != order.RestaurantId || reader.GetDecimal(2) != order.TotalAmount);
        await reader.DisposeAsync();
        await transaction.CommitAsync(ct);
        return result;
    }

    public async Task<OrderResponse?> GetAsync(long id, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand("""
            SELECT id, customer_id, restaurant_id, total_amount, created_at FROM orders WHERE id = $1;
            """);
        command.Parameters.AddWithValue(id);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? ReadOrder(reader) : null;
    }

    public async Task<OrderListResponse> ListAsync(long customerId, int limit, long? before, int offset, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand("""
            SELECT id, customer_id, restaurant_id, total_amount, created_at FROM orders
            WHERE customer_id = $1 AND ($2::bigint IS NULL OR id < $2)
            ORDER BY id DESC LIMIT $3 OFFSET $4;
            """);
        command.Parameters.AddWithValue(customerId);
        command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Bigint, (object?)before ?? DBNull.Value);
        command.Parameters.AddWithValue(limit + 1);
        command.Parameters.AddWithValue(offset);
        var items = new List<OrderResponse>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            items.Add(ReadOrder(reader));
        var hasMore = items.Count > limit;
        if (hasMore) items.RemoveAt(items.Count - 1);
        var cursor = hasMore ? $"{customerId}:{items[^1].Id}" : null;
        return new(items, cursor);
    }

    private static OrderResponse ReadOrder(NpgsqlDataReader reader) =>
        new(reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetDecimal(3),
            new DateTimeOffset(reader.GetDateTime(4)));
}
