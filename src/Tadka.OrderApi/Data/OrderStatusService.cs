using Npgsql;

namespace Tadka.OrderApi.Data;

/// <summary>Persisted current order status or one ordered status event.</summary>
public sealed record OrderStatusResponse(long OrderId, string Status, long Version);
public sealed record StatusChangeResult(OrderStatusResponse? Value, bool Conflict);
public interface IOrderStatusService
{
    Task<OrderStatusResponse?> GetAsync(long id, CancellationToken ct);
    Task<StatusChangeResult> ChangeAsync(long id, string status, CancellationToken ct);
    Task<IReadOnlyList<OrderStatusResponse>> EventsAsync(long id, long after, CancellationToken ct);
}

public sealed class OrderStatusService(NpgsqlDataSource dataSource) : IOrderStatusService
{
    private static readonly string[] Stages = ["placed", "preparing", "ready", "delivered"];

    public async Task<OrderStatusResponse?> GetAsync(long id, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand("SELECT status, status_version FROM orders WHERE id=$1;");
        command.Parameters.AddWithValue(id);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? new(id, reader.GetString(0), reader.GetInt64(1)) : null;
    }

    public async Task<StatusChangeResult> ChangeAsync(long id, string status, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await using var select = new NpgsqlCommand("SELECT status, status_version FROM orders WHERE id=$1 FOR UPDATE;", connection, transaction);
        select.Parameters.AddWithValue(id);
        OrderStatusResponse current;
        await using (var reader = await select.ExecuteReaderAsync(ct))
        {
            if (!await reader.ReadAsync(ct)) return new(null, false);
            current = new(id, reader.GetString(0), reader.GetInt64(1));
        }
        if (current.Status == status) return new(current, false);
        if (Array.IndexOf(Stages, status) != Array.IndexOf(Stages, current.Status) + 1)
            return new(current, true);

        var next = current with { Status = status, Version = current.Version + 1 };
        await using var update = new NpgsqlCommand("""
            WITH changed AS (
                UPDATE orders SET status=$2, status_version=$3 WHERE id=$1
                RETURNING id, status_version, status
            )
            INSERT INTO order_status_events (order_id, version, status)
            SELECT id, status_version, status FROM changed;
            """, connection, transaction);
        update.Parameters.AddWithValue(id);
        update.Parameters.AddWithValue(status);
        update.Parameters.AddWithValue(next.Version);
        await update.ExecuteNonQueryAsync(ct);
        await transaction.CommitAsync(ct);
        return new(next, false);
    }

    public async Task<IReadOnlyList<OrderStatusResponse>> EventsAsync(long id, long after, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand("""
            SELECT version, status FROM order_status_events
            WHERE order_id=$1 AND version>$2 ORDER BY version LIMIT 100;
            """);
        command.Parameters.AddWithValue(id);
        command.Parameters.AddWithValue(after);
        var events = new List<OrderStatusResponse>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) events.Add(new(id, reader.GetString(1), reader.GetInt64(0)));
        return events;
    }
}
