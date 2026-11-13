using Microsoft.Extensions.Options;
using Npgsql;
using Tadka.OrderApi.Configuration;
using Tadka.OrderApi.Instrumentation;

namespace Tadka.OrderApi.Data;

public sealed record NewOrder(long CustomerId, long RestaurantId, decimal TotalAmount);

public sealed class OrderStore(NpgsqlDataSource dataSource, IOptions<DemoOptions> demo)
{
    // One statement: wait for the demo hold time, then insert. pg_sleep returns a single
    // row, so the INSERT ... SELECT inserts exactly one order after the delay.
    private const string InsertSql = """
        INSERT INTO orders (customer_id, restaurant_id, total_amount)
        SELECT $1, $2, $3
        FROM pg_sleep($4)
        RETURNING id;
        """;

    public async Task<long> InsertAsync(NewOrder order, RequestTimings timings)
    {
        // CancellationToken.None on purpose: if the client gives up, the server still
        // finishes the work, the way a real server does. The demo stays honest.

        // 1. Wait timer: only the time spent getting a connection from the pool.
        timings.StartWait();
        await using var connection = await dataSource.OpenConnectionAsync(CancellationToken.None);
        timings.StopWait();

        // 2. DB timer: only the time the database command takes, delay included.
        await using var command = new NpgsqlCommand(InsertSql, connection);
        command.Parameters.AddWithValue(order.CustomerId);
        command.Parameters.AddWithValue(order.RestaurantId);
        command.Parameters.AddWithValue(order.TotalAmount);
        command.Parameters.AddWithValue(demo.Value.HoldMs / 1000.0);

        timings.StartDb();
        var id = (long)(await command.ExecuteScalarAsync(CancellationToken.None))!;
        timings.StopDb();

        return id;
        // 3. Leaving this method disposes the connection, which returns it to the pool,
        //    also when an exception is thrown above.
    }
}
