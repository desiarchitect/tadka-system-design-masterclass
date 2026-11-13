using System.Diagnostics;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using Tadka.OrderApi.Data;
using Tadka.OrderApi.Instrumentation;

namespace Tadka.OrderApi.Controllers;

public sealed record CreateOrderRequest(long CustomerId, long RestaurantId, decimal TotalAmount);

[ApiController]
[Route("orders")]
public sealed class OrdersController(OrderStore store, ILogger<OrdersController> logger) : ControllerBase
{
    [HttpPost]
    public async Task<IActionResult> Create([FromBody] CreateOrderRequest request)
    {
        var requestId = HttpContext.TraceIdentifier;
        var started = Stopwatch.GetTimestamp();
        var timings = new RequestTimings();

        if (request.CustomerId <= 0 || request.RestaurantId <= 0 || request.TotalAmount <= 0)
        {
            Log(requestId, 400, timings, started);
            return Problem(statusCode: 400, title: "customerId, restaurantId and totalAmount must be positive.");
        }

        try
        {
            var id = await store.InsertAsync(
                new NewOrder(request.CustomerId, request.RestaurantId, request.TotalAmount), timings);

            Log(requestId, 201, timings, started);
            return Created($"/orders/{id}", new { id });
        }
        catch (Exception ex) when (ex is NpgsqlException or InvalidOperationException)
        {
            timings.StopWaitIfRunning();
            Log(requestId, 503, timings, started, ex);
            return Problem(statusCode: 503, title: "Could not place the order right now.");
        }
    }

    private void Log(string requestId, int status, RequestTimings t, long started, Exception? ex = null)
    {
        var totalMs = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        if (ex is null)
        {
            logger.LogInformation(
                "order request_id={RequestId} status={Status} wait_ms={WaitMs} db_ms={DbMs} total_ms={TotalMs}",
                requestId, status, RequestTimings.Format(t.WaitMs), RequestTimings.Format(t.DbMs), totalMs.ToString("0"));
        }
        else
        {
            logger.LogError(
                "order request_id={RequestId} status={Status} wait_ms={WaitMs} db_ms={DbMs} total_ms={TotalMs} error={ErrorType}: {ErrorMessage}",
                requestId, status, RequestTimings.Format(t.WaitMs), RequestTimings.Format(t.DbMs), totalMs.ToString("0"),
                ex.GetType().Name, ex.Message);
        }
    }
}
