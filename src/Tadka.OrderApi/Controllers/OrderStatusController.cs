using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using Tadka.OrderApi.Data;

namespace Tadka.OrderApi.Controllers;

/// <summary>A local operator's next status in the fixed demo workflow.</summary>
public sealed record ChangeOrderStatusRequest(string? Status);

[ApiController]
[Route("orders/{id:long}")]
public sealed class OrderStatusController(IOrderStatusService statuses, ILogger<OrderStatusController> logger) : ControllerBase
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    [HttpGet("status")]
    [ProducesResponseType<OrderStatusResponse>(StatusCodes.Status200OK)]
    public async Task<IActionResult> Get(long id, CancellationToken ct)
    {
        var value = await statuses.GetAsync(id, ct);
        return value is null ? Problem(statusCode: 404, title: "Order not found.") : Ok(value);
    }

    [HttpPatch("status")]
    [ProducesResponseType<OrderStatusResponse>(StatusCodes.Status200OK)]
    public async Task<IActionResult> Change(long id, ChangeOrderStatusRequest request, CancellationToken ct)
    {
        if (request.Status is not ("placed" or "preparing" or "ready" or "delivered"))
            return Problem(statusCode: 400, title: "Unknown order status.");
        var result = await statuses.ChangeAsync(id, request.Status, ct);
        if (result.Value is null) return Problem(statusCode: 404, title: "Order not found.");
        if (result.Conflict) return Problem(statusCode: 409, title: "Move only to the next status.");
        logger.LogInformation("status_changed order_id={Id} version={Version}", id, result.Value.Version);
        return Ok(result.Value);
    }

    [HttpGet("events")]
    [Produces("text/event-stream")]
    public async Task<IActionResult> Events(long id, CancellationToken ct)
    {
        var ids = Request.Headers["Last-Event-ID"];
        var hasCursor = ids.Count > 0;
        long after = 0;
        if (ids.Count > 1 || hasCursor && !long.TryParse(ids.ToString(), NumberStyles.None, CultureInfo.InvariantCulture, out after))
            return Problem(statusCode: 400, title: "Last-Event-ID must be a nonnegative integer for this order.");
        var current = await statuses.GetAsync(id, ct);
        if (current is null) return Problem(statusCode: 404, title: "Order not found.");
        if (after > current.Version)
            return Problem(statusCode: 409, title: "Event ID is ahead of this order. Reconnect without it for a snapshot.");

        Response.ContentType = "text/event-stream";
        Response.Headers.CacheControl = "no-cache, no-transform";
        Response.Headers["X-Accel-Buffering"] = "no";
        logger.LogInformation("sse_connected order_id={Id}", id);
        try
        {
            await Response.WriteAsync("retry: 1000\n: connected\n\n", ct);
            if (!hasCursor)
            {
                await WriteEventAsync("snapshot", current, ct);
                after = current.Version;
            }
            await Response.Body.FlushAsync(ct);
            var heartbeat = Stopwatch.StartNew();
            while (!ct.IsCancellationRequested)
            {
                // Each query disposes its connection before the subscriber waits.
                var events = await statuses.EventsAsync(id, after, ct);
                foreach (var item in events)
                {
                    await WriteEventAsync("order-status", item, ct);
                    after = item.Version;
                }
                if (heartbeat.Elapsed >= TimeSpan.FromSeconds(10))
                {
                    await Response.WriteAsync(": heartbeat\n\n", ct);
                    heartbeat.Restart();
                }
                await Response.Body.FlushAsync(ct);
                await Task.Delay(TimeSpan.FromMilliseconds(500), ct);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            logger.LogInformation("sse_cancelled order_id={Id}", id);
        }
        catch (Exception exception) when (exception is Npgsql.NpgsqlException or IOException)
        {
            logger.LogError(exception, "sse_failed_after_headers order_id={Id}", id);
            HttpContext.Abort();
        }
        finally
        {
            logger.LogInformation("sse_disconnected order_id={Id}", id);
        }
        return new EmptyResult();
    }

    private Task WriteEventAsync(string name, OrderStatusResponse value, CancellationToken ct) =>
        Response.WriteAsync($"id: {value.Version}\nevent: {name}\ndata: {JsonSerializer.Serialize(value, Json)}\n\n", ct);
}
