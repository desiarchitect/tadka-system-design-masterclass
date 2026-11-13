using System.Globalization;
using Microsoft.AspNetCore.Mvc;
using Tadka.OrderApi.Data;

namespace Tadka.OrderApi.Controllers;

/// <summary>Local-lab order input. Customer identity is supplied, not authenticated.</summary>
public sealed record CreateOrderRequest(long CustomerId, long RestaurantId, decimal TotalAmount);
/// <summary>The stable create result, also returned on a matching retry.</summary>
public sealed record CreateOrderResponse(long Id);

[ApiController]
[Route("orders")]
public sealed class OrdersController(IOrderService orders, ILogger<OrdersController> logger, IWebHostEnvironment environment) : ControllerBase
{
    [HttpPost]
    [ProducesResponseType<CreateOrderResponse>(StatusCodes.Status201Created)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status400BadRequest)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status409Conflict)]
    public async Task<IActionResult> Create([FromBody] CreateOrderRequest request, CancellationToken ct)
    {
        if (request.CustomerId <= 0 || request.RestaurantId <= 0 || request.TotalAmount <= 0 ||
            request.TotalAmount > 99999999.99m || decimal.Round(request.TotalAmount, 2) != request.TotalAmount)
            return Problem(statusCode: 400, title: "Positive IDs and an amount with at most two decimal places are required.");
        var keys = Request.Headers["Idempotency-Key"];
        var key = keys.Count == 0 ? null : keys.ToString();
        if (keys.Count > 1 || key is not null && (key.Length is < 1 or > 128 || key.Any(c => !char.IsAsciiLetterOrDigit(c) && c != '-')))
            return Problem(statusCode: 400, title: "Idempotency-Key must contain 1 to 128 ASCII letters, digits or hyphens.");

        var result = await orders.CreateAsync(new(request.CustomerId, request.RestaurantId, request.TotalAmount), key, ct);
        if (result.Conflict)
        {
            logger.LogWarning("idempotency_conflict customer_id={CustomerId}", request.CustomerId);
            return Problem(statusCode: 409, title: "This key was already used with a different order.");
        }
        logger.LogInformation("order id={Id} replayed={Replayed} protected={Protected}", result.Id, result.Replayed, key is not null);
        Response.Headers["Idempotency-Replayed"] = result.Replayed ? "true" : "false";
        if (environment.IsEnvironment("Demo") && Request.Headers["X-Demo-Drop-Response"] == "true" && !result.Replayed)
        {
            logger.LogWarning("demo_response_dropped_after_commit order_id={Id}", result.Id);
            HttpContext.Abort();
            return new EmptyResult();
        }
        return CreatedAtAction(nameof(Get), new { id = result.Id }, new CreateOrderResponse(result.Id));
    }

    [HttpGet("{id:long}")]
    [ProducesResponseType<OrderResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> Get(long id, CancellationToken ct)
    {
        var order = await orders.GetAsync(id, ct);
        return order is null ? Problem(statusCode: 404, title: "Order not found.") : Ok(order);
    }

    [HttpGet]
    [ProducesResponseType<OrderListResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> List(long customerId, CancellationToken ct, int limit = 20,
        string pagination = "cursor", string? cursor = null, int offset = 0)
    {
        if (customerId <= 0 || limit is < 1 or > 100 || offset is < 0 or > 100000 ||
            pagination is not ("offset" or "cursor") || pagination == "cursor" && offset != 0 ||
            pagination == "offset" && cursor is not null)
            return Problem(statusCode: 400, title: "Use a positive customerId, limit 1..100 and one pagination mode.");
        long? before = null;
        if (cursor is not null)
        {
            var parts = cursor.Split(':');
            if (parts.Length != 2 || !long.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out var owner) ||
                owner != customerId || !long.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var id) || id <= 0)
                return Problem(statusCode: 400, title: "Cursor must belong to this customer and contain a positive order ID.");
            before = id;
        }
        var page = await orders.ListAsync(customerId, limit, before, offset, ct);
        return Ok(pagination == "offset" ? page with { NextCursor = null } : page);
    }
}
