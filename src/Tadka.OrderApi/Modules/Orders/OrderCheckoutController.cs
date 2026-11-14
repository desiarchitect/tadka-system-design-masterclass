using Microsoft.AspNetCore.Mvc;

namespace Tadka.OrderApi.Modules.Orders;

[ApiController]
[Route("orders")]
public sealed class OrderCheckoutController(
    IOrderCheckoutService checkoutService,
    ILogger<OrderCheckoutController> logger,
    IWebHostEnvironment environment) : ControllerBase
{
    [HttpPost("checkout")]
    [ProducesResponseType<CheckoutResponse>(StatusCodes.Status201Created)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status400BadRequest)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status404NotFound)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status409Conflict)]
    public async Task<IActionResult> Checkout([FromBody] CheckoutRequest request, CancellationToken ct)
    {
        if (request.CustomerId <= 0 || request.RestaurantId <= 0 || request.Items is null || request.Items.Count is < 1 or > 20)
            return Problem(statusCode: 400, title: "Positive customerId, restaurantId and 1 to 20 items are required.");

        var seen = new HashSet<long>();
        foreach (var item in request.Items)
        {
            if (item.MenuItemId <= 0 || item.Quantity is < 1 or > 50 || !seen.Add(item.MenuItemId))
                return Problem(statusCode: 400, title: "Each item needs a unique positive menuItemId and quantity 1..50.");
        }

        var keys = Request.Headers["Idempotency-Key"];
        var key = keys.Count == 0 ? null : keys.ToString();
        if (keys.Count > 1 || key is not null && (key.Length is < 1 or > 128 || key.Any(c => !char.IsAsciiLetterOrDigit(c) && c != '-')))
            return Problem(statusCode: 400, title: "Idempotency-Key must contain 1 to 128 ASCII letters, digits or hyphens.");

        var result = await checkoutService.CheckoutAsync(request.CustomerId, request.RestaurantId, request.Items, key, ct);
        if (!result.RestaurantFound)
            return Problem(statusCode: 404, title: result.ErrorReason ?? "Restaurant not found.");
        if (result.Conflict)
        {
            logger.LogWarning("checkout_conflict customer_id={CustomerId} reason={Reason}", request.CustomerId, result.ErrorReason);
            return Problem(statusCode: 409, title: result.ErrorReason ?? "Order checkout conflict.");
        }

        logger.LogInformation("checkout_order id={Id} total={Total} replayed={Replayed}", result.Id, result.TotalAmount, result.Replayed);
        Response.Headers["Idempotency-Replayed"] = result.Replayed ? "true" : "false";
        return CreatedAtAction(nameof(Receipt), new { id = result.Id }, new CheckoutResponse(result.Id, result.TotalAmount, result.Replayed));
    }

    [HttpGet("{id:long}/receipt")]
    [ProducesResponseType<OrderReceiptResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> Receipt(long id, CancellationToken ct)
    {
        var receipt = await checkoutService.GetReceiptAsync(id, ct);
        return receipt is null ? Problem(statusCode: 404, title: "Order not found.") : Ok(receipt);
    }

    [HttpGet("{id:long}/naive-join-receipt")]
    [ProducesResponseType<NaiveJoinReceiptResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> NaiveJoinReceipt(long id, CancellationToken ct)
    {
        if (!environment.IsEnvironment("Demo"))
            return Problem(statusCode: 404, title: "Demo diagnostic endpoint is disabled outside Demo environment.");

        var receipt = await checkoutService.GetNaiveJoinReceiptAsync(id, ct);
        return receipt is null ? Problem(statusCode: 404, title: "Order not found.") : Ok(receipt);
    }
}
