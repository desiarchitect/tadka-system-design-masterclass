using Microsoft.AspNetCore.Mvc;

namespace Tadka.OrderApi.Modules.Catalog;

public sealed record ResetFlashDealRequest(int RemainingStock = 5);

[ApiController]
[Route("catalog/deals/{menuItemId:long}")]
public sealed class FlashDealsController(
    FlashDealService deals,
    ILogger<FlashDealsController> logger) : ControllerBase
{
    [HttpPost("reset")]
    [ProducesResponseType<FlashDealMvccState>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status400BadRequest)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> Reset(long menuItemId, [FromBody] ResetFlashDealRequest request, CancellationToken ct)
    {
        if (menuItemId <= 0 || request.RemainingStock is < 0 or > 10000)
            return Problem(statusCode: 400, title: "Positive menuItemId and remainingStock 0..10000 are required.");

        var state = await deals.ResetAsync(menuItemId, request.RemainingStock, ct);
        return state is null ? Problem(statusCode: 404, title: "Flash deal not found.") : Ok(state);
    }

    [HttpGet("mvcc-inspect")]
    [ProducesResponseType<FlashDealMvccState>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> Inspect(long menuItemId, CancellationToken ct)
    {
        if (menuItemId <= 0)
            return Problem(statusCode: 400, title: "Positive menuItemId is required.");

        var state = await deals.InspectAsync(menuItemId, ct);
        return state is null ? Problem(statusCode: 404, title: "Flash deal not found.") : Ok(state);
    }

    [HttpPost("claim")]
    [ProducesResponseType<ClaimFlashDealResult>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status400BadRequest)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status404NotFound)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status409Conflict)]
    public async Task<IActionResult> Claim(long menuItemId, CancellationToken ct, string mode = "optimistic")
    {
        if (menuItemId <= 0 || mode is not ("unsafe" or "optimistic" or "pessimistic"))
            return Problem(statusCode: 400, title: "Provide a positive menuItemId and mode unsafe, optimistic or pessimistic.");

        var result = await deals.ClaimAsync(menuItemId, mode, ct);
        if (!result.Found)
            return Problem(statusCode: 404, title: result.ErrorTitle ?? "Flash deal not found.");

        if (!result.Success)
        {
            logger.LogWarning(
                "flash_deal_conflict menu_item_id={MenuItemId} mode={Mode} code={Code} stock={Stock} version={Version}",
                menuItemId, mode, result.ErrorCode, result.RemainingStock, result.Version);

            var details = new ProblemDetails
            {
                Status = StatusCodes.Status409Conflict,
                Title = result.ErrorTitle,
                Type = $"urn:tadka:error:{result.ErrorCode}",
            };
            details.Extensions["code"] = result.ErrorCode;
            details.Extensions["remainingStock"] = result.RemainingStock;
            details.Extensions["version"] = result.Version;
            return new ObjectResult(details) { StatusCode = StatusCodes.Status409Conflict };
        }

        logger.LogInformation(
            "flash_deal_claimed menu_item_id={MenuItemId} mode={Mode} stock={Stock} version={Version}",
            menuItemId, mode, result.RemainingStock, result.Version);
        return Ok(result);
    }
}
