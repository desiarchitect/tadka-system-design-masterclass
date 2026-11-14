using Microsoft.AspNetCore.Mvc;

namespace Tadka.OrderApi.Modules.Catalog;

public sealed record UpdateMenuItemRequest(decimal? Price, bool? IsAvailable);

[ApiController]
[Route("catalog/restaurants/{restaurantId:long}")]
public sealed class CatalogController(
    ICatalogModule catalog,
    ILogger<CatalogController> logger,
    IWebHostEnvironment environment) : ControllerBase
{
    [HttpGet("menu")]
    [ProducesResponseType<CatalogMenuResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetMenu(long restaurantId, CancellationToken ct)
    {
        if (restaurantId <= 0)
            return Problem(statusCode: 400, title: "Restaurant ID must be positive.");

        if (environment.IsEnvironment("Demo") && Request.Headers["X-Demo-Catalog-Failure"] == "true")
        {
            logger.LogWarning("demo_catalog_failure restaurant_id={RestaurantId}", restaurantId);
            return Problem(statusCode: 503, title: "Catalog dependency unavailable (demo fault injection).");
        }

        var menu = await catalog.GetMenuAsync(restaurantId, ct);
        return menu is null ? Problem(statusCode: 404, title: "Restaurant not found.") : Ok(menu);
    }

    [HttpPatch("items/{itemId:long}")]
    [ProducesResponseType<CatalogMenuItem>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status400BadRequest)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> UpdateItem(
        long restaurantId,
        long itemId,
        [FromBody] UpdateMenuItemRequest request,
        CancellationToken ct)
    {
        if (restaurantId <= 0 || itemId <= 0)
            return Problem(statusCode: 400, title: "Restaurant and menu item IDs must be positive.");
        if (request.Price is null && request.IsAvailable is null)
            return Problem(statusCode: 400, title: "Provide price or isAvailable to update.");
        if (request.Price is { } p && (p <= 0 || p > 99999999.99m || decimal.Round(p, 2) != p))
            return Problem(statusCode: 400, title: "Price must be positive with at most two decimal places.");

        var updated = await catalog.UpdateItemAsync(restaurantId, itemId, request.Price, request.IsAvailable, ct);
        if (updated is null)
            return Problem(statusCode: 404, title: "Menu item not found for this restaurant.");

        logger.LogInformation(
            "catalog_item_updated restaurant_id={RestaurantId} item_id={ItemId} price={Price} available={Available} version={Version}",
            restaurantId, itemId, updated.Price, updated.IsAvailable, updated.Version);
        return Ok(updated);
    }
}
