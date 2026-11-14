using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Net.Http.Headers;

namespace Tadka.OrderApi.Modules.Catalog;

public sealed record UpdateMenuItemRequest(decimal? Price, bool? IsAvailable);

[ApiController]
[Route("catalog/restaurants/{restaurantId:long}")]
public sealed class CatalogController(
    MenuCacheService menuCache,
    ILogger<CatalogController> logger,
    IWebHostEnvironment environment) : ControllerBase
{
    [HttpGet("menu")]
    [ProducesResponseType<CatalogMenuResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status304NotModified)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status400BadRequest)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetMenu(
        long restaurantId,
        [FromQuery] string? strategy,
        CancellationToken ct)
    {
        if (restaurantId <= 0)
            return Problem(statusCode: 400, title: "Restaurant ID must be positive.");

        var effectiveStrategy = string.IsNullOrWhiteSpace(strategy) ? "cache-aside" : strategy.Trim().ToLowerInvariant();
        if (effectiveStrategy is not ("cache-aside" or "bypass"))
            return Problem(statusCode: 400, title: "Query strategy must be 'cache-aside' or 'bypass'.");

        if (environment.IsEnvironment("Demo") && Request.Headers["X-Demo-Catalog-Failure"] == "true")
        {
            logger.LogWarning("demo_catalog_failure restaurant_id={RestaurantId}", restaurantId);
            return Problem(statusCode: 503, title: "Catalog dependency unavailable (demo fault injection).");
        }

        var result = await menuCache.GetMenuAsync(restaurantId, effectiveStrategy, ct);
        Response.Headers["X-Cache"] = result.CacheStatus;
        Response.Headers["X-Cache-Key"] = result.CacheKey;
        if (result.TtlSeconds is { } ttl)
            Response.Headers["X-Cache-Ttl-Seconds"] = ttl.ToString(CultureInfo.InvariantCulture);

        if (result.Menu is null)
            return Problem(statusCode: 404, title: "Restaurant not found.");

        var etag = ComputeMenuETag(result.Menu);
        Response.Headers.ETag = etag;
        Response.Headers.CacheControl = "public, max-age=15, stale-while-revalidate=30";
        Response.Headers.Vary = "Accept-Encoding";

        if (MatchesIfNoneMatch(Request.Headers.IfNoneMatch, etag))
            return StatusCode(StatusCodes.Status304NotModified);

        return Ok(result.Menu);
    }

    [HttpGet("menu-stampede")]
    [ProducesResponseType<CatalogMenuResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status400BadRequest)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetMenuStampede(
        long restaurantId,
        [FromQuery] string? mode,
        [FromQuery] int dbDelayMs = 60,
        [FromQuery] int baseTtlSeconds = 30,
        [FromQuery] int jitterSeconds = 6,
        CancellationToken ct = default)
    {
        if (restaurantId <= 0)
            return Problem(statusCode: 400, title: "Restaurant ID must be positive.");

        var effectiveMode = string.IsNullOrWhiteSpace(mode) ? "naive" : mode.Trim().ToLowerInvariant();
        if (effectiveMode is not ("naive" or "single-flight"))
            return Problem(statusCode: 400, title: "Query mode must be 'naive' or 'single-flight'.");
        if (dbDelayMs is < 0 or > 500)
            return Problem(statusCode: 400, title: "dbDelayMs must be between 0 and 500.");
        if (baseTtlSeconds is < 1 or > 3600)
            return Problem(statusCode: 400, title: "baseTtlSeconds must be between 1 and 3600.");
        if (jitterSeconds is < 0 or > 300)
            return Problem(statusCode: 400, title: "jitterSeconds must be between 0 and 300.");

        var result = await menuCache.GetMenuStampedeAsync(
            restaurantId, effectiveMode, dbDelayMs, baseTtlSeconds, jitterSeconds, ct);

        Response.Headers["X-Cache"] = result.CacheStatus;
        Response.Headers["X-Stampede-Role"] = result.StampedeRole;
        Response.Headers["X-Cache-Key"] = result.CacheKey;
        if (result.TtlSeconds is { } ttl)
            Response.Headers["X-Cache-Ttl-Seconds"] = ttl.ToString(CultureInfo.InvariantCulture);

        return result.Menu is null ? Problem(statusCode: 404, title: "Restaurant not found.") : Ok(result.Menu);
    }

    [HttpPatch("items/{itemId:long}")]
    [ProducesResponseType<CatalogMenuItem>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status400BadRequest)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> UpdateItem(
        long restaurantId,
        long itemId,
        [FromBody] UpdateMenuItemRequest request,
        [FromQuery] string? invalidation,
        CancellationToken ct)
    {
        if (restaurantId <= 0 || itemId <= 0)
            return Problem(statusCode: 400, title: "Restaurant and menu item IDs must be positive.");
        if (request.Price is null && request.IsAvailable is null)
            return Problem(statusCode: 400, title: "Provide price or isAvailable to update.");
        if (request.Price is { } p && (p <= 0 || p > 99999999.99m || decimal.Round(p, 2) != p))
            return Problem(statusCode: 400, title: "Price must be positive with at most two decimal places.");

        var effectiveInvalidation = string.IsNullOrWhiteSpace(invalidation) ? "delete" : invalidation.Trim().ToLowerInvariant();
        if (effectiveInvalidation is not ("delete" or "none" or "write-through"))
            return Problem(statusCode: 400, title: "Query invalidation must be 'delete', 'none', or 'write-through'.");

        var (updated, invalidationHeader) = await menuCache.UpdateItemWithCacheAsync(
            restaurantId, itemId, request.Price, request.IsAvailable, effectiveInvalidation, ct);

        if (updated is null)
            return Problem(statusCode: 404, title: "Menu item not found for this restaurant.");

        Response.Headers["X-Cache-Invalidation"] = invalidationHeader;
        logger.LogInformation(
            "catalog_item_updated restaurant_id={RestaurantId} item_id={ItemId} price={Price} available={Available} version={Version} invalidation={Invalidation}",
            restaurantId, itemId, updated.Price, updated.IsAvailable, updated.Version, invalidationHeader);
        return Ok(updated);
    }

    private static string ComputeMenuETag(CatalogMenuResponse menu)
    {
        var sb = new StringBuilder();
        sb.Append(menu.RestaurantId).Append(':').Append(menu.IsOpen);
        foreach (var item in menu.Items.OrderBy(i => i.Id))
        {
            sb.Append('|')
              .Append(item.Id).Append(':')
              .Append(item.Version).Append(':')
              .Append(item.Price.ToString("F2", CultureInfo.InvariantCulture)).Append(':')
              .Append(item.IsAvailable);
        }

        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(sb.ToString()));
        var shortHex = Convert.ToHexStringLower(hash).AsSpan(0, 12);
        return $"\"menu-{menu.RestaurantId}-v{shortHex}\"";
    }

    private static bool MatchesIfNoneMatch(Microsoft.Extensions.Primitives.StringValues headerValues, string currentETag)
    {
        if (headerValues.Count == 0)
            return false;

        if (!EntityTagHeaderValue.TryParse(currentETag, out var currentParsed))
            return false;

        if (EntityTagHeaderValue.TryParseList(headerValues, out var candidates))
        {
            foreach (var candidate in candidates)
            {
                if (candidate.Equals(EntityTagHeaderValue.Any))
                    return true;

                // RFC 9110: GET/HEAD If-None-Match uses weak comparison (same opaque tag).
                if (string.Equals(candidate.Tag.Value, currentParsed.Tag.Value, StringComparison.Ordinal))
                    return true;
            }
        }

        return false;
    }
}
