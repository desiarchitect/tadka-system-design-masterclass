using Microsoft.AspNetCore.Mvc;

namespace Tadka.OrderApi.Modules.Catalog;

[ApiController]
[Route("diagnostics/cache")]
public sealed class CatalogCacheController(MenuCacheService menuCache) : ControllerBase
{
    [HttpPost("reset")]
    public async Task<IActionResult> Reset(CancellationToken ct)
    {
        await menuCache.ResetAsync(ct);
        return Ok(new { reset = true });
    }

    [HttpGet("stats")]
    public async Task<IActionResult> Stats([FromQuery] long restaurantId = 7, CancellationToken ct = default)
    {
        if (restaurantId <= 0)
            return Problem(statusCode: 400, title: "Restaurant ID must be positive.");

        var stats = await menuCache.GetStatsAsync(restaurantId, ct);
        return Ok(stats);
    }
}
