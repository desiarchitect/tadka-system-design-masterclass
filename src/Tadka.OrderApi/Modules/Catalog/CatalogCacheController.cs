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

    [HttpPost("ttl-jitter-sample")]
    public async Task<IActionResult> TtlJitterSample(
        [FromQuery] int count = 12,
        [FromQuery] int baseTtlSeconds = 30,
        [FromQuery] int jitterSeconds = 10,
        CancellationToken ct = default)
    {
        if (count is < 1 or > 50)
            return Problem(statusCode: 400, title: "count must be between 1 and 50.");
        if (baseTtlSeconds is < 1 or > 3600)
            return Problem(statusCode: 400, title: "baseTtlSeconds must be between 1 and 3600.");
        if (jitterSeconds is < 1 or > 300)
            return Problem(statusCode: 400, title: "jitterSeconds must be between 1 and 300.");

        var sample = await menuCache.SampleTtlJitterAsync(count, baseTtlSeconds, jitterSeconds, ct);
        return Ok(sample);
    }
}
