using Microsoft.AspNetCore.Mvc;

namespace Tadka.OrderApi.Modules.DistributedIds;

[ApiController]
public sealed class DistributedIdLabController(
    DistributedIdLabService lab,
    ILogger<DistributedIdLabController> logger) : ControllerBase
{
    [HttpGet("diagnostics/ids/sample")]
    public IActionResult Sample(int count = 5)
    {
        if (count is < 1 or > 50)
            return Problem(statusCode: 400, title: "count must be between 1 and 50.");

        return Ok(lab.GetSample(count));
    }

    [HttpPost("diagnostics/ids/clock-rollback-check")]
    public IActionResult ClockRollbackCheck(long rollbackMs = 150)
    {
        if (rollbackMs is < 1 or > 60_000)
            return Problem(statusCode: 400, title: "rollbackMs must be between 1 and 60000.");

        var result = lab.RunClockRollbackCheck(rollbackMs);
        logger.LogInformation("clock_rollback_check rollback_ms={RollbackMs}", rollbackMs);
        return Ok(result);
    }

    [HttpPost("diagnostics/ids/benchmark")]
    [ProducesResponseType<IdBenchmarkResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> Benchmark(CancellationToken ct, int rows = 25000)
    {
        if (rows is < 1000 or > 50_000)
            return Problem(statusCode: 400, title: "rows must be between 1000 and 50000.");

        var result = await lab.RunBenchmarkAsync(rows, ct);
        logger.LogInformation(
            "id_benchmark rows={Rows} v4_density={V4Density} v7_density={V7Density} sf_density={SfDensity}",
            rows,
            result.UuidV4.AvgLeafDensityPct,
            result.UuidV7.AvgLeafDensityPct,
            result.Snowflake.AvgLeafDensityPct);
        return Ok(result);
    }
}
