using Microsoft.AspNetCore.Mvc;

namespace Tadka.OrderApi.Modules.Indexing;

public sealed record SetIndexingModeRequest(string Mode);

[ApiController]
public sealed class IndexingLabController(
    IndexingLabService indexing,
    ILogger<IndexingLabController> logger) : ControllerBase
{
    private static readonly HashSet<string> AllowedStatuses = ["placed", "preparing", "ready", "delivered"];

    [HttpPost("diagnostics/indexing/mode")]
    [ProducesResponseType<IndexingModeResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> SetMode([FromBody] SetIndexingModeRequest request, CancellationToken ct)
    {
        if (request.Mode is not ("none" or "composite" or "covering"))
            return Problem(statusCode: 400, title: "Mode must be 'none', 'composite' or 'covering'.");

        var result = await indexing.SetModeAsync(request.Mode, ct);
        logger.LogInformation(
            "indexing_mode_set mode={Mode} table_bytes={TableBytes} indexes_bytes={IndexesBytes} rows={Rows}",
            result.Mode, result.TableBytes, result.IndexesBytes, result.EstimatedRows);
        return Ok(result);
    }

    [HttpPost("diagnostics/indexing/reseed")]
    [ProducesResponseType<IndexingModeResponse>(StatusCodes.Status200OK)]
    public async Task<IActionResult> Reseed(CancellationToken ct)
    {
        var result = await indexing.ReseedAsync(ct);
        return Ok(result);
    }

    [HttpGet("orders/kitchen-feed")]
    [ProducesResponseType<KitchenFeedResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> KitchenFeed(
        CancellationToken ct,
        long restaurantId = 7,
        string status = "preparing",
        int limit = 20,
        bool explain = true)
    {
        if (restaurantId <= 0 || limit is < 1 or > 100 || !AllowedStatuses.Contains(status))
            return Problem(statusCode: 400, title: "Provide a positive restaurantId, valid status and limit 1..100.");

        var result = await indexing.GetKitchenFeedAsync(restaurantId, status, limit, explain, ct);
        if (result.Plan is { } p)
        {
            logger.LogInformation(
                "kitchen_feed_plan node={Node} index={Index} actual_rows={Rows} removed={Removed} heap_fetches={HeapFetches} exec_ms={ExecMs}",
                p.NodeType, p.IndexName ?? "none", p.ActualRows, p.RowsRemovedByFilter, p.HeapFetches, p.ExecutionTimeMs);
        }

        return Ok(result);
    }

    /// <summary>
    /// Negative control for the leftmost-prefix rule: filters by status without restaurant_id.
    /// </summary>
    [HttpGet("orders/status-scan")]
    [ProducesResponseType<KitchenFeedResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> StatusScan(
        CancellationToken ct,
        string status = "preparing",
        int limit = 20,
        bool explain = true)
    {
        if (limit is < 1 or > 100 || !AllowedStatuses.Contains(status))
            return Problem(statusCode: 400, title: "Provide a valid status and limit 1..100.");

        var result = await indexing.GetStatusScanAsync(status, limit, explain, ct);
        if (result.Plan is { } p)
        {
            logger.LogInformation(
                "status_scan_plan node={Node} index={Index} index_cond={IndexCond} removed={Removed} exec_ms={ExecMs}",
                p.NodeType, p.IndexName ?? "none", p.IndexCond ?? "none", p.RowsRemovedByFilter, p.ExecutionTimeMs);
        }

        return Ok(result);
    }
}
