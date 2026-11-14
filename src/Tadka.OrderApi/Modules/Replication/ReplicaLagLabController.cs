using Microsoft.AspNetCore.Mvc;

namespace Tadka.OrderApi.Modules.Replication;

public sealed record ToggleWalReplayRequest(bool Paused);

public sealed record LsnCreateOrderRequest(long CustomerId, long RestaurantId, decimal TotalAmount);

public sealed record LsnUpdateStatusRequest(string Status);

[ApiController]
public sealed class ReplicaLagLabController(
    ReplicaLagLabService replication,
    ILogger<ReplicaLagLabController> logger) : ControllerBase
{
    private static readonly HashSet<string> AllowedStatuses = ["placed", "preparing", "ready", "delivered"];

    [HttpGet("diagnostics/replication/status")]
    [ProducesResponseType<ReplicationStatusResponse>(StatusCodes.Status200OK)]
    public async Task<IActionResult> Status(CancellationToken ct)
    {
        var status = await replication.GetStatusAsync(ct);
        return Ok(status);
    }

    [HttpPost("diagnostics/replication/replay")]
    [ProducesResponseType<ReplayToggleResponse>(StatusCodes.Status200OK)]
    public async Task<IActionResult> ToggleReplay([FromBody] ToggleWalReplayRequest request, CancellationToken ct)
    {
        var result = await replication.SetWalReplayPausedAsync(request.Paused, ct);
        logger.LogInformation(
            "replica_wal_replay paused={Paused} receive_lsn={ReceiveLsn} replay_lsn={ReplayLsn}",
            result.ReplicaWalReplayPaused,
            result.ReplicaReceiveLsn,
            result.ReplicaReplayLsn);
        return Ok(result);
    }

    [HttpPost("diagnostics/replication/readonly-check")]
    public async Task<IActionResult> ReadOnlyCheck(CancellationToken ct)
    {
        var result = await replication.ProbeReplicaReadOnlyAsync(ct);
        return Ok(result);
    }

    [HttpPost("orders/with-lsn")]
    [ProducesResponseType<LsnWriteOrderResponse>(StatusCodes.Status201Created)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> CreateOrderWithLsn([FromBody] LsnCreateOrderRequest request, CancellationToken ct)
    {
        if (request.CustomerId <= 0 || request.RestaurantId <= 0 || request.TotalAmount <= 0)
            return Problem(statusCode: 400, title: "customerId, restaurantId and totalAmount must be positive.");

        var created = await replication.CreateOrderWithLsnAsync(
            request.CustomerId,
            request.RestaurantId,
            request.TotalAmount,
            ct);

        Response.Headers["X-Commit-Lsn"] = created.CommitLsn;
        logger.LogInformation("order_with_lsn_created id={Id} commit_lsn={CommitLsn}", created.Id, created.CommitLsn);
        return Created($"/orders/{created.Id}", created);
    }

    [HttpPatch("orders/{id:long}/status-with-lsn")]
    [ProducesResponseType<LsnWriteOrderResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status400BadRequest)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> UpdateStatusWithLsn(long id, [FromBody] LsnUpdateStatusRequest request, CancellationToken ct)
    {
        if (id <= 0 || !AllowedStatuses.Contains(request.Status))
            return Problem(statusCode: 400, title: "Provide a positive id and valid status.");

        var updated = await replication.UpdateStatusWithLsnAsync(id, request.Status, ct);
        if (updated is null)
            return Problem(statusCode: 404, title: $"Order {id} not found.");

        Response.Headers["X-Commit-Lsn"] = updated.CommitLsn;
        logger.LogInformation(
            "order_status_with_lsn id={Id} status={Status} version={Version} commit_lsn={CommitLsn}",
            updated.Id,
            updated.Status,
            updated.StatusVersion,
            updated.CommitLsn);
        return Ok(updated);
    }

    [HttpGet("orders/{id:long}/read-your-writes")]
    [ProducesResponseType<ReadYourWritesOrderResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status400BadRequest)]
    [ProducesResponseType<ReadYourWritesOrderResponse>(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> ReadYourWrites(
        long id,
        CancellationToken ct,
        string routing = "ryow",
        string? minLsn = null)
    {
        if (id <= 0 || routing is not ("replica-only" or "ryow"))
            return Problem(statusCode: 400, title: "Positive id and routing 'replica-only' or 'ryow' are required.");

        var effectiveMinLsn = minLsn ?? Request.Headers["X-Min-Lsn"].FirstOrDefault();
        var (validLsn, response) = await replication.ReadOrderWithRoutingAsync(id, routing, effectiveMinLsn, ct);
        if (!validLsn || response is null)
            return Problem(statusCode: 400, title: "Invalid PostgreSQL LSN format in minLsn or X-Min-Lsn.");

        if (!response.Found)
            return NotFound(response);

        return Ok(response);
    }
}
