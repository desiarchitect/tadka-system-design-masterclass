using System.Text.Json;
using Microsoft.AspNetCore.Mvc;

namespace Tadka.OrderApi.Modules.Instance;

public sealed record InstanceStateRequest(string? State, int? SlowMs, bool? ResetCounters);

[ApiController]
public sealed class InstanceDiagnosticsController(
    InstanceLifecycleService lifecycle,
    ILogger<InstanceDiagnosticsController> logger) : ControllerBase
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    [HttpGet("/health")]
    [HttpGet("/health/ready")]
    public IActionResult Ready()
    {
        var snapshot = lifecycle.GetSnapshot();
        return snapshot.State == "healthy"
            ? Ok(snapshot)
            : StatusCode(StatusCodes.Status503ServiceUnavailable, snapshot);
    }

    [HttpGet("/health/live")]
    public IActionResult Live()
    {
        var snapshot = lifecycle.GetSnapshot();
        var alive = snapshot.State is "healthy" or "draining";
        var payload = new
        {
            status = alive ? "alive" : "unhealthy",
            snapshot.InstanceId,
            snapshot.State,
            snapshot.InFlightRequests
        };
        return alive ? Ok(payload) : StatusCode(StatusCodes.Status503ServiceUnavailable, payload);
    }

    [HttpPost("/diagnostics/instance/state")]
    public async Task<IActionResult> UpdateState(
        [FromQuery] string? state,
        [FromQuery] int? slowMs,
        [FromQuery] bool? resetCounters,
        CancellationToken ct)
    {
        InstanceStateRequest? body = null;
        if (Request.ContentLength is > 0 || Request.ContentType?.Contains("json", StringComparison.OrdinalIgnoreCase) == true)
        {
            try
            {
                body = await JsonSerializer.DeserializeAsync<InstanceStateRequest>(Request.Body, JsonOptions, ct);
            }
            catch (JsonException)
            {
                return Problem(statusCode: 400, title: "Invalid JSON payload.");
            }
        }

        var nextState = state ?? body?.State;
        var nextSlowMs = slowMs ?? body?.SlowMs;
        var shouldReset = (resetCounters ?? false) || (body?.ResetCounters ?? false);

        try
        {
            var updated = lifecycle.Update(nextState, nextSlowMs, shouldReset);
            logger.LogWarning(
                "instance_state_updated instance_id={InstanceId} state={State} slow_ms={SlowMs} in_flight={InFlight}",
                updated.InstanceId, updated.State, updated.SlowMs, updated.InFlightRequests);
            return Ok(updated);
        }
        catch (ArgumentOutOfRangeException ex)
        {
            return Problem(statusCode: 400, title: ex.Message);
        }
        catch (ArgumentException ex)
        {
            return Problem(statusCode: 400, title: ex.Message);
        }
    }

    [HttpGet("/diagnostics/instance/in-flight-demo")]
    public async Task<IActionResult> InFlightDemo([FromQuery] int workMs = 350, CancellationToken ct = default)
    {
        if (workMs is < 0 or > 5000)
            return Problem(statusCode: 400, title: "workMs must be between 0 and 5000.");

        if (lifecycle.State == "unhealthy")
            return StatusCode(StatusCodes.Status503ServiceUnavailable, lifecycle.GetSnapshot());

        await Task.Delay(workMs, ct);

        return Ok(new
        {
            instanceId = lifecycle.InstanceId,
            stateAtCompletion = lifecycle.State,
            workMs,
            inFlightRequests = lifecycle.InFlightRequests
        });
    }
}
