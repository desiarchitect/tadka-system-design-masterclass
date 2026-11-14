using Microsoft.AspNetCore.Mvc;

namespace Tadka.OrderApi.Modules.TadkaV1;

[ApiController]
public sealed class TadkaV1ProductionController(TadkaV1ProductionService v1Service) : ControllerBase
{
    [HttpGet("/v1/tadka/architecture")]
    public async Task<IActionResult> GetArchitecture(CancellationToken ct)
    {
        var manifest = await v1Service.GetArchitectureAsync(ct);
        return Ok(manifest);
    }

    [HttpPost("/v1/tadka/orders/checkout")]
    public async Task<IActionResult> Checkout([FromBody] V1CheckoutRequest? request, CancellationToken ct)
    {
        var idempotencyKey = Request.Headers["Idempotency-Key"].FirstOrDefault();
        var traceparent = Request.Headers["traceparent"].FirstOrDefault();

        var result = await v1Service.CheckoutAsync(request ?? new V1CheckoutRequest(), idempotencyKey, traceparent, ct);
        Response.Headers["Idempotency-Status"] = result.IdempotencyStatus;
        if (!string.IsNullOrEmpty(result.TraceId))
            Response.Headers["X-Trace-Id"] = result.TraceId;
        if (!string.IsNullOrEmpty(result.Traceparent))
            Response.Headers["traceparent"] = result.Traceparent;

        return StatusCode(result.StatusCode, result);
    }

    [HttpPost("/diagnostics/v1/chaos-drill")]
    public async Task<IActionResult> RunChaosDrill([FromBody] V1ChaosDrillRequest? request, [FromQuery] string? mode, CancellationToken ct)
    {
        try
        {
            var effectiveMode = !string.IsNullOrWhiteSpace(mode) ? mode : request?.Mode;
            var result = await v1Service.RunChaosDrillAsync(effectiveMode, ct);
            return Ok(result);
        }
        catch (ArgumentException ex)
        {
            return Problem(statusCode: 400, title: ex.Message);
        }
    }

    [HttpPost("/diagnostics/v1/outbox-flush")]
    public async Task<IActionResult> FlushOutbox(CancellationToken ct)
    {
        var result = await v1Service.FlushOutboxAsync(ct);
        return Ok(result);
    }

    [HttpPost("/diagnostics/v1/reset")]
    public async Task<IActionResult> Reset(CancellationToken ct)
    {
        var result = await v1Service.ResetAsync(ct);
        return Ok(result);
    }
}
