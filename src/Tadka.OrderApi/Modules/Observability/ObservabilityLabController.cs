using System.Text.Json;
using Microsoft.AspNetCore.Mvc;

namespace Tadka.OrderApi.Modules.Observability;

[ApiController]
public sealed class ObservabilityLabController(ObservabilityLabService observability) : ControllerBase
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    [HttpPost("/observability/orders/trace-checkout")]
    public async Task<IActionResult> TraceCheckout(
        [FromQuery] long? customerId,
        [FromQuery] long? restaurantId,
        [FromQuery] bool? simulateFail,
        [FromQuery] int? slowDbMs,
        CancellationToken ct)
    {
        TraceCheckoutRequest? body = null;
        if (Request.ContentLength is > 0 || Request.ContentType?.Contains("json", StringComparison.OrdinalIgnoreCase) == true)
        {
            try
            {
                body = await JsonSerializer.DeserializeAsync<TraceCheckoutRequest>(Request.Body, JsonOptions, ct);
            }
            catch (JsonException)
            {
                return Problem(statusCode: 400, title: "Invalid JSON payload.");
            }
        }

        var merged = new TraceCheckoutRequest(
            CustomerId: customerId ?? body?.CustomerId,
            RestaurantId: restaurantId ?? body?.RestaurantId,
            SimulateFail: simulateFail ?? body?.SimulateFail,
            SlowDbMs: slowDbMs ?? body?.SlowDbMs);

        var traceparent = Request.Headers["traceparent"].FirstOrDefault();

        TraceCheckoutResult result;
        try
        {
            result = await observability.TraceCheckoutAsync(merged, traceparent, ct);
        }
        catch (ArgumentOutOfRangeException ex)
        {
            return Problem(statusCode: 400, title: ex.Message);
        }

        Response.Headers["X-Trace-Id"] = result.TraceId;
        Response.Headers["traceparent"] = $"00-{result.TraceId}-{result.SpanId}-01";

        return StatusCode(result.StatusCode, result);
    }

    [HttpPost("/observability/spans/ingest")]
    public IActionResult IngestSpan([FromBody] IngestSpanRequest request)
    {
        try
        {
            return Ok(observability.IngestSpan(request));
        }
        catch (ArgumentException ex)
        {
            return Problem(statusCode: 400, title: ex.Message);
        }
    }

    [HttpGet("/diagnostics/observability/traces/{traceId}")]
    public IActionResult GetTrace(string traceId)
    {
        var trace = observability.GetTrace(traceId);
        if (trace is null)
            return Problem(statusCode: 404, title: $"Trace '{traceId}' was not found.");

        return Ok(trace);
    }

    [HttpGet("/diagnostics/observability/metrics")]
    public IActionResult GetMetrics() => Ok(observability.GetMetricsSnapshot());

    [HttpPost("/diagnostics/observability/log-storm-demo")]
    public IActionResult LogStormDemo([FromBody] LogStormDemoRequest? request)
    {
        try
        {
            return Ok(observability.ProcessLogStorm(request ?? new LogStormDemoRequest()));
        }
        catch (ArgumentOutOfRangeException ex)
        {
            return Problem(statusCode: 400, title: ex.Message);
        }
    }

    [HttpPost("/diagnostics/observability/reset")]
    public IActionResult Reset() => Ok(observability.Reset());
}
