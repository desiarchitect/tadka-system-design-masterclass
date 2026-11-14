using System.Text.Json;
using Microsoft.AspNetCore.Mvc;

namespace Tadka.OrderApi.Modules.OverloadProtection;

[ApiController]
public sealed class OverloadProtectionLabController(OverloadProtectionLabService overload) : ControllerBase
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    [HttpGet("/diagnostics/overload/status")]
    public IActionResult Status() => Ok(overload.GetStatus());

    [HttpPost("/diagnostics/overload/rate-limit-config")]
    public async Task<IActionResult> ConfigureRateLimit(
        [FromQuery] int? capacity,
        [FromQuery] double? refillPerSecond,
        [FromQuery] string? mode,
        [FromQuery] int? raceWindowMs,
        [FromQuery] bool? simulateRedisDown,
        [FromQuery] string? failurePolicy,
        [FromQuery] int? localCapacity,
        [FromQuery] bool? resetState,
        CancellationToken ct)
    {
        RateLimitConfigRequest? body = null;
        if (Request.ContentLength is > 0 || Request.ContentType?.Contains("json", StringComparison.OrdinalIgnoreCase) == true)
        {
            try
            {
                body = await JsonSerializer.DeserializeAsync<RateLimitConfigRequest>(Request.Body, JsonOptions, ct);
            }
            catch (JsonException)
            {
                return Problem(statusCode: 400, title: "Invalid JSON payload.");
            }
        }

        var merged = new RateLimitConfigRequest(
            Capacity: capacity ?? body?.Capacity,
            RefillPerSecond: refillPerSecond ?? body?.RefillPerSecond,
            Mode: mode ?? body?.Mode,
            RaceWindowMs: raceWindowMs ?? body?.RaceWindowMs,
            SimulateRedisDown: simulateRedisDown ?? body?.SimulateRedisDown,
            FailurePolicy: failurePolicy ?? body?.FailurePolicy,
            LocalCapacity: localCapacity ?? body?.LocalCapacity,
            ResetState: resetState ?? body?.ResetState);

        try
        {
            return Ok(await overload.ConfigureRateLimiterAsync(merged, ct));
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

    [HttpPost("/overload/rate-limit/check")]
    public async Task<IActionResult> CheckRateLimit(
        [FromQuery] string? key,
        [FromQuery] string? endpoint,
        [FromQuery] int? cost,
        [FromQuery] string? failurePolicy,
        CancellationToken ct)
    {
        RateLimitCheckRequest? body = null;
        if (Request.ContentLength is > 0 || Request.ContentType?.Contains("json", StringComparison.OrdinalIgnoreCase) == true)
        {
            try
            {
                body = await JsonSerializer.DeserializeAsync<RateLimitCheckRequest>(Request.Body, JsonOptions, ct);
            }
            catch (JsonException)
            {
                return Problem(statusCode: 400, title: "Invalid JSON payload.");
            }
        }

        var effectiveKey = (key ?? body?.Key ?? "customer:42").Trim();
        var effectiveEndpoint = (endpoint ?? body?.Endpoint ?? "catalog-browse").Trim();
        var effectiveCost = cost ?? body?.Cost ?? 1;
        var effectivePolicy = failurePolicy ?? body?.FailurePolicy;

        if (string.IsNullOrWhiteSpace(effectiveKey))
            return Problem(statusCode: 400, title: "key cannot be empty.");
        if (effectiveCost is < 1 or > 100)
            return Problem(statusCode: 400, title: "cost must be between 1 and 100.");

        try
        {
            var result = await overload.CheckRateLimitAsync(effectiveKey, effectiveEndpoint, effectiveCost, effectivePolicy, ct);
            Response.Headers["X-RateLimit-Limit"] = result.Limit.ToString();
            Response.Headers["X-RateLimit-Remaining"] = result.Remaining.ToString();
            Response.Headers["X-RateLimit-Reset"] = result.ResetSeconds.ToString();
            if (result.RetryAfterSeconds > 0 && result.StatusCode == StatusCodes.Status429TooManyRequests)
                Response.Headers["Retry-After"] = result.RetryAfterSeconds.ToString();
            if (result.FallbackUsed && !string.IsNullOrWhiteSpace(result.FailurePolicy))
                Response.Headers["X-RateLimit-Fallback"] = result.FailurePolicy == "local-fallback" ? "local-bucket" : result.FailurePolicy;

            return StatusCode(result.StatusCode, result);
        }
        catch (ArgumentException ex)
        {
            return Problem(statusCode: 400, title: ex.Message);
        }
    }

    [HttpPost("/diagnostics/overload/bulkhead-config")]
    public async Task<IActionResult> ConfigureBulkhead(
        [FromQuery] string? isolationMode,
        [FromQuery] int? sharedMaxConcurrency,
        [FromQuery] int? analyticsMaxConcurrency,
        [FromQuery] int? checkoutMaxConcurrency,
        [FromQuery] bool? resetCounters,
        CancellationToken ct)
    {
        BulkheadConfigRequest? body = null;
        if (Request.ContentLength is > 0 || Request.ContentType?.Contains("json", StringComparison.OrdinalIgnoreCase) == true)
        {
            try
            {
                body = await JsonSerializer.DeserializeAsync<BulkheadConfigRequest>(Request.Body, JsonOptions, ct);
            }
            catch (JsonException)
            {
                return Problem(statusCode: 400, title: "Invalid JSON payload.");
            }
        }

        var merged = new BulkheadConfigRequest(
            IsolationMode: isolationMode ?? body?.IsolationMode,
            SharedMaxConcurrency: sharedMaxConcurrency ?? body?.SharedMaxConcurrency,
            AnalyticsMaxConcurrency: analyticsMaxConcurrency ?? body?.AnalyticsMaxConcurrency,
            CheckoutMaxConcurrency: checkoutMaxConcurrency ?? body?.CheckoutMaxConcurrency,
            ResetCounters: resetCounters ?? body?.ResetCounters);

        try
        {
            return Ok(overload.ConfigureBulkhead(merged));
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

    [HttpPost("/overload/bulkhead/work")]
    public async Task<IActionResult> RunBulkheadWork(
        [FromQuery] string? lane,
        [FromQuery] int? workMs,
        CancellationToken ct)
    {
        BulkheadWorkRequest? body = null;
        if (Request.ContentLength is > 0 || Request.ContentType?.Contains("json", StringComparison.OrdinalIgnoreCase) == true)
        {
            try
            {
                body = await JsonSerializer.DeserializeAsync<BulkheadWorkRequest>(Request.Body, JsonOptions, ct);
            }
            catch (JsonException)
            {
                return Problem(statusCode: 400, title: "Invalid JSON payload.");
            }
        }

        var effectiveLane = (lane ?? body?.Lane ?? "checkout").Trim().ToLowerInvariant();
        if (effectiveLane is not ("analytics" or "checkout"))
            return Problem(statusCode: 400, title: "lane must be 'analytics' or 'checkout'.");

        var effectiveWorkMs = workMs ?? body?.WorkMs ?? 20;
        if (effectiveWorkMs is < 0 or > 5000)
            return Problem(statusCode: 400, title: "workMs must be between 0 and 5000.");

        var result = await overload.ExecuteBulkheadWorkAsync(effectiveLane, effectiveWorkMs, ct);
        return StatusCode(result.StatusCode, result);
    }

    [HttpPost("/diagnostics/overload/shedding-config")]
    public async Task<IActionResult> ConfigureShedding(
        [FromQuery] bool? enabled,
        [FromQuery] int? simulatedLoadPercent,
        [FromQuery] int? shedLowThresholdPercent,
        [FromQuery] int? shedNormalThresholdPercent,
        [FromQuery] bool? resetCounters,
        CancellationToken ct)
    {
        SheddingConfigRequest? body = null;
        if (Request.ContentLength is > 0 || Request.ContentType?.Contains("json", StringComparison.OrdinalIgnoreCase) == true)
        {
            try
            {
                body = await JsonSerializer.DeserializeAsync<SheddingConfigRequest>(Request.Body, JsonOptions, ct);
            }
            catch (JsonException)
            {
                return Problem(statusCode: 400, title: "Invalid JSON payload.");
            }
        }

        var merged = new SheddingConfigRequest(
            Enabled: enabled ?? body?.Enabled,
            SimulatedLoadPercent: simulatedLoadPercent ?? body?.SimulatedLoadPercent,
            ShedLowThresholdPercent: shedLowThresholdPercent ?? body?.ShedLowThresholdPercent,
            ShedNormalThresholdPercent: shedNormalThresholdPercent ?? body?.ShedNormalThresholdPercent,
            ResetCounters: resetCounters ?? body?.ResetCounters);

        try
        {
            return Ok(overload.ConfigureLoadShedding(merged));
        }
        catch (ArgumentOutOfRangeException ex)
        {
            return Problem(statusCode: 400, title: ex.Message);
        }
    }

    [HttpPost("/overload/shedding/request")]
    public async Task<IActionResult> CheckShedding([FromQuery] string? priority, CancellationToken ct)
    {
        SheddingCheckRequest? body = null;
        if (Request.ContentLength is > 0 || Request.ContentType?.Contains("json", StringComparison.OrdinalIgnoreCase) == true)
        {
            try
            {
                body = await JsonSerializer.DeserializeAsync<SheddingCheckRequest>(Request.Body, JsonOptions, ct);
            }
            catch (JsonException)
            {
                return Problem(statusCode: 400, title: "Invalid JSON payload.");
            }
        }

        var effectivePriority = (priority ?? body?.Priority ?? "normal-browse").Trim().ToLowerInvariant();
        if (effectivePriority is not ("critical-checkout" or "normal-browse" or "low-recommendations"))
            return Problem(statusCode: 400, title: "priority must be 'critical-checkout', 'normal-browse', or 'low-recommendations'.");

        var result = overload.EvaluateLoadShedding(effectivePriority);
        Response.Headers["X-Load-Shed"] = result.Shed ? "true" : "false";
        if (result.RetryAfterSeconds > 0)
            Response.Headers["Retry-After"] = result.RetryAfterSeconds.ToString();

        return StatusCode(result.StatusCode, result);
    }
}
