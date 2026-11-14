using System.Text.Json;
using Microsoft.AspNetCore.Mvc;

namespace Tadka.OrderApi.Modules.Resilience;

[ApiController]
public sealed class ResilienceLabController(ResilienceLabService resilience) : ControllerBase
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    [HttpPost("/diagnostics/resilience/downstream-config")]
    public async Task<IActionResult> ConfigureDownstream(
        [FromQuery] string? mode,
        [FromQuery] int? delayMs,
        [FromQuery] int? perTryTimeoutMs,
        [FromQuery] int? overallDeadlineMs,
        [FromQuery] int? maxRetries,
        [FromQuery] int? retryBudgetPercent,
        [FromQuery] int? circuitBreakerFailureThreshold,
        [FromQuery] int? circuitBreakerOpenMs,
        [FromQuery] bool? resetCounters,
        CancellationToken ct)
    {
        ResilienceConfigRequest? body = null;
        if (Request.ContentLength is > 0 || Request.ContentType?.Contains("json", StringComparison.OrdinalIgnoreCase) == true)
        {
            try
            {
                body = await JsonSerializer.DeserializeAsync<ResilienceConfigRequest>(Request.Body, JsonOptions, ct);
            }
            catch (JsonException)
            {
                return Problem(statusCode: 400, title: "Invalid JSON payload.");
            }
        }

        var merged = new ResilienceConfigRequest(
            Mode: mode ?? body?.Mode,
            DelayMs: delayMs ?? body?.DelayMs,
            PerTryTimeoutMs: perTryTimeoutMs ?? body?.PerTryTimeoutMs,
            OverallDeadlineMs: overallDeadlineMs ?? body?.OverallDeadlineMs,
            MaxRetries: maxRetries ?? body?.MaxRetries,
            RetryBudgetPercent: retryBudgetPercent ?? body?.RetryBudgetPercent,
            CircuitBreakerFailureThreshold: circuitBreakerFailureThreshold ?? body?.CircuitBreakerFailureThreshold,
            CircuitBreakerOpenMs: circuitBreakerOpenMs ?? body?.CircuitBreakerOpenMs,
            ResetCounters: resetCounters ?? body?.ResetCounters);

        try
        {
            return Ok(resilience.Configure(merged));
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

    [HttpGet("/diagnostics/resilience/status")]
    public IActionResult Status() => Ok(resilience.GetStatus());

    [HttpPost("/diagnostics/resilience/jitter-sample")]
    public IActionResult JitterSample(
        [FromQuery] int baseMs = 50,
        [FromQuery] int capMs = 1000,
        [FromQuery] int attempt = 3,
        [FromQuery] int samples = 12)
    {
        if (baseMs is < 1 or > 5000)
            return Problem(statusCode: 400, title: "baseMs must be between 1 and 5000.");
        if (capMs is < 1 or > 60000)
            return Problem(statusCode: 400, title: "capMs must be between 1 and 60000.");
        if (attempt is < 0 or > 10)
            return Problem(statusCode: 400, title: "attempt must be between 0 and 10.");
        if (samples is < 2 or > 50)
            return Problem(statusCode: 400, title: "samples must be between 2 and 50.");

        return Ok(resilience.SampleFullJitter(baseMs, capMs, attempt, samples));
    }

    [HttpPost("/resilience/orders/quote-with-deadline")]
    public async Task<IActionResult> QuoteWithDeadline(
        [FromQuery] int? callerBudgetMs,
        [FromQuery] int? gatewaySpentMs,
        [FromQuery] int? downstreamDelayMs,
        [FromQuery] bool? enforceDeadline,
        CancellationToken ct)
    {
        QuoteWithDeadlineRequest? body = null;
        if (Request.ContentLength is > 0 || Request.ContentType?.Contains("json", StringComparison.OrdinalIgnoreCase) == true)
        {
            try
            {
                body = await JsonSerializer.DeserializeAsync<QuoteWithDeadlineRequest>(Request.Body, JsonOptions, ct);
            }
            catch (JsonException)
            {
                return Problem(statusCode: 400, title: "Invalid JSON payload.");
            }
        }

        var budget = callerBudgetMs ?? body?.CallerBudgetMs ?? 400;
        var spent = gatewaySpentMs ?? body?.GatewaySpentMs ?? 0;
        var delay = downstreamDelayMs ?? body?.DownstreamDelayMs ?? 600;
        var enforce = enforceDeadline ?? body?.EnforceDeadline ?? true;

        if (budget is < 10 or > 10000)
            return Problem(statusCode: 400, title: "callerBudgetMs must be between 10 and 10000.");
        if (spent is < 0 or > 10000)
            return Problem(statusCode: 400, title: "gatewaySpentMs must be between 0 and 10000.");
        if (delay is < 0 or > 5000)
            return Problem(statusCode: 400, title: "downstreamDelayMs must be between 0 and 5000.");

        int? headerRemaining = null;
        if (Request.Headers.TryGetValue("X-Deadline-Remaining-Ms", out var rawHeader) &&
            int.TryParse(rawHeader.FirstOrDefault(), out var parsedHeader))
        {
            headerRemaining = parsedHeader;
        }

        var result = await resilience.ExecuteQuoteWithDeadlineAsync(
            budget, spent, delay, enforce, headerRemaining, ct);
        Response.Headers["X-Deadline-Remaining-Ms"] = result.RemainingAfterMs.ToString();
        return StatusCode(result.StatusCode, result);
    }

    [HttpPost("/resilience/orders/dispatch-probe")]
    public async Task<IActionResult> DispatchProbe(
        [FromQuery] string? mode,
        [FromQuery] int? hops,
        [FromQuery] int? perHopAttempts,
        [FromQuery] int? baseBackoffMs,
        [FromQuery] int? capBackoffMs,
        CancellationToken ct)
    {
        DispatchProbeRequest? body = null;
        if (Request.ContentLength is > 0 || Request.ContentType?.Contains("json", StringComparison.OrdinalIgnoreCase) == true)
        {
            try
            {
                body = await JsonSerializer.DeserializeAsync<DispatchProbeRequest>(Request.Body, JsonOptions, ct);
            }
            catch (JsonException)
            {
                return Problem(statusCode: 400, title: "Invalid JSON payload.");
            }
        }

        var effectiveMode = (mode ?? body?.Mode ?? "resilient").Trim().ToLowerInvariant();
        if (effectiveMode is not ("naive-amplified" or "resilient"))
            return Problem(statusCode: 400, title: "mode must be 'naive-amplified' or 'resilient'.");

        var effectiveHops = hops ?? body?.Hops ?? 2;
        var effectiveAttempts = perHopAttempts ?? body?.PerHopAttempts ?? 3;
        var effectiveBase = baseBackoffMs ?? body?.BaseBackoffMs ?? 10;
        var effectiveCap = capBackoffMs ?? body?.CapBackoffMs ?? 80;

        if (effectiveHops is < 1 or > 3)
            return Problem(statusCode: 400, title: "hops must be between 1 and 3.");
        if (effectiveAttempts is < 1 or > 5)
            return Problem(statusCode: 400, title: "perHopAttempts must be between 1 and 5.");
        if (effectiveBase is < 1 or > 200)
            return Problem(statusCode: 400, title: "baseBackoffMs must be between 1 and 200.");
        if (effectiveCap is < 5 or > 1000)
            return Problem(statusCode: 400, title: "capBackoffMs must be between 5 and 1000.");

        var result = await resilience.ExecuteDispatchProbeAsync(
            effectiveMode, effectiveHops, effectiveAttempts, effectiveBase, effectiveCap, ct);
        return StatusCode(result.StatusCode, result);
    }

    [HttpPost("/resilience/orders/protected-call")]
    public async Task<IActionResult> ProtectedCall(CancellationToken ct)
    {
        var result = await resilience.ExecuteProtectedCallAsync(ct);
        Response.Headers["X-Circuit-State"] = result.CircuitState;
        return StatusCode(result.StatusCode, result);
    }
}
