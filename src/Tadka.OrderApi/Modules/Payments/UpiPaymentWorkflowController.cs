using System.Text;
using Microsoft.AspNetCore.Mvc;

namespace Tadka.OrderApi.Modules.Payments;

[ApiController]
[Route("payments/upi")]
public sealed class UpiPaymentWorkflowController(UpiPaymentWorkflowService payments) : ControllerBase
{
    [HttpPost("reset")]
    public async Task<IActionResult> Reset(CancellationToken ct)
    {
        var result = await payments.ResetAsync(ct);
        return Ok(result);
    }

    [HttpPost("intents")]
    public async Task<IActionResult> CreateIntent(
        [FromHeader(Name = "Idempotency-Key")] string? idempotencyKey,
        [FromBody] CreateUpiIntentRequest request,
        CancellationToken ct)
    {
        var (statusCode, body, errorCode, errorMessage) =
            await payments.CreateOrReplayIntentAsync(idempotencyKey, request, ct);

        if (statusCode == 201 && body is not null)
        {
            Response.Headers["Idempotency-Status"] = "CREATED";
            return Created($"/payments/upi/intents/{body.IntentRef}/audit", body);
        }

        if (statusCode == 200 && body is not null)
        {
            Response.Headers["Idempotency-Status"] = "REPLAYED";
            return Ok(body);
        }

        return StatusCode(statusCode, new { error = errorCode, message = errorMessage });
    }

    [HttpPost("sign-webhook")]
    public IActionResult SignWebhook([FromBody] SignWebhookRequest request)
    {
        try
        {
            return Ok(payments.SignWebhook(request));
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    [HttpPost("webhooks")]
    public async Task<IActionResult> HandleWebhook(
        [FromQuery] string mode = "safe",
        CancellationToken ct = default)
    {
        string rawBody;
        using (var reader = new StreamReader(Request.Body, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, leaveOpen: true))
        {
            rawBody = await reader.ReadToEndAsync(ct);
        }

        var sigHeader = Request.Headers["X-Tadka-Signature"].FirstOrDefault();
        var (statusCode, body, errorCode, errorMessage) =
            await payments.ProcessWebhookAsync(rawBody, sigHeader, mode, ct);

        if (statusCode == 200 && body is not null)
            return Ok(body);

        return StatusCode(statusCode, new { error = errorCode, message = errorMessage });
    }

    [HttpPost("reconcile")]
    public async Task<IActionResult> Reconcile(
        [FromQuery] string pspOutcome = "CAPTURED",
        CancellationToken ct = default)
    {
        try
        {
            var result = await payments.ReconcilePendingAsync(pspOutcome, ct);
            return Ok(result);
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    [HttpGet("intents/{intentRef}/audit")]
    public async Task<IActionResult> GetAudit(string intentRef, CancellationToken ct)
    {
        var audit = await payments.GetAuditAsync(intentRef, ct);
        return audit is null ? NotFound(new { error = "INTENT_NOT_FOUND" }) : Ok(audit);
    }
}
