using Microsoft.AspNetCore.Mvc;

namespace Tadka.OrderApi.Modules.Messaging;

[ApiController]
[Route("diagnostics/kafka/reliable")]
public sealed class ReliableConsumerLabController(ReliableConsumerLabService lab) : ControllerBase
{
    [HttpPost("reset")]
    public async Task<IActionResult> Reset(CancellationToken ct)
    {
        var result = await lab.ResetAsync(ct);
        return Ok(result);
    }

    [HttpPost("duplicate-delivery")]
    public async Task<IActionResult> DuplicateDelivery(
        [FromQuery] string mode = "idempotent",
        [FromQuery] long customerId = 501,
        [FromQuery] long orderId = 9001,
        [FromQuery] int points = 50,
        [FromQuery] int deliveries = 3,
        CancellationToken ct = default)
    {
        try
        {
            var result = await lab.SimulateDuplicateDeliveryAsync(mode, customerId, orderId, points, deliveries, ct);
            return Ok(result);
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    [HttpPost("poison-pipeline")]
    public async Task<IActionResult> PoisonPipeline(
        [FromQuery] string policy = "retry-then-dlq",
        [FromQuery] int maxAttempts = 3,
        CancellationToken ct = default)
    {
        try
        {
            var result = await lab.RunPoisonPipelineAsync(policy, maxAttempts, ct);
            return Ok(result);
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }
}
