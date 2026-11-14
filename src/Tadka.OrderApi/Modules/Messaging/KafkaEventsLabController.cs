using Microsoft.AspNetCore.Mvc;

namespace Tadka.OrderApi.Modules.Messaging;

[ApiController]
[Route("diagnostics/kafka")]
public sealed class KafkaEventsLabController(KafkaEventsLabService service) : ControllerBase
{
    [HttpPost("reset")]
    public async Task<IActionResult> Reset(CancellationToken ct)
    {
        var result = await service.ResetAsync(ct);
        return Ok(result);
    }

    [HttpPost("publish-lifecycle")]
    public async Task<IActionResult> PublishLifecycle(
        [FromQuery] string keyStrategy = "order-id",
        [FromQuery] long orderId = 9001,
        [FromQuery] long customerId = 42,
        [FromQuery] long restaurantId = 7,
        CancellationToken ct = default)
    {
        try
        {
            var result = await service.PublishLifecycleAsync(keyStrategy, orderId, customerId, restaurantId, ct);
            return Ok(result);
        }
        catch (ArgumentException ex)
        {
            return Problem(statusCode: 400, title: ex.Message);
        }
    }

    [HttpPost("consumer-group-layout")]
    public async Task<IActionResult> ConsumerGroupLayout(
        [FromQuery] int consumerCount = 2,
        CancellationToken ct = default)
    {
        try
        {
            var result = await service.InspectConsumerGroupLayoutAsync(consumerCount, ct);
            return Ok(result);
        }
        catch (ArgumentOutOfRangeException ex)
        {
            return Problem(statusCode: 400, title: ex.Message);
        }
    }

    [HttpPost("rebalance-simulation")]
    public async Task<IActionResult> RebalanceSimulation(
        [FromQuery] string commitMode = "cooperative-commit-on-revoke",
        CancellationToken ct = default)
    {
        try
        {
            var result = await service.SimulateRebalanceAsync(commitMode, ct);
            return Ok(result);
        }
        catch (ArgumentException ex)
        {
            return Problem(statusCode: 400, title: ex.Message);
        }
    }
}
