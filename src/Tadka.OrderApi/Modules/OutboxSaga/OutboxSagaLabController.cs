using Microsoft.AspNetCore.Mvc;

namespace Tadka.OrderApi.Modules.OutboxSaga;

[ApiController]
[Route("diagnostics")]
public sealed class OutboxSagaLabController(OutboxSagaLabService lab) : ControllerBase
{
    [HttpPost("outbox/reset")]
    public async Task<IActionResult> Reset(CancellationToken ct)
    {
        var result = await lab.ResetAsync(ct);
        return Ok(result);
    }

    [HttpPost("outbox/dual-write")]
    public async Task<IActionResult> DualWrite(
        [FromQuery] string mode = "transactional-outbox",
        CancellationToken ct = default)
    {
        try
        {
            var result = await lab.ExecuteDualWriteAsync(mode, ct);
            return Ok(result);
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    [HttpPost("outbox/relay")]
    public async Task<IActionResult> Relay(
        [FromQuery] bool simulateCrashAfterPublishBeforeMark = false,
        CancellationToken ct = default)
    {
        var result = await lab.RelayAsync(simulateCrashAfterPublishBeforeMark, ct);
        return Ok(result);
    }

    [HttpPost("saga/execute")]
    public async Task<IActionResult> ExecuteSaga(
        [FromQuery] string scenario = "happy-path",
        CancellationToken ct = default)
    {
        try
        {
            var result = await lab.ExecuteSagaAsync(scenario, ct);
            return Ok(result);
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }
}
