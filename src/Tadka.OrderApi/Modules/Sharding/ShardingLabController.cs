using Microsoft.AspNetCore.Mvc;

namespace Tadka.OrderApi.Modules.Sharding;

public sealed record SimulateShardingWorkloadRequest(
    string Strategy = "by-restaurant",
    int TotalOrders = 400,
    double HotRestaurantShare = 0.80);

[ApiController]
public sealed class ShardingLabController(
    ShardingLabService sharding,
    ILogger<ShardingLabController> logger) : ControllerBase
{
    [HttpPost("sharding/reset")]
    public async Task<IActionResult> Reset(CancellationToken ct)
    {
        await sharding.ResetAsync(ct);
        return Ok(new { reset = true, shards = ShardRouter.ShardCount });
    }

    [HttpPost("sharding/simulate-workload")]
    [ProducesResponseType<ShardingSimulationResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> SimulateWorkload(
        [FromBody] SimulateShardingWorkloadRequest request,
        CancellationToken ct)
    {
        if (request.Strategy is not ("by-restaurant" or "by-customer" or "salted-restaurant"))
            return Problem(statusCode: 400, title: "Strategy must be 'by-restaurant', 'by-customer' or 'salted-restaurant'.");

        if (request.TotalOrders is < 20 or > 5000 || request.HotRestaurantShare is < 0 or > 1)
            return Problem(statusCode: 400, title: "totalOrders must be 20..5000 and hotRestaurantShare 0..1.");

        var result = await sharding.SimulateWorkloadAsync(
            request.Strategy,
            request.TotalOrders,
            request.HotRestaurantShare,
            ct);

        logger.LogInformation(
            "sharding_simulated strategy={Strategy} orders={Orders} max_pct={MaxPct} skew={Skew}",
            result.Strategy,
            result.TotalOrders,
            result.MaxShardPct,
            result.SkewRatio);
        return Ok(result);
    }

    [HttpGet("sharding/customer-orders")]
    [ProducesResponseType<ShardedQueryResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> CustomerOrders(
        CancellationToken ct,
        long customerId = 1042,
        string strategy = "by-customer",
        int limit = 20)
    {
        if (customerId <= 0 || limit is < 1 or > 100 || strategy is not ("by-restaurant" or "by-customer" or "salted-restaurant"))
            return Problem(statusCode: 400, title: "Provide positive customerId, limit 1..100 and valid strategy.");

        var result = await sharding.GetCustomerOrdersAsync(customerId, strategy, limit, ct);
        logger.LogInformation(
            "sharding_customer_query strategy={Strategy} shards_touched={ShardsTouched} returned={Returned}",
            strategy,
            result.ShardsTouched,
            result.ReturnedCount);
        return Ok(result);
    }

    [HttpGet("sharding/restaurant-orders")]
    [ProducesResponseType<ShardedQueryResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> RestaurantOrders(
        CancellationToken ct,
        long restaurantId = 7,
        string strategy = "salted-restaurant",
        int limit = 20)
    {
        if (restaurantId <= 0 || limit is < 1 or > 100 || strategy is not ("by-restaurant" or "by-customer" or "salted-restaurant"))
            return Problem(statusCode: 400, title: "Provide positive restaurantId, limit 1..100 and valid strategy.");

        var result = await sharding.GetRestaurantOrdersAsync(restaurantId, strategy, limit, ct);
        logger.LogInformation(
            "sharding_restaurant_query strategy={Strategy} shards_touched={ShardsTouched} returned={Returned}",
            strategy,
            result.ShardsTouched,
            result.ReturnedCount);
        return Ok(result);
    }
}
