using Microsoft.AspNetCore.Diagnostics;
using Npgsql;

namespace Tadka.OrderApi.Middleware;

public sealed class DatabaseExceptionHandler(ILogger<DatabaseExceptionHandler> logger) : IExceptionHandler
{
    public async ValueTask<bool> TryHandleAsync(HttpContext context, Exception exception, CancellationToken ct)
    {
        if (exception is not NpgsqlException) return false;
        logger.LogError(exception, "Database operation failed request_id={RequestId}", context.TraceIdentifier);
        context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
        await Results.Problem(statusCode: 503, title: "Database unavailable. Retry with the same idempotency key.").ExecuteAsync(context);
        return true;
    }
}
