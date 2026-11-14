using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(o =>
{
    o.SingleLine = true;
    o.TimestampFormat = "HH:mm:ss.fff ";
});

var orderApiBaseUrl = builder.Configuration["Upstream:OrderApiBaseUrl"]
    ?? throw new InvalidOperationException("Upstream:OrderApiBaseUrl is missing.");
var catalogTimeoutMs = builder.Configuration.GetValue<int?>("Upstream:CatalogTimeoutMs") ?? 1500;

builder.Services.AddProblemDetails();
builder.Services.AddHttpClient("OrderApi", client =>
{
    client.BaseAddress = new Uri(orderApiBaseUrl);
    client.Timeout = TimeSpan.FromSeconds(10);
});

var app = builder.Build();

app.Logger.LogWarning(
    "gateway_config upstream_order_api={Upstream} catalog_timeout_ms={TimeoutMs} environment={Env}",
    orderApiBaseUrl, catalogTimeoutMs, app.Environment.EnvironmentName);

var jsonOptions = new JsonSerializerOptions(JsonSerializerDefaults.Web);
var internalNaivePattern = new Regex(@"^/api/orders/\d+/naive-join-receipt$", RegexOptions.IgnoreCase | RegexOptions.Compiled);

app.Use(async (context, next) =>
{
    var path = context.Request.Path.Value ?? string.Empty;
    if (!path.StartsWith("/api", StringComparison.OrdinalIgnoreCase) &&
        !path.StartsWith("/bff", StringComparison.OrdinalIgnoreCase))
    {
        await next();
        return;
    }

    var correlationId = context.Request.Headers["X-Correlation-Id"].FirstOrDefault();
    if (string.IsNullOrWhiteSpace(correlationId) || correlationId.Length > 128)
        correlationId = Guid.NewGuid().ToString("N");

    context.Items["CorrelationId"] = correlationId;
    context.Response.OnStarting(() =>
    {
        context.Response.Headers["X-Correlation-Id"] = correlationId;
        return Task.CompletedTask;
    });

    var clientHeaders = context.Request.Headers["X-Tadka-Client"];
    var clientType = clientHeaders.Count == 1 ? clientHeaders.ToString() : null;
    if (clientType is not ("mobile-app" or "partner-web"))
    {
        await Results.Problem(
            statusCode: StatusCodes.Status400BadRequest,
            title: "X-Tadka-Client header must be 'mobile-app' or 'partner-web'."
        ).ExecuteAsync(context);
        return;
    }

    context.Items["ClientType"] = clientType;

    if (internalNaivePattern.IsMatch(path))
    {
        app.Logger.LogWarning("gw_blocked_internal path={Path} client={Client} correlation_id={CorrelationId}",
            path, clientType, correlationId);
        await Results.Problem(
            statusCode: StatusCodes.Status403Forbidden,
            title: "Internal diagnostic routes are not exposed through the gateway."
        ).ExecuteAsync(context);
        return;
    }

    using (app.Logger.BeginScope(new Dictionary<string, object> { ["CorrelationId"] = correlationId }))
    {
        await next();
    }
});

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.MapGet("/api/catalog/restaurants/{restaurantId:long}/menu", async (
    long restaurantId,
    HttpContext context,
    IHttpClientFactory httpClientFactory,
    CancellationToken ct) =>
{
    var correlationId = (string)context.Items["CorrelationId"]!;
    var client = httpClientFactory.CreateClient("OrderApi");
    using var request = new HttpRequestMessage(HttpMethod.Get, $"/catalog/restaurants/{restaurantId}/menu");
    request.Headers.TryAddWithoutValidation("X-Correlation-Id", correlationId);
    ForwardDemoCatalogFailureHeader(context, request, app.Environment);

    return await ForwardUpstreamAsync(client, request, app.Logger, correlationId, ct);
});

app.MapPost("/api/orders/checkout", async (
    HttpContext context,
    IHttpClientFactory httpClientFactory,
    CancellationToken ct) =>
{
    var correlationId = (string)context.Items["CorrelationId"]!;
    using var reader = new StreamReader(context.Request.Body, Encoding.UTF8);
    var body = await reader.ReadToEndAsync(ct);

    var client = httpClientFactory.CreateClient("OrderApi");
    using var request = new HttpRequestMessage(HttpMethod.Post, "/orders/checkout")
    {
        Content = new StringContent(body, Encoding.UTF8, "application/json")
    };
    request.Headers.TryAddWithoutValidation("X-Correlation-Id", correlationId);
    if (context.Request.Headers.TryGetValue("Idempotency-Key", out var idemKeys))
    {
        foreach (var k in idemKeys)
            request.Headers.TryAddWithoutValidation("Idempotency-Key", k);
    }

    try
    {
        using var response = await client.SendAsync(request, ct);
        if (response.Headers.TryGetValues("Idempotency-Replayed", out var replayValues))
            context.Response.Headers["Idempotency-Replayed"] = string.Join(",", replayValues);

        var responseBody = await response.Content.ReadAsStringAsync(ct);
        var contentType = response.Content.Headers.ContentType?.ToString() ?? "application/json";
        return Results.Content(responseBody, contentType, statusCode: (int)response.StatusCode);
    }
    catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
    {
        app.Logger.LogError(ex, "gw_upstream_checkout_failed correlation_id={CorrelationId}", correlationId);
        return Results.Problem(statusCode: StatusCodes.Status502BadGateway, title: "Upstream Order API unavailable.");
    }
});

app.MapPatch("/api/orders/{id:long}/status", async (
    long id,
    HttpContext context,
    IHttpClientFactory httpClientFactory,
    CancellationToken ct) =>
{
    var clientType = (string)context.Items["ClientType"]!;
    var correlationId = (string)context.Items["CorrelationId"]!;

    if (clientType != "partner-web")
    {
        app.Logger.LogWarning("gw_forbidden_role order_id={OrderId} client={Client} correlation_id={CorrelationId}",
            id, clientType, correlationId);
        return Results.Problem(
            statusCode: StatusCodes.Status403Forbidden,
            title: "Only partner-web clients may update order status.");
    }

    using var reader = new StreamReader(context.Request.Body, Encoding.UTF8);
    var body = await reader.ReadToEndAsync(ct);

    var client = httpClientFactory.CreateClient("OrderApi");
    using var request = new HttpRequestMessage(HttpMethod.Patch, $"/orders/{id}/status")
    {
        Content = new StringContent(body, Encoding.UTF8, "application/json")
    };
    request.Headers.TryAddWithoutValidation("X-Correlation-Id", correlationId);

    return await ForwardUpstreamAsync(client, request, app.Logger, correlationId, ct);
});

app.MapGet("/bff/mobile/orders/{id:long}/tracking", async (
    long id,
    string? mode,
    HttpContext context,
    IHttpClientFactory httpClientFactory,
    CancellationToken ct) =>
{
    var effectiveMode = string.IsNullOrEmpty(mode) ? "resilient" : mode;
    if (effectiveMode is not ("resilient" or "strict"))
        return Results.Problem(statusCode: StatusCodes.Status400BadRequest, title: "Query mode must be 'resilient' or 'strict'.");

    var correlationId = (string)context.Items["CorrelationId"]!;
    var client = httpClientFactory.CreateClient("OrderApi");

    UpstreamReceipt? receipt;
    UpstreamStatus? status;
    try
    {
        var receiptTask = FetchJsonAsync<UpstreamReceipt>(client, $"/orders/{id}/receipt", correlationId, jsonOptions, ct);
        var statusTask = FetchJsonAsync<UpstreamStatus>(client, $"/orders/{id}/status", correlationId, jsonOptions, ct);
        await Task.WhenAll(receiptTask, statusTask);
        receipt = receiptTask.Result;
        status = statusTask.Result;
    }
    catch (UpstreamMissingException)
    {
        return Results.Problem(statusCode: StatusCodes.Status404NotFound, title: "Order not found.");
    }
    catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or UpstreamFailureException)
    {
        app.Logger.LogError(ex, "bff_orders_upstream_failed order_id={OrderId} correlation_id={CorrelationId}", id, correlationId);
        return Results.Problem(statusCode: StatusCodes.Status502BadGateway, title: "Upstream Order API unavailable.");
    }

    string? restaurantName = null;
    var catalogDegraded = false;
    try
    {
        using var catalogCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        catalogCts.CancelAfter(TimeSpan.FromMilliseconds(catalogTimeoutMs));

        using var menuReq = new HttpRequestMessage(HttpMethod.Get, $"/catalog/restaurants/{receipt!.RestaurantId}/menu");
        menuReq.Headers.TryAddWithoutValidation("X-Correlation-Id", correlationId);
        ForwardDemoCatalogFailureHeader(context, menuReq, app.Environment);

        using var menuRes = await client.SendAsync(menuReq, catalogCts.Token);
        if (!menuRes.IsSuccessStatusCode)
            throw new UpstreamFailureException((int)menuRes.StatusCode);

        var menu = await menuRes.Content.ReadFromJsonAsync<UpstreamMenu>(jsonOptions, catalogCts.Token);
        restaurantName = menu?.RestaurantName;
    }
    catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or UpstreamFailureException)
    {
        if (effectiveMode == "strict")
        {
            app.Logger.LogWarning(ex, "bff_catalog_strict_failure order_id={OrderId} correlation_id={CorrelationId}", id, correlationId);
            return Results.Problem(
                statusCode: StatusCodes.Status502BadGateway,
                title: "Catalog dependency failed and mode=strict does not allow degradation.");
        }

        catalogDegraded = true;
        app.Logger.LogWarning(ex, "bff_catalog_degraded order_id={OrderId} correlation_id={CorrelationId}", id, correlationId);
    }

    var itemCount = receipt!.Items.Sum(i => i.Quantity);
    app.Logger.LogInformation(
        "bff_mobile_tracking order_id={OrderId} degraded={Degraded} correlation_id={CorrelationId}",
        id, catalogDegraded, correlationId);

    return Results.Ok(new MobileTrackingResponse(
        id,
        status!.Status,
        status.Version,
        receipt.TotalAmount,
        itemCount,
        receipt.RestaurantId,
        restaurantName,
        catalogDegraded,
        correlationId));
});

app.MapGet("/bff/partner/orders/{id:long}", async (
    long id,
    HttpContext context,
    IHttpClientFactory httpClientFactory,
    CancellationToken ct) =>
{
    var clientType = (string)context.Items["ClientType"]!;
    var correlationId = (string)context.Items["CorrelationId"]!;
    if (clientType != "partner-web")
    {
        app.Logger.LogWarning("gw_forbidden_role path=/bff/partner/orders client={Client} correlation_id={CorrelationId}",
            clientType, correlationId);
        return Results.Problem(
            statusCode: StatusCodes.Status403Forbidden,
            title: "Only partner-web clients may access partner order details.");
    }

    var client = httpClientFactory.CreateClient("OrderApi");

    UpstreamReceipt? receipt;
    UpstreamStatus? status;
    try
    {
        var receiptTask = FetchJsonAsync<UpstreamReceipt>(client, $"/orders/{id}/receipt", correlationId, jsonOptions, ct);
        var statusTask = FetchJsonAsync<UpstreamStatus>(client, $"/orders/{id}/status", correlationId, jsonOptions, ct);
        await Task.WhenAll(receiptTask, statusTask);
        receipt = receiptTask.Result;
        status = statusTask.Result;
    }
    catch (UpstreamMissingException)
    {
        return Results.Problem(statusCode: StatusCodes.Status404NotFound, title: "Order not found.");
    }
    catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or UpstreamFailureException)
    {
        app.Logger.LogError(ex, "bff_partner_upstream_failed order_id={OrderId} correlation_id={CorrelationId}", id, correlationId);
        return Results.Problem(statusCode: StatusCodes.Status502BadGateway, title: "Upstream Order API unavailable.");
    }

    string? restaurantName = null;
    bool? restaurantOpen = null;
    var catalogDegraded = false;
    try
    {
        using var catalogCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        catalogCts.CancelAfter(TimeSpan.FromMilliseconds(catalogTimeoutMs));

        using var menuReq = new HttpRequestMessage(HttpMethod.Get, $"/catalog/restaurants/{receipt!.RestaurantId}/menu");
        menuReq.Headers.TryAddWithoutValidation("X-Correlation-Id", correlationId);
        ForwardDemoCatalogFailureHeader(context, menuReq, app.Environment);

        using var menuRes = await client.SendAsync(menuReq, catalogCts.Token);
        if (!menuRes.IsSuccessStatusCode)
            throw new UpstreamFailureException((int)menuRes.StatusCode);

        var menu = await menuRes.Content.ReadFromJsonAsync<UpstreamMenu>(jsonOptions, catalogCts.Token);
        restaurantName = menu?.RestaurantName;
        restaurantOpen = menu?.IsOpen;
    }
    catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or UpstreamFailureException)
    {
        catalogDegraded = true;
        app.Logger.LogWarning(ex, "bff_partner_catalog_degraded order_id={OrderId} correlation_id={CorrelationId}", id, correlationId);
    }

    app.Logger.LogInformation("bff_partner_order order_id={OrderId} degraded={Degraded} correlation_id={CorrelationId}",
        id, catalogDegraded, correlationId);

    return Results.Ok(new PartnerOrderResponse(
        id,
        receipt!.CustomerId,
        receipt.RestaurantId,
        restaurantName,
        restaurantOpen,
        status!.Status,
        status.Version,
        receipt.TotalAmount,
        receipt.Items,
        catalogDegraded,
        correlationId));
});

app.Run();

static void ForwardDemoCatalogFailureHeader(HttpContext context, HttpRequestMessage request, IHostEnvironment environment)
{
    if (environment.IsEnvironment("Demo") &&
        context.Request.Headers.TryGetValue("X-Demo-Catalog-Failure", out var failureHeader))
    {
        request.Headers.TryAddWithoutValidation("X-Demo-Catalog-Failure", failureHeader.ToString());
    }
}

static async Task<IResult> ForwardUpstreamAsync(
    HttpClient client,
    HttpRequestMessage request,
    ILogger logger,
    string correlationId,
    CancellationToken ct)
{
    try
    {
        using var response = await client.SendAsync(request, ct);
        var body = await response.Content.ReadAsStringAsync(ct);
        var contentType = response.Content.Headers.ContentType?.ToString() ?? "application/json";
        return Results.Content(body, contentType, statusCode: (int)response.StatusCode);
    }
    catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
    {
        logger.LogError(ex, "gw_upstream_failed correlation_id={CorrelationId}", correlationId);
        return Results.Problem(statusCode: StatusCodes.Status502BadGateway, title: "Upstream Order API unavailable.");
    }
}

static async Task<T> FetchJsonAsync<T>(
    HttpClient client,
    string path,
    string correlationId,
    JsonSerializerOptions options,
    CancellationToken ct)
{
    using var request = new HttpRequestMessage(HttpMethod.Get, path);
    request.Headers.TryAddWithoutValidation("X-Correlation-Id", correlationId);
    using var response = await client.SendAsync(request, ct);
    if (response.StatusCode == HttpStatusCode.NotFound)
        throw new UpstreamMissingException();
    if (!response.IsSuccessStatusCode)
        throw new UpstreamFailureException((int)response.StatusCode);
    return (await response.Content.ReadFromJsonAsync<T>(options, ct))
        ?? throw new UpstreamFailureException(502);
}

internal sealed class UpstreamMissingException : Exception;
internal sealed class UpstreamFailureException(int statusCode) : Exception($"Upstream status {statusCode}");

internal sealed record UpstreamReceiptLine(long MenuItemId, string ItemName, decimal UnitPrice, int Quantity, decimal LineTotal);
internal sealed record UpstreamReceipt(long OrderId, long CustomerId, long RestaurantId, string Status, long StatusVersion, decimal TotalAmount, IReadOnlyList<UpstreamReceiptLine> Items);
internal sealed record UpstreamStatus(long OrderId, string Status, long Version);
internal sealed record UpstreamMenu(long RestaurantId, string RestaurantName, bool IsOpen);

internal sealed record MobileTrackingResponse(
    long OrderId,
    string Status,
    long StatusVersion,
    decimal TotalAmount,
    int ItemCount,
    long RestaurantId,
    string? RestaurantName,
    bool CatalogDegraded,
    string CorrelationId);

internal sealed record PartnerOrderResponse(
    long OrderId,
    long CustomerId,
    long RestaurantId,
    string? RestaurantName,
    bool? RestaurantOpen,
    string Status,
    long StatusVersion,
    decimal TotalAmount,
    IReadOnlyList<UpstreamReceiptLine> Items,
    bool CatalogDegraded,
    string CorrelationId);
