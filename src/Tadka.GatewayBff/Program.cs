using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

Activity.DefaultIdFormat = ActivityIdFormat.W3C;
Activity.ForceDefaultIdFormat = true;

using var gatewayActivitySource = new ActivitySource("Tadka.GatewayBff", "1.0.0");
using var gatewayActivityListener = new ActivityListener
{
    ShouldListenTo = source => source.Name == "Tadka.GatewayBff",
    Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
};
ActivitySource.AddActivityListener(gatewayActivityListener);

var builder = WebApplication.CreateBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(o =>
{
    o.SingleLine = true;
    o.TimestampFormat = "HH:mm:ss.fff ";
});

var orderApiBaseUrl = builder.Configuration["Upstream:OrderApiBaseUrl"]
    ?? throw new InvalidOperationException("Upstream:OrderApiBaseUrl is missing.");
var configuredNodes = builder.Configuration.GetSection("Upstream:OrderApiNodes").Get<string[]>();
var envNodesRaw = Environment.GetEnvironmentVariable("ORDER_API_NODES");
var orderApiNodes = !string.IsNullOrWhiteSpace(envNodesRaw)
    ? envNodesRaw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
    : (configuredNodes is { Length: > 0 } ? configuredNodes : [orderApiBaseUrl]);
var catalogTimeoutMs = builder.Configuration.GetValue<int?>("Upstream:CatalogTimeoutMs") ?? 1500;
const int EdgeTtlSeconds = 15;

builder.Services.AddProblemDetails();
builder.Services.AddHttpClient("OrderApi", client =>
{
    client.BaseAddress = new Uri(orderApiBaseUrl);
    client.Timeout = TimeSpan.FromSeconds(10);
});
builder.Services.AddHttpClient("OrderApiNode", client =>
{
    client.Timeout = TimeSpan.FromSeconds(10);
});

var app = builder.Build();

var lbPool = new LoadBalancerPool(orderApiNodes, app.Logger);
app.Logger.LogWarning(
    "gateway_config upstream_order_api={Upstream} lb_nodes={LbNodes} catalog_timeout_ms={TimeoutMs} edge_ttl_s={EdgeTtl} environment={Env}",
    orderApiBaseUrl, string.Join(",", orderApiNodes), catalogTimeoutMs, EdgeTtlSeconds, app.Environment.EnvironmentName);

var jsonOptions = new JsonSerializerOptions(JsonSerializerDefaults.Web);
var internalNaivePattern = new Regex(@"^/api/orders/\d+/naive-join-receipt$", RegexOptions.IgnoreCase | RegexOptions.Compiled);

var edgeStore = new ConcurrentDictionary<long, EdgeCacheEntry>();
var edgeStats = new EdgeCacheCounters();

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

app.MapPost("/diagnostics/edge-cache/reset", () =>
{
    edgeStore.Clear();
    edgeStats.Reset();
    return Results.Ok(new { reset = true });
});

app.MapGet("/diagnostics/edge-cache/stats", () =>
{
    return Results.Ok(new
    {
        edgeHits = edgeStats.Hits,
        edgeMisses = edgeStats.Misses,
        edgeRevalidated = edgeStats.Revalidated,
        edgeRefreshed = edgeStats.Refreshed,
        cachedRestaurantIds = edgeStore.Keys.OrderBy(k => k).ToArray(),
        entryCount = edgeStore.Count
    });
});

app.MapPost("/diagnostics/lb/config", async (
    string? algorithm,
    bool? healthCheckEnabled,
    bool? resetStats,
    HttpContext context) =>
{
    LbConfigRequest? body = null;
    if (context.Request.ContentLength is > 0 || context.Request.ContentType?.Contains("json", StringComparison.OrdinalIgnoreCase) == true)
    {
        try
        {
            body = await JsonSerializer.DeserializeAsync<LbConfigRequest>(context.Request.Body, jsonOptions, context.RequestAborted);
        }
        catch (JsonException)
        {
            return Results.Problem(statusCode: 400, title: "Invalid JSON payload.");
        }
    }

    try
    {
        var snapshot = lbPool.Configure(
            algorithm ?? body?.Algorithm,
            healthCheckEnabled ?? body?.HealthCheckEnabled,
            (resetStats ?? false) || (body?.ResetStats ?? false));
        return Results.Ok(snapshot);
    }
    catch (ArgumentException ex)
    {
        return Results.Problem(statusCode: 400, title: ex.Message);
    }
});

app.MapPost("/diagnostics/lb/probe", async (
    IHttpClientFactory httpClientFactory,
    CancellationToken ct) =>
{
    await lbPool.ProbeAllAsync(httpClientFactory, ct);
    return Results.Ok(lbPool.GetStatus());
});

app.MapGet("/diagnostics/lb/status", () => Results.Ok(lbPool.GetStatus()));

async Task<IResult> RouteCatalogMenuThroughPoolAsync(
    long restaurantId,
    HttpContext context,
    IHttpClientFactory httpClientFactory,
    CancellationToken ct)
{
    var correlationId = (string)context.Items["CorrelationId"]!;
    await lbPool.EnsureInitialProbeAsync(httpClientFactory, ct);

    if (!lbPool.TryLeaseNode(out var node) || node is null)
    {
        context.Response.Headers["X-LB-Algorithm"] = lbPool.Algorithm;
        return Results.Problem(statusCode: StatusCodes.Status503ServiceUnavailable, title: "No healthy upstream OrderApi instances available.");
    }

    try
    {
        var client = httpClientFactory.CreateClient("OrderApiNode");
        using var request = new HttpRequestMessage(HttpMethod.Get, $"{node.Url}/catalog/restaurants/{restaurantId}/menu");
        request.Headers.TryAddWithoutValidation("X-Correlation-Id", correlationId);
        ForwardDemoCatalogFailureHeader(context, request, app.Environment);
        if (context.Request.Headers.TryGetValue("If-None-Match", out var ifNoneMatch))
        {
            foreach (var val in ifNoneMatch)
                request.Headers.TryAddWithoutValidation("If-None-Match", val);
        }

        context.Response.Headers["X-Upstream-Instance"] = node.InstanceId;
        context.Response.Headers["X-Upstream-Url"] = node.Url;
        context.Response.Headers["X-LB-Algorithm"] = lbPool.Algorithm;

        using var response = await client.SendAsync(request, ct);
        CopyCacheHeaders(response, context.Response.Headers);
        if (response.Headers.TryGetValues("X-Instance-Id", out var upstreamIds))
        {
            var actualId = string.Join(",", upstreamIds);
            context.Response.Headers["X-Instance-Id"] = actualId;
            context.Response.Headers["X-Upstream-Instance"] = actualId;
        }

        if (response.StatusCode == HttpStatusCode.NotModified)
            return Results.StatusCode(StatusCodes.Status304NotModified);

        var body = await response.Content.ReadAsStringAsync(ct);
        var contentType = response.Content.Headers.ContentType?.ToString() ?? "application/json";
        return Results.Content(body, contentType, statusCode: (int)response.StatusCode);
    }
    catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
    {
        app.Logger.LogError(ex, "gw_upstream_menu_failed instance={Instance} correlation_id={CorrelationId}", node.InstanceId, correlationId);
        return Results.Problem(statusCode: StatusCodes.Status502BadGateway, title: "Upstream Order API unavailable.");
    }
    finally
    {
        lbPool.ReleaseNode(node);
    }
}

app.MapGet("/api/catalog/restaurants/{restaurantId:long}/menu", RouteCatalogMenuThroughPoolAsync);
app.MapGet("/api/lb/catalog/restaurants/{restaurantId:long}/menu", RouteCatalogMenuThroughPoolAsync);

app.MapGet("/api/edge/catalog/restaurants/{restaurantId:long}/menu", async (
    long restaurantId,
    bool? revalidate,
    HttpContext context,
    IHttpClientFactory httpClientFactory,
    CancellationToken ct) =>
{
    var correlationId = (string)context.Items["CorrelationId"]!;
    var forceRevalidate = revalidate ?? false;
    var now = DateTimeOffset.UtcNow;

    if (!forceRevalidate &&
        edgeStore.TryGetValue(restaurantId, out var existing) &&
        now < existing.ExpiresAtUtc)
    {
        Interlocked.Increment(ref edgeStats.HitsValue);
        context.Response.Headers["X-Edge-Cache"] = "HIT";
        context.Response.Headers.ETag = existing.ETag;
        context.Response.Headers.CacheControl = existing.CacheControl;
        return Results.Content(existing.Payload, existing.ContentType, statusCode: StatusCodes.Status200OK);
    }

    var client = httpClientFactory.CreateClient("OrderApi");
    using var request = new HttpRequestMessage(HttpMethod.Get, $"/catalog/restaurants/{restaurantId}/menu");
    request.Headers.TryAddWithoutValidation("X-Correlation-Id", correlationId);
    ForwardDemoCatalogFailureHeader(context, request, app.Environment);

    var hadEntry = edgeStore.TryGetValue(restaurantId, out var staleEntry);
    if (hadEntry && staleEntry is not null)
        request.Headers.TryAddWithoutValidation("If-None-Match", staleEntry.ETag);

    try
    {
        using var response = await client.SendAsync(request, ct);
        if (response.StatusCode == HttpStatusCode.NotModified && staleEntry is not null)
        {
            Interlocked.Increment(ref edgeStats.RevalidatedValue);
            var renewed = staleEntry with { ExpiresAtUtc = now.AddSeconds(EdgeTtlSeconds) };
            edgeStore[restaurantId] = renewed;

            CopyCacheHeaders(response, context.Response.Headers);
            context.Response.Headers["X-Edge-Cache"] = "REVALIDATED";
            if (!context.Response.Headers.ContainsKey("ETag"))
                context.Response.Headers.ETag = renewed.ETag;
            if (!context.Response.Headers.ContainsKey("Cache-Control"))
                context.Response.Headers.CacheControl = renewed.CacheControl;

            return Results.Content(renewed.Payload, renewed.ContentType, statusCode: StatusCodes.Status200OK);
        }

        var body = await response.Content.ReadAsStringAsync(ct);
        var contentType = response.Content.Headers.ContentType?.ToString() ?? "application/json";
        CopyCacheHeaders(response, context.Response.Headers);

        if (!response.IsSuccessStatusCode)
        {
            context.Response.Headers["X-Edge-Cache"] = "BYPASS";
            return Results.Content(body, contentType, statusCode: (int)response.StatusCode);
        }

        var newETag = response.Headers.ETag?.ToString() ?? string.Empty;
        var cacheControl = response.Headers.CacheControl?.ToString() ?? $"public, max-age={EdgeTtlSeconds}";
        edgeStore[restaurantId] = new EdgeCacheEntry(body, contentType, newETag, cacheControl, now.AddSeconds(EdgeTtlSeconds));

        if (hadEntry)
        {
            Interlocked.Increment(ref edgeStats.RefreshedValue);
            context.Response.Headers["X-Edge-Cache"] = "REFRESHED";
        }
        else
        {
            Interlocked.Increment(ref edgeStats.MissesValue);
            context.Response.Headers["X-Edge-Cache"] = "MISS";
        }

        return Results.Content(body, contentType, statusCode: StatusCodes.Status200OK);
    }
    catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
    {
        app.Logger.LogError(ex, "gw_edge_upstream_failed correlation_id={CorrelationId}", correlationId);
        return Results.Problem(statusCode: StatusCodes.Status502BadGateway, title: "Upstream Order API unavailable.");
    }
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

app.MapPost("/api/observability/trace-order", async (HttpContext http, IHttpClientFactory clientFactory, CancellationToken ct) =>
{
    var correlationId = http.Request.Headers["X-Correlation-Id"].FirstOrDefault();
    if (string.IsNullOrWhiteSpace(correlationId) || correlationId.Length > 128)
        correlationId = Guid.NewGuid().ToString("N");
    var incomingTraceparent = http.Request.Headers["traceparent"].FirstOrDefault();
    var hasParent = !string.IsNullOrWhiteSpace(incomingTraceparent)
        && ActivityContext.TryParse(incomingTraceparent, null, out var parsedParent);

    TraceOrderGatewayRequest? payload = null;
    if (http.Request.ContentLength is > 0 || http.Request.ContentType?.Contains("json", StringComparison.OrdinalIgnoreCase) == true)
    {
        try
        {
            payload = await JsonSerializer.DeserializeAsync<TraceOrderGatewayRequest>(http.Request.Body, jsonOptions, ct);
        }
        catch (JsonException)
        {
            return Results.Problem(statusCode: 400, title: "Invalid JSON payload.");
        }
    }

    payload ??= new TraceOrderGatewayRequest();

    using var gatewayActivity = hasParent
        ? gatewayActivitySource.StartActivity("gateway.http.post_trace_order", ActivityKind.Server, parsedParent)
        : gatewayActivitySource.StartActivity("gateway.http.post_trace_order", ActivityKind.Server);

    var traceId = gatewayActivity?.TraceId.ToHexString() ?? ActivityTraceId.CreateRandom().ToHexString();
    var gatewaySpanId = gatewayActivity?.SpanId.ToHexString() ?? ActivitySpanId.CreateRandom().ToHexString();
    var traceparentOut = $"00-{traceId}-{gatewaySpanId}-01";
    var startedAtUtc = DateTime.UtcNow;
    var sw = Stopwatch.StartNew();

    var client = clientFactory.CreateClient("OrderApi");
    using var upstreamReq = new HttpRequestMessage(HttpMethod.Post, "/observability/orders/trace-checkout")
    {
        Content = JsonContent.Create(payload)
    };
    upstreamReq.Headers.Add("traceparent", traceparentOut);
    upstreamReq.Headers.Add("X-Correlation-Id", correlationId);

    using var upstreamRes = await client.SendAsync(upstreamReq, ct);
    var upstreamBodyText = await upstreamRes.Content.ReadAsStringAsync(ct);
    sw.Stop();
    gatewayActivity?.Stop();

    var gatewayStatus = upstreamRes.IsSuccessStatusCode ? "ok" : "error";
    var ingestPayload = new
    {
        traceId,
        spanId = gatewaySpanId,
        parentSpanId = hasParent ? parsedParent.SpanId.ToHexString() : "0000000000000000",
        service = "gateway-bff",
        operation = "gateway.http.post_trace_order",
        durationMs = Math.Round(sw.Elapsed.TotalMilliseconds, 3),
        status = gatewayStatus,
        startedAtUtc,
        attributes = new Dictionary<string, string>
        {
            ["http.route"] = "/api/observability/trace-order",
            ["http.status_code"] = ((int)upstreamRes.StatusCode).ToString()
        }
    };

    using var ingestRes = await client.PostAsJsonAsync("/observability/spans/ingest", ingestPayload, ct);

    http.Response.Headers["X-Correlation-Id"] = correlationId;
    http.Response.Headers["X-Trace-Id"] = traceId;
    http.Response.Headers["traceparent"] = traceparentOut;

    JsonElement? upstreamJson = null;
    if (!string.IsNullOrWhiteSpace(upstreamBodyText))
    {
        try
        {
            using var doc = JsonDocument.Parse(upstreamBodyText);
            upstreamJson = doc.RootElement.Clone();
        }
        catch (JsonException)
        {
            upstreamJson = null;
        }
    }

    return Results.Json(new
    {
        traceId,
        traceparent = traceparentOut,
        gatewaySpanId,
        orderApiStatusCode = (int)upstreamRes.StatusCode,
        durationMs = Math.Round(sw.Elapsed.TotalMilliseconds, 3),
        upstream = upstreamJson
    }, statusCode: (int)upstreamRes.StatusCode);
});

app.Run();

static void CopyCacheHeaders(HttpResponseMessage upstream, IHeaderDictionary downstreamHeaders)
{
    if (upstream.Headers.ETag is { } etag)
        downstreamHeaders.ETag = etag.ToString();
    if (upstream.Headers.CacheControl is { } cc)
        downstreamHeaders.CacheControl = cc.ToString();
    if (upstream.Headers.Vary.Count > 0)
        downstreamHeaders.Vary = string.Join(", ", upstream.Headers.Vary);

    foreach (var headerName in new[] { "X-Cache", "X-Cache-Key", "X-Cache-Ttl-Seconds" })
    {
        if (upstream.Headers.TryGetValues(headerName, out var values))
            downstreamHeaders[headerName] = string.Join(",", values);
    }
}

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

internal sealed record EdgeCacheEntry(
    string Payload,
    string ContentType,
    string ETag,
    string CacheControl,
    DateTimeOffset ExpiresAtUtc);

internal sealed class EdgeCacheCounters
{
    public long HitsValue;
    public long MissesValue;
    public long RevalidatedValue;
    public long RefreshedValue;

    public long Hits => Interlocked.Read(ref HitsValue);
    public long Misses => Interlocked.Read(ref MissesValue);
    public long Revalidated => Interlocked.Read(ref RevalidatedValue);
    public long Refreshed => Interlocked.Read(ref RefreshedValue);

    public void Reset()
    {
        Interlocked.Exchange(ref HitsValue, 0);
        Interlocked.Exchange(ref MissesValue, 0);
        Interlocked.Exchange(ref RevalidatedValue, 0);
        Interlocked.Exchange(ref RefreshedValue, 0);
    }
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

internal sealed record LbConfigRequest(string? Algorithm, bool? HealthCheckEnabled, bool? ResetStats);
internal sealed record TraceOrderGatewayRequest(long? CustomerId = null, long? RestaurantId = null, bool? SimulateFail = null, int? SlowDbMs = null);
internal sealed record HealthReadyPayload(string? Status, string? InstanceId, string? State);

internal sealed class PoolNodeState(string url, string defaultInstanceId)
{
    public string Url { get; } = url.TrimEnd('/');
    public string InstanceId { get; set; } = defaultInstanceId;
    public bool Healthy { get; set; } = true;
    public int ActiveRequests { get; set; }
    public long TotalRouted { get; set; }
    public int LastStatusCode { get; set; } = 200;
}

internal sealed class LoadBalancerPool
{
    private readonly object _gate = new();
    private readonly SemaphoreSlim _probeLock = new(1, 1);
    private readonly List<PoolNodeState> _nodes;
    private readonly ILogger _logger;
    private string _algorithm = "round-robin";
    private bool _healthCheckEnabled = true;
    private bool _hasProbed;
    private long _rrCounter;

    public LoadBalancerPool(IEnumerable<string> nodeUrls, ILogger logger)
    {
        _logger = logger;
        var list = nodeUrls
            .Where(u => !string.IsNullOrWhiteSpace(u))
            .Select(u => u.Trim().TrimEnd('/'))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        _nodes = list.Select((url, idx) =>
        {
            var fallbackId = idx switch
            {
                0 => "node-a",
                1 => "node-b",
                _ => $"node-{idx}"
            };
            return new PoolNodeState(url, fallbackId);
        }).ToList();
    }

    public string Algorithm
    {
        get { lock (_gate) return _algorithm; }
    }

    public object Configure(string? algorithm, bool? healthCheckEnabled, bool resetStats)
    {
        lock (_gate)
        {
            if (!string.IsNullOrWhiteSpace(algorithm))
            {
                var normalized = algorithm.Trim().ToLowerInvariant();
                if (normalized is not ("round-robin" or "least-requests"))
                    throw new ArgumentException("algorithm must be 'round-robin' or 'least-requests'.");
                _algorithm = normalized;
            }

            if (healthCheckEnabled is { } hc)
                _healthCheckEnabled = hc;

            if (resetStats)
            {
                _rrCounter = 0;
                foreach (var n in _nodes)
                    n.TotalRouted = 0;
            }

            return GetStatusUnsafe();
        }
    }

    public async Task EnsureInitialProbeAsync(IHttpClientFactory httpClientFactory, CancellationToken ct)
    {
        lock (_gate)
        {
            if (_hasProbed) return;
        }

        await _probeLock.WaitAsync(ct);
        try
        {
            lock (_gate)
            {
                if (_hasProbed) return;
            }

            await ProbeAllAsync(httpClientFactory, ct);
        }
        finally
        {
            _probeLock.Release();
        }
    }

    public async Task ProbeAllAsync(IHttpClientFactory httpClientFactory, CancellationToken ct)
    {
        var client = httpClientFactory.CreateClient("OrderApiNode");
        List<PoolNodeState> snapshot;
        lock (_gate) snapshot = _nodes.ToList();

        foreach (var node in snapshot)
        {
            var healthy = false;
            var statusCode = 0;
            string? probedId = null;

            try
            {
                using var probeCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                probeCts.CancelAfter(TimeSpan.FromMilliseconds(1000));
                using var resp = await client.GetAsync($"{node.Url}/health/ready", probeCts.Token);
                statusCode = (int)resp.StatusCode;
                healthy = resp.StatusCode == HttpStatusCode.OK;
                if (resp.Content.Headers.ContentType?.MediaType?.Contains("json", StringComparison.OrdinalIgnoreCase) == true)
                {
                    var payload = await resp.Content.ReadFromJsonAsync<HealthReadyPayload>(probeCts.Token);
                    if (!string.IsNullOrWhiteSpace(payload?.InstanceId))
                        probedId = payload.InstanceId;
                }
            }
            catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
            {
                healthy = false;
            }

            lock (_gate)
            {
                node.Healthy = healthy;
                node.LastStatusCode = statusCode;
                if (!string.IsNullOrWhiteSpace(probedId))
                    node.InstanceId = probedId;
            }
        }

        lock (_gate) _hasProbed = true;
        _logger.LogInformation("lb_probe_completed healthy_count={HealthyCount}", snapshot.Count(n => n.Healthy));
    }

    public bool TryLeaseNode(out PoolNodeState? leased)
    {
        lock (_gate)
        {
            var candidates = _healthCheckEnabled
                ? _nodes.Where(n => n.Healthy).ToList()
                : _nodes.ToList();

            if (candidates.Count == 0)
            {
                leased = null;
                return false;
            }

            if (_algorithm == "least-requests")
            {
                var startIdx = (int)(_rrCounter++ % candidates.Count);
                var best = candidates[startIdx];
                for (var offset = 1; offset < candidates.Count; offset++)
                {
                    var candidate = candidates[(startIdx + offset) % candidates.Count];
                    if (candidate.ActiveRequests < best.ActiveRequests)
                        best = candidate;
                }
                leased = best;
            }
            else
            {
                var idx = (int)(_rrCounter++ % candidates.Count);
                leased = candidates[idx];
            }

            leased.ActiveRequests++;
            leased.TotalRouted++;
            return true;
        }
    }

    public void ReleaseNode(PoolNodeState node)
    {
        lock (_gate)
        {
            if (node.ActiveRequests > 0)
                node.ActiveRequests--;
        }
    }

    public object GetStatus()
    {
        lock (_gate) return GetStatusUnsafe();
    }

    private object GetStatusUnsafe() => new
    {
        algorithm = _algorithm,
        healthCheckEnabled = _healthCheckEnabled,
        hasProbed = _hasProbed,
        nodes = _nodes.Select(n => new
        {
            url = n.Url,
            instanceId = n.InstanceId,
            healthy = n.Healthy,
            activeRequests = n.ActiveRequests,
            totalRouted = n.TotalRouted,
            lastStatusCode = n.LastStatusCode
        }).ToArray()
    };
}
