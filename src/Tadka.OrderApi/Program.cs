using Npgsql;
using Tadka.OrderApi.Data;
using Tadka.OrderApi.Modules.Catalog;
using Tadka.OrderApi.Modules.DistributedIds;
using Tadka.OrderApi.Modules.Indexing;
using Tadka.OrderApi.Modules.Orders;
using Tadka.OrderApi.Modules.Replication;

var builder = WebApplication.CreateBuilder(args);

// One-line console logs, so each request is one readable line in the log pane.
builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(o =>
{
    o.SingleLine = true;
    o.TimestampFormat = "HH:mm:ss.fff ";
});

// The pool lives inside this data source. Pool size and pool timeout come from the
// connection string in appsettings.json, set explicitly. Nothing here relies on defaults.
var connectionString = builder.Configuration.GetConnectionString("Tadka")
    ?? throw new InvalidOperationException("ConnectionStrings:Tadka is missing.");
var replicaConnectionString = builder.Configuration.GetConnectionString("TadkaReplica")
    ?? "Host=localhost;Port=5434;Database=tadka;Username=tadka;Password=tadka_local;Maximum Pool Size=10;Minimum Pool Size=0;Timeout=15";

builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(connectionString));
builder.Services.AddSingleton(_ => new ReplicaDataSource(NpgsqlDataSource.Create(replicaConnectionString)));
builder.Services.AddSingleton<ReplicaLagLabService>();
builder.Services.AddSingleton<IOrderService, OrderStore>();
builder.Services.AddSingleton<IOrderStatusService, OrderStatusService>();
builder.Services.AddSingleton<ICatalogModule, CatalogModule>();
builder.Services.AddSingleton<FlashDealService>();
builder.Services.AddSingleton<IOrderCheckoutService, OrderCheckoutService>();
builder.Services.AddSingleton<IndexingLabService>();
builder.Services.AddSingleton(_ => new SnowflakeIdGenerator(nodeId: 7));
builder.Services.AddSingleton<DistributedIdLabService>();

builder.Services.AddControllers();
builder.Services.AddProblemDetails();
builder.Services.AddExceptionHandler<Tadka.OrderApi.Middleware.DatabaseExceptionHandler>();

var app = builder.Build();

var csb = new NpgsqlConnectionStringBuilder(connectionString);
app.Logger.LogWarning(
    "contract_config max_pool_size={MaxPoolSize} pool_timeout_s={Timeout} artificial_delay=false environment={Env}",
    csb.MaxPoolSize, csb.Timeout, app.Environment.EnvironmentName);

app.Use(async (context, next) =>
{
    var correlationId = context.Request.Headers["X-Correlation-Id"].FirstOrDefault();
    if (string.IsNullOrWhiteSpace(correlationId) || correlationId.Length > 128)
        correlationId = Guid.NewGuid().ToString("N");

    context.Response.OnStarting(() =>
    {
        context.Response.Headers["X-Correlation-Id"] = correlationId;
        return Task.CompletedTask;
    });

    using (app.Logger.BeginScope(new Dictionary<string, object> { ["CorrelationId"] = correlationId }))
    {
        await next();
    }
});

app.UseExceptionHandler();
app.MapControllers();
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.Run();
