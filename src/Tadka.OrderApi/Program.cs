using Npgsql;
using StackExchange.Redis;
using Tadka.OrderApi.Data;
using Tadka.OrderApi.Modules.Catalog;
using Tadka.OrderApi.Modules.DistributedIds;
using Tadka.OrderApi.Modules.Indexing;
using Tadka.OrderApi.Modules.Instance;
using Tadka.OrderApi.Modules.Messaging;
using Tadka.OrderApi.Modules.Orders;
using Tadka.OrderApi.Modules.OutboxSaga;
using Tadka.OrderApi.Modules.Replication;
using Tadka.OrderApi.Modules.Sharding;

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
var redisConnectionString = builder.Configuration.GetConnectionString("Redis")
    ?? "127.0.0.1:6379,abortConnect=false,connectTimeout=2000,syncTimeout=2000";

builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(connectionString));
builder.Services.AddSingleton(_ => new ReplicaDataSource(NpgsqlDataSource.Create(replicaConnectionString)));
builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConnectionString));
builder.Services.AddSingleton<ReplicaLagLabService>();
builder.Services.AddSingleton<IOrderService, OrderStore>();
builder.Services.AddSingleton<IOrderStatusService, OrderStatusService>();
builder.Services.AddSingleton<ICatalogModule, CatalogModule>();
builder.Services.AddSingleton<MenuCacheService>();
builder.Services.AddSingleton<FlashDealService>();
builder.Services.AddSingleton<IOrderCheckoutService, OrderCheckoutService>();
builder.Services.AddSingleton<IndexingLabService>();
builder.Services.AddSingleton(_ => new SnowflakeIdGenerator(nodeId: 7));
builder.Services.AddSingleton<DistributedIdLabService>();
builder.Services.AddSingleton<ShardingLabService>();
builder.Services.AddSingleton<InstanceLifecycleService>();
builder.Services.AddSingleton<KafkaEventsLabService>();
builder.Services.AddSingleton<ReliableConsumerLabService>();
builder.Services.AddSingleton<OutboxSagaLabService>();

builder.Services.AddControllers();
builder.Services.AddProblemDetails();
builder.Services.AddExceptionHandler<Tadka.OrderApi.Middleware.DatabaseExceptionHandler>();

var app = builder.Build();

var csb = new NpgsqlConnectionStringBuilder(connectionString);
var lifecycle = app.Services.GetRequiredService<InstanceLifecycleService>();
app.Logger.LogWarning(
    "contract_config instance_id={InstanceId} max_pool_size={MaxPoolSize} pool_timeout_s={Timeout} artificial_delay=false environment={Env}",
    lifecycle.InstanceId, csb.MaxPoolSize, csb.Timeout, app.Environment.EnvironmentName);

app.Use(async (context, next) =>
{
    var correlationId = context.Request.Headers["X-Correlation-Id"].FirstOrDefault();
    if (string.IsNullOrWhiteSpace(correlationId) || correlationId.Length > 128)
        correlationId = Guid.NewGuid().ToString("N");

    context.Response.OnStarting(() =>
    {
        context.Response.Headers["X-Correlation-Id"] = correlationId;
        context.Response.Headers["X-Instance-Id"] = lifecycle.InstanceId;
        return Task.CompletedTask;
    });

    var path = context.Request.Path.Value ?? string.Empty;
    var trackRequest =
        !path.StartsWith("/health", StringComparison.OrdinalIgnoreCase) &&
        !path.StartsWith("/diagnostics/instance/state", StringComparison.OrdinalIgnoreCase);

    if (trackRequest)
        lifecycle.BeginRequest(countServed: true);

    try
    {
        using (app.Logger.BeginScope(new Dictionary<string, object> { ["CorrelationId"] = correlationId }))
        {
            await next();
        }
    }
    finally
    {
        if (trackRequest)
            lifecycle.EndRequest();
    }
});

app.UseExceptionHandler();
app.MapControllers();

app.Run();
