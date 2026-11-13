using Microsoft.Extensions.Options;
using Npgsql;
using Tadka.OrderApi.Configuration;
using Tadka.OrderApi.Data;

var builder = WebApplication.CreateBuilder(args);

// One-line console logs, so each request is one readable line in the log pane.
builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(o =>
{
    o.SingleLine = true;
    o.TimestampFormat = "HH:mm:ss.fff ";
});

builder.Services.Configure<DemoOptions>(builder.Configuration.GetSection(DemoOptions.Section));

// The pool lives inside this data source. Pool size and pool timeout come from the
// connection string in appsettings.json, set explicitly. Nothing here relies on defaults.
var connectionString = builder.Configuration.GetConnectionString("Tadka")
    ?? throw new InvalidOperationException("ConnectionStrings:Tadka is missing.");
builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(connectionString));
builder.Services.AddSingleton<OrderStore>();

builder.Services.AddControllers();

var app = builder.Build();

// Print the settings that matter for the experiment, so they are visible on camera.
var csb = new NpgsqlConnectionStringBuilder(connectionString);
var holdMs = app.Services.GetRequiredService<IOptions<DemoOptions>>().Value.HoldMs;
app.Logger.LogWarning(
    "demo_config max_pool_size={MaxPoolSize} pool_timeout_s={Timeout} hold_ms={HoldMs} environment={Env}",
    csb.MaxPoolSize, csb.Timeout, holdMs, app.Environment.EnvironmentName);

app.MapControllers();
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.Run();
