using Npgsql;
using Tadka.OrderApi.Data;

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
builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(connectionString));
builder.Services.AddSingleton<IOrderService, OrderStore>();

builder.Services.AddControllers();
builder.Services.AddProblemDetails();
builder.Services.AddExceptionHandler<Tadka.OrderApi.Middleware.DatabaseExceptionHandler>();

var app = builder.Build();

var csb = new NpgsqlConnectionStringBuilder(connectionString);
app.Logger.LogWarning(
    "contract_config max_pool_size={MaxPoolSize} pool_timeout_s={Timeout} artificial_delay=false environment={Env}",
    csb.MaxPoolSize, csb.Timeout, app.Environment.EnvironmentName);

app.UseExceptionHandler();
app.MapControllers();
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.Run();
