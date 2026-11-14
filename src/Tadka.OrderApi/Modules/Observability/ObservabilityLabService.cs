using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text.RegularExpressions;
using Npgsql;
using StackExchange.Redis;

namespace Tadka.OrderApi.Modules.Observability;

public sealed partial class ObservabilityLabService : IDisposable
{
    public const string ActivitySourceName = "Tadka.OrderApi";

    private static readonly string[] AllowedLabelKeys = ["service", "route", "method", "status_class"];
    private static readonly string[] ForbiddenHighCardinalityKeys = ["order_id", "user_id", "trace_id", "span_id", "customer_id"];

    private readonly NpgsqlDataSource _dataSource;
    private readonly IConnectionMultiplexer _redis;
    private readonly ILogger<ObservabilityLabService> _logger;
    private readonly int _maxPoolSize;
    private readonly ActivitySource _activitySource = new(ActivitySourceName, "1.0.0");
    private readonly ActivityListener _activityListener;

    private readonly object _telemetryGate = new();
    private readonly Dictionary<string, List<TraceSpanRecord>> _traces = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, List<StructuredLogEntry>> _traceLogs = new(StringComparer.OrdinalIgnoreCase);
    private readonly Queue<string> _traceInsertionOrder = new();
    private readonly Dictionary<string, RedSeriesCounter> _redSeries = new(StringComparer.Ordinal);
    private readonly List<double> _latenciesMs = [];

    private long _totalRequests;
    private long _totalErrors;
    private int _activeDbConnections;
    private int _peakDbConnections;
    private long _dbWaitCount;
    private double _maxDbWaitMs;
    private long _dbPoolErrorsTotal;
    private long _rejectedHighCardinalityTags;

    public ObservabilityLabService(
        NpgsqlDataSource dataSource,
        IConnectionMultiplexer redis,
        IConfiguration configuration,
        ILogger<ObservabilityLabService> logger)
    {
        _dataSource = dataSource;
        _redis = redis;
        _logger = logger;

        var cs = configuration.GetConnectionString("Tadka") ?? string.Empty;
        var csb = new NpgsqlConnectionStringBuilder(cs);
        _maxPoolSize = csb.MaxPoolSize > 0 ? csb.MaxPoolSize : 100;

        _activityListener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                var traceId = activity.TraceId.ToHexString();
                var spanId = activity.SpanId.ToHexString();
                var parentSpanId = activity.ParentSpanId == default
                    ? "0000000000000000"
                    : activity.ParentSpanId.ToHexString();

                var status = activity.Status == ActivityStatusCode.Error ? "error" : "ok";
                var attributes = new Dictionary<string, string>(StringComparer.Ordinal);
                foreach (var tag in activity.TagObjects)
                {
                    if (tag.Value is not null)
                        attributes[tag.Key] = tag.Value.ToString() ?? string.Empty;
                }

                RecordSpan(new TraceSpanRecord(
                    TraceId: traceId,
                    SpanId: spanId,
                    ParentSpanId: parentSpanId,
                    Service: "order-api",
                    Operation: activity.OperationName,
                    StartedAtUtc: activity.StartTimeUtc,
                    DurationMs: Math.Round(activity.Duration.TotalMilliseconds, 3),
                    Status: status,
                    Attributes: attributes));
            }
        };

        ActivitySource.AddActivityListener(_activityListener);
    }

    public async Task<TraceCheckoutResult> TraceCheckoutAsync(
        TraceCheckoutRequest request,
        string? traceparentHeader,
        CancellationToken ct)
    {
        var customerId = request.CustomerId is > 0 ? request.CustomerId.Value : 1042L;
        var restaurantId = request.RestaurantId is > 0 ? request.RestaurantId.Value : 7L;
        var simulateFail = request.SimulateFail ?? false;
        var slowDbMs = request.SlowDbMs ?? 0;

        if (slowDbMs is < 0 or > 2000)
            throw new ArgumentOutOfRangeException(nameof(request.SlowDbMs), "slowDbMs must be between 0 and 2000.");

        ActivityContext parentContext = default;
        var hasValidParent = !string.IsNullOrWhiteSpace(traceparentHeader)
            && ActivityContext.TryParse(traceparentHeader, null, out parentContext);

        var routeSw = Stopwatch.StartNew();
        long? createdOrderId = null;
        var statusCode = 200;
        var statusClass = "2xx";

        using var processActivity = hasValidParent
            ? _activitySource.StartActivity("orderapi.checkout.process", ActivityKind.Server, parentContext)
            : _activitySource.StartActivity("orderapi.checkout.process", ActivityKind.Server);

        var traceId = processActivity?.TraceId.ToHexString() ?? ActivityTraceId.CreateRandom().ToHexString();
        var processSpanId = processActivity?.SpanId.ToHexString() ?? ActivitySpanId.CreateRandom().ToHexString();

        processActivity?.SetTag("http.route", "/observability/orders/trace-checkout");
        processActivity?.SetTag("customer.id", customerId.ToString(CultureInfo.InvariantCulture));
        processActivity?.SetTag("restaurant.id", restaurantId.ToString(CultureInfo.InvariantCulture));

        try
        {
            // Step 1: Redis menu lookup child span
            string? cachedMenuValue;
            using (var redisActivity = _activitySource.StartActivity("redis.cache.menu_lookup", ActivityKind.Client))
            {
                redisActivity?.SetTag("db.system", "redis");
                redisActivity?.SetTag("db.operation", "GET");

                var db = _redis.GetDatabase();
                var cacheKey = $"tadka:obs:menu:{restaurantId}";
                cachedMenuValue = await db.StringGetAsync(cacheKey);
                if (cachedMenuValue is null)
                {
                    cachedMenuValue = $"{{\"restaurantId\":{restaurantId},\"item\":\"Paneer Tikka\",\"price\":240.00}}";
                    await db.StringSetAsync(cacheKey, cachedMenuValue, TimeSpan.FromMinutes(5));
                }
                redisActivity?.SetTag("cache.hit", "true");
            }

            // Step 2: PostgreSQL insert_order child span (measures Npgsql pool open + SQL execution)
            using (var dbActivity = _activitySource.StartActivity("postgres.db.insert_order", ActivityKind.Client))
            {
                dbActivity?.SetTag("db.system", "postgresql");
                dbActivity?.SetTag("db.statement", "INSERT INTO orders");

                var openSw = Stopwatch.StartNew();
                NpgsqlConnection? conn = null;
                try
                {
                    conn = await _dataSource.OpenConnectionAsync(ct);
                }
                catch
                {
                    Interlocked.Increment(ref _dbPoolErrorsTotal);
                    dbActivity?.SetStatus(ActivityStatusCode.Error, "db_pool_open_failed");
                    throw;
                }
                finally
                {
                    openSw.Stop();
                }

                var active = Interlocked.Increment(ref _activeDbConnections);
                RecordPoolAcquireSample(openSw.Elapsed.TotalMilliseconds, active);

                try
                {
                    if (simulateFail)
                    {
                        dbActivity?.SetStatus(ActivityStatusCode.Error, "simulated_downstream_failure");
                        throw new InvalidOperationException("Simulated PostgreSQL checkout failure.");
                    }

                    await using (conn)
                    {
                        if (slowDbMs > 0)
                        {
                            await using var sleepCmd = conn.CreateCommand();
                            sleepCmd.CommandText = "SELECT pg_sleep(@delaySec);";
                            sleepCmd.Parameters.AddWithValue("delaySec", slowDbMs / 1000.0);
                            await sleepCmd.ExecuteNonQueryAsync(ct);
                        }

                        await using var insertCmd = conn.CreateCommand();
                        insertCmd.CommandText = """
                            INSERT INTO orders (customer_id, restaurant_id, total_amount, idempotency_key, status)
                            VALUES (@cid, @rid, @amount, @ikey, 'placed')
                            RETURNING id;
                            """;
                        insertCmd.Parameters.AddWithValue("cid", customerId);
                        insertCmd.Parameters.AddWithValue("rid", restaurantId);
                        insertCmd.Parameters.AddWithValue("amount", 240.00m);
                        insertCmd.Parameters.AddWithValue("ikey", $"obs:{traceId}:{Guid.NewGuid():N}");

                        var result = await insertCmd.ExecuteScalarAsync(ct);
                        createdOrderId = Convert.ToInt64(result, CultureInfo.InvariantCulture);
                        dbActivity?.SetTag("order.id", createdOrderId.Value.ToString(CultureInfo.InvariantCulture));
                    }
                }
                finally
                {
                    Interlocked.Decrement(ref _activeDbConnections);
                }
            }

            AppendStructuredLog(new StructuredLogEntry(
                TimestampUtc: DateTime.UtcNow,
                Level: "INFO",
                Service: "order-api",
                TraceId: traceId,
                SpanId: processSpanId,
                OrderId: createdOrderId?.ToString(CultureInfo.InvariantCulture),
                Message: $"Checkout trace completed for restaurant {restaurantId}",
                PiiMasked: false));

            _logger.LogInformation(
                "obs_trace_checkout_ok trace_id={TraceId} span_id={SpanId} order_id={OrderId} duration_ms={DurationMs}",
                traceId, processSpanId, createdOrderId, Math.Round(routeSw.Elapsed.TotalMilliseconds, 2));

            return new TraceCheckoutResult(
                StatusCode: 200,
                TraceId: traceId,
                SpanId: processSpanId,
                ParentSpanId: hasValidParent ? parentContext.SpanId.ToHexString() : "0000000000000000",
                OrderId: createdOrderId,
                Status: "ok",
                DurationMs: Math.Round(routeSw.Elapsed.TotalMilliseconds, 3),
                Error: null);
        }
        catch (Exception ex) when (ex is InvalidOperationException or NpgsqlException)
        {
            statusCode = 500;
            statusClass = "5xx";
            processActivity?.SetStatus(ActivityStatusCode.Error, ex.Message);

            AppendStructuredLog(new StructuredLogEntry(
                TimestampUtc: DateTime.UtcNow,
                Level: "ERROR",
                Service: "order-api",
                TraceId: traceId,
                SpanId: processSpanId,
                OrderId: createdOrderId?.ToString(CultureInfo.InvariantCulture),
                Message: $"Checkout trace failed: {ex.Message}",
                PiiMasked: false));

            _logger.LogError(
                "obs_trace_checkout_error trace_id={TraceId} span_id={SpanId} message={Message}",
                traceId, processSpanId, ex.Message);

            return new TraceCheckoutResult(
                StatusCode: 500,
                TraceId: traceId,
                SpanId: processSpanId,
                ParentSpanId: hasValidParent ? parentContext.SpanId.ToHexString() : "0000000000000000",
                OrderId: null,
                Status: "error",
                DurationMs: Math.Round(routeSw.Elapsed.TotalMilliseconds, 3),
                Error: ex.Message);
        }
        finally
        {
            routeSw.Stop();
            var durationMs = Math.Round(routeSw.Elapsed.TotalMilliseconds, 3);
            processActivity?.SetTag("http.status_code", statusCode.ToString(CultureInfo.InvariantCulture));
            RecordRedMetricSample(
                service: "order-api",
                route: "/observability/orders/trace-checkout",
                method: "POST",
                statusClass: statusClass,
                isError: statusCode >= 500,
                durationMs: durationMs,
                attemptedHighCardinalityTags: ["order_id", "trace_id"]);
        }
    }

    public TraceSpanRecord IngestSpan(IngestSpanRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.TraceId) || request.TraceId.Length != 32)
            throw new ArgumentException("traceId must be a 32-character W3C hex string.");
        if (string.IsNullOrWhiteSpace(request.SpanId) || request.SpanId.Length != 16)
            throw new ArgumentException("spanId must be a 16-character W3C hex string.");
        if (string.IsNullOrWhiteSpace(request.Operation))
            throw new ArgumentException("operation is required.");

        var parent = string.IsNullOrWhiteSpace(request.ParentSpanId) ? "0000000000000000" : request.ParentSpanId;
        var service = string.IsNullOrWhiteSpace(request.Service) ? "gateway-bff" : request.Service;
        var status = string.Equals(request.Status, "error", StringComparison.OrdinalIgnoreCase) ? "error" : "ok";

        var record = new TraceSpanRecord(
            TraceId: request.TraceId,
            SpanId: request.SpanId,
            ParentSpanId: parent,
            Service: service,
            Operation: request.Operation,
            StartedAtUtc: request.StartedAtUtc ?? DateTime.UtcNow,
            DurationMs: Math.Round(Math.Max(0, request.DurationMs), 3),
            Status: status,
            Attributes: request.Attributes ?? new Dictionary<string, string>(StringComparer.Ordinal));

        RecordSpan(record);
        return record;
    }

    public TraceViewResponse? GetTrace(string traceId)
    {
        if (string.IsNullOrWhiteSpace(traceId))
            return null;

        lock (_telemetryGate)
        {
            if (!_traces.TryGetValue(traceId, out var spans) || spans.Count == 0)
                return null;

            var orderedSpans = spans
                .OrderBy(s => s.StartedAtUtc)
                .ThenBy(s => s.ParentSpanId == "0000000000000000" ? 0 : 1)
                .ToArray();

            var logs = _traceLogs.TryGetValue(traceId, out var traceLogList)
                ? traceLogList.ToArray()
                : [];

            var rootSpan = orderedSpans.FirstOrDefault(s => s.ParentSpanId == "0000000000000000")
                ?? orderedSpans[0];

            return new TraceViewResponse(
                TraceId: traceId,
                SpanCount: orderedSpans.Length,
                RootOperation: rootSpan.Operation,
                TotalDurationMs: Math.Round(orderedSpans.Max(s => s.DurationMs), 3),
                Spans: orderedSpans,
                StructuredLogs: logs);
        }
    }

    public ObservabilityMetricsSnapshot GetMetricsSnapshot()
    {
        lock (_telemetryGate)
        {
            var sorted = _latenciesMs.OrderBy(x => x).ToArray();
            var p50 = Percentile(sorted, 0.50);
            var p95 = Percentile(sorted, 0.95);
            var p99 = Percentile(sorted, 0.99);
            var avg = sorted.Length > 0 ? Math.Round(sorted.Average(), 3) : 0;

            var active = Volatile.Read(ref _activeDbConnections);
            var utilRatio = _maxPoolSize > 0 ? Math.Round((double)active / _maxPoolSize, 3) : 0;
            var errorRatePct = _totalRequests > 0
                ? Math.Round((double)_totalErrors * 100.0 / _totalRequests, 2)
                : 0;

            var series = _redSeries.Values
                .Select(s => new RedSeriesView(
                    SeriesKey: s.SeriesKey,
                    Service: s.Service,
                    Route: s.Route,
                    Method: s.Method,
                    StatusClass: s.StatusClass,
                    RequestsTotal: s.RequestsTotal,
                    ErrorsTotal: s.ErrorsTotal))
                .ToArray();

            return new ObservabilityMetricsSnapshot(
                Red: new RedMetricsSection(
                    RequestsTotal: _totalRequests,
                    ErrorsTotal: _totalErrors,
                    ErrorRatePercent: errorRatePct,
                    DurationMs: new LatencyPercentiles(p50, p95, p99, avg),
                    SeriesCount: series.Length,
                    Series: series),
                Use: new UseMetricsSection(
                    DbPool: new DbPoolUseMetrics(
                        MaxPoolSize: _maxPoolSize,
                        ActiveConnections: active,
                        PeakActiveConnections: _peakDbConnections,
                        UtilizationRatio: utilRatio,
                        SaturationWaitCount: _dbWaitCount,
                        MaxWaitMs: Math.Round(_maxDbWaitMs, 3),
                        PoolErrorsTotal: _dbPoolErrorsTotal)),
                CardinalityGuard: new CardinalityGuardInfo(
                    AllowedLabelKeys: AllowedLabelKeys,
                    ForbiddenHighCardinalityKeys: ForbiddenHighCardinalityKeys,
                    RejectedHighCardinalityTagCount: _rejectedHighCardinalityTags,
                    UniqueSeriesCount: series.Length));
        }
    }

    public LogStormDemoResponse ProcessLogStorm(LogStormDemoRequest request)
    {
        var dropDebug = request.DropDebug ?? true;
        var sampleInfoPercent = request.SampleInfoPercent ?? 100;
        if (sampleInfoPercent is < 0 or > 100)
            throw new ArgumentOutOfRangeException(nameof(request.SampleInfoPercent), "sampleInfoPercent must be between 0 and 100.");

        var events = request.Events is { Count: > 0 }
            ? request.Events
            :
            [
                new RawLogInput("DEBUG", "trace-storm-1", "ORD-9001", "Polling cache key payload=ok"),
                new RawLogInput("INFO", "trace-storm-1", "ORD-9002", "Payment card 4111-1111-1111-1111 authorized for user"),
                new RawLogInput("WARN", "trace-storm-1", "ORD-9003", "Customer KYC 234567890123 nearing retry budget"),
                new RawLogInput("ERROR", "trace-storm-1", "ORD-9004", "DB timeout for order ORD-9004 card 5500000000000004")
            ];

        var droppedByLevel = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
        {
            ["DEBUG"] = 0,
            ["INFO"] = 0,
            ["WARN"] = 0,
            ["ERROR"] = 0
        };

        var emitted = new List<StructuredLogEntry>();
        var totalMaskedFields = 0;
        var infoCounter = 0;

        foreach (var raw in events)
        {
            var level = (raw.Level ?? "INFO").ToUpperInvariant();
            if (level == "DEBUG" && dropDebug)
            {
                droppedByLevel["DEBUG"] = droppedByLevel.GetValueOrDefault("DEBUG") + 1;
                continue;
            }

            if (level == "INFO" && sampleInfoPercent < 100)
            {
                infoCounter++;
                var bucket = (infoCounter * 37) % 100;
                if (sampleInfoPercent == 0 || bucket >= sampleInfoPercent)
                {
                    droppedByLevel["INFO"] = droppedByLevel.GetValueOrDefault("INFO") + 1;
                    continue;
                }
            }

            var (maskedMessage, masksApplied) = MaskPii(raw.Message ?? string.Empty);
            totalMaskedFields += masksApplied;

            var entry = new StructuredLogEntry(
                TimestampUtc: DateTime.UtcNow,
                Level: level,
                Service: "order-api",
                TraceId: string.IsNullOrWhiteSpace(raw.TraceId) ? "00000000000000000000000000000000" : raw.TraceId,
                SpanId: ActivitySpanId.CreateRandom().ToHexString(),
                OrderId: raw.OrderId,
                Message: maskedMessage,
                PiiMasked: masksApplied > 0);

            emitted.Add(entry);
            AppendStructuredLog(entry);
        }

        return new LogStormDemoResponse(
            InputCount: events.Count,
            EmittedCount: emitted.Count,
            DroppedByLevel: droppedByLevel,
            MaskedFieldCount: totalMaskedFields,
            DropDebug: dropDebug,
            SampleInfoPercent: sampleInfoPercent,
            EmittedLogs: emitted.ToArray());
    }

    public object Reset()
    {
        lock (_telemetryGate)
        {
            _traces.Clear();
            _traceLogs.Clear();
            _traceInsertionOrder.Clear();
            _redSeries.Clear();
            _latenciesMs.Clear();
            _totalRequests = 0;
            _totalErrors = 0;
            _peakDbConnections = Volatile.Read(ref _activeDbConnections);
            _dbWaitCount = 0;
            _maxDbWaitMs = 0;
            _dbPoolErrorsTotal = 0;
            _rejectedHighCardinalityTags = 0;
        }

        return new { reset = true };
    }

    public static (string Masked, int Count) MaskPii(string input)
    {
        if (string.IsNullOrEmpty(input))
            return (input, 0);

        var count = 0;

        // First mask Luhn-valid 13 to 19 digit payment cards (optionally separated by '-' or space)
        var cardMasked = CardCandidateRegex().Replace(input, match =>
        {
            var digits = DigitsOnlyRegex().Replace(match.Value, string.Empty);
            if (digits.Length is < 13 or > 19 || !PassesLuhn(digits))
                return match.Value;

            count++;
            var last4 = digits[^4..];
            return new string('*', digits.Length - 4) + last4;
        });

        // Next mask 12-digit Aadhaar-like numbers starting with 2..9
        var piiMasked = AadhaarCandidateRegex().Replace(cardMasked, match =>
        {
            var digits = DigitsOnlyRegex().Replace(match.Value, string.Empty);
            if (digits.Length != 12)
                return match.Value;

            count++;
            var last4 = digits[^4..];
            return "********" + last4;
        });

        return (piiMasked, count);
    }

    private static bool PassesLuhn(string digits)
    {
        var sum = 0;
        var doubleDigit = false;
        for (var i = digits.Length - 1; i >= 0; i--)
        {
            var c = digits[i];
            if (c is < '0' or > '9')
                return false;

            var d = c - '0';
            if (doubleDigit)
            {
                d *= 2;
                if (d > 9)
                    d -= 9;
            }
            sum += d;
            doubleDigit = !doubleDigit;
        }

        return sum % 10 == 0;
    }

    private void RecordSpan(TraceSpanRecord span)
    {
        lock (_telemetryGate)
        {
            if (!_traces.TryGetValue(span.TraceId, out var list))
            {
                list = [];
                _traces[span.TraceId] = list;
                _traceInsertionOrder.Enqueue(span.TraceId);

                while (_traceInsertionOrder.Count > 250)
                {
                    var oldest = _traceInsertionOrder.Dequeue();
                    _traces.Remove(oldest);
                    _traceLogs.Remove(oldest);
                }
            }

            list.Add(span);
        }
    }

    private void AppendStructuredLog(StructuredLogEntry entry)
    {
        lock (_telemetryGate)
        {
            if (!_traceLogs.TryGetValue(entry.TraceId, out var list))
            {
                list = [];
                _traceLogs[entry.TraceId] = list;
            }
            if (list.Count < 100)
                list.Add(entry);
        }
    }

    private void RecordPoolAcquireSample(double waitMs, int currentActive)
    {
        lock (_telemetryGate)
        {
            if (currentActive > _peakDbConnections)
                _peakDbConnections = currentActive;
            if (waitMs > 5.0 || currentActive >= _maxPoolSize)
                _dbWaitCount++;
            if (waitMs > _maxDbWaitMs)
                _maxDbWaitMs = waitMs;
        }
    }

    private void RecordRedMetricSample(
        string service,
        string route,
        string method,
        string statusClass,
        bool isError,
        double durationMs,
        string[] attemptedHighCardinalityTags)
    {
        lock (_telemetryGate)
        {
            _totalRequests++;
            if (isError)
                _totalErrors++;

            _latenciesMs.Add(durationMs);
            if (_latenciesMs.Count > 5000)
                _latenciesMs.RemoveRange(0, _latenciesMs.Count - 5000);

            foreach (var key in attemptedHighCardinalityTags)
            {
                if (ForbiddenHighCardinalityKeys.Contains(key, StringComparer.OrdinalIgnoreCase))
                    _rejectedHighCardinalityTags++;
            }

            var seriesKey = $"{service}|{route}|{method}|{statusClass}";
            if (!_redSeries.TryGetValue(seriesKey, out var counter))
            {
                counter = new RedSeriesCounter(seriesKey, service, route, method, statusClass);
                _redSeries[seriesKey] = counter;
            }

            counter.RequestsTotal++;
            if (isError)
                counter.ErrorsTotal++;
        }
    }

    private static double Percentile(double[] sortedValues, double percentile)
    {
        if (sortedValues.Length == 0)
            return 0;

        var index = (int)Math.Ceiling(percentile * sortedValues.Length) - 1;
        index = Math.Clamp(index, 0, sortedValues.Length - 1);
        return Math.Round(sortedValues[index], 3);
    }

    public void Dispose()
    {
        _activityListener.Dispose();
        _activitySource.Dispose();
    }

    [GeneratedRegex(@"(?<!\d)(?:\d[ -]?){12,18}\d(?!\d)")]
    private static partial Regex CardCandidateRegex();

    [GeneratedRegex(@"(?<!\d)[2-9]\d{3}[ -]?\d{4}[ -]?\d{4}(?!\d)")]
    private static partial Regex AadhaarCandidateRegex();

    [GeneratedRegex(@"[^\d]")]
    private static partial Regex DigitsOnlyRegex();

    private sealed class RedSeriesCounter(string seriesKey, string service, string route, string method, string statusClass)
    {
        public string SeriesKey { get; } = seriesKey;
        public string Service { get; } = service;
        public string Route { get; } = route;
        public string Method { get; } = method;
        public string StatusClass { get; } = statusClass;
        public long RequestsTotal { get; set; }
        public long ErrorsTotal { get; set; }
    }
}

public sealed record TraceCheckoutRequest(long? CustomerId = null, long? RestaurantId = null, bool? SimulateFail = null, int? SlowDbMs = null);

public sealed record TraceCheckoutResult(
    int StatusCode,
    string TraceId,
    string SpanId,
    string ParentSpanId,
    long? OrderId,
    string Status,
    double DurationMs,
    string? Error);

public sealed record IngestSpanRequest(
    string TraceId,
    string SpanId,
    string? ParentSpanId,
    string? Service,
    string Operation,
    double DurationMs,
    string? Status,
    DateTime? StartedAtUtc = null,
    Dictionary<string, string>? Attributes = null);

public sealed record TraceSpanRecord(
    string TraceId,
    string SpanId,
    string ParentSpanId,
    string Service,
    string Operation,
    DateTime StartedAtUtc,
    double DurationMs,
    string Status,
    Dictionary<string, string> Attributes);

public sealed record StructuredLogEntry(
    DateTime TimestampUtc,
    string Level,
    string Service,
    string TraceId,
    string SpanId,
    string? OrderId,
    string Message,
    bool PiiMasked);

public sealed record TraceViewResponse(
    string TraceId,
    int SpanCount,
    string RootOperation,
    double TotalDurationMs,
    TraceSpanRecord[] Spans,
    StructuredLogEntry[] StructuredLogs);

public sealed record RedSeriesView(
    string SeriesKey,
    string Service,
    string Route,
    string Method,
    string StatusClass,
    long RequestsTotal,
    long ErrorsTotal);

public sealed record LatencyPercentiles(double P50, double P95, double P99, double Avg);

public sealed record RedMetricsSection(
    long RequestsTotal,
    long ErrorsTotal,
    double ErrorRatePercent,
    LatencyPercentiles DurationMs,
    int SeriesCount,
    RedSeriesView[] Series);

public sealed record DbPoolUseMetrics(
    int MaxPoolSize,
    int ActiveConnections,
    int PeakActiveConnections,
    double UtilizationRatio,
    long SaturationWaitCount,
    double MaxWaitMs,
    long PoolErrorsTotal);

public sealed record UseMetricsSection(DbPoolUseMetrics DbPool);

public sealed record CardinalityGuardInfo(
    string[] AllowedLabelKeys,
    string[] ForbiddenHighCardinalityKeys,
    long RejectedHighCardinalityTagCount,
    int UniqueSeriesCount);

public sealed record ObservabilityMetricsSnapshot(
    RedMetricsSection Red,
    UseMetricsSection Use,
    CardinalityGuardInfo CardinalityGuard);

public sealed record RawLogInput(string? Level, string? TraceId, string? OrderId, string? Message);

public sealed record LogStormDemoRequest(
    bool? DropDebug = null,
    int? SampleInfoPercent = null,
    IReadOnlyList<RawLogInput>? Events = null);

public sealed record LogStormDemoResponse(
    int InputCount,
    int EmittedCount,
    Dictionary<string, int> DroppedByLevel,
    int MaskedFieldCount,
    bool DropDebug,
    int SampleInfoPercent,
    StructuredLogEntry[] EmittedLogs);
