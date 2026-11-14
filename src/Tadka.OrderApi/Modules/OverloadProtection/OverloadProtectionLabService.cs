using System.Collections.Concurrent;
using System.Globalization;
using StackExchange.Redis;

namespace Tadka.OrderApi.Modules.OverloadProtection;

public sealed record RateLimitConfigRequest(
    int? Capacity,
    double? RefillPerSecond,
    string? Mode,
    int? RaceWindowMs,
    bool? SimulateRedisDown,
    string? FailurePolicy,
    int? LocalCapacity,
    bool? ResetState);

public sealed record RateLimitCheckRequest(
    string? Key,
    string? Endpoint,
    int? Cost,
    string? FailurePolicy);

public sealed record RateLimitCheckResult(
    int StatusCode,
    bool Ok,
    string Decision,
    string Mode,
    string Key,
    string Endpoint,
    int Limit,
    int Remaining,
    int RetryAfterSeconds,
    int ResetSeconds,
    bool FallbackUsed,
    string? FailurePolicy);

public sealed record BulkheadConfigRequest(
    string? IsolationMode,
    int? SharedMaxConcurrency,
    int? AnalyticsMaxConcurrency,
    int? CheckoutMaxConcurrency,
    bool? ResetCounters);

public sealed record BulkheadWorkRequest(string? Lane, int? WorkMs);

public sealed record BulkheadWorkResult(
    int StatusCode,
    bool Ok,
    string Lane,
    string IsolationMode,
    int WorkMs,
    string Reason);

public sealed record SheddingConfigRequest(
    bool? Enabled,
    int? SimulatedLoadPercent,
    int? ShedLowThresholdPercent,
    int? ShedNormalThresholdPercent,
    bool? ResetCounters);

public sealed record SheddingCheckRequest(string? Priority);

public sealed record SheddingCheckResult(
    int StatusCode,
    bool Ok,
    bool Shed,
    string Priority,
    int SimulatedLoadPercent,
    int RetryAfterSeconds,
    string Reason);

public sealed record OverloadStatusSnapshot(
    object RateLimiter,
    object Bulkhead,
    object LoadShedding);

internal sealed class LocalTokenBucketState(double tokens, DateTimeOffset lastRefillUtc)
{
    public readonly object Gate = new();
    public double Tokens = tokens;
    public DateTimeOffset LastRefillUtc = lastRefillUtc;
}

public sealed class OverloadProtectionLabService(
    IConnectionMultiplexer redis,
    ILogger<OverloadProtectionLabService> logger)
{
    private const string TokenBucketLuaScript = @"
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])

local t = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local last_ts = tonumber(data[2])

if tokens == nil or last_ts == nil then
  tokens = capacity
  last_ts = now_ms
else
  local elapsed_ms = math.max(0, now_ms - last_ts)
  local added = (elapsed_ms / 1000.0) * refill_rate
  tokens = math.min(capacity, tokens + added)
  last_ts = now_ms
end

local allowed = 0
local retry_after_ms = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  local needed = cost - tokens
  if refill_rate > 0 then
    retry_after_ms = math.ceil((needed / refill_rate) * 1000)
  else
    retry_after_ms = 60000
  end
end

local ttl_s = 60
if refill_rate > 0 then
  ttl_s = math.max(60, math.ceil((capacity / refill_rate) * 2))
end
redis.call('HSET', key, 'tokens', string.format('%.6f', tokens), 'ts', tostring(last_ts))
redis.call('EXPIRE', key, ttl_s)

local reset_s = 0
if refill_rate > 0 and tokens < capacity then
  reset_s = math.ceil((capacity - tokens) / refill_rate)
end

return { allowed, math.floor(tokens), retry_after_ms, reset_s }
";

    private readonly object _gate = new();

    // Rate limiter state
    private int _rlCapacity = 5;
    private double _rlRefillPerSecond = 1.0;
    private string _rlMode = "atomic-lua";
    private int _rlRaceWindowMs = 15;
    private bool _simulateRedisDown;
    private string _failurePolicy = "fail-open";
    private int _localCapacity = 3;
    private readonly ConcurrentDictionary<string, LocalTokenBucketState> _localBuckets = new();

    private long _rlAllowed;
    private long _rlRejected;
    private long _rlFailClosedRejected;
    private long _rlFailOpenAllowed;
    private long _rlLocalFallbackAllowed;
    private long _rlLocalFallbackRejected;

    // Bulkhead state
    private string _isolationMode = "bulkhead";
    private int _sharedMaxConcurrency = 4;
    private int _analyticsMaxConcurrency = 2;
    private int _checkoutMaxConcurrency = 4;
    private SemaphoreSlim _sharedSemaphore = new(4, 4);
    private SemaphoreSlim _analyticsSemaphore = new(2, 2);
    private SemaphoreSlim _checkoutSemaphore = new(4, 4);

    private long _sharedAdmitted;
    private long _sharedRejected;
    private long _analyticsAdmitted;
    private long _analyticsRejected;
    private long _checkoutAdmitted;
    private long _checkoutRejected;

    // Load shedding state
    private bool _sheddingEnabled = true;
    private int _simulatedLoadPercent;
    private int _shedLowThresholdPercent = 80;
    private int _shedNormalThresholdPercent = 95;

    private long _sheddingAdmitted;
    private long _sheddingShedLow;
    private long _sheddingShedNormal;

    public async Task<OverloadStatusSnapshot> ConfigureRateLimiterAsync(RateLimitConfigRequest req, CancellationToken ct)
    {
        bool shouldReset;
        lock (_gate)
        {
            if (req.Capacity is { } cap)
            {
                if (cap is < 1 or > 1000)
                    throw new ArgumentOutOfRangeException(nameof(req.Capacity), "capacity must be between 1 and 1000.");
                _rlCapacity = cap;
            }

            if (req.RefillPerSecond is { } rate)
            {
                if (rate is < 0.01 or > 1000.0)
                    throw new ArgumentOutOfRangeException(nameof(req.RefillPerSecond), "refillPerSecond must be between 0.01 and 1000.");
                _rlRefillPerSecond = rate;
            }

            if (!string.IsNullOrWhiteSpace(req.Mode))
            {
                var normalized = req.Mode.Trim().ToLowerInvariant();
                if (normalized is not ("atomic-lua" or "naive-non-atomic"))
                    throw new ArgumentException("mode must be 'atomic-lua' or 'naive-non-atomic'.");
                _rlMode = normalized;
            }

            if (req.RaceWindowMs is { } rw)
            {
                if (rw is < 0 or > 500)
                    throw new ArgumentOutOfRangeException(nameof(req.RaceWindowMs), "raceWindowMs must be between 0 and 500.");
                _rlRaceWindowMs = rw;
            }

            if (req.SimulateRedisDown is { } srd)
                _simulateRedisDown = srd;

            if (!string.IsNullOrWhiteSpace(req.FailurePolicy))
            {
                var fp = req.FailurePolicy.Trim().ToLowerInvariant();
                if (fp is not ("fail-closed" or "fail-open" or "local-fallback"))
                    throw new ArgumentException("failurePolicy must be 'fail-closed', 'fail-open', or 'local-fallback'.");
                _failurePolicy = fp;
            }

            if (req.LocalCapacity is { } lc)
            {
                if (lc is < 1 or > 100)
                    throw new ArgumentOutOfRangeException(nameof(req.LocalCapacity), "localCapacity must be between 1 and 100.");
                _localCapacity = lc;
            }

            shouldReset = req.ResetState == true;
            if (shouldReset)
            {
                _localBuckets.Clear();
                Interlocked.Exchange(ref _rlAllowed, 0);
                Interlocked.Exchange(ref _rlRejected, 0);
                Interlocked.Exchange(ref _rlFailClosedRejected, 0);
                Interlocked.Exchange(ref _rlFailOpenAllowed, 0);
                Interlocked.Exchange(ref _rlLocalFallbackAllowed, 0);
                Interlocked.Exchange(ref _rlLocalFallbackRejected, 0);
            }
        }

        if (shouldReset && redis.IsConnected)
        {
            try
            {
                var db = redis.GetDatabase();
                foreach (var endpoint in redis.GetEndPoints())
                {
                    var server = redis.GetServer(endpoint);
                    var keys = server.Keys(pattern: "tadka:rl:*").ToArray();
                    if (keys.Length > 0)
                        await db.KeyDeleteAsync(keys);
                }
            }
            catch (RedisException ex)
            {
                logger.LogWarning(ex, "overload_rl_redis_reset_skipped");
            }
        }

        ct.ThrowIfCancellationRequested();
        return GetStatus();
    }

    public async Task<RateLimitCheckResult> CheckRateLimitAsync(
        string key,
        string endpoint,
        int cost,
        string? requestFailurePolicy,
        CancellationToken ct)
    {
        int capacity;
        double refillRate;
        string mode;
        int raceWindowMs;
        bool redisDown;
        string policy;
        int localCapacity;

        lock (_gate)
        {
            capacity = _rlCapacity;
            refillRate = _rlRefillPerSecond;
            mode = _rlMode;
            raceWindowMs = _rlRaceWindowMs;
            redisDown = _simulateRedisDown || !redis.IsConnected;
            policy = string.IsNullOrWhiteSpace(requestFailurePolicy)
                ? _failurePolicy
                : requestFailurePolicy.Trim().ToLowerInvariant();
            localCapacity = _localCapacity;
        }

        if (policy is not ("fail-closed" or "fail-open" or "local-fallback"))
            throw new ArgumentException("failurePolicy must be 'fail-closed', 'fail-open', or 'local-fallback'.");

        if (redisDown)
        {
            return EvaluateFallback(key, endpoint, cost, policy, localCapacity, refillRate, mode);
        }

        try
        {
            var db = redis.GetDatabase();
            if (mode == "naive-non-atomic")
            {
                // Negative control: separate GET + disclosed race delay + SET leaks concurrent requests past capacity.
                var naiveKey = $"tadka:rl:naive:{key}";
                var raw = await db.StringGetAsync(naiveKey);
                var current = raw.HasValue && int.TryParse(raw.ToString(), out var parsed)
                    ? parsed
                    : capacity;

                if (current < cost)
                {
                    Interlocked.Increment(ref _rlRejected);
                    return new RateLimitCheckResult(
                        StatusCodes.Status429TooManyRequests,
                        false,
                        "rejected_rate_limit",
                        mode,
                        key,
                        endpoint,
                        capacity,
                        0,
                        1,
                        1,
                        false,
                        null);
                }

                if (raceWindowMs > 0)
                    await Task.Delay(raceWindowMs, ct);

                var next = Math.Max(0, current - cost);
                await db.StringSetAsync(naiveKey, next.ToString(CultureInfo.InvariantCulture), TimeSpan.FromSeconds(60));
                Interlocked.Increment(ref _rlAllowed);

                return new RateLimitCheckResult(
                    StatusCodes.Status200OK,
                    true,
                    "allowed",
                    mode,
                    key,
                    endpoint,
                    capacity,
                    next,
                    0,
                    1,
                    false,
                    null);
            }

            var luaKey = (RedisKey)$"tadka:rl:tb:{key}";
            var res = (RedisResult[]?)await db.ScriptEvaluateAsync(
                TokenBucketLuaScript,
                [luaKey],
                [
                    capacity,
                    refillRate.ToString(CultureInfo.InvariantCulture),
                    cost
                ]);

            var allowed = res is not null && (long)res[0] == 1;
            var remaining = res is not null ? (int)(long)res[1] : 0;
            var retryAfterMs = res is not null ? (long)res[2] : 1000;
            var resetSeconds = res is not null ? (int)(long)res[3] : 1;

            if (allowed)
            {
                Interlocked.Increment(ref _rlAllowed);
                return new RateLimitCheckResult(
                    StatusCodes.Status200OK,
                    true,
                    "allowed",
                    mode,
                    key,
                    endpoint,
                    capacity,
                    remaining,
                    0,
                    resetSeconds,
                    false,
                    null);
            }

            Interlocked.Increment(ref _rlRejected);
            var retryAfterSeconds = Math.Max(1, (int)Math.Ceiling(retryAfterMs / 1000.0));
            return new RateLimitCheckResult(
                StatusCodes.Status429TooManyRequests,
                false,
                "rejected_rate_limit",
                mode,
                key,
                endpoint,
                capacity,
                0,
                retryAfterSeconds,
                resetSeconds,
                false,
                null);
        }
        catch (RedisException ex)
        {
            logger.LogWarning(ex, "overload_rl_redis_error_fallback policy={Policy} endpoint={Endpoint}", policy, endpoint);
            return EvaluateFallback(key, endpoint, cost, policy, localCapacity, refillRate, mode);
        }
    }

    private RateLimitCheckResult EvaluateFallback(
        string key,
        string endpoint,
        int cost,
        string policy,
        int localCapacity,
        double refillRate,
        string mode)
    {
        if (policy == "fail-closed")
        {
            Interlocked.Increment(ref _rlFailClosedRejected);
            return new RateLimitCheckResult(
                StatusCodes.Status503ServiceUnavailable,
                false,
                "rejected_fail_closed",
                mode,
                key,
                endpoint,
                localCapacity,
                0,
                1,
                1,
                true,
                "fail-closed");
        }

        if (policy == "fail-open")
        {
            Interlocked.Increment(ref _rlFailOpenAllowed);
            return new RateLimitCheckResult(
                StatusCodes.Status200OK,
                true,
                "allowed_fail_open",
                mode,
                key,
                endpoint,
                localCapacity,
                localCapacity,
                0,
                0,
                true,
                "fail-open");
        }

        var bucket = _localBuckets.GetOrAdd(key, _ => new LocalTokenBucketState(localCapacity, DateTimeOffset.UtcNow));
        lock (bucket.Gate)
        {
            var now = DateTimeOffset.UtcNow;
            var elapsed = Math.Max(0.0, (now - bucket.LastRefillUtc).TotalSeconds);
            bucket.Tokens = Math.Min(localCapacity, bucket.Tokens + elapsed * refillRate);
            bucket.LastRefillUtc = now;

            if (bucket.Tokens >= cost)
            {
                bucket.Tokens -= cost;
                Interlocked.Increment(ref _rlLocalFallbackAllowed);
                return new RateLimitCheckResult(
                    StatusCodes.Status200OK,
                    true,
                    "allowed_local_fallback",
                    mode,
                    key,
                    endpoint,
                    localCapacity,
                    (int)Math.Floor(bucket.Tokens),
                    0,
                    1,
                    true,
                    "local-fallback");
            }

            Interlocked.Increment(ref _rlLocalFallbackRejected);
            return new RateLimitCheckResult(
                StatusCodes.Status429TooManyRequests,
                false,
                "rejected_local_fallback",
                mode,
                key,
                endpoint,
                localCapacity,
                0,
                1,
                1,
                true,
                "local-fallback");
        }
    }

    public OverloadStatusSnapshot ConfigureBulkhead(BulkheadConfigRequest req)
    {
        lock (_gate)
        {
            if (!string.IsNullOrWhiteSpace(req.IsolationMode))
            {
                var mode = req.IsolationMode.Trim().ToLowerInvariant();
                if (mode is not ("shared" or "bulkhead"))
                    throw new ArgumentException("isolationMode must be 'shared' or 'bulkhead'.");
                _isolationMode = mode;
            }

            if (req.SharedMaxConcurrency is { } s)
            {
                if (s is < 1 or > 50)
                    throw new ArgumentOutOfRangeException(nameof(req.SharedMaxConcurrency), "sharedMaxConcurrency must be between 1 and 50.");
                _sharedMaxConcurrency = s;
                _sharedSemaphore = new SemaphoreSlim(s, s);
            }

            if (req.AnalyticsMaxConcurrency is { } a)
            {
                if (a is < 1 or > 50)
                    throw new ArgumentOutOfRangeException(nameof(req.AnalyticsMaxConcurrency), "analyticsMaxConcurrency must be between 1 and 50.");
                _analyticsMaxConcurrency = a;
                _analyticsSemaphore = new SemaphoreSlim(a, a);
            }

            if (req.CheckoutMaxConcurrency is { } c)
            {
                if (c is < 1 or > 50)
                    throw new ArgumentOutOfRangeException(nameof(req.CheckoutMaxConcurrency), "checkoutMaxConcurrency must be between 1 and 50.");
                _checkoutMaxConcurrency = c;
                _checkoutSemaphore = new SemaphoreSlim(c, c);
            }

            if (req.ResetCounters == true)
            {
                _sharedSemaphore = new SemaphoreSlim(_sharedMaxConcurrency, _sharedMaxConcurrency);
                _analyticsSemaphore = new SemaphoreSlim(_analyticsMaxConcurrency, _analyticsMaxConcurrency);
                _checkoutSemaphore = new SemaphoreSlim(_checkoutMaxConcurrency, _checkoutMaxConcurrency);

                Interlocked.Exchange(ref _sharedAdmitted, 0);
                Interlocked.Exchange(ref _sharedRejected, 0);
                Interlocked.Exchange(ref _analyticsAdmitted, 0);
                Interlocked.Exchange(ref _analyticsRejected, 0);
                Interlocked.Exchange(ref _checkoutAdmitted, 0);
                Interlocked.Exchange(ref _checkoutRejected, 0);
            }

            return GetStatusUnsafe();
        }
    }

    public async Task<BulkheadWorkResult> ExecuteBulkheadWorkAsync(string lane, int workMs, CancellationToken ct)
    {
        string isolationMode;
        SemaphoreSlim semaphore;

        lock (_gate)
        {
            isolationMode = _isolationMode;
            semaphore = isolationMode == "shared"
                ? _sharedSemaphore
                : (lane == "analytics" ? _analyticsSemaphore : _checkoutSemaphore);
        }

        if (!semaphore.Wait(0))
        {
            if (isolationMode == "shared")
            {
                Interlocked.Increment(ref _sharedRejected);
                return new BulkheadWorkResult(
                    StatusCodes.Status503ServiceUnavailable,
                    false,
                    lane,
                    isolationMode,
                    workMs,
                    "shared_pool_exhausted");
            }

            if (lane == "analytics")
                Interlocked.Increment(ref _analyticsRejected);
            else
                Interlocked.Increment(ref _checkoutRejected);

            return new BulkheadWorkResult(
                StatusCodes.Status503ServiceUnavailable,
                false,
                lane,
                isolationMode,
                workMs,
                $"bulkhead_rejected:{lane}");
        }

        try
        {
            if (isolationMode == "shared")
                Interlocked.Increment(ref _sharedAdmitted);
            else if (lane == "analytics")
                Interlocked.Increment(ref _analyticsAdmitted);
            else
                Interlocked.Increment(ref _checkoutAdmitted);

            if (workMs > 0)
                await Task.Delay(workMs, ct);

            return new BulkheadWorkResult(
                StatusCodes.Status200OK,
                true,
                lane,
                isolationMode,
                workMs,
                "completed");
        }
        finally
        {
            semaphore.Release();
        }
    }

    public OverloadStatusSnapshot ConfigureLoadShedding(SheddingConfigRequest req)
    {
        lock (_gate)
        {
            if (req.Enabled is { } en)
                _sheddingEnabled = en;

            if (req.SimulatedLoadPercent is { } slp)
            {
                if (slp is < 0 or > 100)
                    throw new ArgumentOutOfRangeException(nameof(req.SimulatedLoadPercent), "simulatedLoadPercent must be between 0 and 100.");
                _simulatedLoadPercent = slp;
            }

            if (req.ShedLowThresholdPercent is { } low)
            {
                if (low is < 10 or > 100)
                    throw new ArgumentOutOfRangeException(nameof(req.ShedLowThresholdPercent), "shedLowThresholdPercent must be between 10 and 100.");
                _shedLowThresholdPercent = low;
            }

            if (req.ShedNormalThresholdPercent is { } norm)
            {
                if (norm is < 10 or > 100)
                    throw new ArgumentOutOfRangeException(nameof(req.ShedNormalThresholdPercent), "shedNormalThresholdPercent must be between 10 and 100.");
                _shedNormalThresholdPercent = norm;
            }

            if (req.ResetCounters == true)
            {
                Interlocked.Exchange(ref _sheddingAdmitted, 0);
                Interlocked.Exchange(ref _sheddingShedLow, 0);
                Interlocked.Exchange(ref _sheddingShedNormal, 0);
            }

            return GetStatusUnsafe();
        }
    }

    public SheddingCheckResult EvaluateLoadShedding(string priority)
    {
        bool enabled;
        int load;
        int lowThreshold;
        int normalThreshold;

        lock (_gate)
        {
            enabled = _sheddingEnabled;
            load = _simulatedLoadPercent;
            lowThreshold = _shedLowThresholdPercent;
            normalThreshold = _shedNormalThresholdPercent;
        }

        if (enabled && priority == "low-recommendations" && load >= lowThreshold)
        {
            Interlocked.Increment(ref _sheddingShedLow);
            return new SheddingCheckResult(
                StatusCodes.Status503ServiceUnavailable,
                false,
                true,
                priority,
                load,
                2,
                "load_shed_low_priority");
        }

        if (enabled && priority == "normal-browse" && load >= normalThreshold)
        {
            Interlocked.Increment(ref _sheddingShedNormal);
            return new SheddingCheckResult(
                StatusCodes.Status503ServiceUnavailable,
                false,
                true,
                priority,
                load,
                2,
                "load_shed_normal_priority");
        }

        Interlocked.Increment(ref _sheddingAdmitted);
        return new SheddingCheckResult(
            StatusCodes.Status200OK,
            true,
            false,
            priority,
            load,
            0,
            "admitted");
    }

    public OverloadStatusSnapshot GetStatus()
    {
        lock (_gate)
            return GetStatusUnsafe();
    }

    private OverloadStatusSnapshot GetStatusUnsafe() => new(
        RateLimiter: new
        {
            capacity = _rlCapacity,
            refillPerSecond = _rlRefillPerSecond,
            mode = _rlMode,
            raceWindowMs = _rlRaceWindowMs,
            simulateRedisDown = _simulateRedisDown,
            failurePolicy = _failurePolicy,
            localCapacity = _localCapacity,
            allowed = Interlocked.Read(ref _rlAllowed),
            rejected = Interlocked.Read(ref _rlRejected),
            failClosedRejected = Interlocked.Read(ref _rlFailClosedRejected),
            failOpenAllowed = Interlocked.Read(ref _rlFailOpenAllowed),
            localFallbackAllowed = Interlocked.Read(ref _rlLocalFallbackAllowed),
            localFallbackRejected = Interlocked.Read(ref _rlLocalFallbackRejected)
        },
        Bulkhead: new
        {
            isolationMode = _isolationMode,
            sharedMaxConcurrency = _sharedMaxConcurrency,
            analyticsMaxConcurrency = _analyticsMaxConcurrency,
            checkoutMaxConcurrency = _checkoutMaxConcurrency,
            sharedAdmitted = Interlocked.Read(ref _sharedAdmitted),
            sharedRejected = Interlocked.Read(ref _sharedRejected),
            analyticsAdmitted = Interlocked.Read(ref _analyticsAdmitted),
            analyticsRejected = Interlocked.Read(ref _analyticsRejected),
            checkoutAdmitted = Interlocked.Read(ref _checkoutAdmitted),
            checkoutRejected = Interlocked.Read(ref _checkoutRejected)
        },
        LoadShedding: new
        {
            enabled = _sheddingEnabled,
            simulatedLoadPercent = _simulatedLoadPercent,
            shedLowThresholdPercent = _shedLowThresholdPercent,
            shedNormalThresholdPercent = _shedNormalThresholdPercent,
            admitted = Interlocked.Read(ref _sheddingAdmitted),
            shedLow = Interlocked.Read(ref _sheddingShedLow),
            shedNormal = Interlocked.Read(ref _sheddingShedNormal)
        });
}
