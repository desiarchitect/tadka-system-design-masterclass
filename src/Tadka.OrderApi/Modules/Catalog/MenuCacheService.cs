using System.Text.Json;
using StackExchange.Redis;

namespace Tadka.OrderApi.Modules.Catalog;

public sealed record CachedMenuResult(
    CatalogMenuResponse? Menu,
    string CacheStatus,
    string CacheKey,
    int? TtlSeconds);

public sealed record StampedeMenuResult(
    CatalogMenuResponse? Menu,
    string CacheStatus,
    string StampedeRole,
    string CacheKey,
    int? TtlSeconds);

public sealed record TtlJitterSampleResponse(
    int Count,
    int BaseTtlSeconds,
    int JitterSeconds,
    IReadOnlyList<long> FixedTtlsMs,
    long FixedSpreadMs,
    IReadOnlyList<long> JitteredTtlsMs,
    long JitteredSpreadMs,
    int DistinctJitteredSeconds);

public sealed record CacheStatsResponse(
    long RestaurantId,
    string CacheKey,
    bool KeyExists,
    double? TtlRemainingSeconds,
    long CacheHits,
    long CacheMisses,
    long DbQueries,
    long Invalidations,
    long WriteThroughUpdates,
    long CoalescedRequests,
    CatalogMenuResponse? CachedMenu,
    CatalogMenuResponse? DbMenu);

public sealed class MenuCacheService(
    IConnectionMultiplexer redis,
    ICatalogModule catalog,
    IConfiguration configuration,
    ILogger<MenuCacheService> logger)
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly int _menuTtlSeconds = configuration.GetValue<int?>("Caching:MenuTtlSeconds") ?? 30;
    private readonly object _inFlightLock = new();
    private readonly Dictionary<string, Task<(CatalogMenuResponse? Menu, int? TtlSeconds)>> _inFlight = new();

    private long _cacheHits;
    private long _cacheMisses;
    private long _dbQueries;
    private long _invalidations;
    private long _writeThroughUpdates;
    private long _coalescedRequests;

    public static string GetCacheKey(long restaurantId) => $"tadka:menu:{restaurantId}";

    public async Task<CachedMenuResult> GetMenuAsync(long restaurantId, string strategy, CancellationToken ct)
    {
        var key = GetCacheKey(restaurantId);
        if (string.Equals(strategy, "bypass", StringComparison.OrdinalIgnoreCase))
        {
            Interlocked.Increment(ref _dbQueries);
            var direct = await catalog.GetMenuAsync(restaurantId, ct);
            return new CachedMenuResult(direct, "BYPASS", key, null);
        }

        var hit = await TryReadCacheAsync(key, restaurantId);
        if (hit is { } cachedHit)
            return cachedHit;

        if (!redis.IsConnected)
        {
            Interlocked.Increment(ref _dbQueries);
            var fallback = await catalog.GetMenuAsync(restaurantId, ct);
            return new CachedMenuResult(fallback, "FALLBACK", key, null);
        }

        Interlocked.Increment(ref _cacheMisses);
        Interlocked.Increment(ref _dbQueries);
        var menu = await catalog.GetMenuAsync(restaurantId, ct);
        if (menu is null)
            return new CachedMenuResult(null, "MISS", key, null);

        await TryWriteCacheAsync(key, restaurantId, menu, _menuTtlSeconds);
        return new CachedMenuResult(menu, "MISS", key, _menuTtlSeconds);
    }

    public async Task<StampedeMenuResult> GetMenuStampedeAsync(
        long restaurantId,
        string mode,
        int dbDelayMs,
        int baseTtlSeconds,
        int jitterSeconds,
        CancellationToken ct)
    {
        var key = GetCacheKey(restaurantId);
        var hit = await TryReadCacheAsync(key, restaurantId);
        if (hit is { } cachedHit)
        {
            return new StampedeMenuResult(cachedHit.Menu, "HIT", "cache-hit", key, cachedHit.TtlSeconds);
        }

        if (string.Equals(mode, "naive", StringComparison.OrdinalIgnoreCase))
        {
            Interlocked.Increment(ref _cacheMisses);
            Interlocked.Increment(ref _dbQueries);
            if (dbDelayMs > 0)
                await Task.Delay(dbDelayMs, ct);

            var naiveMenu = await catalog.GetMenuAsync(restaurantId, ct);
            if (naiveMenu is null)
                return new StampedeMenuResult(null, "MISS", "uncoordinated-miss", key, null);

            await TryWriteCacheAsync(key, restaurantId, naiveMenu, baseTtlSeconds);
            return new StampedeMenuResult(naiveMenu, "MISS", "uncoordinated-miss", key, baseTtlSeconds);
        }

        TaskCompletionSource<(CatalogMenuResponse? Menu, int? TtlSeconds)>? leaderTcs = null;
        Task<(CatalogMenuResponse? Menu, int? TtlSeconds)> sharedTask;
        bool isLeader;

        lock (_inFlightLock)
        {
            if (_inFlight.TryGetValue(key, out var existingTask))
            {
                sharedTask = existingTask;
                isLeader = false;
            }
            else
            {
                leaderTcs = new TaskCompletionSource<(CatalogMenuResponse? Menu, int? TtlSeconds)>(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                sharedTask = leaderTcs.Task;
                _inFlight[key] = sharedTask;
                isLeader = true;
            }
        }

        if (!isLeader)
        {
            Interlocked.Increment(ref _coalescedRequests);
            var joined = await sharedTask.WaitAsync(ct);
            return new StampedeMenuResult(joined.Menu, "COALESCED", "coalesced", key, joined.TtlSeconds);
        }

        try
        {
            // Re-check cache after claiming leadership in case a previous leader just finished.
            var doubleCheck = await TryReadCacheAsync(key, restaurantId, countAsHit: false);
            if (doubleCheck is { } populated)
            {
                var early = (populated.Menu, populated.TtlSeconds);
                lock (_inFlightLock)
                    _inFlight.Remove(key);
                leaderTcs!.TrySetResult(early);
                return new StampedeMenuResult(populated.Menu, "HIT", "cache-hit", key, populated.TtlSeconds);
            }

            Interlocked.Increment(ref _cacheMisses);
            Interlocked.Increment(ref _dbQueries);

            // Disclosed artificial delay ensures concurrent loopback calls overlap deterministically.
            if (dbDelayMs > 0)
                await Task.Delay(dbDelayMs, CancellationToken.None);

            var menu = await catalog.GetMenuAsync(restaurantId, CancellationToken.None);
            int? ttl = null;
            if (menu is not null)
            {
                var offset = jitterSeconds > 0 ? Random.Shared.Next(0, jitterSeconds + 1) : 0;
                ttl = baseTtlSeconds + offset;
                await TryWriteCacheAsync(key, restaurantId, menu, ttl.Value);
            }

            var outcome = (menu, ttl);
            lock (_inFlightLock)
                _inFlight.Remove(key);
            leaderTcs!.TrySetResult(outcome);
            return new StampedeMenuResult(menu, "MISS", "leader", key, ttl);
        }
        catch (Exception ex)
        {
            lock (_inFlightLock)
                _inFlight.Remove(key);
            leaderTcs!.TrySetException(ex);
            throw;
        }
    }

    public async Task<TtlJitterSampleResponse> SampleTtlJitterAsync(
        int count,
        int baseTtlSeconds,
        int jitterSeconds,
        CancellationToken ct)
    {
        var db = redis.GetDatabase();
        var fixedTtlsMs = new List<long>(count);
        var jitteredTtlsMs = new List<long>(count);

        for (var i = 1; i <= count; i++)
        {
            ct.ThrowIfCancellationRequested();
            var fixedKey = $"tadka:sample:fixed:{i}";
            await db.StringSetAsync(fixedKey, "v1", TimeSpan.FromSeconds(baseTtlSeconds));
            var fixedRemaining = await db.KeyTimeToLiveAsync(fixedKey);
            fixedTtlsMs.Add(fixedRemaining is { } fr ? (long)Math.Round(fr.TotalMilliseconds) : 0L);

            var offset = jitterSeconds > 0 ? Random.Shared.Next(0, jitterSeconds + 1) : 0;
            var jitteredKey = $"tadka:sample:jittered:{i}";
            await db.StringSetAsync(jitteredKey, "v1", TimeSpan.FromSeconds(baseTtlSeconds + offset));
            var jitteredRemaining = await db.KeyTimeToLiveAsync(jitteredKey);
            jitteredTtlsMs.Add(jitteredRemaining is { } jr ? (long)Math.Round(jr.TotalMilliseconds) : 0L);
        }

        var fixedSpreadMs = fixedTtlsMs.Count > 0 ? fixedTtlsMs.Max() - fixedTtlsMs.Min() : 0L;
        var jitteredSpreadMs = jitteredTtlsMs.Count > 0 ? jitteredTtlsMs.Max() - jitteredTtlsMs.Min() : 0L;
        var distinctJitteredSeconds = jitteredTtlsMs
            .Select(ms => (int)Math.Round(ms / 1000.0))
            .Distinct()
            .Count();

        return new TtlJitterSampleResponse(
            count,
            baseTtlSeconds,
            jitterSeconds,
            fixedTtlsMs,
            fixedSpreadMs,
            jitteredTtlsMs,
            jitteredSpreadMs,
            distinctJitteredSeconds);
    }

    public async Task<(CatalogMenuItem? Item, string InvalidationHeader)> UpdateItemWithCacheAsync(
        long restaurantId,
        long itemId,
        decimal? price,
        bool? isAvailable,
        string invalidation,
        CancellationToken ct)
    {
        var updated = await catalog.UpdateItemAsync(restaurantId, itemId, price, isAvailable, ct);
        if (updated is null)
            return (null, "none");

        var key = GetCacheKey(restaurantId);
        var mode = invalidation.ToLowerInvariant();
        if (mode == "none")
            return (updated, "skipped");

        if (mode == "write-through")
        {
            var freshMenu = await catalog.GetMenuAsync(restaurantId, ct);
            if (freshMenu is not null && redis.IsConnected)
            {
                await TryWriteCacheAsync(key, restaurantId, freshMenu, _menuTtlSeconds);
                Interlocked.Increment(ref _writeThroughUpdates);
            }
            return (updated, "write-through");
        }

        if (redis.IsConnected)
        {
            try
            {
                var db = redis.GetDatabase();
                await db.KeyDeleteAsync(key);
                Interlocked.Increment(ref _invalidations);
            }
            catch (RedisException ex)
            {
                logger.LogWarning(ex, "redis_invalidate_failed restaurant_id={RestaurantId}", restaurantId);
            }
        }
        return (updated, "deleted");
    }

    public async Task ResetAsync(CancellationToken ct)
    {
        Interlocked.Exchange(ref _cacheHits, 0);
        Interlocked.Exchange(ref _cacheMisses, 0);
        Interlocked.Exchange(ref _dbQueries, 0);
        Interlocked.Exchange(ref _invalidations, 0);
        Interlocked.Exchange(ref _writeThroughUpdates, 0);
        Interlocked.Exchange(ref _coalescedRequests, 0);

        await catalog.UpdateItemAsync(7, 101, 240.00m, true, ct);

        if (!redis.IsConnected)
            return;

        try
        {
            var db = redis.GetDatabase();
            foreach (var endpoint in redis.GetEndPoints())
            {
                var server = redis.GetServer(endpoint);
                var keys = server.Keys(pattern: "tadka:*").ToArray();
                if (keys.Length > 0)
                    await db.KeyDeleteAsync(keys);
            }
        }
        catch (RedisException ex)
        {
            logger.LogWarning(ex, "redis_reset_failed");
        }
    }

    public async Task<CacheStatsResponse> GetStatsAsync(long restaurantId, CancellationToken ct)
    {
        var key = GetCacheKey(restaurantId);
        var keyExists = false;
        double? ttlRemaining = null;
        CatalogMenuResponse? cachedMenu = null;

        if (redis.IsConnected)
        {
            try
            {
                var db = redis.GetDatabase();
                var entry = await db.StringGetWithExpiryAsync(key);
                if (entry.Value.HasValue)
                {
                    keyExists = true;
                    ttlRemaining = entry.Expiry is { } exp ? Math.Round(exp.TotalSeconds, 2) : null;
                    cachedMenu = JsonSerializer.Deserialize<CatalogMenuResponse>(entry.Value.ToString(), JsonOptions);
                }
            }
            catch (RedisException ex)
            {
                logger.LogWarning(ex, "redis_stats_failed restaurant_id={RestaurantId}", restaurantId);
            }
        }

        // Direct diagnostics read from PostgreSQL does not increment user-traffic _dbQueries counter.
        var dbMenu = await catalog.GetMenuAsync(restaurantId, ct);

        return new CacheStatsResponse(
            restaurantId,
            key,
            keyExists,
            ttlRemaining,
            Interlocked.Read(ref _cacheHits),
            Interlocked.Read(ref _cacheMisses),
            Interlocked.Read(ref _dbQueries),
            Interlocked.Read(ref _invalidations),
            Interlocked.Read(ref _writeThroughUpdates),
            Interlocked.Read(ref _coalescedRequests),
            cachedMenu,
            dbMenu);
    }

    private async Task<CachedMenuResult?> TryReadCacheAsync(string key, long restaurantId, bool countAsHit = true)
    {
        if (!redis.IsConnected)
            return null;

        try
        {
            var db = redis.GetDatabase();
            var entry = await db.StringGetWithExpiryAsync(key);
            if (entry.Value.HasValue)
            {
                var cached = JsonSerializer.Deserialize<CatalogMenuResponse>(entry.Value.ToString(), JsonOptions);
                if (cached is not null)
                {
                    if (countAsHit)
                        Interlocked.Increment(ref _cacheHits);
                    int? ttlSeconds = entry.Expiry is { } exp
                        ? Math.Max(1, (int)Math.Ceiling(exp.TotalSeconds))
                        : null;
                    return new CachedMenuResult(cached, "HIT", key, ttlSeconds);
                }
            }
        }
        catch (RedisException ex)
        {
            logger.LogWarning(ex, "redis_read_fallback restaurant_id={RestaurantId}", restaurantId);
        }

        return null;
    }

    private async Task TryWriteCacheAsync(string key, long restaurantId, CatalogMenuResponse menu, int ttlSeconds)
    {
        if (!redis.IsConnected)
            return;

        try
        {
            var db = redis.GetDatabase();
            var json = JsonSerializer.Serialize(menu, JsonOptions);
            await db.StringSetAsync(key, json, TimeSpan.FromSeconds(ttlSeconds));
        }
        catch (RedisException ex)
        {
            logger.LogWarning(ex, "redis_write_skipped restaurant_id={RestaurantId}", restaurantId);
        }
    }
}
