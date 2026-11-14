using System.Text.Json;
using StackExchange.Redis;

namespace Tadka.OrderApi.Modules.Catalog;

public sealed record CachedMenuResult(
    CatalogMenuResponse? Menu,
    string CacheStatus,
    string CacheKey,
    int? TtlSeconds);

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

        if (redis.IsConnected)
        {
            try
            {
                var db = redis.GetDatabase();
                var entry = await db.StringGetWithExpiryAsync(key);
                if (entry.Value.HasValue)
                {
                    var cached = JsonSerializer.Deserialize<CatalogMenuResponse>(entry.Value.ToString(), JsonOptions);
                    if (cached is not null)
                    {
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
                Interlocked.Increment(ref _dbQueries);
                var fallback = await catalog.GetMenuAsync(restaurantId, ct);
                return new CachedMenuResult(fallback, "FALLBACK", key, null);
            }
        }
        else
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

        try
        {
            var db = redis.GetDatabase();
            var json = JsonSerializer.Serialize(menu, JsonOptions);
            await db.StringSetAsync(key, json, TimeSpan.FromSeconds(_menuTtlSeconds));
        }
        catch (RedisException ex)
        {
            logger.LogWarning(ex, "redis_write_skipped restaurant_id={RestaurantId}", restaurantId);
        }

        return new CachedMenuResult(menu, "MISS", key, _menuTtlSeconds);
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
        {
            return (updated, "skipped");
        }

        if (mode == "write-through")
        {
            var freshMenu = await catalog.GetMenuAsync(restaurantId, ct);
            if (freshMenu is not null && redis.IsConnected)
            {
                try
                {
                    var db = redis.GetDatabase();
                    var json = JsonSerializer.Serialize(freshMenu, JsonOptions);
                    await db.StringSetAsync(key, json, TimeSpan.FromSeconds(_menuTtlSeconds));
                    Interlocked.Increment(ref _writeThroughUpdates);
                }
                catch (RedisException ex)
                {
                    logger.LogWarning(ex, "redis_write_through_failed restaurant_id={RestaurantId}", restaurantId);
                }
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
}
