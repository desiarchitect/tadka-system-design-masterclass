using System.Text;

namespace Tadka.OrderApi.Modules.Sharding;

public static class ShardRouter
{
    public const int ShardCount = 4;

    public static string SchemaForShard(int shardIndex) => shardIndex switch
    {
        0 => "shard_0",
        1 => "shard_1",
        2 => "shard_2",
        3 => "shard_3",
        _ => throw new ArgumentOutOfRangeException(nameof(shardIndex), "Shard index must be 0..3."),
    };

    public static (int ShardIndex, string ShardKey) Resolve(
        string strategy,
        long customerId,
        long restaurantId,
        int sequenceHint)
    {
        return strategy switch
        {
            "by-restaurant" => (Bucket($"rest:{restaurantId}"), $"rest:{restaurantId}"),
            "by-customer" => (Bucket($"cust:{customerId}"), $"cust:{customerId}"),
            "salted-restaurant" => ResolveSalted(restaurantId, sequenceHint),
            _ => throw new ArgumentException($"Unknown sharding strategy: {strategy}", nameof(strategy)),
        };
    }

    public static int RouteCustomer(long customerId) => Bucket($"cust:{customerId}");

    public static int RouteRestaurant(long restaurantId) => Bucket($"rest:{restaurantId}");

    private static (int ShardIndex, string ShardKey) ResolveSalted(long restaurantId, int sequenceHint)
    {
        var salt = Math.Abs(sequenceHint) % ShardCount;
        var key = $"rest:{restaurantId}#salt:{salt}";
        // Deterministic salt spread: start from restaurant hash + salt so the 4 salts land on all 4 shards cleanly.
        var shard = (Bucket($"rest:{restaurantId}") + salt) % ShardCount;
        return (shard, key);
    }

    // FNV-1a 64-bit hash: deterministic across process restarts (unlike string.GetHashCode).
    private static int Bucket(string key)
    {
        const ulong fnvOffset = 14_695_981_039_346_656_037UL;
        const ulong fnvPrime = 1_099_511_628_211UL;

        var hash = fnvOffset;
        foreach (var b in Encoding.UTF8.GetBytes(key))
        {
            hash ^= b;
            hash *= fnvPrime;
        }

        return (int)(hash % ShardCount);
    }
}
