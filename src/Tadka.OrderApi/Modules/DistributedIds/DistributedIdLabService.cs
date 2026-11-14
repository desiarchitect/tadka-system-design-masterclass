using System.Buffers.Binary;
using System.Diagnostics;
using System.Security.Cryptography;
using Npgsql;
using NpgsqlTypes;

namespace Tadka.OrderApi.Modules.DistributedIds;

public sealed record IndexLocalityMetrics(
    string Strategy,
    int KeyBytes,
    int Rows,
    double InsertMs,
    long IndexSizeBytes,
    int TreeLevel,
    long LeafPages,
    long InternalPages,
    double AvgLeafDensityPct,
    double LeafFragmentationPct);

public sealed record IdBenchmarkResponse(
    int RowsPerTable,
    IndexLocalityMetrics UuidV4,
    IndexLocalityMetrics UuidV7,
    IndexLocalityMetrics Snowflake);

public sealed class DistributedIdLabService(NpgsqlDataSource db, SnowflakeIdGenerator snowflake)
{
    public object GetSample(int count)
    {
        var uuidV4 = new List<string>(count);
        var uuidV7 = new List<string>(count);
        var snowflakes = new List<SnowflakeParts>(count);

        for (var i = 0; i < count; i++)
        {
            uuidV4.Add(Guid.NewGuid().ToString());
            uuidV7.Add(Guid.CreateVersion7().ToString());
            snowflakes.Add(SnowflakeIdGenerator.Decode(snowflake.NextId()));
        }

        return new
        {
            count,
            nodeId = snowflake.NodeId,
            snowflakeLayout = new
            {
                signBits = 1,
                timestampBits = 41,
                nodeBits = SnowflakeIdGenerator.NodeBits,
                sequenceBits = SnowflakeIdGenerator.SequenceBits,
                epochUtc = DateTimeOffset.FromUnixTimeMilliseconds(SnowflakeIdGenerator.TadkaEpochMs),
            },
            uuidV4,
            uuidV7,
            snowflakes,
        };
    }

    public object RunClockRollbackCheck(long rollbackMs)
    {
        var simulatedNow = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var testGenerator = new SnowflakeIdGenerator(nodeId: 42, utcMsClock: () => simulatedNow);
        var idBeforeDrift = testGenerator.NextId();

        simulatedNow -= rollbackMs;

        try
        {
            _ = testGenerator.NextId();
            return new { blocked = false, rollbackMs, idBeforeDrift };
        }
        catch (ClockMovedBackwardsException ex)
        {
            return new
            {
                blocked = true,
                code = "clock_moved_backwards",
                rollbackMs,
                detectedDriftMs = ex.DriftMs,
                idBeforeDrift,
                message = ex.Message,
            };
        }
    }

    public async Task<IdBenchmarkResponse> RunBenchmarkAsync(int rows, CancellationToken ct)
    {
        await using (var cleanConn = await db.OpenConnectionAsync(ct))
        await using (var truncateCmd = cleanConn.CreateCommand())
        {
            truncateCmd.CommandText = "TRUNCATE TABLE id_bench_uuidv4, id_bench_uuidv7, id_bench_snowflake;";
            await truncateCmd.ExecuteNonQueryAsync(ct);
        }

        var v4Keys = new Guid[rows];
        var v7Keys = new Guid[rows];
        var sfKeys = new long[rows];
        var v7Gen = new MonotonicUuidV7Generator();
        for (var i = 0; i < rows; i++)
        {
            v4Keys[i] = Guid.NewGuid();
            v7Keys[i] = v7Gen.Next();
            sfKeys[i] = snowflake.NextId();
        }

        var v4InsertMs = await InsertGuidRowsAsync("id_bench_uuidv4", v4Keys, ct);
        var v7InsertMs = await InsertGuidRowsAsync("id_bench_uuidv7", v7Keys, ct);
        var sfInsertMs = await InsertSnowflakeRowsAsync(sfKeys, ct);

        await VacuumTablesAsync(ct);

        var v4Metrics = await ReadPgStatIndexAsync("uuidv4", 16, rows, v4InsertMs, "id_bench_uuidv4_pkey", ct);
        var v7Metrics = await ReadPgStatIndexAsync("uuidv7", 16, rows, v7InsertMs, "id_bench_uuidv7_pkey", ct);
        var sfMetrics = await ReadPgStatIndexAsync("snowflake", 8, rows, sfInsertMs, "id_bench_snowflake_pkey", ct);

        return new IdBenchmarkResponse(rows, v4Metrics, v7Metrics, sfMetrics);
    }

    private async Task<double> InsertGuidRowsAsync(string tableName, Guid[] keys, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        var sw = Stopwatch.StartNew();
        const int batchSize = 1000;

        for (var offset = 0; offset < keys.Length; offset += batchSize)
        {
            var count = Math.Min(batchSize, keys.Length - offset);
            var batchKeys = new Guid[count];
            var payloads = new string[count];
            for (var i = 0; i < count; i++)
            {
                batchKeys[i] = keys[offset + i];
                payloads[i] = $"order-event-{offset + i}";
            }

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"""
                INSERT INTO {tableName} (id, payload)
                SELECT k, p FROM UNNEST(@keys, @payloads) AS t(k, p);
                """;
            cmd.Parameters.Add(new NpgsqlParameter<Guid[]>("keys", NpgsqlDbType.Array | NpgsqlDbType.Uuid)
            {
                TypedValue = batchKeys,
            });
            cmd.Parameters.Add(new NpgsqlParameter<string[]>("payloads", NpgsqlDbType.Array | NpgsqlDbType.Text)
            {
                TypedValue = payloads,
            });
            await cmd.ExecuteNonQueryAsync(ct);
        }

        sw.Stop();
        return Math.Round(sw.Elapsed.TotalMilliseconds, 2);
    }

    private async Task<double> InsertSnowflakeRowsAsync(long[] keys, CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        var sw = Stopwatch.StartNew();
        const int batchSize = 1000;

        for (var offset = 0; offset < keys.Length; offset += batchSize)
        {
            var count = Math.Min(batchSize, keys.Length - offset);
            var batchKeys = new long[count];
            var payloads = new string[count];
            for (var i = 0; i < count; i++)
            {
                batchKeys[i] = keys[offset + i];
                payloads[i] = $"order-event-{offset + i}";
            }

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                INSERT INTO id_bench_snowflake (id, payload)
                SELECT k, p FROM UNNEST(@keys, @payloads) AS t(k, p);
                """;
            cmd.Parameters.Add(new NpgsqlParameter<long[]>("keys", NpgsqlDbType.Array | NpgsqlDbType.Bigint)
            {
                TypedValue = batchKeys,
            });
            cmd.Parameters.Add(new NpgsqlParameter<string[]>("payloads", NpgsqlDbType.Array | NpgsqlDbType.Text)
            {
                TypedValue = payloads,
            });
            await cmd.ExecuteNonQueryAsync(ct);
        }

        sw.Stop();
        return Math.Round(sw.Elapsed.TotalMilliseconds, 2);
    }

    private async Task VacuumTablesAsync(CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        foreach (var table in new[] { "id_bench_uuidv4", "id_bench_uuidv7", "id_bench_snowflake" })
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"VACUUM ANALYZE {table};";
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    private async Task<IndexLocalityMetrics> ReadPgStatIndexAsync(
        string strategy,
        int keyBytes,
        int rows,
        double insertMs,
        string indexName,
        CancellationToken ct)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT
                index_size,
                tree_level,
                leaf_pages,
                internal_pages,
                ROUND(avg_leaf_density::numeric, 2)::double precision,
                ROUND(leaf_fragmentation::numeric, 2)::double precision
            FROM pgstatindex(@indexName);
            """;
        cmd.Parameters.AddWithValue("indexName", indexName);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);

        return new IndexLocalityMetrics(
            Strategy: strategy,
            KeyBytes: keyBytes,
            Rows: rows,
            InsertMs: insertMs,
            IndexSizeBytes: reader.GetInt64(0),
            TreeLevel: reader.GetInt32(1),
            LeafPages: reader.GetInt64(2),
            InternalPages: reader.GetInt64(3),
            AvgLeafDensityPct: reader.GetDouble(4),
            LeafFragmentationPct: reader.GetDouble(5));
    }

    /// <summary>
    /// RFC 9562 Section 6.2 (Method 1) monotonic UUIDv7: 48-bit ms timestamp + 12-bit monotonic rand_a counter.
    /// Avoids intra-millisecond random page splits during multi-thousand/ms burst inserts.
    /// </summary>
    private sealed class MonotonicUuidV7Generator
    {
        private long _lastMs = -1L;
        private ushort _seq;

        public Guid Next()
        {
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            if (nowMs < _lastMs)
            {
                nowMs = _lastMs;
            }

            if (nowMs == _lastMs)
            {
                _seq++;
                if (_seq >= 4096)
                {
                    while (nowMs <= _lastMs)
                    {
                        nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    }
                    _lastMs = nowMs;
                    _seq = 0;
                }
            }
            else
            {
                _lastMs = nowMs;
                _seq = 0;
            }

            Span<byte> bytes = stackalloc byte[16];
            RandomNumberGenerator.Fill(bytes[8..]);

            // Bytes 0..5: 48-bit big-endian Unix ms timestamp
            Span<byte> ts = stackalloc byte[8];
            BinaryPrimitives.WriteInt64BigEndian(ts, _lastMs);
            ts[2..].CopyTo(bytes[..6]);

            // Bytes 6..7: 4-bit version (0x7) + 12-bit monotonic counter (rand_a)
            bytes[6] = (byte)(0x70 | ((_seq >> 8) & 0x0F));
            bytes[7] = (byte)(_seq & 0xFF);

            // Byte 8: 2-bit variant (10xx xxxx)
            bytes[8] = (byte)((bytes[8] & 0x3F) | 0x80);

            return new Guid(bytes, bigEndian: true);
        }
    }
}
