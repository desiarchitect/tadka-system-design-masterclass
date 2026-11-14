using Npgsql;

namespace Tadka.OrderApi.Modules.Replication;

public sealed class ReplicaDataSource(NpgsqlDataSource dataSource) : IAsyncDisposable
{
    public NpgsqlDataSource DataSource { get; } = dataSource;
    public ValueTask DisposeAsync() => DataSource.DisposeAsync();
}

public sealed record ReplicationPeerStatus(
    string ApplicationName,
    string State,
    string SyncState,
    string? SentLsn,
    string? WriteLsn,
    string? FlushLsn,
    string? ReplayLsn);

public sealed record ReplicationStatusResponse(
    string PrimaryCurrentLsn,
    bool ReplicaInRecovery,
    bool ReplicaWalReplayPaused,
    string ReplicaReceiveLsn,
    string ReplicaReplayLsn,
    long ReplayLagBytes,
    IReadOnlyList<ReplicationPeerStatus> ConnectedReplicas);

public sealed record ReplayToggleResponse(
    bool Paused,
    bool ReplicaWalReplayPaused,
    string ReplicaReceiveLsn,
    string ReplicaReplayLsn);

public sealed record LsnWriteOrderResponse(
    long Id,
    long CustomerId,
    long RestaurantId,
    decimal TotalAmount,
    string Status,
    long StatusVersion,
    string CommitLsn);

public sealed record ReadYourWritesOrderResponse(
    bool Found,
    string RoutingMode,
    string ServedFrom,
    string Reason,
    string? RequestedMinLsn,
    string ReplicaReplayLsn,
    bool StaleRelativeToMinLsn,
    long? Id,
    long? CustomerId,
    long? RestaurantId,
    decimal? TotalAmount,
    string? Status,
    long? StatusVersion);

public sealed class ReplicaLagLabService(NpgsqlDataSource primary, ReplicaDataSource replica)
{
    public async Task<ReplicationStatusResponse> GetStatusAsync(CancellationToken ct)
    {
        string primaryLsn;
        var peers = new List<ReplicationPeerStatus>();

        await using (var pConn = await primary.OpenConnectionAsync(ct))
        {
            await using (var cmd = pConn.CreateCommand())
            {
                cmd.CommandText = "SELECT pg_current_wal_lsn()::text;";
                primaryLsn = (string)(await cmd.ExecuteScalarAsync(ct))!;
            }

            await using (var repCmd = pConn.CreateCommand())
            {
                repCmd.CommandText = """
                    SELECT
                        application_name,
                        state,
                        sync_state,
                        sent_lsn::text,
                        write_lsn::text,
                        flush_lsn::text,
                        replay_lsn::text
                    FROM pg_stat_replication;
                    """;
                await using var reader = await repCmd.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    peers.Add(new ReplicationPeerStatus(
                        reader.GetString(0),
                        reader.GetString(1),
                        reader.GetString(2),
                        reader.IsDBNull(3) ? null : reader.GetString(3),
                        reader.IsDBNull(4) ? null : reader.GetString(4),
                        reader.IsDBNull(5) ? null : reader.GetString(5),
                        reader.IsDBNull(6) ? null : reader.GetString(6)));
                }
            }
        }

        bool inRecovery;
        bool replayPaused;
        string receiveLsn;
        string replayLsn;

        await using (var rConn = await replica.DataSource.OpenConnectionAsync(ct))
        await using (var cmd = rConn.CreateCommand())
        {
            cmd.CommandText = """
                SELECT
                    pg_is_in_recovery(),
                    pg_is_wal_replay_paused(),
                    COALESCE(pg_last_wal_receive_lsn()::text, '0/0'),
                    COALESCE(pg_last_wal_replay_lsn()::text, '0/0');
                """;
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            inRecovery = reader.GetBoolean(0);
            replayPaused = reader.GetBoolean(1);
            receiveLsn = reader.GetString(2);
            replayLsn = reader.GetString(3);
        }

        long replayLagBytes;
        await using (var pConn = await primary.OpenConnectionAsync(ct))
        await using (var diffCmd = pConn.CreateCommand())
        {
            diffCmd.CommandText = "SELECT GREATEST(0, pg_wal_lsn_diff(@primary::pg_lsn, @replay::pg_lsn)::bigint);";
            diffCmd.Parameters.AddWithValue("primary", primaryLsn);
            diffCmd.Parameters.AddWithValue("replay", replayLsn);
            replayLagBytes = (long)(await diffCmd.ExecuteScalarAsync(ct))!;
        }

        return new ReplicationStatusResponse(
            primaryLsn,
            inRecovery,
            replayPaused,
            receiveLsn,
            replayLsn,
            replayLagBytes,
            peers);
    }

    public async Task<ReplayToggleResponse> SetWalReplayPausedAsync(bool paused, CancellationToken ct)
    {
        await using var rConn = await replica.DataSource.OpenConnectionAsync(ct);
        await using (var toggleCmd = rConn.CreateCommand())
        {
            toggleCmd.CommandText = paused ? "SELECT pg_wal_replay_pause();" : "SELECT pg_wal_replay_resume();";
            await toggleCmd.ExecuteNonQueryAsync(ct);
        }

        await using var checkCmd = rConn.CreateCommand();
        checkCmd.CommandText = """
            SELECT
                pg_is_wal_replay_paused(),
                COALESCE(pg_last_wal_receive_lsn()::text, '0/0'),
                COALESCE(pg_last_wal_replay_lsn()::text, '0/0');
            """;
        await using var reader = await checkCmd.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);

        return new ReplayToggleResponse(
            Paused: paused,
            ReplicaWalReplayPaused: reader.GetBoolean(0),
            ReplicaReceiveLsn: reader.GetString(1),
            ReplicaReplayLsn: reader.GetString(2));
    }

    public async Task<object> ProbeReplicaReadOnlyAsync(CancellationToken ct)
    {
        await using var rConn = await replica.DataSource.OpenConnectionAsync(ct);
        await using var cmd = rConn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO orders (customer_id, restaurant_id, total_amount, status)
            VALUES (999999, 7, 100.00, 'placed');
            """;

        try
        {
            await cmd.ExecuteNonQueryAsync(ct);
            return new { blocked = false };
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.ReadOnlySqlTransaction)
        {
            return new
            {
                blocked = true,
                sqlState = ex.SqlState,
                message = ex.MessageText,
            };
        }
    }

    public async Task<LsnWriteOrderResponse> CreateOrderWithLsnAsync(
        long customerId,
        long restaurantId,
        decimal totalAmount,
        CancellationToken ct)
    {
        await using var conn = await primary.OpenConnectionAsync(ct);

        long id;
        string status;
        long statusVersion;

        await using (var insertCmd = conn.CreateCommand())
        {
            insertCmd.CommandText = """
                INSERT INTO orders (customer_id, restaurant_id, total_amount, status, status_version)
                VALUES (@customerId, @restaurantId, @totalAmount, 'placed', 0)
                RETURNING id, status, status_version;
                """;
            insertCmd.Parameters.AddWithValue("customerId", customerId);
            insertCmd.Parameters.AddWithValue("restaurantId", restaurantId);
            insertCmd.Parameters.AddWithValue("totalAmount", totalAmount);

            await using var reader = await insertCmd.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            id = reader.GetInt64(0);
            status = reader.GetString(1);
            statusVersion = reader.GetInt64(2);
        }

        // Read post-commit WAL position after INSERT auto-commit completes.
        await using var lsnCmd = conn.CreateCommand();
        lsnCmd.CommandText = "SELECT pg_current_wal_lsn()::text;";
        var commitLsn = (string)(await lsnCmd.ExecuteScalarAsync(ct))!;

        return new LsnWriteOrderResponse(id, customerId, restaurantId, totalAmount, status, statusVersion, commitLsn);
    }

    public async Task<LsnWriteOrderResponse?> UpdateStatusWithLsnAsync(long orderId, string status, CancellationToken ct)
    {
        await using var conn = await primary.OpenConnectionAsync(ct);

        long customerId;
        long restaurantId;
        decimal totalAmount;
        string updatedStatus;
        long statusVersion;

        await using (var updateCmd = conn.CreateCommand())
        {
            updateCmd.CommandText = """
                UPDATE orders
                SET status = @status,
                    status_version = status_version + 1
                WHERE id = @orderId
                RETURNING customer_id, restaurant_id, total_amount, status, status_version;
                """;
            updateCmd.Parameters.AddWithValue("orderId", orderId);
            updateCmd.Parameters.AddWithValue("status", status);

            await using var reader = await updateCmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
                return null;

            customerId = reader.GetInt64(0);
            restaurantId = reader.GetInt64(1);
            totalAmount = reader.GetDecimal(2);
            updatedStatus = reader.GetString(3);
            statusVersion = reader.GetInt64(4);
        }

        await using var lsnCmd = conn.CreateCommand();
        lsnCmd.CommandText = "SELECT pg_current_wal_lsn()::text;";
        var commitLsn = (string)(await lsnCmd.ExecuteScalarAsync(ct))!;

        return new LsnWriteOrderResponse(orderId, customerId, restaurantId, totalAmount, updatedStatus, statusVersion, commitLsn);
    }

    public async Task<(bool ValidLsn, ReadYourWritesOrderResponse? Response)> ReadOrderWithRoutingAsync(
        long orderId,
        string routing,
        string? minLsn,
        CancellationToken ct)
    {
        string replicaReplayLsn;
        bool caughtUp = true;

        await using (var rConn = await replica.DataSource.OpenConnectionAsync(ct))
        await using (var lsnCmd = rConn.CreateCommand())
        {
            if (string.IsNullOrWhiteSpace(minLsn))
            {
                lsnCmd.CommandText = "SELECT COALESCE(pg_last_wal_replay_lsn()::text, '0/0'), TRUE;";
            }
            else
            {
                lsnCmd.CommandText = """
                    SELECT
                        COALESCE(pg_last_wal_replay_lsn()::text, '0/0'),
                        COALESCE(pg_last_wal_replay_lsn() >= @minLsn::pg_lsn, FALSE);
                    """;
                lsnCmd.Parameters.AddWithValue("minLsn", minLsn.Trim());
            }

            try
            {
                await using var reader = await lsnCmd.ExecuteReaderAsync(ct);
                await reader.ReadAsync(ct);
                replicaReplayLsn = reader.GetString(0);
                caughtUp = reader.GetBoolean(1);
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InvalidTextRepresentation)
            {
                return (false, null);
            }
        }

        var usePrimaryFallback = routing == "ryow" && !string.IsNullOrWhiteSpace(minLsn) && !caughtUp;
        var source = usePrimaryFallback ? primary : replica.DataSource;
        var servedFrom = usePrimaryFallback ? "primary-fallback" : "replica";
        var reason = usePrimaryFallback
            ? "replica_behind_min_lsn"
            : string.IsNullOrWhiteSpace(minLsn)
                ? "no_min_lsn_requested"
                : caughtUp
                    ? "replica_at_or_ahead_of_min_lsn"
                    : "replica_only_routing_ignores_lag";

        await using var readConn = await source.OpenConnectionAsync(ct);
        await using var readCmd = readConn.CreateCommand();
        readCmd.CommandText = """
            SELECT id, customer_id, restaurant_id, total_amount, status, status_version
            FROM orders
            WHERE id = @orderId;
            """;
        readCmd.Parameters.AddWithValue("orderId", orderId);

        await using var orderReader = await readCmd.ExecuteReaderAsync(ct);
        if (!await orderReader.ReadAsync(ct))
        {
            return (true, new ReadYourWritesOrderResponse(
                Found: false,
                RoutingMode: routing,
                ServedFrom: servedFrom,
                Reason: reason,
                RequestedMinLsn: minLsn,
                ReplicaReplayLsn: replicaReplayLsn,
                StaleRelativeToMinLsn: !usePrimaryFallback && !caughtUp,
                Id: null,
                CustomerId: null,
                RestaurantId: null,
                TotalAmount: null,
                Status: null,
                StatusVersion: null));
        }

        return (true, new ReadYourWritesOrderResponse(
            Found: true,
            RoutingMode: routing,
            ServedFrom: servedFrom,
            Reason: reason,
            RequestedMinLsn: minLsn,
            ReplicaReplayLsn: replicaReplayLsn,
            StaleRelativeToMinLsn: !usePrimaryFallback && !caughtUp,
            Id: orderReader.GetInt64(0),
            CustomerId: orderReader.GetInt64(1),
            RestaurantId: orderReader.GetInt64(2),
            TotalAmount: orderReader.GetDecimal(3),
            Status: orderReader.GetString(4),
            StatusVersion: orderReader.GetInt64(5)));
    }
}
