using Npgsql;

namespace Tadka.OrderApi.Modules.Catalog;

public sealed record FlashDealMvccState(
    long MenuItemId,
    string Title,
    int RemainingStock,
    long Version,
    string Xmin,
    string Xmax,
    string Ctid,
    long LiveTuples,
    long DeadTuples);

public sealed record ClaimFlashDealResult(
    bool Found,
    bool Success,
    string? ErrorCode,
    string? ErrorTitle,
    long MenuItemId,
    string Mode,
    int RemainingStock,
    long Version,
    int ConcurrencyWindowAppliedMs);

public sealed class FlashDealService(NpgsqlDataSource dataSource)
{
    public async Task<FlashDealMvccState?> ResetAsync(long menuItemId, int remainingStock, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("""
            UPDATE catalog.flash_deals
            SET remaining_stock = $2, version = 1
            WHERE menu_item_id = $1;
            """, connection);
        cmd.Parameters.AddWithValue(menuItemId);
        cmd.Parameters.AddWithValue(remainingStock);
        var rows = await cmd.ExecuteNonQueryAsync(ct);
        if (rows == 0)
            return null;

        return await InspectAsync(menuItemId, ct);
    }

    public async Task<FlashDealMvccState?> InspectAsync(long menuItemId, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        await using var cmd = new NpgsqlCommand("""
            SELECT
                menu_item_id,
                title,
                remaining_stock,
                version,
                xmin::text,
                xmax::text,
                ctid::text,
                COALESCE((SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname = 'catalog' AND relname = 'flash_deals'), 0),
                COALESCE((SELECT n_dead_tup FROM pg_stat_user_tables WHERE schemaname = 'catalog' AND relname = 'flash_deals'), 0)
            FROM catalog.flash_deals
            WHERE menu_item_id = $1;
            """, connection);
        cmd.Parameters.AddWithValue(menuItemId);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return null;

        return new FlashDealMvccState(
            reader.GetInt64(0),
            reader.GetString(1),
            reader.GetInt32(2),
            reader.GetInt64(3),
            reader.GetString(4),
            reader.GetString(5),
            reader.GetString(6),
            reader.GetInt64(7),
            reader.GetInt64(8));
    }

    public async Task<ClaimFlashDealResult> ClaimAsync(long menuItemId, string mode, CancellationToken ct)
    {
        return mode switch
        {
            "unsafe" => await ClaimUnsafeAsync(menuItemId, ct),
            "optimistic" => await ClaimOptimisticAsync(menuItemId, ct),
            "pessimistic" => await ClaimPessimisticAsync(menuItemId, ct),
            _ => throw new ArgumentException($"Unsupported concurrency mode '{mode}'.", nameof(mode)),
        };
    }

    private async Task<ClaimFlashDealResult> ClaimUnsafeAsync(long menuItemId, CancellationToken ct)
    {
        const int windowMs = 25;
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        int currentStock;
        long currentVersion;
        await using (var readCmd = new NpgsqlCommand("""
            SELECT remaining_stock, version
            FROM catalog.flash_deals
            WHERE menu_item_id = $1;
            """, connection))
        {
            readCmd.Parameters.AddWithValue(menuItemId);
            await using var reader = await readCmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
                return new(false, false, "not_found", "Flash deal not found.", menuItemId, "unsafe", 0, 0, 0);
            currentStock = reader.GetInt32(0);
            currentVersion = reader.GetInt64(1);
        }

        if (currentStock <= 0)
            return new(true, false, "out_of_stock", "Flash deal is out of stock.", menuItemId, "unsafe", 0, currentVersion, 0);

        // Disclosed concurrency alignment window so parallel read-modify-write callers overlap.
        await Task.Delay(windowMs, ct);

        var nextStock = currentStock - 1;
        await using var writeCmd = new NpgsqlCommand("""
            UPDATE catalog.flash_deals
            SET remaining_stock = $2, version = version + 1
            WHERE menu_item_id = $1
            RETURNING remaining_stock, version;
            """, connection);
        writeCmd.Parameters.AddWithValue(menuItemId);
        writeCmd.Parameters.AddWithValue(nextStock);
        await using var writeReader = await writeCmd.ExecuteReaderAsync(ct);
        await writeReader.ReadAsync(ct);

        return new(
            true,
            true,
            null,
            null,
            menuItemId,
            "unsafe",
            writeReader.GetInt32(0),
            writeReader.GetInt64(1),
            windowMs);
    }

    private async Task<ClaimFlashDealResult> ClaimOptimisticAsync(long menuItemId, CancellationToken ct)
    {
        const int windowMs = 25;
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        int currentStock;
        long currentVersion;
        await using (var readCmd = new NpgsqlCommand("""
            SELECT remaining_stock, version
            FROM catalog.flash_deals
            WHERE menu_item_id = $1;
            """, connection))
        {
            readCmd.Parameters.AddWithValue(menuItemId);
            await using var reader = await readCmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
                return new(false, false, "not_found", "Flash deal not found.", menuItemId, "optimistic", 0, 0, 0);
            currentStock = reader.GetInt32(0);
            currentVersion = reader.GetInt64(1);
        }

        if (currentStock <= 0)
            return new(true, false, "out_of_stock", "Flash deal is out of stock.", menuItemId, "optimistic", 0, currentVersion, 0);

        await Task.Delay(windowMs, ct);

        await using (var casCmd = new NpgsqlCommand("""
            UPDATE catalog.flash_deals
            SET remaining_stock = remaining_stock - 1, version = version + 1
            WHERE menu_item_id = $1 AND version = $2 AND remaining_stock > 0
            RETURNING remaining_stock, version;
            """, connection))
        {
            casCmd.Parameters.AddWithValue(menuItemId);
            casCmd.Parameters.AddWithValue(currentVersion);
            await using var casReader = await casCmd.ExecuteReaderAsync(ct);
            if (await casReader.ReadAsync(ct))
            {
                return new(
                    true,
                    true,
                    null,
                    null,
                    menuItemId,
                    "optimistic",
                    casReader.GetInt32(0),
                    casReader.GetInt64(1),
                    windowMs);
            }
        }

        await using var checkCmd = new NpgsqlCommand("""
            SELECT remaining_stock, version
            FROM catalog.flash_deals
            WHERE menu_item_id = $1;
            """, connection);
        checkCmd.Parameters.AddWithValue(menuItemId);
        await using var checkReader = await checkCmd.ExecuteReaderAsync(ct);
        await checkReader.ReadAsync(ct);
        var latestStock = checkReader.GetInt32(0);
        var latestVersion = checkReader.GetInt64(1);

        if (latestStock <= 0)
        {
            return new(
                true,
                false,
                "out_of_stock",
                "Flash deal is out of stock.",
                menuItemId,
                "optimistic",
                latestStock,
                latestVersion,
                windowMs);
        }

        return new(
            true,
            false,
            "optimistic_version_conflict",
            "Concurrent update changed the deal version. Retry with the latest state.",
            menuItemId,
            "optimistic",
            latestStock,
            latestVersion,
            windowMs);
    }

    private async Task<ClaimFlashDealResult> ClaimPessimisticAsync(long menuItemId, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);

        int lockedStock;
        long lockedVersion;
        await using (var lockCmd = new NpgsqlCommand("""
            SELECT remaining_stock, version
            FROM catalog.flash_deals
            WHERE menu_item_id = $1
            FOR UPDATE;
            """, connection, transaction))
        {
            lockCmd.Parameters.AddWithValue(menuItemId);
            await using var reader = await lockCmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
                return new(false, false, "not_found", "Flash deal not found.", menuItemId, "pessimistic", 0, 0, 0);
            lockedStock = reader.GetInt32(0);
            lockedVersion = reader.GetInt64(1);
        }

        if (lockedStock <= 0)
        {
            await transaction.RollbackAsync(ct);
            return new(true, false, "out_of_stock", "Flash deal is out of stock.", menuItemId, "pessimistic", 0, lockedVersion, 0);
        }

        await using var updateCmd = new NpgsqlCommand("""
            UPDATE catalog.flash_deals
            SET remaining_stock = remaining_stock - 1, version = version + 1
            WHERE menu_item_id = $1
            RETURNING remaining_stock, version;
            """, connection, transaction);
        updateCmd.Parameters.AddWithValue(menuItemId);

        int updatedStock;
        long updatedVersion;
        await using (var updateReader = await updateCmd.ExecuteReaderAsync(ct))
        {
            await updateReader.ReadAsync(ct);
            updatedStock = updateReader.GetInt32(0);
            updatedVersion = updateReader.GetInt64(1);
        }

        await transaction.CommitAsync(ct);
        return new(true, true, null, null, menuItemId, "pessimistic", updatedStock, updatedVersion, 0);
    }
}
