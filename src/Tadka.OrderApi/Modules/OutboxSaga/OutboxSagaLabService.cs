using System.Text.Json;
using Confluent.Kafka;
using Npgsql;
using Tadka.OrderApi.Modules.Messaging;

namespace Tadka.OrderApi.Modules.OutboxSaga;

public sealed record OutboxResetResponse(
    string Status,
    string OutboxTopic,
    int FlashDealStock,
    long OutboxPendingCount,
    long SagaCount);

public sealed record DualWriteResponse(
    string Mode,
    long OrderId,
    string EventId,
    bool OrderInDatabase,
    bool OutboxRowInDatabase,
    bool OutboxPublished,
    bool EventInKafka,
    string Inconsistency);

public sealed record OutboxRelayResponse(
    string Topic,
    bool CrashSimulatedAfterPublish,
    int RelayedCount,
    long PendingOutboxRemaining,
    IReadOnlyList<string> RelayedEventIds,
    int KafkaMessageCountForRelayedEvents,
    int UniqueDownstreamEffects,
    int DuplicatesSuppressed,
    int LoyaltyPointsAwarded);

public sealed record SagaExecutionResponse(
    string SagaId,
    long OrderId,
    long MenuItemId,
    string Scenario,
    string CurrentState,
    string OrderStatus,
    int InitialStock,
    int RemainingStock,
    int StockAfterReplay,
    bool InventoryReserved,
    bool InventoryCompensated,
    bool PaymentCaptured,
    bool PaymentRefunded,
    bool KitchenAccepted,
    int CompensationAttempts,
    string Inconsistency,
    IReadOnlyList<string> StepLog);

public sealed class OutboxSagaLabService(
    IConfiguration configuration,
    NpgsqlDataSource dataSource,
    ILogger<OutboxSagaLabService> logger)
{
    private const string OutboxDownstreamGroup = "outbox-downstream-group";
    private const long OutboxDemoCustomerId = 701;
    private const long FlashDealMenuItemId = 101;
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private string BootstrapServers => configuration["Kafka:BootstrapServers"] ?? "127.0.0.1:9092";
    private string OutboxTopic => configuration["Kafka:OutboxTopic"] ?? "tadka.order.outbox";

    public async Task<OutboxResetResponse> ResetAsync(CancellationToken ct)
    {
        await using (var conn = await dataSource.OpenConnectionAsync(ct))
        await using (var cmd = new NpgsqlCommand(
            """
            TRUNCATE TABLE outbox_events, saga_executions RESTART IDENTITY;
            DELETE FROM processed_consumer_events WHERE consumer_group = @groupName;
            DELETE FROM consumer_loyalty_ledger WHERE customer_id = @customerId;
            DELETE FROM orders WHERE idempotency_key LIKE 'outbox-lab-%' OR idempotency_key LIKE 'saga-lab-%';
            UPDATE catalog.flash_deals
               SET remaining_stock = 5,
                   version = 1
             WHERE menu_item_id = @menuItemId;
            """,
            conn))
        {
            cmd.Parameters.AddWithValue("groupName", OutboxDownstreamGroup);
            cmd.Parameters.AddWithValue("customerId", OutboxDemoCustomerId);
            cmd.Parameters.AddWithValue("menuItemId", FlashDealMenuItemId);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        await KafkaTopicAdmin.RecreateTopicAsync(BootstrapServers, OutboxTopic, partitions: 1, ct);

        logger.LogInformation("outbox_saga_reset topic={Topic}", OutboxTopic);

        return new OutboxResetResponse(
            Status: "RESET",
            OutboxTopic: OutboxTopic,
            FlashDealStock: 5,
            OutboxPendingCount: 0,
            SagaCount: 0);
    }

    public async Task<DualWriteResponse> ExecuteDualWriteAsync(string mode, CancellationToken ct)
    {
        var normalized = string.IsNullOrWhiteSpace(mode)
            ? "transactional-outbox"
            : mode.Trim().ToLowerInvariant();
        if (normalized is not ("db-first-broker-fails" or "broker-first-db-fails" or "transactional-outbox"))
            throw new ArgumentException("mode must be 'db-first-broker-fails', 'broker-first-db-fails', or 'transactional-outbox'.");

        await KafkaTopicAdmin.EnsureTopicAsync(BootstrapServers, OutboxTopic, partitions: 1, ct);

        var tag = Guid.NewGuid().ToString("N")[..10];
        var eventId = $"evt-outbox-{normalized}-{tag}";
        long orderId = 0;

        if (normalized == "db-first-broker-fails")
        {
            await using (var conn = await dataSource.OpenConnectionAsync(ct))
            await using (var tx = await conn.BeginTransactionAsync(ct))
            {
                orderId = await InsertLabOrderAsync(conn, tx, $"outbox-lab-dbfirst-{tag}", "placed", ct);
                await tx.CommitAsync(ct);
            }

            // Simulate Kafka publish failure AFTER PostgreSQL transaction already committed.
            try
            {
                throw new InvalidOperationException("Simulated Kafka broker network timeout after PostgreSQL commit.");
            }
            catch (InvalidOperationException ex)
            {
                logger.LogWarning("dual_write_db_first_broker_failed order_id={OrderId} reason={Reason}", orderId, ex.Message);
            }
        }
        else if (normalized == "broker-first-db-fails")
        {
            orderId = 900_000 + Math.Abs(tag.GetHashCode() % 90_000);

            using (var producer = BuildProducer())
            {
                var payload = JsonSerializer.Serialize(new
                {
                    eventId,
                    aggregateType = "Order",
                    aggregateId = orderId,
                    eventType = "OrderCreated",
                    totalAmount = 340.00m
                }, JsonOptions);

                await producer.ProduceAsync(OutboxTopic, new Message<string, string>
                {
                    Key = $"order-{orderId}",
                    Value = payload
                }, ct);
                producer.Flush(ct);
            }

            await using var conn = await dataSource.OpenConnectionAsync(ct);
            await using var tx = await conn.BeginTransactionAsync(ct);
            await using (var insertCmd = new NpgsqlCommand(
                """
                INSERT INTO orders (id, customer_id, restaurant_id, total_amount, idempotency_key, status)
                VALUES (@id, @customerId, 7, 340.00, @idem, 'placed');
                """,
                conn,
                tx))
            {
                insertCmd.Parameters.AddWithValue("id", orderId);
                insertCmd.Parameters.AddWithValue("customerId", OutboxDemoCustomerId);
                insertCmd.Parameters.AddWithValue("idem", $"outbox-lab-brokerfirst-{tag}");
                await insertCmd.ExecuteNonQueryAsync(ct);
            }

            // Simulate post-publish database failure -> SQL transaction rolls back while Kafka already holds the message.
            await tx.RollbackAsync(ct);
            logger.LogWarning("dual_write_broker_first_db_rolled_back order_id={OrderId} event_id={EventId}", orderId, eventId);
        }
        else
        {
            await using var conn = await dataSource.OpenConnectionAsync(ct);
            await using var tx = await conn.BeginTransactionAsync(ct);

            orderId = await InsertLabOrderAsync(conn, tx, $"outbox-lab-atomic-{tag}", "placed", ct);
            var payload = JsonSerializer.Serialize(new
            {
                eventId,
                orderId,
                customerId = OutboxDemoCustomerId,
                restaurantId = 7L,
                totalAmount = 340.00m
            }, JsonOptions);

            await using (var outboxCmd = new NpgsqlCommand(
                """
                INSERT INTO outbox_events (event_id, aggregate_type, aggregate_id, event_type, payload, published_at, publish_attempts)
                VALUES (@eventId, 'Order', @aggregateId, 'OrderCreated', @payload::jsonb, NULL, 0);
                """,
                conn,
                tx))
            {
                outboxCmd.Parameters.AddWithValue("eventId", eventId);
                outboxCmd.Parameters.AddWithValue("aggregateId", orderId);
                outboxCmd.Parameters.AddWithValue("payload", payload);
                await outboxCmd.ExecuteNonQueryAsync(ct);
            }

            await tx.CommitAsync(ct);
            logger.LogInformation("transactional_outbox_committed order_id={OrderId} event_id={EventId}", orderId, eventId);
        }

        var (orderInDb, outboxInDb, outboxPublished) = await InspectDbStateAsync(orderId, eventId, ct);
        var eventInKafka = CountMatchingEventsOnKafka(new HashSet<string>(StringComparer.Ordinal) { eventId }) > 0;

        var inconsistency = (orderInDb, outboxInDb, eventInKafka) switch
        {
            (true, false, false) => "GHOST_ORDER_NO_EVENT",
            (false, false, true) => "PHANTOM_EVENT_NO_ORDER",
            (true, true, false) => "NONE_PENDING_RELAY",
            _ => "UNEXPECTED"
        };

        return new DualWriteResponse(
            Mode: normalized,
            OrderId: orderId,
            EventId: eventId,
            OrderInDatabase: orderInDb,
            OutboxRowInDatabase: outboxInDb,
            OutboxPublished: outboxPublished,
            EventInKafka: eventInKafka,
            Inconsistency: inconsistency);
    }

    public async Task<OutboxRelayResponse> RelayAsync(bool simulateCrashAfterPublishBeforeMark, CancellationToken ct)
    {
        await KafkaTopicAdmin.EnsureTopicAsync(BootstrapServers, OutboxTopic, partitions: 1, ct);

        var relayedIds = new List<string>();
        var pendingRows = new List<(long Id, string EventId, long AggregateId, string EventType, string Payload)>();

        await using (var conn = await dataSource.OpenConnectionAsync(ct))
        await using (var tx = await conn.BeginTransactionAsync(ct))
        {
            await using (var selectCmd = new NpgsqlCommand(
                """
                SELECT id, event_id, aggregate_id, event_type, payload::text
                FROM outbox_events
                WHERE published_at IS NULL
                ORDER BY created_at ASC, id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 20;
                """,
                conn,
                tx))
            await using (var reader = await selectCmd.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    pendingRows.Add((
                        reader.GetInt64(0),
                        reader.GetString(1),
                        reader.GetInt64(2),
                        reader.GetString(3),
                        reader.GetString(4)));
                }
            }

            using var producer = BuildProducer();
            foreach (var row in pendingRows)
            {
                var envelope = JsonSerializer.Serialize(new
                {
                    eventId = row.EventId,
                    aggregateType = "Order",
                    aggregateId = row.AggregateId,
                    eventType = row.EventType,
                    payload = JsonDocument.Parse(row.Payload).RootElement
                }, JsonOptions);

                await producer.ProduceAsync(OutboxTopic, new Message<string, string>
                {
                    Key = $"order-{row.AggregateId}",
                    Value = envelope
                }, ct);

                relayedIds.Add(row.EventId);
            }
            producer.Flush(ct);

            foreach (var row in pendingRows)
            {
                if (simulateCrashAfterPublishBeforeMark)
                {
                    await using var bumpCmd = new NpgsqlCommand(
                        "UPDATE outbox_events SET publish_attempts = publish_attempts + 1 WHERE id = @id;",
                        conn,
                        tx);
                    bumpCmd.Parameters.AddWithValue("id", row.Id);
                    await bumpCmd.ExecuteNonQueryAsync(ct);
                }
                else
                {
                    await using var markCmd = new NpgsqlCommand(
                        """
                        UPDATE outbox_events
                           SET published_at = NOW(),
                               publish_attempts = publish_attempts + 1
                         WHERE id = @id;
                        """,
                        conn,
                        tx);
                    markCmd.Parameters.AddWithValue("id", row.Id);
                    await markCmd.ExecuteNonQueryAsync(ct);
                }
            }

            await tx.CommitAsync(ct);
        }

        if (simulateCrashAfterPublishBeforeMark)
            logger.LogWarning("outbox_relay_simulated_crash_before_mark count={Count}", relayedIds.Count);
        else
            logger.LogInformation("outbox_relay_published count={Count}", relayedIds.Count);

        var targetSet = relayedIds.ToHashSet(StringComparer.Ordinal);
        var (kafkaMatches, suppressed, uniqueEffects) = await ConsumeAndDeduplicateOutboxTopicAsync(targetSet, ct);

        long remainingPending = 0;
        int loyaltyPoints = 0;
        await using (var conn = await dataSource.OpenConnectionAsync(ct))
        {
            await using (var remCmd = new NpgsqlCommand(
                "SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL;",
                conn))
            {
                remainingPending = (long)(await remCmd.ExecuteScalarAsync(ct) ?? 0L);
            }

            await using (var ptsCmd = new NpgsqlCommand(
                "SELECT COALESCE(MAX(points_balance), 0) FROM consumer_loyalty_ledger WHERE customer_id = @customerId;",
                conn))
            {
                ptsCmd.Parameters.AddWithValue("customerId", OutboxDemoCustomerId);
                loyaltyPoints = Convert.ToInt32(await ptsCmd.ExecuteScalarAsync(ct) ?? 0);
            }
        }

        return new OutboxRelayResponse(
            Topic: OutboxTopic,
            CrashSimulatedAfterPublish: simulateCrashAfterPublishBeforeMark,
            RelayedCount: relayedIds.Count,
            PendingOutboxRemaining: remainingPending,
            RelayedEventIds: relayedIds,
            KafkaMessageCountForRelayedEvents: kafkaMatches,
            UniqueDownstreamEffects: uniqueEffects,
            DuplicatesSuppressed: suppressed,
            LoyaltyPointsAwarded: loyaltyPoints);
    }

    public async Task<SagaExecutionResponse> ExecuteSagaAsync(string scenario, CancellationToken ct)
    {
        var normalized = string.IsNullOrWhiteSpace(scenario)
            ? "happy-path"
            : scenario.Trim().ToLowerInvariant();
        if (normalized is not ("happy-path" or "kitchen-rejected-uncompensated" or "kitchen-rejected-compensated"))
            throw new ArgumentException("scenario must be 'happy-path', 'kitchen-rejected-uncompensated', or 'kitchen-rejected-compensated'.");

        const int initialStock = 5;
        var tag = Guid.NewGuid().ToString("N")[..10];
        var stepLog = new List<string>();
        long orderId;
        string sagaId;

        // Ensure deterministic starting stock before this scenario executes.
        await using (var conn = await dataSource.OpenConnectionAsync(ct))
        {
            await using (var resetStockCmd = new NpgsqlCommand(
                """
                UPDATE catalog.flash_deals
                   SET remaining_stock = 5,
                       version = 1
                 WHERE menu_item_id = @menuItemId;
                """,
                conn))
            {
                resetStockCmd.Parameters.AddWithValue("menuItemId", FlashDealMenuItemId);
                await resetStockCmd.ExecuteNonQueryAsync(ct);
            }

            await using (var tx = await conn.BeginTransactionAsync(ct))
            {
                orderId = await InsertLabOrderAsync(conn, tx, $"saga-lab-{normalized}-{tag}", "saga_started", ct);
                sagaId = $"saga-{orderId}-{tag}";

                await using var sagaInitCmd = new NpgsqlCommand(
                    """
                    INSERT INTO saga_executions (saga_id, order_id, menu_item_id, quantity, scenario, current_state)
                    VALUES (@sagaId, @orderId, @menuItemId, 1, @scenario, 'STARTED');
                    """,
                    conn,
                    tx);
                sagaInitCmd.Parameters.AddWithValue("sagaId", sagaId);
                sagaInitCmd.Parameters.AddWithValue("orderId", orderId);
                sagaInitCmd.Parameters.AddWithValue("menuItemId", FlashDealMenuItemId);
                sagaInitCmd.Parameters.AddWithValue("scenario", normalized);
                await sagaInitCmd.ExecuteNonQueryAsync(ct);

                await tx.CommitAsync(ct);
            }
        }

        // Step 1: Inventory reserve (5 -> 4)
        int stockAfterReserve;
        await using (var conn = await dataSource.OpenConnectionAsync(ct))
        await using (var tx = await conn.BeginTransactionAsync(ct))
        {
            await using (var invCmd = new NpgsqlCommand(
                """
                UPDATE catalog.flash_deals
                   SET remaining_stock = remaining_stock - 1,
                       version = version + 1
                 WHERE menu_item_id = @menuItemId AND remaining_stock >= 1
                 RETURNING remaining_stock;
                """,
                conn,
                tx))
            {
                invCmd.Parameters.AddWithValue("menuItemId", FlashDealMenuItemId);
                var scalar = await invCmd.ExecuteScalarAsync(ct)
                    ?? throw new InvalidOperationException("Flash deal stock exhausted unexpectedly.");
                stockAfterReserve = Convert.ToInt32(scalar);
            }

            stepLog.Add($"STEP_1_INVENTORY_RESERVED (stock {initialStock} -> {stockAfterReserve})");

            await using (var updateSagaCmd = new NpgsqlCommand(
                """
                UPDATE saga_executions
                   SET current_state = 'INVENTORY_RESERVED',
                       inventory_reserved = TRUE,
                       step_log = @stepLog::jsonb,
                       updated_at = NOW()
                 WHERE saga_id = @sagaId;
                """,
                conn,
                tx))
            {
                updateSagaCmd.Parameters.AddWithValue("sagaId", sagaId);
                updateSagaCmd.Parameters.AddWithValue("stepLog", JsonSerializer.Serialize(stepLog, JsonOptions));
                await updateSagaCmd.ExecuteNonQueryAsync(ct);
            }

            await tx.CommitAsync(ct);
        }

        // Step 2: Payment capture
        stepLog.Add("STEP_2_PAYMENT_CAPTURED (340.00 INR)");
        await using (var conn = await dataSource.OpenConnectionAsync(ct))
        await using (var cmd = new NpgsqlCommand(
            """
            UPDATE saga_executions
               SET current_state = 'PAYMENT_CAPTURED',
                   payment_captured = TRUE,
                   step_log = @stepLog::jsonb,
                   updated_at = NOW()
             WHERE saga_id = @sagaId;
            """,
            conn))
        {
            cmd.Parameters.AddWithValue("sagaId", sagaId);
            cmd.Parameters.AddWithValue("stepLog", JsonSerializer.Serialize(stepLog, JsonOptions));
            await cmd.ExecuteNonQueryAsync(ct);
        }

        // Step 3: Kitchen decision
        if (normalized == "happy-path")
        {
            stepLog.Add("STEP_3_KITCHEN_ACCEPTED");
            await using var conn = await dataSource.OpenConnectionAsync(ct);
            await using var tx = await conn.BeginTransactionAsync(ct);

            await using (var cmd1 = new NpgsqlCommand(
                """
                UPDATE saga_executions
                   SET current_state = 'COMPLETED',
                       kitchen_accepted = TRUE,
                       step_log = @stepLog::jsonb,
                       updated_at = NOW()
                 WHERE saga_id = @sagaId;
                """,
                conn,
                tx))
            {
                cmd1.Parameters.AddWithValue("sagaId", sagaId);
                cmd1.Parameters.AddWithValue("stepLog", JsonSerializer.Serialize(stepLog, JsonOptions));
                await cmd1.ExecuteNonQueryAsync(ct);
            }

            await using (var cmd2 = new NpgsqlCommand(
                "UPDATE orders SET status = 'confirmed', status_version = status_version + 1 WHERE id = @orderId;",
                conn,
                tx))
            {
                cmd2.Parameters.AddWithValue("orderId", orderId);
                await cmd2.ExecuteNonQueryAsync(ct);
            }

            await tx.CommitAsync(ct);

            return await BuildSagaResponseAsync(sagaId, orderId, normalized, initialStock, stockAfterReserve, stepLog, "NONE", ct);
        }

        // Kitchen rejects!
        stepLog.Add("STEP_3_KITCHEN_REJECTED (OUT_OF_PANEER)");

        if (normalized == "kitchen-rejected-uncompensated")
        {
            await using var conn = await dataSource.OpenConnectionAsync(ct);
            await using var tx = await conn.BeginTransactionAsync(ct);

            await using (var cmd1 = new NpgsqlCommand(
                """
                UPDATE saga_executions
                   SET current_state = 'FAILED_UNCOMPENSATED',
                       kitchen_accepted = FALSE,
                       step_log = @stepLog::jsonb,
                       updated_at = NOW()
                 WHERE saga_id = @sagaId;
                """,
                conn,
                tx))
            {
                cmd1.Parameters.AddWithValue("sagaId", sagaId);
                cmd1.Parameters.AddWithValue("stepLog", JsonSerializer.Serialize(stepLog, JsonOptions));
                await cmd1.ExecuteNonQueryAsync(ct);
            }

            await using (var cmd2 = new NpgsqlCommand(
                "UPDATE orders SET status = 'failed_uncompensated', status_version = status_version + 1 WHERE id = @orderId;",
                conn,
                tx))
            {
                cmd2.Parameters.AddWithValue("orderId", orderId);
                await cmd2.ExecuteNonQueryAsync(ct);
            }

            await tx.CommitAsync(ct);

            return await BuildSagaResponseAsync(
                sagaId,
                orderId,
                normalized,
                initialStock,
                stockAfterReserve,
                stepLog,
                "STOCK_LEAKED_AND_PAYMENT_UNREFUNDED",
                ct);
        }

        // Compensate backward (reverse order: Refund payment -> Release inventory), then prove idempotent replay.
        await RunIdempotentCompensationAsync(sagaId, orderId, stepLog, ct);
        var stockAfterFirstComp = await ReadCurrentFlashDealStockAsync(ct);

        // Second compensation replay must be a safe no-op (stock remains 5, never increments to 6).
        await RunIdempotentCompensationAsync(sagaId, orderId, stepLog, ct);
        var stockAfterReplay = await ReadCurrentFlashDealStockAsync(ct);

        logger.LogInformation(
            "saga_compensated saga_id={SagaId} order_id={OrderId} stock_first={StockFirst} stock_replay={StockReplay}",
            sagaId, orderId, stockAfterFirstComp, stockAfterReplay);

        return await BuildSagaResponseAsync(sagaId, orderId, normalized, initialStock, stockAfterReplay, stepLog, "NONE", ct);
    }

    private async Task RunIdempotentCompensationAsync(string sagaId, long orderId, List<string> stepLog, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        bool alreadyInventoryCompensated = false;
        bool alreadyPaymentRefunded = false;

        await using (var lockCmd = new NpgsqlCommand(
            """
            SELECT inventory_compensated, payment_refunded
              FROM saga_executions
             WHERE saga_id = @sagaId
               FOR UPDATE;
            """,
            conn,
            tx))
        {
            lockCmd.Parameters.AddWithValue("sagaId", sagaId);
            await using var reader = await lockCmd.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
            {
                alreadyInventoryCompensated = reader.GetBoolean(0);
                alreadyPaymentRefunded = reader.GetBoolean(1);
            }
        }

        if (!alreadyPaymentRefunded)
        {
            stepLog.Add("COMPENSATE_2_PAYMENT_REFUNDED (340.00 INR)");
        }
        if (!alreadyInventoryCompensated)
        {
            await using var releaseCmd = new NpgsqlCommand(
                """
                UPDATE catalog.flash_deals
                   SET remaining_stock = remaining_stock + 1,
                       version = version + 1
                 WHERE menu_item_id = @menuItemId;
                """,
                conn,
                tx);
            releaseCmd.Parameters.AddWithValue("menuItemId", FlashDealMenuItemId);
            await releaseCmd.ExecuteNonQueryAsync(ct);
            stepLog.Add("COMPENSATE_1_INVENTORY_RELEASED (stock 4 -> 5)");
        }
        if (alreadyInventoryCompensated && alreadyPaymentRefunded)
        {
            stepLog.Add("COMPENSATE_REPLAY_NOOP (idempotent guard prevented double restock)");
        }

        await using (var updateSagaCmd = new NpgsqlCommand(
            """
            UPDATE saga_executions
               SET current_state = 'COMPENSATED',
                   payment_refunded = TRUE,
                   inventory_compensated = TRUE,
                   compensation_attempts = compensation_attempts + 1,
                   step_log = @stepLog::jsonb,
                   updated_at = NOW()
             WHERE saga_id = @sagaId;
            """,
            conn,
            tx))
        {
            updateSagaCmd.Parameters.AddWithValue("sagaId", sagaId);
            updateSagaCmd.Parameters.AddWithValue("stepLog", JsonSerializer.Serialize(stepLog, JsonOptions));
            await updateSagaCmd.ExecuteNonQueryAsync(ct);
        }

        await using (var orderCmd = new NpgsqlCommand(
            "UPDATE orders SET status = 'cancelled_compensated', status_version = status_version + 1 WHERE id = @orderId;",
            conn,
            tx))
        {
            orderCmd.Parameters.AddWithValue("orderId", orderId);
            await orderCmd.ExecuteNonQueryAsync(ct);
        }

        await tx.CommitAsync(ct);
    }

    private async Task<SagaExecutionResponse> BuildSagaResponseAsync(
        string sagaId,
        long orderId,
        string scenario,
        int initialStock,
        int stockAfterReplay,
        IReadOnlyList<string> stepLog,
        string inconsistency,
        CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);

        var remainingStock = await ReadCurrentFlashDealStockAsync(ct);
        string orderStatus = "unknown";
        string currentState = "UNKNOWN";
        bool invReserved = false;
        bool invComp = false;
        bool payCap = false;
        bool payRef = false;
        bool kitAcc = false;
        int compAttempts = 0;

        await using (var cmd = new NpgsqlCommand(
            """
            SELECT s.current_state, s.inventory_reserved, s.inventory_compensated,
                   s.payment_captured, s.payment_refunded, s.kitchen_accepted,
                   s.compensation_attempts, o.status
              FROM saga_executions s
              JOIN orders o ON o.id = s.order_id
             WHERE s.saga_id = @sagaId;
            """,
            conn))
        {
            cmd.Parameters.AddWithValue("sagaId", sagaId);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
            {
                currentState = reader.GetString(0);
                invReserved = reader.GetBoolean(1);
                invComp = reader.GetBoolean(2);
                payCap = reader.GetBoolean(3);
                payRef = reader.GetBoolean(4);
                kitAcc = reader.GetBoolean(5);
                compAttempts = reader.GetInt32(6);
                orderStatus = reader.GetString(7);
            }
        }

        return new SagaExecutionResponse(
            SagaId: sagaId,
            OrderId: orderId,
            MenuItemId: FlashDealMenuItemId,
            Scenario: scenario,
            CurrentState: currentState,
            OrderStatus: orderStatus,
            InitialStock: initialStock,
            RemainingStock: remainingStock,
            StockAfterReplay: stockAfterReplay,
            InventoryReserved: invReserved,
            InventoryCompensated: invComp,
            PaymentCaptured: payCap,
            PaymentRefunded: payRef,
            KitchenAccepted: kitAcc,
            CompensationAttempts: compAttempts,
            Inconsistency: inconsistency,
            StepLog: stepLog);
    }

    private async Task<int> ReadCurrentFlashDealStockAsync(CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "SELECT remaining_stock FROM catalog.flash_deals WHERE menu_item_id = @menuItemId;",
            conn);
        cmd.Parameters.AddWithValue("menuItemId", FlashDealMenuItemId);
        return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct) ?? 0);
    }

    private async Task<long> InsertLabOrderAsync(
        NpgsqlConnection conn,
        NpgsqlTransaction tx,
        string idempotencyKey,
        string status,
        CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            """
            INSERT INTO orders (customer_id, restaurant_id, total_amount, idempotency_key, status)
            VALUES (@customerId, 7, 340.00, @idem, @status)
            RETURNING id;
            """,
            conn,
            tx);
        cmd.Parameters.AddWithValue("customerId", OutboxDemoCustomerId);
        cmd.Parameters.AddWithValue("idem", idempotencyKey);
        cmd.Parameters.AddWithValue("status", status);
        var result = await cmd.ExecuteScalarAsync(ct);
        return Convert.ToInt64(result);
    }

    private async Task<(bool OrderInDb, bool OutboxInDb, bool OutboxPublished)> InspectDbStateAsync(
        long orderId,
        string eventId,
        CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);

        await using var orderCmd = new NpgsqlCommand("SELECT EXISTS(SELECT 1 FROM orders WHERE id = @orderId);", conn);
        orderCmd.Parameters.AddWithValue("orderId", orderId);
        var orderInDb = (bool)(await orderCmd.ExecuteScalarAsync(ct) ?? false);

        await using var outboxCmd = new NpgsqlCommand(
            "SELECT published_at FROM outbox_events WHERE event_id = @eventId;",
            conn);
        outboxCmd.Parameters.AddWithValue("eventId", eventId);
        await using var reader = await outboxCmd.ExecuteReaderAsync(ct);
        var outboxInDb = await reader.ReadAsync(ct);
        var outboxPublished = outboxInDb && !await reader.IsDBNullAsync(0, ct);

        return (orderInDb, outboxInDb, outboxPublished);
    }

    private IProducer<string, string> BuildProducer() =>
        new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true
        }).Build();

    private int CountMatchingEventsOnKafka(IReadOnlySet<string> targetEventIds)
    {
        if (targetEventIds.Count == 0)
            return 0;

        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = $"tadka-outbox-probe-{Guid.NewGuid():N}"[..28],
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        }).Build();

        var tp = new TopicPartition(OutboxTopic, new Partition(0));
        var wm = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5));
        if (wm.High.Value <= wm.Low.Value)
            return 0;

        consumer.Assign(new TopicPartitionOffset(tp, wm.Low));
        var matches = 0;
        var deadline = DateTime.UtcNow.AddSeconds(10);

        while (DateTime.UtcNow < deadline)
        {
            var cr = consumer.Consume(TimeSpan.FromMilliseconds(200));
            if (cr is null)
                continue;
            if (cr.IsPartitionEOF)
            {
                if (cr.Offset.Value >= wm.High.Value)
                    break;
                continue;
            }

            using var doc = JsonDocument.Parse(cr.Message.Value);
            if (doc.RootElement.TryGetProperty("eventId", out var eidProp) &&
                targetEventIds.Contains(eidProp.GetString() ?? ""))
            {
                matches++;
            }

            if (cr.Offset.Value + 1 >= wm.High.Value)
                break;
        }

        return matches;
    }

    private async Task<(int KafkaMatches, int DuplicatesSuppressed, int UniqueEffects)> ConsumeAndDeduplicateOutboxTopicAsync(
        IReadOnlySet<string> targetEventIds,
        CancellationToken ct)
    {
        if (targetEventIds.Count == 0)
            return (0, 0, 0);

        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = $"tadka-outbox-sink-{Guid.NewGuid():N}"[..28],
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        }).Build();

        var tp = new TopicPartition(OutboxTopic, new Partition(0));
        var wm = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5));
        if (wm.High.Value <= wm.Low.Value)
            return (0, 0, 0);

        consumer.Assign(new TopicPartitionOffset(tp, wm.Low));
        var kafkaMatches = 0;
        var suppressed = 0;
        var deadline = DateTime.UtcNow.AddSeconds(10);

        await using var conn = await dataSource.OpenConnectionAsync(ct);

        while (DateTime.UtcNow < deadline)
        {
            ct.ThrowIfCancellationRequested();
            var cr = consumer.Consume(TimeSpan.FromMilliseconds(200));
            if (cr is null)
                continue;
            if (cr.IsPartitionEOF)
            {
                if (cr.Offset.Value >= wm.High.Value)
                    break;
                continue;
            }

            using var doc = JsonDocument.Parse(cr.Message.Value);
            var root = doc.RootElement;
            var eventId = root.TryGetProperty("eventId", out var eidProp) ? (eidProp.GetString() ?? "") : "";
            var aggregateId = root.TryGetProperty("aggregateId", out var aggProp) ? aggProp.GetInt64() : 0L;

            if (targetEventIds.Contains(eventId))
            {
                kafkaMatches++;

                await using var tx = await conn.BeginTransactionAsync(ct);
                await using var dedupCmd = new NpgsqlCommand(
                    """
                    INSERT INTO processed_consumer_events (event_id, consumer_group, order_id, customer_id, points_awarded)
                    VALUES (@eventId, @groupName, @orderId, @customerId, 25)
                    ON CONFLICT (event_id, consumer_group) DO NOTHING;
                    """,
                    conn,
                    tx);
                dedupCmd.Parameters.AddWithValue("eventId", eventId);
                dedupCmd.Parameters.AddWithValue("groupName", OutboxDownstreamGroup);
                dedupCmd.Parameters.AddWithValue("orderId", aggregateId);
                dedupCmd.Parameters.AddWithValue("customerId", OutboxDemoCustomerId);

                var inserted = await dedupCmd.ExecuteNonQueryAsync(ct);
                if (inserted == 1)
                {
                    await using var ledgerCmd = new NpgsqlCommand(
                        """
                        INSERT INTO consumer_loyalty_ledger (customer_id, points_balance, credit_count, updated_at)
                        VALUES (@customerId, 25, 1, NOW())
                        ON CONFLICT (customer_id) DO UPDATE
                        SET points_balance = consumer_loyalty_ledger.points_balance + 25,
                            credit_count = consumer_loyalty_ledger.credit_count + 1,
                            updated_at = NOW();
                        """,
                        conn,
                        tx);
                    ledgerCmd.Parameters.AddWithValue("customerId", OutboxDemoCustomerId);
                    await ledgerCmd.ExecuteNonQueryAsync(ct);
                }
                else
                {
                    suppressed++;
                }

                await tx.CommitAsync(ct);
            }

            if (cr.Offset.Value + 1 >= wm.High.Value)
                break;
        }

        await using var uniqueCmd = new NpgsqlCommand(
            "SELECT COUNT(*) FROM processed_consumer_events WHERE consumer_group = @groupName AND event_id = ANY(@ids);",
            conn);
        uniqueCmd.Parameters.AddWithValue("groupName", OutboxDownstreamGroup);
        uniqueCmd.Parameters.AddWithValue("ids", targetEventIds.ToArray());
        var uniqueEffects = Convert.ToInt32(await uniqueCmd.ExecuteScalarAsync(ct) ?? 0);

        return (kafkaMatches, suppressed, uniqueEffects);
    }
}
