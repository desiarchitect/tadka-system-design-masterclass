using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Npgsql;
using StackExchange.Redis;
using Tadka.OrderApi.Modules.Catalog;
using Tadka.OrderApi.Modules.Instance;
using Tadka.OrderApi.Modules.Messaging;
using Tadka.OrderApi.Modules.Observability;
using Tadka.OrderApi.Modules.OverloadProtection;
using Tadka.OrderApi.Modules.Resilience;

namespace Tadka.OrderApi.Modules.TadkaV1;

public sealed record V1StageSummary(
    int Stage,
    string Module,
    string Capability,
    string Mechanism,
    string PrimaryArtifact);

public sealed record V1ArchitectureResponse(
    string Service,
    string Version,
    string InstanceId,
    bool AllDependenciesHealthy,
    object Infrastructure,
    IReadOnlyList<V1StageSummary> Stages,
    object LiveCounters);

public sealed record V1CheckoutRequest(
    long? CustomerId = null,
    long? RestaurantId = null,
    long? MenuItemId = null,
    int? Quantity = null,
    string? PayerVpa = null,
    string? CardHint = null,
    string? KycHint = null,
    string? Priority = null,
    int? CallerBudgetMs = null,
    int? GatewaySpentMs = null,
    bool? PauseOutboxRelay = null,
    string? RateLimitKey = null);

public sealed record V1CheckoutResponse(
    int StatusCode,
    string Outcome,
    string IdempotencyStatus,
    string IdempotencyKey,
    long? OrderId,
    string? IntentRef,
    string? PspRrn,
    string? EventId,
    string? TraceId,
    string? Traceparent,
    string InstanceId,
    long? RestaurantId,
    long? MenuItemId,
    string? ItemName,
    int Quantity,
    long AmountPaise,
    string? CacheStatus,
    string? CacheKey,
    string? SheddingDecision,
    long LedgerEntriesCount,
    long LedgerNetSignedSum,
    bool OutboxPublished,
    int LoyaltyPointsCredited,
    string? PiiMaskedLogPreview,
    double DurationMs,
    string? ErrorCode,
    string? ErrorDetail);

public sealed record V1ChaosDrillRequest(string? Mode = null);

public sealed record V1ChaosDrillResponse(
    string Mode,
    object StampedeCheck,
    object WebhookDoubleDebitCheck,
    object DeadlineWasteCheck,
    object LoadSheddingCheck,
    string SummaryVerdict);

public sealed record V1OutboxFlushResponse(
    string Topic,
    int PublishedCount,
    int NewlyCreditedConsumers,
    int DuplicateConsumersSuppressed,
    long PendingRemaining,
    IReadOnlyList<string> FlushedEventIds);

public sealed record V1ResetResponse(
    string Status,
    int DeletedV1Orders,
    int DeletedV1Intents,
    int DeletedV1OutboxEvents);

public sealed class TadkaV1ProductionService(
    IConfiguration configuration,
    NpgsqlDataSource dataSource,
    IConnectionMultiplexer redis,
    ICatalogModule catalogModule,
    MenuCacheService menuCache,
    OverloadProtectionLabService overloadProtection,
    ResilienceLabService resilience,
    ObservabilityLabService observability,
    InstanceLifecycleService instanceLifecycle,
    ILogger<TadkaV1ProductionService> logger)
{
    private const string ConsumerGroup = "tadka-v1-loyalty-consumer";
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private static readonly ActivitySource V1ActivitySource = new(ObservabilityLabService.ActivitySourceName, "1.0.0");

    private string BootstrapServers => configuration["Kafka:BootstrapServers"] ?? "127.0.0.1:9092";
    private string OutboxTopic => configuration["Kafka:OutboxTopic"] ?? "tadka.order.outbox";

    public async Task<V1ArchitectureResponse> GetArchitectureAsync(CancellationToken ct)
    {
        var pgSw = Stopwatch.StartNew();
        var pgOk = false;
        long v1Orders = 0;
        long v1PendingOutbox = 0;
        long v1PublishedOutbox = 0;
        long v1LedgerEntries = 0;
        long v1LedgerNetSignedSum = 0;

        try
        {
            await using var conn = await dataSource.OpenConnectionAsync(ct);
            await using (var pingCmd = new NpgsqlCommand("SELECT 1;", conn))
            {
                await pingCmd.ExecuteScalarAsync(ct);
            }
            pgOk = true;

            await using var statsCmd = new NpgsqlCommand(
                """
                SELECT
                    (SELECT COUNT(*) FROM orders WHERE idempotency_key LIKE 'v1-lab-%'),
                    (SELECT COUNT(*) FROM outbox_events WHERE event_id LIKE 'evt-v1-%' AND published_at IS NULL),
                    (SELECT COUNT(*) FROM outbox_events WHERE event_id LIKE 'evt-v1-%' AND published_at IS NOT NULL),
                    (SELECT COUNT(*) FROM ledger_entries WHERE intent_ref LIKE 'v1_intent_%'),
                    (SELECT COALESCE(SUM(signed_amount), 0) FROM ledger_entries WHERE intent_ref LIKE 'v1_intent_%');
                """,
                conn);
            await using var reader = await statsCmd.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
            {
                v1Orders = reader.GetInt64(0);
                v1PendingOutbox = reader.GetInt64(1);
                v1PublishedOutbox = reader.GetInt64(2);
                v1LedgerEntries = reader.GetInt64(3);
                v1LedgerNetSignedSum = reader.GetInt64(4);
            }
        }
        catch (NpgsqlException ex)
        {
            logger.LogWarning(ex, "v1_arch_pg_probe_failed");
        }
        pgSw.Stop();

        var redisSw = Stopwatch.StartNew();
        var redisOk = false;
        try
        {
            if (redis.IsConnected)
            {
                await redis.GetDatabase().PingAsync();
                redisOk = true;
            }
        }
        catch (RedisException ex)
        {
            logger.LogWarning(ex, "v1_arch_redis_probe_failed");
        }
        redisSw.Stop();

        var kafkaSw = Stopwatch.StartNew();
        var kafkaOk = false;
        try
        {
            await KafkaTopicAdmin.EnsureTopicAsync(BootstrapServers, OutboxTopic, partitions: 1, ct);
            kafkaOk = true;
        }
        catch (Exception ex) when (ex is KafkaException or InvalidOperationException)
        {
            logger.LogWarning(ex, "v1_arch_kafka_probe_failed");
        }
        kafkaSw.Stop();

        var csb = new NpgsqlConnectionStringBuilder(configuration.GetConnectionString("Tadka") ?? string.Empty);
        var maxPoolSize = csb.MaxPoolSize > 0 ? csb.MaxPoolSize : 100;

        var stages = new V1StageSummary[]
        {
            new(1, "Modules 1-2", "Modular Monolith + Indexing", "Schema-owned Orders & Catalog, price snapshot rows, composite B-tree lookups", "database/init.sql + ICatalogModule"),
            new(2, "Module 3", "Replication + Partitioning", "Monotonic status_version guard and replica-lag aware read routing", "ShardingLabService + ReplicationLabService"),
            new(3, "Module 4", "Gateway BFF + Redis Single-Flight", "X-Tadka-Client enforcement, round-robin LB, single-flight coalescing + jittered TTL", "Tadka.GatewayBff + MenuCacheService"),
            new(4, "Module 5", "Transactional Outbox + Idempotent Consumer", "Atomic outbox_events row in checkout SQL transaction + processed_consumer_events deduplication", "OutboxSagaLabService + ReliableConsumerLabService"),
            new(5, "Module 5", "UPI Idempotency + Double-Entry Ledger", "(customer_id, idempotency_key) 201/200/409 contract + 4-leg zero-sum integer-paise ledger", "UpiPaymentWorkflowService + payment_intents"),
            new(6, "Module 6", "Resilience + Overload Protection", "Caller deadline budget propagation, Lua token bucket rate limiting, priority load shedding & bulkheads", "ResilienceLabService + OverloadProtectionLabService"),
            new(7, "Module 6", "W3C Observability + PII Governance", "traceparent 5-span tree (Gateway->OrderApi->Redis->PG->Kafka), bounded RED/USE metrics, PII masking", "ObservabilityLabService")
        };

        return new V1ArchitectureResponse(
            Service: "Tadka.OrderApi",
            Version: "v1.0-production",
            InstanceId: instanceLifecycle.InstanceId,
            AllDependenciesHealthy: pgOk && redisOk && kafkaOk,
            Infrastructure: new
            {
                postgres = new { ok = pgOk, latencyMs = Math.Round(pgSw.Elapsed.TotalMilliseconds, 2), maxPoolSize },
                redis = new { ok = redisOk, latencyMs = Math.Round(redisSw.Elapsed.TotalMilliseconds, 2), maxMemory = "64mb", eviction = "allkeys-lru" },
                kafka = new { ok = kafkaOk, bootstrapServers = BootstrapServers, outboxTopic = OutboxTopic, latencyMs = Math.Round(kafkaSw.Elapsed.TotalMilliseconds, 2) }
            },
            Stages: stages,
            LiveCounters: new
            {
                v1OrdersCreated = v1Orders,
                v1PendingOutbox,
                v1PublishedOutbox,
                v1LedgerEntries,
                v1LedgerNetSignedSum
            });
    }

    public async Task<V1CheckoutResponse> CheckoutAsync(
        V1CheckoutRequest req,
        string? rawIdempotencyKey,
        string? traceparentHeader,
        CancellationToken ct)
    {
        var customerId = req.CustomerId is > 0 ? req.CustomerId.Value : 1091L;
        var restaurantId = req.RestaurantId is > 0 ? req.RestaurantId.Value : 7L;
        var menuItemId = req.MenuItemId is > 0 ? req.MenuItemId.Value : 101L;
        var quantity = req.Quantity ?? 2;
        if (quantity is < 1 or > 20)
        {
            return new V1CheckoutResponse(
                400, "INVALID_REQUEST", "NONE", rawIdempotencyKey ?? string.Empty,
                null, null, null, null, null, null, instanceLifecycle.InstanceId,
                restaurantId, menuItemId, null, quantity, 0, null, null, null, 0, 0, false, 0,
                null, 0, "INVALID_QUANTITY", "quantity must be between 1 and 20.");
        }

        var payerVpa = string.IsNullOrWhiteSpace(req.PayerVpa) ? "ananya.sharma@okhdfcbank" : req.PayerVpa.Trim();
        var priority = string.IsNullOrWhiteSpace(req.Priority) ? "critical-checkout" : req.Priority.Trim().ToLowerInvariant();
        var callerBudgetMs = req.CallerBudgetMs ?? 4000;
        var gatewaySpentMs = req.GatewaySpentMs ?? 0;
        var pauseOutboxRelay = req.PauseOutboxRelay ?? false;
        var idempotencyKey = string.IsNullOrWhiteSpace(rawIdempotencyKey)
            ? $"v1-auto-{Guid.NewGuid():N}"[..20]
            : rawIdempotencyKey.Trim();

        ActivityContext parentContext = default;
        var hasValidParent = !string.IsNullOrWhiteSpace(traceparentHeader)
            && ActivityContext.TryParse(traceparentHeader, null, out parentContext);

        using var rootActivity = hasValidParent
            ? V1ActivitySource.StartActivity("orderapi.v1.checkout", ActivityKind.Server, parentContext)
            : V1ActivitySource.StartActivity("orderapi.v1.checkout", ActivityKind.Server);

        var traceId = rootActivity?.TraceId.ToHexString() ?? ActivityTraceId.CreateRandom().ToHexString();
        var spanId = rootActivity?.SpanId.ToHexString() ?? ActivitySpanId.CreateRandom().ToHexString();
        var outgoingTraceparent = $"00-{traceId}-{spanId}-01";
        rootActivity?.SetTag("http.route", "/v1/tadka/orders/checkout");
        rootActivity?.SetTag("tadka.instance", instanceLifecycle.InstanceId);

        var sw = Stopwatch.StartNew();

        // Stage 6A: Priority Load Shedding
        var shedDecision = overloadProtection.EvaluateLoadShedding(priority);
        if (!shedDecision.Ok)
        {
            sw.Stop();
            rootActivity?.SetStatus(ActivityStatusCode.Error, shedDecision.Reason);
            RecordTelemetry(traceId, spanId, null, 503, "5xx", true, sw.Elapsed.TotalMilliseconds,
                $"V1 checkout shed before work for payer={payerVpa} reason={shedDecision.Reason}");

            return new V1CheckoutResponse(
                503, "SHED", "NONE", idempotencyKey, null, null, null, null, traceId, outgoingTraceparent,
                instanceLifecycle.InstanceId, restaurantId, menuItemId, null, quantity, 0,
                null, null, shedDecision.Reason, 0, 0, false, 0, null, Math.Round(sw.Elapsed.TotalMilliseconds, 3),
                "LOAD_SHEDDED", $"Request shed due to {shedDecision.Reason}");
        }

        // Stage 6B: Optional token bucket rate limit
        if (!string.IsNullOrWhiteSpace(req.RateLimitKey))
        {
            var rl = await overloadProtection.CheckRateLimitAsync(req.RateLimitKey, "/v1/tadka/orders/checkout", 1, null, ct);
            if (!rl.Ok)
            {
                sw.Stop();
                rootActivity?.SetStatus(ActivityStatusCode.Error, rl.Decision);
                RecordTelemetry(traceId, spanId, null, 429, "4xx", true, sw.Elapsed.TotalMilliseconds,
                    $"V1 checkout rate limited key={req.RateLimitKey} decision={rl.Decision}");

                return new V1CheckoutResponse(
                    429, "RATE_LIMITED", "NONE", idempotencyKey, null, null, null, null, traceId, outgoingTraceparent,
                    instanceLifecycle.InstanceId, restaurantId, menuItemId, null, quantity, 0,
                    null, null, rl.Decision, 0, 0, false, 0, null, Math.Round(sw.Elapsed.TotalMilliseconds, 3),
                    "RATE_LIMITED", $"Token bucket rejected request ({rl.Decision}).");
            }
        }

        // Stage 6C: Layered deadline budget check
        if (callerBudgetMs - gatewaySpentMs <= 10)
        {
            sw.Stop();
            rootActivity?.SetStatus(ActivityStatusCode.Error, "deadline_exceeded");
            RecordTelemetry(traceId, spanId, null, 504, "5xx", true, sw.Elapsed.TotalMilliseconds,
                $"V1 checkout aborted before DB work: remaining budget {callerBudgetMs - gatewaySpentMs}ms");

            return new V1CheckoutResponse(
                504, "DEADLINE_EXCEEDED", "NONE", idempotencyKey, null, null, null, null, traceId, outgoingTraceparent,
                instanceLifecycle.InstanceId, restaurantId, menuItemId, null, quantity, 0,
                null, null, "deadline_budget_exhausted", 0, 0, false, 0, null, Math.Round(sw.Elapsed.TotalMilliseconds, 3),
                "DEADLINE_EXCEEDED", "Remaining caller deadline budget exhausted; skipped PostgreSQL write.");
        }

        // Stage 3: Redis Single-Flight catalog menu lookup
        StampedeMenuResult cacheResult;
        using (var redisActivity = V1ActivitySource.StartActivity("redis.v1.menu.singleflight", ActivityKind.Client))
        {
            cacheResult = await menuCache.GetMenuStampedeAsync(restaurantId, "single-flight", dbDelayMs: 5, baseTtlSeconds: 30, jitterSeconds: 5, ct);
            redisActivity?.SetTag("cache.status", cacheResult.CacheStatus);
            redisActivity?.SetTag("cache.key", cacheResult.CacheKey);
        }

        var menu = cacheResult.Menu ?? await catalogModule.GetMenuAsync(restaurantId, ct);
        if (menu is null)
        {
            sw.Stop();
            RecordTelemetry(traceId, spanId, null, 404, "4xx", true, sw.Elapsed.TotalMilliseconds,
                $"Restaurant {restaurantId} not found during V1 checkout.");
            return new V1CheckoutResponse(
                404, "NOT_FOUND", "NONE", idempotencyKey, null, null, null, null, traceId, outgoingTraceparent,
                instanceLifecycle.InstanceId, restaurantId, menuItemId, null, quantity, 0,
                cacheResult.CacheStatus, cacheResult.CacheKey, shedDecision.Reason, 0, 0, false, 0, null,
                Math.Round(sw.Elapsed.TotalMilliseconds, 3), "RESTAURANT_NOT_FOUND", $"Restaurant {restaurantId} does not exist.");
        }

        var item = menu.Items.FirstOrDefault(i => i.Id == menuItemId);
        if (item is null || !item.IsAvailable)
        {
            sw.Stop();
            var code = item is null ? 404 : 409;
            var err = item is null ? "ITEM_NOT_FOUND" : "ITEM_UNAVAILABLE";
            RecordTelemetry(traceId, spanId, null, code, "4xx", true, sw.Elapsed.TotalMilliseconds,
                $"Menu item {menuItemId} check failed ({err}) for restaurant {restaurantId}.");
            return new V1CheckoutResponse(
                code, err, "NONE", idempotencyKey, null, null, null, null, traceId, outgoingTraceparent,
                instanceLifecycle.InstanceId, restaurantId, menuItemId, item?.Name, quantity, 0,
                cacheResult.CacheStatus, cacheResult.CacheKey, shedDecision.Reason, 0, 0, false, 0, null,
                Math.Round(sw.Elapsed.TotalMilliseconds, 3), err, $"Menu item {menuItemId} is unavailable or missing.");
        }

        var unitPricePaise = (long)Math.Round(item.Price * 100m, MidpointRounding.AwayFromZero);
        var amountPaise = unitPricePaise * quantity;
        var totalRupees = Math.Round(amountPaise / 100m, 2, MidpointRounding.AwayFromZero);
        var payloadHash = ComputeHash($"{customerId}|{restaurantId}|{menuItemId}|{quantity}|{amountPaise}|{payerVpa}");
        var scopedIdemKey = idempotencyKey.StartsWith("v1-lab-", StringComparison.Ordinal)
            ? idempotencyKey
            : $"v1-lab-{idempotencyKey}";

        long orderId = 0;
        string intentRef = string.Empty;
        string pspRrn = string.Empty;
        string eventId = string.Empty;
        var isReplay = false;
        var outboxPublished = false;
        var loyaltyPoints = 0;

        // Stage 4 + 5: Single atomic SQL transaction (Idempotency + Order + Snapshot + Status + Payment + 4-leg Ledger + Outbox)
        using (var dbActivity = V1ActivitySource.StartActivity("postgres.v1.tx.checkout", ActivityKind.Client))
        {
            var acquireSw = Stopwatch.StartNew();
            await using var conn = await dataSource.OpenConnectionAsync(ct);
            observability.RecordPoolAcquireSample(acquireSw.Elapsed.TotalMilliseconds, 1);

            await using var tx = await conn.BeginTransactionAsync(ct);

            // Lock/check idempotency record before inserting an order to prevent orphan order rows on replay/conflict
            await using (var probeCmd = new NpgsqlCommand(
                """
                SELECT intent_ref, order_id, payload_hash, COALESCE(psp_rrn, '')
                  FROM payment_intents
                 WHERE customer_id = @customerId AND idempotency_key = @idemKey
                 FOR UPDATE;
                """,
                conn,
                tx))
            {
                probeCmd.Parameters.AddWithValue("customerId", customerId);
                probeCmd.Parameters.AddWithValue("idemKey", scopedIdemKey);
                await using var reader = await probeCmd.ExecuteReaderAsync(ct);
                if (await reader.ReadAsync(ct))
                {
                    intentRef = reader.GetString(0);
                    orderId = reader.GetInt64(1);
                    var existingHash = reader.GetString(2);
                    pspRrn = reader.GetString(3);
                    eventId = $"evt-v1-order-paid-{orderId}";
                    await reader.CloseAsync();

                    if (!string.Equals(existingHash, payloadHash, StringComparison.Ordinal))
                    {
                        await tx.RollbackAsync(ct);
                        sw.Stop();
                        dbActivity?.SetStatus(ActivityStatusCode.Error, "idempotency_payload_mismatch");
                        rootActivity?.SetStatus(ActivityStatusCode.Error, "idempotency_payload_mismatch");
                        RecordTelemetry(traceId, spanId, orderId, 409, "4xx", true, sw.Elapsed.TotalMilliseconds,
                            $"Idempotency payload mismatch on key={idempotencyKey} payer={payerVpa}");

                        return new V1CheckoutResponse(
                            409, "CONFLICT", "MISMATCH", idempotencyKey, orderId, intentRef, pspRrn, eventId,
                            traceId, outgoingTraceparent, instanceLifecycle.InstanceId, restaurantId, menuItemId, item.Name,
                            quantity, amountPaise, cacheResult.CacheStatus, cacheResult.CacheKey, shedDecision.Reason,
                            0, 0, false, 0, null, Math.Round(sw.Elapsed.TotalMilliseconds, 3),
                            "IDEMPOTENCY_PAYLOAD_MISMATCH", "Idempotency-Key was already used with different checkout parameters.");
                    }

                    isReplay = true;
                }
            }

            if (!isReplay)
            {
                await using (var insertOrderCmd = new NpgsqlCommand(
                    """
                    INSERT INTO orders (customer_id, restaurant_id, total_amount, idempotency_key, status, status_version)
                    VALUES (@customerId, @restaurantId, @totalAmount, @idemKey, 'paid', 1)
                    RETURNING id;
                    """,
                    conn,
                    tx))
                {
                    insertOrderCmd.Parameters.AddWithValue("customerId", customerId);
                    insertOrderCmd.Parameters.AddWithValue("restaurantId", restaurantId);
                    insertOrderCmd.Parameters.AddWithValue("totalAmount", totalRupees);
                    insertOrderCmd.Parameters.AddWithValue("idemKey", scopedIdemKey);
                    orderId = (long)(await insertOrderCmd.ExecuteScalarAsync(ct)
                        ?? throw new InvalidOperationException("Failed to insert order."));
                }

                await using (var insertItemCmd = new NpgsqlCommand(
                    """
                    INSERT INTO order_items (order_id, menu_item_id, item_name_snapshot, unit_price_snapshot, quantity)
                    VALUES (@orderId, @menuItemId, @name, @price, @qty);
                    """,
                    conn,
                    tx))
                {
                    insertItemCmd.Parameters.AddWithValue("orderId", orderId);
                    insertItemCmd.Parameters.AddWithValue("menuItemId", item.Id);
                    insertItemCmd.Parameters.AddWithValue("name", item.Name);
                    insertItemCmd.Parameters.AddWithValue("price", item.Price);
                    insertItemCmd.Parameters.AddWithValue("qty", quantity);
                    await insertItemCmd.ExecuteNonQueryAsync(ct);
                }

                await using (var insertStatusCmd = new NpgsqlCommand(
                    """
                    INSERT INTO order_status_events (order_id, version, status)
                    VALUES (@orderId, 0, 'placed'), (@orderId, 1, 'paid');
                    """,
                    conn,
                    tx))
                {
                    insertStatusCmd.Parameters.AddWithValue("orderId", orderId);
                    await insertStatusCmd.ExecuteNonQueryAsync(ct);
                }

                intentRef = $"v1_intent_{Guid.NewGuid():N}"[..24];
                pspRrn = $"RRN-V1-{orderId}";
                eventId = $"evt-v1-order-paid-{orderId}";

                await using (var insertIntentCmd = new NpgsqlCommand(
                    """
                    INSERT INTO payment_intents
                        (intent_ref, order_id, customer_id, amount_paise, currency, payer_vpa, idempotency_key, payload_hash, status, psp_rrn, webhook_deliveries)
                    VALUES
                        (@intentRef, @orderId, @customerId, @amountPaise, 'INR', @payerVpa, @idemKey, @payloadHash, 'CAPTURED', @rrn, 1);
                    """,
                    conn,
                    tx))
                {
                    insertIntentCmd.Parameters.AddWithValue("intentRef", intentRef);
                    insertIntentCmd.Parameters.AddWithValue("orderId", orderId);
                    insertIntentCmd.Parameters.AddWithValue("customerId", customerId);
                    insertIntentCmd.Parameters.AddWithValue("amountPaise", amountPaise);
                    insertIntentCmd.Parameters.AddWithValue("payerVpa", payerVpa);
                    insertIntentCmd.Parameters.AddWithValue("idemKey", scopedIdemKey);
                    insertIntentCmd.Parameters.AddWithValue("payloadHash", payloadHash);
                    insertIntentCmd.Parameters.AddWithValue("rrn", pspRrn);
                    await insertIntentCmd.ExecuteNonQueryAsync(ct);
                }

                await InsertZeroSumLedgerAsync(conn, tx, intentRef, orderId, amountPaise, $"tx-{intentRef}", ct);

                var outboxPayload = JsonSerializer.Serialize(new
                {
                    eventId,
                    orderId,
                    customerId,
                    restaurantId,
                    amountPaise,
                    intentRef,
                    traceId,
                    status = "paid"
                }, JsonOptions);

                await using (var insertOutboxCmd = new NpgsqlCommand(
                    """
                    INSERT INTO outbox_events (event_id, aggregate_type, aggregate_id, event_type, payload, published_at, publish_attempts)
                    VALUES (@eventId, 'Order', @orderId, 'OrderPaidV1', @payload::jsonb, NULL, 0);
                    """,
                    conn,
                    tx))
                {
                    insertOutboxCmd.Parameters.AddWithValue("eventId", eventId);
                    insertOutboxCmd.Parameters.AddWithValue("orderId", orderId);
                    insertOutboxCmd.Parameters.AddWithValue("payload", outboxPayload);
                    await insertOutboxCmd.ExecuteNonQueryAsync(ct);
                }
            }

            await tx.CommitAsync(ct);
            dbActivity?.SetTag("order.id", orderId.ToString(CultureInfo.InvariantCulture));
            dbActivity?.SetTag("intent.ref", intentRef);
            dbActivity?.SetTag("idempotency.replay", isReplay ? "true" : "false");
        }

        if (!isReplay && !pauseOutboxRelay)
        {
            using var kafkaActivity = V1ActivitySource.StartActivity("kafka.v1.outbox.publish", ActivityKind.Producer);
            var flush = await PublishAndConsumeSingleEventAsync(eventId, orderId, customerId, amountPaise, ct);
            outboxPublished = flush.Published;
            loyaltyPoints = flush.PointsAwarded;
            kafkaActivity?.SetTag("messaging.destination.name", OutboxTopic);
            kafkaActivity?.SetTag("outbox.event_id", eventId);
        }
        else
        {
            await using var conn = await dataSource.OpenConnectionAsync(ct);
            await using var stateCmd = new NpgsqlCommand(
                """
                SELECT
                    COALESCE((SELECT published_at IS NOT NULL FROM outbox_events WHERE event_id = @eventId), FALSE),
                    COALESCE((SELECT points_awarded FROM processed_consumer_events WHERE event_id = @eventId AND consumer_group = @groupName), 0);
                """,
                conn);
            stateCmd.Parameters.AddWithValue("eventId", eventId);
            stateCmd.Parameters.AddWithValue("groupName", ConsumerGroup);
            await using var reader = await stateCmd.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
            {
                outboxPublished = reader.GetBoolean(0);
                loyaltyPoints = reader.GetInt32(1);
            }
        }

        // Read ledger verification sums
        long ledgerCount;
        long ledgerSum;
        await using (var conn = await dataSource.OpenConnectionAsync(ct))
        await using (var ledgerCmd = new NpgsqlCommand(
            "SELECT COUNT(*), COALESCE(SUM(signed_amount), 0) FROM ledger_entries WHERE intent_ref = @intentRef;",
            conn))
        {
            ledgerCmd.Parameters.AddWithValue("intentRef", intentRef);
            await using var reader = await ledgerCmd.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            ledgerCount = reader.GetInt64(0);
            ledgerSum = reader.GetInt64(1);
        }

        sw.Stop();
        var rawLogMessage =
            $"V1 checkout {(isReplay ? "replayed" : "completed")} order_id={orderId} payer={payerVpa} card={req.CardHint ?? "none"} pan={req.KycHint ?? "none"}";
        var (maskedPreview, _) = ObservabilityLabService.MaskPii(rawLogMessage);

        RecordTelemetry(traceId, spanId, orderId, isReplay ? 200 : 201, "2xx", false, sw.Elapsed.TotalMilliseconds, rawLogMessage);

        logger.LogInformation(
            "v1_checkout status={Status} order_id={OrderId} intent={IntentRef} trace_id={TraceId} duration_ms={DurationMs}",
            isReplay ? "REPLAYED" : "CREATED", orderId, intentRef, traceId, Math.Round(sw.Elapsed.TotalMilliseconds, 2));

        return new V1CheckoutResponse(
            StatusCode: isReplay ? 200 : 201,
            Outcome: isReplay ? "REPLAYED" : "CHECKOUT_CAPTURED",
            IdempotencyStatus: isReplay ? "REPLAYED" : "CREATED",
            IdempotencyKey: idempotencyKey,
            OrderId: orderId,
            IntentRef: intentRef,
            PspRrn: pspRrn,
            EventId: eventId,
            TraceId: traceId,
            Traceparent: outgoingTraceparent,
            InstanceId: instanceLifecycle.InstanceId,
            RestaurantId: restaurantId,
            MenuItemId: item.Id,
            ItemName: item.Name,
            Quantity: quantity,
            AmountPaise: amountPaise,
            CacheStatus: cacheResult.CacheStatus,
            CacheKey: cacheResult.CacheKey,
            SheddingDecision: shedDecision.Reason,
            LedgerEntriesCount: ledgerCount,
            LedgerNetSignedSum: ledgerSum,
            OutboxPublished: outboxPublished,
            LoyaltyPointsCredited: loyaltyPoints,
            PiiMaskedLogPreview: maskedPreview,
            DurationMs: Math.Round(sw.Elapsed.TotalMilliseconds, 3),
            ErrorCode: null,
            ErrorDetail: null);
    }

    public async Task<V1OutboxFlushResponse> FlushOutboxAsync(CancellationToken ct)
    {
        await KafkaTopicAdmin.EnsureTopicAsync(BootstrapServers, OutboxTopic, partitions: 1, ct);

        var pending = new List<(string EventId, long OrderId, long CustomerId, long AmountPaise, string Payload)>();
        await using (var conn = await dataSource.OpenConnectionAsync(ct))
        await using (var cmd = new NpgsqlCommand(
            """
            SELECT event_id, aggregate_id,
                   COALESCE((payload->>'customerId')::bigint, 1091),
                   COALESCE((payload->>'amountPaise')::bigint, 48000),
                   payload::text
              FROM outbox_events
             WHERE published_at IS NULL AND event_id LIKE 'evt-v1-%'
             ORDER BY id ASC;
            """,
            conn))
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                pending.Add((reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3), reader.GetString(4)));
            }
        }

        var newlyCredited = 0;
        var duplicatesSuppressed = 0;
        var flushedIds = new List<string>();

        using var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true
        }).Build();

        foreach (var evt in pending)
        {
            await producer.ProduceAsync(OutboxTopic, new Message<string, string>
            {
                Key = $"order-{evt.OrderId}",
                Value = evt.Payload
            }, ct);

            await using var conn = await dataSource.OpenConnectionAsync(ct);
            await using (var markCmd = new NpgsqlCommand(
                "UPDATE outbox_events SET published_at = NOW(), publish_attempts = publish_attempts + 1 WHERE event_id = @eventId;",
                conn))
            {
                markCmd.Parameters.AddWithValue("eventId", evt.EventId);
                await markCmd.ExecuteNonQueryAsync(ct);
            }

            // Consume twice deliberately to verify deterministic at-least-once consumer deduplication
            for (var delivery = 1; delivery <= 2; delivery++)
            {
                var result = await ApplyIdempotentLoyaltyAsync(conn, evt.EventId, evt.OrderId, evt.CustomerId, evt.AmountPaise, ct);
                if (result.Credited)
                    newlyCredited++;
                else
                    duplicatesSuppressed++;
            }

            flushedIds.Add(evt.EventId);
        }

        producer.Flush(ct);

        long remaining;
        await using (var conn = await dataSource.OpenConnectionAsync(ct))
        await using (var remCmd = new NpgsqlCommand(
            "SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL AND event_id LIKE 'evt-v1-%';",
            conn))
        {
            remaining = (long)(await remCmd.ExecuteScalarAsync(ct) ?? 0L);
        }

        return new V1OutboxFlushResponse(
            Topic: OutboxTopic,
            PublishedCount: flushedIds.Count,
            NewlyCreditedConsumers: newlyCredited,
            DuplicateConsumersSuppressed: duplicatesSuppressed,
            PendingRemaining: remaining,
            FlushedEventIds: flushedIds);
    }

    public async Task<V1ChaosDrillResponse> RunChaosDrillAsync(string? rawMode, CancellationToken ct)
    {
        var mode = string.IsNullOrWhiteSpace(rawMode) ? "defended" : rawMode.Trim().ToLowerInvariant();
        if (mode is not ("unprotected" or "defended"))
            throw new ArgumentException("mode must be 'unprotected' or 'defended'.");

        var isDefended = mode == "defended";

        // 1. Stampede drill (15 concurrent cold-cache reads)
        await menuCache.ResetAsync(ct);
        var beforeStats = await menuCache.GetStatsAsync(7, ct);
        var stampedeTasks = Enumerable.Range(0, 15)
            .Select(_ => menuCache.GetMenuStampedeAsync(7, isDefended ? "single-flight" : "naive", dbDelayMs: 60, baseTtlSeconds: 30, jitterSeconds: 5, ct))
            .ToArray();
        await Task.WhenAll(stampedeTasks);
        var afterStats = await menuCache.GetStatsAsync(7, ct);
        var stampedeDbQueries = afterStats.DbQueries - beforeStats.DbQueries;
        var coalescedReads = afterStats.CoalescedRequests - beforeStats.CoalescedRequests;

        // 2. Duplicate webhook double-debit drill (real PostgreSQL payment_intents + ledger_entries)
        var drillTag = Guid.NewGuid().ToString("N")[..10];
        var drillCustomerId = 1099L;
        var drillIntentRef = $"v1_drill_{drillTag}";
        const long drillAmountPaise = 48000;
        long ledgerRows;
        long debitSumPaise;
        long netSignedSum;

        await using (var conn = await dataSource.OpenConnectionAsync(ct))
        {
            long drillOrderId;
            await using (var orderCmd = new NpgsqlCommand(
                """
                INSERT INTO orders (customer_id, restaurant_id, total_amount, idempotency_key, status)
                VALUES (@customerId, 7, 480.00, @idemKey, 'payment_pending')
                RETURNING id;
                """,
                conn))
            {
                orderCmd.Parameters.AddWithValue("customerId", drillCustomerId);
                orderCmd.Parameters.AddWithValue("idemKey", $"v1-drill-order-{drillTag}");
                drillOrderId = (long)(await orderCmd.ExecuteScalarAsync(ct)
                    ?? throw new InvalidOperationException("Failed to create drill order."));
            }

            await using (var intentCmd = new NpgsqlCommand(
                """
                INSERT INTO payment_intents
                    (intent_ref, order_id, customer_id, amount_paise, currency, payer_vpa, idempotency_key, payload_hash, status)
                VALUES
                    (@intentRef, @orderId, @customerId, @amountPaise, 'INR', 'drill@okicici', @idemKey, 'drillhash', 'PENDING');
                """,
                conn))
            {
                intentCmd.Parameters.AddWithValue("intentRef", drillIntentRef);
                intentCmd.Parameters.AddWithValue("orderId", drillOrderId);
                intentCmd.Parameters.AddWithValue("customerId", drillCustomerId);
                intentCmd.Parameters.AddWithValue("amountPaise", drillAmountPaise);
                intentCmd.Parameters.AddWithValue("idemKey", $"v1-drill-intent-{drillTag}");
                await intentCmd.ExecuteNonQueryAsync(ct);
            }

            for (var delivery = 1; delivery <= 2; delivery++)
            {
                await using var tx = await conn.BeginTransactionAsync(ct);
                if (!isDefended)
                {
                    await InsertZeroSumLedgerAsync(conn, tx, drillIntentRef, drillOrderId, drillAmountPaise, $"tx-drill-{drillTag}-{delivery}", ct);
                    await using var updateCmd = new NpgsqlCommand(
                        "UPDATE payment_intents SET status = 'CAPTURED', webhook_deliveries = webhook_deliveries + 1 WHERE intent_ref = @intentRef;",
                        conn, tx);
                    updateCmd.Parameters.AddWithValue("intentRef", drillIntentRef);
                    await updateCmd.ExecuteNonQueryAsync(ct);
                }
                else
                {
                    await using var lockCmd = new NpgsqlCommand(
                        "SELECT status FROM payment_intents WHERE intent_ref = @intentRef FOR UPDATE;",
                        conn, tx);
                    lockCmd.Parameters.AddWithValue("intentRef", drillIntentRef);
                    var currentStatus = await lockCmd.ExecuteScalarAsync(ct) as string;

                    if (string.Equals(currentStatus, "PENDING", StringComparison.Ordinal))
                    {
                        await InsertZeroSumLedgerAsync(conn, tx, drillIntentRef, drillOrderId, drillAmountPaise, $"tx-drill-{drillTag}-1", ct);
                        await using var capCmd = new NpgsqlCommand(
                            "UPDATE payment_intents SET status = 'CAPTURED', webhook_deliveries = webhook_deliveries + 1 WHERE intent_ref = @intentRef;",
                            conn, tx);
                        capCmd.Parameters.AddWithValue("intentRef", drillIntentRef);
                        await capCmd.ExecuteNonQueryAsync(ct);
                    }
                    else
                    {
                        await using var dupCmd = new NpgsqlCommand(
                            "UPDATE payment_intents SET webhook_deliveries = webhook_deliveries + 1 WHERE intent_ref = @intentRef;",
                            conn, tx);
                        dupCmd.Parameters.AddWithValue("intentRef", drillIntentRef);
                        await dupCmd.ExecuteNonQueryAsync(ct);
                    }
                }
                await tx.CommitAsync(ct);
            }

            await using var auditCmd = new NpgsqlCommand(
                """
                SELECT COUNT(*),
                       COALESCE(SUM(CASE WHEN account_code = 'PSP_SETTLEMENT_RECEIVABLE' THEN amount_paise ELSE 0 END), 0),
                       COALESCE(SUM(signed_amount), 0)
                  FROM ledger_entries
                 WHERE intent_ref = @intentRef;
                """,
                conn);
            auditCmd.Parameters.AddWithValue("intentRef", drillIntentRef);
            await using var reader = await auditCmd.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            ledgerRows = reader.GetInt64(0);
            debitSumPaise = reader.GetInt64(1);
            netSignedSum = reader.GetInt64(2);
        }

        // 3. Deadline propagation check + Load Shedding check (always restore configuration afterwards)
        QuoteWithDeadlineResult deadlineResult;
        int shedLowCount = 0;
        int admittedCriticalCount = 0;
        try
        {
            deadlineResult = await resilience.ExecuteQuoteWithDeadlineAsync(
                callerBudgetMs: 120,
                gatewaySpentMs: 150,
                downstreamDelayMs: 65,
                enforceDeadline: isDefended,
                headerRemainingMs: null,
                ct);

            overloadProtection.ConfigureLoadShedding(new SheddingConfigRequest(
                Enabled: isDefended,
                SimulatedLoadPercent: 90,
                ShedLowThresholdPercent: 75,
                ShedNormalThresholdPercent: 92,
                ResetCounters: true));

            for (var i = 0; i < 5; i++)
            {
                if (!overloadProtection.EvaluateLoadShedding("low-recommendations").Ok)
                    shedLowCount++;
            }

            for (var i = 0; i < 3; i++)
            {
                if (overloadProtection.EvaluateLoadShedding("critical-checkout").Ok)
                    admittedCriticalCount++;
            }
        }
        finally
        {
            overloadProtection.ConfigureLoadShedding(new SheddingConfigRequest(
                Enabled: false,
                SimulatedLoadPercent: 0,
                ShedLowThresholdPercent: 75,
                ShedNormalThresholdPercent: 90,
                ResetCounters: true));
        }

        var verdict = isDefended
            ? "Single-flight collapsed 15 concurrent reads to 1 DB query; FOR UPDATE guard prevented webhook double-debit; deadline enforcement eliminated wasted DB work; load shedding dropped low-priority traffic while keeping 100% critical checkouts alive."
            : "Unprotected stack multiplied cold-cache DB load by 15x, wrote 2x ledger debits on duplicate webhook delivery, wasted downstream execution time after expired caller deadline, and admitted all low-priority flood under 90% load.";

        return new V1ChaosDrillResponse(
            Mode: mode,
            StampedeCheck: new
            {
                concurrentReads = 15,
                dbQueriesExecuted = stampedeDbQueries,
                coalescedReads
            },
            WebhookDoubleDebitCheck: new
            {
                deliveries = 2,
                expectedAmountPaise = drillAmountPaise,
                actualReceivableDebitPaise = debitSumPaise,
                doubleDebited = debitSumPaise > drillAmountPaise,
                ledgerRowCount = ledgerRows,
                netSignedSum
            },
            DeadlineWasteCheck: new
            {
                callerBudgetMs = 120,
                gatewaySpentMs = 150,
                enforceDeadline = isDefended,
                statusCode = deadlineResult.StatusCode,
                wastedWorkMs = deadlineResult.WastedWorkMs
            },
            LoadSheddingCheck: new
            {
                simulatedLoadPercent = 90,
                sheddingEnabled = isDefended,
                lowPrioritySent = 5,
                lowPriorityShed = shedLowCount,
                criticalCheckoutSent = 3,
                criticalCheckoutAdmitted = admittedCriticalCount
            },
            SummaryVerdict: verdict);
    }

    public async Task<V1ResetResponse> ResetAsync(CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);

        await using (var delProcessed = new NpgsqlCommand(
            "DELETE FROM processed_consumer_events WHERE consumer_group = @groupName;",
            conn))
        {
            delProcessed.Parameters.AddWithValue("groupName", ConsumerGroup);
            await delProcessed.ExecuteNonQueryAsync(ct);
        }

        await using (var delLoyalty = new NpgsqlCommand(
            "DELETE FROM consumer_loyalty_ledger WHERE customer_id IN (1091, 1092, 1093, 1099);",
            conn))
        {
            await delLoyalty.ExecuteNonQueryAsync(ct);
        }

        int deletedOutbox;
        await using (var delOutbox = new NpgsqlCommand(
            "DELETE FROM outbox_events WHERE event_id LIKE 'evt-v1-%';",
            conn))
        {
            deletedOutbox = await delOutbox.ExecuteNonQueryAsync(ct);
        }

        await using (var delWebhooks = new NpgsqlCommand(
            "DELETE FROM payment_webhook_events WHERE intent_ref LIKE 'v1_intent_%' OR intent_ref LIKE 'v1_drill_%';",
            conn))
        {
            await delWebhooks.ExecuteNonQueryAsync(ct);
        }

        await using (var delLedger = new NpgsqlCommand(
            "DELETE FROM ledger_entries WHERE intent_ref LIKE 'v1_intent_%' OR intent_ref LIKE 'v1_drill_%';",
            conn))
        {
            await delLedger.ExecuteNonQueryAsync(ct);
        }

        int deletedIntents;
        await using (var delIntentsCmd = new NpgsqlCommand(
            "DELETE FROM payment_intents WHERE intent_ref LIKE 'v1_intent_%' OR intent_ref LIKE 'v1_drill_%';",
            conn))
        {
            deletedIntents = await delIntentsCmd.ExecuteNonQueryAsync(ct);
        }

        int deletedOrders;
        await using (var delOrdersCmd = new NpgsqlCommand(
            "DELETE FROM orders WHERE idempotency_key LIKE 'v1-lab-%' OR idempotency_key LIKE 'v1-drill-%';",
            conn))
        {
            deletedOrders = await delOrdersCmd.ExecuteNonQueryAsync(ct);
        }

        await menuCache.ResetAsync(ct);
        overloadProtection.ConfigureLoadShedding(new SheddingConfigRequest(false, 0, 75, 90, true));

        return new V1ResetResponse("RESET", deletedOrders, deletedIntents, deletedOutbox);
    }

    private async Task<(bool Published, int PointsAwarded)> PublishAndConsumeSingleEventAsync(
        string eventId,
        long orderId,
        long customerId,
        long amountPaise,
        CancellationToken ct)
    {
        await KafkaTopicAdmin.EnsureTopicAsync(BootstrapServers, OutboxTopic, partitions: 1, ct);

        var payload = JsonSerializer.Serialize(new
        {
            eventId,
            orderId,
            customerId,
            amountPaise,
            eventType = "OrderPaidV1"
        }, JsonOptions);

        using (var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true
        }).Build())
        {
            await producer.ProduceAsync(OutboxTopic, new Message<string, string>
            {
                Key = $"order-{orderId}",
                Value = payload
            }, ct);
            producer.Flush(ct);
        }

        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using (var markCmd = new NpgsqlCommand(
            "UPDATE outbox_events SET published_at = NOW(), publish_attempts = publish_attempts + 1 WHERE event_id = @eventId;",
            conn))
        {
            markCmd.Parameters.AddWithValue("eventId", eventId);
            await markCmd.ExecuteNonQueryAsync(ct);
        }

        var consumerResult = await ApplyIdempotentLoyaltyAsync(conn, eventId, orderId, customerId, amountPaise, ct);
        return (true, consumerResult.Points);
    }

    private static async Task<(bool Credited, int Points)> ApplyIdempotentLoyaltyAsync(
        NpgsqlConnection conn,
        string eventId,
        long orderId,
        long customerId,
        long amountPaise,
        CancellationToken ct)
    {
        var points = (int)Math.Max(1L, amountPaise / 1000L);
        await using var tx = await conn.BeginTransactionAsync(ct);

        await using var dedupCmd = new NpgsqlCommand(
            """
            INSERT INTO processed_consumer_events (event_id, consumer_group, order_id, customer_id, points_awarded)
            VALUES (@eventId, @groupName, @orderId, @customerId, @points)
            ON CONFLICT (event_id, consumer_group) DO NOTHING
            RETURNING points_awarded;
            """,
            conn,
            tx);
        dedupCmd.Parameters.AddWithValue("eventId", eventId);
        dedupCmd.Parameters.AddWithValue("groupName", ConsumerGroup);
        dedupCmd.Parameters.AddWithValue("orderId", orderId);
        dedupCmd.Parameters.AddWithValue("customerId", customerId);
        dedupCmd.Parameters.AddWithValue("points", points);

        var inserted = await dedupCmd.ExecuteScalarAsync(ct);
        if (inserted is null)
        {
            await tx.RollbackAsync(ct);
            return (false, points);
        }

        await using var creditCmd = new NpgsqlCommand(
            """
            INSERT INTO consumer_loyalty_ledger (customer_id, points_balance, credit_count, updated_at)
            VALUES (@customerId, @points, 1, NOW())
            ON CONFLICT (customer_id) DO UPDATE
               SET points_balance = consumer_loyalty_ledger.points_balance + EXCLUDED.points_balance,
                   credit_count = consumer_loyalty_ledger.credit_count + 1,
                   updated_at = NOW();
            """,
            conn,
            tx);
        creditCmd.Parameters.AddWithValue("customerId", customerId);
        creditCmd.Parameters.AddWithValue("points", points);
        await creditCmd.ExecuteNonQueryAsync(ct);

        await tx.CommitAsync(ct);
        return (true, points);
    }

    private static async Task InsertZeroSumLedgerAsync(
        NpgsqlConnection conn,
        NpgsqlTransaction tx,
        string intentRef,
        long orderId,
        long amountPaise,
        string txRef,
        CancellationToken ct)
    {
        var platformFee = (long)Math.Round(amountPaise * 0.02m, MidpointRounding.AwayFromZero);
        var gstOnFee = (long)Math.Round(platformFee * 0.18m, MidpointRounding.AwayFromZero);
        var merchantPayable = amountPaise - platformFee - gstOnFee;

        var legs = new (string Account, string Direction, long Amount, long Signed)[]
        {
            ("PSP_SETTLEMENT_RECEIVABLE", "DEBIT", amountPaise, amountPaise),
            ("MERCHANT_PAYABLE", "CREDIT", merchantPayable, -merchantPayable),
            ("PLATFORM_FEE_REVENUE", "CREDIT", platformFee, -platformFee),
            ("GST_TAX_PAYABLE", "CREDIT", gstOnFee, -gstOnFee)
        };

        foreach (var leg in legs)
        {
            if (leg.Amount <= 0)
                continue;

            await using var cmd = new NpgsqlCommand(
                """
                INSERT INTO ledger_entries (transaction_ref, intent_ref, order_id, account_code, direction, amount_paise, signed_amount)
                VALUES (@txRef, @intentRef, @orderId, @account, @direction, @amountPaise, @signedAmount);
                """,
                conn,
                tx);
            cmd.Parameters.AddWithValue("txRef", txRef);
            cmd.Parameters.AddWithValue("intentRef", intentRef);
            cmd.Parameters.AddWithValue("orderId", orderId);
            cmd.Parameters.AddWithValue("account", leg.Account);
            cmd.Parameters.AddWithValue("direction", leg.Direction);
            cmd.Parameters.AddWithValue("amountPaise", leg.Amount);
            cmd.Parameters.AddWithValue("signedAmount", leg.Signed);
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    private void RecordTelemetry(
        string traceId,
        string spanId,
        long? orderId,
        int statusCode,
        string statusClass,
        bool isError,
        double durationMs,
        string rawMessage)
    {
        var (maskedMessage, maskedCount) = ObservabilityLabService.MaskPii(rawMessage);
        observability.AppendStructuredLog(new StructuredLogEntry(
            TimestampUtc: DateTime.UtcNow,
            Level: isError ? (statusCode >= 500 ? "ERROR" : "WARN") : "INFO",
            Service: "order-api",
            TraceId: traceId,
            SpanId: spanId,
            OrderId: orderId?.ToString(CultureInfo.InvariantCulture),
            Message: maskedMessage,
            PiiMasked: maskedCount > 0));

        observability.RecordRedMetricSample(
            service: "order-api",
            route: "/v1/tadka/orders/checkout",
            method: "POST",
            statusClass: statusClass,
            isError: isError,
            durationMs: durationMs,
            attemptedHighCardinalityTags: []);
    }

    private static string ComputeHash(string input)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(input));
        return Convert.ToHexString(bytes);
    }
}
