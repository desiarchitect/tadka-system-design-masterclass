using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Npgsql;

namespace Tadka.OrderApi.Modules.Messaging;

public sealed record ReliableResetResponse(
    string Status,
    string ReliableTopic,
    string DlqTopic,
    long LoyaltyRows,
    long ProcessedEventRows,
    long DlqRows);

public sealed record DuplicateDeliveryResponse(
    string Topic,
    string Mode,
    string EventId,
    long CustomerId,
    long OrderId,
    int PointsPerEvent,
    int DeliveriesPublished,
    int DeliveriesConsumed,
    int DuplicatesSuppressed,
    int DuplicateCreditsApplied,
    int PointsBalance,
    int CreditCount,
    long ProcessedEventRowCount);

public sealed record DlqRecordSummary(
    string EventId,
    string OriginalTopic,
    string DlqTopic,
    int OriginalPartition,
    long OriginalOffset,
    int AttemptCount,
    string FailureReason);

public sealed record PoisonPipelineResponse(
    string Topic,
    string DlqTopic,
    string Policy,
    int MaxAttempts,
    int TotalPublished,
    int ValidProcessedCount,
    int HeadOfLineBlockedCount,
    string? BlockedBehindEventId,
    int AttemptCountOnPoison,
    int[] BackoffScheduleMs,
    int DlqRoutedCount,
    bool DlqMessageVerifiedOnKafka,
    DlqRecordSummary? DlqRecord,
    IReadOnlyList<long> ProcessedCustomerIds,
    IReadOnlyList<string> UnprocessedValidEvents);

public sealed class ReliableConsumerLabService(
    IConfiguration configuration,
    NpgsqlDataSource dataSource,
    ILogger<ReliableConsumerLabService> logger)
{
    private const string LogicalConsumerGroup = "loyalty-consumer-group";
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private string BootstrapServers => configuration["Kafka:BootstrapServers"] ?? "127.0.0.1:9092";
    private string ReliableTopic => configuration["Kafka:ReliableTopic"] ?? "tadka.order.reliable";
    private string DlqTopic => configuration["Kafka:DlqTopic"] ?? "tadka.order.reliable.dlq";

    public async Task<ReliableResetResponse> ResetAsync(CancellationToken ct)
    {
        await using (var conn = await dataSource.OpenConnectionAsync(ct))
        await using (var cmd = new NpgsqlCommand(
            "TRUNCATE TABLE consumer_loyalty_ledger, processed_consumer_events, consumer_dlq_records RESTART IDENTITY;",
            conn))
        {
            await cmd.ExecuteNonQueryAsync(ct);
        }

        await KafkaTopicAdmin.RecreateTopicAsync(BootstrapServers, ReliableTopic, partitions: 1, ct);
        await KafkaTopicAdmin.RecreateTopicAsync(BootstrapServers, DlqTopic, partitions: 1, ct);

        logger.LogInformation("kafka_reliable_reset reliable_topic={ReliableTopic} dlq_topic={DlqTopic}", ReliableTopic, DlqTopic);

        return new ReliableResetResponse(
            Status: "RESET",
            ReliableTopic: ReliableTopic,
            DlqTopic: DlqTopic,
            LoyaltyRows: 0,
            ProcessedEventRows: 0,
            DlqRows: 0);
    }

    public async Task<DuplicateDeliveryResponse> SimulateDuplicateDeliveryAsync(
        string mode,
        long customerId,
        long orderId,
        int points,
        int deliveries,
        CancellationToken ct)
    {
        var normalized = string.IsNullOrWhiteSpace(mode)
            ? "idempotent"
            : mode.Trim().ToLowerInvariant();
        if (normalized is not ("naive" or "idempotent"))
            throw new ArgumentException("mode must be 'naive' or 'idempotent'.");
        if (deliveries is < 2 or > 10)
            throw new ArgumentOutOfRangeException(nameof(deliveries), "deliveries must be between 2 and 10.");
        if (points <= 0)
            throw new ArgumentOutOfRangeException(nameof(points), "points must be positive.");

        var eventId = $"evt-loyalty-order-{orderId}";

        await KafkaTopicAdmin.RecreateTopicAsync(BootstrapServers, ReliableTopic, partitions: 1, ct);

        await using (var conn = await dataSource.OpenConnectionAsync(ct))
        await using (var cleanCmd = new NpgsqlCommand(
            """
            DELETE FROM consumer_loyalty_ledger WHERE customer_id = @customerId;
            DELETE FROM processed_consumer_events WHERE event_id = @eventId AND consumer_group = @groupName;
            """,
            conn))
        {
            cleanCmd.Parameters.AddWithValue("customerId", customerId);
            cleanCmd.Parameters.AddWithValue("eventId", eventId);
            cleanCmd.Parameters.AddWithValue("groupName", LogicalConsumerGroup);
            await cleanCmd.ExecuteNonQueryAsync(ct);
        }

        using (var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true
        }).Build())
        {
            var payload = JsonSerializer.Serialize(new
            {
                eventId,
                orderId,
                customerId,
                points
            }, JsonOptions);

            for (var i = 0; i < deliveries; i++)
            {
                await producer.ProduceAsync(ReliableTopic, new Message<string, string>
                {
                    Key = $"order-{orderId}",
                    Value = payload
                }, ct);
            }

            producer.Flush(ct);
        }

        var kafkaGroupId = $"tadka-dup-{normalized}-{Guid.NewGuid():N}"[..28];
        var consumedCount = 0;
        var duplicatesSuppressed = 0;

        using (var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = kafkaGroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            SessionTimeoutMs = 6000,
            HeartbeatIntervalMs = 1000
        }).Build())
        {
            consumer.Subscribe(ReliableTopic);
            var deadline = DateTime.UtcNow.AddSeconds(15);

            await using var conn = await dataSource.OpenConnectionAsync(ct);

            while (DateTime.UtcNow < deadline && consumedCount < deliveries)
            {
                ct.ThrowIfCancellationRequested();
                var cr = consumer.Consume(TimeSpan.FromMilliseconds(200));
                if (cr is null || cr.IsPartitionEOF)
                    continue;

                consumedCount++;
                var root = JsonDocument.Parse(cr.Message.Value).RootElement;
                var msgEventId = root.GetProperty("eventId").GetString() ?? eventId;
                var msgOrderId = root.GetProperty("orderId").GetInt64();
                var msgCustomerId = root.GetProperty("customerId").GetInt64();
                var msgPoints = root.GetProperty("points").GetInt32();

                if (normalized == "naive")
                {
                    await using var upsertCmd = new NpgsqlCommand(
                        """
                        INSERT INTO consumer_loyalty_ledger (customer_id, points_balance, credit_count, updated_at)
                        VALUES (@customerId, @points, 1, NOW())
                        ON CONFLICT (customer_id) DO UPDATE
                        SET points_balance = consumer_loyalty_ledger.points_balance + EXCLUDED.points_balance,
                            credit_count = consumer_loyalty_ledger.credit_count + 1,
                            updated_at = NOW();
                        """,
                        conn);
                    upsertCmd.Parameters.AddWithValue("customerId", msgCustomerId);
                    upsertCmd.Parameters.AddWithValue("points", msgPoints);
                    await upsertCmd.ExecuteNonQueryAsync(ct);
                }
                else
                {
                    await using var tx = await conn.BeginTransactionAsync(ct);

                    await using var dedupCmd = new NpgsqlCommand(
                        """
                        INSERT INTO processed_consumer_events (event_id, consumer_group, order_id, customer_id, points_awarded)
                        VALUES (@eventId, @groupName, @orderId, @customerId, @points)
                        ON CONFLICT (event_id, consumer_group) DO NOTHING;
                        """,
                        conn,
                        tx);
                    dedupCmd.Parameters.AddWithValue("eventId", msgEventId);
                    dedupCmd.Parameters.AddWithValue("groupName", LogicalConsumerGroup);
                    dedupCmd.Parameters.AddWithValue("orderId", msgOrderId);
                    dedupCmd.Parameters.AddWithValue("customerId", msgCustomerId);
                    dedupCmd.Parameters.AddWithValue("points", msgPoints);

                    var inserted = await dedupCmd.ExecuteNonQueryAsync(ct);
                    if (inserted == 1)
                    {
                        await using var upsertCmd = new NpgsqlCommand(
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
                        upsertCmd.Parameters.AddWithValue("customerId", msgCustomerId);
                        upsertCmd.Parameters.AddWithValue("points", msgPoints);
                        await upsertCmd.ExecuteNonQueryAsync(ct);
                    }
                    else
                    {
                        duplicatesSuppressed++;
                    }

                    await tx.CommitAsync(ct);
                }

                consumer.Commit(cr);
            }

            consumer.Close();
        }

        int pointsBalance = 0;
        int creditCount = 0;
        long processedRowCount = 0;

        await using (var conn = await dataSource.OpenConnectionAsync(ct))
        {
            await using (var ledgerCmd = new NpgsqlCommand(
                "SELECT points_balance, credit_count FROM consumer_loyalty_ledger WHERE customer_id = @customerId;",
                conn))
            {
                ledgerCmd.Parameters.AddWithValue("customerId", customerId);
                await using var reader = await ledgerCmd.ExecuteReaderAsync(ct);
                if (await reader.ReadAsync(ct))
                {
                    pointsBalance = reader.GetInt32(0);
                    creditCount = reader.GetInt32(1);
                }
            }

            await using (var countCmd = new NpgsqlCommand(
                "SELECT COUNT(*) FROM processed_consumer_events WHERE event_id = @eventId AND consumer_group = @groupName;",
                conn))
            {
                countCmd.Parameters.AddWithValue("eventId", eventId);
                countCmd.Parameters.AddWithValue("groupName", LogicalConsumerGroup);
                processedRowCount = (long)(await countCmd.ExecuteScalarAsync(ct) ?? 0L);
            }
        }

        logger.LogInformation(
            "kafka_duplicate_delivery mode={Mode} customer_id={CustomerId} points_balance={PointsBalance} credit_count={CreditCount} suppressed={Suppressed}",
            normalized, customerId, pointsBalance, creditCount, duplicatesSuppressed);

        return new DuplicateDeliveryResponse(
            Topic: ReliableTopic,
            Mode: normalized,
            EventId: eventId,
            CustomerId: customerId,
            OrderId: orderId,
            PointsPerEvent: points,
            DeliveriesPublished: deliveries,
            DeliveriesConsumed: consumedCount,
            DuplicatesSuppressed: duplicatesSuppressed,
            DuplicateCreditsApplied: Math.Max(0, creditCount - 1),
            PointsBalance: pointsBalance,
            CreditCount: creditCount,
            ProcessedEventRowCount: processedRowCount);
    }

    public async Task<PoisonPipelineResponse> RunPoisonPipelineAsync(
        string policy,
        int maxAttempts,
        CancellationToken ct)
    {
        var normalized = string.IsNullOrWhiteSpace(policy)
            ? "retry-then-dlq"
            : policy.Trim().ToLowerInvariant();
        if (normalized is not ("block-on-poison" or "retry-then-dlq"))
            throw new ArgumentException("policy must be 'block-on-poison' or 'retry-then-dlq'.");
        if (maxAttempts is < 1 or > 5)
            throw new ArgumentOutOfRangeException(nameof(maxAttempts), "maxAttempts must be between 1 and 5.");

        await KafkaTopicAdmin.RecreateTopicAsync(BootstrapServers, ReliableTopic, partitions: 1, ct);
        await KafkaTopicAdmin.RecreateTopicAsync(BootstrapServers, DlqTopic, partitions: 1, ct);

        await using (var conn = await dataSource.OpenConnectionAsync(ct))
        await using (var cleanCmd = new NpgsqlCommand(
            """
            DELETE FROM consumer_loyalty_ledger WHERE customer_id IN (601, 602, 603, 604);
            DELETE FROM processed_consumer_events WHERE event_id IN ('evt-valid-1', 'evt-poison-2', 'evt-valid-3', 'evt-valid-4');
            DELETE FROM consumer_dlq_records WHERE event_id = 'evt-poison-2';
            """,
            conn))
        {
            await cleanCmd.ExecuteNonQueryAsync(ct);
        }

        var testBatch = new[]
        {
            new { eventId = "evt-valid-1", orderId = 9101L, customerId = 601L, points = 25, malformation = (string?)null },
            new { eventId = "evt-poison-2", orderId = 9102L, customerId = 602L, points = -999, malformation = (string?)"NEGATIVE_POINTS_SCHEMA_VIOLATION" },
            new { eventId = "evt-valid-3", orderId = 9103L, customerId = 603L, points = 30, malformation = (string?)null },
            new { eventId = "evt-valid-4", orderId = 9104L, customerId = 604L, points = 40, malformation = (string?)null }
        };

        using (var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true
        }).Build())
        {
            foreach (var item in testBatch)
            {
                var payload = JsonSerializer.Serialize(item, JsonOptions);
                await producer.ProduceAsync(new TopicPartition(ReliableTopic, new Partition(0)), new Message<string, string>
                {
                    Key = $"order-{item.orderId}",
                    Value = payload
                }, ct);
            }

            producer.Flush(ct);
        }

        var kafkaGroupId = $"tadka-poison-{normalized}-{Guid.NewGuid():N}"[..28];
        var processedCustomerIds = new List<long>();
        var unprocessedValidEvents = new List<string>();
        // One wait between consecutive attempts, so N attempts have N - 1 waits.
        var backoffSchedule = Enumerable.Range(0, maxAttempts - 1).Select(i => 100 * (1 << i)).ToArray();
        string? blockedBehindEventId = null;
        var attemptCountOnPoison = 0;
        DlqRecordSummary? dlqRecord = null;
        var messagesSeen = 0;

        using var dlqProducer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true
        }).Build();

        using (var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = kafkaGroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            SessionTimeoutMs = 6000,
            HeartbeatIntervalMs = 1000
        }).Build())
        {
            consumer.Subscribe(ReliableTopic);
            var deadline = DateTime.UtcNow.AddSeconds(20);

            await using var conn = await dataSource.OpenConnectionAsync(ct);

            while (DateTime.UtcNow < deadline && messagesSeen < testBatch.Length)
            {
                ct.ThrowIfCancellationRequested();
                var cr = consumer.Consume(TimeSpan.FromMilliseconds(200));
                if (cr is null || cr.IsPartitionEOF)
                    continue;

                messagesSeen++;
                var raw = cr.Message.Value;
                var root = JsonDocument.Parse(raw).RootElement;
                var eventId = root.GetProperty("eventId").GetString() ?? "";
                var orderId = root.GetProperty("orderId").GetInt64();
                var customerId = root.GetProperty("customerId").GetInt64();
                var points = root.GetProperty("points").GetInt32();
                var malformation = root.TryGetProperty("malformation", out var malProp) && malProp.ValueKind == JsonValueKind.String
                    ? malProp.GetString()
                    : null;

                if (points > 0 && string.IsNullOrEmpty(malformation))
                {
                    await using var tx = await conn.BeginTransactionAsync(ct);

                    await using var dedupCmd = new NpgsqlCommand(
                        """
                        INSERT INTO processed_consumer_events (event_id, consumer_group, order_id, customer_id, points_awarded)
                        VALUES (@eventId, @groupName, @orderId, @customerId, @points)
                        ON CONFLICT (event_id, consumer_group) DO NOTHING;
                        """,
                        conn,
                        tx);
                    dedupCmd.Parameters.AddWithValue("eventId", eventId);
                    dedupCmd.Parameters.AddWithValue("groupName", LogicalConsumerGroup);
                    dedupCmd.Parameters.AddWithValue("orderId", orderId);
                    dedupCmd.Parameters.AddWithValue("customerId", customerId);
                    dedupCmd.Parameters.AddWithValue("points", points);

                    var inserted = await dedupCmd.ExecuteNonQueryAsync(ct);
                    if (inserted == 1)
                    {
                        await using var upsertCmd = new NpgsqlCommand(
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
                        upsertCmd.Parameters.AddWithValue("customerId", customerId);
                        upsertCmd.Parameters.AddWithValue("points", points);
                        await upsertCmd.ExecuteNonQueryAsync(ct);
                    }

                    await tx.CommitAsync(ct);
                    consumer.Commit(cr);
                    processedCustomerIds.Add(customerId);
                    continue;
                }

                // Poison message detected: retry with bounded exponential backoff.
                var failureReason = malformation ?? "NEGATIVE_POINTS_SCHEMA_VIOLATION";
                for (var attempt = 0; attempt < maxAttempts; attempt++)
                {
                    attemptCountOnPoison++;
                    if (attempt < backoffSchedule.Length)
                    {
                        await Task.Delay(backoffSchedule[attempt], ct);
                    }
                }

                if (normalized == "block-on-poison")
                {
                    // Do NOT advance offset when refusing to lose an unprocessable record without a DLQ.
                    blockedBehindEventId = eventId;
                    unprocessedValidEvents.AddRange(
                        testBatch.Skip(messagesSeen).Where(b => b.points > 0).Select(b => b.eventId));
                    break;
                }

                var headers = new Headers
                {
                    { "x-original-topic", Encoding.UTF8.GetBytes(ReliableTopic) },
                    { "x-original-partition", Encoding.UTF8.GetBytes(cr.Partition.Value.ToString()) },
                    { "x-original-offset", Encoding.UTF8.GetBytes(cr.Offset.Value.ToString()) },
                    { "x-failure-reason", Encoding.UTF8.GetBytes(failureReason) },
                    { "x-attempt-count", Encoding.UTF8.GetBytes(attemptCountOnPoison.ToString()) }
                };

                await dlqProducer.ProduceAsync(DlqTopic, new Message<string, string>
                {
                    Key = cr.Message.Key,
                    Value = raw,
                    Headers = headers
                }, ct);
                dlqProducer.Flush(ct);

                await using (var dlqInsertCmd = new NpgsqlCommand(
                    """
                    INSERT INTO consumer_dlq_records
                        (event_id, original_topic, dlq_topic, partition_id, offset_value, attempt_count, failure_reason, raw_payload)
                    VALUES
                        (@eventId, @origTopic, @dlqTopic, @partitionId, @offsetValue, @attemptCount, @reason, @rawPayload)
                    ON CONFLICT (event_id) DO UPDATE
                    SET attempt_count = EXCLUDED.attempt_count,
                        failure_reason = EXCLUDED.failure_reason,
                        routed_at = NOW();
                    """,
                    conn))
                {
                    dlqInsertCmd.Parameters.AddWithValue("eventId", eventId);
                    dlqInsertCmd.Parameters.AddWithValue("origTopic", ReliableTopic);
                    dlqInsertCmd.Parameters.AddWithValue("dlqTopic", DlqTopic);
                    dlqInsertCmd.Parameters.AddWithValue("partitionId", cr.Partition.Value);
                    dlqInsertCmd.Parameters.AddWithValue("offsetValue", cr.Offset.Value);
                    dlqInsertCmd.Parameters.AddWithValue("attemptCount", attemptCountOnPoison);
                    dlqInsertCmd.Parameters.AddWithValue("reason", failureReason);
                    dlqInsertCmd.Parameters.AddWithValue("rawPayload", raw);
                    await dlqInsertCmd.ExecuteNonQueryAsync(ct);
                }

                dlqRecord = new DlqRecordSummary(
                    EventId: eventId,
                    OriginalTopic: ReliableTopic,
                    DlqTopic: DlqTopic,
                    OriginalPartition: cr.Partition.Value,
                    OriginalOffset: cr.Offset.Value,
                    AttemptCount: attemptCountOnPoison,
                    FailureReason: failureReason);

                // Commit past the poison message only after DLQ persistence succeeded.
                consumer.Commit(cr);
            }

            consumer.Close();
        }

        var dlqVerifiedOnKafka = false;
        if (dlqRecord is not null)
        {
            using var dlqConsumer = new ConsumerBuilder<string, string>(new ConsumerConfig
            {
                BootstrapServers = BootstrapServers,
                GroupId = $"tadka-dlq-verify-{Guid.NewGuid():N}"[..28],
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                SessionTimeoutMs = 6000,
                HeartbeatIntervalMs = 1000
            }).Build();

            dlqConsumer.Subscribe(DlqTopic);
            var verifyDeadline = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < verifyDeadline)
            {
                var cr = dlqConsumer.Consume(TimeSpan.FromMilliseconds(200));
                if (cr is null || cr.IsPartitionEOF)
                    continue;

                var parsed = JsonDocument.Parse(cr.Message.Value).RootElement;
                if (parsed.GetProperty("eventId").GetString() == dlqRecord.EventId &&
                    cr.Message.Headers.Any(h => h.Key == "x-failure-reason"))
                {
                    dlqVerifiedOnKafka = true;
                    break;
                }
            }

            dlqConsumer.Close();
        }

        logger.LogInformation(
            "kafka_poison_pipeline policy={Policy} valid_processed={ValidCount} hol_blocked={BlockedCount} dlq_routed={DlqCount}",
            normalized, processedCustomerIds.Count, unprocessedValidEvents.Count, dlqRecord is null ? 0 : 1);

        return new PoisonPipelineResponse(
            Topic: ReliableTopic,
            DlqTopic: DlqTopic,
            Policy: normalized,
            MaxAttempts: maxAttempts,
            TotalPublished: testBatch.Length,
            ValidProcessedCount: processedCustomerIds.Count,
            HeadOfLineBlockedCount: unprocessedValidEvents.Count,
            BlockedBehindEventId: blockedBehindEventId,
            AttemptCountOnPoison: attemptCountOnPoison,
            BackoffScheduleMs: backoffSchedule,
            DlqRoutedCount: dlqRecord is null ? 0 : 1,
            DlqMessageVerifiedOnKafka: dlqVerifiedOnKafka,
            DlqRecord: dlqRecord,
            ProcessedCustomerIds: processedCustomerIds,
            UnprocessedValidEvents: unprocessedValidEvents);
    }
}
