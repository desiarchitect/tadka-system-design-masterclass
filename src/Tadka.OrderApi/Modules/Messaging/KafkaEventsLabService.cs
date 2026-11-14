using System.Collections.Concurrent;
using System.Text.Json;
using Confluent.Kafka;

namespace Tadka.OrderApi.Modules.Messaging;

public sealed record KafkaPublishedEvent(
    string EventId,
    string Status,
    string? Key,
    int Partition,
    long Offset);

public sealed record KafkaLifecycleResponse(
    string Topic,
    string KeyStrategy,
    long OrderId,
    int PartitionsUsed,
    bool SinglePartitionOrdered,
    IReadOnlyList<KafkaPublishedEvent> Events);

public sealed record ConsumerMemberLayout(
    string MemberId,
    int[] AssignedPartitions,
    int ConsumedCount);

public sealed record ConsumerGroupLayoutSummary(
    string GroupName,
    string KafkaGroupId,
    int ConsumerCount,
    int ActiveConsumerCount,
    int IdleConsumerCount,
    int TotalDelivered,
    IReadOnlyList<ConsumerMemberLayout> Members);

public sealed record KafkaConsumerGroupLayoutResponse(
    string Topic,
    int PartitionCount,
    long TotalTopicMessages,
    ConsumerGroupLayoutSummary KitchenGroup,
    ConsumerGroupLayoutSummary AnalyticsGroup);

public sealed record RebalanceConsumerRecord(
    string ConsumerId,
    int Partition,
    long Offset,
    string EventId,
    string Status);

public sealed record KafkaRebalanceSimulationResponse(
    string Topic,
    string CommitMode,
    int TargetPartition,
    int PublishedCount,
    int TotalProcessedEvents,
    int DuplicateDeliveries,
    IReadOnlyList<long> Consumer1Offsets,
    IReadOnlyList<long> Consumer2Offsets,
    IReadOnlyList<RebalanceConsumerRecord> Timeline);

public sealed class KafkaEventsLabService(IConfiguration configuration, ILogger<KafkaEventsLabService> logger)
{
    private static readonly string[] LifecycleStages = ["placed", "preparing", "ready", "delivered"];
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private string BootstrapServers => configuration["Kafka:BootstrapServers"] ?? "127.0.0.1:9092";
    private string OrderEventsTopic => configuration["Kafka:OrderEventsTopic"] ?? "tadka.order.events";
    private int PartitionCount => configuration.GetValue<int?>("Kafka:PartitionCount") ?? 4;

    public async Task<object> ResetAsync(CancellationToken ct)
    {
        await KafkaTopicAdmin.RecreateTopicAsync(BootstrapServers, OrderEventsTopic, PartitionCount, ct);
        logger.LogInformation("kafka_topic_reset topic={Topic} partitions={Partitions}", OrderEventsTopic, PartitionCount);
        return new
        {
            topic = OrderEventsTopic,
            partitionCount = PartitionCount,
            reset = true
        };
    }

    public async Task<KafkaLifecycleResponse> PublishLifecycleAsync(
        string keyStrategy,
        long orderId,
        long customerId,
        long restaurantId,
        CancellationToken ct)
    {
        var normalized = string.IsNullOrWhiteSpace(keyStrategy)
            ? "order-id"
            : keyStrategy.Trim().ToLowerInvariant();
        if (normalized is not ("unkeyed" or "order-id"))
            throw new ArgumentException("keyStrategy must be 'unkeyed' or 'order-id'.");

        await KafkaTopicAdmin.EnsureTopicAsync(BootstrapServers, OrderEventsTopic, PartitionCount, ct);

        using var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true
        }).Build();

        var published = new List<KafkaPublishedEvent>(LifecycleStages.Length);
        var orderKey = $"order-{orderId}";

        for (var i = 0; i < LifecycleStages.Length; i++)
        {
            ct.ThrowIfCancellationRequested();
            var stage = LifecycleStages[i];
            var eventId = $"evt-{orderId}-{stage}-{Guid.NewGuid():N}"[..24];
            var payload = JsonSerializer.Serialize(new
            {
                eventId,
                orderId,
                customerId,
                restaurantId,
                status = stage,
                stageSeq = i + 1,
                occurredAt = DateTimeOffset.UtcNow
            }, JsonOptions);

            DeliveryResult<string, string> dr;
            if (normalized == "unkeyed")
            {
                // Explicitly cycle partitions 0..3 so single-machine localhost runs do not collapse
                // unkeyed messages onto one partition via librdkafka's sticky batcher.
                var targetPartition = new TopicPartition(OrderEventsTopic, new Partition(i % PartitionCount));
                dr = await producer.ProduceAsync(targetPartition, new Message<string, string>
                {
                    Key = string.Empty,
                    Value = payload
                }, ct);
            }
            else
            {
                dr = await producer.ProduceAsync(OrderEventsTopic, new Message<string, string>
                {
                    Key = orderKey,
                    Value = payload
                }, ct);
            }

            published.Add(new KafkaPublishedEvent(
                EventId: eventId,
                Status: stage,
                Key: normalized == "unkeyed" ? null : orderKey,
                Partition: dr.Partition.Value,
                Offset: dr.Offset.Value));
        }

        producer.Flush(ct);

        var distinctPartitions = published.Select(e => e.Partition).Distinct().Count();
        var orderedOffsets = distinctPartitions == 1 &&
                             published.Zip(published.Skip(1), (a, b) => b.Offset > a.Offset).All(x => x);

        logger.LogInformation(
            "kafka_lifecycle_published strategy={Strategy} order_id={OrderId} partitions_used={PartitionsUsed} ordered={Ordered}",
            normalized, orderId, distinctPartitions, orderedOffsets);

        return new KafkaLifecycleResponse(
            Topic: OrderEventsTopic,
            KeyStrategy: normalized,
            OrderId: orderId,
            PartitionsUsed: distinctPartitions,
            SinglePartitionOrdered: orderedOffsets,
            Events: published);
    }

    public async Task<KafkaConsumerGroupLayoutResponse> InspectConsumerGroupLayoutAsync(
        int consumerCount,
        CancellationToken ct)
    {
        if (consumerCount is < 1 or > 8)
            throw new ArgumentOutOfRangeException(nameof(consumerCount), "consumerCount must be between 1 and 8.");

        await KafkaTopicAdmin.EnsureTopicAsync(BootstrapServers, OrderEventsTopic, PartitionCount, ct);

        // Seed 4 events if the topic is completely empty so both groups observe delivered records.
        if (GetTopicMessageCount() == 0)
            await PublishLifecycleAsync("order-id", 9001, 42, 7, ct);

        var totalMessages = GetTopicMessageCount();
        var kitchenSummary = await RunIndependentGroupAsync("kitchen-group", consumerCount, totalMessages, ct);
        var analyticsSummary = await RunIndependentGroupAsync("analytics-group", 1, totalMessages, ct);

        return new KafkaConsumerGroupLayoutResponse(
            Topic: OrderEventsTopic,
            PartitionCount: PartitionCount,
            TotalTopicMessages: totalMessages,
            KitchenGroup: kitchenSummary,
            AnalyticsGroup: analyticsSummary);
    }

    public async Task<KafkaRebalanceSimulationResponse> SimulateRebalanceAsync(
        string commitMode,
        CancellationToken ct)
    {
        var normalized = string.IsNullOrWhiteSpace(commitMode)
            ? "cooperative-commit-on-revoke"
            : commitMode.Trim().ToLowerInvariant();
        if (normalized is not ("uncommitted-on-revoke" or "cooperative-commit-on-revoke"))
            throw new ArgumentException("commitMode must be 'uncommitted-on-revoke' or 'cooperative-commit-on-revoke'.");

        await KafkaTopicAdmin.RecreateTopicAsync(BootstrapServers, OrderEventsTopic, PartitionCount, ct);
        var seeded = await PublishLifecycleAsync("order-id", 9001, 42, 7, ct);
        var targetPartition = seeded.Events[0].Partition;

        var groupId = $"tadka-rebalance-{Guid.NewGuid():N}"[..24];
        var timeline = new List<RebalanceConsumerRecord>();
        var consumer1Offsets = new List<long>();
        var consumer2Offsets = new List<long>();

        var revokedSignal = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        TopicPartitionOffset? pendingOffsetToCommit = null;

        var c1Config = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            SessionTimeoutMs = 6000,
            HeartbeatIntervalMs = 1000
        };

        using (var consumer1 = new ConsumerBuilder<string, string>(c1Config)
            .SetPartitionsRevokedHandler((c, revokedPartitions) =>
            {
                if (normalized == "cooperative-commit-on-revoke" && pendingOffsetToCommit is not null)
                {
                    if (revokedPartitions.Any(tp => tp.TopicPartition == pendingOffsetToCommit.TopicPartition))
                    {
                        c.Commit([pendingOffsetToCommit]);
                    }
                }
                revokedSignal.TrySetResult(true);
            })
            .Build())
        {
            consumer1.Subscribe(OrderEventsTopic);

            var deadline = DateTime.UtcNow.AddSeconds(15);
            while (DateTime.UtcNow < deadline && consumer1Offsets.Count == 0)
            {
                ct.ThrowIfCancellationRequested();
                var result = consumer1.Consume(TimeSpan.FromMilliseconds(200));
                if (result is null || result.IsPartitionEOF)
                    continue;

                var doc = JsonDocument.Parse(result.Message.Value).RootElement;
                var eventId = doc.GetProperty("eventId").GetString() ?? "";
                var status = doc.GetProperty("status").GetString() ?? "";

                consumer1Offsets.Add(result.Offset.Value);
                timeline.Add(new RebalanceConsumerRecord(
                    "consumer-1", result.Partition.Value, result.Offset.Value, eventId, status));

                // Next offset to resume from after processing offset 0.
                pendingOffsetToCommit = new TopicPartitionOffset(result.TopicPartition, result.Offset + 1);
            }

            if (consumer1Offsets.Count == 0)
                throw new InvalidOperationException("consumer-1 timed out before reading the first order event from Kafka.");

            // Graceful leave triggers SetPartitionsRevokedHandler and hands ownership to consumer-2.
            consumer1.Close();
        }

        await revokedSignal.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);

        var c2Config = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            SessionTimeoutMs = 6000,
            HeartbeatIntervalMs = 1000
        };

        var expectedRemaining = normalized == "cooperative-commit-on-revoke" ? 3 : 4;
        using (var consumer2 = new ConsumerBuilder<string, string>(c2Config).Build())
        {
            consumer2.Subscribe(OrderEventsTopic);
            var deadline = DateTime.UtcNow.AddSeconds(15);
            while (DateTime.UtcNow < deadline && consumer2Offsets.Count < expectedRemaining)
            {
                ct.ThrowIfCancellationRequested();
                var result = consumer2.Consume(TimeSpan.FromMilliseconds(200));
                if (result is null || result.IsPartitionEOF)
                    continue;

                var doc = JsonDocument.Parse(result.Message.Value).RootElement;
                var eventId = doc.GetProperty("eventId").GetString() ?? "";
                var status = doc.GetProperty("status").GetString() ?? "";

                consumer2Offsets.Add(result.Offset.Value);
                timeline.Add(new RebalanceConsumerRecord(
                    "consumer-2", result.Partition.Value, result.Offset.Value, eventId, status));
                consumer2.Commit(result);
            }

            consumer2.Close();
        }

        var duplicates = consumer1Offsets.Intersect(consumer2Offsets).Count();
        var totalProcessed = consumer1Offsets.Count + consumer2Offsets.Count;

        logger.LogInformation(
            "kafka_rebalance_simulated mode={Mode} partition={Partition} total_processed={Total} duplicates={Duplicates}",
            normalized, targetPartition, totalProcessed, duplicates);

        return new KafkaRebalanceSimulationResponse(
            Topic: OrderEventsTopic,
            CommitMode: normalized,
            TargetPartition: targetPartition,
            PublishedCount: seeded.Events.Count,
            TotalProcessedEvents: totalProcessed,
            DuplicateDeliveries: duplicates,
            Consumer1Offsets: consumer1Offsets,
            Consumer2Offsets: consumer2Offsets,
            Timeline: timeline);
    }

    private long GetTopicMessageCount()
    {
        using var probe = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = $"tadka-wm-probe-{Guid.NewGuid():N}"[..24]
        }).Build();

        long total = 0;
        for (var p = 0; p < PartitionCount; p++)
        {
            var wm = probe.QueryWatermarkOffsets(new TopicPartition(OrderEventsTopic, new Partition(p)), TimeSpan.FromSeconds(5));
            total += Math.Max(0, wm.High.Value - wm.Low.Value);
        }

        return total;
    }

    private async Task<ConsumerGroupLayoutSummary> RunIndependentGroupAsync(
        string logicalGroupName,
        int consumerCount,
        long expectedMessages,
        CancellationToken ct)
    {
        var kafkaGroupId = $"tadka-{logicalGroupName}-{Guid.NewGuid():N}"[..28];
        var memberAssignments = new ConcurrentDictionary<string, int[]>();
        var memberCounts = new ConcurrentDictionary<string, int>();
        var deliveredOffsets = new ConcurrentDictionary<(int Partition, long Offset), string>();
        long totalDelivered = 0;
        long lastAssignmentTick = 0;

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(20));

        var workers = Enumerable.Range(1, consumerCount).Select(idx =>
        {
            var memberId = $"member-{idx}";
            memberAssignments[memberId] = [];
            memberCounts[memberId] = 0;

            return Task.Run(() =>
            {
                var config = new ConsumerConfig
                {
                    BootstrapServers = BootstrapServers,
                    GroupId = kafkaGroupId,
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    EnableAutoCommit = false,
                    PartitionAssignmentStrategy = PartitionAssignmentStrategy.Range,
                    SessionTimeoutMs = 6000,
                    HeartbeatIntervalMs = 1000
                };

                using var consumer = new ConsumerBuilder<string, string>(config)
                    .SetPartitionsAssignedHandler((_, partitions) =>
                    {
                        if (!cts.IsCancellationRequested)
                        {
                            memberAssignments[memberId] = partitions.Select(tp => tp.Partition.Value).OrderBy(x => x).ToArray();
                            Interlocked.Exchange(ref lastAssignmentTick, Environment.TickCount64);
                        }
                    })
                    .SetPartitionsRevokedHandler((_, _) =>
                    {
                        if (!cts.IsCancellationRequested)
                        {
                            memberAssignments[memberId] = [];
                            Interlocked.Exchange(ref lastAssignmentTick, Environment.TickCount64);
                        }
                    })
                    .Build();

                consumer.Subscribe(OrderEventsTopic);
                try
                {
                    while (!cts.Token.IsCancellationRequested)
                    {
                        var cr = consumer.Consume(TimeSpan.FromMilliseconds(100));
                        if (cr is null || cr.IsPartitionEOF)
                            continue;

                        if (deliveredOffsets.TryAdd((cr.Partition.Value, cr.Offset.Value), memberId))
                        {
                            memberCounts.AddOrUpdate(memberId, 1, (_, prev) => prev + 1);
                            Interlocked.Increment(ref totalDelivered);
                        }
                        try
                        {
                            consumer.Commit(cr);
                        }
                        catch (KafkaException)
                        {
                            // Ignore transient rebalance commit race during staggered startup.
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Expected on completion shutdown.
                }
                finally
                {
                    consumer.Close();
                }
            }, cts.Token);
        }).ToArray();

        var expectedActive = Math.Min(consumerCount, PartitionCount);
        var waitDeadline = DateTime.UtcNow.AddSeconds(16);
        while (DateTime.UtcNow < waitDeadline)
        {
            var assignedPartitions = memberAssignments.Values.Sum(arr => arr.Length);
            var currentActive = memberAssignments.Values.Count(arr => arr.Length > 0);
            var delivered = Interlocked.Read(ref totalDelivered);
            var lastTick = Interlocked.Read(ref lastAssignmentTick);
            var stableForMs = lastTick > 0 ? Environment.TickCount64 - lastTick : 0;

            if (assignedPartitions == PartitionCount &&
                currentActive == expectedActive &&
                delivered >= expectedMessages &&
                stableForMs >= 300)
            {
                break;
            }

            await Task.Delay(50, ct);
        }

        var snapshotAssignments = memberAssignments.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.ToArray());
        await cts.CancelAsync();
        await Task.WhenAll(workers);

        var members = Enumerable.Range(1, consumerCount)
            .Select(i => $"member-{i}")
            .Select(id => new ConsumerMemberLayout(
                MemberId: id,
                AssignedPartitions: snapshotAssignments[id],
                ConsumedCount: memberCounts[id]))
            .ToArray();

        var active = members.Count(m => m.AssignedPartitions.Length > 0);
        var idle = members.Count(m => m.AssignedPartitions.Length == 0);

        return new ConsumerGroupLayoutSummary(
            GroupName: logicalGroupName,
            KafkaGroupId: kafkaGroupId,
            ConsumerCount: consumerCount,
            ActiveConsumerCount: active,
            IdleConsumerCount: idle,
            TotalDelivered: (int)Interlocked.Read(ref totalDelivered),
            Members: members);
    }
}
