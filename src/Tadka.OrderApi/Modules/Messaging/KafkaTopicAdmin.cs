using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Tadka.OrderApi.Modules.Messaging;

public static class KafkaTopicAdmin
{
    public static async Task RecreateTopicAsync(
        string bootstrapServers,
        string topic,
        int partitions,
        CancellationToken ct)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = bootstrapServers
        }).Build();

        var meta = admin.GetMetadata(TimeSpan.FromSeconds(5));
        var exists = meta.Topics.Any(t =>
            string.Equals(t.Topic, topic, StringComparison.Ordinal) &&
            t.Error.Code != ErrorCode.UnknownTopicOrPart);

        if (exists)
        {
            try
            {
                await admin.DeleteTopicsAsync([topic]);
            }
            catch (DeleteTopicsException ex) when (
                ex.Results.All(r => r.Error.Code == ErrorCode.UnknownTopicOrPart))
            {
                // Already deleted concurrently.
            }

            var deleteDeadline = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < deleteDeadline)
            {
                ct.ThrowIfCancellationRequested();
                var check = admin.GetMetadata(TimeSpan.FromSeconds(3));
                var entry = check.Topics.FirstOrDefault(t => string.Equals(t.Topic, topic, StringComparison.Ordinal));
                if (entry is null || entry.Error.Code == ErrorCode.UnknownTopicOrPart)
                    break;
                await Task.Delay(150, ct);
            }
        }

        for (var attempt = 0; attempt < 20; attempt++)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                await admin.CreateTopicsAsync(
                [
                    new TopicSpecification
                    {
                        Name = topic,
                        NumPartitions = partitions,
                        ReplicationFactor = 1
                    }
                ]);
                return;
            }
            catch (CreateTopicsException ex) when (
                ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists) &&
                attempt < 19)
            {
                await Task.Delay(250, ct);
            }
        }
    }

    public static async Task EnsureTopicAsync(
        string bootstrapServers,
        string topic,
        int partitions,
        CancellationToken ct)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = bootstrapServers
        }).Build();

        var meta = admin.GetMetadata(TimeSpan.FromSeconds(5));
        var entry = meta.Topics.FirstOrDefault(t => string.Equals(t.Topic, topic, StringComparison.Ordinal));
        if (entry is not null && entry.Error.Code == ErrorCode.NoError && entry.Partitions.Count >= partitions)
            return;

        try
        {
            await admin.CreateTopicsAsync(
            [
                new TopicSpecification
                {
                    Name = topic,
                    NumPartitions = partitions,
                    ReplicationFactor = 1
                }
            ]);
        }
        catch (CreateTopicsException ex) when (
            ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // Created concurrently.
        }
    }
}
