namespace Tadka.OrderApi.Modules.DistributedIds;

public sealed record SnowflakeParts(
    long Id,
    string IdString,
    long TimestampOffsetMs,
    DateTimeOffset UtcTimestamp,
    int NodeId,
    int Sequence,
    int KeyBytes = 8);

public sealed class ClockMovedBackwardsException(long driftMs)
    : InvalidOperationException($"Refusing to generate ID: system clock moved backwards by {driftMs} ms.")
{
    public long DriftMs { get; } = driftMs;
}

/// <summary>
/// 64-bit time-ordered distributed ID generator:
/// 1 sign bit (0) | 41 timestamp ms bits (~69.7 yrs) | 10 node bits (0..1023) | 12 sequence bits (0..4095/ms).
/// </summary>
public sealed class SnowflakeIdGenerator
{
    public const long TadkaEpochMs = 1_767_225_600_000L; // 2026-01-01T00:00:00Z
    public const int NodeBits = 10;
    public const int SequenceBits = 12;
    public const int MaxNodeId = (1 << NodeBits) - 1;       // 1023
    public const int MaxSequence = (1 << SequenceBits) - 1; // 4095

    private const int NodeShift = SequenceBits;
    private const int TimestampShift = SequenceBits + NodeBits;

    private readonly Lock _lock = new();
    private readonly int _nodeId;
    private readonly Func<long> _utcMsClock;
    private long _lastTimestampMs = -1L;
    private int _sequence;

    public SnowflakeIdGenerator(int nodeId = 1, Func<long>? utcMsClock = null)
    {
        if (nodeId is < 0 or > MaxNodeId)
            throw new ArgumentOutOfRangeException(nameof(nodeId), $"NodeId must be between 0 and {MaxNodeId}.");

        _nodeId = nodeId;
        _utcMsClock = utcMsClock ?? (() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
    }

    public int NodeId => _nodeId;

    public long NextId()
    {
        lock (_lock)
        {
            var nowMs = _utcMsClock();
            if (nowMs < _lastTimestampMs)
                throw new ClockMovedBackwardsException(_lastTimestampMs - nowMs);

            if (nowMs == _lastTimestampMs)
            {
                _sequence = (_sequence + 1) & MaxSequence;
                if (_sequence == 0)
                {
                    while (nowMs <= _lastTimestampMs)
                    {
                        nowMs = _utcMsClock();
                        if (nowMs < _lastTimestampMs)
                            throw new ClockMovedBackwardsException(_lastTimestampMs - nowMs);
                    }
                }
            }
            else
            {
                _sequence = 0;
            }

            _lastTimestampMs = nowMs;
            var offsetMs = nowMs - TadkaEpochMs;
            if (offsetMs < 0)
                throw new InvalidOperationException("Current clock is before Tadka custom epoch.");

            return (offsetMs << TimestampShift) | ((long)_nodeId << NodeShift) | (uint)_sequence;
        }
    }

    public static SnowflakeParts Decode(long id)
    {
        var offsetMs = id >> TimestampShift;
        var nodeId = (int)((id >> NodeShift) & MaxNodeId);
        var sequence = (int)(id & MaxSequence);
        return new SnowflakeParts(
            Id: id,
            IdString: id.ToString(),
            TimestampOffsetMs: offsetMs,
            UtcTimestamp: DateTimeOffset.FromUnixTimeMilliseconds(TadkaEpochMs + offsetMs),
            NodeId: nodeId,
            Sequence: sequence);
    }
}
