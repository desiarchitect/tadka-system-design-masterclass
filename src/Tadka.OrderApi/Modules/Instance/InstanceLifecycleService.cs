namespace Tadka.OrderApi.Modules.Instance;

public sealed class InstanceLifecycleService
{
    private readonly object _gate = new();
    private string _state = "healthy";
    private int _slowMs;
    private long _inFlightRequests;
    private long _totalRequestsServed;

    public InstanceLifecycleService(IConfiguration configuration)
    {
        var configured = configuration["INSTANCE_ID"] ?? configuration["Instance:Id"];
        InstanceId = string.IsNullOrWhiteSpace(configured) ? "node-a" : configured.Trim();
    }

    public string InstanceId { get; }

    public string State
    {
        get { lock (_gate) return _state; }
    }

    public int SlowMs
    {
        get { lock (_gate) return _slowMs; }
    }

    public long InFlightRequests => Interlocked.Read(ref _inFlightRequests);
    public long TotalRequestsServed => Interlocked.Read(ref _totalRequestsServed);

    public void BeginRequest(bool countServed)
    {
        Interlocked.Increment(ref _inFlightRequests);
        if (countServed)
            Interlocked.Increment(ref _totalRequestsServed);
    }

    public void EndRequest()
    {
        Interlocked.Decrement(ref _inFlightRequests);
    }

    public InstanceSnapshot Update(string? state, int? slowMs, bool resetCounters)
    {
        lock (_gate)
        {
            if (!string.IsNullOrWhiteSpace(state))
            {
                var normalized = state.Trim().ToLowerInvariant();
                if (normalized is not ("healthy" or "draining" or "unhealthy"))
                    throw new ArgumentException("state must be 'healthy', 'draining', or 'unhealthy'.");
                _state = normalized;
            }

            if (slowMs is { } ms)
            {
                if (ms is < 0 or > 2000)
                    throw new ArgumentOutOfRangeException(nameof(slowMs), "slowMs must be between 0 and 2000.");
                _slowMs = ms;
            }

            if (resetCounters)
                Interlocked.Exchange(ref _totalRequestsServed, 0);

            return GetSnapshotUnsafe();
        }
    }

    public InstanceSnapshot GetSnapshot()
    {
        lock (_gate) return GetSnapshotUnsafe();
    }

    private InstanceSnapshot GetSnapshotUnsafe() => new(
        Status: _state == "healthy" ? "ok" : _state,
        InstanceId: InstanceId,
        State: _state,
        InFlightRequests: Interlocked.Read(ref _inFlightRequests),
        SlowMs: _slowMs,
        TotalRequestsServed: Interlocked.Read(ref _totalRequestsServed));
}

public sealed record InstanceSnapshot(
    string Status,
    string InstanceId,
    string State,
    long InFlightRequests,
    int SlowMs,
    long TotalRequestsServed);
