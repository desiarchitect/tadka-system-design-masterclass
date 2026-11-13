using System.Diagnostics;

namespace Tadka.OrderApi.Instrumentation;

/// <summary>
/// Two separate timers for one request.
/// wait_ms: time spent acquiring a connection from the pool.
/// db_ms:   time spent executing the database command.
/// A value stays null if that step never started (for example, db_ms when the pool timed out).
/// </summary>
public sealed class RequestTimings
{
    private long _waitStart, _dbStart;

    public double? WaitMs { get; private set; }
    public double? DbMs { get; private set; }

    public void StartWait() => _waitStart = Stopwatch.GetTimestamp();
    public void StopWait() => WaitMs = Stopwatch.GetElapsedTime(_waitStart).TotalMilliseconds;

    public void StartDb() => _dbStart = Stopwatch.GetTimestamp();
    public void StopDb() => DbMs = Stopwatch.GetElapsedTime(_dbStart).TotalMilliseconds;

    /// <summary>
    /// If the pool times out, the wait timer never stopped. Record how long we waited before failing.
    /// </summary>
    public void StopWaitIfRunning()
    {
        if (WaitMs is null && _waitStart != 0) StopWait();
    }

    public static string Format(double? ms) => ms is null ? "-" : ms.Value.ToString("0");
}
