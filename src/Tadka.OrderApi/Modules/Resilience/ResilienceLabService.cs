using System.Diagnostics;

namespace Tadka.OrderApi.Modules.Resilience;

public sealed record ResilienceConfigRequest(
    string? Mode,
    int? DelayMs,
    int? PerTryTimeoutMs,
    int? OverallDeadlineMs,
    int? MaxRetries,
    int? RetryBudgetPercent,
    int? CircuitBreakerFailureThreshold,
    int? CircuitBreakerOpenMs,
    bool? ResetCounters);

public sealed record ResilienceStatusSnapshot(
    string Mode,
    int DelayMs,
    int PerTryTimeoutMs,
    int OverallDeadlineMs,
    int MaxRetries,
    int RetryBudgetPercent,
    int CircuitBreakerFailureThreshold,
    int CircuitBreakerOpenMs,
    string CircuitBreakerState,
    int ConsecutiveFailures,
    double RetryBudgetTokens,
    long ClientRequests,
    long DownstreamAttempts,
    long DownstreamSuccesses,
    long DownstreamTimeouts,
    long DownstreamFailures,
    long RetriesExecuted,
    long RetriesBlockedByBudget,
    long DeadlineExceededCount,
    long CircuitFastFails,
    long CircuitTransitions);

public sealed record QuoteWithDeadlineRequest(
    int? CallerBudgetMs,
    int? GatewaySpentMs,
    int? DownstreamDelayMs,
    bool? EnforceDeadline);

public sealed record QuoteWithDeadlineResult(
    int StatusCode,
    bool Ok,
    string Reason,
    bool EnforceDeadline,
    int CallerBudgetMs,
    int GatewaySpentMs,
    int RemainingBudgetMs,
    int DownstreamDelayMs,
    long ActualWorkMs,
    long WastedWorkMs,
    bool DeadlineExceeded,
    long RemainingAfterMs);

public sealed record DispatchProbeRequest(
    string? Mode,
    int? Hops,
    int? PerHopAttempts,
    int? BaseBackoffMs,
    int? CapBackoffMs);

public sealed record DispatchProbeResult(
    int StatusCode,
    bool Ok,
    string Mode,
    string Reason,
    int Hops,
    int AttemptsThisCall,
    int RetriesThisCall,
    int BlockedRetriesThisCall,
    IReadOnlyList<int> BackoffDelaysMs,
    int AmplificationFactor,
    double RetryBudgetTokensRemaining);

public sealed record JitterSampleResponse(
    int BaseMs,
    int CapMs,
    int Attempt,
    int CeilingMs,
    int Samples,
    IReadOnlyList<int> FixedBackoffsMs,
    int FixedSpreadMs,
    int DistinctFixedValues,
    IReadOnlyList<int> FullJitterBackoffsMs,
    int FullJitterSpreadMs,
    int DistinctFullJitterValues);

public sealed record ProtectedCallResult(
    int StatusCode,
    bool Ok,
    string PreviousCircuitState,
    string CircuitState,
    bool FastFailed,
    bool DownstreamCalled,
    int ConsecutiveFailures,
    long ElapsedMs,
    string Reason);

public sealed class ResilienceLabService(ILogger<ResilienceLabService> logger)
{
    private const double MaxRetryBudgetTokens = 5.0;
    private const double InitialRetryBudgetTokens = 2.0;

    private readonly object _gate = new();

    private string _mode = "healthy";
    private int _delayMs;
    private int _perTryTimeoutMs = 250;
    private int _overallDeadlineMs = 800;
    private int _maxRetries = 2;
    private int _retryBudgetPercent = 20;
    private int _cbFailureThreshold = 3;
    private int _cbOpenMs = 500;

    private string _cbState = "closed";
    private int _consecutiveFailures;
    private DateTimeOffset _cbOpenedAtUtc = DateTimeOffset.MinValue;
    private bool _halfOpenProbeInFlight;
    private double _retryBudgetTokens = InitialRetryBudgetTokens;

    private long _clientRequests;
    private long _downstreamAttempts;
    private long _downstreamSuccesses;
    private long _downstreamTimeouts;
    private long _downstreamFailures;
    private long _retriesExecuted;
    private long _retriesBlockedByBudget;
    private long _deadlineExceededCount;
    private long _circuitFastFails;
    private long _circuitTransitions;

    public ResilienceStatusSnapshot Configure(ResilienceConfigRequest req)
    {
        lock (_gate)
        {
            if (!string.IsNullOrWhiteSpace(req.Mode))
            {
                var normalized = req.Mode.Trim().ToLowerInvariant();
                if (normalized is not ("healthy" or "slow" or "failing"))
                    throw new ArgumentException("mode must be 'healthy', 'slow', or 'failing'.");
                _mode = normalized;
            }

            if (req.DelayMs is { } d)
            {
                if (d is < 0 or > 5000)
                    throw new ArgumentOutOfRangeException(nameof(req.DelayMs), "delayMs must be between 0 and 5000.");
                _delayMs = d;
            }

            if (req.PerTryTimeoutMs is { } ptt)
            {
                if (ptt is < 50 or > 5000)
                    throw new ArgumentOutOfRangeException(nameof(req.PerTryTimeoutMs), "perTryTimeoutMs must be between 50 and 5000.");
                _perTryTimeoutMs = ptt;
            }

            if (req.OverallDeadlineMs is { } od)
            {
                if (od is < 50 or > 10000)
                    throw new ArgumentOutOfRangeException(nameof(req.OverallDeadlineMs), "overallDeadlineMs must be between 50 and 10000.");
                _overallDeadlineMs = od;
            }

            if (req.MaxRetries is { } mr)
            {
                if (mr is < 0 or > 5)
                    throw new ArgumentOutOfRangeException(nameof(req.MaxRetries), "maxRetries must be between 0 and 5.");
                _maxRetries = mr;
            }

            if (req.RetryBudgetPercent is { } rbp)
            {
                if (rbp is < 0 or > 100)
                    throw new ArgumentOutOfRangeException(nameof(req.RetryBudgetPercent), "retryBudgetPercent must be between 0 and 100.");
                _retryBudgetPercent = rbp;
                if (rbp == 0)
                    _retryBudgetTokens = 0.0;
            }

            if (req.CircuitBreakerFailureThreshold is { } cbft)
            {
                if (cbft is < 1 or > 20)
                    throw new ArgumentOutOfRangeException(nameof(req.CircuitBreakerFailureThreshold), "circuitBreakerFailureThreshold must be between 1 and 20.");
                _cbFailureThreshold = cbft;
            }

            if (req.CircuitBreakerOpenMs is { } cbom)
            {
                if (cbom is < 100 or > 30000)
                    throw new ArgumentOutOfRangeException(nameof(req.CircuitBreakerOpenMs), "circuitBreakerOpenMs must be between 100 and 30000.");
                _cbOpenMs = cbom;
            }

            if (req.ResetCounters == true)
            {
                _cbState = "closed";
                _consecutiveFailures = 0;
                _cbOpenedAtUtc = DateTimeOffset.MinValue;
                _halfOpenProbeInFlight = false;
                _retryBudgetTokens = _retryBudgetPercent == 0 ? 0.0 : InitialRetryBudgetTokens;

                Interlocked.Exchange(ref _clientRequests, 0);
                Interlocked.Exchange(ref _downstreamAttempts, 0);
                Interlocked.Exchange(ref _downstreamSuccesses, 0);
                Interlocked.Exchange(ref _downstreamTimeouts, 0);
                Interlocked.Exchange(ref _downstreamFailures, 0);
                Interlocked.Exchange(ref _retriesExecuted, 0);
                Interlocked.Exchange(ref _retriesBlockedByBudget, 0);
                Interlocked.Exchange(ref _deadlineExceededCount, 0);
                Interlocked.Exchange(ref _circuitFastFails, 0);
                Interlocked.Exchange(ref _circuitTransitions, 0);
            }

            return GetSnapshotUnsafe();
        }
    }

    public ResilienceStatusSnapshot GetStatus()
    {
        lock (_gate)
        {
            return GetSnapshotUnsafe();
        }
    }

    public async Task<QuoteWithDeadlineResult> ExecuteQuoteWithDeadlineAsync(
        int callerBudgetMs,
        int gatewaySpentMs,
        int downstreamDelayMs,
        bool enforceDeadline,
        int? headerRemainingMs,
        CancellationToken ct)
    {
        Interlocked.Increment(ref _clientRequests);
        var remainingBudgetMs = headerRemainingMs ?? (callerBudgetMs - gatewaySpentMs);

        if (enforceDeadline && remainingBudgetMs <= 0)
        {
            Interlocked.Increment(ref _deadlineExceededCount);
            logger.LogWarning(
                "resilience_deadline_rejected_early remaining_ms={RemainingMs} caller_budget_ms={CallerBudgetMs} gateway_spent_ms={GatewaySpentMs}",
                remainingBudgetMs, callerBudgetMs, gatewaySpentMs);
            return new QuoteWithDeadlineResult(
                StatusCode: StatusCodes.Status504GatewayTimeout,
                Ok: false,
                Reason: "deadline_exceeded",
                EnforceDeadline: true,
                CallerBudgetMs: callerBudgetMs,
                GatewaySpentMs: gatewaySpentMs,
                RemainingBudgetMs: remainingBudgetMs,
                DownstreamDelayMs: downstreamDelayMs,
                ActualWorkMs: 0,
                WastedWorkMs: 0,
                DeadlineExceeded: true,
                RemainingAfterMs: 0);
        }

        var sw = Stopwatch.StartNew();
        if (!enforceDeadline)
        {
            // Negative control: OrderApi ignores caller budget and performs full downstream work.
            Interlocked.Increment(ref _downstreamAttempts);
            if (downstreamDelayMs > 0)
                await Task.Delay(downstreamDelayMs, ct);
            sw.Stop();

            Interlocked.Increment(ref _downstreamSuccesses);
            var actualMs = sw.ElapsedMilliseconds;
            var allowedMs = Math.Max(0, remainingBudgetMs);
            var exceeded = actualMs > allowedMs;
            var wastedMs = exceeded ? Math.Max(1, actualMs - allowedMs) : 0;
            if (exceeded)
                Interlocked.Increment(ref _deadlineExceededCount);

            logger.LogWarning(
                "resilience_deadline_ignored_work actual_ms={ActualMs} remaining_ms={RemainingMs} wasted_ms={WastedMs}",
                actualMs, remainingBudgetMs, wastedMs);

            return new QuoteWithDeadlineResult(
                StatusCode: StatusCodes.Status200OK,
                Ok: true,
                Reason: exceeded ? "completed_after_caller_deadline" : "ok",
                EnforceDeadline: false,
                CallerBudgetMs: callerBudgetMs,
                GatewaySpentMs: gatewaySpentMs,
                RemainingBudgetMs: remainingBudgetMs,
                DownstreamDelayMs: downstreamDelayMs,
                ActualWorkMs: actualMs,
                WastedWorkMs: wastedMs,
                DeadlineExceeded: exceeded,
                RemainingAfterMs: Math.Max(0, remainingBudgetMs - actualMs));
        }

        using var deadlineCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        deadlineCts.CancelAfter(TimeSpan.FromMilliseconds(remainingBudgetMs));

        Interlocked.Increment(ref _downstreamAttempts);
        try
        {
            if (downstreamDelayMs > 0)
                await Task.Delay(downstreamDelayMs, deadlineCts.Token);
            sw.Stop();

            Interlocked.Increment(ref _downstreamSuccesses);
            var actualMs = sw.ElapsedMilliseconds;
            return new QuoteWithDeadlineResult(
                StatusCode: StatusCodes.Status200OK,
                Ok: true,
                Reason: "ok",
                EnforceDeadline: true,
                CallerBudgetMs: callerBudgetMs,
                GatewaySpentMs: gatewaySpentMs,
                RemainingBudgetMs: remainingBudgetMs,
                DownstreamDelayMs: downstreamDelayMs,
                ActualWorkMs: actualMs,
                WastedWorkMs: 0,
                DeadlineExceeded: false,
                RemainingAfterMs: Math.Max(0, remainingBudgetMs - actualMs));
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            sw.Stop();
            Interlocked.Increment(ref _downstreamTimeouts);
            Interlocked.Increment(ref _deadlineExceededCount);
            logger.LogWarning(
                "resilience_deadline_aborted actual_ms={ActualMs} remaining_ms={RemainingMs}",
                sw.ElapsedMilliseconds, remainingBudgetMs);

            return new QuoteWithDeadlineResult(
                StatusCode: StatusCodes.Status504GatewayTimeout,
                Ok: false,
                Reason: "deadline_exceeded",
                EnforceDeadline: true,
                CallerBudgetMs: callerBudgetMs,
                GatewaySpentMs: gatewaySpentMs,
                RemainingBudgetMs: remainingBudgetMs,
                DownstreamDelayMs: downstreamDelayMs,
                ActualWorkMs: sw.ElapsedMilliseconds,
                WastedWorkMs: 0,
                DeadlineExceeded: true,
                RemainingAfterMs: 0);
        }
    }

    public async Task<DispatchProbeResult> ExecuteDispatchProbeAsync(
        string mode,
        int hops,
        int perHopAttempts,
        int baseBackoffMs,
        int capBackoffMs,
        CancellationToken ct)
    {
        Interlocked.Increment(ref _clientRequests);

        string simMode;
        int simDelayMs;
        int perTryTimeoutMs;
        int overallDeadlineMs;
        int maxRetries;
        int retryBudgetPercent;

        lock (_gate)
        {
            simMode = _mode;
            simDelayMs = _delayMs;
            perTryTimeoutMs = _perTryTimeoutMs;
            overallDeadlineMs = _overallDeadlineMs;
            maxRetries = _maxRetries;
            retryBudgetPercent = _retryBudgetPercent;
        }

        if (string.Equals(mode, "naive-amplified", StringComparison.OrdinalIgnoreCase))
        {
            if (simMode == "healthy" && simDelayMs <= perTryTimeoutMs)
            {
                Interlocked.Increment(ref _downstreamAttempts);
                if (simDelayMs > 0)
                    await Task.Delay(simDelayMs, ct);
                Interlocked.Increment(ref _downstreamSuccesses);

                return new DispatchProbeResult(
                    StatusCodes.Status200OK,
                    true,
                    "naive-amplified",
                    "ok",
                    hops,
                    1,
                    0,
                    0,
                    [],
                    1,
                    GetRetryBudgetTokens());
            }

            // Multi-hop uncoordinated retries multiply attempts as (perHopAttempts ^ hops).
            var totalAttempts = 1;
            for (var h = 0; h < hops; h++)
                totalAttempts *= perHopAttempts;

            var backoffDelays = new List<int>(Math.Max(0, totalAttempts - 1));
            var hitTimeout = false;

            for (var i = 0; i < totalAttempts; i++)
            {
                Interlocked.Increment(ref _downstreamAttempts);
                if (i > 0)
                {
                    Interlocked.Increment(ref _retriesExecuted);
                    backoffDelays.Add(0);
                }

                if (simMode == "slow" || simDelayMs > perTryTimeoutMs)
                {
                    var waitMs = Math.Min(simDelayMs > 0 ? simDelayMs : perTryTimeoutMs + 20, perTryTimeoutMs);
                    await Task.Delay(waitMs, ct);
                    Interlocked.Increment(ref _downstreamTimeouts);
                    hitTimeout = true;
                }
                else
                {
                    if (simDelayMs > 0)
                        await Task.Delay(Math.Min(simDelayMs, 50), ct);
                    Interlocked.Increment(ref _downstreamFailures);
                }
            }

            return new DispatchProbeResult(
                hitTimeout ? StatusCodes.Status504GatewayTimeout : StatusCodes.Status502BadGateway,
                false,
                "naive-amplified",
                hitTimeout ? "downstream_timeout_amplified" : "downstream_failed_amplified",
                hops,
                totalAttempts,
                Math.Max(0, totalAttempts - 1),
                0,
                backoffDelays,
                totalAttempts,
                GetRetryBudgetTokens());
        }

        // Resilient mode: single-hop retry with overall deadline, full-jitter exponential backoff, and retry budget.
        lock (_gate)
        {
            if (retryBudgetPercent > 0)
                _retryBudgetTokens = Math.Min(MaxRetryBudgetTokens, _retryBudgetTokens + retryBudgetPercent / 100.0);
        }

        var overallSw = Stopwatch.StartNew();
        var attempts = 0;
        var retries = 0;
        var blockedRetries = 0;
        var delays = new List<int>();
        var lastTimeout = false;

        for (var attemptIndex = 0; attemptIndex <= maxRetries; attemptIndex++)
        {
            if (attemptIndex > 0)
            {
                var remainingMs = overallDeadlineMs - (int)overallSw.ElapsedMilliseconds;
                if (remainingMs <= 10)
                {
                    Interlocked.Increment(ref _deadlineExceededCount);
                    break;
                }

                bool budgetAllowed;
                lock (_gate)
                {
                    if (_retryBudgetTokens >= 1.0)
                    {
                        _retryBudgetTokens -= 1.0;
                        budgetAllowed = true;
                    }
                    else
                    {
                        budgetAllowed = false;
                    }
                }

                if (!budgetAllowed)
                {
                    Interlocked.Increment(ref _retriesBlockedByBudget);
                    blockedRetries++;
                    continue;
                }

                var ceiling = Math.Min(capBackoffMs, baseBackoffMs * (1 << (attemptIndex - 1)));
                var delay = Random.Shared.Next(0, ceiling + 1);
                var boundedDelay = Math.Min(delay, Math.Max(0, remainingMs - 10));
                delays.Add(boundedDelay);
                if (boundedDelay > 0)
                    await Task.Delay(boundedDelay, ct);

                Interlocked.Increment(ref _retriesExecuted);
                retries++;
            }

            attempts++;
            Interlocked.Increment(ref _downstreamAttempts);

            if (simMode == "healthy" && simDelayMs <= perTryTimeoutMs)
            {
                if (simDelayMs > 0)
                    await Task.Delay(simDelayMs, ct);
                Interlocked.Increment(ref _downstreamSuccesses);

                return new DispatchProbeResult(
                    StatusCodes.Status200OK,
                    true,
                    "resilient",
                    "ok",
                    1,
                    attempts,
                    retries,
                    blockedRetries,
                    delays,
                    attempts,
                    GetRetryBudgetTokens());
            }

            if (simMode == "slow" || simDelayMs > perTryTimeoutMs)
            {
                var waitMs = Math.Min(simDelayMs > 0 ? simDelayMs : perTryTimeoutMs + 20, perTryTimeoutMs);
                await Task.Delay(waitMs, ct);
                Interlocked.Increment(ref _downstreamTimeouts);
                lastTimeout = true;
            }
            else
            {
                if (simDelayMs > 0)
                    await Task.Delay(Math.Min(simDelayMs, 50), ct);
                Interlocked.Increment(ref _downstreamFailures);
                lastTimeout = false;
            }
        }

        return new DispatchProbeResult(
            lastTimeout ? StatusCodes.Status504GatewayTimeout : StatusCodes.Status503ServiceUnavailable,
            false,
            "resilient",
            blockedRetries > 0 ? "retry_budget_exhausted" : (lastTimeout ? "downstream_timeout" : "downstream_failed"),
            1,
            attempts,
            retries,
            blockedRetries,
            delays,
            attempts,
            GetRetryBudgetTokens());
    }

    public JitterSampleResponse SampleFullJitter(int baseMs, int capMs, int attempt, int samples)
    {
        var ceiling = Math.Min(capMs, (int)Math.Min(capMs, baseMs * Math.Pow(2, attempt)));
        var fixedBackoffs = Enumerable.Repeat(ceiling, samples).ToArray();
        var jitteredBackoffs = Enumerable.Range(0, samples)
            .Select(_ => Random.Shared.Next(0, ceiling + 1))
            .ToArray();

        return new JitterSampleResponse(
            BaseMs: baseMs,
            CapMs: capMs,
            Attempt: attempt,
            CeilingMs: ceiling,
            Samples: samples,
            FixedBackoffsMs: fixedBackoffs,
            FixedSpreadMs: 0,
            DistinctFixedValues: 1,
            FullJitterBackoffsMs: jitteredBackoffs,
            FullJitterSpreadMs: jitteredBackoffs.Max() - jitteredBackoffs.Min(),
            DistinctFullJitterValues: jitteredBackoffs.Distinct().Count());
    }

    public async Task<ProtectedCallResult> ExecuteProtectedCallAsync(CancellationToken ct)
    {
        Interlocked.Increment(ref _clientRequests);
        var sw = Stopwatch.StartNew();

        string previousState;
        string simMode;
        int simDelayMs;
        int perTryTimeoutMs;
        var isHalfOpenProbe = false;

        lock (_gate)
        {
            previousState = _cbState;
            simMode = _mode;
            simDelayMs = _delayMs;
            perTryTimeoutMs = _perTryTimeoutMs;

            if (_cbState == "open")
            {
                var elapsedOpen = DateTimeOffset.UtcNow - _cbOpenedAtUtc;
                if (elapsedOpen >= TimeSpan.FromMilliseconds(_cbOpenMs) && !_halfOpenProbeInFlight)
                {
                    _cbState = "half-open";
                    previousState = "half-open";
                    _halfOpenProbeInFlight = true;
                    Interlocked.Increment(ref _circuitTransitions);
                    isHalfOpenProbe = true;
                }
                else
                {
                    Interlocked.Increment(ref _circuitFastFails);
                    sw.Stop();
                    return new ProtectedCallResult(
                        StatusCodes.Status503ServiceUnavailable,
                        false,
                        previousState,
                        _cbState,
                        FastFailed: true,
                        DownstreamCalled: false,
                        _consecutiveFailures,
                        sw.ElapsedMilliseconds,
                        "circuit_open_fast_fail");
                }
            }
            else if (_cbState == "half-open")
            {
                if (_halfOpenProbeInFlight)
                {
                    Interlocked.Increment(ref _circuitFastFails);
                    sw.Stop();
                    return new ProtectedCallResult(
                        StatusCodes.Status503ServiceUnavailable,
                        false,
                        previousState,
                        _cbState,
                        FastFailed: true,
                        DownstreamCalled: false,
                        _consecutiveFailures,
                        sw.ElapsedMilliseconds,
                        "circuit_half_open_probe_busy");
                }

                _halfOpenProbeInFlight = true;
                isHalfOpenProbe = true;
            }
        }

        Interlocked.Increment(ref _downstreamAttempts);
        var succeeded = false;
        var timedOut = false;

        try
        {
            if (simMode == "healthy" && simDelayMs <= perTryTimeoutMs)
            {
                if (simDelayMs > 0)
                    await Task.Delay(simDelayMs, ct);
                Interlocked.Increment(ref _downstreamSuccesses);
                succeeded = true;
            }
            else if (simMode == "slow" || simDelayMs > perTryTimeoutMs)
            {
                var waitMs = Math.Min(simDelayMs > 0 ? simDelayMs : perTryTimeoutMs + 20, perTryTimeoutMs);
                await Task.Delay(waitMs, ct);
                Interlocked.Increment(ref _downstreamTimeouts);
                timedOut = true;
            }
            else
            {
                if (simDelayMs > 0)
                    await Task.Delay(Math.Min(simDelayMs, 50), ct);
                Interlocked.Increment(ref _downstreamFailures);
            }
        }
        catch
        {
            if (isHalfOpenProbe)
            {
                lock (_gate)
                    _halfOpenProbeInFlight = false;
            }
            throw;
        }

        sw.Stop();

        lock (_gate)
        {
            if (succeeded)
            {
                if (_cbState == "half-open")
                {
                    _cbState = "closed";
                    _halfOpenProbeInFlight = false;
                    Interlocked.Increment(ref _circuitTransitions);
                }
                _consecutiveFailures = 0;

                return new ProtectedCallResult(
                    StatusCodes.Status200OK,
                    true,
                    previousState,
                    _cbState,
                    FastFailed: false,
                    DownstreamCalled: true,
                    _consecutiveFailures,
                    sw.ElapsedMilliseconds,
                    isHalfOpenProbe ? "half_open_probe_succeeded" : "ok");
            }

            if (_cbState == "half-open")
            {
                _cbState = "open";
                _cbOpenedAtUtc = DateTimeOffset.UtcNow;
                _halfOpenProbeInFlight = false;
                Interlocked.Increment(ref _circuitTransitions);
                logger.LogWarning("resilience_circuit_reopened_from_half_open");
            }
            else
            {
                _consecutiveFailures++;
                if (_consecutiveFailures >= _cbFailureThreshold)
                {
                    _cbState = "open";
                    _cbOpenedAtUtc = DateTimeOffset.UtcNow;
                    Interlocked.Increment(ref _circuitTransitions);
                    logger.LogWarning("resilience_circuit_opened consecutive_failures={Failures}", _consecutiveFailures);
                }
            }

            return new ProtectedCallResult(
                timedOut ? StatusCodes.Status504GatewayTimeout : StatusCodes.Status503ServiceUnavailable,
                false,
                previousState,
                _cbState,
                FastFailed: false,
                DownstreamCalled: true,
                _consecutiveFailures,
                sw.ElapsedMilliseconds,
                timedOut ? "downstream_timeout" : "downstream_failed");
        }
    }

    private double GetRetryBudgetTokens()
    {
        lock (_gate) return Math.Round(_retryBudgetTokens, 2);
    }

    private ResilienceStatusSnapshot GetSnapshotUnsafe() => new(
        Mode: _mode,
        DelayMs: _delayMs,
        PerTryTimeoutMs: _perTryTimeoutMs,
        OverallDeadlineMs: _overallDeadlineMs,
        MaxRetries: _maxRetries,
        RetryBudgetPercent: _retryBudgetPercent,
        CircuitBreakerFailureThreshold: _cbFailureThreshold,
        CircuitBreakerOpenMs: _cbOpenMs,
        CircuitBreakerState: _cbState,
        ConsecutiveFailures: _consecutiveFailures,
        RetryBudgetTokens: Math.Round(_retryBudgetTokens, 2),
        ClientRequests: Interlocked.Read(ref _clientRequests),
        DownstreamAttempts: Interlocked.Read(ref _downstreamAttempts),
        DownstreamSuccesses: Interlocked.Read(ref _downstreamSuccesses),
        DownstreamTimeouts: Interlocked.Read(ref _downstreamTimeouts),
        DownstreamFailures: Interlocked.Read(ref _downstreamFailures),
        RetriesExecuted: Interlocked.Read(ref _retriesExecuted),
        RetriesBlockedByBudget: Interlocked.Read(ref _retriesBlockedByBudget),
        DeadlineExceededCount: Interlocked.Read(ref _deadlineExceededCount),
        CircuitFastFails: Interlocked.Read(ref _circuitFastFails),
        CircuitTransitions: Interlocked.Read(ref _circuitTransitions));
}
