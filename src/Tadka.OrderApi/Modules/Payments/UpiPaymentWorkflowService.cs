using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;

namespace Tadka.OrderApi.Modules.Payments;

public sealed record CreateUpiIntentRequest(
    long OrderId,
    long CustomerId,
    long AmountPaise,
    string PayerVpa);

public sealed record UpiIntentResponse(
    string IntentRef,
    long OrderId,
    long CustomerId,
    long AmountPaise,
    string Currency,
    string PayerVpa,
    string Status,
    string IdempotencyStatus,
    string? PspRrn,
    int WebhookDeliveries);

public sealed record SignWebhookRequest(
    string? RawBody,
    string? WebhookId,
    string? IntentRef,
    string? Status,
    string? PspRrn,
    long? AmountPaise);

public sealed record SignWebhookResponse(
    string Signature,
    string RawBody);

public sealed record UpiWebhookPayload(
    string WebhookId,
    string IntentRef,
    string Status,
    string? PspRrn,
    long AmountPaise);

public sealed record UpiWebhookResult(
    string IntentRef,
    long OrderId,
    string Mode,
    string Status,
    string TransitionOutcome,
    int WebhookDeliveries,
    long LedgerEntryCount,
    long TotalDebitPaise,
    long TotalCreditPaise,
    long NetSignedSum);

public sealed record ReconciledIntentSummary(
    string IntentRef,
    long OrderId,
    string NewStatus,
    string PspRrn,
    long LedgerEntryCount,
    long NetSignedSum);

public sealed record UpiReconcileResponse(
    int ScannedPendingCount,
    int ReconciledCount,
    IReadOnlyList<ReconciledIntentSummary> ReconciledIntents);

public sealed record LedgerEntryDto(
    long Id,
    string TransactionRef,
    string AccountCode,
    string Direction,
    long AmountPaise,
    long SignedAmount);

public sealed record WebhookAuditDto(
    string WebhookId,
    string ReportedStatus,
    string ProcessingMode,
    string Outcome);

public sealed record OutboxAuditDto(
    string EventId,
    string EventType,
    bool Published);

public sealed record UpiAuditResponse(
    UpiIntentResponse Intent,
    string OrderStatus,
    long TotalDebitPaise,
    long TotalCreditPaise,
    long NetSignedSum,
    IReadOnlyList<LedgerEntryDto> LedgerEntries,
    IReadOnlyList<OutboxAuditDto> OutboxEvents,
    IReadOnlyList<WebhookAuditDto> WebhookEvents);

public sealed class UpiPaymentWorkflowService(
    IConfiguration configuration,
    NpgsqlDataSource dataSource,
    ILogger<UpiPaymentWorkflowService> logger)
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private string WebhookSecret => configuration["Payments:WebhookSecret"] ?? "tadka-upi-webhook-secret-2026";

    public async Task<object> ResetAsync(CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            """
            TRUNCATE TABLE payment_webhook_events, ledger_entries, payment_intents RESTART IDENTITY CASCADE;
            DELETE FROM outbox_events WHERE event_id LIKE 'evt-upi-%';
            DELETE FROM orders WHERE id >= 95000 OR idempotency_key LIKE 'upi-lab-%';
            """,
            conn);
        await cmd.ExecuteNonQueryAsync(ct);

        logger.LogInformation("upi_workflow_reset complete=true");
        return new
        {
            status = "RESET",
            paymentIntentRows = 0L,
            ledgerRows = 0L,
            webhookEventRows = 0L
        };
    }

    public async Task<(int StatusCode, UpiIntentResponse? Body, string? ErrorCode, string? ErrorMessage)> CreateOrReplayIntentAsync(
        string? idempotencyKey,
        CreateUpiIntentRequest req,
        CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(idempotencyKey))
            return (400, null, "MISSING_IDEMPOTENCY_KEY", "Idempotency-Key header is required.");
        if (req.OrderId <= 0 || req.CustomerId <= 0 || req.AmountPaise <= 0 || string.IsNullOrWhiteSpace(req.PayerVpa))
            return (400, null, "INVALID_INTENT_REQUEST", "orderId, customerId, amountPaise (> 0), and payerVpa are required.");

        var normalizedKey = idempotencyKey.Trim();
        var normalizedVpa = req.PayerVpa.Trim().ToLowerInvariant();
        var payloadHash = ComputeSha256Hex($"{req.OrderId}|{req.CustomerId}|{req.AmountPaise}|{normalizedVpa}");
        var intentRef = $"upi_intent_{req.OrderId}";
        var amountRupees = req.AmountPaise / 100.00m;

        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        // Ensure demo order exists when caller supplies an explicit demo orderId >= 95000.
        await using (var ensureOrderCmd = new NpgsqlCommand(
            """
            INSERT INTO orders (id, customer_id, restaurant_id, total_amount, idempotency_key, status)
            VALUES (@orderId, @customerId, 7, @totalAmount, @orderIdem, 'payment_pending')
            ON CONFLICT (id) DO NOTHING;
            """,
            conn,
            tx))
        {
            ensureOrderCmd.Parameters.AddWithValue("orderId", req.OrderId);
            ensureOrderCmd.Parameters.AddWithValue("customerId", req.CustomerId);
            ensureOrderCmd.Parameters.AddWithValue("totalAmount", amountRupees);
            ensureOrderCmd.Parameters.AddWithValue("orderIdem", $"upi-lab-order-{req.OrderId}");
            await ensureOrderCmd.ExecuteNonQueryAsync(ct);
        }

        // Probe idempotency record under lock or insert atomically.
        await using (var insertCmd = new NpgsqlCommand(
            """
            INSERT INTO payment_intents
                (intent_ref, order_id, customer_id, amount_paise, currency, payer_vpa, idempotency_key, payload_hash, status)
            VALUES
                (@intentRef, @orderId, @customerId, @amountPaise, 'INR', @payerVpa, @idempotencyKey, @payloadHash, 'PENDING')
            ON CONFLICT (customer_id, idempotency_key) DO NOTHING
            RETURNING intent_ref;
            """,
            conn,
            tx))
        {
            insertCmd.Parameters.AddWithValue("intentRef", intentRef);
            insertCmd.Parameters.AddWithValue("orderId", req.OrderId);
            insertCmd.Parameters.AddWithValue("customerId", req.CustomerId);
            insertCmd.Parameters.AddWithValue("amountPaise", req.AmountPaise);
            insertCmd.Parameters.AddWithValue("payerVpa", normalizedVpa);
            insertCmd.Parameters.AddWithValue("idempotencyKey", normalizedKey);
            insertCmd.Parameters.AddWithValue("payloadHash", payloadHash);

            var insertedRef = await insertCmd.ExecuteScalarAsync(ct) as string;
            if (!string.IsNullOrEmpty(insertedRef))
            {
                await tx.CommitAsync(ct);
                logger.LogInformation(
                    "upi_intent_created intent_ref={IntentRef} order_id={OrderId} amount_paise={AmountPaise}",
                    insertedRef, req.OrderId, req.AmountPaise);

                return (201, new UpiIntentResponse(
                    IntentRef: insertedRef,
                    OrderId: req.OrderId,
                    CustomerId: req.CustomerId,
                    AmountPaise: req.AmountPaise,
                    Currency: "INR",
                    PayerVpa: normalizedVpa,
                    Status: "PENDING",
                    IdempotencyStatus: "CREATED",
                    PspRrn: null,
                    WebhookDeliveries: 0), null, null);
            }
        }

        // Row already exists for (customer_id, idempotency_key): compare payload hash.
        await using var existingCmd = new NpgsqlCommand(
            """
            SELECT intent_ref, order_id, customer_id, amount_paise, currency, payer_vpa, payload_hash, status, psp_rrn, webhook_deliveries
              FROM payment_intents
             WHERE customer_id = @customerId AND idempotency_key = @idempotencyKey;
            """,
            conn,
            tx);
        existingCmd.Parameters.AddWithValue("customerId", req.CustomerId);
        existingCmd.Parameters.AddWithValue("idempotencyKey", normalizedKey);

        await using var reader = await existingCmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            await tx.RollbackAsync(ct);
            return (500, null, "UNEXPECTED_STATE", "Idempotency row missing after conflict.");
        }

        var existingHash = reader.GetString(6);
        if (!string.Equals(existingHash, payloadHash, StringComparison.Ordinal))
        {
            await reader.CloseAsync();
            await tx.RollbackAsync(ct);
            logger.LogWarning(
                "upi_intent_idempotency_conflict customer_id={CustomerId} idempotency_key={Key}",
                req.CustomerId, normalizedKey);
            return (409, null, "IDEMPOTENCY_PAYLOAD_MISMATCH",
                "Idempotency-Key was already used with a different payment payload or amount.");
        }

        var existingResponse = new UpiIntentResponse(
            IntentRef: reader.GetString(0),
            OrderId: reader.GetInt64(1),
            CustomerId: reader.GetInt64(2),
            AmountPaise: reader.GetInt64(3),
            Currency: reader.GetString(4),
            PayerVpa: reader.GetString(5),
            Status: reader.GetString(7),
            IdempotencyStatus: "REPLAYED",
            PspRrn: await reader.IsDBNullAsync(8, ct) ? null : reader.GetString(8),
            WebhookDeliveries: reader.GetInt32(9));

        await reader.CloseAsync();
        await tx.CommitAsync(ct);

        logger.LogInformation("upi_intent_replayed intent_ref={IntentRef} status={Status}", existingResponse.IntentRef, existingResponse.Status);
        return (200, existingResponse, null, null);
    }

    public SignWebhookResponse SignWebhook(SignWebhookRequest req)
    {
        var rawBody = !string.IsNullOrEmpty(req.RawBody)
            ? req.RawBody
            : JsonSerializer.Serialize(new UpiWebhookPayload(
                WebhookId: req.WebhookId ?? $"wh-{Guid.NewGuid():N}"[..16],
                IntentRef: req.IntentRef ?? throw new ArgumentException("intentRef or rawBody is required."),
                Status: (req.Status ?? "CAPTURED").Trim().ToUpperInvariant(),
                PspRrn: req.PspRrn ?? "RRN431298765432",
                AmountPaise: req.AmountPaise ?? 45000), JsonOptions);

        return new SignWebhookResponse(
            Signature: ComputeHmacHex(rawBody),
            RawBody: rawBody);
    }

    public bool VerifySignature(string rawBody, string? providedSignature)
    {
        if (string.IsNullOrWhiteSpace(providedSignature))
            return false;

        var expectedHex = ComputeHmacHex(rawBody);
        var actualHex = providedSignature.Trim().ToLowerInvariant();
        var expectedBytes = Encoding.UTF8.GetBytes(expectedHex);
        var actualBytes = Encoding.UTF8.GetBytes(actualHex);

        return expectedBytes.Length == actualBytes.Length &&
               CryptographicOperations.FixedTimeEquals(expectedBytes, actualBytes);
    }

    public async Task<(int StatusCode, UpiWebhookResult? Body, string? ErrorCode, string? ErrorMessage)> ProcessWebhookAsync(
        string rawBody,
        string? signatureHeader,
        string mode,
        CancellationToken ct)
    {
        if (!VerifySignature(rawBody, signatureHeader))
        {
            logger.LogWarning("upi_webhook_rejected_invalid_signature");
            return (401, null, "INVALID_WEBHOOK_SIGNATURE", "HMAC-SHA256 webhook signature is missing or invalid.");
        }

        var normalizedMode = string.IsNullOrWhiteSpace(mode) ? "safe" : mode.Trim().ToLowerInvariant();
        if (normalizedMode is not ("safe" or "naive"))
            return (400, null, "INVALID_MODE", "mode must be 'safe' or 'naive'.");

        UpiWebhookPayload? payload;
        try
        {
            payload = JsonSerializer.Deserialize<UpiWebhookPayload>(rawBody, JsonOptions);
        }
        catch (JsonException)
        {
            return (400, null, "INVALID_JSON", "Webhook body is not valid JSON.");
        }

        if (payload is null || string.IsNullOrWhiteSpace(payload.IntentRef) || string.IsNullOrWhiteSpace(payload.Status))
            return (400, null, "INVALID_PAYLOAD", "webhookId, intentRef, and status are required.");

        var reportedStatus = payload.Status.Trim().ToUpperInvariant();
        if (reportedStatus is not ("CAPTURED" or "FAILED" or "EXPIRED"))
            return (400, null, "INVALID_STATUS", "reported status must be CAPTURED, FAILED, or EXPIRED.");

        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        string currentStatus;
        long orderId;
        long amountPaise;
        int deliveries;

        await using (var lockCmd = new NpgsqlCommand(
            """
            SELECT status, order_id, amount_paise, webhook_deliveries
              FROM payment_intents
             WHERE intent_ref = @intentRef
               FOR UPDATE;
            """,
            conn,
            tx))
        {
            lockCmd.Parameters.AddWithValue("intentRef", payload.IntentRef);
            await using var reader = await lockCmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
            {
                await tx.RollbackAsync(ct);
                return (404, null, "INTENT_NOT_FOUND", $"No payment intent found for '{payload.IntentRef}'.");
            }

            currentStatus = reader.GetString(0);
            orderId = reader.GetInt64(1);
            amountPaise = reader.GetInt64(2);
            deliveries = reader.GetInt32(3);
        }

        if (normalizedMode == "naive")
        {
            deliveries++;
            await using (var updateCmd = new NpgsqlCommand(
                """
                UPDATE payment_intents
                   SET status = @status,
                       psp_rrn = COALESCE(@rrn, psp_rrn),
                       webhook_deliveries = webhook_deliveries + 1,
                       updated_at = NOW()
                 WHERE intent_ref = @intentRef;
                """,
                conn,
                tx))
            {
                updateCmd.Parameters.AddWithValue("status", reportedStatus);
                updateCmd.Parameters.AddWithValue("rrn", (object?)payload.PspRrn ?? DBNull.Value);
                updateCmd.Parameters.AddWithValue("intentRef", payload.IntentRef);
                await updateCmd.ExecuteNonQueryAsync(ct);
            }

            if (reportedStatus == "CAPTURED")
            {
                await InsertDoubleEntryCaptureAsync(conn, tx, payload.IntentRef, orderId, amountPaise, $"tx-naive-{Guid.NewGuid():N}"[..18], ct);
                await UpdateOrderPaidAsync(conn, tx, orderId, ct);
            }

            await RecordWebhookAuditAsync(conn, tx, payload.WebhookId, payload.IntentRef, reportedStatus, payload.PspRrn, "naive", "APPLIED_NAIVE", ct);
            await tx.CommitAsync(ct);

            var naiveResult = await BuildWebhookResultAsync(conn, payload.IntentRef, orderId, "naive", reportedStatus, "APPLIED_NAIVE", deliveries, ct);
            return (200, naiveResult, null, null);
        }

        // Safe monotonic state machine under row lock (PENDING -> CAPTURED | FAILED | EXPIRED; terminal states immutable).
        string finalStatus = currentStatus;
        string transitionOutcome;

        if (currentStatus != "PENDING")
        {
            deliveries++;
            transitionOutcome = string.Equals(currentStatus, reportedStatus, StringComparison.Ordinal)
                ? "DUPLICATE_IGNORED"
                : "TERMINAL_STATE_GUARDED";

            await using (var bumpCmd = new NpgsqlCommand(
                "UPDATE payment_intents SET webhook_deliveries = webhook_deliveries + 1, updated_at = NOW() WHERE intent_ref = @intentRef;",
                conn,
                tx))
            {
                bumpCmd.Parameters.AddWithValue("intentRef", payload.IntentRef);
                await bumpCmd.ExecuteNonQueryAsync(ct);
            }

            await RecordWebhookAuditAsync(conn, tx, payload.WebhookId, payload.IntentRef, reportedStatus, payload.PspRrn, "safe", transitionOutcome, ct);
            await tx.CommitAsync(ct);

            logger.LogInformation(
                "upi_webhook_guarded intent_ref={IntentRef} current={Current} incoming={Incoming} outcome={Outcome}",
                payload.IntentRef, currentStatus, reportedStatus, transitionOutcome);
        }
        else
        {
            finalStatus = reportedStatus;
            transitionOutcome = "TRANSITION_APPLIED";
            deliveries++;

            await ApplyTransitionFromPendingAsync(
                conn,
                tx,
                payload.IntentRef,
                orderId,
                amountPaise,
                reportedStatus,
                payload.PspRrn,
                payload.WebhookId,
                "safe",
                incrementWebhookCounter: true,
                ct);
            await tx.CommitAsync(ct);

            logger.LogInformation(
                "upi_webhook_applied intent_ref={IntentRef} new_status={Status} rrn={Rrn}",
                payload.IntentRef, reportedStatus, payload.PspRrn);
        }

        var safeResult = await BuildWebhookResultAsync(conn, payload.IntentRef, orderId, "safe", finalStatus, transitionOutcome, deliveries, ct);
        return (200, safeResult, null, null);
    }

    public async Task<UpiReconcileResponse> ReconcilePendingAsync(string pspOutcome, CancellationToken ct)
    {
        var normalizedOutcome = string.IsNullOrWhiteSpace(pspOutcome) ? "CAPTURED" : pspOutcome.Trim().ToUpperInvariant();
        if (normalizedOutcome is not ("CAPTURED" or "FAILED"))
            throw new ArgumentException("pspOutcome must be 'CAPTURED' or 'FAILED'.");

        var pendingIntents = new List<(string IntentRef, long OrderId, long AmountPaise)>();

        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using (var selectCmd = new NpgsqlCommand(
            """
            SELECT intent_ref, order_id, amount_paise
              FROM payment_intents
             WHERE status = 'PENDING'
             ORDER BY id ASC;
            """,
            conn))
        await using (var reader = await selectCmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                pendingIntents.Add((reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2)));
            }
        }

        var reconciled = new List<ReconciledIntentSummary>();
        foreach (var item in pendingIntents)
        {
            var rrn = $"RRN-RECON-{item.OrderId}";
            await using var tx = await conn.BeginTransactionAsync(ct);

            await using var lockCmd = new NpgsqlCommand(
                "SELECT status FROM payment_intents WHERE intent_ref = @intentRef FOR UPDATE;",
                conn,
                tx);
            lockCmd.Parameters.AddWithValue("intentRef", item.IntentRef);
            var status = await lockCmd.ExecuteScalarAsync(ct) as string;
            if (!string.Equals(status, "PENDING", StringComparison.Ordinal))
            {
                await tx.RollbackAsync(ct);
                continue;
            }

            await ApplyTransitionFromPendingAsync(
                conn,
                tx,
                item.IntentRef,
                item.OrderId,
                item.AmountPaise,
                normalizedOutcome,
                rrn,
                $"reconcile-{item.IntentRef}",
                "reconcile",
                incrementWebhookCounter: false,
                ct);
            await tx.CommitAsync(ct);

            var (_, _, _, entryCount, netSum) = await ReadLedgerTotalsAsync(conn, item.IntentRef, ct);
            reconciled.Add(new ReconciledIntentSummary(
                IntentRef: item.IntentRef,
                OrderId: item.OrderId,
                NewStatus: normalizedOutcome,
                PspRrn: rrn,
                LedgerEntryCount: entryCount,
                NetSignedSum: netSum));
        }

        logger.LogInformation("upi_reconciliation_complete scanned={Scanned} reconciled={Reconciled}", pendingIntents.Count, reconciled.Count);
        return new UpiReconcileResponse(pendingIntents.Count, reconciled.Count, reconciled);
    }

    public async Task<UpiAuditResponse?> GetAuditAsync(string intentRef, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);

        UpiIntentResponse? intent = null;
        string orderStatus = "unknown";
        await using (var cmd = new NpgsqlCommand(
            """
            SELECT p.intent_ref, p.order_id, p.customer_id, p.amount_paise, p.currency,
                   p.payer_vpa, p.status, p.psp_rrn, p.webhook_deliveries, o.status
              FROM payment_intents p
              JOIN orders o ON o.id = p.order_id
             WHERE p.intent_ref = @intentRef;
            """,
            conn))
        {
            cmd.Parameters.AddWithValue("intentRef", intentRef);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
                return null;

            intent = new UpiIntentResponse(
                IntentRef: reader.GetString(0),
                OrderId: reader.GetInt64(1),
                CustomerId: reader.GetInt64(2),
                AmountPaise: reader.GetInt64(3),
                Currency: reader.GetString(4),
                PayerVpa: reader.GetString(5),
                Status: reader.GetString(6),
                IdempotencyStatus: "AUDIT",
                PspRrn: await reader.IsDBNullAsync(7, ct) ? null : reader.GetString(7),
                WebhookDeliveries: reader.GetInt32(8));
            orderStatus = reader.GetString(9);
        }

        var entries = new List<LedgerEntryDto>();
        await using (var ledgerCmd = new NpgsqlCommand(
            """
            SELECT id, transaction_ref, account_code, direction, amount_paise, signed_amount
              FROM ledger_entries
             WHERE intent_ref = @intentRef
             ORDER BY id ASC;
            """,
            conn))
        {
            ledgerCmd.Parameters.AddWithValue("intentRef", intentRef);
            await using var reader = await ledgerCmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                entries.Add(new LedgerEntryDto(
                    Id: reader.GetInt64(0),
                    TransactionRef: reader.GetString(1),
                    AccountCode: reader.GetString(2),
                    Direction: reader.GetString(3),
                    AmountPaise: reader.GetInt64(4),
                    SignedAmount: reader.GetInt64(5)));
            }
        }

        var outboxEvents = new List<OutboxAuditDto>();
        await using (var outboxCmd = new NpgsqlCommand(
            """
            SELECT event_id, event_type, published_at IS NOT NULL
              FROM outbox_events
             WHERE aggregate_id = @orderId AND event_id LIKE 'evt-upi-%'
             ORDER BY id ASC;
            """,
            conn))
        {
            outboxCmd.Parameters.AddWithValue("orderId", intent.OrderId);
            await using var reader = await outboxCmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                outboxEvents.Add(new OutboxAuditDto(reader.GetString(0), reader.GetString(1), reader.GetBoolean(2)));
            }
        }

        var webhookEvents = new List<WebhookAuditDto>();
        await using (var whCmd = new NpgsqlCommand(
            """
            SELECT webhook_id, reported_status, processing_mode, outcome
              FROM payment_webhook_events
             WHERE intent_ref = @intentRef
             ORDER BY id ASC;
            """,
            conn))
        {
            whCmd.Parameters.AddWithValue("intentRef", intentRef);
            await using var reader = await whCmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                webhookEvents.Add(new WebhookAuditDto(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3)));
            }
        }

        var totalDebit = entries.Where(e => e.Direction == "DEBIT").Sum(e => e.AmountPaise);
        var totalCredit = entries.Where(e => e.Direction == "CREDIT").Sum(e => e.AmountPaise);
        var netSigned = entries.Sum(e => e.SignedAmount);

        return new UpiAuditResponse(
            Intent: intent,
            OrderStatus: orderStatus,
            TotalDebitPaise: totalDebit,
            TotalCreditPaise: totalCredit,
            NetSignedSum: netSigned,
            LedgerEntries: entries,
            OutboxEvents: outboxEvents,
            WebhookEvents: webhookEvents);
    }

    private async Task ApplyTransitionFromPendingAsync(
        NpgsqlConnection conn,
        NpgsqlTransaction tx,
        string intentRef,
        long orderId,
        long amountPaise,
        string newStatus,
        string? pspRrn,
        string webhookId,
        string processingMode,
        bool incrementWebhookCounter,
        CancellationToken ct)
    {
        var bumpExpr = incrementWebhookCounter ? "webhook_deliveries + 1" : "webhook_deliveries";
        await using (var casCmd = new NpgsqlCommand(
            $"""
            UPDATE payment_intents
               SET status = @newStatus,
                   psp_rrn = @rrn,
                   webhook_deliveries = {bumpExpr},
                   updated_at = NOW()
             WHERE intent_ref = @intentRef AND status = 'PENDING';
            """,
            conn,
            tx))
        {
            casCmd.Parameters.AddWithValue("newStatus", newStatus);
            casCmd.Parameters.AddWithValue("rrn", (object?)pspRrn ?? DBNull.Value);
            casCmd.Parameters.AddWithValue("intentRef", intentRef);
            var rows = await casCmd.ExecuteNonQueryAsync(ct);
            if (rows != 1)
                throw new InvalidOperationException($"Expected single PENDING transition for {intentRef}, updated {rows} rows.");
        }

        if (newStatus == "CAPTURED")
        {
            var txRef = $"tx-{intentRef}";
            await InsertDoubleEntryCaptureAsync(conn, tx, intentRef, orderId, amountPaise, txRef, ct);
            await UpdateOrderPaidAsync(conn, tx, orderId, ct);

            var outboxEventId = $"evt-upi-captured-{intentRef}";
            var outboxPayload = JsonSerializer.Serialize(new
            {
                eventId = outboxEventId,
                intentRef,
                orderId,
                amountPaise,
                pspRrn,
                status = "CAPTURED"
            }, JsonOptions);

            await using var outboxCmd = new NpgsqlCommand(
                """
                INSERT INTO outbox_events (event_id, aggregate_type, aggregate_id, event_type, payload, published_at, publish_attempts)
                VALUES (@eventId, 'PaymentIntent', @orderId, 'PaymentCaptured', @payload::jsonb, NULL, 0)
                ON CONFLICT (event_id) DO NOTHING;
                """,
                conn,
                tx);
            outboxCmd.Parameters.AddWithValue("eventId", outboxEventId);
            outboxCmd.Parameters.AddWithValue("orderId", orderId);
            outboxCmd.Parameters.AddWithValue("payload", outboxPayload);
            await outboxCmd.ExecuteNonQueryAsync(ct);
        }
        else if (newStatus == "FAILED")
        {
            await using var failOrderCmd = new NpgsqlCommand(
                "UPDATE orders SET status = 'payment_failed', status_version = status_version + 1 WHERE id = @orderId;",
                conn,
                tx);
            failOrderCmd.Parameters.AddWithValue("orderId", orderId);
            await failOrderCmd.ExecuteNonQueryAsync(ct);
        }

        await RecordWebhookAuditAsync(conn, tx, webhookId, intentRef, newStatus, pspRrn, processingMode, "TRANSITION_APPLIED", ct);
    }

    private static async Task InsertDoubleEntryCaptureAsync(
        NpgsqlConnection conn,
        NpgsqlTransaction tx,
        string intentRef,
        long orderId,
        long amountPaise,
        string txRef,
        CancellationToken ct)
    {
        // Integer paise arithmetic matching gateway ledger rule: Gross = MerchantPayable + Fee (2%) + GST (18% of fee)
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

    private static async Task UpdateOrderPaidAsync(NpgsqlConnection conn, NpgsqlTransaction tx, long orderId, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "UPDATE orders SET status = 'paid', status_version = status_version + 1 WHERE id = @orderId;",
            conn,
            tx);
        cmd.Parameters.AddWithValue("orderId", orderId);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task RecordWebhookAuditAsync(
        NpgsqlConnection conn,
        NpgsqlTransaction tx,
        string webhookId,
        string intentRef,
        string reportedStatus,
        string? pspRrn,
        string processingMode,
        string outcome,
        CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            """
            INSERT INTO payment_webhook_events (webhook_id, intent_ref, reported_status, psp_rrn, processing_mode, outcome)
            VALUES (@webhookId, @intentRef, @status, @rrn, @mode, @outcome);
            """,
            conn,
            tx);
        cmd.Parameters.AddWithValue("webhookId", webhookId);
        cmd.Parameters.AddWithValue("intentRef", intentRef);
        cmd.Parameters.AddWithValue("status", reportedStatus);
        cmd.Parameters.AddWithValue("rrn", (object?)pspRrn ?? DBNull.Value);
        cmd.Parameters.AddWithValue("mode", processingMode);
        cmd.Parameters.AddWithValue("outcome", outcome);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task<UpiWebhookResult> BuildWebhookResultAsync(
        NpgsqlConnection conn,
        string intentRef,
        long orderId,
        string mode,
        string finalStatus,
        string transitionOutcome,
        int deliveries,
        CancellationToken ct)
    {
        var (totalDebit, totalCredit, _, count, netSigned) = await ReadLedgerTotalsAsync(conn, intentRef, ct);
        return new UpiWebhookResult(
            IntentRef: intentRef,
            OrderId: orderId,
            Mode: mode,
            Status: finalStatus,
            TransitionOutcome: transitionOutcome,
            WebhookDeliveries: deliveries,
            LedgerEntryCount: count,
            TotalDebitPaise: totalDebit,
            TotalCreditPaise: totalCredit,
            NetSignedSum: netSigned);
    }

    private static async Task<(long TotalDebit, long TotalCredit, long TxCount, long EntryCount, long NetSignedSum)> ReadLedgerTotalsAsync(
        NpgsqlConnection conn,
        string intentRef,
        CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            """
            SELECT
                COALESCE(SUM(CASE WHEN direction = 'DEBIT' THEN amount_paise ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount_paise ELSE 0 END), 0),
                COUNT(DISTINCT transaction_ref),
                COUNT(*),
                COALESCE(SUM(signed_amount), 0)
              FROM ledger_entries
             WHERE intent_ref = @intentRef;
            """,
            conn);
        cmd.Parameters.AddWithValue("intentRef", intentRef);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);

        return (
            Convert.ToInt64(reader.GetValue(0)),
            Convert.ToInt64(reader.GetValue(1)),
            reader.GetInt64(2),
            reader.GetInt64(3),
            Convert.ToInt64(reader.GetValue(4)));
    }

    private string ComputeHmacHex(string data)
    {
        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(WebhookSecret));
        var hash = hmac.ComputeHash(Encoding.UTF8.GetBytes(data));
        return Convert.ToHexStringLower(hash);
    }

    private static string ComputeSha256Hex(string input)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(input));
        return Convert.ToHexStringLower(hash);
    }
}
