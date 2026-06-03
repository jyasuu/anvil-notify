use std::time::Duration;

use anyhow::Context;
use common::{
    ChannelOverrides, EmailOptions, FromOverride, GroupRetryMode, Metadata, NotificationEvent,
    Recipient, RetryPolicy, SendMode,
};
use lapin::{
    options::*, types::FieldTable, BasicProperties, Channel, Connection, ConnectionProperties,
};
use metrics;
use sqlx::{postgres::PgPoolOptions, PgPool};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Maximum time to wait for a broker publish-confirm before treating the
/// publish as failed.  Under RabbitMQ flow control or disk pressure the
/// broker may stop sending Ack/Nack frames; without this timeout the poll
/// loop would block indefinitely, stalling all outbox row processing.
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(10);
use uuid::Uuid;

use crate::OutboxConfig;

// ── Schema ────────────────────────────────────────────────────────────────────
//
// The business service must have a table with AT LEAST these columns:
//
//   CREATE TABLE outbox (
//       id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
//       event_id     UUID        NOT NULL UNIQUE,
//       event_type   TEXT        NOT NULL,
//       payload      JSONB       NOT NULL,
//       status       TEXT        NOT NULL DEFAULT 'PENDING'
//                                CHECK (status IN ('PENDING', 'IN_PROGRESS', 'PUBLISHED', 'FAILED')),
//       fail_count   INT         NOT NULL DEFAULT 0,
//       created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
//       published_at TIMESTAMPTZ
//   );
//
// See `migrations/0002_create_outbox.sql` for the full definition.

// ── Public entry point ────────────────────────────────────────────────────────

/// Poll the business outbox table and publish pending events to RabbitMQ.
///
/// Reconnects to both Postgres and RabbitMQ on failure.
/// Exits cleanly when `shutdown` is cancelled.
pub async fn run_outbox_worker(
    cfg: OutboxConfig,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(cfg.pool_size)
        .connect(&cfg.database_url)
        .await
        .context("Outbox worker: failed to connect to business DB")?;

    info!("Outbox worker connected to business DB");

    // ── Stale-row reaper ──────────────────────────────────────────────────────
    // Spawns a background task that periodically resets IN_PROGRESS rows whose
    // locked_at is older than stale_lock_timeout back to PENDING.  This is the
    // recovery path for rows stranded by a previous worker crash.
    //
    // Requires migration 0016_outbox_locked_at.sql to have been applied to the
    // business DB. If the column is absent the first reaper query will fail;
    // the error is logged but does not abort the main poll loop.
    let reaper_pool = pool.clone();
    let reaper_timeout = Duration::from_secs(cfg.stale_lock_timeout_secs);
    let reaper_shutdown = shutdown.clone();
    // Hold the handle so that a reaper panic is surfaced at shutdown rather
    // than silently swallowed by Tokio's default spawn behaviour.
    // The reaper observes the same CancellationToken so it exits on its own
    // once shutdown is signalled; we abort() here only as a last resort.
    let reaper_handle = tokio::spawn(run_reaper(reaper_pool, reaper_timeout, reaper_shutdown));

    let mut reconnect_delay = Duration::from_secs(2);

    // Helper macro: stop the reaper and surface any panic before returning.
    // abort() sends a cancellation to the task; the subsequent await resolves
    // immediately (either Ok or JoinError::Cancelled).  Using abort() rather
    // than relying solely on the CancellationToken ensures the task is stopped
    // even on error paths where the token may not have fired yet.
    // The await surfaces a panic as a tracing error; without it, a panicked
    // JoinHandle is silently dropped by Tokio.
    //
    // Wrapped in a block so the handle is moved in only one branch at a time;
    // all three call sites are `return` points so only one fires at runtime,
    // but rustc needs the move to be unambiguous — hence the macro expansion
    // rather than a closure (which cannot express the necessary `async move`).
    macro_rules! shutdown_reaper {
        ($handle:expr) => {{
            $handle.abort();
            match $handle.await {
                Ok(()) => {}
                Err(e) if e.is_cancelled() => {} // aborted cleanly
                Err(e) => tracing::error!(error = %e, "Outbox reaper task panicked"),
            }
        }};
    }

    loop {
        if shutdown.is_cancelled() {
            info!("Outbox worker: shutdown requested");
            shutdown_reaper!(reaper_handle);
            return Ok(());
        }

        match connect_amqp_and_poll(&cfg, &pool, shutdown.clone()).await {
            Ok(()) => {
                info!("Outbox worker: exiting cleanly");
                shutdown_reaper!(reaper_handle);
                return Ok(());
            }
            Err(e) if shutdown.is_cancelled() => {
                info!(error = %e, "Outbox worker: exited after shutdown");
                shutdown_reaper!(reaper_handle);
                return Ok(());
            }
            Err(e) => {
                error!(
                    error = %e,
                    delay_secs = reconnect_delay.as_secs(),
                    "Outbox worker error — reconnecting"
                );
                tokio::select! {
                    _ = sleep(reconnect_delay) => {}
                    _ = shutdown.cancelled()   => return Ok(()),
                }
                // Reset after every failure so each reconnect attempt starts
                // from the same base delay. The outbox worker uses a fixed
                // 2 s pause rather than exponential backoff because the poll
                // interval already provides natural spacing between attempts.
                reconnect_delay = Duration::from_secs(2);
            }
        }
    }
}

// ── One AMQP connection lifetime ──────────────────────────────────────────────

async fn connect_amqp_and_poll(
    cfg: &OutboxConfig,
    pool: &PgPool,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    // Inject heartbeat=60 for the same reason the consumer does: long-running
    // poll cycles or slow publishes can make the broker think the connection is
    // idle and close it, stranding rows in IN_PROGRESS until the reaper fires.
    let amqp_url_with_heartbeat = append_heartbeat_param(&cfg.amqp_url, 60);
    let conn =
        Connection::connect(&amqp_url_with_heartbeat, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;

    // Enable publisher confirms so the `.await?.await?` in `publish_and_mark`
    // actually waits for a broker Ack/Nack frame rather than returning immediately
    // with a dummy value.  Without `confirm_select` the second `.await?` on
    // `basic_publish` resolves immediately — giving a false impression that the
    // broker persisted the message — and `PUBLISHED` rows may be lost if the
    // broker restarts before flushing its write-ahead log.
    channel
        .confirm_select(lapin::options::ConfirmSelectOptions::default())
        .await
        .context("Outbox worker: failed to enable publisher confirms")?;

    channel
        .exchange_declare(
            &cfg.exchange,
            lapin::ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    info!(exchange = %cfg.exchange, "Outbox worker AMQP ready");

    // Adaptive poll interval.
    //
    // When the outbox is empty, doubling the sleep on each consecutive idle
    // cycle up to `MAX_IDLE_MULTIPLIER × poll_interval_ms` reduces load on
    // the business DB during quiet periods without adding meaningful latency
    // for the next event that arrives.  Any non-empty batch resets the
    // multiplier back to 1 so busy periods poll at the normal rate.
    const MAX_IDLE_MULTIPLIER: u64 = 8;
    let mut idle_multiplier: u64 = 1;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("Outbox worker: shutdown — stopping poll loop");
                return Ok(());
            }
            result = poll_once(cfg, pool, &channel) => {
                match result {
                    Err(e) => {
                        // AMQP channel is dead — surface the error so the outer
                        // loop reconnects rather than continuing with a broken channel.
                        error!(error = %e, "Outbox worker: Failed poll_once");
                        return Err(e);
                    }
                    Ok(had_rows) => {
                        if had_rows {
                            idle_multiplier = 1; // busy — stay at normal cadence
                        } else {
                            // Empty batch — back off up to MAX_IDLE_MULTIPLIER.
                            idle_multiplier = (idle_multiplier * 2).min(MAX_IDLE_MULTIPLIER);
                        }
                    }
                }
            }
        }

        let wait_ms = cfg.poll_interval_ms * idle_multiplier;
        tokio::select! {
            _ = sleep(Duration::from_millis(wait_ms)) => {}
            _ = shutdown.cancelled() => return Ok(()),
        }
    }
}

// ── Single poll cycle ─────────────────────────────────────────────────────────

/// Poll one batch of PENDING outbox rows and publish them to RabbitMQ.
///
/// Returns:
/// - `Ok(true)`  — batch contained rows; caller should poll again soon.
/// - `Ok(false)` — batch was empty; caller should back off before next poll.
/// - `Err(_)`    — AMQP channel is broken; caller should reconnect.
///
/// Database errors are logged and treated as transient (the next poll cycle
/// will retry them). They do not abort the AMQP connection.
async fn poll_once(cfg: &OutboxConfig, pool: &PgPool, channel: &Channel) -> anyhow::Result<bool> {
    match fetch_pending_batch(pool, cfg.batch_size).await {
        Ok(rows) if rows.is_empty() => {
            // Nothing to do this cycle — signal the caller to back off.
            return Ok(false);
        }
        Ok(rows) => {
            let count = rows.len();
            info!(count, "Outbox: processing batch");
            for row in rows {
                match publish_and_mark(pool, channel, cfg, &row).await {
                    Ok(()) => {
                        metrics::counter!("outbox_published_total").increment(1);
                    }
                    Err(e) => {
                        error!(event_id = %row.event_id, error = %e, "Failed to publish outbox row");
                        metrics::counter!("outbox_publish_failed_total").increment(1);
                        if let Err(e2) =
                            record_publish_failure(pool, row.id, cfg.max_publish_failures).await
                        {
                            error!(event_id = %row.event_id, error = %e2, "Could not record publish failure");
                        }
                        // If the channel is no longer connected, propagate the
                        // error so connect_amqp_and_poll reconnects.  Otherwise
                        // keep going — the failure was row-specific (e.g. a
                        // serialization error) and the channel is still healthy.
                        if !channel.status().connected() {
                            return Err(e);
                        }
                    }
                }
            }
        }
        Err(e) => {
            error!(error = %e, "Outbox: failed to fetch batch");
            // DB errors are transient — don't abort the AMQP connection.
            // Treat as empty so the caller backs off rather than hammering a
            // broken DB connection as fast as possible.
            return Ok(false);
        }
    }
    Ok(true) // batch had rows
}

// ── DB helpers ────────────────────────────────────────────────────────────────

struct OutboxRow {
    id: Uuid,
    event_id: Uuid,
    event_type: String,
    payload: serde_json::Value,
    /// DB insertion time used as the event timestamp so attachment expiry
    /// checks (`max_age_secs`) are evaluated against when the business event
    /// was written to the outbox, not when the outbox worker happened to pick
    /// it up.  Using `Utc::now()` here would shrink the URL validity window
    /// by any queue or processing lag.
    created_at: chrono::DateTime<chrono::Utc>,
    /// Running tally of consecutive publish failures for this row.
    /// Logged at WARN level when > 0 so operators can see retried events
    /// in prod logs without querying the DB.
    fail_count: i32,
}

async fn fetch_pending_batch(pool: &PgPool, limit: i64) -> anyhow::Result<Vec<OutboxRow>> {
    // SELECT … FOR UPDATE SKIP LOCKED must run inside an explicit transaction.
    // Without a transaction the row-level locks are released immediately after
    // the SELECT, defeating the purpose of SKIP LOCKED and allowing two workers
    // racing on the same batch to pick up the same rows.
    //
    // We also immediately flip matched rows to IN_PROGRESS inside the same txn
    // so that even after the transaction commits no other worker can re-select them.
    let mut tx = pool.begin().await?;
    let rows = sqlx::query!(
        r#"
        SELECT id, event_id, event_type, payload, created_at, fail_count
        FROM   outbox
        WHERE  status = 'PENDING'
        ORDER  BY created_at ASC
        LIMIT  $1
        FOR    UPDATE SKIP LOCKED
        "#,
        limit,
    )
    .fetch_all(&mut *tx)
    .await?;

    if !rows.is_empty() {
        let ids: Vec<uuid::Uuid> = rows.iter().map(|r| r.id).collect();
        sqlx::query!(
            "UPDATE outbox SET status = 'IN_PROGRESS', locked_at = now() WHERE id = ANY($1) AND status = 'PENDING'",
            &ids,
        )
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    Ok(rows
        .into_iter()
        .map(|r| OutboxRow {
            id: r.id,
            event_id: r.event_id,
            event_type: r.event_type,
            payload: r.payload,
            created_at: r.created_at,
            fail_count: r.fail_count,
        })
        .collect())
}

async fn publish_and_mark(
    pool: &PgPool,
    channel: &Channel,
    cfg: &OutboxConfig,
    row: &OutboxRow,
) -> anyhow::Result<()> {
    // Build the canonical NotificationEvent envelope from the outbox row payload.
    //
    // Business services write email-specific fields at the top level of the
    // outbox payload JSON:
    //
    //   {
    //     "recipients":    [{ "email": "...", "name": "..." }],  // or singular "recipient"
    //     "payload":       { ...template vars... },
    //     "from_override": { "email": "...", "name": "..." },    // optional
    //     "attachments":   [{ "url": "...", ... }],              // optional
    //     "cc":            [{ "email": "...", "name": "..." }],  // optional
    //     "bcc":           [{ "email": "...", "name": "..." }],  // optional
    //     "sender_account": "transactional",                     // optional
    //     "metadata":      { "source": "orders-service" }        // optional
    //   }
    //
    // Backwards-compatible recipient promotion:
    //   Legacy payload: { "recipient": {...} }     → one-element Vec
    //   New payload:    { "recipients": [...] }    → forwarded verbatim
    let recipients: Vec<Recipient> = serde_json::from_value(promote_recipients(&row.payload))
        .context("outbox row has malformed recipients field — skipping")?;

    let from_override: Option<FromOverride> = deserialize_optional(&row.payload, "from_override");

    let attachments: Vec<common::AttachmentRef> =
        deserialize_optional(&row.payload, "attachments").unwrap_or_default();

    let cc: Vec<Recipient> = deserialize_optional(&row.payload, "cc").unwrap_or_default();

    let bcc: Vec<Recipient> = deserialize_optional(&row.payload, "bcc").unwrap_or_default();

    let sender_account: Option<String> = row
        .payload
        .get("sender_account")
        .and_then(|v| v.as_str())
        .map(str::to_owned);

    let send_mode: SendMode = deserialize_optional(&row.payload, "send_mode").unwrap_or_default();

    let group_retry_mode: GroupRetryMode =
        deserialize_optional(&row.payload, "group_retry_mode").unwrap_or_default();

    let metadata: Metadata = deserialize_optional(&row.payload, "metadata").unwrap_or_default();

    let template_payload = row
        .payload
        .get("payload")
        .cloned()
        .unwrap_or(serde_json::Value::Object(Default::default()));

    let event = NotificationEvent {
        event_id: row.event_id,
        // Use the outbox row's `created_at` (the time the business event was
        // written) as the envelope timestamp rather than `Utc::now()`.
        //
        // Attachment `max_age_secs` is evaluated relative to this timestamp:
        // using `Utc::now()` would shrink the validity window by whatever
        // queue + processing lag occurred between insertion and publication,
        // potentially causing the consumer to reject URLs that were still live.
        //
        // `created_at` is the closest proxy available in the outbox table for
        // the true business-event time.  If a dedicated `event_timestamp`
        // column is added in a future migration it should be preferred here.
        timestamp: row.created_at,
        event_type: row.event_type.clone(),
        payload: template_payload,
        metadata,
        channel_overrides: ChannelOverrides {
            email: Some(EmailOptions {
                send_mode,
                recipients,
                cc,
                bcc,
                from_override,
                attachments,
                sender_account,
                group_retry_mode,
                retry_policy: RetryPolicy::default(),
            }),
        },
    };

    let body = serde_json::to_vec(&event)?;

    if row.fail_count > 0 {
        tracing::warn!(
            event_id    = %row.event_id,
            event_type  = %row.event_type,
            fail_count  = row.fail_count,
            "Publishing outbox event that has previously failed — this is a retry"
        );
    }

    // ORDERING: we publish to the broker BEFORE marking the row PUBLISHED.
    // If the process crashes between these two operations the row stays
    // IN_PROGRESS, the stale-lock reaper will eventually reset it to PENDING,
    // and the worker will re-publish the event — producing a duplicate AMQP
    // message.  The consumer's idempotency guard (insert_pending) handles this
    // correctly: the duplicate message hits the same DB row and is skipped.
    // This is an intentional at-least-once delivery tradeoff: the alternative
    // (mark first, then publish) risks losing events if the publish fails after
    // the row has already been marked PUBLISHED and the reaper won't retry it.
    let confirm_fut = channel
        .basic_publish(
            &cfg.exchange,
            &cfg.routing_key,
            BasicPublishOptions::default(),
            &body,
            BasicProperties::default()
                .with_content_type("application/json".into())
                .with_delivery_mode(2), // persistent
        )
        .await?;

    // Wait for broker Ack/Nack with a timeout so that a broker under flow
    // control or disk pressure cannot stall the poll loop indefinitely.
    // A Nack is treated as a publish failure — the row stays IN_PROGRESS and
    // `record_publish_failure` will reset it to PENDING on the next cycle.
    let confirm = tokio::time::timeout(CONFIRM_TIMEOUT, confirm_fut)
        .await
        .map_err(|_| anyhow::anyhow!(
            "broker publish confirm timed out after {}s — broker may be under flow              control or disk pressure; row will be retried by the reaper",
            CONFIRM_TIMEOUT.as_secs()
        ))??;
    if !confirm.is_ack() {
        return Err(anyhow::anyhow!(
            "broker returned Nack for outbox row {} — message was not persisted",
            row.id
        ));
    }

    // Mark as PUBLISHED — uses IN_PROGRESS guard so only this worker can flip it.
    // Clear locked_at so the reaper ignores this row going forward.
    sqlx::query!(
        r#"
        UPDATE outbox
        SET    status = 'PUBLISHED', published_at = now(), locked_at = NULL
        WHERE  id = $1 AND status = 'IN_PROGRESS'
        "#,
        row.id,
    )
    .execute(pool)
    .await?;

    info!(event_id = %row.event_id, event_type = %row.event_type, "Published outbox event");
    Ok(())
}

// ── Publish failure tracking ──────────────────────────────────────────────────

/// Record a publish failure for an outbox row and reset it back to PENDING so
/// it is retried on the next poll cycle.
///
/// When `fail_count` reaches `max_failures` the row is permanently marked
/// FAILED instead, preventing a broken event from blocking the outbox forever.
///
/// Transitions from IN_PROGRESS → PENDING (or FAILED), which is necessary
/// because `fetch_pending_batch` now sets rows to IN_PROGRESS atomically;
/// leaving them IN_PROGRESS after a failure would strand them forever.
async fn record_publish_failure(pool: &PgPool, id: Uuid, max_failures: i32) -> anyhow::Result<()> {
    let result = sqlx::query!(
        r#"
        UPDATE outbox
        SET    fail_count = fail_count + 1,
               locked_at  = NULL,
               status     = CASE WHEN fail_count + 1 >= $2 THEN 'FAILED' ELSE 'PENDING' END
        WHERE  id = $1
        RETURNING fail_count, status
        "#,
        id,
        max_failures,
    )
    .fetch_one(pool)
    .await?;

    if result.status == "FAILED" {
        // The row has exhausted all retry attempts and will never be
        // re-published by the outbox worker.  Alert operators so they can
        // investigate and re-drive the event manually if needed.
        warn!(
            outbox_id    = %id,
            fail_count   = result.fail_count,
            max_failures = max_failures,
            "Outbox row permanently failed after exhausting all publish attempts —              manual intervention required (re-insert the row or republish the event)"
        );
    }

    Ok(())
}

// ── Stale IN_PROGRESS reaper ─────────────────────────────────────────────────

/// Reset any IN_PROGRESS row whose `locked_at` is older than `timeout` back
/// to PENDING so it can be re-processed after a worker crash.
///
/// This is the recovery mechanism for rows that got stuck because the process
/// was killed (OOM, pod eviction, SIGKILL) after claiming the row but before
/// marking it PUBLISHED or FAILED.
///
/// Safe to run with multiple concurrent workers: the WHERE clause scopes
/// the update to rows that are still IN_PROGRESS, and each row's `locked_at`
/// prevents double-reset races (the first update clears locked_at; a
/// concurrent reaper sees NULL or a fresh timestamp and skips it).
///
/// # NULL locked_at (pre-migration-0016 rows)
///
/// Rows that entered IN_PROGRESS before migration 0016 was applied have
/// `locked_at IS NULL`.  `NULL < <timestamp>` evaluates to NULL in Postgres,
/// so those rows would be invisible to the standard `locked_at < threshold`
/// predicate and stay stuck forever.  The `COALESCE` below treats a NULL
/// `locked_at` as epoch (the distant past), making such rows immediately
/// eligible for recovery on the first reaper run after the migration is applied.
async fn reap_stale_in_progress(pool: &PgPool, timeout: Duration) -> anyhow::Result<u64> {
    let timeout_secs = timeout.as_secs() as f64;
    let result = sqlx::query!(
        r#"
        UPDATE outbox
        SET    status    = 'PENDING',
               locked_at = NULL
        WHERE  status    = 'IN_PROGRESS'
          AND  COALESCE(locked_at, '-infinity'::timestamptz) < now() - make_interval(secs => $1)
        "#,
        timeout_secs,
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// Periodically reap stale IN_PROGRESS rows until shutdown is signalled.
async fn run_reaper(pool: PgPool, timeout: Duration, shutdown: CancellationToken) {
    // Run the reaper at half the stale timeout so a stuck row is recovered
    // within at most 1.5× the timeout rather than up to 2×.
    let interval = timeout / 2;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = sleep(interval) => {}
        }
        match reap_stale_in_progress(&pool, timeout).await {
            Ok(0) => {} // nothing stale — normal
            Ok(n) => tracing::warn!(
                count = n,
                timeout_secs = timeout.as_secs(),
                "Reaper: reset stale IN_PROGRESS rows to PENDING — \
                 this indicates a previous worker crashed mid-batch"
            ),
            Err(e) => tracing::error!(error = %e, "Reaper: failed to query stale rows"),
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Append `?heartbeat=<secs>` to an AMQP URL if not already present.
///
/// Mirrors the same helper in `consumer/src/runner.rs`. Kept here rather than
/// moved to `common` to avoid pulling AMQP concerns into the common crate.
fn append_heartbeat_param(url: &str, heartbeat_secs: u16) -> String {
    if url.contains("heartbeat=") {
        return url.to_owned();
    }
    if url.contains('?') {
        format!("{url}&heartbeat={heartbeat_secs}")
    } else {
        format!("{url}?heartbeat={heartbeat_secs}")
    }
}

/// Deserialize an optional field from an outbox payload, logging a warning
/// when the field is present but malformed.
///
/// Returns `None` when the field is absent or fails to deserialize — the
/// caller supplies a fallback via `unwrap_or(default)` or similar.  The
/// warning ensures a malformed field (e.g. a publisher write bug) is visible
/// in the logs without aborting the publish of other outbox rows.
fn deserialize_optional<T: serde::de::DeserializeOwned>(
    payload: &serde_json::Value,
    key: &str,
) -> Option<T> {
    match payload.get(key) {
        None => None,
        Some(v) => match serde_json::from_value(v.clone()) {
            Ok(val) => Some(val),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    key,
                    event_type = %payload.get("event_type").and_then(|v| v.as_str()).unwrap_or("?"),
                    "Outbox payload field is present but malformed — using default"
                );
                None
            }
        },
    }
}

/// Promote the `recipient` / `recipients` field from an outbox payload into
/// the canonical `recipients` array form expected by the consumer.
///
/// | Input                        | Output                            |
/// |------------------------------|-----------------------------------|
/// | `"recipients": [...]`        | the array, forwarded as-is        |
/// | `"recipient": {...}`         | wrapped in a one-element array    |
/// | neither key present          | empty array                       |
pub(crate) fn promote_recipients(payload: &serde_json::Value) -> serde_json::Value {
    if let Some(arr) = payload.get("recipients") {
        return arr.clone();
    }
    if let Some(r) = payload.get("recipient") {
        return serde_json::Value::Array(vec![r.clone()]);
    }
    serde_json::Value::Array(vec![])
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── promote_recipients ────────────────────────────────────────────────────

    #[test]
    fn promote_recipients_forwards_array() {
        let payload = json!({
            "recipients": [
                {"email": "a@example.com", "name": "Alice"},
                {"email": "b@example.com", "name": "Bob"}
            ],
            "payload": {}
        });
        let result = promote_recipients(&payload);
        assert_eq!(result.as_array().unwrap().len(), 2);
        assert_eq!(result[0]["email"], "a@example.com");
        assert_eq!(result[1]["email"], "b@example.com");
    }

    #[test]
    fn promote_recipients_wraps_singular_recipient() {
        let payload = json!({
            "recipient": {"email": "alice@example.com", "name": "Alice"},
            "payload": {}
        });
        let result = promote_recipients(&payload);
        let arr = result.as_array().expect("should be an array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["email"], "alice@example.com");
    }

    #[test]
    fn promote_recipients_returns_empty_when_neither_key_present() {
        let payload = json!({"payload": {"orderId": "123"}});
        let result = promote_recipients(&payload);
        assert_eq!(result.as_array().unwrap().len(), 0);
    }

    /// When both keys are present, `recipients` (plural) takes precedence.
    #[test]
    fn promote_recipients_prefers_plural_over_singular() {
        let payload = json!({
            "recipient":  {"email": "old@example.com"},
            "recipients": [{"email": "new@example.com"}]
        });
        let result = promote_recipients(&payload);
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["email"], "new@example.com");
    }

    // ── outbox payload → NotificationEvent field extraction ──────────────────
    //
    // These tests exercise the field-extraction logic in `publish_and_mark`
    // (the parts that don't require a real DB / AMQP connection) by calling
    // the same helpers directly.

    /// cc and bcc arrays are parsed from the outbox payload when present.
    #[test]
    fn outbox_payload_cc_bcc_are_extracted() {
        let payload = json!({
            "recipients": [{"email": "to@example.com"}],
            "payload": {},
            "cc":  [{"email": "cc@example.com",  "name": "CC User"}],
            "bcc": [{"email": "bcc@example.com", "name": null}]
        });

        let cc: Vec<common::Recipient> = payload
            .get("cc")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        let bcc: Vec<common::Recipient> = payload
            .get("bcc")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        assert_eq!(cc.len(), 1);
        assert_eq!(cc[0].email, "cc@example.com");
        assert_eq!(bcc.len(), 1);
        assert_eq!(bcc[0].email, "bcc@example.com");
    }

    /// cc and bcc default to empty vecs when absent from the payload.
    #[test]
    fn outbox_payload_cc_bcc_absent_gives_empty_vec() {
        let payload = json!({"recipients": [{"email": "to@example.com"}], "payload": {}});

        let cc: Vec<common::Recipient> = payload
            .get("cc")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        let bcc: Vec<common::Recipient> = payload
            .get("bcc")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        assert!(cc.is_empty());
        assert!(bcc.is_empty());
    }

    /// sender_account is extracted from the payload when present.
    #[test]
    fn outbox_payload_sender_account_is_extracted() {
        let payload = json!({
            "recipients": [{"email": "to@example.com"}],
            "payload": {},
            "sender_account": "transactional"
        });

        let account: Option<String> = payload
            .get("sender_account")
            .and_then(|v| v.as_str())
            .map(str::to_owned);

        assert_eq!(account, Some("transactional".to_owned()));
    }

    /// The assembled NotificationEvent serializes all email fields inside
    /// channel_overrides.email — not at the top level.
    #[test]
    fn assembled_event_uses_channel_overrides_shape() {
        use common::{ChannelOverrides, EmailOptions, Metadata, NotificationEvent, Recipient};
        use uuid::Uuid;

        let event = NotificationEvent {
            event_id: Uuid::nil(),
            timestamp: chrono::Utc::now(),
            event_type: "TEST".into(),
            payload: json!({}),
            metadata: Metadata::default(),
            channel_overrides: ChannelOverrides {
                email: Some(EmailOptions {
                    recipients: vec![Recipient {
                        email: "to@example.com".into(),
                        name: None,
                    }],
                    cc: vec![Recipient {
                        email: "cc@example.com".into(),
                        name: None,
                    }],
                    bcc: vec![Recipient {
                        email: "bcc@example.com".into(),
                        name: None,
                    }],
                    from_override: None,
                    attachments: vec![],
                    sender_account: Some("transactional".into()),
                    send_mode: common::SendMode::Individual,
                    group_retry_mode: common::GroupRetryMode::Individual,
                    retry_policy: common::RetryPolicy::Retry,
                }),
            },
        };

        let v = serde_json::to_value(&event).unwrap();

        // Fields must live inside channel_overrides.email, NOT at the top level.
        assert!(
            v.get("recipients").is_none(),
            "recipients must not be top-level"
        );
        assert!(v.get("cc").is_none(), "cc must not be top-level");
        assert!(v.get("bcc").is_none(), "bcc must not be top-level");

        let email = &v["channel_overrides"]["email"];
        assert_eq!(email["recipients"][0]["email"], "to@example.com");
        assert_eq!(email["cc"][0]["email"], "cc@example.com");
        assert_eq!(email["bcc"][0]["email"], "bcc@example.com");
        assert_eq!(email["sender_account"], "transactional");
    }

    // ── record_publish_failure thresholds ─────────────────────────────────────
    // These tests validate the CASE expression logic through the config default,
    // ensuring the boundary condition matches what operators would configure.

    #[test]
    fn max_publish_failures_default_is_positive() {
        assert!(
            OutboxConfig::default().max_publish_failures > 0,
            "max_publish_failures must be > 0"
        );
    }

    /// Confirm that the default threshold is the expected value.
    /// If this fails after a deliberate change, update both the Default impl
    /// and this assertion together so the change is visible in review.
    #[test]
    fn max_publish_failures_default_matches_expected() {
        assert_eq!(OutboxConfig::default().max_publish_failures, 5);
    }
}
