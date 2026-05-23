-- migrations/0028_email_notification_log_group_retry_mode.sql
--
-- Adds `group_retry_mode` to `email_notification_log` so that manual retries
-- via the HTTP API faithfully replay the original retry strategy.
--
-- Without this column, `republish_event()` always restored
-- `GroupRetryMode::Whole` (the default), even for events that were originally
-- published with `GroupRetryMode::Individual`.  That silent downgrade meant
-- a group event with per-recipient DB rows (Individual mode) would be retried
-- as a unit (Whole mode) — skipping the individual-fallback path and risking
-- duplicate sends to recipients whose delivery had already been accepted by
-- the SMTP server in a prior attempt.
--
-- Allowed values mirror the `GroupRetryMode` enum in the Rust codebase:
--   'whole'      — retry the whole group email as a unit (default).
--   'individual' — on failure, fall back to per-recipient individual sends,
--                  skipping addresses that already have a SENT row.
--
-- NULL for rows written before this migration; `republish_event()` falls back
-- to `GroupRetryMode::Whole` for those rows, preserving the previous behaviour.

ALTER TABLE email_notification_log
    ADD COLUMN IF NOT EXISTS group_retry_mode TEXT
        CHECK (group_retry_mode IN ('whole', 'individual'));

COMMENT ON COLUMN email_notification_log.group_retry_mode IS
    'Retry strategy for group-mode events: ''whole'' (retry as a unit) or '
    '''individual'' (fall back to per-recipient sends, skipping SENT rows). '
    'NULL for rows written before migration 0028; treated as ''whole'' on retry.';
