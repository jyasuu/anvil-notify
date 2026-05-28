-- migrations/0030_email_notification_log_to_recipients.sql
--
-- Adds `to_recipients` to `email_notification_log` so that group sends with
-- GroupRetryMode::Whole store the full recipient list alongside the single
-- tracking row written for the primary address.
--
-- Background
-- ──────────
-- GroupRetryMode::Whole writes exactly ONE notification_log row (for the
-- primary / first recipient) to track the delivery as a unit.  Before this
-- migration, operators querying by any non-primary recipient email would find
-- nothing in the log, and there was no way to determine from a failed event
-- which other addresses were included in the original group email.
--
-- With this column:
--   • The HTTP API status endpoint surfaces the full To: list alongside the
--     primary row's delivery state.
--   • Operators can cross-reference the group email's recipients without
--     digging through logs.
--
-- Semantics
-- ─────────
--   • Non-NULL only for `send_mode = 'group'` AND `group_retry_mode = 'whole'`
--     rows (the only case where not every address gets its own row).
--   • NULL for:
--       - Individual sends (every recipient has their own row, so the column
--         would be redundant).
--       - Group sends with GroupRetryMode::Individual (every recipient also
--         has its own row — same reasoning).
--   • Each element: { "email": "...", "name": "..." }  (mirrors the Recipient
--     struct already used for cc/bcc columns).
--
-- NULL for rows written before this migration; the API falls back gracefully.

ALTER TABLE email_notification_log
    ADD COLUMN IF NOT EXISTS to_recipients JSONB;

COMMENT ON COLUMN email_notification_log.to_recipients IS
    'Full To: recipient list for group sends with group_retry_mode = ''whole''. '
    'NULL for individual sends and group sends with group_retry_mode = ''individual'' '
    '(those modes write one row per recipient, so the column would be redundant). '
    'Each element: {"email": "...", "name": "..."}. NULL for pre-0030 rows.';
