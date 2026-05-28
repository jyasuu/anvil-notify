
CREATE TABLE IF NOT EXISTS outbox (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Stable ID forwarded as NotificationEvent.event_id — used for idempotency
    -- in the notification service.
    event_id     UUID        NOT NULL UNIQUE DEFAULT gen_random_uuid(),

    -- Logical event type, e.g. 'ORDER_CONFIRMATION', 'PASSWORD_RESET'.
    event_type   TEXT        NOT NULL,

    -- Full event body.  See migrations/business_db/0002_create_outbox.sql for
    -- the detailed payload contract.
    payload      JSONB       NOT NULL,

    status       TEXT        NOT NULL DEFAULT 'PENDING'
                             CHECK (status IN ('PENDING', 'IN_PROGRESS', 'PUBLISHED', 'FAILED')),

    -- Incremented by the outbox worker on each failed publish attempt.
    -- Once fail_count reaches the configured threshold the row is permanently
    -- marked FAILED and removed from the retry pool.
    fail_count   INT         NOT NULL DEFAULT 0,

    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,

    -- Set to now() when the worker claims a row (IN_PROGRESS).
    -- Cleared when the row reaches PUBLISHED or FAILED.
    -- Used by the stale-row reaper to detect and recover rows stranded by a
    -- worker crash.  NULL for rows created before migration 0016 was applied.
    locked_at    TIMESTAMPTZ
);

-- FOR UPDATE SKIP LOCKED requires an index on (status, created_at).
CREATE INDEX IF NOT EXISTS outbox_status_created_idx
    ON outbox (status, created_at ASC)
    WHERE status = 'PENDING';

-- Monitoring: find permanently failed rows quickly.
CREATE INDEX IF NOT EXISTS outbox_failed_idx
    ON outbox (created_at DESC)
    WHERE status = 'FAILED';

-- Monitoring: find rows with a From-address override.
CREATE INDEX IF NOT EXISTS outbox_from_override_idx
    ON outbox ((payload->>'from_override'))
    WHERE payload ? 'from_override';

-- Monitoring: find rows with attachments.
CREATE INDEX IF NOT EXISTS outbox_has_attachments_idx
    ON outbox ((jsonb_array_length(payload -> 'attachments')))
    WHERE payload ? 'attachments'
      AND jsonb_array_length(payload -> 'attachments') > 0;

-- Reaper index: find stale IN_PROGRESS rows quickly.
CREATE INDEX IF NOT EXISTS outbox_locked_at_idx
    ON outbox (locked_at ASC)
    WHERE status = 'IN_PROGRESS';
