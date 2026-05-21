Looking at the schema evolution across the 23 migrations, here's what I see and a few directions worth thinking through.

**The core tension**

`email_log` started as a simple audit table and has absorbed a lot of email-specific columns over time: `recipient_email`, `from_override`, `cc`, `bcc`, `sender_account`, `send_mode`, `recipient_name`. If you add SMS or push notifications later, you'd either bloat this table further with nullable channel-specific columns, or create parallel `sms_log`, `push_log` tables — which then makes cross-channel queries (e.g. "what notifications did we send for event X?") awkward.

**Three realistic directions**

**Option A — Keep email_log as-is, add sibling tables per channel**

The simplest path. Add `sms_log`, `push_log` etc. as independent tables when needed. Cross-channel visibility lives in a view or in the application layer.

- Pro: zero migration risk to existing data, email path unchanged
- Con: no unified "what happened for event X" query; each channel re-implements status/retry columns

**Option B — Split into a generic `notification_log` + channel detail tables**

A parent table holds the channel-agnostic columns, child tables hold channel-specific data:

```sql
notification_log          -- event_id, event_type, status, retry_count,
                          -- channel, created_at, updated_at, payload
email_notification_log    -- notification_log_id FK, recipient_email,
                          -- from_override, cc, bcc, sender_account, send_mode
```

The idempotency key moves to `notification_log(event_id, channel, recipient_identity)`. Cross-channel queries hit just `notification_log`; email-specific queries join to the child.

- Pro: clean extension point; unified status/retry/observability per channel
- Con: every email query now needs a join; migration of existing data is non-trivial; the `send_mode` / group semantics get more complex to express

**Option C — JSONB `channel_data` column on a unified table**

A single `notification_log` table with a `channel_data jsonb` column for channel-specific fields:

```sql
notification_log (
  event_id, event_type, channel TEXT, recipient_identity TEXT,
  status, retry_count, payload jsonb, channel_data jsonb,
  ...
)
```

Email rows store `{"recipient_email":"...", "cc":[...], "sender_account":"..."}` in `channel_data`; SMS rows store `{"phone_number":"..."}`.

- Pro: single table, no joins, trivially extensible
- Con: loses type safety and indexability on channel-specific fields; harder to query (e.g. "all emails to alice@..."); `recipient_identity` abstraction is leaky

**My honest take**

For your current scale and team size, **Option A is probably the right call for now** — it has zero risk and real new channels are almost always far enough away that you'd rather not pay the migration cost today. The design is already channel-aware at the event level (`NotificationEvent` → `ChannelOverrides`), so the consumer is structurally ready to fan out to new channels; the log table doesn't have to be.

If you do want to invest in Option B, the right time is *before* the second channel ships, not after. The migration would need to:
1. Create `notification_log` and backfill from `email_log`
2. Create `email_notification_log` with the email-specific columns
3. Update the store layer to write both tables transactionally
4. Keep `email_log` readable for a transition period

Would it help to sketch out the Option B schema and migration in more detail, or think through what the store layer API would look like under that design?


<img width="1440" height="1160" alt="image" src="https://github.com/user-attachments/assets/4898a7c1-a3c2-4f47-8a21-fb898f250e40" />



Good call. Let me look at the full schema and store layer before proposing anything concrete.Now I have everything I need. Let me design this properly.Here's the full plan, broken into schema, Rust layer, and migration strategy.

---

## Core idea: `notification_log` + `email_notification_log`

The key insight from reading your store layer is that `EmailLogStore` already has a clean interface — the refactor mostly lives in the DB and the store crate, not in the processor or consumer logic.

---

## Schema

**`notification_log`** — everything channel-agnostic:

```sql
CREATE TABLE notification_log (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id         UUID        NOT NULL,
    event_type       TEXT        NOT NULL,
    channel          TEXT        NOT NULL,   -- 'email', 'sms', 'push'
    recipient_id     TEXT        NOT NULL,   -- email addr, phone, device token
    status           TEXT        NOT NULL DEFAULT 'PENDING'
                                 CHECK (status IN ('PENDING','SENT','FAILED','BLOCKED')),
    retry_count      INT         NOT NULL DEFAULT 0,
    total_attempts   INT         NOT NULL DEFAULT 0,
    last_error       TEXT,
    payload          JSONB       NOT NULL DEFAULT '{}',
    event_timestamp  TIMESTAMPTZ NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT notification_log_idempotency
        UNIQUE (event_id, channel, recipient_id)
);

CREATE INDEX notification_log_event_id_idx  ON notification_log (event_id);
CREATE INDEX notification_log_status_idx    ON notification_log (status);
CREATE INDEX notification_log_channel_idx   ON notification_log (channel);
CREATE INDEX notification_log_created_at_idx ON notification_log (created_at DESC);
```

`recipient_id` is the channel-native identity — email address for email, E.164 phone number for SMS, device token for push. This is what the idempotency key needs; you never need a single "identity" type that spans channels.

**`email_notification_log`** — email-only columns, 1:1 with the parent:

```sql
CREATE TABLE email_notification_log (
    notification_id  UUID PRIMARY KEY
                         REFERENCES notification_log(id) ON DELETE CASCADE,
    recipient_email  TEXT        NOT NULL,   -- mirrors recipient_id, typed
    recipient_name   TEXT,
    from_override    JSONB,
    sender_account   TEXT,
    send_mode        TEXT CHECK (send_mode IN ('individual','group')),
    cc               JSONB,
    bcc              JSONB,
    attachments      JSONB
);
```

Future SMS or push channels just add their own sibling tables (`sms_notification_log`, `push_notification_log`) without touching `notification_log` at all.

**`notification_template`** — rename `email_template`, add a `channel` column:

```sql
ALTER TABLE email_template RENAME TO notification_template;

ALTER TABLE notification_template
    ADD COLUMN channel TEXT NOT NULL DEFAULT 'email';

-- New PK: (type, channel) so the same event type can have different
-- templates per channel (e.g. ORDER_CONFIRMATION email vs SMS)
ALTER TABLE notification_template DROP CONSTRAINT email_template_pkey;
ALTER TABLE notification_template ADD PRIMARY KEY (type, channel);
```

---

## Rust store layer

The cleanest approach is a **unified trait** with channel-specific implementations. This keeps the processor code largely unchanged — it still calls `store.insert_pending(...)`, `store.mark_sent(...)` etc.

```rust
// crates/store/src/notification_log.rs

#[async_trait]
pub trait NotificationStore: Send + Sync + Clone {
    type PendingArgs<'a>: Send;

    async fn insert_pending(
        &self,
        args: Self::PendingArgs<'_>,
    ) -> Result<InsertResult, AppError>;

    async fn mark_sent(&self, id: Uuid) -> Result<(), AppError>;
    async fn mark_failed(&self, id: Uuid, error: &str, exhausted: bool)
        -> Result<(), AppError>;
    async fn mark_blocked(&self, id: Uuid, reason: &str) -> Result<(), AppError>;
    async fn get_by_event_id(&self, event_id: Uuid) -> Result<Vec<NotificationLog>, AppError>;
}
```

`EmailNotificationStore` implements this trait, writing to both tables in a single transaction:

```rust
pub struct EmailNotificationStore {
    pool: PgPool,
}

impl EmailNotificationStore {
    async fn insert_pending_inner(
        &self,
        args: &EmailInsertPendingArgs<'_>,
    ) -> Result<InsertResult, AppError> {
        let mut tx = self.pool.begin().await?;

        // Insert into notification_log (channel-agnostic)
        let row = sqlx::query!(r#"
            INSERT INTO notification_log
                (event_id, event_type, channel, recipient_id, payload, event_timestamp)
            VALUES ($1, $2, 'email', $3, $4, $5)
            ON CONFLICT (event_id, channel, recipient_id) DO UPDATE
                SET updated_at = notification_log.updated_at
            RETURNING id, retry_count, status, (xmax <> 0) AS "was_conflict!: bool"
        "#, args.event_id, args.event_type, args.recipient_email,
            args.payload, args.event_timestamp)
        .fetch_one(&mut *tx).await?;

        if !row.was_conflict {
            // Insert email-specific detail row
            sqlx::query!(r#"
                INSERT INTO email_notification_log
                    (notification_id, recipient_email, recipient_name,
                     from_override, sender_account, send_mode, cc, bcc, attachments)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#, row.id, args.recipient_email, args.recipient_name,
                args.from_override, args.sender_account, args.send_mode,
                args.cc, args.bcc, args.attachments)
            .execute(&mut *tx).await?;
        }

        tx.commit().await?;

        if row.was_conflict {
            Ok(InsertResult::Duplicate {
                retry_count: row.retry_count,
                status: row.status,
            })
        } else {
            Ok(InsertResult::Inserted)
        }
    }
}
```

`mark_sent`, `mark_failed`, `mark_blocked` all target `notification_log(id)` — no join needed, status lives on the parent.

The `republish_event` handler in `api/src/handlers.rs` joins both tables to reconstruct the full event:

```sql
SELECT n.*, e.recipient_email, e.recipient_name, e.from_override,
       e.sender_account, e.send_mode, e.cc, e.bcc, e.attachments
FROM   notification_log n
JOIN   email_notification_log e ON e.notification_id = n.id
WHERE  n.event_id = $1 AND n.channel = 'email'
ORDER BY n.created_at
```

---

## Migration strategy

The tricky part is getting existing `email_log` data into the new tables without downtime. A safe path:

**Migration 0024** — create new tables alongside `email_log` (additive, no risk):
```sql
CREATE TABLE notification_log (...);
CREATE TABLE email_notification_log (...);
```

**Migration 0025** — backfill from `email_log` in one statement:
```sql
WITH inserted AS (
    INSERT INTO notification_log
        (event_id, event_type, channel, recipient_id, status, retry_count,
         total_attempts, last_error, payload, event_timestamp, created_at, updated_at)
    SELECT event_id, event_type, 'email', recipient_email, status, retry_count,
           total_attempts, last_error, payload,
           COALESCE(event_timestamp, created_at), created_at, updated_at
    FROM email_log
    RETURNING id, (SELECT recipient_email FROM email_log el
                   WHERE el.event_id = notification_log.event_id LIMIT 1) AS remail
)
INSERT INTO email_notification_log
    (notification_id, recipient_email, ...)
SELECT i.id, el.recipient_email, el.recipient_name, ...
FROM inserted i
JOIN email_log el ON el.event_id = ...;
```

In practice you'd want to do this in batches with a cursor if `email_log` is large.

**Code cutover** — deploy the new store implementation pointed at the new tables. Both tables exist in parallel; the old `email_log` table is now read-only legacy.

**Migration 0026** — once you've confirmed the new tables are healthy: `DROP TABLE email_log` (or rename to `email_log_archive` and keep for 30 days).

---

## What doesn't change

The processor (`processor.rs`, `runner.rs`) is almost untouched — it talks to the store trait, not the tables directly. `ProcessorContext` just holds `store: Arc<dyn NotificationStore>` instead of `EmailLogStore`. The `TemplateStore` gets a `channel` parameter on `resolve()`.

The biggest change is in `api/src/handlers.rs` where `republish_event` reconstructs events — that function needs the join query above, but the logic stays the same.

---

## Summary of what you gain

| | Now | After |
|---|---|---|
| Add SMS channel | New columns on `email_log` or parallel table, no unified query | New `sms_notification_log` table, all status queries hit `notification_log` |
| "What happened for event X across all channels?" | Impossible in one query | `SELECT * FROM notification_log WHERE event_id = $1` |
| Template per channel | `email_template.type` only | `notification_template (type, channel)` PK |
| Store layer API | `EmailLogStore` (concrete struct) | `NotificationStore` trait, channel-specific impls |

Want me to write out the full migration SQL and the complete Rust store trait + email implementation?



## Refactor plan: `email_log` → multi-channel notification log

---

### Phase 1 — Schema (additive, no breaking changes)

- [ ] Create `notification_log` table with `channel` + `recipient_id` idempotency key
- [ ] Create `email_notification_log` table with FK to `notification_log`
- [ ] Add `channel` column to `email_template`, rename to `notification_template`, update PK to `(type, channel)`
- [ ] Backfill `email_log` → both new tables in batches
- [ ] Add indexes: `(event_id)`, `(status)`, `(channel)`, `(created_at DESC)` on `notification_log`

---

### Phase 2 — Store layer

- [ ] Define `NotificationStore` trait in `crates/store` with `insert_pending`, `mark_sent`, `mark_failed`, `mark_blocked`, `get_by_event_id`
- [ ] Implement `EmailNotificationStore` writing both tables in a single transaction
- [ ] Update `TemplateStore::resolve()` to accept a `channel: &str` parameter
- [ ] Rename `crates/store/src/email_log.rs` → `notification_log.rs`, expose new types from `lib.rs`
- [ ] Write unit tests for `EmailNotificationStore` (insert, conflict, mark_* transitions)

---

### Phase 3 — Consumer + processor

- [ ] Replace `EmailLogStore` with `Arc<dyn NotificationStore>` in `ProcessorContext`
- [ ] Update `process_recipient` and `process_group` to call the new store methods
- [ ] Pass `channel = "email"` through all store calls (sets up the pattern for future channels)
- [ ] Update `InsertPendingArgs` struct — remove email-specific fields, move them to `EmailInsertPendingArgs`

---

### Phase 4 — HTTP API

- [ ] Update `republish_event` to JOIN `notification_log` + `email_notification_log` when reconstructing events
- [ ] Update all list/status endpoints to query `notification_log` instead of `email_log`
- [ ] Add `channel` filter param to list endpoints (e.g. `GET /notifications?channel=email`)
- [ ] Update `ApiState` to hold `Arc<dyn NotificationStore>` instead of `EmailLogStore`

---

### Phase 5 — Cleanup

- [ ] Remove `EmailLogStore` struct and `email_log.rs`
- [ ] Rename `email_template` → `notification_template` in all Rust references
- [ ] Update `common::EmailLog` → `common::NotificationLog` + `common::EmailDetail`
- [ ] Drop `email_log` table (or archive as `email_log_legacy` for one release cycle)
- [ ] Update `README.md`, config docs, and `ns-cli` help text

---

### Phase 6 — Validation

- [ ] Confirm all `.sqlx` query cache files regenerated (`cargo sqlx prepare`)
- [ ] Run existing consumer integration tests against new schema
- [ ] Manually retry a FAILED notification through the API end-to-end
- [ ] Verify Prometheus metrics still emit correctly after cutover
- [ ] Check `ns-cli` commands (`status`, `retry`, `logs`) still work against new tables
