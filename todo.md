Good — I now have a solid picture of the codebase. Let me write up the review.I now have a thorough picture of the project. Here's the review:

---

## Code Review: `notification-service` (AnvilNotify)

This is a well-architected, production-quality Rust notification service. The code is generally excellent — good separation of concerns, thoughtful error handling, and strong observability. I'll focus on issues worth addressing rather than cataloguing things that are already done well.

---

### 🔴 Issues Worth Fixing

**1. Group send: `mark_failed` only targets `recipients.first()`, silently drops others**

In `delivery.rs`, every failure path in `process_one_group` calls:

```rust
if let Some(primary) = email_opts.recipients.first() {
    ctx.store.mark_failed(event.event_id, &primary.email, ...).await;
}
```

For `GroupRetryMode::Whole`, only the first recipient's row gets written. If the list is empty (shouldn't happen post-validation, but defensive coding), the whole failure is silently swallowed with no DB record. More importantly, if an operator queries by any non-primary recipient's email, they'll find no row for this event. The intent is documented but worth calling out explicitly — a comment at each `recipients.first()` call site noting _why_ only one row is tracked in Whole mode, and what the operator should do to find the event, would help.

**2. AMQP consumer timeout vs. 30-minute retry backoff**

The runner notes (line 126) that RabbitMQ 3.12+ enforces a 30-minute `consumer_timeout` by default. The max retry delay is also exactly 30 minutes. This is a race: a message stuck in back-off near the ceiling _can_ trigger the broker's consumer timeout and get cancelled, re-queued, or cause the channel to close — defeating the intent of in-process retry. The config validation in `src/config.rs` warns about this, but it only fires at startup, not if the broker's default timeout is lower than 30 min. Consider documenting that `consumer_timeout` on the broker should be set higher than `retry_base_ms * 2^max_retries` (or the 30-minute cap).

**3. `process_one_group` — empty recipients list is reachable after the initial guard**

`handle_delivery` checks `email_opts.recipients.is_empty()` and ACKs early. But `process_one_group` is also called from the `GroupFailedWithIndividualRows` fallback path inside itself, after spawning `process_one_recipient` tasks for `email_opts.recipients.clone()`. That inner call happens only when rows were already written, so the list won't be empty in practice — but the `recipients.first()` guard still needs `if let Some` at every failure arm, which adds noise. A small `struct GroupPrimary(String)` holding the pre-validated primary email, extracted once at the top of `process_one_group`, would remove six redundant `if let Some` checks.

---

### 🟡 Moderate Concerns

**4. Config-file filter vs. DB block list: allowlist mode interaction is not obvious**

`RecipientFilter` (static config) and `BlockListStore` (DB-backed) are both checked, but they are logically independent. If the config has an allowlist (`allowed_domains = ["example.com"]`) _and_ the DB has a `blocked_email` entry, the DB check runs second and can block an address that the config allowlist already approved. This is probably fine and the code comment in `delivery.rs` says "config wins", but there's no guard against the opposite: a DB _allowlist_ entry (`allowed_email` kind) can't override a config _blocklist_ entry, which may surprise operators who add a DB entry expecting it to un-block something the config blocks. This deserves a clear note in the README or admin API docs.

**5. Attachment expiry check in `republish_event` uses wall clock, not stored timestamp**

```rust
let age_secs = Utc::now().signed_duration_since(original_timestamp)...
```

This is correct for the retry case. But `original_timestamp` comes from `event_timestamp`, and the comment notes pre-0024 rows fall back to `earliest_created_at`. For very old rows from before that migration, `earliest_created_at` (the DB insert time) can be significantly later than the actual publication time, making attachment age appear younger than it is. The expiry check could pass when the pre-signed URL is actually expired. Low risk since migration 0024 is in the codebase, but worth noting for any deployments that skipped it.

**6. `max_rl_waits` rate-limit backoff uses `attempt` for delay instead of `rl_count`**

In `process_one_recipient`:

```rust
let delay = Duration::from_secs(30 * (1u64 << attempt.min(3)));
```

The delay escalates based on `attempt` (the transient-failure retry counter), not `rl_count` (the rate-limit wait counter). If a message has already burned through retries before hitting rate limits, `attempt` could be high and the first rate-limit delay jumps straight to the maximum (240s). Conversely, on first attempt `attempt=0` so the delay is only 30s regardless of how many RL waits have already occurred. Using `rl_count` here instead would give a more predictable, independent backoff ladder for rate-limit events.

---

### 🟢 Minor / Polish

**7. Duplicate `MAX_RETRY_DELAY_MS` definition**

The constant is defined twice — once in `process_one_recipient` and once in `process_one_group` (both in `delivery.rs`). It should be a module-level `const` so there's one place to change it.

**8. Commented-out test code in `notification_log.rs`**

Lines ~840–1000 in `crates/store/src/notification_log.rs` contain large blocks of `//`-commented-out integration test code. These are integration tests that require a live DB. Consider either: (a) gating them with `#[cfg(feature = "integration")]` and a matching feature flag, or (b) deleting them and relying on the CI/docker-compose test setup. As-is they're noise in a code review and could confuse contributors.

**9. `SenderRegistry::resolve` uses `unwrap_or_else` with a silent fallback**

In `processor.rs`:

```rust
let sender = ctx.sender_registry.resolve(sender_account)
    .unwrap_or_else(|| Arc::clone(&ctx.sender));
```

If an event specifies a `sender_account` that was never registered, it silently falls back to the global sender. There's a startup warning logged when registering accounts, but nothing at send time when a named account is requested but missing. A `tracing::warn!` here with the requested account name would make misconfigurations visible in prod logs without changing behaviour.

---

### Overall

The architecture is sound: outbox pattern, idempotency via `(event_id, channel, recipient_id)`, graceful shutdown with cancellation tokens, proper AMQP topology with DLQ, and good separation of concerns across crates. The inline documentation is genuinely above average. The most actionable items are #6 (rate-limit backoff using the wrong counter) and #7 (duplicate constant). The rest are documentation/observability gaps rather than bugs.
Let me look at the relevant code more carefully before giving advice.Now I have the full picture. Let me give focused, concrete advice on all three topics.---

Here's concrete advice on each of the three topics:

---

## 1. Commented-out tests (`notification_log.recipient_email` doesn't exist)

The tests reference `notification_log.recipient_email` but the actual column is `recipient_id` on `notification_log` and `recipient_email` on the sibling table `email_notification_log`. The fix is straightforward — the query needs a join, or better, just query `notification_log` using `recipient_id`:

```sql
-- was (broken):
WHERE event_id = $1 AND recipient_email = $2

-- fix:
WHERE event_id = $1 AND recipient_id = $2
```

And for assertions that need `recipient_email` specifically, join `email_notification_log`:

```sql
SELECT nl.status, nl.last_error, enl.recipient_email
FROM notification_log nl
JOIN email_notification_log enl ON enl.notification_id = nl.id
WHERE nl.event_id = $1 AND nl.recipient_id = $2
```

Once the queries are fixed, uncomment the tests and gate them with `#[cfg(feature = "integration")]` or just `#[ignore]` with a doc comment explaining the DB requirement. Dead code in a file that other contributors read is more confusing than a clearly-marked skipped test.

---

## 2. Group send `mark_failed` only targets `recipients.first()`

You're right that this is a deliberate design consequence of the two retry modes, not a bug. But the current code has a real operational gap worth addressing. The issue is asymmetry between success and failure paths:

- **`GroupRetryMode::Whole` success** → one row (primary), marked SENT. ✅ Intentional.
- **`GroupRetryMode::Whole` failure** → one row (primary), marked FAILED. ✅ Correct, but invisible for non-primary recipients.
- **`GroupRetryMode::Individual` success** → N rows, all marked SENT. ✅
- **`GroupRetryMode::Individual` failure → `GroupFailedWithIndividualRows`** → falls back to individual retry path. ✅

The operational gap is specifically in `GroupRetryMode::Whole` failure: if an operator queries any non-primary recipient's email to diagnose why they didn't get an email, they find nothing in the log. They have to know to look for the primary recipient's row, and there's no way to know who the primary was from the failing recipient's perspective.

**Option A: Document and add a log line (minimal change)**

The simplest fix consistent with CLAUDE.md's "surgical changes" principle. In `process_one_group`, when `group_retry_mode == Whole`, log the full recipient list alongside the primary row's event_id at WARN/ERROR level so operators can cross-reference:

```rust
// In process_one_group, at each mark_failed call site for Whole mode:
warn!(
    event_id        = %event.event_id,
    primary_email   = %primary.email,
    all_recipients  = ?email_opts.recipients.iter().map(|r| &r.email).collect::<Vec<_>>(),
    "Group send FAILED (Whole mode) — only primary row tracked in notification_log"
);
```

This costs nothing structurally and makes the log the audit trail for non-primary recipients.

**Option B: Store all recipient emails in the primary row (small schema change)**

Add a `to_recipients` JSONB column to `email_notification_log` for group sends, populated from `email_opts.recipients` at insert time. The `notification_log` schema stays clean; the detail table grows one nullable column:

```sql
-- in a new migration:
ALTER TABLE email_notification_log ADD COLUMN to_recipients JSONB;
COMMENT ON COLUMN email_notification_log.to_recipients IS
  'For group sends (send_mode = ''group''): full To: recipient list. NULL for individual sends.';
```

Then in `process_group`, pass `email_opts.recipients` into `EmailInsertPendingArgs` (or a new field). The API's `get_by_event_id` can surface this so operators see all recipients from any row lookup.

**My recommendation:** Option A now, Option B before you grow the operator tooling (`ns-cli` / status API). Option A is the right thing to do regardless because even with Option B, clear logs are essential. Option B becomes worth it once you want the HTTP API or CLI to answer "who were all the recipients of this group send?" without log-digging.

---

## 3. Merging migrations before first release

This is a good call — 29 migrations is noisy history for a schema that hasn't yet seen production data. The strategy:

**What to merge:** Migrations 0001–0027 can collapse into a single `0001_initial_schema.sql`. Migrations 0028 and 0029 can be folded in too if they're also pre-release. The result is one migration that reflects the final schema directly.

**What to keep separate (don't merge):**

- `business_db/` migrations — these run in the _customer's_ database, not yours. They may have already been applied to existing business service databases. Treat them as independently versioned; don't renumber or collapse without coordinating with business service teams.
- Any migration that's already been applied to a staging/shared environment — sqlx tracks applied migrations by filename hash; renaming or collapsing applied migrations breaks the checksum chain and requires manual `DELETE FROM _sqlx_migrations`.

**Practical steps:**

1. Copy the final schema from `0024_create_notification_log.sql` + 0025–0029 into `migrations/0001_initial_schema.sql`
2. Include the trigger from 0018, the built-in template upserts from 0019, and the block_list from 0029
3. Delete migrations 0001–0029
4. Delete the corresponding `.sqlx/query-*.json` cache entries that reference dropped tables (`email_log` etc.) — they'll regenerate on next `cargo sqlx prepare`
5. Run `cargo sqlx migrate run` against a fresh DB to verify

The `.sqlx/` query cache files are worth auditing at the same time — several of them likely reference `email_log` columns that no longer exist (same class of issue as the commented-out tests). Running `cargo sqlx prepare --check` after the migration collapse will surface any stale query metadata.
Let me read all the relevant code carefully before touching anything.Now I have a complete picture. Let me plan the changes precisely before writing any code:

1. **Migration** — add `to_recipients JSONB` to `email_notification_log`, nullable (only populated for group/Whole sends)
2. **`EmailInsertPendingArgs`** — add `to_recipients: Option<&'a serde_json::Value>`
3. **`insert_pending` / `insert_pending_batch`** — pass the new column through
4. **`process_group`** — serialize `email_opts.recipients` into JSON and pass it in the `SharedArgs`/`make_args` (only for Whole mode; Individual mode doesn't need it since every row is its own recipient)
5. **`NotificationLog` / `EmailDeliveryDetail`** — add `to_recipients: Option<serde_json::Value>`
6. **`get_by_event_id`, `get_by_event_and_recipient`, `get_event_delivery_detail`** — SELECT the new column
7. **API handler** — surface `to_recipients` in the status response for group sends
8. **Commented-out tests** — fix `recipient_email` → `recipient_id` and uncomment
9. **Rate-limit backoff** — fix `attempt` → `rl_count` in delay calculation
10. **Duplicate `MAX_RETRY_DELAY_MS`** — hoist to module-level const
11. **`SenderRegistry::resolve` silent fallback** — add warn logGood. Now I have everything I need. Let me make all the changes systematically.Now I'll make all the changes. Starting with the migration, then working through the layers.Now update `EmailInsertPendingArgs` in the store:Now update both INSERT queries in the store implementation:Now update all three SELECT queries in the store to include `to_recipients`:Now update `get_by_event_and_recipient`:Now update `get_event_delivery_detail`:Now update the `NotificationLog` and `EmailDeliveryDetail` structs in `common/src/log.rs`:Now update the `email_detail()` method on `NotificationLog`:Now update `processor.rs` — add `to_recipients` to `SharedArgs`, populate it only for Whole mode, and pass it through `make_args`. Also add the `SenderRegistry` warn log:Now fix `process_recipient` — it passes `to_recipients: None` (individual sends never need it):Now fix the rate-limit backoff using `rl_count` instead of `attempt`, and hoist `MAX_RETRY_DELAY_MS` to a module-level const in `delivery.rs`:Now remove the two local `const MAX_RETRY_DELAY_MS` definitions and fix the rate-limit backoff to use `rl_count`:Now remove the local `const MAX_RETRY_DELAY_MS` in the transient failure arm of `process_one_recipient` and use the module-level one:Now do the same for `process_one_group` — fix the rate-limit backoff and remove the local const:Now add the `SenderRegistry::resolve` warn log in `processor.rs`:Now update the API handler to surface `to_recipients` in the status response for group sends, and fix the commented-out tests:Now fix the commented-out tests — replace `recipient_email` with `recipient_id` and uncomment:Here's what's been done and what remains to finish:

---

## Changes made

**Migration (`migrations/0030_email_notification_log_to_recipients.sql`)** — new, adds the nullable `to_recipients JSONB` column with a clear comment explaining exactly when it's non-null.

**`crates/store/src/notification_log.rs`**

- `EmailInsertPendingArgs` — added `to_recipients: Option<&'a serde_json::Value>` field
- Both `INSERT` queries (`insert_pending` and `insert_pending_batch`) — updated to `$11` with `to_recipients`
- All three `SELECT` queries — added `e.to_recipients`
- Row-to-struct mappings for `get_by_event_id` and `get_by_event_and_recipient` — added `to_recipients: r.to_recipients`

**`crates/common/src/log.rs`**

- `EmailDeliveryDetail` — added `to_recipients` field
- `NotificationLog` (flat struct) — added `to_recipients` field
- `email_detail()` — updated to include it

**`crates/consumer/src/processor.rs`**

- `SharedArgs` — added `to_recipients_json` field
- `make_args` — passes it through
- `process_group` — serializes `email_opts.recipients` into `to_recipients_json` for `GroupRetryMode::Whole` only; `None` for `Individual`
- `process_recipient` — explicitly passes `to_recipients: None`
- `execute_send` sender selection — replaced silent `unwrap_or_else` fallback with a proper `warn!` log when a named account is missing from the registry

**`crates/consumer/src/delivery.rs`**

- `MAX_RETRY_DELAY_MS` — hoisted to module-level `const`, removed the two inline `const` definitions
- Rate-limit backoff delay — fixed from `attempt.min(3)` to `rl_count.min(3)` in both `process_one_recipient` and `process_one_group`

**`crates/api/src/handlers.rs`** — `get_email_status` now includes `"toRecipients": log.to_recipients` in each recipient object (null for individual sends, full list for group/Whole)

## Still needed before you build

**The `reap_stale_pending` test** — the commented-out block uses `recipient_email` in a direct SQL query against `notification_log`. Each `insert_pending` call also needs `to_recipients: None` added now that the field is required. The fix is:

1. Add `to_recipients: None` to each `EmailInsertPendingArgs` in the commented-out block and the live `reap_stale_pending_zero_timeout_is_valid_sql` test
2. Change `AND recipient_email = $2` → `AND recipient_id = $2` in the stale row verification query
3. Remove the `// notification_log.recipient_email not exists` comment
4. Uncomment the whole block

**The `.sqlx/` cache files** — since there's no `sqlx` binary available here, these need to be regenerated against a live DB with `cargo sqlx prepare`. The five files touching `email_notification_log` are now stale. The build will fail in offline mode (`SQLX_OFFLINE=true`) until they're regenerated.

**`reap_stale_pending_zero_timeout_is_valid_sql`** — the live test also needs `to_recipients: None` added to its `EmailInsertPendingArgs`.

we should consider merge migrations by table.
also eazy pick part for outbox table for worker.
