//! Database tests for [`EmailNotificationStore`] against a real Postgres.
//!
//! Each `#[sqlx::test]` gets its own temporary database with the workspace
//! migrations applied.  They need `DATABASE_URL` to point at a Postgres
//! instance whose user may `CREATE DATABASE`:
//!
//! ```text
//! docker compose up -d db
//! DATABASE_URL=postgres://user:pass@localhost/notify cargo test -p store
//! ```

use chrono::Utc;
use common::{AppError, NotificationStatus};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{EmailInsertPendingArgs, EmailNotificationStore, InsertResult, NotificationStore};

const EVENT_TYPE: &str = "test.store";

fn args<'a>(
    event_id: Uuid,
    email: &'a str,
    payload: &'a serde_json::Value,
) -> EmailInsertPendingArgs<'a> {
    EmailInsertPendingArgs {
        event_id,
        event_type: EVENT_TYPE,
        recipient_email: email,
        recipient_name: None,
        payload,
        from_override: None,
        attachments: None,
        sender_account: None,
        cc: None,
        bcc: None,
        send_mode: "individual",
        group_retry_mode: None,
        to_recipients: None,
        event_timestamp: Utc::now(),
    }
}

async fn insert(store: &EmailNotificationStore, event_id: Uuid, email: &str) -> InsertResult {
    let payload = serde_json::json!({"k": "v"});
    store
        .insert_pending(&args(event_id, email, &payload))
        .await
        .unwrap()
}

fn is_not_found<T>(r: Result<T, AppError>) -> bool {
    matches!(r, Err(AppError::NotFound(_)))
}

// ── insert_pending / idempotency ──────────────────────────────────────────────

/// The consumer's retry loop relies on this exact contract: a second insert of
/// the same `(event, recipient)` is a `Duplicate` that reports the row's
/// *current* status and retry counter, including `PENDING` after a
/// non-exhausted failure.
#[sqlx::test(migrations = "../../migrations")]
async fn duplicate_insert_reports_current_status_and_retry_count(pool: PgPool) {
    let store = EmailNotificationStore::new(pool);
    let id = Uuid::now_v7();

    assert!(matches!(
        insert(&store, id, "a@example.com").await,
        InsertResult::Inserted
    ));
    assert!(matches!(
        insert(&store, id, "a@example.com").await,
        InsertResult::Duplicate { retry_count: 0, ref status } if status.as_str() == "PENDING"
    ));

    store
        .mark_failed(id, "a@example.com", "boom", false)
        .await
        .unwrap();
    assert!(matches!(
        insert(&store, id, "a@example.com").await,
        InsertResult::Duplicate { retry_count: 1, ref status } if status.as_str() == "PENDING"
    ));

    store.mark_sent(id, "a@example.com").await.unwrap();
    assert!(matches!(
        insert(&store, id, "a@example.com").await,
        InsertResult::Duplicate { retry_count: 1, ref status } if status.as_str() == "SENT"
    ));
}

#[sqlx::test(migrations = "../../migrations")]
async fn insert_pending_batch_mixes_inserted_and_duplicate(pool: PgPool) {
    let store = EmailNotificationStore::new(pool);
    let id = Uuid::now_v7();
    assert!(matches!(
        insert(&store, id, "old@example.com").await,
        InsertResult::Inserted
    ));

    let payload = serde_json::json!({"k": "v"});
    let batch = [
        args(id, "new@example.com", &payload),
        args(id, "old@example.com", &payload),
    ];
    let results = store.insert_pending_batch(&batch).await.unwrap();

    assert_eq!(results.len(), 2);
    assert!(matches!(results[0], InsertResult::Inserted));
    assert!(matches!(results[1], InsertResult::Duplicate { .. }));
    assert_eq!(store.get_by_event_id(id).await.unwrap().len(), 2);
}

// ── mark_* ────────────────────────────────────────────────────────────────────

#[sqlx::test(migrations = "../../migrations")]
async fn mark_failed_keeps_pending_until_exhausted(pool: PgPool) {
    let store = EmailNotificationStore::new(pool);
    let id = Uuid::now_v7();
    insert(&store, id, "a@example.com").await;

    store
        .mark_failed(id, "a@example.com", "first", false)
        .await
        .unwrap();
    let row = store
        .get_by_event_and_recipient(id, "a@example.com")
        .await
        .unwrap();
    assert_eq!(row.status, NotificationStatus::Pending);
    assert_eq!((row.retry_count, row.total_attempts), (1, 1));
    assert_eq!(row.last_error.as_deref(), Some("first"));

    store
        .mark_failed(id, "a@example.com", "second", true)
        .await
        .unwrap();
    let row = store
        .get_by_event_and_recipient(id, "a@example.com")
        .await
        .unwrap();
    assert_eq!(row.status, NotificationStatus::Failed);
    assert_eq!((row.retry_count, row.total_attempts), (2, 2));
    assert_eq!(row.last_error.as_deref(), Some("second"));
}

#[sqlx::test(migrations = "../../migrations")]
async fn mark_sent_and_mark_blocked_set_terminal_status(pool: PgPool) {
    let store = EmailNotificationStore::new(pool);
    let id = Uuid::now_v7();
    insert(&store, id, "sent@example.com").await;
    insert(&store, id, "blocked@example.com").await;

    store.mark_sent(id, "sent@example.com").await.unwrap();
    store
        .mark_blocked(id, "blocked@example.com", "on block list")
        .await
        .unwrap();

    let sent = store
        .get_by_event_and_recipient(id, "sent@example.com")
        .await
        .unwrap();
    assert_eq!(sent.status, NotificationStatus::Sent);
    assert_eq!((sent.retry_count, sent.total_attempts), (0, 1));

    let blocked = store
        .get_by_event_and_recipient(id, "blocked@example.com")
        .await
        .unwrap();
    assert_eq!(blocked.status, NotificationStatus::Blocked);
    assert_eq!(blocked.last_error.as_deref(), Some("on block list"));
}

#[sqlx::test(migrations = "../../migrations")]
async fn operations_on_a_missing_row_are_not_found(pool: PgPool) {
    let store = EmailNotificationStore::new(pool);
    let id = Uuid::now_v7();

    assert!(is_not_found(store.mark_sent(id, "ghost@example.com").await));
    assert!(is_not_found(
        store.mark_failed(id, "ghost@example.com", "e", true).await
    ));
    assert!(is_not_found(
        store.mark_blocked(id, "ghost@example.com", "r").await
    ));
    assert!(is_not_found(
        store.reset_for_retry(id, "ghost@example.com").await
    ));
    assert!(is_not_found(
        store
            .get_by_event_and_recipient(id, "ghost@example.com")
            .await
    ));
    assert!(is_not_found(store.get_event_delivery_detail(id).await));
}

#[sqlx::test(migrations = "../../migrations")]
async fn mark_skipped_is_idempotent_and_not_retryable(pool: PgPool) {
    let store = EmailNotificationStore::new(pool);
    let id = Uuid::now_v7();
    let payload = serde_json::json!({"k": "v"});
    let ts = Utc::now();

    store
        .mark_skipped(
            id,
            EVENT_TYPE,
            "a@example.com",
            "no recipients",
            ts,
            &payload,
        )
        .await
        .unwrap();
    // A re-delivered duplicate must not fail or overwrite the original row.
    store
        .mark_skipped(id, EVENT_TYPE, "a@example.com", "different", ts, &payload)
        .await
        .unwrap();

    let row = store
        .get_by_event_and_recipient(id, "a@example.com")
        .await
        .unwrap();
    assert_eq!(row.status, NotificationStatus::Skipped);
    assert_eq!(row.last_error.as_deref(), Some("no recipients"));
    assert_eq!((row.retry_count, row.total_attempts), (0, 0));

    // SKIPPED rows are not eligible for the manual retry API.
    assert!(is_not_found(
        store.reset_for_retry(id, "a@example.com").await
    ));
    assert!(store
        .reset_all_failed_for_event(id)
        .await
        .unwrap()
        .is_empty());
}

// ── manual retry: reset / revert ──────────────────────────────────────────────

#[sqlx::test(migrations = "../../migrations")]
async fn reset_for_retry_accepts_failed_and_blocked_but_not_sent(pool: PgPool) {
    let store = EmailNotificationStore::new(pool);
    let id = Uuid::now_v7();
    for email in [
        "failed@example.com",
        "blocked@example.com",
        "sent@example.com",
    ] {
        insert(&store, id, email).await;
    }
    store
        .mark_failed(id, "failed@example.com", "e", true)
        .await
        .unwrap();
    store
        .mark_blocked(id, "blocked@example.com", "r")
        .await
        .unwrap();
    store.mark_sent(id, "sent@example.com").await.unwrap();

    store
        .reset_for_retry(id, "failed@example.com")
        .await
        .unwrap();
    store
        .reset_for_retry(id, "blocked@example.com")
        .await
        .unwrap();
    assert!(is_not_found(
        store.reset_for_retry(id, "sent@example.com").await
    ));

    for (email, want) in [
        ("failed@example.com", NotificationStatus::Pending),
        ("blocked@example.com", NotificationStatus::Pending),
        ("sent@example.com", NotificationStatus::Sent),
    ] {
        let row = store.get_by_event_and_recipient(id, email).await.unwrap();
        assert_eq!(row.status, want, "{email}");
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn reset_all_failed_only_touches_failed_rows(pool: PgPool) {
    let store = EmailNotificationStore::new(pool);
    let id = Uuid::now_v7();
    for email in [
        "a@example.com",
        "b@example.com",
        "c@example.com",
        "d@example.com",
    ] {
        insert(&store, id, email).await;
    }
    store
        .mark_failed(id, "a@example.com", "e", true)
        .await
        .unwrap();
    store
        .mark_failed(id, "b@example.com", "e", true)
        .await
        .unwrap();
    store.mark_sent(id, "c@example.com").await.unwrap();
    store.mark_blocked(id, "d@example.com", "r").await.unwrap();

    let mut reset = store.reset_all_failed_for_event(id).await.unwrap();
    reset.sort();
    assert_eq!(reset, ["a@example.com", "b@example.com"]);

    for (email, want) in [
        ("a@example.com", NotificationStatus::Pending),
        ("b@example.com", NotificationStatus::Pending),
        ("c@example.com", NotificationStatus::Sent),
        ("d@example.com", NotificationStatus::Blocked),
    ] {
        let row = store.get_by_event_and_recipient(id, email).await.unwrap();
        assert_eq!(row.status, want, "{email}");
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn revert_to_failed_only_reverts_rows_still_pending(pool: PgPool) {
    let store = EmailNotificationStore::new(pool);
    let id = Uuid::now_v7();
    insert(&store, id, "pending@example.com").await;
    insert(&store, id, "sent@example.com").await;
    store.mark_sent(id, "sent@example.com").await.unwrap();

    let reverted = store
        .revert_to_failed(
            id,
            &[
                "pending@example.com".to_string(),
                "sent@example.com".to_string(),
            ],
            "republish failed",
        )
        .await
        .unwrap();
    assert_eq!(reverted, 1);

    let pending = store
        .get_by_event_and_recipient(id, "pending@example.com")
        .await
        .unwrap();
    assert_eq!(pending.status, NotificationStatus::Failed);
    assert_eq!(pending.last_error.as_deref(), Some("republish failed"));

    // A row the consumer already completed is never clobbered.
    let sent = store
        .get_by_event_and_recipient(id, "sent@example.com")
        .await
        .unwrap();
    assert_eq!(sent.status, NotificationStatus::Sent);
}

// ── queries ───────────────────────────────────────────────────────────────────

#[sqlx::test(migrations = "../../migrations")]
async fn recipient_queries_filter_by_event_and_address(pool: PgPool) {
    let store = EmailNotificationStore::new(pool);
    let id = Uuid::now_v7();
    let other = Uuid::now_v7();
    insert(&store, id, "a@example.com").await;
    insert(&store, id, "b@example.com").await;
    insert(&store, other, "a@example.com").await;

    assert_eq!(store.get_by_event_id(id).await.unwrap().len(), 2);
    assert_eq!(
        store
            .get_recipients_for_event(id, None)
            .await
            .unwrap()
            .len(),
        2
    );

    let only_a = vec!["a@example.com".to_string()];
    let rows = store
        .get_recipients_for_event(id, Some(&only_a))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].recipient_email, "a@example.com");
    assert_eq!(rows[0].event_id, id);

    // An event with no rows is `NotFound` (the API maps it to a 404), not an
    // empty list.
    assert!(is_not_found(store.get_by_event_id(Uuid::now_v7()).await));
}

/// `get_by_event_id` is capped at 500 rows so a very large event cannot load
/// an unbounded result set into memory.
#[sqlx::test(migrations = "../../migrations")]
async fn get_by_event_id_is_capped_at_500_rows(pool: PgPool) {
    let store = EmailNotificationStore::new(pool);
    let id = Uuid::now_v7();
    let payload = serde_json::json!({"k": "v"});
    let emails: Vec<String> = (0..501).map(|i| format!("user{i}@example.com")).collect();
    let batch: Vec<_> = emails.iter().map(|e| args(id, e, &payload)).collect();

    let results = store.insert_pending_batch(&batch).await.unwrap();
    assert_eq!(results.len(), 501);

    assert_eq!(store.get_by_event_id(id).await.unwrap().len(), 500);
}

#[sqlx::test(migrations = "../../migrations")]
async fn event_delivery_detail_returns_event_level_fields(pool: PgPool) {
    let store = EmailNotificationStore::new(pool);
    let id = Uuid::now_v7();
    insert(&store, id, "a@example.com").await;
    insert(&store, id, "b@example.com").await;

    let detail = store.get_event_delivery_detail(id).await.unwrap();
    assert_eq!(detail.event_type, EVENT_TYPE);
    assert_eq!(detail.payload, serde_json::json!({"k": "v"}));
    assert_eq!(detail.send_mode.as_deref(), Some("individual"));
    assert!(detail.from_override.is_none());
    assert!(detail.attachments.is_none());
}
