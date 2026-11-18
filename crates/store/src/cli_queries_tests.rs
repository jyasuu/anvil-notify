//! Database tests for the read-only `anctl` queries in [`crate::cli_queries`].
//! See `notification_log_tests.rs` for how to run them.

use chrono::Utc;
use sqlx::PgPool;
use uuid::Uuid;

use crate::cli_queries::{
    get_status_for_event, get_status_for_recipient, list_notification_logs, list_outbox_rows,
    list_templates, show_template,
};
use crate::{EmailInsertPendingArgs, EmailNotificationStore, NotificationStore};

async fn seed(store: &EmailNotificationStore, event_id: Uuid, event_type: &str, email: &str) {
    let payload = serde_json::json!({"k": "v"});
    store
        .insert_pending(&EmailInsertPendingArgs {
            event_id,
            event_type,
            recipient_email: email,
            recipient_name: None,
            payload: &payload,
            from_override: None,
            attachments: None,
            sender_account: None,
            cc: None,
            bcc: None,
            send_mode: "individual",
            group_retry_mode: None,
            to_recipients: None,
            event_timestamp: Utc::now(),
        })
        .await
        .unwrap();
}

// ── notification_log ──────────────────────────────────────────────────────────

#[sqlx::test(migrations = "../../migrations")]
async fn list_notification_logs_filters_by_status_event_type_and_email(pool: PgPool) {
    let store = EmailNotificationStore::new(pool.clone());
    let (e1, e2) = (Uuid::now_v7(), Uuid::now_v7());
    seed(&store, e1, "order.created", "a@corp.example").await;
    seed(&store, e2, "user.signup", "b@other.example").await;
    store.mark_sent(e1, "a@corp.example").await.unwrap();
    store
        .mark_failed(e2, "b@other.example", "boom", true)
        .await
        .unwrap();

    let all = list_notification_logs(&pool, None, "%", "%", 100)
        .await
        .unwrap();
    assert_eq!(all.len(), 2);

    let sent = list_notification_logs(&pool, Some("SENT"), "%", "%", 100)
        .await
        .unwrap();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].recipient_email, "a@corp.example");
    assert_eq!(sent[0].event_id, e1.to_string());

    // The filters are ILIKE patterns, so they are case-insensitive.
    let by_type = list_notification_logs(&pool, None, "ORDER%", "%", 100)
        .await
        .unwrap();
    assert_eq!(by_type.len(), 1);
    assert_eq!(by_type[0].event_type, "order.created");

    let by_email = list_notification_logs(&pool, Some("FAILED"), "%", "%OTHER.example", 100)
        .await
        .unwrap();
    assert_eq!(by_email.len(), 1);
    assert_eq!(by_email[0].last_error.as_deref(), Some("boom"));

    // Status and pattern filters combine with AND.
    assert!(
        list_notification_logs(&pool, Some("SENT"), "%", "%other%", 100)
            .await
            .unwrap()
            .is_empty()
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn list_notification_logs_is_newest_first_and_respects_limit(pool: PgPool) {
    let store = EmailNotificationStore::new(pool.clone());
    let (e1, e2) = (Uuid::now_v7(), Uuid::now_v7());
    seed(&store, e1, "t", "first@example.com").await;
    seed(&store, e2, "t", "second@example.com").await;
    // Touch the first row last, so it has the most recent `updated_at`.
    store.mark_sent(e1, "first@example.com").await.unwrap();

    let rows = list_notification_logs(&pool, None, "%", "%", 1)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].recipient_email, "first@example.com");
}

#[sqlx::test(migrations = "../../migrations")]
async fn status_queries_return_per_recipient_rows(pool: PgPool) {
    let store = EmailNotificationStore::new(pool.clone());
    let id = Uuid::now_v7();
    seed(&store, id, "t", "a@example.com").await;
    seed(&store, id, "t", "b@example.com").await;
    store
        .mark_failed(id, "b@example.com", "smtp 451", false)
        .await
        .unwrap();

    let rows = get_status_for_event(&pool, id).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].recipient_email, "a@example.com");
    assert_eq!(rows[0].status, "PENDING");
    assert_eq!(rows[1].recipient_email, "b@example.com");
    assert_eq!(
        (rows[1].retry_count, rows[1].last_error.as_deref()),
        (1, Some("smtp 451"))
    );

    assert!(get_status_for_event(&pool, Uuid::now_v7())
        .await
        .unwrap()
        .is_empty());

    let one = get_status_for_recipient(&pool, id, "b@example.com")
        .await
        .unwrap()
        .expect("row for b should exist");
    assert_eq!(one.status, "PENDING");
    assert!(get_status_for_recipient(&pool, id, "ghost@example.com")
        .await
        .unwrap()
        .is_none());
}

// ── templates ─────────────────────────────────────────────────────────────────

async fn add_template(pool: &PgPool, event_type: &str, channel: &str, active: bool) {
    sqlx::query(
        "INSERT INTO notification_template (type, channel, subject, body_html, body_text, active) \
         VALUES ($1, $2, $3, '<b>hi</b>', 'hi', $4)",
    )
    .bind(event_type)
    .bind(channel)
    .bind(format!("{event_type} subject"))
    .bind(active)
    .execute(pool)
    .await
    .unwrap();
}

#[sqlx::test(migrations = "../../migrations")]
async fn template_queries_are_ordered_and_scoped_by_type(pool: PgPool) {
    // The migrations seed some default templates, so use unique type names and
    // look only at the rows this test added.
    add_template(&pool, "TEST_ALPHA", "sms", true).await;
    add_template(&pool, "TEST_ALPHA", "email", true).await;
    add_template(&pool, "TEST_BETA", "email", false).await;

    let all = list_templates(&pool).await.unwrap();
    let mine: Vec<_> = all
        .iter()
        .filter(|t| t.event_type.starts_with("TEST_"))
        .collect();
    let keys: Vec<_> = mine
        .iter()
        .map(|t| (t.event_type.as_str(), t.channel.as_str()))
        .collect();
    assert_eq!(
        keys,
        [
            ("TEST_ALPHA", "email"),
            ("TEST_ALPHA", "sms"),
            ("TEST_BETA", "email"),
        ]
    );
    // Inactive templates are listed too, flagged as such.
    assert!(mine[0].active && !mine[2].active);

    let alpha = show_template(&pool, "TEST_ALPHA").await.unwrap();
    assert_eq!(alpha.len(), 2);
    assert_eq!(alpha[0].channel, "email");
    assert_eq!(alpha[0].body_html, "<b>hi</b>");
    assert_eq!(alpha[0].subject, "TEST_ALPHA subject");

    assert!(show_template(&pool, "NOPE").await.unwrap().is_empty());
}

// ── outbox ────────────────────────────────────────────────────────────────────

#[sqlx::test(migrations = "../../migrations")]
async fn list_outbox_rows_filters_by_status_newest_first_with_limit(pool: PgPool) {
    for (event_type, status, age_minutes) in [
        ("old.pending", "PENDING", 30),
        ("new.pending", "PENDING", 1),
        ("done", "PUBLISHED", 10),
    ] {
        sqlx::query(
            "INSERT INTO outbox (event_type, payload, status, created_at) \
             VALUES ($1, '{}'::jsonb, $2, now() - make_interval(mins => $3))",
        )
        .bind(event_type)
        .bind(status)
        .bind(age_minutes)
        .execute(&pool)
        .await
        .unwrap();
    }

    let pending = list_outbox_rows(&pool, "PENDING", 10).await.unwrap();
    let types: Vec<_> = pending.iter().map(|r| r.event_type.as_str()).collect();
    assert_eq!(types, ["new.pending", "old.pending"]);
    assert!(pending
        .iter()
        .all(|r| r.fail_count == 0 && r.published_at.is_none()));

    let limited = list_outbox_rows(&pool, "PENDING", 1).await.unwrap();
    assert_eq!(limited.len(), 1);
    assert_eq!(limited[0].event_type, "new.pending");

    assert_eq!(
        list_outbox_rows(&pool, "PUBLISHED", 10)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(list_outbox_rows(&pool, "FAILED", 10)
        .await
        .unwrap()
        .is_empty());
}
