//! Database tests for [`BlockListStore`] against a real Postgres.
//! See `notification_log_tests.rs` for how to run them.

use std::time::Duration;

use common::AppError;
use sqlx::PgPool;

use crate::{BlockListChecker, BlockListStore};

/// No caching, so every `check` reads the database.
fn uncached(pool: PgPool) -> BlockListStore {
    BlockListStore::new(pool, Duration::ZERO)
}

fn is_blocked<T>(r: Result<T, AppError>) -> bool {
    matches!(r, Err(AppError::Blocked(_)))
}

fn is_not_found<T>(r: Result<T, AppError>) -> bool {
    matches!(r, Err(AppError::NotFound(_)))
}

#[sqlx::test(migrations = "../../migrations")]
async fn empty_list_allows_everyone(pool: PgPool) {
    let store = uncached(pool);
    assert!(store.is_empty().await);
    assert!(store.check("anyone@example.com").await.is_ok());
}

#[sqlx::test(migrations = "../../migrations")]
async fn blocked_email_matches_case_insensitively(pool: PgPool) {
    let store = uncached(pool);
    store
        .add_entry("blocked_email", "Bad@Example.COM", Some("abuse"))
        .await
        .unwrap();

    assert!(is_blocked(store.check("bad@example.com").await));
    assert!(is_blocked(store.check("BAD@EXAMPLE.COM").await));
    assert!(store.check("good@example.com").await.is_ok());
    assert!(!store.is_empty().await);
}

#[sqlx::test(migrations = "../../migrations")]
async fn blocked_domain_blocks_every_address_at_that_domain(pool: PgPool) {
    let store = uncached(pool);
    store
        .add_entry("blocked_domain", "Spam.Test", None)
        .await
        .unwrap();

    assert!(is_blocked(store.check("a@spam.test").await));
    assert!(is_blocked(store.check("B@SPAM.TEST").await));
    assert!(store.check("a@example.com").await.is_ok());
}

#[sqlx::test(migrations = "../../migrations")]
async fn allowlist_mode_admits_only_allowed_addresses_and_domains(pool: PgPool) {
    let store = uncached(pool);
    store
        .add_entry("allowed_domain", "corp.example", None)
        .await
        .unwrap();
    store
        .add_entry("allowed_email", "friend@other.example", None)
        .await
        .unwrap();

    assert!(store.check("anyone@corp.example").await.is_ok());
    assert!(store.check("Friend@Other.Example").await.is_ok());
    assert!(is_blocked(store.check("stranger@other.example").await));
    // An allowlist is a restriction, so the list is not "empty".
    assert!(!store.is_empty().await);
}

#[sqlx::test(migrations = "../../migrations")]
async fn blocklist_takes_precedence_over_allowlist(pool: PgPool) {
    let store = uncached(pool);
    store
        .add_entry("allowed_domain", "corp.example", None)
        .await
        .unwrap();
    store
        .add_entry("blocked_email", "mallory@corp.example", None)
        .await
        .unwrap();

    assert!(store.check("alice@corp.example").await.is_ok());
    assert!(is_blocked(store.check("mallory@corp.example").await));
}

#[sqlx::test(migrations = "../../migrations")]
async fn remove_entry_unblocks_and_second_removal_is_not_found(pool: PgPool) {
    let store = uncached(pool);
    let entry = store
        .add_entry("blocked_email", "bad@example.com", None)
        .await
        .unwrap();
    assert!(is_blocked(store.check("bad@example.com").await));

    store.remove_entry(entry.id).await.unwrap();
    assert!(store.check("bad@example.com").await.is_ok());
    assert!(store.list_entries().await.unwrap().is_empty());

    assert!(is_not_found(store.remove_entry(entry.id).await));
    assert!(is_not_found(store.remove_entry(i64::MAX).await));
}

#[sqlx::test(migrations = "../../migrations")]
async fn re_adding_an_entry_reactivates_it_and_updates_the_reason(pool: PgPool) {
    let store = uncached(pool);
    let first = store
        .add_entry("blocked_email", "bad@example.com", Some("old reason"))
        .await
        .unwrap();
    store.remove_entry(first.id).await.unwrap();

    let again = store
        .add_entry("blocked_email", "BAD@example.com", Some("new reason"))
        .await
        .unwrap();

    // Same row (the unique key is case-insensitive), now active again.
    assert_eq!(again.id, first.id);
    assert!(again.active);
    assert_eq!(again.reason.as_deref(), Some("new reason"));
    assert_eq!(store.list_entries().await.unwrap().len(), 1);
    assert!(is_blocked(store.check("bad@example.com").await));
}

#[sqlx::test(migrations = "../../migrations")]
async fn cached_snapshot_is_refreshed_only_after_invalidate(pool: PgPool) {
    let store = BlockListStore::new(pool.clone(), Duration::from_secs(60));
    assert!(store.check("late@example.com").await.is_ok()); // loads + caches

    // Change the table behind the store's back.
    sqlx::query("INSERT INTO block_list (kind, value, active) VALUES ('blocked_email', $1, TRUE)")
        .bind("late@example.com")
        .execute(&pool)
        .await
        .unwrap();

    assert!(
        store.check("late@example.com").await.is_ok(),
        "snapshot should still be served from the cache"
    );
    store.invalidate().await;
    assert!(is_blocked(store.check("late@example.com").await));
}

#[sqlx::test(migrations = "../../migrations")]
async fn zero_ttl_disables_caching(pool: PgPool) {
    let store = uncached(pool.clone());
    assert!(store.check("late@example.com").await.is_ok());

    sqlx::query("INSERT INTO block_list (kind, value, active) VALUES ('blocked_email', $1, TRUE)")
        .bind("late@example.com")
        .execute(&pool)
        .await
        .unwrap();

    assert!(is_blocked(store.check("late@example.com").await));
}

#[sqlx::test(migrations = "../../migrations")]
async fn checker_trait_delegates_to_the_store(pool: PgPool) {
    let store = uncached(pool);
    store
        .add_entry("blocked_email", "bad@example.com", None)
        .await
        .unwrap();

    let checker: &dyn BlockListChecker = &store;
    assert!(is_blocked(checker.check("bad@example.com").await));
    assert!(checker.check("ok@example.com").await.is_ok());
}
