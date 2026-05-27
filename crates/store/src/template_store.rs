//! DB-backed template store with a moka TTL cache.
//!
//! The cache key is `"{channel}:{event_type}"`.  Entries expire automatically
//! after `cache_ttl` (default 5 minutes).  For immediate invalidation call
//! [`TemplateStore::invalidate`] or [`TemplateStore::invalidate_all`] via the
//! `DELETE /templates/{event_type}/cache` or `DELETE /templates/cache` endpoints.

use std::time::Duration;

use common::AppError;
use moka::future::Cache;
use sqlx::PgPool;
use tracing::{debug, info, instrument};

/// One template row from the `notification_template` table.
#[derive(Debug, Clone)]
pub struct NotificationTemplate {
    pub subject: String,
    pub body_html: String,
    pub body_text: String,
}

// Back-compat alias.
pub use NotificationTemplate as EmailTemplate;

/// DB-backed template store with a moka TTL cache.
///
/// `Clone` is cheap — all clones share the same underlying moka cache.
#[derive(Clone)]
pub struct TemplateStore {
    pool: PgPool,
    cache: Cache<String, NotificationTemplate>,
}

impl TemplateStore {
    /// Construct with an explicit TTL.  Pass `Duration::ZERO` to disable caching.
    pub fn new_with_ttl(pool: PgPool, cache_ttl: Duration) -> Self {
        let cache = if cache_ttl.is_zero() {
            Cache::builder().max_capacity(0).build()
        } else {
            Cache::builder()
                .max_capacity(1_024) // up to 1 k distinct event-type × channel pairs
                .time_to_live(cache_ttl)
                .build()
        };
        Self { pool, cache }
    }

    /// Construct with the default TTL (5 minutes).
    pub fn new(pool: PgPool) -> Self {
        Self::new_with_ttl(pool, Duration::from_secs(300))
    }

    /// Resolve the template for `(event_type, channel)`.
    ///
    /// Returns `AppError::Template` for unknown pairs so the consumer
    /// immediately routes to DLQ without wasting retry slots.
    #[instrument(skip(self), fields(event_type, channel))]
    pub async fn resolve(
        &self,
        event_type: &str,
        channel: &str,
    ) -> Result<NotificationTemplate, AppError> {
        let key = format!("{channel}:{event_type}");

        if let Some(tpl) = self.cache.get(&key).await {
            debug!("Template cache hit");
            return Ok(tpl);
        }

        let row = sqlx::query!(
            r#"
            SELECT subject, body_html, body_text
            FROM   notification_template
            WHERE  type = $1 AND channel = $2 AND active = TRUE
            "#,
            event_type,
            channel,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(AppError::Database)?;

        let Some(r) = row else {
            return Err(AppError::Template(format!(
                "Unknown event type '{event_type}' for channel '{channel}' \
                 — add a row to notification_template"
            )));
        };

        let tpl = NotificationTemplate {
            subject: r.subject,
            body_html: r.body_html,
            body_text: r.body_text,
        };
        self.cache.insert(key, tpl.clone()).await;
        info!("Template loaded from DB and cached");
        Ok(tpl)
    }

    /// Evict all cache entries for `event_type` across every channel.
    ///
    /// Cache keys are `"{channel}:{event_type}"` so a plain `remove(event_type)`
    /// would miss multi-channel entries.  This method iterates all keys and
    /// drops those whose suffix matches `":{event_type}"`.
    pub async fn invalidate(&self, event_type: &str) {
        let suffix = format!(":{event_type}");
        // moka doesn't expose key iteration for async cache; collect the matching
        // keys from a sync snapshot of the entry set via run_pending_tasks first.
        self.cache.run_pending_tasks().await;
        let keys: Vec<String> = self
            .cache
            .iter()
            .filter(|(k, _)| k.ends_with(&suffix))
            .map(|(k, _)| k.as_ref().clone())
            .collect();
        let count = keys.len();
        for k in keys {
            self.cache.invalidate(&k).await;
        }
        info!(
            event_type,
            removed = count,
            "Template cache entries invalidated"
        );
    }

    /// Clear the entire template cache.
    pub async fn invalidate_all(&self) {
        self.cache.invalidate_all();
        self.cache.run_pending_tasks().await;
        info!("Template cache cleared");
    }
}
