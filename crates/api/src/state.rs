use std::sync::Arc;

use recipient_filter::RecipientFilter;
use store::{BlockListStore, NotificationStore, TemplateStore};

use crate::publisher::Publisher;

#[derive(Clone)]
pub struct ApiState {
    pub store:            Arc<dyn NotificationStore>,
    pub template_store:   TemplateStore,
    /// DB-backed block/allow-list store. Used by admin endpoints and retry
    /// validation; the consumer uses `RecipientFilter` which merges config-file
    /// and DB entries at startup and is refreshed per-check via BlockListStore.
    pub block_list_store: BlockListStore,
    /// Used by retry endpoints to re-enqueue events after resetting DB rows.
    pub publisher:        Publisher,
    /// When `Some`, every request must supply `Authorization: Bearer <token>`.
    pub api_key:          Option<String>,
    /// Static recipient filter (config-file entries). The BlockListStore covers
    /// the DB-backed entries.
    pub filter:           RecipientFilter,
}
