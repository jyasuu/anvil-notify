pub mod email_log;
pub mod notification_log;
pub mod template_store;

// Legacy — still used during the transition period (Phases 3-4).
// Will be removed when email_log is dropped (Phase 5).
pub use email_log::{EmailLogStore, InsertPendingArgs, InsertResult};

// New multi-channel store.
pub use notification_log::{
    EmailNotificationStore, NotificationStore, CHANNEL_EMAIL,
};
pub use template_store::{EmailTemplate, TemplateStore};