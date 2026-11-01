use async_trait::async_trait;
use common::AppError;

use crate::EmailMessage;

/// Abstraction over email transport backends.
///
/// Both `SmtpSender` and `WebhookSender` implement this trait,
/// so the consumer is backend-agnostic.
// clippy >= 1.9x flags `double_must_use` on every method of an `#[async_trait]`
// trait (the boxed future is already `#[must_use]`).  The attribute is added
// by the macro expansion, not written here, so allow the lint on the trait.
#[async_trait]
#[allow(clippy::double_must_use)]
pub trait EmailSender: Send + Sync {
    async fn send(&self, msg: &EmailMessage) -> Result<(), AppError>;
}
