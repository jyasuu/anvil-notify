use std::sync::Arc;

use anyhow::Context;
use chrono::Utc;
use common::{
    is_valid_email, AttachmentRef, ChannelOverrides, EmailOptions, FromOverride, Metadata,
    NotificationEvent, Recipient, SendMode,
};
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    serve_server,
    transport::io,
    tool, tool_handler, tool_router, ServerHandler,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use store::{BlockListStore, EmailNotificationStore, NotificationStore, TemplateStore};
use tracing::info;
use uuid::Uuid;

use api::Publisher;

pub mod config;

// ── Tool input types ───────────────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
struct SendEmailInput {
    event_type: String,
    payload: serde_json::Value,
    recipients: Vec<Recipient>,
    #[serde(default)]
    cc: Vec<Recipient>,
    #[serde(default)]
    bcc: Vec<Recipient>,
    from_override: Option<FromOverride>,
    #[serde(default)]
    attachments: Vec<AttachmentRef>,
    sender_account: Option<String>,
    #[serde(default)]
    send_mode: Option<String>,
    event_id: Option<Uuid>,
    source: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct EventIdInput {
    event_id: Uuid,
}

#[derive(Deserialize, JsonSchema)]
struct RecipientStatusInput {
    event_id: Uuid,
    email: String,
}

#[derive(Deserialize, JsonSchema)]
struct EventTypeInput {
    event_type: String,
}

#[derive(Deserialize, JsonSchema)]
struct UpsertTemplateInput {
    event_type: String,
    #[serde(default)]
    channel: String,
    subject: String,
    body_html: String,
    body_text: String,
    #[serde(default = "default_true")]
    active: bool,
}

#[derive(Deserialize, JsonSchema)]
struct PatchTemplateInput {
    event_type: String,
    #[serde(default)]
    channel: String,
    subject: Option<String>,
    body_html: Option<String>,
    body_text: Option<String>,
    active: Option<bool>,
}

#[derive(Deserialize, JsonSchema)]
struct AddBlocklistInput {
    kind: String,
    value: String,
    reason: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
struct RemoveBlocklistInput {
    id: i64,
}

#[derive(Deserialize, JsonSchema)]
struct RetryRecipientInput {
    event_id: Uuid,
    email: String,
}

fn default_true() -> bool {
    true
}

// ── MCP Handler ────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct McpHandler {
    store: Arc<dyn NotificationStore>,
    template_store: TemplateStore,
    block_list_store: BlockListStore,
    publisher: Publisher,
    max_recipients_per_event: usize,
    tool_router: ToolRouter<Self>,
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for McpHandler {}

#[tool_router]
impl McpHandler {
    /// Send an email notification.
    ///
    /// Enqueues a notification event for delivery via the configured email backend.
    /// Returns the event_id for tracking delivery status.
    #[tool(description = "Send an email notification")]
    async fn send_email(&self, params: Parameters<SendEmailInput>) -> String {
        let input = params.0;

        if input.event_type.trim().is_empty() {
            return error_json("event_type must not be empty");
        }
        if input.recipients.is_empty() {
            return error_json("recipients must contain at least one entry");
        }
        if input.recipients.len() > self.max_recipients_per_event {
            return error_json(&format!(
                "recipient count {} exceeds max_recipients_per_event {}",
                input.recipients.len(),
                self.max_recipients_per_event
            ));
        }
        for r in &input.recipients {
            if !is_valid_email(&r.email) {
                return error_json(&format!("invalid recipient email: '{}'", r.email));
            }
        }
        for r in &input.cc {
            if !is_valid_email(&r.email) {
                return error_json(&format!("invalid cc email: '{}'", r.email));
            }
        }
        for r in &input.bcc {
            if !is_valid_email(&r.email) {
                return error_json(&format!("invalid bcc email: '{}'", r.email));
            }
        }
        if let Some(ref ov) = input.from_override {
            if !is_valid_email(&ov.email) {
                return error_json(&format!("invalid from_override email: '{}'", ov.email));
            }
        }

        let send_mode = match input.send_mode.as_deref() {
            Some("group") => SendMode::Group,
            _ => SendMode::Individual,
        };

        let event_id = input.event_id.unwrap_or_else(Uuid::now_v7);
        let event = NotificationEvent {
            event_id,
            timestamp: Utc::now(),
            event_type: input.event_type,
            payload: input.payload,
            metadata: Metadata { source: input.source },
            channel_overrides: ChannelOverrides {
                email: Some(EmailOptions {
                    send_mode,
                    group_retry_mode: common::GroupRetryMode::Whole,
                    retry_policy: common::RetryPolicy::Retry,
                    recipients: input.recipients,
                    cc: input.cc,
                    bcc: input.bcc,
                    from_override: input.from_override,
                    attachments: input.attachments,
                    sender_account: input.sender_account,
                    send_at: None,
                    priority: None,
                }),
            },
        };

        let body = match serde_json::to_vec(&event) {
            Ok(b) => b,
            Err(e) => return error_json(&format!("serialization error: {e}")),
        };

        if let Err(e) = self.publisher.publish(body).await {
            return error_json(&format!("publish failed: {e}"));
        }

        json!({
            "status": "accepted",
            "eventId": event_id.to_string(),
        })
        .to_string()
    }

    /// Get delivery status for all recipients of an event.
    #[tool(description = "Get delivery status for all recipients of an event")]
    async fn get_status(&self, params: Parameters<EventIdInput>) -> String {
        let input = params.0;
        match self.store.get_by_event_id(input.event_id).await {
            Ok(logs) => {
                let mut sent = 0u32;
                let mut blocked = 0u32;
                let mut failed = 0u32;
                let mut pending = 0u32;
                let mut skipped = 0u32;

                let recipients: Vec<_> = logs
                    .iter()
                    .map(|log| {
                        match log.status {
                            common::EmailStatus::Sent => sent += 1,
                            common::EmailStatus::Blocked => blocked += 1,
                            common::EmailStatus::Failed => failed += 1,
                            common::EmailStatus::Pending => pending += 1,
                            common::EmailStatus::Skipped => skipped += 1,
                        }
                        json!({
                            "email": log.recipient_email,
                            "status": log.status.as_str(),
                            "retryCount": log.retry_count,
                            "totalAttempts": log.total_attempts,
                            "lastError": log.last_error,
                            "createdAt": log.created_at,
                            "updatedAt": log.updated_at,
                        })
                    })
                    .collect();

                json!({
                    "eventId": input.event_id,
                    "recipients": recipients,
                    "summary": {
                        "total": recipients.len(),
                        "sent": sent,
                        "blocked": blocked,
                        "failed": failed,
                        "pending": pending,
                        "skipped": skipped,
                    }
                })
                .to_string()
            }
            Err(e) => error_json(&e.to_string()),
        }
    }

    /// Get delivery status for a single recipient within an event.
    #[tool(description = "Get delivery status for a single recipient within an event")]
    async fn get_recipient_status(
        &self,
        params: Parameters<RecipientStatusInput>,
    ) -> String {
        let input = params.0;
        match self
            .store
            .get_by_event_and_recipient(input.event_id, &input.email)
            .await
        {
            Ok(log) => {
                json!({
                    "eventId": log.event_id,
                    "email": log.recipient_email,
                    "status": log.status.as_str(),
                    "retryCount": log.retry_count,
                    "totalAttempts": log.total_attempts,
                    "lastError": log.last_error,
                    "createdAt": log.created_at,
                    "updatedAt": log.updated_at,
                })
                .to_string()
            }
            Err(e) => error_json(&e.to_string()),
        }
    }

    /// List all notification templates.
    #[tool(description = "List all notification templates")]
    async fn list_templates(&self) -> String {
        match self.template_store.list().await {
            Ok(rows) => {
                let templates: Vec<_> = rows
                    .iter()
                    .map(|r| {
                        json!({
                            "event_type": r.event_type,
                            "channel": r.channel,
                            "subject": r.subject,
                            "version": r.version,
                            "active": r.active,
                            "updated_at": r.updated_at,
                        })
                    })
                    .collect();
                json!({ "templates": templates }).to_string()
            }
            Err(e) => error_json(&e.to_string()),
        }
    }

    /// Get a template by event type.
    #[tool(description = "Get a template by event type")]
    async fn get_template(&self, params: Parameters<EventTypeInput>) -> String {
        let input = params.0;
        match self.template_store.get(&input.event_type).await {
            Ok(rows) if rows.is_empty() => {
                error_json(&format!("No template found for event type '{}'", input.event_type))
            }
            Ok(rows) => {
                let templates: Vec<_> = rows
                    .iter()
                    .map(|r| {
                        json!({
                            "event_type": r.event_type,
                            "channel": r.channel,
                            "subject": r.subject,
                            "body_html": r.body_html,
                            "body_text": r.body_text,
                            "version": r.version,
                            "active": r.active,
                            "updated_at": r.updated_at,
                        })
                    })
                    .collect();
                json!({ "templates": templates }).to_string()
            }
            Err(e) => error_json(&e.to_string()),
        }
    }

    /// Create or update a template.
    #[tool(description = "Create or update a notification template")]
    async fn upsert_template(&self, params: Parameters<UpsertTemplateInput>) -> String {
        let input = params.0;

        if input.event_type.trim().is_empty() {
            return error_json("event_type must not be empty");
        }
        if input.subject.trim().is_empty() {
            return error_json("subject must not be empty");
        }

        match self
            .template_store
            .upsert(
                &input.event_type,
                &if input.channel.is_empty() { "email" } else { &input.channel },
                &input.subject,
                &input.body_html,
                &input.body_text,
                input.active,
            )
            .await
        {
            Ok((version, inserted)) => json!({
                "event_type": input.event_type,
                "channel": if input.channel.is_empty() { "email" } else { &input.channel },
                "version": version,
                "active": input.active,
                "inserted": inserted,
            })
            .to_string(),
            Err(e) => error_json(&e.to_string()),
        }
    }

    /// Partially update a template.
    #[tool(description = "Partially update a notification template")]
    async fn patch_template(&self, params: Parameters<PatchTemplateInput>) -> String {
        let input = params.0;

        match self
            .template_store
            .patch(
                &input.event_type,
                &if input.channel.is_empty() { "email" } else { &input.channel },
                input.subject.as_deref(),
                input.body_html.as_deref(),
                input.body_text.as_deref(),
                input.active,
            )
            .await
        {
            Ok(Some((version, active))) => json!({
                "event_type": input.event_type,
                "channel": if input.channel.is_empty() { "email" } else { &input.channel },
                "version": version,
                "active": active,
            })
            .to_string(),
            Ok(None) => error_json(&format!(
                "No template found for event type '{}' channel '{}'",
                input.event_type,
                if input.channel.is_empty() { "email" } else { &input.channel }
            )),
            Err(e) => error_json(&e.to_string()),
        }
    }

    /// List all blocklist entries.
    #[tool(description = "List all blocklist and allowlist entries")]
    async fn list_blocklist(&self) -> String {
        match self.block_list_store.list_entries().await {
            Ok(entries) => {
                let body: Vec<_> = entries
                    .iter()
                    .map(|e| {
                        json!({
                            "id": e.id,
                            "kind": e.kind,
                            "value": e.value,
                            "reason": e.reason,
                            "createdAt": e.created_at,
                        })
                    })
                    .collect();
                json!({ "entries": body }).to_string()
            }
            Err(e) => error_json(&e.to_string()),
        }
    }

    /// Add an entry to the blocklist or allowlist.
    #[tool(description = "Add an entry to the blocklist or allowlist")]
    async fn add_blocklist_entry(
        &self,
        params: Parameters<AddBlocklistInput>,
    ) -> String {
        let input = params.0;

        let valid_kinds = [
            "blocked_email",
            "blocked_domain",
            "allowed_email",
            "allowed_domain",
        ];
        if !valid_kinds.contains(&input.kind.as_str()) {
            return error_json(&format!(
                "invalid kind '{}' — must be one of: {}",
                input.kind,
                valid_kinds.join(", ")
            ));
        }

        match self
            .block_list_store
            .add_entry(&input.kind, &input.value, input.reason.as_deref())
            .await
        {
            Ok(entry) => json!({
                "id": entry.id,
                "kind": entry.kind,
                "value": entry.value,
                "reason": entry.reason,
                "createdAt": entry.created_at,
            })
            .to_string(),
            Err(e) => error_json(&e.to_string()),
        }
    }

    /// Remove an entry from the blocklist by id.
    #[tool(description = "Remove an entry from the blocklist by id")]
    async fn remove_blocklist_entry(
        &self,
        params: Parameters<RemoveBlocklistInput>,
    ) -> String {
        let input = params.0;
        match self.block_list_store.remove_entry(input.id).await {
            Ok(()) => json!({ "status": "deleted", "id": input.id }).to_string(),
            Err(e) => error_json(&e.to_string()),
        }
    }

    /// Retry delivery for a single failed recipient.
    #[tool(description = "Retry delivery for a single failed recipient within an event")]
    async fn retry_recipient(
        &self,
        params: Parameters<RetryRecipientInput>,
    ) -> String {
        let input = params.0;

        if !is_valid_email(&input.email) {
            return error_json(&format!("'{}' is not a valid email address", input.email));
        }

        if let Err(e) = self
            .store
            .reset_for_retry(input.event_id, &input.email)
            .await
        {
            return error_json(&e.to_string());
        }

        self.republish_event(input.event_id, Some(vec![input.email.clone()]))
            .await
    }

    /// Retry delivery for all failed recipients in an event.
    #[tool(description = "Retry delivery for all failed recipients in an event")]
    async fn retry_event(&self, params: Parameters<EventIdInput>) -> String {
        let input = params.0;

        match self
            .store
            .reset_all_failed_for_event(input.event_id)
            .await
        {
            Ok(reset) if reset.is_empty() => {
                error_json(&format!("No FAILED recipients for event {}", input.event_id))
            }
            Ok(reset) => self.republish_event(input.event_id, Some(reset)).await,
            Err(e) => error_json(&e.to_string()),
        }
    }
}

// ── Re-publish logic (shared by retry tools) ──────────────────────────────────

impl McpHandler {
    async fn republish_event(&self, event_id: Uuid, only_emails: Option<Vec<String>>) -> String {
        let logs = match self
            .store
            .get_recipients_for_event(event_id, only_emails.as_deref())
            .await
        {
            Ok(l) => l,
            Err(e) => return error_json(&e.to_string()),
        };

        let recipients: Vec<Recipient> = logs
            .iter()
            .filter(|l| l.status != common::EmailStatus::Skipped)
            .map(|l| Recipient {
                email: l.recipient_email.clone(),
                name: l.recipient_name.clone(),
            })
            .collect();

        if recipients.is_empty() {
            return error_json("No deliverable recipients to retry");
        }

        let detail = match self.store.get_event_delivery_detail(event_id).await {
            Ok(d) => d,
            Err(e) => return error_json(&e.to_string()),
        };

        let from_override: Option<FromOverride> = detail
            .from_override
            .and_then(|v| serde_json::from_value(v).ok());

        let attachments: Vec<AttachmentRef> = detail
            .attachments
            .map(|v| serde_json::from_value(v).unwrap_or_default())
            .unwrap_or_default();

        let cc: Vec<Recipient> = detail
            .cc
            .map(|v| serde_json::from_value(v).unwrap_or_default())
            .unwrap_or_default();

        let bcc: Vec<Recipient> = detail
            .bcc
            .map(|v| serde_json::from_value(v).unwrap_or_default())
            .unwrap_or_default();

        if detail.payload.is_null() {
            return error_json("Stored payload is null — repair the notification_log row first");
        }

        let event = NotificationEvent {
            event_id,
            timestamp: detail.event_timestamp,
            event_type: detail.event_type,
            payload: detail.payload,
            metadata: Metadata { source: None },
            channel_overrides: ChannelOverrides {
                email: Some(EmailOptions {
                    send_mode: SendMode::Individual,
                    group_retry_mode: common::GroupRetryMode::Whole,
                    retry_policy: common::RetryPolicy::Retry,
                    recipients,
                    cc,
                    bcc,
                    from_override,
                    attachments,
                    sender_account: detail.sender_account,
                    send_at: None,
                    priority: None,
                }),
            },
        };

        let body = match serde_json::to_vec(&event) {
            Ok(b) => b,
            Err(e) => return error_json(&format!("serialization error: {e}")),
        };

        if let Err(e) = self.publisher.publish(body).await {
            return error_json(&format!("publish failed: {e}"));
        }

        json!({
            "status": "accepted",
            "eventId": event_id.to_string(),
        })
        .to_string()
    }
}

fn error_json(msg: &str) -> String {
    json!({ "error": msg }).to_string()
}

// ── Stdio server (standalone binary) ──────────────────────────────────────────

pub async fn serve_stdio() -> anyhow::Result<()> {
    let cfg = config::McpConfig::load().context("Failed to load config")?;
    info!("Config loaded");

    let pool = PgPoolOptions::new()
        .max_connections(cfg.database.pool_size)
        .connect(&cfg.database.url)
        .await
        .context("Failed to connect to PostgreSQL")?;

    let store = Arc::new(EmailNotificationStore::new(pool.clone())) as Arc<dyn NotificationStore>;
    let template_store = TemplateStore::new_with_ttl(
        pool.clone(),
        std::time::Duration::from_secs(cfg.template_cache_ttl_secs),
    );
    let block_list_store = BlockListStore::new(
        pool.clone(),
        std::time::Duration::from_secs(cfg.block_list_cache_ttl_secs),
    );

    let publisher = Publisher::connect(&cfg.amqp.url, &cfg.amqp.exchange, &cfg.amqp.routing_key)
        .await
        .context("Failed to create AMQP publisher")?;
    info!("Publisher connected to RabbitMQ");

    let handler = McpHandler {
        store,
        template_store,
        block_list_store,
        publisher,
        max_recipients_per_event: cfg.amqp.max_recipients_per_event,
        tool_router: McpHandler::tool_router(),
    };

    info!("Starting MCP server (stdio transport)");
    serve_server(handler, io::stdio()).await?;

    Ok(())
}

// ── HTTP MCP route (integrated into anvil-notify) ────────────────────────────

use std::sync::Arc as StdArc;
use std::time::Duration;

use axum::{Router, http::HeaderValue, middleware};
use rmcp::transport::streamable_http_server::{
    StreamableHttpService, StreamableHttpServerConfig,
    session::local::LocalSessionManager,
};

/// Axum middleware that ensures the `Accept` header includes BOTH
/// `application/json` and `text/event-stream` as required by rmcp's
/// Streamable HTTP handler. Overwrites whatever the client sent.
async fn ensure_mcp_accept(
    mut req: axum::http::Request<axum::body::Body>,
    next: middleware::Next,
) -> axum::response::Response {
    tracing::debug!(
        method = %req.method(),
        uri = %req.uri(),
        accept = ?req.headers().get(axum::http::header::ACCEPT),
        content_type = ?req.headers().get(axum::http::header::CONTENT_TYPE),
        session_id = ?req.headers().get("mcp-session-id"),
        "MCP request"
    );
    req.headers_mut().insert(
        axum::http::header::ACCEPT,
        HeaderValue::from_static("application/json, text/event-stream"),
    );
    next.run(req).await
}

/// Build an Axum `Router` with the MCP Streamable HTTP endpoint mounted at `/mcp`.
///
/// Merge this router into the main anvil-notify API router via `Router::merge()`.
pub fn build_mcp_route(
    store: Arc<dyn NotificationStore>,
    template_store: TemplateStore,
    block_list_store: BlockListStore,
    publisher: Publisher,
    max_recipients_per_event: usize,
    mcp_path: &str,
) -> Router {
    let handler = McpHandler {
        store,
        template_store,
        block_list_store,
        publisher,
        max_recipients_per_event,
        tool_router: McpHandler::tool_router(),
    };

    let config = StreamableHttpServerConfig {
        sse_keep_alive: Some(Duration::from_secs(15)),
        ..Default::default()
    };

    let session_manager = StdArc::new(LocalSessionManager::default());

    let service = StreamableHttpService::new(
        move || Ok(handler.clone()),
        session_manager,
        config,
    );

    Router::new()
        .nest_service(mcp_path, service)
        .layer(middleware::from_fn(ensure_mcp_accept))
}
