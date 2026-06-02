//! `anctl template` — list, show, create, and flush notification templates.
//!
//! * `list` and `show` call `GET /templates` and `GET /templates/{event_type}`
//!   respectively — they require a running service but no direct DB access.
//! * `create` calls `POST /templates` — requires a running service.
//! * `flush` calls the HTTP API's DELETE cache endpoints.
//!
//! All HTTP subcommands require the service to be reachable at the URL
//! configured in `[http] base_url` (or `ANVIL_HTTP_BASE_URL`).

use anyhow::{bail, Context, Result};
use minijinja::{Environment, UndefinedBehavior};
use reqwest::Client;
use serde::Serialize;
use tabled::Tabled;

use crate::{
    cli::{OutputFormat, TemplateAction, TemplateArgs},
    config::CliConfig,
    output,
};

// ── display types ─────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Tabled)]
struct TemplateListRow {
    #[tabled(rename = "Type")]
    event_type: String,
    #[tabled(rename = "Channel")]
    channel: String,
    #[tabled(rename = "Subject")]
    subject: String,
    #[tabled(rename = "Ver")]
    version: i64,
    #[tabled(rename = "Active")]
    active: bool,
    #[tabled(rename = "Updated")]
    updated_at: String,
}

// ── .j2 parsing ───────────────────────────────────────────────────────────────

/// Parsed sections from a `.j2` template file.
struct J2Sections {
    subject: String,
    body_html: String,
    body_text: String,
}

/// Parse a `.j2` file into its three named sections.
///
/// Sections are delimited by Jinja2 comment markers on their own line:
/// `{# subject #}`, `{# body_html #}`, `{# body_text #}`.
/// Sections may appear in any order. Leading/trailing whitespace is trimmed.
fn parse_j2(content: &str) -> Result<J2Sections> {
    let mut subject: Option<String> = None;
    let mut body_html: Option<String> = None;
    let mut body_text: Option<String> = None;

    let mut current_section: Option<&str> = None;
    let mut current_buf = String::new();

    for line in content.lines() {
        let trimmed = line.trim();
        match trimmed {
            "{# subject #}" | "{# body_html #}" | "{# body_text #}" => {
                if let Some(sec) = current_section {
                    store_section(
                        sec,
                        current_buf.trim().to_owned(),
                        &mut subject,
                        &mut body_html,
                        &mut body_text,
                    )?;
                }
                current_section = Some(trimmed);
                current_buf = String::new();
            }
            _ => {
                if current_section.is_some() {
                    if !current_buf.is_empty() {
                        current_buf.push('\n');
                    }
                    current_buf.push_str(line);
                }
            }
        }
    }

    if let Some(sec) = current_section {
        store_section(
            sec,
            current_buf.trim().to_owned(),
            &mut subject,
            &mut body_html,
            &mut body_text,
        )?;
    }

    Ok(J2Sections {
        subject: subject.ok_or_else(|| anyhow::anyhow!("missing {{# subject #}} section"))?,
        body_html: body_html.ok_or_else(|| anyhow::anyhow!("missing {{# body_html #}} section"))?,
        body_text: body_text.ok_or_else(|| anyhow::anyhow!("missing {{# body_text #}} section"))?,
    })
}

fn store_section(
    marker: &str,
    content: String,
    subject: &mut Option<String>,
    body_html: &mut Option<String>,
    body_text: &mut Option<String>,
) -> Result<()> {
    match marker {
        "{# subject #}" => {
            if subject.is_some() {
                bail!("duplicate {{# subject #}} section");
            }
            *subject = Some(content);
        }
        "{# body_html #}" => {
            if body_html.is_some() {
                bail!("duplicate {{# body_html #}} section");
            }
            *body_html = Some(content);
        }
        "{# body_text #}" => {
            if body_text.is_some() {
                bail!("duplicate {{# body_text #}} section");
            }
            *body_text = Some(content);
        }
        _ => unreachable!(),
    }
    Ok(())
}

// ── Jinja2 syntax validation ──────────────────────────────────────────────────

/// Validate that each section is syntactically valid Jinja2.
///
/// Uses `UndefinedBehavior::Lenient` so that unknown variables (which we
/// cannot know at upload time) are silently ignored.  Only genuine parse
/// errors — unclosed tags, malformed expressions, etc. — are reported.
///
/// Returns a list of human-readable error strings, one per failing section.
/// An empty vec means all sections parsed successfully.
fn validate_j2_syntax(sections: &J2Sections) -> Vec<String> {
    let mut env = Environment::new();
    env.set_undefined_behavior(UndefinedBehavior::Lenient);

    let checks = [
        ("subject", sections.subject.as_str()),
        ("body_html", sections.body_html.as_str()),
        ("body_text", sections.body_text.as_str()),
    ];

    checks
        .iter()
        .filter_map(|(name, src)| {
            env.add_template_owned(name.to_string(), src.to_string())
                .err()
                .map(|e| format!("{name}: {e}"))
        })
        .collect()
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────

fn build_client(cfg: &CliConfig) -> (Client, String) {
    let client = Client::new();
    let base = cfg.api_base_url();
    (client, base)
}

fn maybe_auth(req: reqwest::RequestBuilder, cfg: &CliConfig) -> reqwest::RequestBuilder {
    if let Some(key) = &cfg.http.api_key {
        req.bearer_auth(key)
    } else {
        req
    }
}

// ── subcommand dispatch ───────────────────────────────────────────────────────

pub async fn run(args: TemplateArgs, cfg: CliConfig, fmt: OutputFormat) -> Result<()> {
    match args.action {
        // ── list: GET /templates ───────────────────────────────────────────
        TemplateAction::List => {
            let (client, base) = build_client(&cfg);
            let resp = maybe_auth(client.get(format!("{base}/templates")), &cfg)
                .send()
                .await
                .context("HTTP request failed")?;

            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                bail!("List failed (HTTP {status}): {body}");
            }

            let json: serde_json::Value = resp.json().await.context("Invalid JSON response")?;
            let templates = json
                .get("templates")
                .and_then(|v| v.as_array())
                .ok_or_else(|| anyhow::anyhow!("Unexpected response shape"))?;

            let rows: Vec<TemplateListRow> = templates
                .iter()
                .filter_map(|t| {
                    Some(TemplateListRow {
                        event_type: t.get("event_type")?.as_str()?.to_owned(),
                        channel: t.get("channel")?.as_str()?.to_owned(),
                        subject: t.get("subject")?.as_str()?.to_owned(),
                        version: t.get("version")?.as_i64()?,
                        active: t.get("active")?.as_bool()?,
                        updated_at: t.get("updated_at")?.as_str()?.to_owned(),
                    })
                })
                .collect();

            match fmt {
                OutputFormat::Json => output::print_json(&rows),
                OutputFormat::Table => output::print_table(&rows),
            }
        }

        // ── show: GET /templates/{event_type} ──────────────────────────────
        TemplateAction::Show { event_type } => {
            let (client, base) = build_client(&cfg);
            let resp = maybe_auth(client.get(format!("{base}/templates/{event_type}")), &cfg)
                .send()
                .await
                .context("HTTP request failed")?;

            let status = resp.status();
            if status == reqwest::StatusCode::NOT_FOUND {
                bail!("No template found for event type '{event_type}'");
            }
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                bail!("Show failed (HTTP {status}): {body}");
            }

            let json: serde_json::Value = resp.json().await.context("Invalid JSON response")?;
            let templates = json
                .get("templates")
                .and_then(|v| v.as_array())
                .ok_or_else(|| anyhow::anyhow!("Unexpected response shape"))?;

            for t in templates {
                let channel = t.get("channel").and_then(|v| v.as_str()).unwrap_or("?");
                let version = t.get("version").and_then(|v| v.as_i64()).unwrap_or(0);
                let active = t.get("active").and_then(|v| v.as_bool()).unwrap_or(false);
                let subject = t.get("subject").and_then(|v| v.as_str()).unwrap_or("");
                let html = t.get("body_html").and_then(|v| v.as_str()).unwrap_or("");
                let text = t.get("body_text").and_then(|v| v.as_str()).unwrap_or("");

                println!("── {event_type} / {channel}  (v{version}, active={active}) ──");
                println!("Subject:\n{subject}\n");
                println!("HTML body:\n{html}\n");
                println!("Text body:\n{text}");
                println!("{}", "─".repeat(60));
            }
        }

        // ── create: parse .j2, validate, POST /templates ───────────────────
        TemplateAction::Create {
            event_type,
            file,
            channel,
            dry_run,
            yes,
            inactive,
        } => {
            let content = std::fs::read_to_string(&file)
                .with_context(|| format!("Failed to read '{file}'"))?;

            let sections =
                parse_j2(&content).with_context(|| format!("Failed to parse '{file}'"))?;

            // Always show the parsed sections so the operator can verify.
            println!("Event type : {event_type}");
            println!("Channel    : {channel}");
            println!("Active     : {}", !inactive);
            println!("File       : {file}");
            println!();
            println!("Subject :");
            println!("{}", sections.subject);
            println!();
            println!("HTML body :");
            println!("{}", sections.body_html);
            println!();
            println!("Text body :");
            println!("{}", sections.body_text);
            println!("{}", "─".repeat(60));

            // Validate Jinja2 syntax against all three sections.
            let errors = validate_j2_syntax(&sections);
            if !errors.is_empty() {
                eprintln!("✗ Jinja2 syntax errors found:");
                for e in &errors {
                    eprintln!("  • {e}");
                }
                bail!("Template has syntax errors — aborting");
            }
            println!("✓ Jinja2 syntax OK");

            if dry_run {
                println!("(dry-run — not sent)");
                return Ok(());
            }

            if !yes {
                let active_label = if inactive { "inactive" } else { "active" };
                print!(
                    "Upload template '{event_type}' ({channel}, {active_label}) to {}? [y/N] ",
                    cfg.api_base_url()
                );
                use std::io::Write;
                std::io::stdout().flush().ok();
                let mut input = String::new();
                std::io::stdin().read_line(&mut input)?;
                if !matches!(input.trim().to_lowercase().as_str(), "y" | "yes") {
                    println!("Aborted.");
                    return Ok(());
                }
            }

            let (client, base) = build_client(&cfg);
            let req = maybe_auth(
                client
                    .post(format!("{base}/templates"))
                    .json(&serde_json::json!({
                        "event_type": event_type,
                        "channel":    channel,
                        "subject":    sections.subject,
                        "body_html":  sections.body_html,
                        "body_text":  sections.body_text,
                        "active":     !inactive,
                    })),
                &cfg,
            );

            let resp = req.send().await.context("HTTP request failed")?;
            let status = resp.status();
            if status.is_success() {
                let body: serde_json::Value = resp.json().await.unwrap_or_default();
                let version = body.get("version").and_then(|v| v.as_i64()).unwrap_or(0);
                let action = if body
                    .get("inserted")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    "created"
                } else {
                    "updated"
                };
                let active_label = if inactive { "inactive" } else { "active" };
                println!("✓ Template {action} (v{version}, {active_label}, HTTP {status})");
            } else {
                let body = resp.text().await.unwrap_or_default();
                bail!("Create failed (HTTP {status}): {body}");
            }
        }

        // ── activate: PATCH /templates/{event_type} ────────────────────────
        TemplateAction::Activate {
            event_type,
            channel,
            disable,
        } => {
            let active = !disable;
            let (client, base) = build_client(&cfg);
            let req = maybe_auth(
                client
                    .patch(format!("{base}/templates/{event_type}"))
                    .json(&serde_json::json!({
                        "channel": channel,
                        "active":  active,
                    })),
                &cfg,
            );

            let resp = req.send().await.context("HTTP request failed")?;
            let status = resp.status();
            if status == reqwest::StatusCode::NOT_FOUND {
                bail!(
                    "No template found for '{event_type}' ({channel}). \
                     Create it first with `anctl template create`."
                );
            }
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                bail!("Activate failed (HTTP {status}): {body}");
            }

            let body: serde_json::Value = resp.json().await.unwrap_or_default();
            let version = body.get("version").and_then(|v| v.as_i64()).unwrap_or(0);
            let label = if active { "activated" } else { "deactivated" };
            println!("✓ Template {label} (v{version}, HTTP {status})");
        }

        // ── flush: DELETE /template-cache[/{event_type}] ──────────────────
        TemplateAction::Flush { event_type } => {
            let (client, base) = build_client(&cfg);
            let url = match &event_type {
                Some(et) => format!("{base}/template-cache/{et}"),
                None => format!("{base}/template-cache"),
            };

            let resp = maybe_auth(client.delete(&url), &cfg)
                .send()
                .await
                .context("HTTP request failed")?;

            let status = resp.status();
            if status.is_success() {
                println!("✓ Template cache flushed (HTTP {status})");
            } else {
                let body = resp.text().await.unwrap_or_default();
                bail!("Flush failed (HTTP {status}): {body}");
            }
        }
    }

    Ok(())
}
