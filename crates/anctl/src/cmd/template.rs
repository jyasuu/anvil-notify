//! `anctl template` — list, show, create, and flush notification templates.
//!
//! * `list` and `show` query `notification_template` via the `store` crate —
//!   they do not require the HTTP API to be running.
//! * `create` calls `POST /templates` — requires a running service.
//! * `flush` calls the HTTP API's DELETE cache endpoints, which do require a
//!   running service.

use anyhow::{bail, Context, Result};
use reqwest::Client;
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use tabled::Tabled;

use store::cli_queries;

use crate::{
    cli::{OutputFormat, TemplateAction, TemplateArgs},
    config::CliConfig,
    output,
};

#[derive(Debug, Serialize, Tabled)]
struct TemplateRow {
    #[tabled(rename = "Type")]
    event_type: String,
    #[tabled(rename = "Channel")]
    channel: String,
    #[tabled(rename = "Subject")]
    subject: String,
    #[tabled(rename = "Version")]
    version: i32,
    #[tabled(rename = "Active")]
    active: bool,
    #[tabled(rename = "Updated")]
    updated_at: String,
}

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

    // Split on section header lines. A header is a line whose trimmed content
    // is exactly one of the three markers.
    let mut current_section: Option<&str> = None;
    let mut current_buf = String::new();

    for line in content.lines() {
        let trimmed = line.trim();
        match trimmed {
            "{# subject #}" | "{# body_html #}" | "{# body_text #}" => {
                // Flush the previous section buffer.
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

    // Flush the final section.
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

pub async fn run(args: TemplateArgs, cfg: CliConfig, fmt: OutputFormat) -> Result<()> {
    match args.action {
        // ── list: read notification_template directly from the DB ─────────────
        TemplateAction::List => {
            let pool = PgPoolOptions::new()
                .max_connections(2)
                .connect(&cfg.database.url)
                .await
                .context("Failed to connect to database")?;

            let rows = cli_queries::list_templates(&pool).await?;

            if rows.is_empty() {
                println!("(no templates in database)");
                return Ok(());
            }

            let display: Vec<TemplateRow> = rows
                .into_iter()
                .map(|r| TemplateRow {
                    event_type: r.event_type,
                    channel: r.channel,
                    subject: output::truncate(&r.subject, 50),
                    version: r.version,
                    active: r.active,
                    updated_at: r.updated_at.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                })
                .collect();

            match fmt {
                OutputFormat::Json => output::print_json(&display),
                OutputFormat::Table => output::print_table(&display),
            }
        }

        // ── show: read one template row from the DB ───────────────────────────
        TemplateAction::Show { event_type } => {
            let pool = PgPoolOptions::new()
                .max_connections(2)
                .connect(&cfg.database.url)
                .await
                .context("Failed to connect to database")?;

            let rows = cli_queries::show_template(&pool, &event_type).await?;

            if rows.is_empty() {
                bail!("No template found for event type '{event_type}'");
            }

            for r in rows {
                println!("Type    : {}", r.event_type);
                println!("Channel : {}", r.channel);
                println!("Version : {}  Active: {}", r.version, r.active);
                println!("Updated : {}", r.updated_at.format("%Y-%m-%d %H:%M:%S UTC"));
                println!();
                println!("Subject :\n{}\n", r.subject);
                println!("HTML body:\n{}\n", r.body_html);
                println!("Text body:\n{}", r.body_text);
                println!("{}", "─".repeat(60));
            }
        }

        // ── flush: call the HTTP API's DELETE cache endpoint ──────────────────
        TemplateAction::Flush { event_type } => {
            let base_url = cfg.api_base_url();
            let client = Client::new();

            let url = match event_type {
                Some(ref et) => format!("{base_url}/templates/{et}/cache"),
                None => format!("{base_url}/templates/cache"),
            };

            let mut req = client.delete(&url);
            if let Some(key) = &cfg.http.api_key {
                req = req.bearer_auth(key);
            }

            let resp = req.send().await.context("HTTP request failed")?;
            let status = resp.status();
            if status.is_success() {
                println!("✓ Template cache flushed (HTTP {status})");
            } else {
                let body = resp.text().await.unwrap_or_default();
                bail!("Flush failed (HTTP {status}): {body}");
            }
        }

        // ── create: parse .j2 file and POST to /templates ─────────────────────
        TemplateAction::Create {
            event_type,
            file,
            channel,
            dry_run,
            yes,
        } => {
            let content = std::fs::read_to_string(&file)
                .with_context(|| format!("Failed to read '{file}'"))?;

            let sections =
                parse_j2(&content).with_context(|| format!("Failed to parse '{file}'"))?;

            // Always show the parsed sections so the operator can verify.
            println!("Event type : {event_type}");
            println!("Channel    : {channel}");
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

            if dry_run {
                println!("(dry-run — not sent)");
                return Ok(());
            }

            if !yes {
                print!(
                    "Upload template '{event_type}' ({channel}) to {}? [y/N] ",
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

            let base_url = cfg.api_base_url();
            let client = Client::new();
            let mut req = client
                .post(format!("{base_url}/templates"))
                .json(&serde_json::json!({
                    "event_type": event_type,
                    "channel":    channel,
                    "subject":    sections.subject,
                    "body_html":  sections.body_html,
                    "body_text":  sections.body_text,
                }));
            if let Some(key) = &cfg.http.api_key {
                req = req.bearer_auth(key);
            }

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
                println!("✓ Template {action} (version {version}, HTTP {status})");
            } else {
                let body = resp.text().await.unwrap_or_default();
                bail!("Create failed (HTTP {status}): {body}");
            }
        }
    }

    Ok(())
}
