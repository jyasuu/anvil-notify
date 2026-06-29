mod config;
mod server;

use anyhow::Context;
use rmcp::{transport::stdio, ServiceExt};
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use config::load;
use server::NotifyServer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with(tracing_subscriber::fmt::layer().compact())
        .init();

    let cfg_path = std::env::var("MCP_CONFIG").ok();
    let cfg = load(cfg_path.as_deref()).context("Failed to load config")?;

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&cfg.database.url)
        .await
        .context("Failed to connect to PostgreSQL")?;

    let server = NotifyServer::new(pool, cfg);
    tracing::info!("Starting anvil-notify MCP server over stdio...");
    server.serve(stdio()).await?.waiting().await?;

    Ok(())
}
