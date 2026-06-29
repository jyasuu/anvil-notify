use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct McpConfig {
    pub database: DatabaseConfig,
    pub amqp: AmqpConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub url: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AmqpConfig {
    pub url: String,
    pub exchange: String,
    pub routing_key: String,
}

impl McpConfig {
    #[allow(dead_code)]
    pub fn new(
        database_url: String,
        amqp_url: String,
        exchange: String,
        routing_key: String,
    ) -> Self {
        Self {
            database: DatabaseConfig { url: database_url },
            amqp: AmqpConfig {
                url: amqp_url,
                exchange,
                routing_key,
            },
        }
    }
}

pub fn load(path: Option<&str>) -> Result<McpConfig> {
    let mut builder = config::Config::builder()
        .set_default("amqp.exchange", "anvil-notify")?
        .set_default("amqp.routing_key", "email.requested")?;

    if let Some(p) = path {
        builder = builder.add_source(config::File::with_name(p).required(true));
    } else {
        builder = builder
            .add_source(config::File::with_name("config/default").required(false))
            .add_source(config::File::with_name("config/local").required(false));
    }

    let cfg = builder
        .add_source(config::Environment::with_prefix("AN").separator("__"))
        .build()
        .context("Failed to load config")?
        .try_deserialize::<McpConfig>()
        .context("Failed to parse config")?;

    Ok(cfg)
}
