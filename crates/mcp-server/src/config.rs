use recipient_filter::FilterConfig;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct McpConfig {
    pub database: DatabaseConfig,
    pub amqp: AmqpConfig,
    /// How long resolved templates are cached in memory (seconds).
    #[serde(default = "default_template_cache_ttl_secs")]
    pub template_cache_ttl_secs: u64,
    /// How long the DB-backed block/allow-list snapshot is cached in memory (seconds).
    #[serde(default = "default_block_list_cache_ttl_secs")]
    pub block_list_cache_ttl_secs: u64,
    /// Recipient block/allow-list.
    #[serde(default)]
    pub filter: FilterConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub url: String,
    #[serde(default = "default_db_pool_size")]
    pub pool_size: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AmqpConfig {
    pub url: String,
    pub exchange: String,
    pub routing_key: String,
    #[serde(default = "default_max_recipients_per_event")]
    pub max_recipients_per_event: usize,
}

fn default_db_pool_size() -> u32 {
    5
}

fn default_template_cache_ttl_secs() -> u64 {
    300
}

fn default_block_list_cache_ttl_secs() -> u64 {
    30
}

fn default_max_recipients_per_event() -> usize {
    500
}

impl McpConfig {
    pub fn load() -> anyhow::Result<Self> {
        let cfg = config::Config::builder()
            .add_source(config::File::with_name("config/default").required(false))
            .add_source(config::File::with_name("config/local").required(false))
            .add_source(config::Environment::with_prefix("AN").separator("__"))
            .build()?;

        Ok(cfg.try_deserialize()?)
    }
}
