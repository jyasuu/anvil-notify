pub mod config;
pub mod worker;

pub use config::OutboxConfig;
pub use worker::run_outbox_worker;

/// Record initial zero values so all outbox metrics appear immediately
/// in the Prometheus /metrics output, even before any events are polled.
pub fn bootstrap_metrics() {
    metrics::counter!("outbox_published_total").increment(0);
    metrics::counter!("outbox_publish_failed_total").increment(0);
}
