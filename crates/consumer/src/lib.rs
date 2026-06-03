pub mod config;
pub mod delivery;
pub mod processor;
pub mod runner;

#[cfg(test)]
mod tests;

pub use config::ConsumerConfig;
pub use processor::ProcessorContext;
pub use runner::run_consumer;

/// Record initial zero values so all consumer metrics appear immediately
/// in the Prometheus /metrics output, even before any events are processed.
pub fn bootstrap_metrics() {
    // processor.rs
    metrics::counter!("emails_sent_total", "event_type" => "").increment(0);
    metrics::counter!("emails_failed_total", "event_type" => "", "reason" => "").increment(0);
    metrics::counter!("emails_blocked_total", "event_type" => "").increment(0);
    metrics::histogram!("email_send_duration_seconds", "event_type" => "").record(0.0);
    metrics::counter!("email_rate_limit_waits_total", "event_type" => "").increment(0);
    metrics::counter!("email_mark_sent_failed_total", "event_type" => "").increment(0);
    metrics::counter!("email_group_mark_sent_partial_total", "event_type" => "").increment(0);
    // runner.rs
    metrics::counter!("consumer_reconnects_total").increment(0);
}
