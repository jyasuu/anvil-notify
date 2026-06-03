mod errors;
mod handlers;
mod publisher;
mod routes;
mod state;

pub use publisher::Publisher;
pub use routes::build_router;
pub use state::ApiState;

/// Record initial zero values so all API metrics appear immediately
/// in the Prometheus /metrics output, even before any requests arrive.
pub fn bootstrap_metrics() {
    metrics::counter!("api_send_email_total").increment(0);
    // retry_publish_failed_total has a dynamic `event_id` label — we register
    // it with an empty value so the metric name is always present.
    metrics::counter!("retry_publish_failed_total", "event_id" => "").increment(0);
}
