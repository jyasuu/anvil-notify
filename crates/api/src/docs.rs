//! Machine-readable API documents, embedded at compile time.
//!
//! * `GET /openapi.yaml`  — the HTTP API (OpenAPI 3.1)
//! * `GET /asyncapi.yaml` — the RabbitMQ contract (AsyncAPI 3.0)
//!
//! The sources live in the repository's `docs/` directory. Edit them there;
//! `scripts/check_api_docs.py` fails CI when a route and `openapi.yaml` disagree.

use axum::{http::header, response::IntoResponse, routing::get, Router};

use crate::state::ApiState;

const OPENAPI: &str = include_str!("../../../docs/openapi.yaml");
const ASYNCAPI: &str = include_str!("../../../docs/asyncapi.yaml");

pub fn router() -> Router<ApiState> {
    Router::new()
        .route("/openapi.yaml", get(openapi))
        .route("/asyncapi.yaml", get(asyncapi))
}

async fn openapi() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "application/yaml")], OPENAPI)
}

async fn asyncapi() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "application/yaml")], ASYNCAPI)
}
