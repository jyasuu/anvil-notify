//! Machine-readable API documents, embedded at compile time.
//!
//! * `GET /openapi.yaml`  — the HTTP API (OpenAPI 3.1)
//! * `GET /asyncapi.yaml` — the RabbitMQ contract (AsyncAPI 3.0)
//! * `GET /docs`          — browsable Redoc rendering of `/openapi.yaml`
//!
//! Both are served without authentication (see `routes::build_router`).
//!
//! The sources live in the repository's `docs/` directory. Edit them there;
//! `scripts/check_api_docs.py` fails CI when a route and `openapi.yaml` disagree.

use axum::{
    http::header,
    response::{Html, IntoResponse},
    routing::get,
    Router,
};

use crate::state::ApiState;

const OPENAPI: &str = include_str!("../../../docs/openapi.yaml");
const ASYNCAPI: &str = include_str!("../../../docs/asyncapi.yaml");

/// Minimal Redoc host page.
///
/// Redoc is loaded by the *browser* from jsDelivr, pinned to an exact version
/// and verified with a Subresource Integrity hash, so the service itself has
/// no runtime dependency on it and a tampered CDN file is rejected.  Air-gapped
/// deployments can still use `/openapi.yaml` with any local viewer.
/// To bump Redoc: change the version and recompute the hash with
/// `openssl dgst -sha384 -binary redoc.standalone.js | openssl base64 -A`.
const DOCS_HTML: &str = r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>AnvilNotify API</title>
  </head>
  <body style="margin:0">
    <redoc spec-url="/openapi.yaml"></redoc>
    <script
      src="https://cdn.jsdelivr.net/npm/redoc@2.1.5/bundles/redoc.standalone.js"
      integrity="sha384-0GrsyTQc9Oqd8h+b2dbc4XdR2T/DYpy0tLNNstyx+LBMUyiBbcWPbEs9aRmUcaxD"
      crossorigin="anonymous"
    ></script>
  </body>
</html>
"#;

pub fn router() -> Router<ApiState> {
    Router::new()
        .route("/docs", get(docs_page))
        .route("/openapi.yaml", get(openapi))
        .route("/asyncapi.yaml", get(asyncapi))
}

async fn docs_page() -> impl IntoResponse {
    Html(DOCS_HTML)
}

async fn openapi() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "application/yaml")], OPENAPI)
}

async fn asyncapi() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "application/yaml")], ASYNCAPI)
}
