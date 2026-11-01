//! Tests for the embedded API documents (`/openapi.yaml`, `/asyncapi.yaml`).
//!
//! Gated like the other integration tests, but none of these requests touch the
//! database: the pool is lazy and never connects.
//!
//! ```bash
//! cargo test -p api --features integration --test docs_api
//! ```

#![cfg(feature = "integration")]

use std::{sync::Arc, time::Duration};

use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

use api::{build_router, ApiState, Publisher};
use recipient_filter::{FilterConfig, RecipientFilter};
use store::{BlockListStore, EmailNotificationStore, TemplateStore};

fn state(api_key: Option<&str>) -> ApiState {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@localhost:5432/unused")
        .expect("lazy pool");
    ApiState {
        store: Arc::new(EmailNotificationStore::new(pool.clone())),
        template_store: TemplateStore::new(pool.clone()),
        block_list_store: BlockListStore::new(pool, Duration::ZERO),
        publisher: Publisher::disconnected(),
        api_key: api_key.map(str::to_owned),
        filter: RecipientFilter::new(FilterConfig::default()),
        max_recipients_per_event: 50,
    }
}

async fn get(state: ApiState, path: &str, bearer: Option<&str>) -> (StatusCode, String, String) {
    let mut req = Request::builder().uri(path);
    if let Some(t) = bearer {
        req = req.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    let resp = build_router(state)
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let ctype = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    (status, ctype, String::from_utf8(bytes.to_vec()).unwrap())
}

#[tokio::test]
async fn openapi_document_is_served() {
    let (status, ctype, body) = get(state(None), "/openapi.yaml", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ctype, "application/yaml");
    assert!(
        body.starts_with("openapi: 3.1"),
        "unexpected body: {body:.60}"
    );
}

#[tokio::test]
async fn asyncapi_document_is_served() {
    let (status, ctype, body) = get(state(None), "/asyncapi.yaml", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ctype, "application/yaml");
    assert!(
        body.starts_with("asyncapi: 3.0"),
        "unexpected body: {body:.60}"
    );
}

#[tokio::test]
async fn documents_require_the_api_key_when_one_is_configured() {
    for path in ["/openapi.yaml", "/asyncapi.yaml"] {
        let s = state(Some("secret"));
        assert_eq!(get(s.clone(), path, None).await.0, StatusCode::UNAUTHORIZED);
        assert_eq!(
            get(s.clone(), path, Some("wrong")).await.0,
            StatusCode::FORBIDDEN
        );
        assert_eq!(get(s, path, Some("secret")).await.0, StatusCode::OK);
    }
}
