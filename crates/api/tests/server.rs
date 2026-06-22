use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use clap::Parser;
use lumen_api::{router, AppState, Config};
use lumen_core::Catalog;
use tower::ServiceExt;

fn state() -> (tempfile::TempDir, AppState) {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = Arc::new(Catalog::open(dir.path()).expect("open catalog"));
    (dir, AppState { catalog })
}

#[test]
fn flags_override_defaults() {
    let config = Config::try_parse_from([
        "lumen",
        "--data-dir",
        "/tmp/lumen-test",
        "--bind",
        "0.0.0.0:9999",
    ])
    .expect("parse");
    assert_eq!(config.data_dir.to_str(), Some("/tmp/lumen-test"));
    assert_eq!(config.bind.to_string(), "0.0.0.0:9999");
}

#[test]
fn defaults_apply_without_args() {
    let config = Config::try_parse_from(["lumen"]).expect("parse");
    assert_eq!(config.data_dir.to_str(), Some("data"));
    assert_eq!(config.bind.to_string(), "127.0.0.1:7700");
}

#[tokio::test]
async fn health_returns_ok() {
    let (_dir, state) = state();
    let response = router(state)
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
}
