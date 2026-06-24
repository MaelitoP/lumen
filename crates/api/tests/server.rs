use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use clap::Parser;
use lumen_api::{router, AppState, Cli, Command, Engine, StandaloneEngine};
use lumen_core::Catalog;
use serde_json::Value;
use tower::ServiceExt;

fn state() -> (tempfile::TempDir, AppState, Arc<Catalog>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog = Arc::new(Catalog::open(dir.path()).expect("open catalog"));
    let engine: Arc<dyn Engine> = Arc::new(StandaloneEngine::new(Arc::clone(&catalog)));
    (dir, AppState { engine }, catalog)
}

async fn call(state: &AppState, method: Method, uri: &str, body: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::from(body.to_owned()))
        .expect("request");
    let response = router(state.clone())
        .oneshot(request)
        .await
        .expect("response");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("json body")
    };
    (status, value)
}

const MAPPING: &str = r#"{"fields":{"title":{"type":"text","indexed":true},"year":{"type":"i64","indexed":true,"fast":true}}}"#;

async fn create_books(state: &AppState) {
    let (status, _) = call(state, Method::PUT, "/collections/books", MAPPING).await;
    assert_eq!(status, StatusCode::CREATED);
}

fn standalone(args: &[&str]) -> lumen_api::StandaloneConfig {
    match Cli::try_parse_from(args).expect("parse").command {
        Command::Standalone(config) => config,
        Command::Cluster(_) => panic!("expected standalone"),
    }
}

#[test]
fn flags_override_defaults() {
    let config = standalone(&[
        "lumen",
        "standalone",
        "--data-dir",
        "/tmp/lumen-test",
        "--bind",
        "0.0.0.0:9999",
        "--checkpoint-interval-secs",
        "1",
    ]);
    assert_eq!(config.data_dir.to_str(), Some("/tmp/lumen-test"));
    assert_eq!(config.bind.to_string(), "0.0.0.0:9999");
    assert_eq!(config.checkpoint_interval_secs, 1);
}

#[test]
fn defaults_apply_without_args() {
    let config = standalone(&["lumen", "standalone"]);
    assert_eq!(config.data_dir.to_str(), Some("data"));
    assert_eq!(config.bind.to_string(), "127.0.0.1:7700");
    assert_eq!(config.checkpoint_interval_secs, 30);
}

#[tokio::test]
async fn health_returns_ok() {
    let (_dir, state, _catalog) = state();
    let (status, _) = call(&state, Method::GET, "/health", "").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn create_describe_list_roundtrip() {
    let (_dir, state, _catalog) = state();
    create_books(&state).await;

    let (status, body) = call(&state, Method::GET, "/collections/books", "").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["fields"]["title"]["type"], "text");

    let (status, body) = call(&state, Method::GET, "/collections", "").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["collections"], serde_json::json!(["books"]));
}

#[tokio::test]
async fn create_is_idempotent_and_rejects_conflict() {
    let (_dir, state, _catalog) = state();
    create_books(&state).await;

    let (status, _) = call(&state, Method::PUT, "/collections/books", MAPPING).await;
    assert_eq!(status, StatusCode::OK);

    let other = r#"{"fields":{"title":{"type":"keyword","indexed":true}}}"#;
    let (status, body) = call(&state, Method::PUT, "/collections/books", other).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["type"], "schema_conflict");
}

#[tokio::test]
async fn index_get_and_search_document() {
    let (_dir, state, catalog) = state();
    create_books(&state).await;

    let (status, body) = call(
        &state,
        Method::POST,
        "/collections/books/documents",
        r#"{"title":"the hobbit","year":1937}"#,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["result"], "created");
    let id = body["id"].as_str().expect("id").to_owned();

    catalog.checkpoint().expect("checkpoint");

    let (status, body) = call(
        &state,
        Method::GET,
        &format!("/collections/books/documents/{id}"),
        "",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["source"]["title"], "the hobbit");

    let (status, body) = call(
        &state,
        Method::GET,
        "/collections/books/documents/search?q=hobbit&limit=10",
        "",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total"], 1);
    assert_eq!(body["hits"][0]["id"], id);
    assert_eq!(body["hits"][0]["source"]["year"], 1937);
    assert!(body["took_ms"].is_u64());
}

#[tokio::test]
async fn put_reports_created_then_updated() {
    let (_dir, state, catalog) = state();
    create_books(&state).await;

    let (status, body) = call(
        &state,
        Method::PUT,
        "/collections/books/documents/b1",
        r#"{"title":"first"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["result"], "created");

    catalog.checkpoint().expect("checkpoint");

    let (status, body) = call(
        &state,
        Method::PUT,
        "/collections/books/documents/b1",
        r#"{"title":"second"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], "updated");
}

#[tokio::test]
async fn delete_document_then_get_is_not_found() {
    let (_dir, state, catalog) = state();
    create_books(&state).await;
    call(
        &state,
        Method::PUT,
        "/collections/books/documents/b1",
        r#"{"title":"gone"}"#,
    )
    .await;
    catalog.checkpoint().expect("checkpoint");

    let (status, _) = call(
        &state,
        Method::DELETE,
        "/collections/books/documents/b1",
        "",
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    catalog.checkpoint().expect("checkpoint");

    let (status, body) = call(&state, Method::GET, "/collections/books/documents/b1", "").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["type"], "document_not_found");
}

#[tokio::test]
async fn drop_collection_then_operations_are_not_found() {
    let (_dir, state, _catalog) = state();
    create_books(&state).await;

    let (status, _) = call(&state, Method::DELETE, "/collections/books", "").await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, body) = call(&state, Method::GET, "/collections/books", "").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["type"], "collection_not_found");
}

#[tokio::test]
async fn rejects_invalid_documents() {
    let (_dir, state, _catalog) = state();
    create_books(&state).await;

    let (status, body) = call(
        &state,
        Method::POST,
        "/collections/books/documents",
        r#"{"unmapped":"x"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["type"], "validation");
}

#[tokio::test]
async fn rejects_invalid_mapping() {
    let (_dir, state, _catalog) = state();
    let (status, body) = call(&state, Method::PUT, "/collections/books", "not json").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["type"], "mapping");
}

#[tokio::test]
async fn index_into_missing_collection_is_not_found() {
    let (_dir, state, _catalog) = state();
    let (status, body) = call(
        &state,
        Method::POST,
        "/collections/ghost/documents",
        r#"{"title":"x"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["type"], "collection_not_found");
}

#[tokio::test]
async fn drop_missing_collection_is_not_found() {
    let (_dir, state, _catalog) = state();
    let (status, body) = call(&state, Method::DELETE, "/collections/ghost", "").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["type"], "collection_not_found");
}

#[tokio::test]
async fn document_verbs_on_missing_collection_are_not_found() {
    let (_dir, state, _catalog) = state();
    for (method, uri) in [
        (Method::PUT, "/collections/ghost/documents/x"),
        (Method::GET, "/collections/ghost/documents/x"),
        (Method::DELETE, "/collections/ghost/documents/x"),
        (Method::GET, "/collections/ghost/documents/search?q=x"),
    ] {
        let (status, body) = call(&state, method, uri, r#"{"title":"x"}"#).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}");
        assert_eq!(body["error"]["type"], "collection_not_found", "{uri}");
    }
}

#[tokio::test]
async fn delete_missing_document_is_idempotent() {
    let (_dir, state, _catalog) = state();
    create_books(&state).await;
    let (status, _) = call(
        &state,
        Method::DELETE,
        "/collections/books/documents/absent",
        "",
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn search_honors_limit_and_offset() {
    let (_dir, state, catalog) = state();
    create_books(&state).await;
    for id in ["a", "b", "c"] {
        call(
            &state,
            Method::PUT,
            &format!("/collections/books/documents/{id}"),
            r#"{"title":"shared tale"}"#,
        )
        .await;
    }
    catalog.checkpoint().expect("checkpoint");

    let (status, body) = call(
        &state,
        Method::GET,
        "/collections/books/documents/search?q=shared&limit=2&offset=0",
        "",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total"], 3);
    assert_eq!(body["hits"].as_array().expect("hits").len(), 2);

    let (status, body) = call(
        &state,
        Method::GET,
        "/collections/books/documents/search?q=shared&limit=2&offset=2",
        "",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total"], 3);
    assert_eq!(body["hits"].as_array().expect("hits").len(), 1);
}
