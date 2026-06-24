use std::time::Instant;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use lumen_core::{Mapping, SearchResults, Value};
use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::AppState;

const DEFAULT_LIMIT: usize = 10;

pub(crate) async fn create_collection(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let mapping: Mapping =
        serde_json::from_slice(&body).map_err(|e| ApiError::Mapping(e.to_string()))?;
    let outcome = state.engine.create_collection(name, mapping).await?;
    let status = if outcome.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(outcome.mapping)))
}

pub(crate) async fn list_collections(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let collections = state.engine.list().await?;
    Ok(Json(ListResponse { collections }))
}

pub(crate) async fn describe_collection(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let mapping = state.engine.describe(name).await?;
    Ok(Json(mapping))
}

pub(crate) async fn drop_collection(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    state.engine.drop_collection(name).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn index_document(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let outcome = state.engine.index(name, None, body).await?;
    Ok((StatusCode::CREATED, Json(index_response(&outcome.id, true))))
}

pub(crate) async fn put_document(
    State(state): State<AppState>,
    Path((name, id)): Path<(String, String)>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let outcome = state.engine.index(name, Some(id), body).await?;
    let status = if outcome.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(index_response(&outcome.id, outcome.created))))
}

pub(crate) async fn get_document(
    State(state): State<AppState>,
    Path((name, id)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let response_id = id.clone();
    let source = state.engine.get_document(name, id).await?;
    Ok(Json(GetResponse {
        id: response_id,
        source: parse_source(source)?,
    }))
}

pub(crate) async fn delete_document(
    State(state): State<AppState>,
    Path((name, id)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    state.engine.delete(name, id).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn search_documents(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<SearchParams>,
) -> Result<impl IntoResponse, ApiError> {
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT);
    let offset = params.offset.unwrap_or(0);
    let start = Instant::now();
    let results = state.engine.search(name, params.q, limit, offset).await?;
    let took_ms = start.elapsed().as_millis() as u64;
    Ok(Json(search_response(results, took_ms)?))
}

fn index_response(id: &str, created: bool) -> IndexResponse {
    IndexResponse {
        id: id.to_owned(),
        result: if created {
            WriteResult::Created
        } else {
            WriteResult::Updated
        },
    }
}

fn search_response(results: SearchResults, took_ms: u64) -> Result<SearchResponse, ApiError> {
    let hits = results
        .hits
        .into_iter()
        .map(|hit| {
            Ok(Hit {
                id: hit.id,
                score: hit.score,
                source: parse_source(hit.source)?,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(SearchResponse {
        hits,
        total: results.total,
        took_ms,
    })
}

fn parse_source(bytes: Vec<u8>) -> Result<Value, ApiError> {
    serde_json::from_slice(&bytes).map_err(|error| {
        tracing::error!(%error, "stored _source is not valid json");
        ApiError::Internal
    })
}

#[derive(Debug, Serialize)]
struct ListResponse {
    collections: Vec<String>,
}

#[derive(Debug, Serialize)]
struct IndexResponse {
    id: String,
    result: WriteResult,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
enum WriteResult {
    Created,
    Updated,
}

#[derive(Debug, Serialize)]
struct GetResponse {
    id: String,
    source: Value,
}

#[derive(Debug, Serialize)]
struct SearchResponse {
    hits: Vec<Hit>,
    total: usize,
    took_ms: u64,
}

#[derive(Debug, Serialize)]
struct Hit {
    id: String,
    score: f32,
    source: Value,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SearchParams {
    #[serde(default)]
    q: String,
    limit: Option<usize>,
    offset: Option<usize>,
}
