use std::sync::Arc;
use std::time::Instant;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use lumen_core::{Catalog, Mapping, SearchResults, Value};
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
    let created = run(state.catalog, move |c| c.create(&name, mapping)).await?;
    let status = if created.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(created.collection.mapping().clone())))
}

pub(crate) async fn list_collections(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let collections = run(state.catalog, |c| Ok(c.list())).await?;
    Ok(Json(ListResponse { collections }))
}

pub(crate) async fn describe_collection(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let mapping = run(state.catalog, move |c| c.describe(&name)).await?;
    Ok(Json(mapping))
}

pub(crate) async fn drop_collection(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    run(state.catalog, move |c| c.drop(&name)).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn index_document(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let upserted = run(state.catalog, move |c| {
        c.upsert_document(&name, None, &body)
    })
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(index_response(&upserted.id, true)),
    ))
}

pub(crate) async fn put_document(
    State(state): State<AppState>,
    Path((name, id)): Path<(String, String)>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let upserted = run(state.catalog, move |c| {
        c.upsert_document(&name, Some(&id), &body)
    })
    .await?;
    let status = if upserted.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(index_response(&upserted.id, upserted.created))))
}

pub(crate) async fn get_document(
    State(state): State<AppState>,
    Path((name, id)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let response_id = id.clone();
    let source = run(state.catalog, move |c| c.get_document(&name, &id)).await?;
    Ok(Json(GetResponse {
        id: response_id,
        source: parse_source(source)?,
    }))
}

pub(crate) async fn delete_document(
    State(state): State<AppState>,
    Path((name, id)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    run(state.catalog, move |c| c.delete_document(&name, &id)).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn search_documents(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<SearchParams>,
) -> Result<impl IntoResponse, ApiError> {
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT);
    let offset = params.offset.unwrap_or(0);
    let (results, elapsed) = run(state.catalog, move |c| {
        let collection = c.get(&name)?;
        let start = Instant::now();
        let results = collection.search(&params.q, limit, offset)?;
        Ok((results, start.elapsed()))
    })
    .await?;
    Ok(Json(search_response(results, elapsed.as_millis() as u64)?))
}

async fn run<T, F>(catalog: Arc<Catalog>, f: F) -> Result<T, ApiError>
where
    F: FnOnce(&Catalog) -> lumen_core::Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(move || f(&catalog)).await {
        Ok(result) => result.map_err(ApiError::from),
        Err(error) => {
            tracing::error!(%error, "catalog task panicked");
            Err(ApiError::Internal)
        }
    }
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
