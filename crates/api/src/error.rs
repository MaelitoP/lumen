use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use lumen_core::Error;
use serde_json::json;

#[derive(Debug)]
pub enum ApiError {
    Core(Error),
    Internal,
}

impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        ApiError::Core(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let error = match self {
            ApiError::Core(error) => error,
            ApiError::Internal => return internal("internal error"),
        };
        let (status, kind) = match &error {
            Error::CollectionNotFound(_) => (StatusCode::NOT_FOUND, "collection_not_found"),
            Error::DocumentNotFound(_) => (StatusCode::NOT_FOUND, "document_not_found"),
            Error::SchemaConflict { .. } => (StatusCode::CONFLICT, "schema_conflict"),
            Error::Validation(_) => (StatusCode::BAD_REQUEST, "validation"),
            Error::Mapping(_) => (StatusCode::BAD_REQUEST, "mapping"),
            Error::Recovery(_) | Error::Io(_) | Error::Tantivy(_) => {
                tracing::error!(%error, "internal error");
                return internal("internal error");
            }
        };
        body(status, kind, &error.to_string())
    }
}

fn internal(message: &str) -> Response {
    body(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
}

fn body(status: StatusCode, kind: &str, message: &str) -> Response {
    (
        status,
        Json(json!({ "error": { "type": kind, "message": message } })),
    )
        .into_response()
}
