use axum::http::header::LOCATION;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use lumen_cluster::ClientError;
use lumen_core::Error;
use serde_json::json;

#[derive(Debug)]
pub enum ApiError {
    Core(Error),
    Mapping(String),
    ForwardToLeader(String),
    Unavailable,
    AlreadyInitialized,
    Internal,
}

impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        ApiError::Core(error)
    }
}

impl From<ClientError> for ApiError {
    fn from(error: ClientError) -> Self {
        match error {
            ClientError::Core(core) => ApiError::Core(core),
            ClientError::ForwardToLeader(Some(node)) => ApiError::ForwardToLeader(node.rpc_addr),
            ClientError::ForwardToLeader(None) | ClientError::Unavailable => ApiError::Unavailable,
            ClientError::AlreadyInitialized => ApiError::AlreadyInitialized,
            ClientError::Fatal(error) => {
                tracing::error!(%error, "cluster fatal error");
                ApiError::Internal
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, kind, message) = match self {
            ApiError::Mapping(message) => (StatusCode::BAD_REQUEST, "mapping", message),
            ApiError::ForwardToLeader(addr) => return forward_to_leader(&addr),
            ApiError::Unavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                "no leader available".to_owned(),
            ),
            ApiError::AlreadyInitialized => (
                StatusCode::CONFLICT,
                "already_initialized",
                "cluster already initialized".to_owned(),
            ),
            ApiError::Internal => return internal("internal error"),
            ApiError::Core(error) => {
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
                (status, kind, error.to_string())
            }
        };
        body(status, kind, &message)
    }
}

fn forward_to_leader(addr: &str) -> Response {
    let mut response = body(
        StatusCode::TEMPORARY_REDIRECT,
        "forward_to_leader",
        &format!("leader at {addr}"),
    );
    if let Ok(value) = HeaderValue::from_str(addr) {
        response.headers_mut().insert(LOCATION, value);
    }
    response
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

#[cfg(test)]
mod tests {
    use lumen_cluster::Node;

    use super::*;

    #[test]
    fn forward_to_leader_is_307_with_location() {
        let response = ApiError::ForwardToLeader("127.0.0.1:9001".to_string()).into_response();
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(response.headers().get(LOCATION).unwrap(), "127.0.0.1:9001");
    }

    #[test]
    fn unavailable_is_503() {
        assert_eq!(
            ApiError::Unavailable.into_response().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn already_initialized_is_409() {
        assert_eq!(
            ApiError::AlreadyInitialized.into_response().status(),
            StatusCode::CONFLICT
        );
    }

    #[test]
    fn client_forward_to_known_leader_redirects_to_rpc_addr() {
        let err = ApiError::from(ClientError::ForwardToLeader(Some(Node {
            node_id: 2,
            rpc_addr: "10.0.0.2:9002".to_string(),
        })));
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(response.headers().get(LOCATION).unwrap(), "10.0.0.2:9002");
    }

    #[test]
    fn client_forward_to_unknown_leader_is_503() {
        let err = ApiError::from(ClientError::ForwardToLeader(None));
        assert_eq!(
            err.into_response().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
