pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("collection not found: {0}")]
    CollectionNotFound(String),
    #[error("schema conflict for collection {name}: existing mapping differs from requested")]
    SchemaConflict { name: String },
    #[error("invalid mapping: {0}")]
    Mapping(String),
    #[error("document validation failed: {0}")]
    Validation(String),
    #[error("document not found: {0}")]
    DocumentNotFound(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Tantivy(#[from] tantivy::TantivyError),
}
