mod catalog;
mod collection;
mod error;
mod mapping;

pub use catalog::Catalog;
pub use collection::Collection;
pub use error::{Error, Result};
pub use mapping::{FieldSpec, FieldType, Mapping, ID_FIELD, SOURCE_FIELD};
pub use serde_json::Value;
