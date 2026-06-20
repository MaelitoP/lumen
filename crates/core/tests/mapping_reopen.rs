use std::collections::BTreeMap;

use lumen_core::{FieldSpec, FieldType, Mapping};
use tantivy::Index;
use tempfile::tempdir;

fn mapping() -> Mapping {
    let mut fields = BTreeMap::new();
    fields.insert(
        "title".to_string(),
        FieldSpec {
            ty: FieldType::Text,
            indexed: true,
            fast: false,
        },
    );
    fields.insert(
        "year".to_string(),
        FieldSpec {
            ty: FieldType::I64,
            indexed: true,
            fast: true,
        },
    );
    Mapping::new(fields).unwrap()
}

#[test]
fn schema_survives_reopen() {
    let dir = tempdir().unwrap();
    let schema = mapping().to_schema();

    Index::create_in_dir(dir.path(), schema.clone()).unwrap();
    let reopened = Index::open_in_dir(dir.path()).unwrap();

    assert_eq!(reopened.schema(), schema);
}
