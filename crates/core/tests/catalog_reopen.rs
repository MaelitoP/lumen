use std::collections::BTreeMap;
use std::fs;

use lumen_core::{Catalog, Error, FieldSpec, FieldType, Mapping};
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
    Mapping::new(fields).unwrap()
}

fn other_mapping() -> Mapping {
    let mut fields = BTreeMap::new();
    fields.insert(
        "body".to_string(),
        FieldSpec {
            ty: FieldType::Text,
            indexed: true,
            fast: false,
        },
    );
    Mapping::new(fields).unwrap()
}

#[test]
fn create_list_describe_drop() {
    let dir = tempdir().unwrap();
    let catalog = Catalog::open(dir.path()).unwrap();

    catalog.create("books", mapping()).unwrap();
    catalog.create("movies", mapping()).unwrap();
    assert_eq!(
        catalog.list(),
        vec!["books".to_string(), "movies".to_string()]
    );
    assert_eq!(catalog.describe("books").unwrap(), mapping());

    catalog.drop_collection("books").unwrap();
    assert_eq!(catalog.list(), vec!["movies".to_string()]);
    assert!(matches!(
        catalog.get("books"),
        Err(Error::CollectionNotFound(_))
    ));
}

#[test]
fn reopen_restores_catalog() {
    let dir = tempdir().unwrap();
    {
        let catalog = Catalog::open(dir.path()).unwrap();
        catalog.create("books", mapping()).unwrap();
        catalog.create("movies", mapping()).unwrap();
        catalog.drop_collection("movies").unwrap();
    }

    let catalog = Catalog::open(dir.path()).unwrap();
    assert_eq!(catalog.list(), vec!["books".to_string()]);
    assert_eq!(catalog.describe("books").unwrap(), mapping());
}

#[test]
fn idempotent_create_and_schema_conflict() {
    let dir = tempdir().unwrap();
    let catalog = Catalog::open(dir.path()).unwrap();

    let first = catalog.create("books", mapping()).unwrap();
    let again = catalog.create("books", mapping()).unwrap();
    assert!(first.created);
    assert!(!again.created);
    assert_eq!(first.collection.uuid(), again.collection.uuid());

    assert!(matches!(
        catalog.create("books", other_mapping()),
        Err(Error::SchemaConflict { .. })
    ));
}

#[test]
fn rejects_invalid_names() {
    let dir = tempdir().unwrap();
    let catalog = Catalog::open(dir.path()).unwrap();

    let long = "a".repeat(256);
    for bad in ["_internal", "Books", "with space", "", long.as_str()] {
        assert!(
            matches!(catalog.create(bad, mapping()), Err(Error::Validation(_))),
            "{bad:?} should be rejected"
        );
    }
}

#[test]
fn quarantines_orphan_dir() {
    let dir = tempdir().unwrap();
    {
        let catalog = Catalog::open(dir.path()).unwrap();
        catalog.create("books", mapping()).unwrap();
    }

    let orphan = "00000000-0000-4000-8000-000000000000";
    fs::create_dir(dir.path().join(orphan)).unwrap();
    fs::write(dir.path().join(orphan).join("marker"), b"x").unwrap();

    let catalog = Catalog::open(dir.path()).unwrap();
    assert_eq!(catalog.list(), vec!["books".to_string()]);
    assert!(!dir.path().join(orphan).exists());
    let quarantined = fs::read_dir(dir.path().join("_quarantine"))
        .unwrap()
        .count();
    assert_eq!(quarantined, 1);
}

#[test]
fn missing_dir_is_recovery_error() {
    let dir = tempdir().unwrap();
    let uuid = {
        let catalog = Catalog::open(dir.path()).unwrap();
        let uuid = catalog
            .create("books", mapping())
            .unwrap()
            .collection
            .uuid();
        catalog.checkpoint().unwrap();
        uuid
    };

    fs::remove_dir_all(dir.path().join(uuid.to_string())).unwrap();
    assert!(matches!(Catalog::open(dir.path()), Err(Error::Recovery(_))));
}

#[test]
fn drop_removes_data_dir() {
    let dir = tempdir().unwrap();
    let catalog = Catalog::open(dir.path()).unwrap();
    let uuid = catalog
        .create("books", mapping())
        .unwrap()
        .collection
        .uuid();
    let data = dir.path().join(uuid.to_string());
    assert!(data.exists());

    catalog.drop_collection("books").unwrap();
    assert!(!data.exists());
}

#[test]
fn corrupt_snapshot_is_recovery_error() {
    let dir = tempdir().unwrap();
    {
        let catalog = Catalog::open(dir.path()).unwrap();
        catalog.create("books", mapping()).unwrap();
        catalog.checkpoint().unwrap();
    }

    fs::write(dir.path().join("catalog.pb"), b"not-protobuf").unwrap();
    assert!(matches!(Catalog::open(dir.path()), Err(Error::Recovery(_))));
}
