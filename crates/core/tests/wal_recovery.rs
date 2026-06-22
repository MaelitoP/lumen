use std::collections::BTreeMap;

use lumen_core::{Catalog, FieldSpec, FieldType, Mapping};
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

#[test]
fn document_survives_crash_before_commit() {
    let dir = tempdir().unwrap();
    let source = br#"{"title":"durable"}"#;
    {
        let catalog = Catalog::open(dir.path()).unwrap();
        catalog.create("books", mapping()).unwrap();
        catalog.checkpoint().unwrap();
        catalog
            .upsert_document("books", Some("b1"), source)
            .unwrap();

        // The document is in the WAL but not in a Tantivy commit yet.
        // It is durable, but not searchable until recovery replays it.
        let pending = catalog
            .get("books")
            .unwrap()
            .search("durable", 10, 0)
            .unwrap();
        assert_eq!(pending.total, 0);

        // Dropping the catalog here simulates a process crash before checkpoint.
    }

    let catalog = Catalog::open(dir.path()).unwrap();
    let recovered = catalog
        .get("books")
        .unwrap()
        .search("durable", 10, 0)
        .unwrap();
    assert_eq!(recovered.total, 1);
    assert_eq!(recovered.hits[0].id, "b1");
    assert_eq!(recovered.hits[0].source, source);
}

#[test]
fn committed_document_survives_without_double_apply() {
    let dir = tempdir().unwrap();
    {
        let catalog = Catalog::open(dir.path()).unwrap();
        catalog.create("books", mapping()).unwrap();
        catalog
            .upsert_document("books", Some("b1"), br#"{"title":"kept"}"#)
            .unwrap();
        catalog.checkpoint().unwrap();
    }

    let catalog = Catalog::open(dir.path()).unwrap();
    let results = catalog.get("books").unwrap().search("kept", 10, 0).unwrap();
    assert_eq!(results.total, 1);
}

#[test]
fn recovers_committed_and_uncommitted_documents() {
    let dir = tempdir().unwrap();
    {
        let catalog = Catalog::open(dir.path()).unwrap();
        catalog.create("books", mapping()).unwrap();
        catalog
            .upsert_document("books", Some("committed"), br#"{"title":"shared alpha"}"#)
            .unwrap();
        catalog.checkpoint().unwrap();
        catalog
            .upsert_document("books", Some("pending"), br#"{"title":"shared beta"}"#)
            .unwrap();

        // `committed` is already in Tantivy. `pending` is only in the WAL.
    }

    let catalog = Catalog::open(dir.path()).unwrap();
    let hits = catalog
        .get("books")
        .unwrap()
        .search("shared", 10, 0)
        .unwrap();
    assert_eq!(hits.total, 2);
}

#[test]
fn collection_created_before_crash_recovers() {
    let dir = tempdir().unwrap();
    {
        let catalog = Catalog::open(dir.path()).unwrap();
        catalog.create("books", mapping()).unwrap();

        // No checkpoint: the collection create exists only in the WAL.
    }

    let catalog = Catalog::open(dir.path()).unwrap();
    assert_eq!(catalog.list(), vec!["books".to_string()]);
}

#[test]
fn drop_collection_before_crash_recovers() {
    let dir = tempdir().unwrap();
    {
        let catalog = Catalog::open(dir.path()).unwrap();
        catalog.create("books", mapping()).unwrap();
        catalog.checkpoint().unwrap();
        catalog.drop_collection("books").unwrap();

        // The drop is in the WAL but not in a catalog snapshot yet.
    }

    let catalog = Catalog::open(dir.path()).unwrap();
    assert!(catalog.list().is_empty());
}

#[test]
fn index_for_dropped_collection_is_skipped() {
    let dir = tempdir().unwrap();
    {
        let catalog = Catalog::open(dir.path()).unwrap();
        catalog.create("books", mapping()).unwrap();
        catalog
            .upsert_document("books", Some("b1"), br#"{"title":"x"}"#)
            .unwrap();
        catalog.drop_collection("books").unwrap();

        // Create, index, and drop are all only in the WAL. Replay must not bring
        // the dropped collection back just because it contains an index command.
    }

    let catalog = Catalog::open(dir.path()).unwrap();
    assert!(catalog.list().is_empty());
}

#[test]
fn delete_before_crash_is_replayed() {
    let dir = tempdir().unwrap();
    {
        let catalog = Catalog::open(dir.path()).unwrap();
        catalog.create("books", mapping()).unwrap();
        catalog
            .upsert_document("books", Some("b1"), br#"{"title":"gone"}"#)
            .unwrap();
        catalog.checkpoint().unwrap();
        catalog.delete_document("books", "b1").unwrap();

        // The delete is in the WAL but not in a Tantivy commit yet.
    }

    let catalog = Catalog::open(dir.path()).unwrap();
    let results = catalog.get("books").unwrap().search("gone", 10, 0).unwrap();
    assert_eq!(results.total, 0);
}
