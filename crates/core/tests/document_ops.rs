use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::thread;

use lumen_core::{Catalog, Error, FieldSpec, FieldType, Mapping, SearchResults};
use tempfile::tempdir;

fn spec(ty: FieldType, indexed: bool, fast: bool) -> FieldSpec {
    FieldSpec { ty, indexed, fast }
}

fn mapping() -> Mapping {
    let mut fields = BTreeMap::new();
    fields.insert("title".to_string(), spec(FieldType::Text, true, false));
    fields.insert("tag".to_string(), spec(FieldType::Keyword, true, false));
    fields.insert("year".to_string(), spec(FieldType::I64, true, true));
    Mapping::new(fields).unwrap()
}

fn catalog_with_books(dir: &Path) -> Catalog {
    let catalog = Catalog::open(dir).unwrap();
    catalog.create("books", mapping()).unwrap();
    catalog
}

fn search(catalog: &Catalog, query: &str, limit: usize, offset: usize) -> SearchResults {
    catalog
        .get("books")
        .unwrap()
        .search(query, limit, offset)
        .unwrap()
}

#[test]
fn index_then_search_returns_source() {
    let dir = tempdir().unwrap();
    let catalog = catalog_with_books(dir.path());

    let source = br#"{"title":"the hobbit","year":1937}"#;
    let out = catalog
        .upsert_document("books", Some("b1"), source)
        .unwrap();
    assert_eq!(out.id, "b1");
    assert!(out.created);

    let results = search(&catalog, "hobbit", 10, 0);
    assert_eq!(results.total, 1);
    assert_eq!(results.hits.len(), 1);
    assert_eq!(results.hits[0].id, "b1");
    assert_eq!(results.hits[0].source, source);
}

#[test]
fn generates_id_when_absent() {
    let dir = tempdir().unwrap();
    let catalog = catalog_with_books(dir.path());

    let out = catalog
        .upsert_document("books", None, br#"{"title":"untitled"}"#)
        .unwrap();
    assert!(!out.id.is_empty());
    assert!(out.created);
    assert_eq!(search(&catalog, "untitled", 10, 0).hits[0].id, out.id);
}

#[test]
fn reindexing_replaces_document() {
    let dir = tempdir().unwrap();
    let catalog = catalog_with_books(dir.path());

    assert!(
        catalog
            .upsert_document("books", Some("b1"), br#"{"title":"first"}"#)
            .unwrap()
            .created
    );
    let out = catalog
        .upsert_document("books", Some("b1"), br#"{"title":"second"}"#)
        .unwrap();
    assert!(!out.created);

    assert_eq!(search(&catalog, "second", 10, 0).total, 1);
    assert_eq!(search(&catalog, "first", 10, 0).total, 0);
}

#[test]
fn delete_removes_document() {
    let dir = tempdir().unwrap();
    let catalog = catalog_with_books(dir.path());

    catalog
        .upsert_document("books", Some("b1"), br#"{"title":"gone"}"#)
        .unwrap();
    assert!(catalog.delete_document("books", "b1").unwrap());
    assert_eq!(search(&catalog, "gone", 10, 0).total, 0);
    assert!(!catalog.delete_document("books", "b1").unwrap());
}

#[test]
fn rejects_invalid_documents() {
    let dir = tempdir().unwrap();
    let catalog = catalog_with_books(dir.path());

    let cases: [&[u8]; 3] = [
        br#"{"missing":"y"}"#,
        br#"{"year":"not-a-number"}"#,
        b"not json",
    ];
    for source in cases {
        assert!(matches!(
            catalog.upsert_document("books", Some("x"), source),
            Err(Error::Validation(_))
        ));
    }
}

#[test]
fn invalid_document_does_not_replace_existing() {
    let dir = tempdir().unwrap();
    let catalog = catalog_with_books(dir.path());

    catalog
        .upsert_document("books", Some("b1"), br#"{"title":"keep"}"#)
        .unwrap();
    assert!(catalog
        .upsert_document("books", Some("b1"), br#"{"unmapped":1}"#)
        .is_err());
    assert_eq!(search(&catalog, "keep", 10, 0).total, 1);
}

#[test]
fn field_qualified_query() {
    let dir = tempdir().unwrap();
    let catalog = catalog_with_books(dir.path());

    catalog
        .upsert_document(
            "books",
            Some("b1"),
            br#"{"title":"red apple","tag":"fruit"}"#,
        )
        .unwrap();
    catalog
        .upsert_document(
            "books",
            Some("b2"),
            br#"{"title":"red car","tag":"vehicle"}"#,
        )
        .unwrap();

    let results = search(&catalog, "tag:fruit", 10, 0);
    assert_eq!(results.total, 1);
    assert_eq!(results.hits[0].id, "b1");
}

#[test]
fn total_is_exact_and_independent_of_limit() {
    let dir = tempdir().unwrap();
    let catalog = catalog_with_books(dir.path());

    for i in 0..5 {
        catalog
            .upsert_document(
                "books",
                Some(&format!("b{i}")),
                br#"{"title":"common word"}"#,
            )
            .unwrap();
    }

    let page = search(&catalog, "common", 2, 0);
    assert_eq!(page.total, 5);
    assert_eq!(page.hits.len(), 2);

    let next = search(&catalog, "common", 2, 2);
    assert_eq!(next.total, 5);
    assert_eq!(next.hits.len(), 2);
}

#[test]
fn zero_limit_returns_total_without_hits() {
    let dir = tempdir().unwrap();
    let catalog = catalog_with_books(dir.path());

    catalog
        .upsert_document("books", Some("b1"), br#"{"title":"common"}"#)
        .unwrap();
    catalog
        .upsert_document("books", Some("b2"), br#"{"title":"common"}"#)
        .unwrap();

    let results = search(&catalog, "common", 0, 0);
    assert_eq!(results.total, 2);
    assert!(results.hits.is_empty());
}

#[test]
fn concurrent_writes_stay_consistent() {
    let dir = tempdir().unwrap();
    let catalog = Catalog::open(dir.path()).unwrap();
    catalog.create("books", mapping()).unwrap();
    catalog.create("movies", mapping()).unwrap();
    let catalog = Arc::new(catalog);

    let writers = 4;
    let per_writer = 10;
    let handles: Vec<_> = (0..writers)
        .map(|w| {
            let catalog = Arc::clone(&catalog);
            let collection = if w % 2 == 0 { "books" } else { "movies" };
            thread::spawn(move || {
                for i in 0..per_writer {
                    let id = format!("d{w}-{i}");
                    let source = format!(r#"{{"title":"doc {id}"}}"#);
                    catalog
                        .upsert_document(collection, Some(&id), source.as_bytes())
                        .unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }

    let per_collection = (writers / 2) * per_writer;
    for collection in ["books", "movies"] {
        let hits = catalog
            .get(collection)
            .unwrap()
            .search("doc", 1000, 0)
            .unwrap();
        assert_eq!(hits.total, per_collection);
    }
}
