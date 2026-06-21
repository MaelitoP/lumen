use std::collections::BTreeMap;

use lumen_core::{Catalog, Error, FieldSpec, FieldType, Mapping};
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

fn collection(dir: &std::path::Path) -> std::sync::Arc<lumen_core::Collection> {
    Catalog::open(dir)
        .unwrap()
        .create("books", mapping())
        .unwrap()
}

#[test]
fn index_then_search_returns_source() {
    let dir = tempdir().unwrap();
    let col = collection(dir.path());

    let source = br#"{"title":"the hobbit","year":1937}"#;
    let out = col.upsert(Some("b1"), source).unwrap();
    assert_eq!(out.id, "b1");
    assert!(out.created);

    let results = col.search("hobbit", 10, 0).unwrap();
    assert_eq!(results.total, 1);
    assert_eq!(results.hits.len(), 1);
    assert_eq!(results.hits[0].id, "b1");
    assert_eq!(results.hits[0].source, source);
}

#[test]
fn generates_id_when_absent() {
    let dir = tempdir().unwrap();
    let col = collection(dir.path());

    let out = col.upsert(None, br#"{"title":"untitled"}"#).unwrap();
    assert!(!out.id.is_empty());
    assert!(out.created);
    assert_eq!(col.search("untitled", 10, 0).unwrap().hits[0].id, out.id);
}

#[test]
fn reindexing_replaces_document() {
    let dir = tempdir().unwrap();
    let col = collection(dir.path());

    assert!(
        col.upsert(Some("b1"), br#"{"title":"first"}"#)
            .unwrap()
            .created
    );
    let out = col.upsert(Some("b1"), br#"{"title":"second"}"#).unwrap();
    assert!(!out.created);

    assert_eq!(col.search("second", 10, 0).unwrap().total, 1);
    assert_eq!(col.search("first", 10, 0).unwrap().total, 0);
}

#[test]
fn delete_removes_document() {
    let dir = tempdir().unwrap();
    let col = collection(dir.path());

    col.upsert(Some("b1"), br#"{"title":"gone"}"#).unwrap();
    assert!(col.delete("b1").unwrap());
    assert_eq!(col.search("gone", 10, 0).unwrap().total, 0);
    assert!(!col.delete("b1").unwrap());
}

#[test]
fn rejects_invalid_documents() {
    let dir = tempdir().unwrap();
    let col = collection(dir.path());

    let cases: [&[u8]; 3] = [
        br#"{"missing":"y"}"#,
        br#"{"year":"not-a-number"}"#,
        b"not json",
    ];
    for source in cases {
        assert!(matches!(
            col.upsert(Some("x"), source),
            Err(Error::Validation(_))
        ));
    }
}

#[test]
fn invalid_document_does_not_replace_existing() {
    let dir = tempdir().unwrap();
    let col = collection(dir.path());

    col.upsert(Some("b1"), br#"{"title":"keep"}"#).unwrap();
    assert!(col.upsert(Some("b1"), br#"{"unmapped":1}"#).is_err());
    assert_eq!(col.search("keep", 10, 0).unwrap().total, 1);
}

#[test]
fn field_qualified_query() {
    let dir = tempdir().unwrap();
    let col = collection(dir.path());

    col.upsert(Some("b1"), br#"{"title":"red apple","tag":"fruit"}"#)
        .unwrap();
    col.upsert(Some("b2"), br#"{"title":"red car","tag":"vehicle"}"#)
        .unwrap();

    let results = col.search("tag:fruit", 10, 0).unwrap();
    assert_eq!(results.total, 1);
    assert_eq!(results.hits[0].id, "b1");
}

#[test]
fn total_is_exact_and_independent_of_limit() {
    let dir = tempdir().unwrap();
    let col = collection(dir.path());

    for i in 0..5 {
        col.upsert(Some(&format!("b{i}")), br#"{"title":"common word"}"#)
            .unwrap();
    }

    let page = col.search("common", 2, 0).unwrap();
    assert_eq!(page.total, 5);
    assert_eq!(page.hits.len(), 2);

    let next = col.search("common", 2, 2).unwrap();
    assert_eq!(next.total, 5);
    assert_eq!(next.hits.len(), 2);
}

#[test]
fn zero_limit_returns_total_without_hits() {
    let dir = tempdir().unwrap();
    let col = collection(dir.path());

    col.upsert(Some("b1"), br#"{"title":"common"}"#).unwrap();
    col.upsert(Some("b2"), br#"{"title":"common"}"#).unwrap();

    let results = col.search("common", 0, 0).unwrap();
    assert_eq!(results.total, 2);
    assert!(results.hits.is_empty());
}
