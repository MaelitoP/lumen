use std::collections::BTreeMap;
use std::fs;

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

fn doc(title: &str) -> Vec<u8> {
    format!(r#"{{"title":"{title}"}}"#).into_bytes()
}

#[test]
fn install_brings_a_fresh_catalog_current() {
    let source = tempdir().unwrap();
    let archive = {
        let catalog = Catalog::open(source.path()).unwrap();
        catalog.create("books", mapping()).unwrap();
        catalog
            .upsert_document("books", Some("b1"), &doc("alpha"))
            .unwrap();
        catalog
            .upsert_document("books", Some("b2"), &doc("beta"))
            .unwrap();
        catalog.checkpoint().unwrap();
        catalog.build_snapshot().unwrap()
    };

    let target = tempdir().unwrap();
    let catalog = Catalog::open(target.path()).unwrap();
    catalog.install_snapshot(&archive).unwrap();

    assert_eq!(catalog.list(), vec!["books".to_string()]);
    let books = catalog.get("books").unwrap();
    assert_eq!(books.search("alpha", 10, 0).unwrap().total, 1);
    assert_eq!(books.search("beta", 10, 0).unwrap().total, 1);
    assert_eq!(books.source("b1").unwrap().unwrap(), doc("alpha"));
}

#[test]
fn install_quarantines_dirs_orphaned_by_the_swap() {
    let source = tempdir().unwrap();
    let archive = {
        let catalog = Catalog::open(source.path()).unwrap();
        catalog.create("books", mapping()).unwrap();
        catalog.checkpoint().unwrap();
        catalog.build_snapshot().unwrap()
    };

    let target = tempdir().unwrap();
    let catalog = Catalog::open(target.path()).unwrap();
    let stale = catalog
        .create("movies", mapping())
        .unwrap()
        .collection
        .uuid();
    catalog.checkpoint().unwrap();
    assert!(target.path().join(stale.to_string()).exists());

    catalog.install_snapshot(&archive).unwrap();

    assert_eq!(catalog.list(), vec!["books".to_string()]);
    assert!(!target.path().join(stale.to_string()).exists());
    let quarantined = fs::read_dir(target.path().join("_quarantine"))
        .unwrap()
        .count();
    assert_eq!(quarantined, 1);
}

#[test]
fn current_snapshot_tracks_the_last_archive() {
    let dir = tempdir().unwrap();
    let catalog = Catalog::open(dir.path()).unwrap();
    assert!(catalog.current_snapshot().is_none());

    catalog.create("books", mapping()).unwrap();
    catalog.checkpoint().unwrap();
    let archive = catalog.build_snapshot().unwrap();
    assert_eq!(catalog.current_snapshot(), Some(archive));
}
