use lumen_core::Index;
use tempfile::tempdir;

fn open_index() -> (tempfile::TempDir, Index) {
    let dir = tempdir().unwrap();
    let index = Index::open(dir.path().join("idx"), 50_000_000).unwrap();
    (dir, index)
}

#[test]
fn search_returns_the_matching_document() {
    let (_dir, mut index) = open_index();

    index
        .add_document("Rust in Action", "a hands-on guide to systems programming")
        .unwrap();
    index
        .add_document("Cooking Basics", "an introduction to food and recipes")
        .unwrap();
    index.commit().unwrap();

    let hits = index.search("programming", 10).unwrap();

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].title, "Rust in Action");
}

#[test]
fn search_ranks_both_documents_for_a_shared_term() {
    let (_dir, mut index) = open_index();

    index.add_document("First", "shared term here").unwrap();
    index.add_document("Second", "another shared line").unwrap();
    index.commit().unwrap();

    let hits = index.search("shared", 10).unwrap();

    assert_eq!(hits.len(), 2);
    let titles: Vec<&str> = hits.iter().map(|hit| hit.title.as_str()).collect();
    assert!(titles.contains(&"First"));
    assert!(titles.contains(&"Second"));
}
