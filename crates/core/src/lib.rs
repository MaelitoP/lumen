//! # Lumen Core
//!
//! A minimal single-node index built on Tantivy. It exposes a small `Index`
//! abstraction over a fixed `title`/`body` schema: open, add documents, commit,
//! and run a parsed query. This is the first piece of Lumen that does real
//! indexing work; everything beyond it is still design intent.

use std::path::Path;

use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, Schema, Value, STORED, TEXT};
use tantivy::{doc, Index as TantivyIndex, IndexWriter, TantivyDocument};

pub use tantivy::TantivyError as Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    pub score: f32,
    pub title: String,
}

pub struct Index {
    inner: TantivyIndex,
    writer: IndexWriter,
    title: Field,
    body: Field,
}

impl Index {
    pub fn open(path: impl AsRef<Path>, writer_mem_budget: usize) -> Result<Self> {
        let path = path.as_ref();
        let (schema, title, body) = build_schema();

        let inner = if path.exists() {
            TantivyIndex::open_in_dir(path)?
        } else {
            std::fs::create_dir_all(path)?;
            TantivyIndex::create_in_dir(path, schema)?
        };

        let writer = inner.writer(writer_mem_budget)?;
        Ok(Self {
            inner,
            writer,
            title,
            body,
        })
    }

    pub fn add_document(&mut self, title: &str, body: &str) -> Result<()> {
        self.writer.add_document(doc!(
            self.title => title,
            self.body => body,
        ))?;
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        self.writer.commit()?;
        Ok(())
    }

    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>> {
        let searcher = self.inner.reader()?.searcher();
        let parser = QueryParser::for_index(&self.inner, vec![self.title, self.body]);
        let parsed = parser.parse_query(query)?;

        let top = searcher.search(&parsed, &TopDocs::with_limit(limit))?;
        let mut hits = Vec::with_capacity(top.len());
        for (score, address) in top {
            let document: TantivyDocument = searcher.doc(address)?;
            let title = document
                .get_first(self.title)
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string();
            hits.push(SearchHit { score, title });
        }
        Ok(hits)
    }
}

fn build_schema() -> (Schema, Field, Field) {
    let mut builder = Schema::builder();
    let title = builder.add_text_field("title", TEXT | STORED);
    let body = builder.add_text_field("body", TEXT);
    (builder.build(), title, body)
}
