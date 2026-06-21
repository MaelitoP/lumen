use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde_json::Value;
use tantivy::collector::Count;
use tantivy::query::TermQuery;
use tantivy::schema::{IndexRecordOption, Term};
use tantivy::{Index, IndexReader, IndexWriter};
use uuid::Uuid;

use crate::document::build_doc;
use crate::error::{Error, Result};
use crate::mapping::{Mapping, ID_FIELD};
use crate::search::{self, SearchResults};

const WRITER_HEAP_BYTES: usize = 15_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Upserted {
    pub id: String,
    pub created: bool,
}

pub struct Collection {
    uuid: Uuid,
    mapping: Mapping,
    index: Index,
    writer: Mutex<IndexWriter>,
    reader: IndexReader,
}

impl Collection {
    pub(crate) fn create(root: &Path, uuid: Uuid, mapping: Mapping) -> Result<Self> {
        let dir = collection_dir(root, uuid);
        fs::create_dir(&dir)?;
        let index = Index::create_in_dir(&dir, mapping.to_schema())?;
        Self::from_index(uuid, mapping, index)
    }

    pub(crate) fn open(root: &Path, uuid: Uuid, mapping: Mapping) -> Result<Self> {
        let index = Index::open_in_dir(collection_dir(root, uuid))?;
        Self::from_index(uuid, mapping, index)
    }

    fn from_index(uuid: Uuid, mapping: Mapping, index: Index) -> Result<Self> {
        let writer = index.writer(WRITER_HEAP_BYTES)?;
        let reader = index.reader()?;
        Ok(Self {
            uuid,
            mapping,
            index,
            writer: Mutex::new(writer),
            reader,
        })
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn mapping(&self) -> &Mapping {
        &self.mapping
    }

    pub fn upsert(&self, id: Option<&str>, source: &[u8]) -> Result<Upserted> {
        let parsed: Value = serde_json::from_slice(source)
            .map_err(|e| Error::Validation(format!("invalid JSON: {e}")))?;
        self.mapping.validate_document(&parsed)?;

        let id = id.map_or_else(|| Uuid::new_v4().to_string(), str::to_owned);
        let schema = self.index.schema();
        let doc = build_doc(&schema, &self.mapping, &id, source, &parsed)?;

        let mut writer = self.writer.lock().expect("writer poisoned");
        let created = !self.id_exists(&id)?;
        writer.delete_term(self.id_term(&id));
        writer.add_document(doc)?;
        writer.commit()?;
        self.reader.reload()?;
        Ok(Upserted { id, created })
    }

    pub fn delete(&self, id: &str) -> Result<bool> {
        let mut writer = self.writer.lock().expect("writer poisoned");
        let existed = self.id_exists(id)?;
        writer.delete_term(self.id_term(id));
        writer.commit()?;
        self.reader.reload()?;
        Ok(existed)
    }

    pub fn search(&self, query: &str, limit: usize, offset: usize) -> Result<SearchResults> {
        search::execute(&self.index, &self.reader, query, limit, offset)
    }

    fn id_term(&self, id: &str) -> Term {
        let id_field = self
            .index
            .schema()
            .get_field(ID_FIELD)
            .expect("schema always has _id");
        Term::from_field_text(id_field, id)
    }

    fn id_exists(&self, id: &str) -> Result<bool> {
        let searcher = self.reader.searcher();
        let query = TermQuery::new(self.id_term(id), IndexRecordOption::Basic);
        Ok(searcher.search(&query, &Count)? > 0)
    }
}

impl fmt::Debug for Collection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Collection")
            .field("uuid", &self.uuid)
            .field("mapping", &self.mapping)
            .finish_non_exhaustive()
    }
}

fn collection_dir(root: &Path, uuid: Uuid) -> PathBuf {
    root.join(uuid.to_string())
}
