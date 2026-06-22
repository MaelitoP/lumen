use tantivy::collector::{Count, TopDocs};
use tantivy::query::{QueryParser, TermQuery};
use tantivy::schema::{Field, IndexRecordOption, Schema, Term, Value};
use tantivy::{Index, IndexReader, TantivyDocument};

use crate::error::{Error, Result};
use crate::mapping::{ID_FIELD, SOURCE_FIELD};

#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    pub id: String,
    pub score: f32,
    pub source: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchResults {
    pub hits: Vec<SearchHit>,
    pub total: usize,
}

pub(crate) fn execute(
    index: &Index,
    reader: &IndexReader,
    query: &str,
    limit: usize,
    offset: usize,
) -> Result<SearchResults> {
    let schema = index.schema();
    let parser = QueryParser::for_index(index, default_fields(&schema));
    let query = parser
        .parse_query(query)
        .map_err(|e| Error::Validation(format!("invalid query: {e}")))?;

    let searcher = reader.searcher();
    let total = searcher.search(&query, &Count)?;
    if limit == 0 {
        return Ok(SearchResults {
            hits: Vec::new(),
            total,
        });
    }

    let (id_field, source_field) = system_fields(&schema);

    let top = searcher.search(&query, &TopDocs::with_limit(limit).and_offset(offset))?;
    let mut hits = Vec::with_capacity(top.len());
    for (score, address) in top {
        let doc: TantivyDocument = searcher.doc(address)?;
        let id = text_field(&doc, id_field, ID_FIELD)?;
        let source = bytes_field(&doc, source_field, SOURCE_FIELD)?;
        hits.push(SearchHit { id, score, source });
    }
    Ok(SearchResults { hits, total })
}

pub(crate) fn source_by_id(
    index: &Index,
    reader: &IndexReader,
    id: &str,
) -> Result<Option<Vec<u8>>> {
    let schema = index.schema();
    let (id_field, source_field) = system_fields(&schema);

    let query = TermQuery::new(
        Term::from_field_text(id_field, id),
        IndexRecordOption::Basic,
    );
    let searcher = reader.searcher();
    let top = searcher.search(&query, &TopDocs::with_limit(1))?;
    match top.first() {
        Some(&(_, address)) => {
            let doc: TantivyDocument = searcher.doc(address)?;
            Ok(Some(bytes_field(&doc, source_field, SOURCE_FIELD)?))
        }
        None => Ok(None),
    }
}

fn system_fields(schema: &Schema) -> (Field, Field) {
    (
        schema.get_field(ID_FIELD).expect("schema always has _id"),
        schema
            .get_field(SOURCE_FIELD)
            .expect("schema always has _source"),
    )
}

fn default_fields(schema: &Schema) -> Vec<Field> {
    schema
        .fields()
        .filter(|(_, entry)| {
            entry.is_indexed()
                && entry.name() != ID_FIELD
                && matches!(entry.field_type(), tantivy::schema::FieldType::Str(_))
        })
        .map(|(field, _)| field)
        .collect()
}

fn text_field(doc: &TantivyDocument, field: Field, name: &str) -> Result<String> {
    doc.get_first(field)
        .and_then(|value| value.as_str())
        .map(str::to_owned)
        .ok_or_else(|| Error::Recovery(format!("stored document missing system field {name}")))
}

fn bytes_field(doc: &TantivyDocument, field: Field, name: &str) -> Result<Vec<u8>> {
    doc.get_first(field)
        .and_then(|value| value.as_bytes())
        .map(<[u8]>::to_vec)
        .ok_or_else(|| Error::Recovery(format!("stored document missing system field {name}")))
}
