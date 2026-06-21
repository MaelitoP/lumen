use serde_json::Value;
use tantivy::schema::{Field, Schema};
use tantivy::time::format_description::well_known::Rfc3339;
use tantivy::time::OffsetDateTime;
use tantivy::{DateTime, TantivyDocument};

use crate::error::{Error, Result};
use crate::mapping::{FieldType, Mapping, ID_FIELD, SOURCE_FIELD};

pub(crate) fn build_doc(
    schema: &Schema,
    mapping: &Mapping,
    id: &str,
    source: &[u8],
    parsed: &Value,
) -> Result<TantivyDocument> {
    let id_field = schema.get_field(ID_FIELD).expect("schema always has _id");
    let source_field = schema
        .get_field(SOURCE_FIELD)
        .expect("schema always has _source");

    let mut doc = TantivyDocument::default();
    doc.add_text(id_field, id);
    doc.add_bytes(source_field, source);

    let object = parsed
        .as_object()
        .ok_or_else(|| Error::Validation("document must be a JSON object".into()))?;

    for (name, value) in object {
        let Ok(field) = schema.get_field(name) else {
            continue;
        };
        let spec = mapping
            .field(name)
            .expect("validated: every field is mapped");
        add_value(&mut doc, field, value, spec.ty)?;
    }
    Ok(doc)
}

fn add_value(doc: &mut TantivyDocument, field: Field, value: &Value, ty: FieldType) -> Result<()> {
    match ty {
        FieldType::Text | FieldType::Keyword => {
            doc.add_text(field, value.as_str().ok_or_else(|| type_err(ty))?);
        }
        FieldType::I64 => {
            doc.add_i64(field, value.as_i64().ok_or_else(|| type_err(ty))?);
        }
        FieldType::F64 => {
            doc.add_f64(field, value.as_f64().ok_or_else(|| type_err(ty))?);
        }
        FieldType::Date => {
            let s = value.as_str().ok_or_else(|| type_err(ty))?;
            let odt = OffsetDateTime::parse(s, &Rfc3339).map_err(|_| type_err(ty))?;
            doc.add_date(field, DateTime::from_utc(odt));
        }
    }
    Ok(())
}

fn type_err(ty: FieldType) -> Error {
    Error::Validation(format!("field value does not match type {ty:?}"))
}
