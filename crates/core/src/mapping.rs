use std::collections::BTreeMap;

use lumen_proto::v1 as proto;
use serde::{Deserialize, Serialize};
use tantivy::schema::{
    BytesOptions, DateOptions, IndexRecordOption, NumericOptions, Schema, TextFieldIndexing,
    TextOptions, STRING, TEXT,
};
use tantivy::time::format_description::well_known::Rfc3339;
use tantivy::time::OffsetDateTime;

use crate::error::{Error, Result};

pub const ID_FIELD: &str = "_id";
pub const SOURCE_FIELD: &str = "_source";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    Text,
    Keyword,
    I64,
    F64,
    Date,
}

impl FieldType {
    fn as_str(self) -> &'static str {
        match self {
            FieldType::Text => "text",
            FieldType::Keyword => "keyword",
            FieldType::I64 => "i64",
            FieldType::F64 => "f64",
            FieldType::Date => "date",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldSpec {
    pub ty: FieldType,
    pub indexed: bool,
    pub fast: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "MappingDto", try_from = "MappingDto")]
pub struct Mapping {
    fields: BTreeMap<String, FieldSpec>,
}

impl Mapping {
    pub fn new(fields: BTreeMap<String, FieldSpec>) -> Result<Self> {
        for (name, spec) in &fields {
            validate_field(name, spec)?;
        }
        Ok(Self { fields })
    }

    pub fn to_schema(&self) -> Schema {
        let mut builder = Schema::builder();
        builder.add_text_field(ID_FIELD, STRING.set_stored());
        builder.add_bytes_field(SOURCE_FIELD, BytesOptions::default().set_stored());

        for (name, spec) in &self.fields {
            if !spec.indexed && !spec.fast {
                continue;
            }
            match spec.ty {
                FieldType::Text => {
                    builder.add_text_field(name, TEXT);
                }
                FieldType::Keyword => {
                    let mut opts = TextOptions::default();
                    if spec.indexed {
                        opts = opts.set_indexing_options(
                            TextFieldIndexing::default()
                                .set_tokenizer("raw")
                                .set_index_option(IndexRecordOption::Basic),
                        );
                    }
                    if spec.fast {
                        opts = opts.set_fast(Some("raw"));
                    }
                    builder.add_text_field(name, opts);
                }
                FieldType::I64 => {
                    builder.add_i64_field(name, numeric_opts(spec));
                }
                FieldType::F64 => {
                    builder.add_f64_field(name, numeric_opts(spec));
                }
                FieldType::Date => {
                    let mut opts = DateOptions::default();
                    if spec.indexed {
                        opts = opts.set_indexed();
                    }
                    if spec.fast {
                        opts = opts.set_fast();
                    }
                    builder.add_date_field(name, opts);
                }
            }
        }
        builder.build()
    }

    pub fn validate_document(&self, document: &serde_json::Value) -> Result<()> {
        let object = document
            .as_object()
            .ok_or_else(|| Error::Validation("document must be a JSON object".into()))?;

        for (name, value) in object {
            let spec = self
                .fields
                .get(name)
                .ok_or_else(|| Error::Validation(format!("unmapped field {name}")))?;
            if !value_matches(value, spec.ty) {
                return Err(Error::Validation(format!(
                    "field {name} expected {}",
                    spec.ty.as_str()
                )));
            }
        }
        Ok(())
    }
}

fn validate_field(name: &str, spec: &FieldSpec) -> Result<()> {
    if name.is_empty() {
        return Err(Error::Mapping("field name must not be empty".into()));
    }
    if name.starts_with('_') {
        return Err(Error::Mapping(format!(
            "field name {name} uses the reserved '_' prefix"
        )));
    }
    if spec.ty == FieldType::Text && spec.fast {
        return Err(Error::Mapping(format!("text field {name} cannot be fast")));
    }
    Ok(())
}

fn numeric_opts(spec: &FieldSpec) -> NumericOptions {
    let mut opts = NumericOptions::default();
    if spec.indexed {
        opts = opts.set_indexed();
    }
    if spec.fast {
        opts = opts.set_fast();
    }
    opts
}

fn value_matches(value: &serde_json::Value, ty: FieldType) -> bool {
    match ty {
        FieldType::Text | FieldType::Keyword => value.is_string(),
        FieldType::I64 => value.is_i64(),
        FieldType::F64 => value.is_number(),
        FieldType::Date => value
            .as_str()
            .is_some_and(|s| OffsetDateTime::parse(s, &Rfc3339).is_ok()),
    }
}

#[derive(Serialize, Deserialize)]
struct MappingDto {
    #[serde(default)]
    fields: BTreeMap<String, FieldDto>,
}

#[derive(Serialize, Deserialize)]
struct FieldDto {
    #[serde(rename = "type")]
    ty: FieldType,
    #[serde(default)]
    indexed: bool,
    #[serde(default)]
    fast: bool,
}

impl From<Mapping> for MappingDto {
    fn from(mapping: Mapping) -> Self {
        MappingDto {
            fields: mapping
                .fields
                .into_iter()
                .map(|(name, spec)| {
                    (
                        name,
                        FieldDto {
                            ty: spec.ty,
                            indexed: spec.indexed,
                            fast: spec.fast,
                        },
                    )
                })
                .collect(),
        }
    }
}

impl TryFrom<MappingDto> for Mapping {
    type Error = Error;

    fn try_from(dto: MappingDto) -> Result<Self> {
        let fields = dto
            .fields
            .into_iter()
            .map(|(name, field)| {
                (
                    name,
                    FieldSpec {
                        ty: field.ty,
                        indexed: field.indexed,
                        fast: field.fast,
                    },
                )
            })
            .collect();
        Mapping::new(fields)
    }
}

impl From<FieldType> for proto::FieldType {
    fn from(ty: FieldType) -> Self {
        match ty {
            FieldType::Text => proto::FieldType::Text,
            FieldType::Keyword => proto::FieldType::Keyword,
            FieldType::I64 => proto::FieldType::I64,
            FieldType::F64 => proto::FieldType::F64,
            FieldType::Date => proto::FieldType::Date,
        }
    }
}

impl TryFrom<proto::FieldType> for FieldType {
    type Error = Error;

    fn try_from(ty: proto::FieldType) -> Result<Self> {
        match ty {
            proto::FieldType::Unspecified => Err(Error::Mapping("field type unspecified".into())),
            proto::FieldType::Text => Ok(FieldType::Text),
            proto::FieldType::Keyword => Ok(FieldType::Keyword),
            proto::FieldType::I64 => Ok(FieldType::I64),
            proto::FieldType::F64 => Ok(FieldType::F64),
            proto::FieldType::Date => Ok(FieldType::Date),
        }
    }
}

impl From<Mapping> for proto::Mapping {
    fn from(mapping: Mapping) -> Self {
        proto::Mapping {
            fields: mapping
                .fields
                .into_iter()
                .map(|(name, spec)| proto::Field {
                    name,
                    r#type: proto::FieldType::from(spec.ty) as i32,
                    indexed: spec.indexed,
                    fast: spec.fast,
                })
                .collect(),
        }
    }
}

impl TryFrom<proto::Mapping> for Mapping {
    type Error = Error;

    fn try_from(mapping: proto::Mapping) -> Result<Self> {
        let mut fields = BTreeMap::new();
        for field in mapping.fields {
            let proto_ty = proto::FieldType::try_from(field.r#type).map_err(|_| {
                Error::Mapping(format!(
                    "unknown field type {} for field {}",
                    field.r#type, field.name
                ))
            })?;
            let spec = FieldSpec {
                ty: FieldType::try_from(proto_ty)?,
                indexed: field.indexed,
                fast: field.fast,
            };
            if fields.insert(field.name.clone(), spec).is_some() {
                return Err(Error::Mapping(format!("duplicate field {}", field.name)));
            }
        }
        Mapping::new(fields)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(ty: FieldType, indexed: bool, fast: bool) -> FieldSpec {
        FieldSpec { ty, indexed, fast }
    }

    fn sample() -> Mapping {
        let mut fields = BTreeMap::new();
        fields.insert("title".to_string(), spec(FieldType::Text, true, false));
        fields.insert("tag".to_string(), spec(FieldType::Keyword, true, true));
        fields.insert("year".to_string(), spec(FieldType::I64, true, true));
        fields.insert("score".to_string(), spec(FieldType::F64, false, true));
        fields.insert("created".to_string(), spec(FieldType::Date, true, false));
        fields.insert("note".to_string(), spec(FieldType::Keyword, false, false));
        Mapping::new(fields).unwrap()
    }

    #[test]
    fn round_trips_through_json() {
        let mapping = sample();
        let json = serde_json::to_string(&mapping).unwrap();
        let decoded: Mapping = serde_json::from_str(&json).unwrap();
        assert_eq!(mapping, decoded);
    }

    #[test]
    fn json_field_type_names_are_lowercase() {
        let json = serde_json::to_value(sample()).unwrap();
        assert_eq!(json["fields"]["title"]["type"], "text");
        assert_eq!(json["fields"]["year"]["type"], "i64");
    }

    #[test]
    fn json_roles_default_to_false() {
        let mapping: Mapping =
            serde_json::from_str(r#"{"fields":{"name":{"type":"keyword"}}}"#).unwrap();
        let mut expected = BTreeMap::new();
        expected.insert("name".to_string(), spec(FieldType::Keyword, false, false));
        assert_eq!(mapping, Mapping::new(expected).unwrap());
    }

    #[test]
    fn equality_is_order_independent() {
        let a: Mapping = serde_json::from_str(
            r#"{"fields":{"a":{"type":"i64"},"b":{"type":"text","indexed":true}}}"#,
        )
        .unwrap();
        let b: Mapping = serde_json::from_str(
            r#"{"fields":{"b":{"type":"text","indexed":true},"a":{"type":"i64"}}}"#,
        )
        .unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn round_trips_through_proto() {
        let mapping = sample();
        let wire = proto::Mapping::from(mapping.clone());
        let decoded = Mapping::try_from(wire).unwrap();
        assert_eq!(mapping, decoded);
    }

    #[test]
    fn schema_has_system_fields_and_roles() {
        let schema = sample().to_schema();

        let id = schema.get_field(ID_FIELD).unwrap();
        let id_entry = schema.get_field_entry(id);
        assert!(id_entry.is_indexed());
        assert!(id_entry.is_stored());

        let source = schema.get_field(SOURCE_FIELD).unwrap();
        assert!(schema.get_field_entry(source).is_stored());

        let title = schema.get_field_entry(schema.get_field("title").unwrap());
        assert!(title.is_indexed());
        assert!(!title.is_fast());

        let year = schema.get_field_entry(schema.get_field("year").unwrap());
        assert!(year.is_indexed());
        assert!(year.is_fast());

        let score = schema.get_field_entry(schema.get_field("score").unwrap());
        assert!(!score.is_indexed());
        assert!(score.is_fast());
    }

    #[test]
    fn schema_omits_store_only_fields() {
        let schema = sample().to_schema();
        assert!(schema.get_field("note").is_err());
    }

    #[test]
    fn rejects_reserved_field_name() {
        let mut fields = BTreeMap::new();
        fields.insert("_id".to_string(), spec(FieldType::Keyword, true, false));
        assert!(matches!(Mapping::new(fields), Err(Error::Mapping(_))));
    }

    #[test]
    fn rejects_fast_text_field() {
        let mut fields = BTreeMap::new();
        fields.insert("body".to_string(), spec(FieldType::Text, true, true));
        assert!(matches!(Mapping::new(fields), Err(Error::Mapping(_))));
    }

    #[test]
    fn rejects_unspecified_proto_field_type() {
        let wire = proto::Mapping {
            fields: vec![proto::Field {
                name: "x".to_string(),
                r#type: proto::FieldType::Unspecified as i32,
                indexed: true,
                fast: false,
            }],
        };
        assert!(matches!(Mapping::try_from(wire), Err(Error::Mapping(_))));
    }

    #[test]
    fn rejects_duplicate_proto_field() {
        let field = |ty| proto::Field {
            name: "x".to_string(),
            r#type: proto::FieldType::from(ty) as i32,
            indexed: true,
            fast: false,
        };
        let wire = proto::Mapping {
            fields: vec![field(FieldType::Text), field(FieldType::Keyword)],
        };
        assert!(matches!(Mapping::try_from(wire), Err(Error::Mapping(_))));
    }

    #[test]
    fn rejects_empty_field_name() {
        let mut fields = BTreeMap::new();
        fields.insert(String::new(), spec(FieldType::Keyword, true, false));
        assert!(matches!(Mapping::new(fields), Err(Error::Mapping(_))));
    }

    #[test]
    fn keyword_indexed_uses_raw_tokenizer() {
        use tantivy::schema::FieldType as TantivyType;

        let mut fields = BTreeMap::new();
        fields.insert("tag".to_string(), spec(FieldType::Keyword, true, false));
        let schema = Mapping::new(fields).unwrap().to_schema();
        let entry = schema.get_field_entry(schema.get_field("tag").unwrap());

        assert!(entry.is_indexed());
        assert!(!entry.is_fast());
        match entry.field_type() {
            TantivyType::Str(opts) => {
                assert_eq!(opts.get_indexing_options().unwrap().tokenizer(), "raw");
            }
            other => panic!("expected str field, got {other:?}"),
        }
    }

    #[test]
    fn accepts_valid_document() {
        let mapping = sample();
        let doc = serde_json::json!({
            "title": "hello",
            "year": 2026,
            "score": 1.5,
            "created": "2026-06-20T00:00:00Z",
        });
        assert!(mapping.validate_document(&doc).is_ok());
    }

    #[test]
    fn rejects_invalid_documents() {
        let mapping = sample();
        let cases = [
            ("unmapped field", serde_json::json!({ "missing": "x" })),
            ("wrong type", serde_json::json!({ "year": "not-a-number" })),
            (
                "malformed date",
                serde_json::json!({ "created": "2026-06-20" }),
            ),
            ("non-object", serde_json::json!("not an object")),
        ];
        for (label, doc) in cases {
            assert!(
                matches!(mapping.validate_document(&doc), Err(Error::Validation(_))),
                "{label} should be rejected"
            );
        }
    }

    #[test]
    fn f64_field_accepts_integer() {
        let doc = serde_json::json!({ "score": 3 });
        assert!(sample().validate_document(&doc).is_ok());
    }

    #[test]
    fn allows_missing_fields() {
        let doc = serde_json::json!({ "title": "only this" });
        assert!(sample().validate_document(&doc).is_ok());
    }
}
