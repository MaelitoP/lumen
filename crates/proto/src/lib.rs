pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/lumen.v1.rs"));
}

#[cfg(test)]
mod tests {
    use super::v1::*;
    use prost::Message;

    #[test]
    fn command_round_trips_through_bytes() {
        let entry = WalEntry {
            seq: 7,
            command: Some(Command {
                op: Some(command::Op::CreateCollection(CreateCollection {
                    collection: "books".to_string(),
                    uuid: "1a2b".to_string(),
                    mapping: Some(Mapping {
                        fields: vec![Field {
                            name: "title".to_string(),
                            r#type: FieldType::Text as i32,
                            indexed: true,
                            fast: false,
                        }],
                    }),
                })),
            }),
        };

        let bytes = entry.encode_to_vec();
        let decoded = WalEntry::decode(bytes.as_slice()).expect("decode");

        assert_eq!(entry, decoded);
    }
}
