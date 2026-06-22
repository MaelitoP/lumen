use std::path::Path;

use lumen_proto::v1 as proto;
use prost::Message;

use crate::error::{Error, Result};
use crate::log::SegmentedLog;

#[derive(Debug)]
pub(crate) struct Wal {
    log: SegmentedLog,
}

impl Wal {
    pub(crate) fn open(dir: &Path) -> Result<(Self, Vec<proto::WalEntry>)> {
        let log = SegmentedLog::open(dir)?;
        let entries = log
            .read_all()?
            .into_iter()
            .map(|(_, bytes)| {
                proto::WalEntry::decode(bytes.as_slice())
                    .map_err(|e| Error::Recovery(format!("corrupt wal record: {e}")))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((Self { log }, entries))
    }

    pub(crate) fn append(&mut self, entry: &proto::WalEntry) -> Result<()> {
        self.log.append(entry.seq, &entry.encode_to_vec())?;
        self.log.sync()
    }

    pub(crate) fn trim(&mut self, floor: u64) -> Result<()> {
        self.log.purge_through(floor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn entry(seq: u64) -> proto::WalEntry {
        proto::WalEntry {
            seq,
            command: Some(proto::Command {
                op: Some(proto::command::Op::DropCollection(proto::DropCollection {
                    collection: format!("c{seq}"),
                })),
            }),
        }
    }

    #[test]
    fn entries_round_trip_after_reopen() {
        let dir = tempdir().unwrap();
        {
            let (mut wal, recovered) = Wal::open(dir.path()).unwrap();
            assert!(recovered.is_empty());
            for seq in 1..=3 {
                wal.append(&entry(seq)).unwrap();
            }
        }
        let (_wal, recovered) = Wal::open(dir.path()).unwrap();
        assert_eq!(recovered, vec![entry(1), entry(2), entry(3)]);
    }
}
