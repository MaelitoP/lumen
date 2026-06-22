use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use lumen_proto::v1 as proto;
use prost::Message;

use crate::error::{Error, Result};
use crate::sync::fsync;

const SEGMENT_PREFIX: &str = "wal-";
const SEGMENT_SUFFIX: &str = ".log";
const DEFAULT_SEGMENT_MAX_BYTES: u64 = 64 * 1024 * 1024;
const HEADER_BYTES: usize = 8;

#[derive(Debug)]
pub(crate) struct Wal {
    dir: PathBuf,
    segment_max_bytes: u64,
    segment: Option<Segment>,
}

#[derive(Debug)]
struct Segment {
    file: File,
    start_seq: u64,
    bytes: u64,
}

impl Wal {
    pub(crate) fn open(dir: &Path) -> Result<(Self, Vec<proto::WalEntry>)> {
        Self::open_with_segment_max(dir, DEFAULT_SEGMENT_MAX_BYTES)
    }

    pub(crate) fn open_with_segment_max(
        dir: &Path,
        segment_max_bytes: u64,
    ) -> Result<(Self, Vec<proto::WalEntry>)> {
        fs::create_dir_all(dir)?;

        let mut entries = Vec::new();
        let mut active = None;
        for (start_seq, path) in segment_paths(dir)? {
            let bytes = fs::read(&path)?;
            let (segment_entries, valid_len) = parse_frames(&bytes);
            entries.extend(segment_entries);

            let file = OpenOptions::new().append(true).open(&path)?;
            let torn = valid_len < bytes.len();
            if torn {
                file.set_len(valid_len as u64)?;
                fsync(&file)?;
            }
            active = Some(Segment {
                file,
                start_seq,
                bytes: valid_len as u64,
            });
            if torn {
                break;
            }
        }

        Ok((
            Self {
                dir: dir.to_path_buf(),
                segment_max_bytes,
                segment: active,
            },
            entries,
        ))
    }

    pub(crate) fn append(&mut self, entry: &proto::WalEntry) -> Result<()> {
        let payload = entry.encode_to_vec();
        let len = u32::try_from(payload.len())
            .map_err(|_| Error::Recovery("wal record exceeds u32 length".into()))?;
        let crc = crc32c::crc32c(&payload);

        if self.needs_rotation() {
            self.rotate(entry.seq)?;
        }
        let segment = self
            .segment
            .as_mut()
            .expect("segment present after rotation");

        let mut frame = Vec::with_capacity(HEADER_BYTES + payload.len());
        frame.extend_from_slice(&len.to_le_bytes());
        frame.extend_from_slice(&crc.to_le_bytes());
        frame.extend_from_slice(&payload);

        segment.file.write_all(&frame)?;
        fsync(&segment.file)?;
        segment.bytes += frame.len() as u64;
        Ok(())
    }

    pub(crate) fn trim(&self, floor: u64) -> Result<()> {
        let active_start = self.segment.as_ref().map(|s| s.start_seq);
        let paths = segment_paths(&self.dir)?;
        for index in 0..paths.len() {
            let (start, path) = &paths[index];
            if Some(*start) == active_start {
                continue;
            }
            let max_seq = match paths.get(index + 1) {
                Some((next_start, _)) => next_start - 1,
                None => continue,
            };
            if max_seq <= floor {
                fs::remove_file(path)?;
            }
        }
        Ok(())
    }

    fn needs_rotation(&self) -> bool {
        match &self.segment {
            None => true,
            Some(segment) => segment.bytes >= self.segment_max_bytes,
        }
    }

    fn rotate(&mut self, start_seq: u64) -> Result<()> {
        let path = self
            .dir
            .join(format!("{SEGMENT_PREFIX}{start_seq}{SEGMENT_SUFFIX}"));
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        self.segment = Some(Segment {
            file,
            start_seq,
            bytes: 0,
        });
        Ok(())
    }
}

fn parse_frames(bytes: &[u8]) -> (Vec<proto::WalEntry>, usize) {
    let mut entries = Vec::new();
    let mut offset = 0;
    while offset + HEADER_BYTES <= bytes.len() {
        let len =
            u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("4 bytes")) as usize;
        let crc = u32::from_le_bytes(
            bytes[offset + 4..offset + HEADER_BYTES]
                .try_into()
                .expect("4 bytes"),
        );
        let body = offset + HEADER_BYTES;
        if body + len > bytes.len() {
            break;
        }
        let payload = &bytes[body..body + len];
        if crc32c::crc32c(payload) != crc {
            break;
        }
        match proto::WalEntry::decode(payload) {
            Ok(entry) => entries.push(entry),
            Err(_) => break,
        }
        offset = body + len;
    }
    (entries, offset)
}

fn segment_paths(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(seq) = name
            .strip_prefix(SEGMENT_PREFIX)
            .and_then(|rest| rest.strip_suffix(SEGMENT_SUFFIX))
            .and_then(|seq| seq.parse::<u64>().ok())
        {
            paths.push((seq, entry.path()));
        }
    }
    paths.sort_by_key(|(seq, _)| *seq);
    Ok(paths)
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

    fn seqs(entries: &[proto::WalEntry]) -> Vec<u64> {
        entries.iter().map(|e| e.seq).collect()
    }

    #[test]
    fn appends_round_trip_after_reopen() {
        let dir = tempdir().unwrap();
        {
            let (mut wal, recovered) = Wal::open(dir.path()).unwrap();
            assert!(recovered.is_empty());
            for seq in 1..=3 {
                wal.append(&entry(seq)).unwrap();
            }
        }
        let (_wal, recovered) = Wal::open(dir.path()).unwrap();
        assert_eq!(seqs(&recovered), vec![1, 2, 3]);
    }

    #[test]
    fn torn_tail_is_dropped_and_truncated() {
        let dir = tempdir().unwrap();
        {
            let (mut wal, _) = Wal::open(dir.path()).unwrap();
            wal.append(&entry(1)).unwrap();
            wal.append(&entry(2)).unwrap();
        }
        let (seg_seq, path) = segment_paths(dir.path()).unwrap().pop().unwrap();
        assert_eq!(seg_seq, 1);
        let original = fs::metadata(&path).unwrap().len();
        let mut bytes = fs::read(&path).unwrap();
        bytes.extend_from_slice(&[7, 0, 0, 0, 1, 2, 3, 4, 9, 9]);
        fs::write(&path, &bytes).unwrap();

        let (_wal, recovered) = Wal::open(dir.path()).unwrap();
        assert_eq!(seqs(&recovered), vec![1, 2]);
        assert_eq!(fs::metadata(&path).unwrap().len(), original);
    }

    #[test]
    fn rotates_and_trims_below_floor() {
        let dir = tempdir().unwrap();
        let (mut wal, _) = Wal::open_with_segment_max(dir.path(), 1).unwrap();
        for seq in 1..=3 {
            wal.append(&entry(seq)).unwrap();
        }
        assert_eq!(segment_paths(dir.path()).unwrap().len(), 3);

        wal.trim(2).unwrap();
        let remaining: Vec<u64> = segment_paths(dir.path())
            .unwrap()
            .into_iter()
            .map(|(seq, _)| seq)
            .collect();
        assert_eq!(remaining, vec![3]);

        let (_wal, recovered) = Wal::open(dir.path()).unwrap();
        assert_eq!(seqs(&recovered), vec![3]);
    }
}
