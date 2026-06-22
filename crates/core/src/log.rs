use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::sync::fsync;

const SEGMENT_PREFIX: &str = "seg-";
const SEGMENT_SUFFIX: &str = ".log";
const DEFAULT_SEGMENT_MAX_BYTES: u64 = 64 * 1024 * 1024;
const HEADER_BYTES: u64 = 8;

#[derive(Debug, Clone, Copy)]
struct RecordLoc {
    segment_start: u64,
    payload_offset: u64,
    len: u32,
}

#[derive(Debug)]
struct Active {
    start: u64,
    file: File,
    bytes: u64,
}

/// Append-only log of byte records keyed by a contiguous `u64` index.
///
/// Each record is stored as `[len: u32][crc32c: u32][payload]`. This type owns
/// record framing, torn-tail recovery, segment rotation, suffix truncation, and
/// prefix purge.
///
/// The payload is not decoded here. Higher layers, such as [`crate::wal::Wal`]
/// or a Raft log store, own the payload format and the durable purge floor.
#[derive(Debug)]
pub struct SegmentedLog {
    dir: PathBuf,
    segment_max_bytes: u64,
    starts: Vec<u64>,
    records: BTreeMap<u64, RecordLoc>,
    active: Option<Active>,
}

impl SegmentedLog {
    pub fn open(dir: &Path) -> Result<Self> {
        Self::open_with_segment_max(dir, DEFAULT_SEGMENT_MAX_BYTES)
    }

    pub fn open_with_segment_max(dir: &Path, segment_max_bytes: u64) -> Result<Self> {
        fs::create_dir_all(dir)?;

        let mut starts = segment_starts(dir)?;
        let mut records = BTreeMap::new();
        let mut active = None;
        let mut torn_at = None;

        for (i, &start) in starts.iter().enumerate() {
            let path = segment_path(dir, start);
            let bytes = fs::read(&path)?;
            let (locs, valid_len) = scan_segment(start, &bytes);
            for (index, payload_offset, len) in locs {
                records.insert(
                    index,
                    RecordLoc {
                        segment_start: start,
                        payload_offset,
                        len,
                    },
                );
            }

            let torn = valid_len < bytes.len();
            let is_last = i + 1 == starts.len();
            if torn {
                let file = OpenOptions::new().append(true).open(&path)?;
                file.set_len(valid_len as u64)?;
                fsync(&file)?;
                active = Some(Active {
                    start,
                    file,
                    bytes: valid_len as u64,
                });
                torn_at = Some(i);
                break;
            }
            if is_last {
                let file = OpenOptions::new().append(true).open(&path)?;
                active = Some(Active {
                    start,
                    file,
                    bytes: bytes.len() as u64,
                });
            }
        }

        if let Some(i) = torn_at {
            starts.truncate(i + 1);
        }

        Ok(Self {
            dir: dir.to_path_buf(),
            segment_max_bytes,
            starts,
            records,
            active,
        })
    }

    /// Writes `payload` at `index`.
    ///
    /// The record can be read back after this returns, but it is not durable
    /// until [`Self::sync`] succeeds. `index` must be `last_index() + 1`, or any
    /// value when the log is empty.
    pub fn append(&mut self, index: u64, payload: &[u8]) -> Result<()> {
        debug_assert!(
            match self.last_index() {
                None => true,
                Some(last) => index == last + 1,
            },
            "segmented log append must be contiguous"
        );

        let len = u32::try_from(payload.len())
            .map_err(|_| Error::Recovery("log record exceeds u32 length".into()))?;
        let crc = crc32c::crc32c(payload);

        if self.needs_rotation() {
            self.rotate(index)?;
        }
        let active = self.active.as_mut().expect("active present after rotation");
        let payload_offset = active.bytes + HEADER_BYTES;

        let mut frame = Vec::with_capacity(HEADER_BYTES as usize + payload.len());
        frame.extend_from_slice(&len.to_le_bytes());
        frame.extend_from_slice(&crc.to_le_bytes());
        frame.extend_from_slice(payload);
        active.file.write_all(&frame)?;
        active.bytes += frame.len() as u64;

        self.records.insert(
            index,
            RecordLoc {
                segment_start: active.start,
                payload_offset,
                len,
            },
        );
        Ok(())
    }

    /// Syncs the active segment to disk.
    ///
    /// All earlier [`Self::append`] calls become durable once this succeeds.
    pub fn sync(&self) -> Result<()> {
        if let Some(active) = &self.active {
            fsync(&active.file)?;
        }
        Ok(())
    }

    pub fn read_all(&self) -> Result<Vec<(u64, Vec<u8>)>> {
        self.read_locs(self.records.iter())
    }

    /// Reads records with indexes in `[from, to)`, in index order.
    ///
    /// Records from a purged prefix are not returned.
    pub fn read_range(&self, from: u64, to: u64) -> Result<Vec<(u64, Vec<u8>)>> {
        self.read_locs(self.records.range(from..to))
    }

    /// Drops every record with index `>= index`.
    ///
    /// The segment containing `index` is truncated. Segments fully above `index`
    /// are deleted.
    pub fn truncate_from(&mut self, index: u64) -> Result<()> {
        let cut = self
            .records
            .get(&index)
            .map(|loc| (loc.segment_start, loc.payload_offset - HEADER_BYTES));
        let to_delete: Vec<u64> = self
            .starts
            .iter()
            .copied()
            .filter(|&s| s >= index)
            .collect();

        self.records.retain(|&k, _| k < index);

        if let Some((segment_start, frame_start)) = cut {
            if segment_start < index {
                let file = OpenOptions::new()
                    .write(true)
                    .open(segment_path(&self.dir, segment_start))?;
                file.set_len(frame_start)?;
                fsync(&file)?;
            }
        }
        for start in &to_delete {
            fs::remove_file(segment_path(&self.dir, *start))?;
        }
        self.starts.retain(|s| !to_delete.contains(s));
        self.reopen_active()
    }

    /// Drops every record with index `<= index`.
    ///
    /// Inactive segments that are fully covered are deleted. If only part of a
    /// segment is covered, the file stays on disk but those records are removed
    /// from the in-memory index.
    ///
    /// The purge floor is not persisted here. The owner must apply it again after
    /// [`Self::open`].
    pub fn purge_through(&mut self, index: u64) -> Result<()> {
        self.records.retain(|&k, _| k > index);

        let active_start = self.active.as_ref().map(|a| a.start);
        for i in 0..self.starts.len() {
            let start = self.starts[i];
            if Some(start) == active_start {
                continue;
            }
            let max = match self.starts.get(i + 1) {
                Some(&next) => next - 1,
                None => continue,
            };
            if max <= index {
                fs::remove_file(segment_path(&self.dir, start))?;
            }
        }
        self.starts.retain(|&s| {
            Some(s) == active_start || self.records.values().any(|l| l.segment_start == s)
        });
        Ok(())
    }

    pub fn last_index(&self) -> Option<u64> {
        self.records.keys().next_back().copied()
    }

    fn read_locs<'a>(
        &self,
        locs: impl Iterator<Item = (&'a u64, &'a RecordLoc)>,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let mut handles: HashMap<u64, File> = HashMap::new();
        let mut out = Vec::new();
        for (&index, loc) in locs {
            let file = match handles.entry(loc.segment_start) {
                Entry::Occupied(e) => e.into_mut(),
                Entry::Vacant(e) => {
                    e.insert(File::open(segment_path(&self.dir, loc.segment_start))?)
                }
            };
            let mut buf = vec![0u8; loc.len as usize];
            file.read_exact_at(&mut buf, loc.payload_offset)?;
            out.push((index, buf));
        }
        Ok(out)
    }

    fn needs_rotation(&self) -> bool {
        match &self.active {
            None => true,
            Some(active) => active.bytes >= self.segment_max_bytes,
        }
    }

    fn rotate(&mut self, start: u64) -> Result<()> {
        if let Some(active) = &self.active {
            fsync(&active.file)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(segment_path(&self.dir, start))?;
        self.active = Some(Active {
            start,
            file,
            bytes: 0,
        });
        if !self.starts.contains(&start) {
            self.starts.push(start);
            self.starts.sort_unstable();
        }
        Ok(())
    }

    fn reopen_active(&mut self) -> Result<()> {
        match self.starts.last().copied() {
            Some(start) => {
                let path = segment_path(&self.dir, start);
                let bytes = fs::metadata(&path)?.len();
                let file = OpenOptions::new().append(true).open(&path)?;
                self.active = Some(Active { start, file, bytes });
            }
            None => self.active = None,
        }
        Ok(())
    }
}

fn scan_segment(start: u64, bytes: &[u8]) -> (Vec<(u64, u64, u32)>, usize) {
    let mut locs = Vec::new();
    let mut offset = 0usize;
    let mut index = start;
    while offset + HEADER_BYTES as usize <= bytes.len() {
        let len =
            u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("4 bytes")) as usize;
        let crc = u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().expect("4 bytes"));
        let body = offset + HEADER_BYTES as usize;
        if body + len > bytes.len() {
            break;
        }
        let payload = &bytes[body..body + len];
        if crc32c::crc32c(payload) != crc {
            break;
        }
        locs.push((index, body as u64, len as u32));
        offset = body + len;
        index += 1;
    }
    (locs, offset)
}

fn segment_starts(dir: &Path) -> Result<Vec<u64>> {
    let mut starts = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(start) = name
            .strip_prefix(SEGMENT_PREFIX)
            .and_then(|rest| rest.strip_suffix(SEGMENT_SUFFIX))
            .and_then(|start| start.parse::<u64>().ok())
        {
            starts.push(start);
        }
    }
    starts.sort_unstable();
    Ok(starts)
}

fn segment_path(dir: &Path, start: u64) -> PathBuf {
    dir.join(format!("{SEGMENT_PREFIX}{start}{SEGMENT_SUFFIX}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn indices(records: &[(u64, Vec<u8>)]) -> Vec<u64> {
        records.iter().map(|(i, _)| *i).collect()
    }

    #[test]
    fn appends_round_trip_after_reopen() {
        let dir = tempdir().unwrap();
        {
            let mut log = SegmentedLog::open(dir.path()).unwrap();
            for i in 1..=3 {
                log.append(i, format!("r{i}").as_bytes()).unwrap();
            }
            log.sync().unwrap();
        }
        let log = SegmentedLog::open(dir.path()).unwrap();
        let all = log.read_all().unwrap();
        assert_eq!(indices(&all), vec![1, 2, 3]);
        assert_eq!(all[1].1, b"r2");
    }

    #[test]
    fn torn_tail_is_dropped_and_truncated() {
        let dir = tempdir().unwrap();
        {
            let mut log = SegmentedLog::open(dir.path()).unwrap();
            log.append(1, b"one").unwrap();
            log.append(2, b"two").unwrap();
            log.sync().unwrap();
        }
        let path = segment_path(dir.path(), 1);
        let original = fs::metadata(&path).unwrap().len();
        let mut bytes = fs::read(&path).unwrap();
        bytes.extend_from_slice(&[7, 0, 0, 0, 1, 2, 3, 4, 9, 9]);
        fs::write(&path, &bytes).unwrap();

        let log = SegmentedLog::open(dir.path()).unwrap();
        assert_eq!(indices(&log.read_all().unwrap()), vec![1, 2]);
        assert_eq!(fs::metadata(&path).unwrap().len(), original);
    }

    #[test]
    fn rotates_and_purges_below_floor() {
        let dir = tempdir().unwrap();
        let mut log = SegmentedLog::open_with_segment_max(dir.path(), 1).unwrap();
        for i in 1..=3 {
            log.append(i, b"x").unwrap();
            log.sync().unwrap();
        }
        assert_eq!(segment_starts(dir.path()).unwrap().len(), 3);

        log.purge_through(2).unwrap();
        assert_eq!(segment_starts(dir.path()).unwrap(), vec![3]);
        assert_eq!(indices(&log.read_all().unwrap()), vec![3]);

        let reopened = SegmentedLog::open(dir.path()).unwrap();
        assert_eq!(indices(&reopened.read_all().unwrap()), vec![3]);
    }

    #[test]
    fn purge_within_a_segment_filters_reads_without_deleting() {
        let dir = tempdir().unwrap();
        let mut log = SegmentedLog::open(dir.path()).unwrap();
        for i in 0..=10 {
            log.append(i, format!("{i}").as_bytes()).unwrap();
        }
        log.sync().unwrap();

        log.purge_through(5).unwrap();
        assert_eq!(
            indices(&log.read_range(0, 100).unwrap()),
            vec![6, 7, 8, 9, 10]
        );
        assert_eq!(log.last_index(), Some(10));
        assert_eq!(segment_starts(dir.path()).unwrap(), vec![0]);
    }

    #[test]
    fn truncate_from_drops_suffix_and_allows_reappend() {
        let dir = tempdir().unwrap();
        let mut log = SegmentedLog::open(dir.path()).unwrap();
        for i in 0..=5 {
            log.append(i, format!("a{i}").as_bytes()).unwrap();
        }
        log.sync().unwrap();

        log.truncate_from(3).unwrap();
        assert_eq!(indices(&log.read_all().unwrap()), vec![0, 1, 2]);

        log.append(3, b"b3").unwrap();
        log.sync().unwrap();
        let all = log.read_all().unwrap();
        assert_eq!(indices(&all), vec![0, 1, 2, 3]);
        assert_eq!(all[3].1, b"b3");

        let reopened = SegmentedLog::open(dir.path()).unwrap();
        assert_eq!(reopened.read_all().unwrap(), all);
    }

    #[test]
    fn truncate_from_above_last_is_a_noop_and_from_zero_clears() {
        let dir = tempdir().unwrap();
        let mut log = SegmentedLog::open(dir.path()).unwrap();
        for i in 0..=4 {
            log.append(i, b"v").unwrap();
        }
        log.sync().unwrap();

        log.truncate_from(11).unwrap();
        assert_eq!(log.read_all().unwrap().len(), 5);

        log.truncate_from(0).unwrap();
        assert_eq!(log.read_all().unwrap().len(), 0);
        assert_eq!(log.last_index(), None);
    }
}
