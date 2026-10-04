use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

use verdandi_core::ids::{Address, MessageId, SegmentId};

use crate::fs::FsFile;
use crate::records::{self, Message};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Run {
    pub stream: String,
    pub offset: u64,
    pub len: u32,
    pub count: u32,
    pub crc: u32,
    pub first_id: MessageId,
    pub last_id: MessageId,
}

#[derive(Clone)]
pub struct Location {
    pub file: Arc<dyn FsFile>,
    pub start: Address,
    pub run: Run,
}

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("the run CRC does not match")]
    Damaged,
    #[error(transparent)]
    Io(#[from] io::Error),
}

struct Segment {
    file: Arc<dyn FsFile>,
    runs: BTreeMap<u32, Run>,
}

#[derive(Default)]
pub struct Index {
    segments: BTreeMap<SegmentId, Segment>,
}

impl Index {
    pub fn has(&self, segment: SegmentId) -> bool {
        self.segments.contains_key(&segment)
    }

    pub fn add(&mut self, segment: SegmentId, file: Arc<dyn FsFile>) {
        self.segments.entry(segment).or_insert(Segment {
            file,
            runs: BTreeMap::new(),
        });
    }

    pub fn insert(&mut self, start: Address, run: Run) {
        if let Some(segment) = self.segments.get_mut(&start.segment) {
            segment.runs.insert(start.position, run);
        }
    }

    pub fn runs(&self, segment: SegmentId) -> impl Iterator<Item = (u32, &Run)> {
        self.segments
            .get(&segment)
            .into_iter()
            .flat_map(|s| s.runs.iter().map(|(start, run)| (*start, run)))
    }

    pub fn replace(&mut self, segment: SegmentId, file: Arc<dyn FsFile>, runs: BTreeMap<u32, Run>) {
        self.segments.insert(segment, Segment { file, runs });
    }

    pub fn drop_segment(&mut self, segment: SegmentId) {
        self.segments.remove(&segment);
    }

    pub fn remove(&mut self, start: Address) -> Option<Run> {
        self.segments
            .get_mut(&start.segment)?
            .runs
            .remove(&start.position)
    }

    pub fn locate(&self, at: Address) -> Option<Location> {
        let segment = self.segments.get(&at.segment)?;
        let (first, run) = segment.runs.range(..=at.position).next_back()?;
        (at.position - first < run.count).then(|| Location {
            file: Arc::clone(&segment.file),
            start: Address {
                segment: at.segment,
                position: *first,
            },
            run: run.clone(),
        })
    }
}

pub fn read(location: &Location) -> Result<Vec<Message>, ReadError> {
    let mut bytes = vec![0; location.run.len as usize];
    location.file.read_at(&mut bytes, location.run.offset)?;
    if crc32c::crc32c(&bytes) != location.run.crc {
        return Err(ReadError::Damaged);
    }
    records::decode_run(&bytes, location.run.count).map_err(|_| ReadError::Damaged)
}
