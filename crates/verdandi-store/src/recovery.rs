use std::collections::BTreeMap;
use std::ops::Range;
use std::path::{Path, PathBuf};

use verdandi_core::ids::SegmentId;

use crate::block::{
    BLOCK_HEADER_LEN, Bad, BlockHeader, FILE_HEADER_LEN, FileHeader, PAGE, SEGMENT_BYTES, VERSION,
    align, parse_block,
};
use crate::fs::{Fs, FsFile};
use crate::log::{Error, SealPoint, seal_file};

const TEMP: &str = ".tmp";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Warning {
    pub file: PathBuf,
    pub range: Range<u64>,
}

#[derive(Debug)]
pub struct Recovered {
    pub warnings: Vec<Warning>,
    pub unsealed: Vec<SealPoint>,
    pub next_epoch: u64,
    pub winners: BTreeMap<SegmentId, PathBuf>,
    pub(crate) dir: PathBuf,
}

pub(crate) enum Expect {
    Segment(SegmentId),
    Any,
}

pub(crate) fn expect_for(name: &str) -> Option<Expect> {
    if let Some(id) = name
        .strip_prefix("seg-")
        .and_then(|s| s.strip_suffix(".vdl"))
    {
        return id.parse().ok().map(Expect::Segment);
    }
    cmp_epoch(name).map(|_| Expect::Any)
}

fn cmp_epoch(name: &str) -> Option<u64> {
    let epoch = name.strip_prefix("cmp-")?.strip_suffix(".vdl")?;
    u64::from_str_radix(epoch, 16)
        .ok()
        .filter(|_| epoch.len() == 16)
}

pub(crate) fn temp_name(epoch: u64) -> String {
    format!("cmp-{epoch:016x}{TEMP}")
}

type Held = BTreeMap<SegmentId, (u32, u64)>;

pub fn recover<F: Fs>(
    fs: &F,
    dir: &Path,
    mut visit: impl FnMut(&Path, &BlockHeader, &[u8]),
) -> Result<Recovered, Error> {
    if fs.statfs(dir)?.memory_backed {
        return Err(Error::MemoryBacked(dir.to_path_buf()));
    }
    let mut names = fs.list(dir)?;
    names.sort();
    for name in &names {
        if name.starts_with("cmp-") && name.ends_with(TEMP) {
            fs.remove(&dir.join(name))?;
        }
    }
    let mut out = Recovered {
        warnings: Vec::new(),
        unsealed: Vec::new(),
        next_epoch: 1,
        winners: BTreeMap::new(),
        dir: dir.to_path_buf(),
    };
    let mut files = Vec::new();
    for name in names {
        if let Some(expect) = expect_for(&name) {
            if let Some(epoch) = cmp_epoch(&name) {
                out.next_epoch = out.next_epoch.max(epoch.saturating_add(1));
            }
            let path = dir.join(name);
            let held = recover_file(fs, &path, &expect, &mut out, &mut visit)?;
            files.push((path, held));
        }
    }
    let mut best: BTreeMap<SegmentId, ((u32, u64), &Path)> = BTreeMap::new();
    for (path, held) in &files {
        for (segment, key) in held {
            if best.get(segment).is_none_or(|(k, _)| key > k) {
                best.insert(*segment, (*key, path));
            }
        }
    }
    out.winners = best
        .into_iter()
        .map(|(segment, (_, path))| (segment, path.to_path_buf()))
        .collect();
    for (path, held) in &files {
        let lost = if held.is_empty() {
            !out.warnings.iter().any(|w| w.file == *path)
        } else {
            held.keys().all(|s| out.winners.get(s) != Some(path))
        };
        if lost {
            fs.remove(path)?;
            out.unsealed.retain(|p| p.file != *path);
            out.warnings.retain(|w| w.file != *path);
        }
    }
    fs.sync_dir(dir)?;
    Ok(out)
}

fn check<'a>(
    data: &'a [u8],
    at: u64,
    expect: &Expect,
    epoch: Option<u64>,
) -> Option<(BlockHeader, u16, &'a [u8])> {
    parse_block(data, at).ok().filter(|(header, _, _)| {
        epoch.is_none_or(|e| e == header.epoch)
            && match expect {
                Expect::Segment(s) => *s == header.segment,
                Expect::Any => true,
            }
    })
}

pub(crate) struct Walk<'a> {
    pub(crate) blocks: Vec<(BlockHeader, &'a [u8])>,
    pub(crate) damaged: Vec<Range<u64>>,
    good_end: u64,
    bad_tail: bool,
    max_epoch: u64,
}

pub(crate) fn walk<'a>(data: &'a [u8], expect: &Expect) -> Result<Walk<'a>, u16> {
    let len = data.len() as u64;
    let mut out = Walk {
        blocks: Vec::new(),
        damaged: Vec::new(),
        good_end: len.min(FILE_HEADER_LEN as u64),
        bad_tail: false,
        max_epoch: 0,
    };
    let mut epoch = match FileHeader::decode(data) {
        Ok(header) => Some(header.epoch),
        Err(Bad::UnknownVersion(version)) => return Err(version),
        Err(Bad::Damaged) => {
            if data.iter().take(FILE_HEADER_LEN).any(|b| *b != 0) {
                out.damaged.push(0..FILE_HEADER_LEN as u64);
            }
            None
        }
    };
    out.max_epoch = epoch.unwrap_or(0);
    let mut at = PAGE;
    while at < len {
        if let Some((header, version, body)) = check(data, at, expect, epoch) {
            if version != VERSION {
                return Err(version);
            }
            epoch = Some(header.epoch);
            out.max_epoch = out.max_epoch.max(header.epoch);
            out.good_end = at + (BLOCK_HEADER_LEN + body.len()) as u64;
            out.blocks.push((header, body));
            at = align(out.good_end);
            continue;
        }
        let mut next = None;
        let mut candidate = at + PAGE;
        while candidate < len {
            if check(data, candidate, expect, epoch).is_some_and(|(_, v, _)| v == VERSION) {
                next = Some(candidate);
                break;
            }
            candidate += PAGE;
        }
        match next {
            Some(next) => {
                out.damaged.push(at..next);
                at = next;
            }
            None => {
                if let Some(end) = nonzero_end(&data[at as usize..]) {
                    out.damaged.push(at..at + end as u64);
                    out.bad_tail = true;
                }
                break;
            }
        }
    }
    Ok(out)
}

fn recover_file<F: Fs>(
    fs: &F,
    path: &Path,
    expect: &Expect,
    out: &mut Recovered,
    visit: &mut impl FnMut(&Path, &BlockHeader, &[u8]),
) -> Result<Held, Error> {
    let file = fs.open(path)?;
    let len = file.size()?;
    if len > SEGMENT_BYTES {
        return Err(Error::Oversize {
            path: path.to_path_buf(),
            len,
        });
    }
    let mut data = vec![0; len as usize];
    file.read_at(&mut data, 0)?;
    let walk = walk(&data, expect).map_err(|version| Error::UnknownVersion {
        path: path.to_path_buf(),
        version,
    })?;
    let mut held = Held::new();
    for (header, body) in &walk.blocks {
        visit(path, header, body);
        let key = held.entry(header.segment).or_default();
        *key = (*key).max((header.compaction_version, header.epoch));
    }
    out.warnings
        .extend(walk.damaged.iter().map(|range| Warning {
            file: path.to_path_buf(),
            range: range.clone(),
        }));
    out.next_epoch = out.next_epoch.max(walk.max_epoch + 1);
    if walk.bad_tail {
        out.unsealed.push(SealPoint {
            file: path.to_path_buf(),
            end: walk.good_end,
        });
        file.sync_data()?;
    } else if len > walk.good_end {
        seal_file(&file, walk.good_end)?;
    } else {
        file.sync_data()?;
    }
    Ok(held)
}

fn nonzero_end(data: &[u8]) -> Option<usize> {
    let zero = [0; PAGE as usize];
    let chunk = data
        .rchunks(PAGE as usize)
        .position(|c| c != &zero[..c.len()])?;
    let end = data.len() - chunk * PAGE as usize;
    let start = end.saturating_sub(PAGE as usize);
    let last = data[start..end].iter().rposition(|b| *b != 0)?;
    Some(start + last + 1)
}
