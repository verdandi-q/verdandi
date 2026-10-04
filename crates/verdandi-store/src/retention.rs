use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use verdandi_core::Core;
use verdandi_core::ids::{Address, SegmentId};

use crate::compaction::Input;
use crate::engine::KEY_MS;
use crate::records::Control;

pub const SMALL_BYTES: u64 = 8 << 20;
pub const SMALL_COUNT: usize = 16;
pub const RESCAN_MS: u64 = 24 * 3600 * 1000;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Holding {
    pub groups: Vec<String>,
    pub streams: Vec<String>,
}

pub(crate) struct FileInfo {
    pub(crate) len: u64,
    pub(crate) bodies: u64,
    pub(crate) carried: u64,
    pub(crate) first_ms: u64,
    pub(crate) sealed: bool,
    pub(crate) segments: BTreeSet<SegmentId>,
}

pub(crate) fn carries(
    core: &Core,
    control: &Control,
    commit_ms: u64,
    now: u64,
    gone: &dyn Fn(Address) -> bool,
) -> bool {
    match control {
        Control::Record(record) => core.carries(record, commit_ms, now, gone),
        Control::Key { .. } => commit_ms + KEY_MS > now,
        Control::Damage(d) => core.held(d.segment, gone),
    }
}

pub(crate) fn select(
    files: &BTreeMap<PathBuf, FileInfo>,
    owner: &BTreeMap<SegmentId, PathBuf>,
    pressure: bool,
    rescan: bool,
    mut runs: impl FnMut(SegmentId) -> (u64, u64),
) -> Vec<Input> {
    let mut owned: BTreeMap<&PathBuf, BTreeSet<SegmentId>> = BTreeMap::new();
    for (segment, path) in owner {
        owned.entry(path).or_default().insert(*segment);
    }
    let mut copies: BTreeMap<SegmentId, usize> = BTreeMap::new();
    for segment in files.values().flat_map(|f| &f.segments) {
        *copies.entry(*segment).or_default() += 1;
    }
    let sealed: Vec<(&PathBuf, &FileInfo)> = files.iter().filter(|(_, f)| f.sealed).collect();
    let small = sealed.iter().filter(|(_, f)| f.len < SMALL_BYTES).count() >= SMALL_COUNT;
    let mut inputs: Vec<(u64, Input)> = Vec::new();
    for (path, info) in sealed {
        let segments = owned.remove(path).unwrap_or_default();
        let (all, held) = segments
            .iter()
            .map(|s| runs(*s))
            .fold((0, 0), |(a, h), (all, held)| (a + all, h + held));
        let removable = info.bodies.saturating_sub(info.carried + held);
        let normal = segments.is_empty()
            || !info.segments.is_subset(&segments)
            || removable * 2 >= info.bodies
            || (pressure && all > held)
            || (small && info.len < SMALL_BYTES);
        let recheck = !normal && (pressure || rescan) && info.carried > 0;
        if normal || recheck {
            let pinned = segments
                .iter()
                .filter(|s| copies.get(s).is_some_and(|n| *n > 1))
                .copied()
                .collect();
            let input = Input {
                path: path.clone(),
                segments,
                pinned,
                recheck,
            };
            inputs.push((info.first_ms, input));
        }
    }
    inputs.sort_by_key(|(first_ms, _)| *first_ms);
    inputs.into_iter().map(|(_, input)| input).collect()
}
