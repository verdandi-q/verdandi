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
pub const SWEEP_MS: u64 = 3600 * 1000;

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
    aged: Option<u64>,
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
    let mut inputs: Vec<(bool, u64, Input)> = Vec::new();
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
        let swept = aged.is_some_and(|before| info.first_ms <= before) && all > held;
        let recheck = !normal && ((pressure || rescan) && info.carried > 0 || swept);
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
            inputs.push((held > 0 || info.carried > 0, info.first_ms, input));
        }
    }
    inputs.sort_by_key(|(output, first_ms, _)| (*output, *first_ms));
    inputs.into_iter().map(|(_, _, input)| input).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pass_puts_the_inputs_that_need_no_output_first_and_keeps_oldest_first_inside_each_kind() {
        let file = |name: &str, seq: u32, first_ms: u64| {
            let segment = SegmentId {
                incarnation: 1,
                seq,
            };
            let info = FileInfo {
                len: 100,
                bodies: 100,
                carried: 0,
                first_ms,
                sealed: true,
                segments: BTreeSet::from([segment]),
            };
            ((PathBuf::from(name), info), (segment, PathBuf::from(name)))
        };
        let (files, owner): (BTreeMap<_, _>, BTreeMap<_, _>) = [
            file("held-old", 0, 1),
            file("plain-old", 1, 2),
            file("held-new", 2, 3),
            file("plain-new", 3, 4),
        ]
        .into_iter()
        .unzip();
        let inputs = select(&files, &owner, false, false, None, |segment| {
            (100, if segment.seq % 2 == 0 { 10 } else { 0 })
        });
        let order: Vec<&str> = inputs
            .iter()
            .map(|i| i.path.to_str().unwrap_or_default())
            .collect();
        assert_eq!(order, ["plain-old", "plain-new", "held-old", "held-new"]);
    }
}
