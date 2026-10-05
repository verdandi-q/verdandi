use super::controller::REPAIR_BYTES_PER_S;
use std::cmp::Reverse;

use super::fnv;
use super::placement::{Groups, HolderSet, mix, pick};

pub const REPAIR_STEP: u64 = 8 << 20;
pub const DIGEST_MS: u64 = 5_000;

#[derive(Clone, Copy, Debug)]
pub struct Limiter {
    milli: u64,
    at_ms: u64,
}

impl Limiter {
    pub fn new(now_ms: u64) -> Self {
        Self {
            milli: 0,
            at_ms: now_ms,
        }
    }

    pub fn available(&mut self, now_ms: u64) -> u64 {
        let gained = REPAIR_BYTES_PER_S.saturating_mul(now_ms.saturating_sub(self.at_ms));
        self.milli = self
            .milli
            .saturating_add(gained)
            .min(REPAIR_BYTES_PER_S * 1000);
        self.at_ms = self.at_ms.max(now_ms);
        (self.milli / 1000).min(REPAIR_STEP)
    }

    pub fn spend(&mut self, bytes: u64) {
        self.milli = self.milli.saturating_sub(bytes.saturating_mul(1000));
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Held<'a> {
    pub writer: u64,
    pub bytes: u64,
    pub sealed: bool,
    pub set: &'a HolderSet,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Want {
    pub target: usize,
    pub cold: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    Copy { need: usize, to: Vec<u64> },
    Drop(HolderSet),
    Idle,
}

pub fn sender(set: &HolderSet, live: impl Fn(u64) -> bool) -> Option<u64> {
    pick(&[set.segment], set.holders.iter().copied().filter(|h| live(*h)))
}

fn ranked(segment: u64, mut disks: Vec<u64>) -> Vec<u64> {
    disks.sort_unstable();
    disks.dedup();
    disks.sort_by_cached_key(|d| Reverse(mix(fnv([segment, *d]))));
    disks
}

fn others(groups: &Groups, segment: u64, skip: impl Fn(u64) -> bool) -> Vec<u64> {
    let all = groups.groups.iter().flatten().copied().filter(|d| !skip(*d));
    ranked(segment, all.collect())
}

pub fn destination(held: &Held, groups: &Groups, live: impl Fn(u64) -> bool + Copy, target: usize) -> Vec<u64> {
    let segment = held.set.segment;
    let mut dest = groups.destination(segment, held.writer, live).to_vec();
    let extra = target.saturating_sub(dest.len());
    if extra > 0 {
        let group = dest.clone();
        dest.extend(
            others(groups, segment, |d| group.contains(&d) || !live(d))
                .into_iter()
                .take(extra),
        );
    }
    dest
}

pub fn plan(
    held: &Held,
    groups: &Groups,
    live: impl Fn(u64) -> bool + Copy,
    complete: impl Fn(u64, u64) -> bool,
    want: Want,
) -> Step {
    let set = held.set;
    let has = |m: u64| set.holders.contains(&m) && complete(set.segment, m);
    let dest = destination(held, groups, live, want.target);
    let copies = set.holders.iter().filter(|h| live(**h) && has(**h)).count();
    let mut to: Vec<u64> = dest
        .iter()
        .copied()
        .filter(|m| live(*m) && !has(*m))
        .collect();
    let deficit = want.target.saturating_sub(copies);
    let need = to.len().max(deficit);
    if deficit > 0 {
        to.extend(others(groups, set.segment, |d| {
            dest.contains(&d) || !live(d) || has(d)
        }));
    }
    if need > 0 && !to.is_empty() {
        return Step::Copy {
            need: need.min(to.len()),
            to,
        };
    }
    let keep = if want.cold {
        want.target.max(copies)
    } else {
        want.target
    };
    let whole = !dest.is_empty() && dest.iter().all(|m| live(*m) && has(*m));
    let extra = set.holders.iter().any(|h| !dest.contains(h));
    if held.sealed && whole && extra && dest.len() >= keep {
        Step::Drop(set.with(dest.into_iter().collect()))
    } else {
        Step::Idle
    }
}

pub fn self_drain(
    me: u64,
    held: &[Held],
    next: &Groups,
    serving: impl Fn(u64) -> bool + Copy,
    complete: impl Fn(u64, u64) -> bool,
    run_ms: u64,
    deadline_ms: u64,
) -> Vec<(u64, Vec<u64>)> {
    if run_ms >= deadline_ms {
        return Vec::new();
    }
    let mut planned: Vec<(usize, u64, Vec<u64>)> = held
        .iter()
        .filter_map(|h| {
            let has = |m: u64| h.set.holders.contains(&m) && complete(h.set.segment, m);
            let to: Vec<u64> = next
                .destination(h.set.segment, h.writer, serving)
                .iter()
                .copied()
                .filter(|m| *m != me && !has(*m) && serving(*m))
                .collect();
            let copies = h.set.holders.iter().filter(|d| serving(**d) && has(**d)).count();
            (!to.is_empty()).then_some((copies, h.set.segment, to))
        })
        .collect();
    planned.sort_unstable();
    planned.into_iter().map(|(_, id, to)| (id, to)).collect()
}

pub fn owed(
    me: u64,
    to: u64,
    held: &[Held],
    groups: &Groups,
    live: impl Fn(u64) -> bool + Copy,
    complete: impl Fn(u64, u64) -> bool,
    draining: bool,
) -> u64 {
    held.iter()
        .filter(|h| {
            !(h.set.holders.contains(&to) && complete(h.set.segment, to))
                && groups
                    .destination(h.set.segment, h.writer, live)
                    .contains(&to)
                && (draining
                    || sender(h.set, |d| live(d) && complete(h.set.segment, d)) == Some(me))
        })
        .map(|h| h.bytes)
        .sum()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;

    fn set(segment: u64, holders: &[u64]) -> HolderSet {
        HolderSet {
            segment,
            epoch: 0,
            holders: holders.iter().copied().collect(),
        }
    }

    fn groups(groups: &[&[u64]]) -> Groups {
        Groups {
            epoch: 1,
            size: groups[0].len() as u8,
            groups: groups.iter().map(|g| g.to_vec()).collect(),
            stand_ins: vec![],
        }
    }

    fn all(_: u64, _: u64) -> bool {
        true
    }

    fn held(writer: u64, set: &HolderSet) -> Held<'_> {
        Held {
            writer,
            bytes: 64,
            sealed: true,
            set,
        }
    }

    #[test]
    fn the_limiter_lets_through_the_cap_in_each_five_second_window() {
        let mut limiter = Limiter::new(0);
        let demand = 4 * REPAIR_BYTES_PER_S / 100;
        let mut windows = vec![0u64; 4];
        for tick in 1..=2_000u64 {
            let now = tick * 10;
            let mut want = demand;
            while want > 0 {
                let got = limiter.available(now).min(want);
                if got == 0 {
                    break;
                }
                assert!(got <= REPAIR_STEP);
                limiter.spend(got);
                want -= got;
                windows[((now - 1) / 5_000) as usize] += got;
            }
        }
        let mut idle = Limiter::new(0);
        assert!(idle.available(3_600_000) <= REPAIR_STEP);
        let cap = 5 * REPAIR_BYTES_PER_S;
        for (i, w) in windows.iter().enumerate() {
            assert!(
                *w * 100 >= cap * 90 && *w * 100 <= cap * 110,
                "window {i}: {w} of {cap}"
            );
        }
    }

    #[test]
    fn no_holder_sends_for_more_than_150_of_1000_segments_on_10_holders() {
        let disks: Vec<u64> = (1..=10).map(|d| d * 0x1000_0001).collect();
        let mut sends: BTreeMap<u64, u32> = BTreeMap::new();
        for segment in 1..=1_000 {
            let s = set(segment, &disks);
            *sends.entry(sender(&s, |_| true).expect("a sender")).or_default() += 1;
        }
        assert_eq!(sends.len(), 10);
        assert!(sends.values().all(|n| *n <= 150), "{sends:?}");
        let s = set(5, &disks);
        let first = sender(&s, |_| true).expect("a sender");
        assert_ne!(sender(&s, |d| d != first), Some(first));
    }

    #[test]
    fn the_self_drain_plans_fewest_copies_first_and_nothing_after_the_deadline() {
        let next = groups(&[&[2, 3, 4], &[5, 6, 7]]);
        let sets = [
            set(10, &[1, 2, 3, 9]),
            set(11, &[1]),
            set(12, &[1, 5]),
            set(13, &[1, 2, 3, 4]),
        ];
        let held: Vec<Held> = sets
            .iter()
            .map(|s| held(if s.segment == 13 { 2 } else { 1 }, s))
            .collect();
        let serving = |d: u64| d != 1 && d != 9;
        let plan = self_drain(1, &held, &next, serving, all, 0, 110_000);
        let order: Vec<u64> = plan.iter().map(|(id, _)| *id).collect();
        assert_eq!(order, vec![11, 12, 10]);
        assert!(plan.iter().all(|(_, to)| !to.contains(&1) && !to.is_empty()));
        assert!(self_drain(1, &held, &next, serving, all, 110_000, 110_000).is_empty());
    }

    const THREE: Want = Want {
        target: 3,
        cold: false,
    };

    fn copy(need: usize, to: &[u64]) -> Step {
        Step::Copy {
            need,
            to: to.to_vec(),
        }
    }

    #[test]
    fn repair_copies_to_the_missing_live_members_of_the_writers_group() {
        let g = groups(&[&[1, 2, 3], &[4, 5, 6]]);
        let s = set(7, &[1, 2, 9]);
        assert_eq!(plan(&held(1, &s), &g, |_| true, all, THREE), copy(1, &[3]));
        assert_eq!(plan(&held(1, &s), &g, |d| d != 3, all, THREE), Step::Idle);
        let other = plan(&held(8, &s), &g, |d| d != 8, all, THREE);
        assert!(other == copy(1, &[3]) || other == copy(3, &[4, 5, 6]), "{other:?}");
        let exact = set(7, &[1, 2, 3]);
        let gap = |_: u64, d: u64| d != 3;
        let Step::Copy { need, to } = plan(&held(1, &exact), &g, |_| true, gap, THREE) else {
            panic!("a lost copy");
        };
        assert_eq!((need, to[0]), (1, 3));
    }

    #[test]
    fn a_lost_copy_may_go_to_any_live_node_not_only_the_group() {
        let g = groups(&[&[1, 2, 3], &[4, 5, 6]]);
        let s = set(7, &[1]);
        let Step::Copy { need, to } = plan(&held(1, &s), &g, |_| true, all, THREE) else {
            panic!("lost copies");
        };
        assert_eq!(need, 2);
        assert_eq!(&to[..2], &[2, 3]);
        let mut rest = to[2..].to_vec();
        rest.sort_unstable();
        assert_eq!(rest, vec![4, 5, 6]);
        let spread: BTreeSet<u64> = (1..=60u64)
            .filter_map(|seg| {
                let s = set(seg, &[1]);
                match plan(&held(1, &s), &g, |_| true, all, THREE) {
                    Step::Copy { to, .. } => to.get(2).copied(),
                    _ => None,
                }
            })
            .collect();
        assert_eq!(spread.len(), 3, "{spread:?}");
    }

    #[test]
    fn the_extra_target_copy_goes_outside_the_group_and_the_drop_keeps_it() {
        let g = groups(&[&[1, 2, 3], &[4, 5, 6]]);
        let four = Want {
            target: 4,
            cold: false,
        };
        let s = set(7, &[1, 2, 3]);
        let Step::Copy { need, to } = plan(&held(1, &s), &g, |_| true, all, four) else {
            panic!("an extra copy");
        };
        assert_eq!(need, 1);
        assert!(!g.groups[0].contains(&to[0]));
        let mut with: Vec<u64> = vec![1, 2, 3, to[0], 9];
        with.sort_unstable();
        let s = set(7, &with);
        let Step::Drop(keep) = plan(&held(1, &s), &g, |_| true, all, four) else {
            panic!("a drop");
        };
        assert_eq!(keep.holders, BTreeSet::from([1, 2, 3, to[0]]));
    }

    #[test]
    fn repair_drops_copies_outside_the_destination_only_when_sealed_whole_and_not_cold() {
        let g = groups(&[&[1, 2, 3], &[4, 5, 6]]);
        let s = set(7, &[9, 1, 2, 3]);
        let expect = HolderSet {
            segment: 7,
            epoch: 1,
            holders: BTreeSet::from([1, 2, 3]),
        };
        assert_eq!(plan(&held(1, &s), &g, |_| true, all, THREE), Step::Drop(expect));
        let open = Held {
            sealed: false,
            ..held(1, &s)
        };
        assert_eq!(plan(&open, &g, |_| true, all, THREE), Step::Idle);
        assert_eq!(plan(&held(1, &s), &g, |d| d != 2, all, THREE), Step::Idle);
        let cold = Want {
            target: 3,
            cold: true,
        };
        assert_eq!(plan(&held(1, &s), &g, |_| true, all, cold), Step::Idle);
        let exact = set(7, &[1, 2, 3]);
        assert_eq!(plan(&held(1, &exact), &g, |_| true, all, THREE), Step::Idle);
        let alone = groups(&[&[1, 2, 3]]);
        let four = Want {
            target: 4,
            cold: false,
        };
        assert_eq!(plan(&held(1, &s), &alone, |_| true, all, four), Step::Idle);
    }

    #[test]
    fn owed_bytes_are_the_segments_a_node_sends_that_the_target_lacks() {
        let g = groups(&[&[1, 2, 3]]);
        let sets: Vec<HolderSet> = (1..=20).map(|s| set(s, &[1, 2])).collect();
        let held: Vec<Held> = sets.iter().map(|s| held(1, s)).collect();
        let by_one = owed(1, 3, &held, &g, |_| true, all, false);
        let by_two = owed(2, 3, &held, &g, |_| true, all, false);
        assert_eq!(by_one + by_two, 20 * 64);
        assert!(by_one > 0 && by_two > 0);
        assert_eq!(owed(1, 3, &held, &g, |_| true, all, true), 20 * 64);
        assert_eq!(owed(1, 2, &held, &g, |_| true, all, true), 0);
    }
}
