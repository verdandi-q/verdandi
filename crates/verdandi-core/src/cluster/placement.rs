use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet};

use super::controller::MAX_COPIES;
use super::fnv;
use super::view::JOIN_MS;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Groups {
    pub epoch: u64,
    pub size: u8,
    pub groups: Vec<Vec<u64>>,
    pub stand_ins: Vec<(usize, u64)>,
}

#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct HolderSet {
    pub segment: u64,
    pub epoch: u64,
    pub writer: u64,
    pub holders: BTreeSet<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Candidate {
    pub disk: u64,
    pub domain: u64,
    pub join_ms: u64,
    pub heavy: Option<u64>,
}

pub fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

pub fn pick(key: &[u64], candidates: impl IntoIterator<Item = u64>) -> Option<u64> {
    candidates
        .into_iter()
        .max_by_key(|c| (mix(fnv(key.iter().copied().chain([*c]))), *c))
}

pub fn pair(a: u64, b: u64) -> (u64, u64) {
    (a.min(b), a.max(b))
}

impl Groups {
    pub fn hash(&self) -> u64 {
        let groups = self
            .groups
            .iter()
            .flat_map(|g| g.iter().copied().chain([u64::MAX]));
        let stand_ins = self.stand_ins.iter().flat_map(|(g, d)| [*g as u64, *d]);
        fnv([self.epoch, u64::from(self.size)]
            .into_iter()
            .chain(groups)
            .chain(stand_ins))
    }

    pub fn merge(self, other: Groups) -> Groups {
        let rank = |g: &Groups| (Reverse(g.epoch), g.hash());
        match rank(&self)
            .cmp(&rank(&other))
            .then_with(|| self.cmp(&other))
        {
            Ordering::Greater => other,
            _ => self,
        }
    }

    pub fn of(&self, disk: u64) -> Option<&[u64]> {
        self.groups
            .iter()
            .find(|g| g.contains(&disk))
            .map(Vec::as_slice)
    }

    fn picked(&self, segment: u64) -> Option<&[u64]> {
        let index = pick(&[segment], 0..self.groups.len() as u64)?;
        self.groups.get(index as usize).map(Vec::as_slice)
    }

    pub fn destination(&self, segment: u64, writer: u64, live: impl Fn(u64) -> bool) -> &[u64] {
        live(writer)
            .then(|| self.of(writer))
            .flatten()
            .or_else(|| self.picked(segment))
            .unwrap_or(&[])
    }

    pub fn new_holders(&self, writer: u64, segment: u64, unreachable: &[u64]) -> Vec<u64> {
        let mut holders = match self.of(writer) {
            Some(group) => group.to_vec(),
            None => {
                let mut h = vec![writer];
                h.extend(self.picked(segment).unwrap_or(&[]));
                h
            }
        };
        holders.retain(|h| *h == writer || !unreachable.contains(h));
        holders
    }

    pub fn current(&self, holders: &BTreeSet<u64>) -> bool {
        self.groups
            .iter()
            .any(|g| !g.is_empty() && g.iter().all(|m| holders.contains(m)))
    }
}

impl HolderSet {
    pub fn merge(self, other: HolderSet) -> HolderSet {
        self.max(other)
    }

    pub fn with(&self, writer: u64, holders: BTreeSet<u64>) -> HolderSet {
        HolderSet {
            segment: self.segment,
            epoch: self.epoch + 1,
            writer,
            holders,
        }
    }
}

pub fn ring(candidates: &[Candidate], floor: u8) -> Vec<u64> {
    let mut on: Vec<Candidate> = candidates.to_vec();
    let mut heavy: Vec<Candidate> = on.iter().copied().filter(|c| c.heavy.is_some()).collect();
    heavy.sort_by_key(|c| (Reverse(c.heavy), c.disk));
    for h in heavy {
        if on.len() <= usize::from(floor) + 1 {
            break;
        }
        on.retain(|c| c.disk != h.disk);
    }
    let mut domains: BTreeMap<u64, Vec<(u64, u64)>> = BTreeMap::new();
    for c in &on {
        domains
            .entry(c.domain)
            .or_default()
            .push((c.join_ms, c.disk));
    }
    for members in domains.values_mut() {
        members.sort_unstable();
    }
    let longest = domains.values().map(Vec::len).max().unwrap_or(0);
    (0..longest)
        .flat_map(|i| {
            domains
                .values()
                .filter_map(move |m| m.get(i).map(|(_, d)| *d))
        })
        .collect()
}

pub fn ready(agreed: bool, run_ms_since_start: u64) -> bool {
    agreed || run_ms_since_start >= JOIN_MS
}

struct Step<'a> {
    groups: Vec<Vec<u64>>,
    stand_ins: Vec<(usize, u64)>,
    ring: &'a [u64],
    domain: &'a BTreeMap<u64, u64>,
    pairs: &'a BTreeSet<(u64, u64)>,
    size: usize,
}

impl Step<'_> {
    fn apart(&self, a: u64, b: u64) -> bool {
        matches!((self.domain.get(&a), self.domain.get(&b)), (Some(x), Some(y)) if x != y)
    }

    fn paired(&self, a: u64, b: u64) -> bool {
        self.pairs.contains(&pair(a, b))
    }

    fn count(&self, disk: u64) -> usize {
        self.groups.iter().filter(|g| g.contains(&disk)).count()
    }

    fn remove(&mut self, g: usize, disk: u64) {
        self.groups[g].retain(|m| *m != disk);
        self.stand_ins.retain(|s| *s != (g, disk));
    }

    fn shrink(&mut self) {
        for g in 0..self.groups.len() {
            while self.groups[g].len() > self.size {
                let group = &self.groups[g];
                let out = group
                    .iter()
                    .copied()
                    .max_by_key(|m| {
                        let shared = group.iter().any(|o| o != m && !self.apart(*o, *m));
                        (shared, self.count(*m), *m)
                    })
                    .expect("a group longer than its size has a member");
                self.remove(g, out);
            }
        }
    }

    fn prune(&mut self) {
        for g in 0..self.groups.len() {
            let off: Vec<u64> = self.groups[g]
                .iter()
                .copied()
                .filter(|m| !self.ring.contains(m))
                .collect();
            for m in off {
                self.remove(g, m);
            }
            while let Some(out) = self.groups[g]
                .iter()
                .flat_map(|a| self.groups[g].iter().map(move |b| (*a, *b)))
                .filter(|(a, b)| a < b && self.paired(*a, *b))
                .map(|(_, b)| b)
                .max()
            {
                self.remove(g, out);
            }
        }
    }

    fn fill(&mut self) {
        for g in 0..self.groups.len() {
            while self.groups[g].len() < self.size {
                let group = &self.groups[g];
                let best = self
                    .ring
                    .iter()
                    .enumerate()
                    .filter(|(_, d)| {
                        !group.contains(d) && !group.iter().any(|m| self.paired(*m, **d))
                    })
                    .min_by_key(|(i, d)| {
                        let count = self.count(**d);
                        let present = group.iter().any(|m| !self.apart(*m, **d));
                        let shares = self
                            .groups
                            .iter()
                            .any(|o| o.contains(d) && group.iter().any(|m| o.contains(m)));
                        (count > 0, present, shares, count, *i)
                    })
                    .map(|(_, d)| *d);
                let Some(disk) = best else {
                    break;
                };
                if self.count(disk) > 0 {
                    self.stand_ins.push((g, disk));
                }
                self.groups[g].push(disk);
            }
        }
    }

    fn spread(&self, group: &[u64]) -> usize {
        let known: BTreeSet<u64> = group
            .iter()
            .filter_map(|m| self.domain.get(m).copied())
            .collect();
        known.len()
    }

    fn short_of_domains(&self, group: &[u64]) -> bool {
        let ring: BTreeSet<u64> = self
            .ring
            .iter()
            .filter_map(|d| self.domain.get(d).copied())
            .collect();
        let known = group.iter().filter(|m| self.domain.contains_key(m)).count();
        self.spread(group) < known.min(ring.len())
    }

    fn respread(&mut self) {
        for g in 0..self.groups.len() {
            if !self.short_of_domains(&self.groups[g]) {
                continue;
            }
            let group = self.groups[g].clone();
            let out = group
                .iter()
                .copied()
                .filter(|m| {
                    self.domain.contains_key(m)
                        && group
                            .iter()
                            .any(|o| o != m && self.domain.contains_key(o) && !self.apart(*o, *m))
                })
                .max_by_key(|m| (self.count(*m), *m));
            let Some(out) = out else {
                continue;
            };
            let rest: Vec<u64> = group.iter().copied().filter(|m| *m != out).collect();
            let incoming = self
                .ring
                .iter()
                .enumerate()
                .filter(|(_, d)| {
                    !group.contains(d)
                        && self.domain.contains_key(d)
                        && rest
                            .iter()
                            .all(|m| self.apart(*m, **d) && !self.paired(*m, **d))
                })
                .min_by_key(|(i, d)| (self.count(**d), *i))
                .map(|(_, d)| *d);
            if let Some(disk) = incoming {
                let elsewhere = self.count(disk) > 0;
                self.remove(g, out);
                if elsewhere {
                    self.stand_ins.push((g, disk));
                }
                self.groups[g].push(disk);
            }
        }
    }

    fn place(&mut self) {
        let mut spare: Vec<u64> = Vec::new();
        let free: Vec<u64> = self
            .ring
            .iter()
            .copied()
            .filter(|d| self.count(*d) == 0)
            .collect();
        for disk in free {
            let slot = self.stand_ins.iter().rposition(|(g, s)| {
                let after: Vec<u64> = self.groups[*g]
                    .iter()
                    .map(|m| if m == s { disk } else { *m })
                    .collect();
                self.count(*s) > 1
                    && !self.short_of_domains(&after)
                    && self.groups[*g]
                        .iter()
                        .all(|m| m == s || (*m != disk && !self.paired(*m, disk)))
            });
            match slot {
                Some(i) => {
                    let (g, old) = self.stand_ins.remove(i);
                    for m in &mut self.groups[g] {
                        if *m == old {
                            *m = disk;
                        }
                    }
                }
                None => spare.push(disk),
            }
        }
        while !spare.is_empty() {
            let mut group: Vec<u64> = Vec::new();
            while group.len() < self.size {
                let fits = |d: &u64| !group.iter().any(|m| self.paired(*m, *d));
                let Some(i) = spare
                    .iter()
                    .position(|d| fits(d) && group.iter().all(|m| self.apart(*m, *d)))
                    .or_else(|| spare.iter().position(fits))
                else {
                    break;
                };
                group.push(spare.remove(i));
            }
            let g = self.groups.len();
            let mut first = self.groups.first().cloned().unwrap_or_default();
            first.sort_by_key(|m| group.iter().any(|o| !self.apart(*o, *m)));
            for m in first {
                if group.len() >= self.size {
                    break;
                }
                if !group.contains(&m) && !group.iter().any(|o| self.paired(*o, m)) {
                    group.push(m);
                    self.stand_ins.push((g, m));
                }
            }
            self.groups.push(group);
        }
    }
}

pub fn next(
    current: Option<&Groups>,
    ring: &[u64],
    domain: &BTreeMap<u64, u64>,
    pairs: &BTreeSet<(u64, u64)>,
    floor: u8,
) -> Option<Groups> {
    let target = floor.clamp(1, MAX_COPIES);
    let (epoch, size, groups, stand_ins) = match current {
        Some(c) => {
            let size = match c.size.cmp(&target) {
                Ordering::Less => c.size + 1,
                Ordering::Greater => c.size - 1,
                Ordering::Equal => c.size,
            };
            (c.epoch, size, c.groups.clone(), c.stand_ins.clone())
        }
        None => {
            if ring.is_empty() {
                return None;
            }
            let (n, a) = (ring.len(), usize::from(target));
            let mut stand_ins = Vec::new();
            let groups = (0..n.div_ceil(a))
                .map(|i| {
                    (0..a.min(n))
                        .map(|k| {
                            if i * a + k >= n {
                                stand_ins.push((i, ring[(i * a + k) % n]));
                            }
                            ring[(i * a + k) % n]
                        })
                        .collect()
                })
                .collect();
            (0, target, groups, stand_ins)
        }
    };
    let mut step = Step {
        groups,
        stand_ins,
        ring,
        domain,
        pairs,
        size: usize::from(size),
    };
    step.shrink();
    step.prune();
    step.fill();
    step.respread();
    step.place();
    let mut index = Vec::new();
    let mut groups = Vec::new();
    for g in step.groups {
        index.push((!g.is_empty()).then_some(groups.len()));
        if !g.is_empty() {
            groups.push(g);
        }
    }
    let made = Groups {
        epoch: epoch + 1,
        size,
        groups,
        stand_ins: step
            .stand_ins
            .into_iter()
            .filter_map(|(g, d)| index[g].map(|i| (i, d)))
            .collect(),
    };
    match current {
        Some(c)
            if c.size == made.size && c.groups == made.groups && c.stand_ins == made.stand_ins =>
        {
            None
        }
        _ => Some(made),
    }
}

pub fn leader(
    group: u64,
    set: &HolderSet,
    live_reconciled: impl Fn(u64) -> bool,
    pairs: &BTreeSet<(u64, u64)>,
) -> Option<u64> {
    let candidates = set.holders.iter().copied().filter(|h| {
        live_reconciled(*h)
            && !set
                .holders
                .iter()
                .any(|o| o != h && pairs.contains(&pair(*h, *o)))
    });
    pick(&[group, set.segment], candidates)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::testkit::Rng;

    fn domains(of: &[(u64, u64)]) -> BTreeMap<u64, u64> {
        of.iter().copied().collect()
    }

    fn candidates(disks: &[u64], domain_of: impl Fn(u64) -> u64) -> Vec<Candidate> {
        disks
            .iter()
            .map(|d| Candidate {
                disk: *d,
                domain: domain_of(*d),
                join_ms: *d,
                heavy: None,
            })
            .collect()
    }

    fn first(disks: &[u64], size: u8) -> Groups {
        next(None, disks, &BTreeMap::new(), &BTreeSet::new(), size).expect("a first record")
    }

    fn after(g: &Groups, ring: &[u64], size: u8) -> Groups {
        next(Some(g), ring, &BTreeMap::new(), &BTreeSet::new(), size).expect("a next record")
    }

    fn changed(a: &Groups, b: &Groups) -> usize {
        b.groups.iter().filter(|g| !a.groups.contains(g)).count()
    }

    #[test]
    fn the_same_record_ring_and_pairs_give_the_same_next_record_in_any_order() {
        let mut rng = Rng(7);
        let disks: Vec<u64> = (1..=10).collect();
        let base = first(&disks, 3);
        let pairs = BTreeSet::from([pair(2, 3)]);
        let mut expect = None;
        for _ in 0..20 {
            let mut shuffled = candidates(&disks[1..], |d| d % 3);
            for i in (1..shuffled.len()).rev() {
                shuffled.swap(i, rng.below(i as u64 + 1) as usize);
            }
            let ring = ring(&shuffled, 3);
            let dom = domains(&disks.iter().map(|d| (*d, d % 3)).collect::<Vec<_>>());
            let made = next(Some(&base), &ring, &dom, &pairs, 3);
            assert_eq!(*expect.get_or_insert(made.clone()), made);
        }
    }

    #[test]
    fn a_new_set_of_groups_spans_every_domain_it_can() {
        let disks: Vec<u64> = (1..=9).collect();
        let domain_of = |d: u64| [7, 3, 7, 5, 3, 5, 3, 7, 5][(d - 1) as usize];
        let ring = ring(&candidates(&disks, domain_of), 3);
        let made = first(&ring, 3);
        assert_eq!(made.groups.len(), 3);
        for g in &made.groups {
            let spread: BTreeSet<u64> = g.iter().map(|d| domain_of(*d)).collect();
            assert_eq!(spread.len(), 3, "{g:?}");
        }
    }

    #[test]
    fn every_ring_node_is_in_a_group_and_a_writer_always_holds_its_own_segment() {
        let mut rng = Rng(3);
        for _ in 0..200 {
            let n = 1 + rng.below(14);
            let ring: Vec<u64> = (1..=n).filter(|_| rng.below(5) > 0).collect();
            if ring.is_empty() {
                continue;
            }
            let size = 2 + rng.below(4) as u8;
            let mut g = first(&ring, size);
            for _ in 0..5 {
                let moved: Vec<u64> = (1..=16).filter(|_| rng.below(3) > 0).collect();
                if let Some(made) = next(Some(&g), &moved, &BTreeMap::new(), &BTreeSet::new(), size)
                {
                    g = made;
                }
                for d in &moved {
                    assert!(g.of(*d).is_some(), "{d} in no group of {g:?}");
                    assert!(g.new_holders(*d, rng.next(), &[]).contains(d));
                }
                assert!(g.groups.iter().all(|grp| grp.len() <= usize::from(size)));
            }
            assert!(g.new_holders(99, 5, &[]).contains(&99));
        }
    }

    #[test]
    fn a_loss_changes_one_group_and_a_return_or_a_new_node_at_most_one() {
        let disks: Vec<u64> = (1..=9).collect();
        let base = first(&disks, 3);
        for lost in &disks {
            let ring: Vec<u64> = disks.iter().copied().filter(|d| d != lost).collect();
            let without = after(&base, &ring, 3);
            assert_eq!(changed(&base, &without), 1, "loss of {lost}");
            let back = after(&without, &disks, 3);
            assert!(changed(&without, &back) <= 1, "return of {lost}");
            let mut grown = ring.clone();
            grown.push(10);
            let new = after(&without, &grown, 3);
            assert!(
                changed(&without, &new) <= 1,
                "new node after loss of {lost}"
            );
        }
        let mut grown = disks.clone();
        grown.push(10);
        assert!(changed(&base, &after(&base, &grown, 3)) <= 1);
    }

    #[test]
    fn a_new_node_takes_the_stand_in_slot_that_a_lost_member_left() {
        let disks: Vec<u64> = (1..=6).collect();
        let base = first(&disks, 3);
        assert_eq!(base.groups, vec![vec![1, 2, 3], vec![4, 5, 6]]);
        let without = after(&base, &[2, 3, 4, 5, 6], 3);
        assert_eq!(without.groups.len(), 2);
        assert_eq!(without.stand_ins.len(), 1);
        let (g, _) = without.stand_ins[0];
        assert_eq!(g, 0);
        let with = after(&without, &[2, 3, 4, 5, 6, 7], 3);
        assert_eq!(with.groups, vec![vec![2, 3, 7], vec![4, 5, 6]]);
        assert!(with.stand_ins.is_empty());
    }

    #[test]
    fn a_size_step_adds_or_removes_one_member_in_each_group() {
        let disks: Vec<u64> = (1..=12).collect();
        let dom = domains(&disks.iter().map(|d| (*d, d % 4)).collect::<Vec<_>>());
        let three = next(None, &disks, &dom, &BTreeSet::new(), 3).expect("first");
        let four = next(Some(&three), &disks, &dom, &BTreeSet::new(), 4).expect("rise");
        assert_eq!(four.size, 4);
        for (a, b) in three.groups.iter().zip(&four.groups) {
            assert_eq!(b.len(), 4);
            assert!(a.iter().all(|m| b.contains(m)), "{a:?} {b:?}");
        }
        let back = next(Some(&four), &disks, &dom, &BTreeSet::new(), 3).expect("fall");
        for (a, b) in four.groups.iter().zip(&back.groups) {
            assert_eq!(b.len(), 3);
            assert!(b.iter().all(|m| a.contains(m)), "{a:?} {b:?}");
        }
        let five = next(Some(&three), &disks, &dom, &BTreeSet::new(), 5).expect("one step");
        assert_eq!(five.size, 4);
    }

    #[test]
    fn a_size_fall_drops_a_member_whose_domain_another_shares_first() {
        let g = Groups {
            epoch: 1,
            size: 3,
            groups: vec![vec![1, 2, 3]],
            stand_ins: vec![],
        };
        let dom = domains(&[(1, 1), (2, 1), (3, 0)]);
        let made = next(Some(&g), &[1, 2, 3], &dom, &BTreeSet::new(), 2).expect("fall");
        assert_eq!(made.groups[0], vec![1, 3]);
        let g = Groups {
            epoch: 1,
            size: 3,
            groups: vec![vec![1, 2, 3], vec![3, 4, 5]],
            stand_ins: vec![],
        };
        let dom = domains(&[(1, 0), (2, 0), (3, 1), (4, 0), (5, 1)]);
        let made = next(Some(&g), &[1, 2, 3, 4, 5], &dom, &BTreeSet::new(), 2).expect("fall");
        assert_eq!(made.groups[0], vec![1, 3]);
    }

    #[test]
    fn a_size_fall_keeps_every_ring_node_in_a_group_and_adds_no_member() {
        let ring: Vec<u64> = (1..=7).collect();
        let three = first(&ring, 3);
        let two = after(&three, &ring, 2);
        assert!(ring.iter().all(|d| two.of(*d).is_some()), "{two:?}");
        for (a, b) in three.groups.iter().zip(&two.groups) {
            assert!(b.iter().all(|m| a.contains(m)), "{three:?} {two:?}");
        }
    }

    #[test]
    fn holder_sets_merge_to_the_highest_epoch_and_writer_never_a_union() {
        let set = |epoch: u64, writer: u64, holders: &[u64]| HolderSet {
            segment: 7,
            epoch,
            writer,
            holders: holders.iter().copied().collect(),
        };
        let low = set(2, 1, &[1, 2, 3, 9, 10]);
        let high = set(2, 4, &[1, 2, 3]);
        assert_eq!(low.clone().merge(high.clone()), high);
        assert_eq!(high.clone().merge(low.clone()), high);
        assert_eq!(high.clone().merge(set(3, 1, &[4])), set(3, 1, &[4]));
        let mut rng = Rng(9);
        let random = |rng: &mut Rng| {
            let holders: Vec<u64> = (1..=6).filter(|_| rng.below(2) == 0).collect();
            set(rng.below(3), rng.below(3), &holders)
        };
        for _ in 0..500 {
            let (a, b, c) = (random(&mut rng), random(&mut rng), random(&mut rng));
            assert_eq!(a.clone().merge(b.clone()), b.clone().merge(a.clone()));
            assert_eq!(
                a.clone().merge(b.clone()).merge(c.clone()),
                a.clone().merge(b.clone().merge(c.clone()))
            );
            assert_eq!(a.clone().merge(a.clone()), a);
            let merged = a.clone().merge(b.clone());
            assert!(merged == a || merged == b);
        }
    }

    #[test]
    fn a_group_whose_members_all_left_refills_from_nodes_that_share_no_group() {
        let g = Groups {
            epoch: 1,
            size: 3,
            groups: vec![
                vec![1, 2, 3],
                vec![4, 5, 6],
                vec![7, 8, 9],
                vec![10, 11, 12],
            ],
            stand_ins: vec![],
        };
        let ring = [1, 2, 3, 7, 8, 9, 10, 11, 12];
        let made = after(&g, &ring, 3);
        let refilled = &made.groups[1];
        for other in made
            .groups
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != 1)
            .map(|(_, o)| o)
        {
            let shared = refilled.iter().filter(|m| other.contains(m)).count();
            assert!(shared <= 1, "{made:?}");
        }
    }

    #[test]
    fn a_size_of_six_is_never_written() {
        let disks: Vec<u64> = (1..=12).collect();
        let mut g = first(&disks, 9);
        assert_eq!(g.size, MAX_COPIES);
        for _ in 0..4 {
            if let Some(made) = next(Some(&g), &disks, &BTreeMap::new(), &BTreeSet::new(), 9) {
                g = made;
            }
            assert!(g.size <= MAX_COPIES);
            assert!(
                g.groups
                    .iter()
                    .all(|grp| grp.len() <= usize::from(MAX_COPIES))
            );
        }
    }

    #[test]
    fn a_reported_pair_is_in_no_group_of_the_next_record() {
        let disks: Vec<u64> = (1..=9).collect();
        let base = first(&disks, 3);
        for (a, b) in [(1, 2), (1, 3), (4, 6), (8, 9)] {
            let pairs = BTreeSet::from([pair(a, b)]);
            let made = next(Some(&base), &disks, &BTreeMap::new(), &pairs, 3).expect("split");
            for g in &made.groups {
                assert!(!(g.contains(&a) && g.contains(&b)), "{a} {b} in {g:?}");
            }
            assert!(disks.iter().all(|d| made.of(*d).is_some()));
            let fresh = next(None, &disks, &BTreeMap::new(), &pairs, 3).expect("first");
            assert!(
                fresh
                    .groups
                    .iter()
                    .all(|g| !(g.contains(&a) && g.contains(&b)))
            );
        }
    }

    #[test]
    fn a_record_that_fits_the_ring_gives_no_next_record() {
        let disks: Vec<u64> = (1..=7).collect();
        let base = first(&disks, 3);
        assert_eq!(
            next(Some(&base), &disks, &BTreeMap::new(), &BTreeSet::new(), 3),
            None
        );
        assert!(
            next(
                Some(&base),
                &disks[1..],
                &BTreeMap::new(),
                &BTreeSet::new(),
                3
            )
            .is_some()
        );
    }

    #[test]
    fn a_holder_set_that_holds_a_whole_group_is_current() {
        let g = first(&[1, 2, 3, 4, 5, 6], 3);
        assert!(g.current(&BTreeSet::from([1, 2, 3])));
        assert!(g.current(&BTreeSet::from([1, 2, 3, 9])));
        assert!(!g.current(&BTreeSet::from([1, 2, 4])));
        assert!(!g.current(&BTreeSet::new()));
    }

    #[test]
    fn the_ring_interleaves_domains_and_drops_the_heaviest_while_above_the_floor() {
        let mut c = candidates(&[1, 2, 3, 4, 5, 6], |d| d % 2);
        assert_eq!(ring(&c, 3), vec![2, 1, 4, 3, 6, 5]);
        c[0].heavy = Some(5);
        c[3].heavy = Some(9);
        c[4].heavy = Some(1);
        assert_eq!(ring(&c, 3), vec![2, 3, 6, 5]);
        assert_eq!(ring(&c, 4), vec![2, 1, 6, 3, 5]);
        let three = candidates(&[1, 2, 3], |_| 0)
            .into_iter()
            .map(|c| Candidate {
                heavy: Some(1),
                ..c
            })
            .collect::<Vec<_>>();
        assert_eq!(ring(&three, 3).len(), 3);
    }

    #[test]
    fn records_merge_by_the_highest_epoch_then_the_lowest_hash() {
        let a = first(&[1, 2, 3, 4], 2);
        let b = first(&[1, 2, 3, 5], 2);
        let c = after(&a, &[1, 2, 3], 2);
        let (low, high) = if a.hash() < b.hash() {
            (&a, &b)
        } else {
            (&b, &a)
        };
        assert_eq!(high.clone().merge(low.clone()), *low);
        assert_eq!(low.clone().merge(high.clone()), *low);
        assert_eq!(a.clone().merge(c.clone()), c);
        assert_eq!(c.clone().merge(a.clone()), c);
        assert_eq!(a.clone().merge(a.clone()), a);
    }

    #[test]
    fn the_leader_skips_a_holder_in_a_reported_pair_and_is_the_same_everywhere() {
        let set = HolderSet {
            segment: 9,
            epoch: 0,
            writer: 1,
            holders: BTreeSet::from([1, 2, 3]),
        };
        let all = leader(4, &set, |_| true, &BTreeSet::new()).expect("a leader");
        assert_eq!(leader(4, &set, |_| true, &BTreeSet::new()), Some(all));
        let others: Vec<u64> = [1, 2, 3].into_iter().filter(|h| *h != all).collect();
        let pairs = BTreeSet::from([pair(all, others[0])]);
        assert_eq!(leader(4, &set, |_| true, &pairs), Some(others[1]));
        let none = BTreeSet::from([pair(1, 2), pair(2, 3), pair(1, 3)]);
        assert_eq!(leader(4, &set, |_| true, &none), None);
        assert_eq!(
            leader(4, &set, |h| h == others[0], &BTreeSet::new()),
            Some(others[0])
        );
    }

    #[test]
    fn a_group_short_of_domains_swaps_one_member_and_the_records_settle_spread() {
        let dom = domains(&[(1, 0), (2, 0), (3, 1), (4, 2), (5, 0), (6, 1)]);
        let g = Groups {
            epoch: 1,
            size: 3,
            groups: vec![vec![1, 2, 3], vec![4, 5, 6]],
            stand_ins: vec![],
        };
        let all = [1, 2, 3, 4, 5, 6];
        let made = next(Some(&g), &all, &dom, &BTreeSet::new(), 3).expect("a re-spread");
        assert_eq!(made.groups[0], vec![1, 3, 4]);
        assert_eq!(made.groups[1], vec![4, 5, 6]);
        assert!(made.of(2).is_some());
        let mut rng = Rng(5);
        for _ in 0..300 {
            let n = 3 + rng.below(13);
            let ring: Vec<u64> = (1..=n).collect();
            let dom: BTreeMap<u64, u64> = ring.iter().map(|d| (*d, rng.below(3))).collect();
            let size = 2 + rng.below(3) as u8;
            let mut g = next(None, &ring, &BTreeMap::new(), &BTreeSet::new(), size).expect("first");
            let mut rounds = 0;
            while let Some(made) = next(Some(&g), &ring, &dom, &BTreeSet::new(), size) {
                g = made;
                rounds += 1;
                assert!(rounds < 50, "no fixed point: {g:?} {dom:?}");
            }
            let ring_domains: BTreeSet<u64> = dom.values().copied().collect();
            for grp in &g.groups {
                let spread: BTreeSet<u64> = grp.iter().map(|d| dom[d]).collect();
                assert_eq!(
                    spread.len(),
                    grp.len().min(ring_domains.len()),
                    "{g:?} {dom:?}"
                );
            }
            assert!(ring.iter().all(|d| g.of(*d).is_some()));
        }
    }

    #[test]
    fn an_absent_writers_segment_goes_to_the_group_its_segment_picks() {
        let g = first(&[1, 2, 3, 4, 5, 6], 3);
        assert_eq!(g.destination(9, 1, |_| true), &[1, 2, 3]);
        let picked: BTreeSet<Vec<u64>> = (1..=20)
            .map(|s| g.destination(s, 1, |d| d != 1).to_vec())
            .collect();
        assert_eq!(picked.len(), 2);
    }

    #[test]
    fn a_short_group_and_a_new_group_take_missing_domains_in_one_record() {
        let dom = domains(&[
            (1, 0),
            (2, 0),
            (3, 0),
            (4, 1),
            (5, 2),
            (6, 0),
            (7, 0),
            (8, 1),
            (9, 2),
        ]);
        let g = Groups {
            epoch: 1,
            size: 3,
            groups: vec![vec![1]],
            stand_ins: vec![],
        };
        let made = next(Some(&g), &[1, 2, 3, 4, 5], &dom, &BTreeSet::new(), 3).expect("a fill");
        assert_eq!(made.groups[0], vec![1, 4, 5]);
        let g = Groups {
            epoch: 1,
            size: 3,
            groups: vec![vec![1, 4, 5]],
            stand_ins: vec![],
        };
        let made =
            next(Some(&g), &[1, 4, 5, 6, 7, 8, 9], &dom, &BTreeSet::new(), 3).expect("new groups");
        for grp in &made.groups {
            let spread: BTreeSet<u64> = grp.iter().map(|d| dom[d]).collect();
            assert_eq!(spread.len(), 3, "{made:?}");
        }
    }

    #[test]
    fn a_new_node_takes_a_wrapped_slot_of_the_first_record() {
        let base = first(&[1, 2, 3, 4, 5, 6, 7], 3);
        assert_eq!(base.groups.len(), 3);
        let with = after(&base, &[1, 2, 3, 4, 5, 6, 7, 8], 3);
        assert_eq!(with.groups.len(), 3);
        assert!(with.of(8).is_some());
    }

    #[test]
    fn the_first_record_waits_for_agreed_domains_or_the_join_window() {
        assert!(!ready(false, JOIN_MS - 1));
        assert!(ready(true, 0));
        assert!(ready(false, JOIN_MS));
    }

    #[test]
    fn an_off_ring_writer_writes_to_itself_and_the_group_its_segment_picks() {
        let g = first(&[1, 2, 3, 4, 5, 6], 3);
        let holders = g.new_holders(9, 77, &[]);
        assert_eq!(holders.len(), 4);
        assert_eq!(holders[0], 9);
        assert!(g.current(&holders[1..].iter().copied().collect()));
        assert_eq!(g.new_holders(1, 77, &[2]), vec![1, 3]);
        assert_eq!(g.new_holders(1, 77, &[1]), vec![1, 2, 3]);
    }
}
