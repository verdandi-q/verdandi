use std::collections::{BTreeMap, BTreeSet};

use super::controller::{Floor, KEYED_MIN_COPIES};
use super::view::{JOIN_MS, TOMBSTONE_HORIZON};

pub const FENCE_MS: u64 = 1_500;

pub type Marks = BTreeMap<u64, u64>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Answer {
    pub absent: bool,
    pub marks: Marks,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Block {
    Wait,
    Ack(usize),
    Refuse(Vec<u64>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Known {
    Copy,
    Partial,
    Dropped,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fate {
    Keep,
    Delete,
    Last,
}

pub fn reached(own: &Marks, theirs: &Marks) -> bool {
    theirs
        .iter()
        .all(|(origin, mark)| own.get(origin).copied().unwrap_or(0) >= *mark)
}

pub fn full(set: usize, reaches: usize) -> bool {
    2 * reaches <= set
}

pub fn confirmed(votes: usize, set: usize) -> bool {
    2 * votes > set
}

pub fn enough(copies: usize, set: usize) -> bool {
    copies >= usize::from(KEYED_MIN_COPIES) && confirmed(copies, set)
}

pub fn quorum(others: usize, set: usize) -> bool {
    others > 0 && confirmed(1 + others, set)
}

pub fn fenced(lost: &mut BTreeSet<u64>, stale: &[u64], lifted: impl Fn(u64) -> bool) -> bool {
    lost.extend(stale);
    lost.retain(|x| !lifted(*x));
    !lost.is_empty()
}

pub fn lifted(
    own: &Marks,
    from_x: Option<&Answer>,
    absent: bool,
    others: &[Option<&Answer>],
    holders: usize,
    members: &[&Answer],
    set: usize,
) -> bool {
    if let Some(answer) = from_x {
        return reached(own, &answer.marks);
    }
    absent
        && if quorum(others.len(), holders) {
            others
                .iter()
                .all(|a| a.is_some_and(|a| a.absent && reached(own, &a.marks)))
        } else {
            2 * (1 + members.iter().filter(|a| a.absent).count()) > set
        }
}

// allow: the block rule needs the writer, the holders, their answers, the group size and two floors
#[allow(clippy::too_many_arguments)]
pub fn block(
    writer: u64,
    holders: &[u64],
    confirmed: &BTreeSet<u64>,
    absent: impl Fn(u64) -> bool,
    timed_out: bool,
    size: usize,
    opened: Floor,
    floor: Floor,
) -> Block {
    let late: Vec<u64> = holders
        .iter()
        .copied()
        .filter(|h| *h != writer && !confirmed.contains(h) && !absent(*h))
        .collect();
    if !late.is_empty() {
        return if timed_out {
            Block::Refuse(late)
        } else {
            Block::Wait
        };
    }
    let copies = holders
        .iter()
        .filter(|h| **h == writer || confirmed.contains(h))
        .count();
    let degraded = copies < size;
    if copies >= usize::from(KEYED_MIN_COPIES) && !(degraded && opened != floor) {
        Block::Ack(copies)
    } else {
        Block::Refuse(Vec::new())
    }
}

pub fn unreachable(
    list: &mut BTreeMap<u64, Option<u64>>,
    live: &[u64],
    fresh: impl Fn(u64) -> bool,
    now_ms: u64,
) {
    for peer in live {
        if !fresh(*peer) {
            list.entry(*peer).or_insert(None);
        }
    }
    for (peer, since) in list.iter_mut() {
        if !fresh(*peer) {
            *since = None;
        } else if since.is_none() {
            *since = Some(now_ms);
        }
    }
    list.retain(|peer, since| live.contains(peer) && since.is_none_or(|t| now_ms < t + JOIN_MS));
}

pub fn expired(applied_alive_ms: u64, alive_ms: u64) -> bool {
    alive_ms >= applied_alive_ms + TOMBSTONE_HORIZON
}

pub fn fate(wiping: bool, answers: &[Known]) -> Fate {
    let unknown = !answers.is_empty() && answers.iter().all(|a| *a == Known::Unknown);
    if unknown || answers.contains(&Known::Dropped) {
        Fate::Delete
    } else if !wiping {
        Fate::Keep
    } else if answers.contains(&Known::Copy) {
        Fate::Delete
    } else {
        Fate::Last
    }
}

pub fn no_holder(
    me: u64,
    holders: &BTreeSet<u64>,
    live: impl Fn(u64) -> bool,
    complete: impl Fn(u64) -> bool,
) -> bool {
    !complete(me) && !holders.iter().any(|h| *h != me && live(*h) && complete(*h))
}

#[cfg(test)]
mod tests {
    use super::*;

    const F: Floor = Floor { epoch: 0, value: 3 };

    fn marks(of: &[(u64, u64)]) -> Marks {
        of.iter().copied().collect()
    }

    fn answer(absent: bool, mark: u64) -> Answer {
        Answer {
            absent,
            marks: marks(&[(7, mark)]),
        }
    }

    #[test]
    fn marks_reach_an_answer_only_at_or_above_each_of_its_origins() {
        assert!(reached(&marks(&[(1, 5), (2, 3)]), &marks(&[(1, 5)])));
        assert!(!reached(&marks(&[(1, 4)]), &marks(&[(1, 5)])));
        assert!(!reached(&marks(&[(1, 9)]), &marks(&[(2, 1)])));
        assert!(reached(&Marks::new(), &marks(&[(2, 0)])));
    }

    #[test]
    fn a_stale_co_holder_fences_the_segment_until_its_records_are_fetched() {
        let mut lost = BTreeSet::new();
        assert!(fenced(&mut lost, &[2], |_| false));
        let behind = marks(&[(7, 4)]);
        let ahead = answer(false, 5);
        assert!(!lifted(&behind, Some(&ahead), false, &[], 3, &[], 3));
        assert!(fenced(&mut lost, &[], |_| {
            lifted(&behind, Some(&ahead), false, &[], 3, &[], 3)
        }));
        let caught_up = marks(&[(7, 5)]);
        assert!(!fenced(&mut lost, &[], |_| {
            lifted(&caught_up, Some(&ahead), false, &[], 3, &[], 3)
        }));
        assert!(lost.is_empty());
    }

    #[test]
    fn a_dead_co_holder_that_another_co_holder_sees_alive_keeps_the_fence() {
        let own = marks(&[(7, 5)]);
        let sees_alive = answer(false, 5);
        let sees_dead = answer(true, 5);
        let sees_dead_ahead = answer(true, 6);
        assert!(!lifted(&own, None, true, &[Some(&sees_alive)], 3, &[], 3));
        assert!(!lifted(&own, None, true, &[None], 3, &[], 3));
        assert!(!lifted(
            &own,
            None,
            true,
            &[Some(&sees_dead_ahead)],
            3,
            &[],
            3
        ));
        assert!(!lifted(&own, None, false, &[Some(&sees_dead)], 3, &[], 3));
        assert!(!lifted(
            &own,
            None,
            true,
            &[Some(&sees_dead), Some(&sees_alive)],
            3,
            &[],
            3
        ));
        assert!(lifted(&own, None, true, &[Some(&sees_dead)], 3, &[], 3));
    }

    #[test]
    fn with_no_other_co_holder_the_fence_stays_until_more_than_half_of_the_list_answers() {
        let own = marks(&[(7, 5)]);
        let dead = answer(true, 0);
        let alive = answer(false, 0);
        assert!(!lifted(&own, None, true, &[], 3, &[&dead, &alive], 5));
        assert!(lifted(&own, None, true, &[], 3, &[&dead, &dead, &alive], 5));
        assert!(!lifted(&own, None, true, &[], 3, &[&dead, &dead], 6));
        assert!(lifted(&own, None, true, &[], 3, &[&dead, &dead, &dead], 6));
    }

    #[test]
    fn an_others_lift_needs_the_asker_and_its_answering_co_holders_over_half_of_the_holder_set() {
        let own = marks(&[(7, 5)]);
        let dead = answer(true, 5);
        assert!(!lifted(&own, None, true, &[Some(&dead)], 4, &[], 3));
        assert!(!lifted(&own, None, true, &[Some(&dead)], 5, &[], 3));
        assert!(lifted(
            &own,
            None,
            true,
            &[Some(&dead), Some(&dead)],
            4,
            &[],
            3
        ));
        assert!(lifted(
            &own,
            None,
            true,
            &[Some(&dead), Some(&dead)],
            5,
            &[],
            3
        ));
        assert!(!lifted(
            &own,
            None,
            true,
            &[Some(&dead), Some(&dead)],
            6,
            &[],
            3
        ));
        assert!(lifted(
            &own,
            None,
            true,
            &[Some(&dead)],
            4,
            &[&dead, &dead],
            3
        ));
    }

    #[test]
    fn a_settle_needs_its_copies_on_more_than_half_of_the_holder_set() {
        assert!(enough(2, 2));
        assert!(enough(2, 3));
        assert!(!enough(2, 4));
        assert!(enough(3, 4));
        assert!(!enough(2, 5));
        assert!(enough(3, 5));
        assert!(!enough(1, 1));
    }

    #[test]
    fn a_vote_confirms_only_with_more_than_half_of_the_set() {
        assert!(!confirmed(2, 4));
        assert!(confirmed(3, 4));
        assert!(confirmed(3, 5));
        assert!(!confirmed(0, 0));
    }

    #[test]
    fn the_full_fence_holds_at_half_of_the_set_or_less() {
        assert!(!full(1, 1));
        assert!(full(2, 1));
        assert!(full(4, 2));
        assert!(!full(5, 3));
        assert!(full(5, 2));
        assert!(!full(3, 2));
    }

    #[test]
    fn a_block_waits_for_each_live_holder_and_completes_at_an_absent_one() {
        let none = BTreeSet::new();
        let two = BTreeSet::from([2]);
        let all = BTreeSet::from([2, 3]);
        assert_eq!(
            block(1, &[1, 2, 3], &none, |_| false, false, 3, F, F),
            Block::Wait
        );
        assert_eq!(
            block(1, &[1, 2, 3], &two, |_| false, true, 3, F, F),
            Block::Refuse(vec![3])
        );
        assert_eq!(
            block(1, &[1, 2, 3], &two, |h| h == 3, false, 3, F, F),
            Block::Ack(2)
        );
        assert_eq!(
            block(1, &[1, 2, 3], &none, |h| h != 1, false, 3, F, F),
            Block::Refuse(Vec::new())
        );
        assert_eq!(
            block(1, &[1, 2, 3], &all, |_| false, false, 3, F, F),
            Block::Ack(3)
        );
        assert_eq!(
            block(1, &[1], &none, |_| false, true, 1, F, F),
            Block::Refuse(Vec::new())
        );
    }

    #[test]
    fn a_degraded_ack_is_refused_after_the_floor_changed() {
        let two = BTreeSet::from([2]);
        let all = BTreeSet::from([2, 3]);
        let risen = Floor { epoch: 1, value: 4 };
        assert_eq!(
            block(1, &[1, 2, 3], &two, |h| h == 3, false, 3, F, risen),
            Block::Refuse(Vec::new())
        );
        assert_eq!(
            block(1, &[1, 2, 3], &all, |_| false, false, 3, F, risen),
            Block::Ack(3)
        );
        assert_eq!(
            block(1, &[1, 2], &two, |_| false, false, 3, F, risen),
            Block::Refuse(Vec::new())
        );
        assert_eq!(
            block(1, &[1, 2], &two, |_| false, false, 3, F, F),
            Block::Ack(2)
        );
    }

    #[test]
    fn an_unreachable_peer_leaves_after_join_ms_of_round_trips() {
        let mut list = BTreeMap::from([(5, None)]);
        unreachable(&mut list, &[5], |_| false, 0);
        assert_eq!(list, BTreeMap::from([(5, None)]));
        unreachable(&mut list, &[5], |_| true, 1_000);
        unreachable(&mut list, &[5], |_| true, 1_000 + JOIN_MS - 1);
        assert!(list.contains_key(&5));
        unreachable(&mut list, &[5], |_| false, 1_000 + JOIN_MS - 1);
        unreachable(&mut list, &[5], |_| true, 20_000);
        unreachable(&mut list, &[5], |_| true, 20_000 + JOIN_MS - 1);
        assert!(list.contains_key(&5));
        unreachable(&mut list, &[5], |_| true, 20_000 + JOIN_MS);
        assert!(list.is_empty());
        let mut list = BTreeMap::from([(5, None), (6, None)]);
        unreachable(&mut list, &[6], |_| true, 0);
        assert_eq!(list, BTreeMap::from([(6, Some(0))]));
        let mut list = BTreeMap::new();
        unreachable(&mut list, &[7, 8], |p| p == 8, 0);
        assert_eq!(list, BTreeMap::from([(7, None)]));
    }

    #[test]
    fn a_tombstone_expires_at_the_horizon_of_the_keeping_node_run_time() {
        assert!(!expired(100, 100 + TOMBSTONE_HORIZON - 1));
        assert!(expired(100, 100 + TOMBSTONE_HORIZON));
    }

    #[test]
    fn a_held_segment_is_deleted_on_a_drop_or_when_every_co_holder_forgot_it() {
        use Known::*;
        assert_eq!(fate(false, &[Copy, Dropped]), Fate::Delete);
        assert_eq!(fate(true, &[Partial, Dropped]), Fate::Delete);
        assert_eq!(fate(false, &[Unknown, Unknown]), Fate::Delete);
        assert_eq!(fate(true, &[Unknown]), Fate::Delete);
        assert_eq!(fate(false, &[Unknown, Partial]), Fate::Keep);
        assert_eq!(fate(false, &[]), Fate::Keep);
    }

    #[test]
    fn a_wipe_keeps_only_a_segment_that_no_other_live_holder_has_whole() {
        use Known::*;
        assert_eq!(fate(true, &[Copy, Partial]), Fate::Delete);
        assert_eq!(fate(true, &[Partial, Unknown]), Fate::Last);
        assert_eq!(fate(true, &[]), Fate::Last);
    }

    #[test]
    fn the_no_holder_rule_fires_only_without_a_live_complete_copy() {
        let holders = BTreeSet::from([1, 2, 3]);
        assert!(!no_holder(1, &holders, |_| true, |h| h == 2));
        assert!(no_holder(1, &holders, |h| h != 2, |h| h == 2));
        assert!(!no_holder(1, &holders, |_| true, |h| h == 1));
        assert!(no_holder(4, &holders, |_| true, |_| false));
    }
}
