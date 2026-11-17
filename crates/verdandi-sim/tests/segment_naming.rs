use std::collections::BTreeSet;

use verdandi_core::cluster::placement::pair;
use verdandi_node::Msg;
use verdandi_sim::cluster_sim::{SEGMENT_BYTES, STEP_MS, World};

const OLD: u64 = 25 * 3_600_000;

fn named(w: &World, id: u64) -> Vec<u64> {
    w.newest()
        .and_then(|m| m.segments.get(&id))
        .map(|m| m.holders.holders.iter().copied().collect())
        .unwrap_or_default()
}

fn setup(seed: u64) -> (World, u64, Vec<u64>) {
    let mut w = World::new(seed);
    let n = w.start(6, 3, OLD);
    w.advance(60_000);
    let leader = w.raft_leader().expect("a raft leader");
    let rest: Vec<u64> = n.iter().copied().filter(|d| *d != leader).collect();
    let holders = vec![rest[0], rest[1], rest[2]];
    let id = w.place(&holders, SEGMENT_BYTES);
    w.loss = 1.0;
    w.advance(2 * STEP_MS);
    w.loss = 0.0;
    let cuts: Vec<(u64, u64)> = n
        .iter()
        .filter(|d| **d != holders[0])
        .map(|d| pair(holders[0], *d))
        .collect();
    w.cut(&cuts);
    (w, id, holders)
}

#[test]
fn a_resend_from_another_holder_names_a_segment_while_the_writer_is_cut_off() {
    for seed in 0..5 {
        let (mut w, id, holders) = setup(seed);
        w.advance(120_000);
        assert!(named(&w, id).len() >= 3, "seed {seed}: {holders:?}");
    }
}

#[test]
fn a_restarted_holder_resends_the_epoch_one_wish_after_the_writer_died() {
    for seed in 0..5 {
        let (mut w, id, holders) = setup(seed);
        w.kill(holders[0]);
        for d in [holders[1], holders[2]] {
            w.kill(d);
            assert!(w.boot(d, 1));
        }
        w.advance(120_000);
        assert!(named(&w, id).len() >= 2, "seed {seed}: {holders:?}");
    }
}

fn real_copies(w: &World, id: u64, except: u64) -> usize {
    w.segments[&id]
        .marks
        .keys()
        .filter(|h| **h != except && w.running().contains(h))
        .count()
}

#[test]
fn a_drained_node_is_not_removed_while_a_peer_that_lost_its_copy_still_looks_whole() {
    for seed in 0..10 {
        let mut w = World::new(seed);
        let n = w.start(3, 3, OLD);
        w.advance(60_000);
        let id = w.place(&n, SEGMENT_BYTES);
        w.advance(20_000);
        let x = n[0];
        w.lose_copy(n[1], id);
        w.drain(x);
        for _ in 0..1200 {
            w.advance(STEP_MS);
            if !w.in_list(x) {
                let real = real_copies(&w, id, x);
                assert!(real >= 2, "seed {seed}: removed with {real} other copies");
                break;
            }
        }
    }
}

fn settled_holders(seed: u64) -> BTreeSet<u64> {
    let mut w = World::new(seed);
    let n = w.start(6, 3, OLD);
    w.advance(60_000);
    let id = w.place(&n[..5], SEGMENT_BYTES);
    w.advance(120_000);
    named(&w, id).into_iter().collect()
}

#[test]
fn the_leader_does_not_drop_a_copy_while_a_peer_that_lost_its_copy_still_looks_whole() {
    for seed in 0..3 {
        let target = settled_holders(seed);
        let mut w = World::new(seed);
        let n = w.start(6, 3, OLD);
        w.advance(60_000);
        let extra = n[..5]
            .iter()
            .copied()
            .find(|d| !target.contains(d))
            .expect("extra");
        let mut holders = vec![n[0]];
        holders.extend(target.iter().copied().filter(|d| *d != n[0]));
        holders.push(extra);
        let id = w.place(&holders, SEGMENT_BYTES);
        w.held_repair.insert(id);
        w.advance(15_000);
        while w.now_ms % 10_000 != 500 {
            w.advance(STEP_MS);
        }
        let lost = holders[1];
        w.drop = 1.0;
        w.lose_copy(lost, id);
        w.held_repair.remove(&id);
        for _ in 0..80 {
            w.advance(STEP_MS);
            let real = w.live_copies(&w.segments[&id]);
            assert!(
                real >= target.len(),
                "seed {seed}: {real} copies of {target:?}"
            );
        }
    }
}

fn isr_of(w: &World, id: u64) -> BTreeSet<u64> {
    w.newest()
        .and_then(|m| m.segments.get(&id))
        .map(|m| m.isr.iter().copied().collect())
        .unwrap_or_default()
}

#[test]
fn a_copy_one_settle_behind_does_not_join_the_isr() {
    for seed in 0..10 {
        let mut w = World::new(seed);
        let n = w.start(6, 3, OLD);
        w.advance(60_000);
        let leader = w.raft_leader().expect("a raft leader");
        let rest: Vec<u64> = n.iter().copied().filter(|d| *d != leader).collect();
        let (lead, behind) = (rest[0], rest[2]);
        let id = w.place(&[lead, rest[1], behind], SEGMENT_BYTES);
        w.held_repair.insert(id);
        w.advance(10_000);
        w.oneway.insert((lead, behind));
        for _ in 0..24 {
            w.settle();
            w.advance(STEP_MS);
        }
        assert!(!isr_of(&w, id).contains(&behind), "seed {seed}");
        let epoch = w.newest().map_or(0, |m| m.segments[&id].leader_epoch);
        let old = w.segments[&id].marks[&lead];
        w.inject(behind, lead, Msg::SegAsk(id, epoch));
        w.advance(STEP_MS);
        while w.settled.get(&id).copied().unwrap_or(0) <= old {
            w.settle();
            w.advance(STEP_MS);
        }
        w.inject(behind, lead, Msg::Copied(id, behind, old, false));
        for _ in 0..8 {
            w.advance(STEP_MS);
            assert!(
                !isr_of(&w, id).contains(&behind),
                "seed {seed}: a copy at {old} joined the isr, the leader is at {}",
                w.segments[&id].marks[&lead]
            );
        }
    }
}
