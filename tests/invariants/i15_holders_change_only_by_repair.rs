use std::collections::BTreeMap;

use verdandi_core::cluster::placement::HolderSet;
use verdandi_sim::cluster_sim::{HOUR_MS, World};

const GB: u64 = 1_000_000_000;
const OLD: u64 = 25 * HOUR_MS;
const SEEDS: u64 = 10;

fn sets(w: &World) -> BTreeMap<u64, HolderSet> {
    w.segments
        .iter()
        .map(|(id, s)| (*id, s.holders.clone()))
        .collect()
}

fn justified(w: &World, before: &BTreeMap<u64, HolderSet>, from: usize, seed: u64) {
    let mut replay = before.clone();
    for change in &w.holder_changes[from..] {
        let segment = change.from.segment;
        let held = replay.entry(segment).or_insert_with(|| change.from.clone());
        assert_eq!(
            *held, change.from,
            "seed {seed}: an unlogged change of {segment}"
        );
        assert_eq!(change.to.epoch, change.from.epoch + 1, "seed {seed}");
        let (old, new) = (&change.from.holders, &change.to.holders);
        if new.is_superset(old) && new != old {
            assert!(
                new.difference(old).all(|d| change.copies.contains(d)),
                "seed {seed}: {change:?} adds a holder with no copy"
            );
        } else if new.is_subset(old) && new != old {
            assert!(
                change.current
                    && new.len() >= change.least
                    && new.iter().all(|d| change.copies.contains(d)),
                "seed {seed}: {change:?} drops to a set that is not a whole current group"
            );
        } else {
            panic!("seed {seed}: {change:?} neither adds nor drops");
        }
        *held = change.to.clone();
    }
    let now = sets(w);
    for (segment, set) in &replay {
        assert_eq!(
            now.get(segment),
            Some(set),
            "seed {seed}: an unlogged change of {segment}"
        );
    }
}

#[test]
fn i15_holders_change_only_by_repair() {
    let mut floor_changes = 0;
    for seed in 0..SEEDS {
        let mut w = World::new(seed);
        let nodes = w.start(6, 3, OLD);
        w.fill(GB);
        let before = sets(&w);
        assert!(w.settle_repair(1_800_000), "seed {seed}: no repair");
        justified(&w, &before, 0, seed);
        let held = sets(&w);
        let mark = w.holder_changes.len();
        w.held_repair = held.keys().copied().collect();
        w.create("a", OLD, 1);
        w.advance(60_000);
        w.stop(nodes[1]);
        w.advance(120_000);
        assert!(!w.running().contains(&nodes[1]), "seed {seed}");
        let floors = w.floor_changes.len();
        w.kill(nodes[2]);
        w.advance(20_000);
        w.kill(nodes[3]);
        w.advance(60_000);
        floor_changes += w.floor_changes.len() - floors;
        assert_eq!(w.holder_changes.len(), mark, "seed {seed}");
        assert_eq!(sets(&w), held, "seed {seed}");
        w.held_repair.clear();
        assert!(w.settle_repair(1_800_000), "seed {seed}: no repair");
        assert!(
            w.holder_changes.len() > mark,
            "seed {seed}: repair wrote nothing"
        );
        justified(&w, &held, mark, seed);
    }
    assert!(floor_changes > 0, "no floor change in any seed");
}

#[test]
fn i15_two_of_three_down_and_back_drop_no_holder() {
    for seed in 0..SEEDS {
        let mut w = World::new(seed);
        let nodes = w.start(3, 3, OLD);
        w.fill(GB);
        assert!(w.settle_repair(1_800_000), "seed {seed}: no repair");
        let before = sets(&w);
        let mark = w.holder_changes.len();
        w.kill(nodes[1]);
        w.kill(nodes[2]);
        w.advance(40_000);
        assert!(w.boot(nodes[1], 1));
        assert!(w.boot(nodes[2], 1));
        assert!(w.settle_repair(1_800_000), "seed {seed}: no repair");
        justified(&w, &before, mark, seed);
    }
}
