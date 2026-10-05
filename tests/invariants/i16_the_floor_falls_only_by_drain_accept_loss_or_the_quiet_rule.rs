use std::collections::BTreeSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

use verdandi_core::cluster::controller::Floor;
use verdandi_core::cluster::view::TOMBSTONE_HORIZON;
use verdandi_sim::cluster_sim::{
    Cause, FloorChange, HOUR_MS, Limit, LossKind, SEGMENT_BYTES, World, WorldLoss, mix,
};

const MIXES: u64 = 1_000;
const EVENTS: usize = 20;
const THREADS: usize = 4;

#[test]
fn i16_the_floor_falls_only_by_drain_accept_loss_or_the_quiet_rule() {
    let next = AtomicU64::new(0);
    let falls = Mutex::new([0usize; 5]);
    thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                loop {
                    let seed = next.fetch_add(1, Ordering::Relaxed);
                    if seed >= MIXES {
                        break;
                    }
                    let world = mix(seed, EVENTS);
                    let bad = world.illegal_falls();
                    assert!(bad.is_empty(), "seed {seed}: {bad:?}");
                    let mut falls = falls.lock().expect("falls");
                    for change in world
                        .floor_changes
                        .iter()
                        .filter(|c| c.to.value < c.from.value)
                    {
                        match change.cause {
                            Cause::Quiet => {
                                falls[0] += 1;
                                match change.limit {
                                    Some(Limit::FirstSeen) => falls[3] += 1,
                                    Some(Limit::Blackout) => falls[4] += 1,
                                    None => {}
                                }
                            }
                            Cause::Drain => falls[1] += 1,
                            Cause::AcceptLoss => falls[2] += 1,
                            Cause::Rise | Cause::Merge => {}
                        }
                    }
                }
            });
        }
    });
    let falls = falls.into_inner().expect("falls");
    println!(
        "I16 MIXES PASS mixes={MIXES} quiet-falls={} drain-falls={} accept-loss-falls={} limit-first-seen={} limit-blackout={}",
        falls[0], falls[1], falls[2], falls[3], falls[4]
    );
    assert!(falls[..3].iter().all(|f| *f > 0), "{falls:?}");
}

const OLD: u64 = 25 * HOUR_MS;
const GB: u64 = 1_000_000_000;
const SCENARIO_SEEDS: u64 = 10;

fn quiet(at: u64, from: Floor, to: Floor) -> FloorChange {
    FloorChange {
        run_ms: at,
        disk: 1,
        from,
        to,
        cause: Cause::Quiet,
        debt_ok: true,
        majority: true,
        counts_ok: true,
        limit: None,
    }
}

const THREE: Floor = Floor { epoch: 0, value: 3 };
const TWO: Floor = Floor { epoch: 1, value: 2 };

#[test]
fn i16_the_oracle_refuses_a_backward_or_unmade_merge() {
    let mut w = World::new(1);
    w.floor_changes.push(FloorChange {
        cause: Cause::Merge,
        to: Floor { epoch: 0, value: 2 },
        ..quiet(10 * HOUR_MS, THREE, THREE)
    });
    assert_eq!(w.illegal_falls().len(), 1);
    let mut w = World::new(1);
    let merge = FloorChange {
        cause: Cause::Merge,
        disk: 2,
        ..quiet(11 * HOUR_MS, THREE, TWO)
    };
    w.floor_changes.push(merge);
    assert_eq!(w.illegal_falls().len(), 1);
    w.floor_changes.insert(0, quiet(10 * HOUR_MS, THREE, TWO));
    assert!(w.illegal_falls().is_empty());
}

#[test]
fn i16_the_oracle_sees_a_quiet_fall_soon_after_a_loss() {
    let r = 10 * HOUR_MS;
    for (back, flagged) in [(None, 1), (Some(30_000), 1), (Some(5_000), 0)] {
        let mut w = World::new(1);
        w.losses.push(WorldLoss {
            disk: 7,
            run_ms: r - 240_000,
            kind: LossKind::Kill,
            side: BTreeSet::from([1, 7]),
        });
        if let Some(back) = back {
            w.boots.push((7, r - 240_000 + back));
        }
        w.floor_changes.push(quiet(r, THREE, TWO));
        assert_eq!(w.illegal_falls().len(), flagged, "back {back:?}");
    }
}

#[test]
fn i16_the_oracle_needs_a_majority_and_one_count() {
    for change in [
        FloorChange {
            majority: false,
            ..quiet(10 * HOUR_MS, THREE, TWO)
        },
        FloorChange {
            counts_ok: false,
            ..quiet(10 * HOUR_MS, THREE, TWO)
        },
        FloorChange {
            debt_ok: false,
            ..quiet(10 * HOUR_MS, THREE, TWO)
        },
    ] {
        let mut w = World::new(1);
        w.floor_changes.push(change);
        assert_eq!(w.illegal_falls().len(), 1, "{change:?}");
    }
}

#[test]
fn i16_the_oracle_member_set_comes_from_the_world() {
    let mut w = World::new(1);
    let nodes = w.start(5, 3, OLD);
    w.advance(10_000);
    w.kill(nodes[1]);
    w.advance(10_000);
    w.delete(nodes[0]);
    w.create("a", OLD, 1);
    w.advance(10_000);
    assert_eq!(w.members(), 5);
    w.jump(TOMBSTONE_HORIZON);
    assert_eq!(w.members(), 4);
}

#[test]
fn i16_a_restarted_node_keeps_its_count() {
    for seed in 0..SCENARIO_SEEDS {
        let mut w = World::new(seed);
        let nodes = w.start(6, 3, OLD);
        w.fill(GB);
        w.quiet_hours(7);
        w.kill(nodes[5]);
        w.advance(30_000);
        let count = w.count(nodes[0]);
        w.kill(nodes[0]);
        w.advance(10_000);
        assert!(w.boot(nodes[0], 1));
        assert_eq!(w.count(nodes[0]), count, "seed {seed}");
    }
}

fn kept(w: &World, minority: &[u64], seed: u64) {
    let bad = w.illegal_falls();
    assert!(bad.is_empty(), "seed {seed}: {bad:?}");
    let falls: Vec<&FloorChange> = w
        .floor_changes
        .iter()
        .filter(|c| minority.contains(&c.disk) && c.to.value < c.from.value)
        .collect();
    assert!(
        falls.is_empty(),
        "seed {seed}: a minority node fell {falls:?}"
    );
}

#[test]
fn i16_a_minority_side_of_a_partition_keeps_the_floor() {
    for seed in 0..SCENARIO_SEEDS {
        let mut w = World::new(seed);
        let nodes = w.start(5, 3, OLD);
        w.fill(GB);
        w.advance(60_000);
        w.part(&nodes[..2]);
        w.quiet_hours(7);
        w.advance(60_000);
        kept(&w, &nodes[..2], seed);
    }
}

#[test]
fn i16_a_minority_side_keeps_the_floor_through_restarts() {
    for restarts in 1..=2 {
        for seed in 0..SCENARIO_SEEDS {
            let mut w = World::new(seed);
            let nodes = w.start(5, 3, OLD);
            w.fill(GB);
            w.advance(60_000);
            w.part(&nodes[..2]);
            w.advance(60_000);
            for d in &nodes[..restarts] {
                w.kill(*d);
                w.advance(30_000);
                assert!(w.boot(*d, 1));
                w.advance(60_000);
            }
            w.quiet_hours(7);
            w.advance(60_000);
            kept(&w, &nodes[..2], seed);
        }
    }
}

#[test]
fn i16_a_joiner_after_a_rolling_restart_keeps_the_floor_on_a_minority_side() {
    for seed in 0..SCENARIO_SEEDS {
        let mut w = World::new(seed);
        let nodes = w.start(9, 3, OLD);
        w.fill(GB);
        w.advance(60_000);
        for d in &nodes[5..] {
            w.kill(*d);
        }
        w.advance(60_000);
        for d in &nodes[..5] {
            w.kill(*d);
            w.advance(30_000);
            assert!(w.boot(*d, 1));
            w.advance(60_000);
        }
        let joiner = w.create("a", OLD, 1);
        w.advance(30_000);
        let minority = [joiner, nodes[0], nodes[1], nodes[2]];
        w.part(&minority);
        w.quiet_hours(7);
        w.advance(60_000);
        kept(&w, &minority, seed);
    }
}

fn on_its_side(w: &World, by: u64, seed: u64) {
    let side = w.side(by);
    for d in w.running() {
        assert!(
            !w.knows(by, d) || w.marked_absent(by, d) || side.contains(&d),
            "seed {seed}: {by} sees {d} live across the cut"
        );
    }
}

#[test]
fn i16_a_node_that_boots_or_joins_during_a_partition_stays_on_its_side() {
    for seed in 0..SCENARIO_SEEDS {
        let mut w = World::new(seed);
        let nodes = w.start(5, 3, OLD);
        w.fill(GB);
        w.advance(60_000);
        w.kill(nodes[0]);
        w.part(&nodes[..2]);
        w.advance(60_000);
        let joiner = w.create("", 0, 1);
        on_its_side(&w, joiner, seed);
        assert!(w.boot(nodes[0], 1));
        w.advance(10_000);
        on_its_side(&w, nodes[0], seed);
        on_its_side(&w, joiner, seed);
        w.quiet_hours(7);
        w.advance(60_000);
        kept(&w, &nodes[..2], seed);
    }
}

#[test]
fn i16_the_oracle_records_a_loss_of_a_node_that_holds_bytes() {
    let mut w = World::new(1);
    let nodes = w.start(3, 3, OLD);
    w.fill(GB);
    w.advance(10_000);
    w.kill(nodes[0]);
    assert_eq!(w.losses.len(), 1);
}

#[test]
fn i16_the_oracle_uses_the_left_return_window_of_the_code() {
    let r = 10 * HOUR_MS;
    let left = r - 600_000;
    for (back, flagged) in [(150_000, 1), (100_000, 0)] {
        let mut w = World::new(1);
        w.losses.push(WorldLoss {
            disk: 7,
            run_ms: left,
            kind: LossKind::Left,
            side: BTreeSet::from([1, 7]),
        });
        w.boots.push((7, left + back));
        w.floor_changes.push(quiet(r, THREE, TWO));
        assert_eq!(w.illegal_falls().len(), flagged, "back {back}");
    }
}

#[test]
fn i16_a_return_after_the_join_window_is_still_a_loss() {
    for seed in 0..SCENARIO_SEEDS {
        let mut w = World::new(seed);
        let nodes = w.start(5, 3, OLD);
        w.fill(GB);
        let v = w.create("a", OLD, 1);
        w.place(&[nodes[0], nodes[1], nodes[2], nodes[3], v], SEGMENT_BYTES);
        w.quiet_hours(7);
        w.kill(v);
        w.advance(40_000);
        assert!(w.boot(v, 1));
        w.advance(10_000);
        w.stop(v);
        w.advance(120_000);
        let bad = w.illegal_falls();
        assert!(bad.is_empty(), "seed {seed}: {bad:?}");
    }
}
