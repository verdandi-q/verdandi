use std::collections::BTreeSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

use verdandi_core::cluster::controller::Floor;
use verdandi_sim::cluster_sim::{
    Cause, FloorChange, HOUR_MS, LossKind, SEGMENT_BYTES, World, WorldLoss, mix,
};

const MIXES: u64 = 1_000;
const ELECTION_MS: u64 = 25_000;
const STALL_MS: u64 = 15_000 + ELECTION_MS;
const MIX_STALL_MS: u64 = STALL_MS + 2 * verdandi_core::cluster::fence::FENCE_MS;
const EVENTS: usize = 20;
const THREADS: usize = 4;

#[test]
fn i16_the_floor_falls_only_by_drain_accept_loss_or_the_quiet_rule() {
    let next = AtomicU64::new(0);
    let falls = Mutex::new([0usize; 3]);
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
                    assert!(
                        world.worst_stall <= MIX_STALL_MS,
                        "seed {seed}: a stall of {} ms",
                        world.worst_stall
                    );
                    let mut falls = falls.lock().expect("falls");
                    for change in world
                        .floor_changes
                        .iter()
                        .filter(|c| c.to.value < c.from.value)
                    {
                        match change.cause {
                            Cause::Quiet => falls[0] += 1,
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
        "I16 MIXES PASS mixes={MIXES} quiet-falls={} drain-falls={} accept-loss-falls={}",
        falls[0], falls[1], falls[2]
    );
    assert!(falls.iter().all(|f| *f > 0), "{falls:?}");
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
        let minority = [joiner, nodes[0]];
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
    w.advance(20_000);
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

fn falls(w: &World) -> usize {
    w.floor_changes
        .iter()
        .filter(|c| c.to.value < c.from.value)
        .count()
}

#[test]
fn i16_seed_643_has_no_illegal_fall() {
    let world = mix(643, EVENTS);
    let bad = world.illegal_falls();
    assert!(bad.is_empty(), "{bad:?}");
}

fn left_then_down(seed: u64, hidden_run: bool) -> World {
    let mut w = World::new(seed);
    let n = w.start(4, 3, OLD);
    w.fill(GB);
    w.advance(60_000);
    w.stop(n[3]);
    w.advance(150_000);
    for d in &n[..3] {
        w.kill(*d);
    }
    if hidden_run {
        assert!(w.boot(n[3], 1));
        w.advance(20_000);
        w.kill(n[3]);
    }
    assert!(w.boot(n[0], 1));
    assert!(w.boot(n[1], 1));
    w.advance(60_000);
    w.quiet_hours(7);
    w.advance(60_000);
    w
}

#[test]
fn i16_a_left_member_that_runs_and_dies_unseen_keeps_the_floor() {
    for seed in 0..SCENARIO_SEEDS {
        let w = left_then_down(seed, true);
        assert!(
            w.illegal_falls().is_empty(),
            "seed {seed}: {:?}",
            w.illegal_falls()
        );
    }
}

#[test]
fn i16_a_left_member_removed_by_its_stop_gives_no_illegal_fall() {
    for seed in 0..SCENARIO_SEEDS {
        let w = left_then_down(seed, false);
        assert!(
            w.illegal_falls().is_empty(),
            "seed {seed}: {:?}",
            w.illegal_falls()
        );
    }
}

#[test]
fn i16_a_running_side_keeps_the_floor_when_left_members_return_on_the_other_side() {
    for seed in 0..5 {
        let mut w = World::new(seed);
        let n = w.start(5, 3, OLD);
        w.fill(GB);
        w.advance(60_000);
        for d in &n[2..] {
            w.stop(*d);
        }
        w.advance(150_000);
        w.part(&n[..2]);
        for d in &n[2..] {
            assert!(w.boot(*d, 1));
        }
        w.advance(60_000);
        w.quiet_hours(7);
        w.advance(60_000);
        assert!(
            w.illegal_falls().is_empty(),
            "seed {seed}: {:?}",
            w.illegal_falls()
        );
    }
}

#[test]
fn i16_old_nodes_that_restart_after_a_blackout_replace_no_death() {
    for seed in 0..SCENARIO_SEEDS {
        let mut w = World::new(seed);
        let n = w.start(4, 3, OLD);
        w.fill(GB);
        w.advance(60_000);
        w.kill(n[3]);
        w.advance(60_000);
        for d in &n[..3] {
            w.kill(*d);
        }
        assert!(w.boot(n[0], 1));
        assert!(w.boot(n[1], 1));
        w.advance(60_000);
        w.quiet_hours(7);
        w.advance(60_000);
        assert!(
            w.illegal_falls().is_empty(),
            "seed {seed}: {:?}",
            w.illegal_falls()
        );
        assert_eq!(falls(&w), 0, "seed {seed}");
    }
}

#[test]
fn i16_twenty_mixes_keep_leaders_and_reconcile() {
    for seed in 0..20 {
        let world = mix(seed, EVENTS);
        assert!(
            world.worst_stall <= STALL_MS,
            "seed {seed}: a stall of {} ms",
            world.worst_stall
        );
    }
}

#[test]
fn i16_mixes_with_a_lost_or_cut_holder_keep_leaders() {
    for seed in [17, 21, 36, 52] {
        let world = mix(seed, EVENTS);
        assert!(
            world.worst_stall <= STALL_MS,
            "seed {seed}: a stall of {} ms",
            world.worst_stall
        );
    }
}

#[test]
fn i16_the_oracle_majority_is_the_newest_list_linked_in_the_fence_time() {
    let mut w = World::new(1);
    let n = w.start(5, 3, OLD);
    w.advance(10_000);
    assert!(n.iter().all(|d| w.listed(*d)));
    w.part(&n[..2]);
    w.advance(2_000);
    assert!(!w.listed(n[0]) && !w.listed(n[1]));
    assert!(n[2..].iter().all(|d| w.listed(*d)));
    w.heal();
    w.kill(n[4]);
    w.advance(10_000);
    assert!(w.accept_loss(n[4]));
    w.advance(20_000);
    assert!(!w.in_list(n[4]));
    let j = w.create("a", OLD, 1);
    assert!(!w.listed(j));
}

#[test]
fn i16_the_oracle_refuses_a_drain_fall_before_its_commit() {
    let mut w = World::new(1);
    w.floor_changes.push(FloorChange {
        cause: Cause::Drain,
        ..quiet(10 * HOUR_MS, THREE, TWO)
    });
    assert_eq!(w.illegal_falls().len(), 1);
}
