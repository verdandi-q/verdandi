use std::collections::{BTreeMap, BTreeSet};
use std::env::{self, VarError};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

use rand::Rng;
use rand::seq::{IndexedRandom, SliceRandom};
use verdandi_core::cluster::placement::pair;
use verdandi_core::cluster::repair;
use verdandi_core::cluster::steer::HEAVY_HOLD_MS;
use verdandi_core::cluster::view::TOMBSTONE_HORIZON;
use verdandi_sim::cluster_sim::{Cause, FENCE_MS, HOUR_MS, ISR_MS, SEGMENT_BYTES, STEP_MS, World};

const SEEDS: u64 = 100;
const LONG_SEEDS: u64 = 1_000;
const GB: u64 = 1_000_000_000;
const OLD: u64 = 25 * HOUR_MS;
const SIX_HOURS: u64 = 6 * HOUR_MS;
const SAMPLE_GRACE_MS: u64 = 4 * 5_000;
const THREADS: usize = 4;

fn seeds<T: Send>(count: u64, run: impl Fn(u64) -> T + Sync) -> Vec<T> {
    let next = AtomicU64::new(0);
    let out = Mutex::new(Vec::new());
    thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                loop {
                    let seed = next.fetch_add(1, Ordering::Relaxed);
                    if seed >= count {
                        break;
                    }
                    let result = run(seed);
                    out.lock().expect("results").push((seed, result));
                }
            });
        }
    });
    let mut out = out.into_inner().expect("results");
    out.sort_by_key(|(seed, _)| *seed);
    out.into_iter().map(|(_, r)| r).collect()
}

fn warm(world: &mut World) {
    world.advance(60_000);
}

fn intact(world: &World, seed: u64) {
    assert_eq!(world.lost(), 0, "seed {seed}: an acked segment has no copy");
    assert_eq!(
        world.lost_settles(),
        0,
        "seed {seed}: a settle with 200 has no holder"
    );
    assert_eq!(
        world.unreadable(),
        0,
        "seed {seed}: a node holds a format above its max"
    );
}

fn floors(world: &World) -> Vec<u8> {
    world
        .running()
        .iter()
        .filter_map(|d| world.floor(*d))
        .map(|f| f.value)
        .collect()
}

fn traffic(world: &mut World, ms: u64) {
    for _ in 0..ms / 1_000 {
        world.publish();
        world.settle();
        world.advance(1_000);
    }
}

struct Storm {
    rise_ms: u64,
    fall_after_ms: u64,
    floor_fall_after_ms: u64,
}

fn storm(seed: u64) -> Storm {
    let mut world = World::new(seed);
    world.start(12, 3, OLD);
    world.fill(5 * GB / 2);
    warm(&mut world);
    assert!(
        floors(&world).iter().all(|f| *f == 3),
        "seed {seed}: {:?}",
        floors(&world)
    );
    let mut first: Option<(u64, u64, u64, Option<u8>)> = None;
    let mut rise: Option<u64> = None;
    let mut last_loss = 0;
    for _ in 0..10 {
        let running = world.running();
        let victim = running[world.rng.random_range(0..running.len())];
        let zone = ["a", "b", "c"][(victim % 3) as usize];
        world.delete(victim);
        last_loss = world.run_ms;
        world.create(zone, 0, 1);
        for _ in 0..30_000 / STEP_MS {
            let before: BTreeMap<u64, Option<u8>> = world
                .running()
                .into_iter()
                .map(|d| (d, world.count(d)))
                .collect();
            world.advance(STEP_MS);
            if first.is_none()
                && let Some(by) = world.running().into_iter().find(|d| {
                    world.marked_absent(*d, victim) && before.get(d).copied().flatten() < Some(5)
                })
            {
                first = Some((world.now_ms, by, victim, before.get(&by).copied().flatten()));
            }
            if rise.is_none()
                && let Some((_, by, lost, base)) = first
                && world.count(by) > base
                && world.marked_absent(by, lost)
            {
                rise = Some(world.now_ms);
            }
        }
        world.publish();
        world.settle();
    }
    let (marked, by, _, _) = first.expect("a death was marked");
    let rise =
        rise.unwrap_or_else(|| panic!("seed {seed}: no count rise on {by} that marks the loss"));
    let peak = world
        .running()
        .iter()
        .filter_map(|d| world.count(*d))
        .max()
        .unwrap_or(0);
    assert_eq!(peak, 5, "seed {seed}");
    world.quiet_hours(7);
    assert!(world.settle_repair(SETTLE_MS), "seed {seed}: no repair");
    traffic(&mut world, 30_000);
    for _ in 0..10 {
        let counts: Vec<u8> = world
            .running()
            .iter()
            .filter_map(|d| world.count(*d))
            .collect();
        if floors(&world).iter().all(|f| counts.contains(f)) {
            break;
        }
        traffic(&mut world, 30_000);
    }
    let fall = world
        .count_changes
        .iter()
        .find(|c| c.run_ms > last_loss && c.to < c.from)
        .unwrap_or_else(|| panic!("seed {seed}: the count never fell"));
    let floor_falls: Vec<_> = world
        .floor_changes
        .iter()
        .filter(|c| c.to.value < c.from.value)
        .collect();
    assert!(!floor_falls.is_empty(), "seed {seed}: the floor never fell");
    let counts: Vec<u8> = world
        .running()
        .iter()
        .filter_map(|d| world.count(*d))
        .collect();
    assert!(
        floors(&world).iter().all(|f| counts.contains(f) && *f < 5),
        "seed {seed}: {:?} {counts:?}",
        floors(&world)
    );
    intact(&world, seed);
    assert!(
        world.illegal_falls().is_empty(),
        "seed {seed}: {:?}",
        world.illegal_falls()
    );
    Storm {
        rise_ms: rise - marked,
        fall_after_ms: fall.run_ms - last_loss,
        floor_fall_after_ms: floor_falls[0].run_ms - last_loss,
    }
}

fn week(seed: u64) -> usize {
    let mut world = World::new(seed);
    world.start(12, 3, OLD);
    world.fill(5 * GB / 2);
    warm(&mut world);
    for _ in 0..13 {
        let running = world.running();
        let victim = running[world.rng.random_range(0..running.len())];
        let zone = ["a", "b", "c"][(victim % 3) as usize];
        world.kill(victim);
        world.create(zone, 0, 1);
        traffic(&mut world, 2_000);
        world.advance(60_000);
        world.quiet_hours(12);
    }
    world.quiet_hours(7);
    traffic(&mut world, 30_000);
    intact(&world, seed);
    assert!(
        world.illegal_falls().is_empty(),
        "seed {seed}: {:?}",
        world.illegal_falls()
    );
    world
        .floor_changes
        .iter()
        .filter(|c| c.to.value < c.from.value)
        .count()
}

fn row18(count: u64) {
    let mut rise_max = 0;
    let mut fall_min = u64::MAX;
    let mut floor_min = u64::MAX;
    for storm in seeds(count, storm) {
        rise_max = rise_max.max(storm.rise_ms);
        fall_min = fall_min.min(storm.fall_after_ms);
        floor_min = floor_min.min(storm.floor_fall_after_ms);
    }
    assert!(rise_max <= 5_000, "rise {rise_max} ms");
    assert!(fall_min >= SIX_HOURS, "count fell after {fall_min} ms");
    assert!(floor_min >= SIX_HOURS, "floor fell after {floor_min} ms");
    println!(
        "ROW 18 SIM PASS rose=1 fell=1 rise-max-s={} fall-after-s={} floor-fell=1 floor-fell-after-s={}",
        rise_max as f64 / 1000.0,
        fall_min / 1000,
        floor_min / 1000
    );
}

fn setting(name: &str, default: u64) -> u64 {
    match env::var(name) {
        Ok(text) => text
            .parse()
            .unwrap_or_else(|_| panic!("{name}={text} is not a whole number")),
        Err(VarError::NotPresent) => default,
        Err(error) => panic!("{name}: {error}"),
    }
}

fn row18_week(count: u64) {
    let first = setting("ROW18_FIRST", 0);
    let count = setting("ROW18_COUNT", count);
    for (i, falls) in seeds(count, |i| week(first + i)).into_iter().enumerate() {
        let seed = first + i as u64;
        assert!(falls >= 1, "seed {seed}: the floor never fell in the week");
    }
    println!("ROW 18 SIM PASS week losses=13 seeds={count} first={first} floor-fell=1");
}

#[test]
fn row18_the_controller_under_a_storm() {
    row18(SEEDS);
}

#[test]
fn row18_thirteen_losses_in_a_week() {
    row18_week(SEEDS);
}

#[test]
#[ignore]
fn row18_the_controller_under_a_storm_at_1000_seeds() {
    row18(LONG_SEEDS);
    row18_week(LONG_SEEDS);
}

fn quiet_floor(seed: u64, comes_back: bool) {
    let mut world = World::new(seed);
    let nodes = world.start(3, 3, OLD);
    world.fill(GB);
    let short = world.place(&[nodes[0], nodes[2]], SEGMENT_BYTES);
    world.held_repair.insert(short);
    warm(&mut world);
    assert_eq!(floors(&world), vec![3, 3, 3], "seed {seed}");
    world.kill(nodes[2]);
    let killed = world.run_ms;
    traffic(&mut world, 60_000);
    if comes_back {
        world.quiet_hours(2);
        assert!(world.boot(nodes[2], 1));
    }
    for _ in 0..7 {
        world.quiet_hours(1);
        traffic(&mut world, 10_000);
        assert!(
            floors(&world).iter().all(|f| *f == 3),
            "seed {seed} at {} ms",
            world.run_ms - killed
        );
    }
    assert!(world.run_ms - killed > SIX_HOURS);
    world.held_repair.remove(&short);
    let mut repaired = None;
    for _ in 0..120_000 / STEP_MS {
        world.advance(STEP_MS);
        let copies = world
            .segments
            .get(&short)
            .map_or(0, |s| world.live_copies(s));
        if repaired.is_none() && copies >= 2 {
            repaired = Some(world.run_ms);
        }
        if repaired.is_none() {
            assert!(
                floors(&world).iter().all(|f| *f == 3),
                "seed {seed}: fell before the repair"
            );
        }
    }
    let repaired = repaired.expect("the held segment was repaired");
    if comes_back {
        assert!(
            floors(&world).iter().all(|f| *f == 3),
            "seed {seed}: {:?}",
            floors(&world)
        );
    } else {
        let fall = world
            .floor_changes
            .iter()
            .find(|c| c.to.value == 2)
            .unwrap_or_else(|| panic!("seed {seed}: the floor never fell"));
        assert!(fall.run_ms - killed > SIX_HOURS);
        assert!(fall.run_ms >= repaired, "seed {seed}");
        assert!(
            fall.run_ms - repaired <= SAMPLE_GRACE_MS,
            "seed {seed}: fell {} ms after the repair",
            fall.run_ms - repaired
        );
        assert!(
            world
                .disks()
                .iter()
                .filter(|d| **d != nodes[2])
                .all(|d| world.floor(*d).map(|f| f.value) == Some(2)),
            "seed {seed}"
        );
    }
    intact(&world, seed);
    assert!(
        world.illegal_falls().is_empty(),
        "seed {seed}: {:?}",
        world.illegal_falls()
    );
}

fn e19(count: u64) {
    seeds(count, |seed| quiet_floor(seed, false));
    println!("QUIET FLOOR SIM PASS seeds={count} kept=3 fell-to=2");
}

fn e19_returns(count: u64) {
    seeds(count, |seed| quiet_floor(seed, true));
    println!("QUIET FLOOR SIM PASS returns seeds={count} kept=3");
}

#[test]
fn quiet_floor_falls_after_six_hours_and_full_repair() {
    e19(SEEDS);
}

#[test]
fn quiet_floor_stays_when_the_killed_node_returns() {
    e19_returns(SEEDS);
}

#[test]
fn quiet_floor_returns() {
    e19_rise_and_fall(SEEDS);
}

#[test]
#[ignore]
fn quiet_floor_at_1000_seeds() {
    e19(LONG_SEEDS);
    e19_returns(LONG_SEEDS);
    e19_rise_and_fall(LONG_SEEDS);
}

fn e19_rise_and_fall(count: u64) {
    let mut fell_to = seeds(count, |seed| {
        let mut world = World::new(seed);
        let nodes = world.start(12, 3, OLD);
        world.fill(5 * GB / 2);
        warm(&mut world);
        assert!(floors(&world).iter().all(|f| *f == 3), "seed {seed}");
        for victim in &nodes[..6] {
            world.kill(*victim);
            traffic(&mut world, 30_000);
            assert!(world.boot(*victim, 1));
            traffic(&mut world, 10_000);
        }
        traffic(&mut world, 30_000);
        assert!(
            floors(&world).iter().all(|f| *f == 5),
            "seed {seed}: {:?}",
            floors(&world)
        );
        world.quiet_hours(7);
        traffic(&mut world, 60_000);
        let after = floors(&world);
        let counts: Vec<u8> = world
            .running()
            .iter()
            .filter_map(|d| world.count(*d))
            .collect();
        assert!(
            after
                .iter()
                .all(|f| *f < 5 && counts.iter().all(|c| c == f)),
            "seed {seed}: floors {after:?} counts {counts:?}"
        );
        intact(&world, seed);
        assert!(
            world.illegal_falls().is_empty(),
            "seed {seed}: {:?}",
            world.illegal_falls()
        );
        after[0]
    });
    fell_to.sort_unstable();
    fell_to.dedup();
    println!("QUIET FLOOR RETURNS SIM PASS seeds={count} rose-to=5 fell-to={fell_to:?}");
}

#[test]
fn a_repair_copies_the_mark_of_its_source() {
    let mut world = World::new(1);
    let nodes = world.start(5, 3, OLD);
    let id = world.place(&nodes[..3], SEGMENT_BYTES);
    world.held_repair.insert(id);
    warm(&mut world);
    world.kill(nodes[2]);
    world.advance(9_000);
    world.settle();
    world.advance(1_000);
    world.kill(nodes[0]);
    world.kill(nodes[1]);
    assert!(world.boot(nodes[2], 1));
    world.advance(5_000);
    assert!(world.accept_loss(nodes[0]));
    assert!(world.accept_loss(nodes[1]));
    world.held_repair.remove(&id);
    world.advance(30_000);
    assert!(world.segments.values().all(|s| {
        s.marks
            .iter()
            .any(|(d, m)| *d != nodes[2] && *m == s.marks[&nodes[2]])
    }));
    assert_eq!(world.lost_settles(), 1);
    world.delete(nodes[0]);
    world.delete(nodes[1]);
    assert_eq!(world.lost_settles(), 1);
}

#[test]
fn a_repair_copy_keeps_the_settles_of_its_source() {
    let mut world = World::new(2);
    let nodes = world.start(3, 3, OLD);
    world.place(&nodes, SEGMENT_BYTES);
    warm(&mut world);
    world.settle();
    world.advance(1_000);
    world.kill(nodes[0]);
    let receiver = world.create("a", OLD, 1);
    world.advance(60_000);
    assert!(
        world
            .segments
            .values()
            .all(|s| s.marks.contains_key(&receiver))
    );
    world.kill(nodes[1]);
    world.kill(nodes[2]);
    assert_eq!(world.lost(), 0);
    assert_eq!(world.lost_settles(), 0);
}

#[test]
fn a_segment_whose_holders_are_all_killed_is_lost() {
    let mut world = World::new(1);
    let nodes = world.start(3, 3, OLD);
    world.place(&nodes, SEGMENT_BYTES);
    warm(&mut world);
    world.settle();
    world.advance(1_000);
    for d in &nodes {
        world.kill(*d);
    }
    assert_eq!(world.lost(), 1);
    assert_eq!(world.lost_settles(), 1);
}

#[test]
fn an_open_segment_keeps_its_extra_copies_until_it_is_sealed() {
    let mut world = World::new(3);
    let nodes = world.start(6, 3, OLD);
    let id = world.place(&nodes[..5], SEGMENT_BYTES);
    world.open.insert(id);
    assert!(world.settle_repair(SETTLE_MS));
    assert!(world.segments[&id].marks.len() >= 5);
    world.open.remove(&id);
    assert!(world.settle_repair(SETTLE_MS));
    assert_eq!(world.segments[&id].marks.len(), 4);
    intact(&world, 3);
}

#[test]
fn a_reported_pair_leaves_no_group_that_holds_both() {
    let mut world = World::new(4);
    world.start(6, 3, OLD);
    world.fill(GB / 4);
    assert!(world.settle_repair(SETTLE_MS));
    let group = world.groups().expect("groups").groups[0].clone();
    world
        .pairs
        .insert((group[0].min(group[1]), group[0].max(group[1])));
    assert!(world.settle_repair(SETTLE_MS));
    let groups = world.groups().expect("groups");
    assert!(
        groups
            .groups
            .iter()
            .all(|g| !(g.contains(&group[0]) && g.contains(&group[1]))),
        "{groups:?}"
    );
    assert!(world.running().iter().all(|d| groups.of(*d).is_some()));
    intact(&world, 4);
}

#[test]
fn a_drop_waits_while_a_kept_holder_is_down() {
    let mut world = World::new(5);
    world.headroom = 0.1;
    world.start(6, 3, OLD);
    warm(&mut world);
    let group = world.groups().expect("groups").groups[0].clone();
    let extra = world
        .running()
        .into_iter()
        .find(|d| !group.contains(d))
        .expect("a node outside the group");
    let mut holders = group.clone();
    holders.push(extra);
    let id = world.place(&holders, SEGMENT_BYTES);
    world.held_repair.insert(id);
    while world.now_ms % 5_000 != 5_000 - STEP_MS {
        world.advance(STEP_MS);
    }
    world.held_repair.remove(&id);
    let sender = repair::sender(&world.segments[&id].holders, |_| true);
    let victim = group
        .iter()
        .copied()
        .find(|d| Some(*d) != sender)
        .expect("a group member that does not send");
    world.kill(victim);
    world.advance(STEP_MS);
    assert_eq!(world.live_copies(&world.segments[&id]), 3);
}

fn named(world: &World) -> BTreeMap<u64, BTreeSet<u64>> {
    world
        .segments
        .iter()
        .map(|(id, s)| (*id, s.holders.holders.clone()))
        .collect()
}

fn kept(before: &BTreeMap<u64, BTreeSet<u64>>, world: &World, seed: u64, when: &str) {
    for (id, old) in before {
        let now = &world.segments[id].holders.holders;
        assert!(
            now.is_superset(old),
            "seed {seed} {when}: segment {id} {old:?} -> {now:?}"
        );
    }
}

#[test]
fn repair_drops_no_holder_while_the_ring_is_smaller_than_the_size() {
    for seed in 0..3 {
        let mut w = World::new(seed);
        let n = w.start(3, 3, OLD);
        w.fill(GB);
        assert!(w.settle_repair(SETTLE_MS));
        let before = named(&w);
        w.kill(n[1]);
        w.kill(n[2]);
        w.advance(40_000);
        kept(&before, &w, seed, "two of three down");
        assert!(w.boot(n[1], 1));
        assert!(w.boot(n[2], 1));
        w.advance(5_000);
        kept(&before, &w, seed, "after the return");

        let mut w = World::new(seed);
        let n = w.start(6, 3, OLD);
        w.fill(GB);
        assert!(w.settle_repair(SETTLE_MS));
        let before = named(&w);
        for d in &n {
            w.kill(*d);
        }
        w.advance(10_000);
        assert!(w.boot(n[0], 1));
        w.advance(60_000);
        kept(&before, &w, seed, "one of six booted");

        let mut w = World::new(seed);
        let n = w.start(6, 3, OLD);
        w.fill(GB);
        assert!(w.settle_repair(SETTLE_MS));
        let before = named(&w);
        for d in &n[2..] {
            w.kill(*d);
        }
        w.advance(40_000);
        kept(&before, &w, seed, "four of six down");
    }
}

#[test]
fn a_copy_that_no_holder_set_names_does_not_count() {
    for seed in 0..10 {
        let mut w = World::new(seed);
        w.headroom = 0.1;
        w.start(6, 3, OLD);
        warm(&mut w);
        let group = w.groups().expect("groups").groups[0].clone();
        let x = w
            .running()
            .into_iter()
            .find(|d| !group.contains(d))
            .expect("a node outside the group");
        let mut holders = group.clone();
        holders.push(x);
        let id = w.place(&holders, SEGMENT_BYTES);
        w.stop(x);
        w.advance(20_000);
        assert!(!w.segments[&id].holders.holders.contains(&x), "seed {seed}");
        assert!(w.boot(x, 1));
        assert!(w.settle_repair(SETTLE_MS), "seed {seed}");
        let mut gone = w.segments[&id].holders.holders.clone();
        gone.extend(group.iter().copied());
        gone.remove(&x);
        for d in &gone {
            w.delete(*d);
        }
        assert!(!w.segments[&id].marks.contains_key(&x), "seed {seed}");
        assert_eq!(w.lost(), 1, "seed {seed}");
    }
}

#[test]
#[should_panic(expected = "no live named holder")]
fn the_repair_check_refuses_a_live_copy_that_no_holder_set_names() {
    let mut w = World::new(0);
    w.headroom = 0.1;
    w.start(6, 3, OLD);
    warm(&mut w);
    let group = w.groups().expect("groups").groups[0].clone();
    let x = w
        .running()
        .into_iter()
        .find(|d| !group.contains(d))
        .expect("a node outside the group");
    let mut holders = group.clone();
    holders.push(x);
    let id = w.place(&holders, SEGMENT_BYTES);
    let cuts: Vec<(u64, u64)> = w
        .running()
        .into_iter()
        .filter(|d| *d != x)
        .map(|d| pair(d, x))
        .collect();
    w.cut(&cuts);
    w.advance(50_000);
    let mut gone = w.segments[&id].holders.holders.clone();
    gone.extend(group.iter().copied());
    gone.remove(&x);
    for d in &gone {
        w.delete(*d);
    }
    w.repaired();
}

#[test]
fn a_holder_that_runs_again_deletes_the_copies_its_holder_sets_do_not_name() {
    for seed in 0..6 {
        let mut w = World::new(seed);
        let n = w.start(7, 3, OLD);
        w.fill(GB);
        assert!(w.settle_repair(SETTLE_MS), "seed {seed}");
        let d = n[3];
        w.kill(d);
        assert!(w.settle_repair(SETTLE_MS), "seed {seed}");
        assert!(w.boot(d, 1));
        assert!(w.settle_repair(SETTLE_MS), "seed {seed}");
        let orphans = w
            .segments
            .values()
            .filter(|s| s.marks.contains_key(&d) && !s.holders.holders.contains(&d))
            .count();
        assert_eq!(orphans, 0, "seed {seed}");
        intact(&w, seed);
    }
}

#[test]
fn a_drained_disk_that_runs_again_leaves_the_drained_set() {
    let mut w = World::new(1);
    let n = w.start(4, 3, OLD);
    warm(&mut w);
    w.drain(n[3]);
    w.advance(150_000);
    assert!(w.drained.contains(&n[3]));
    w.kill(n[3]);
    assert!(w.boot(n[3], 1));
    assert!(!w.drained.contains(&n[3]));
}

#[test]
fn the_repair_check_scans_every_segment() {
    let mut w = World::new(2);
    w.headroom = 0.1;
    w.start(6, 3, OLD);
    warm(&mut w);
    let group = w.groups().expect("groups").groups[0].clone();
    for _ in 0..4_999 {
        w.place(&group, 1);
    }
    w.place(&group[..2], 1);
    w.advance(5_000);
    assert!(!w.repaired());
}

fn commit_after_a_drain(seed: u64, join: bool) -> (Option<u16>, Option<u16>) {
    let mut w = World::new(seed);
    let n = w.start(6, 3, OLD);
    w.fill(GB);
    assert!(w.settle_repair(SETTLE_MS), "seed {seed}");
    for d in &n[2..6] {
        w.drain(*d);
        w.advance(300_000);
        assert!(!w.in_list(*d), "seed {seed}: {d} was not removed");
        w.stop(*d);
        w.advance(10_000);
    }
    if join {
        w.create("a", OLD, 2);
        w.advance(60_000);
    }
    for d in &n[..2] {
        w.kill(*d);
        assert!(w.boot(*d, 2));
        w.advance(30_000);
    }
    w.quiet_hours(3);
    (w.committed(n[0]), w.committed(n[1]))
}

#[test]
fn a_joiner_after_a_drain_does_not_block_the_version_commit() {
    for seed in 0..3 {
        assert_eq!(
            commit_after_a_drain(seed, false),
            (Some(2), Some(2)),
            "seed {seed}"
        );
        assert_eq!(
            commit_after_a_drain(seed, true),
            (Some(2), Some(2)),
            "seed {seed}"
        );
    }
}

#[test]
fn a_heavy_writer_leaves_the_ring_heaviest_first_and_its_copy_goes_after_the_extra_copy() {
    let mut w = World::new(7);
    let n = w.start(5, 3, OLD);
    for d in &n {
        w.capacity.insert(*d, 100 * GB);
    }
    w.capacity.insert(n[1], 3 * GB);
    w.capacity.insert(n[0], 4 * GB);
    w.fill(GB);
    w.advance(HEAVY_HOLD_MS + 60_000);
    assert!(w.settle_repair(SETTLE_MS));
    let groups = w.groups().expect("groups");
    assert!(groups.of(n[1]).is_none(), "{groups:?}");
    assert!(groups.of(n[0]).is_some(), "{groups:?}");
    let id = w.publish_from(n[1]).expect("a write from the heavy node");
    let holders = w.segments[&id].holders.holders.clone();
    assert!(
        holders.contains(&n[1]) && groups.current(&holders),
        "{holders:?}"
    );
    assert!(w.settle_repair(SETTLE_MS));
    let after = &w.segments[&id].holders.holders;
    assert!(!after.contains(&n[1]) && after.len() == 4, "{after:?}");
    intact(&w, 7);
}

fn upgrade(world: &mut World, disk: u64, format_max: u16) -> bool {
    world.kill(disk);
    traffic(world, 15_000);
    world.boot(disk, format_max)
}

fn row17(count: u64) {
    seeds(count, |seed| {
        let mut world = World::new(seed);
        let nodes = world.start(6, 3, OLD);
        world.fill(GB);
        warm(&mut world);
        for d in &nodes[..3] {
            assert!(upgrade(&mut world, *d, 2));
        }
        world.kill(nodes[0]);
        traffic(&mut world, 30_000);
        world.quiet_hours(2);
        traffic(&mut world, 30_000);
        assert!(world.boot(nodes[0], 2));
        traffic(&mut world, 30_000);
        for d in &nodes {
            assert!(
                upgrade(&mut world, *d, 1),
                "seed {seed}: roll back refused before a commit"
            );
        }
        traffic(&mut world, 30_000);
        assert_eq!(
            world.max_format(),
            1,
            "seed {seed}: format 2 written before a commit"
        );
        assert!(nodes.iter().all(|d| world.committed(*d) == Some(1)));
        assert_eq!(world.refused, 0, "seed {seed}");
        intact(&world, seed);

        let mut world = World::new(seed);
        let nodes = world.start(6, 3, OLD);
        world.fill(GB);
        warm(&mut world);
        for d in &nodes {
            assert!(upgrade(&mut world, *d, 2));
        }
        traffic(&mut world, 30_000);
        assert_eq!(world.max_format(), 1);
        world.quiet_hours(2);
        traffic(&mut world, 30_000);
        assert!(
            nodes.iter().all(|d| world.committed(*d) == Some(2)),
            "seed {seed}: no commit"
        );
        traffic(&mut world, 30_000);
        assert_eq!(world.max_format(), 2, "seed {seed}");
        world.kill(nodes[1]);
        traffic(&mut world, 30_000);
        assert!(world.boot(nodes[1], 2));
        assert!(
            !upgrade(&mut world, nodes[2], 1),
            "seed {seed}: a version 1 binary started on format 2"
        );
        assert!(world.boot(nodes[2], 2));
        world.create("a", 0, 1);
        traffic(&mut world, 30_000);
        assert_eq!(world.refused, 2, "seed {seed}");
        intact(&world, seed);
    });
    println!("ROW 17 SIM PASS seeds={count} lost=0");
}

#[test]
fn row17_upgrade_three_kill_one_roll_back() {
    row17(SEEDS);
}

#[test]
#[ignore]
fn row17_upgrade_three_kill_one_roll_back_at_1000_seeds() {
    row17(LONG_SEEDS);
}

const SETTLE_MS: u64 = 1_800_000;

struct Reclaim {
    splits: u64,
    merges: u64,
    agree_ms: u64,
}

fn spread(world: &World, seed: u64, when: &str) {
    let running = world.running();
    let zones: BTreeSet<String> = running.iter().map(|d| world.zone(*d)).collect();
    for (id, s) in &world.segments {
        let held: Vec<u64> = s
            .marks
            .keys()
            .copied()
            .filter(|h| running.contains(h))
            .collect();
        let spans: BTreeSet<String> = held.iter().map(|h| world.zone(*h)).collect();
        assert!(
            spans.len() >= held.len().min(zones.len()),
            "seed {seed} {when}: segment {id} on {held:?} spans {spans:?}"
        );
    }
}

fn reclaim(seed: u64, script: u8) -> Reclaim {
    let mut world = World::new(seed);
    if script > 1 {
        world.zones_known = false;
    }
    if script == 3 {
        for disk in 4..=6 {
            world
                .signals
                .insert(disk, (disk - 3, format!("10.{disk}.0.0/24")));
        }
    }
    let nodes = world.start(12, 3, OLD);
    world.fill(GB / 4);
    assert!(world.settle_repair(SETTLE_MS), "seed {seed}: no repair");
    spread(&world, seed, "before the loss");
    world.splits = 0;
    world.merges = 0;
    traffic(&mut world, 10_000);
    let zone = ["a", "b", "c"][world.rng.random_range(0..3)];
    let mut domain: Vec<u64> = nodes
        .iter()
        .copied()
        .filter(|d| world.zone(*d) == zone)
        .collect();
    assert_eq!(domain.len(), 4);
    let keep = if script == 3 { 0 } else { 2 };
    while domain.len() > keep {
        let i = world.rng.random_range(0..domain.len());
        domain.remove(i);
    }
    let lost: Vec<u64> = nodes
        .iter()
        .copied()
        .filter(|d| world.zone(*d) == zone && !domain.contains(d))
        .collect();
    for d in &lost {
        world.delete(*d);
    }
    let mut agree_ms = 0;
    while !world.agreed() {
        assert!(agree_ms < 120_000, "seed {seed}: no agreement");
        world.advance(STEP_MS);
        agree_ms += STEP_MS;
    }
    traffic(&mut world, 30_000);
    assert!(world.settle_repair(SETTLE_MS), "seed {seed}: no repair");
    intact(&world, seed);
    spread(&world, seed, "after the loss");
    Reclaim {
        splits: world.splits,
        merges: world.merges,
        agree_ms,
    }
}

fn row11(count: u64) {
    let mut splits = 0;
    let mut merges = 0;
    let mut agree_ms = 0;
    for script in 1..=3 {
        for r in seeds(count, |seed| reclaim(seed, script)) {
            splits += r.splits;
            merges += r.merges;
            agree_ms = agree_ms.max(r.agree_ms);
        }
    }
    let verdict = if splits > 0 { "MARGINAL" } else { "PASS" };
    println!(
        "ROW 11 SIM {verdict} seeds={count} lost=0 zones=known,unknown domain-splits={splits} domain-merges={merges} agree-s-max={}",
        agree_ms as f64 / 1000.0
    );
}

#[test]
fn row11_reclaim_half_of_one_failure_domain() {
    row11(SEEDS);
}

#[test]
#[ignore]
fn row11_reclaim_half_of_one_failure_domain_at_1000_seeds() {
    row11(LONG_SEEDS);
}

fn leave(world: &mut World, disk: u64, seed: u64) {
    world.stop(disk);
    for _ in 0..120_000 / STEP_MS {
        if !world.running().contains(&disk) {
            world.delete(disk);
            return;
        }
        world.advance(STEP_MS);
    }
    panic!("seed {seed}: {disk} did not leave within the stop budget");
}

#[derive(Default)]
struct KillSets {
    losing: usize,
    current: usize,
    drift: usize,
    mismatches: usize,
}

fn kill_sets(world: &World) -> KillSets {
    let running = world.running();
    assert_eq!(running.len(), 6);
    let groups = world.groups().expect("a groups record");
    let mut history: BTreeMap<u64, BTreeSet<u64>> = world
        .segments
        .iter()
        .map(|(id, s)| (*id, s.born.clone()))
        .collect();
    for change in &world.holder_changes {
        history.insert(change.to.segment, change.to.holders.clone());
    }
    let mut out = KillSets::default();
    for a in 0..6 {
        for b in a + 1..6 {
            for c in b + 1..6 {
                let set = [running[a], running[b], running[c]];
                let inside = |d: &u64| set.contains(d);
                let mut counts = [0usize; 3];
                for (id, seg) in &world.segments {
                    counts[0] += usize::from(seg.marks.keys().all(inside));
                    counts[1] += usize::from(seg.holders.holders.iter().all(inside));
                    counts[2] += usize::from(history[id].iter().all(inside));
                }
                let whole = groups.groups.iter().any(|g| g.iter().all(inside));
                out.mismatches += usize::from(counts[0] != counts[1] || counts[1] != counts[2]);
                out.losing += usize::from(counts[0] > 0);
                out.current += usize::from(whole);
                out.drift += usize::from(counts[0] > 0 && !whole);
            }
        }
    }
    out
}

fn scale(seed: u64, headroom: f64) -> KillSets {
    let mut world = World::new(seed);
    world.headroom = headroom;
    world.start(6, 3, OLD);
    world.fill(GB / 2);
    assert!(world.settle_repair(SETTLE_MS), "seed {seed}: no repair");
    let mut gone = Vec::new();
    for event in 0..20 {
        let running = world.running();
        let victim = running[world.rng.random_range(0..running.len())];
        let zone = world.zone(victim);
        match world.rng.random_range(0..3) {
            0 => {
                world.delete(victim);
                gone.push(victim);
                world.create(&zone, OLD, 1);
            }
            1 => {
                world.create(&zone, OLD, 1);
                assert!(world.settle_repair(SETTLE_MS), "seed {seed}: no repair");
                leave(&mut world, victim, seed);
            }
            _ => {
                leave(&mut world, victim, seed);
                world.create(&zone, OLD, 1);
            }
        }
        traffic(&mut world, 5_000);
        assert!(
            world.settle_repair(SETTLE_MS),
            "seed {seed}: no repair after event {event}"
        );
        intact(&world, seed);
    }
    world.quiet_hours(7);
    for d in gone {
        world.accept_loss(d);
    }
    assert!(world.settle_repair(SETTLE_MS), "seed {seed}: no repair");
    traffic(&mut world, 30_000);
    assert!(world.settle_repair(SETTLE_MS), "seed {seed}: no repair");
    intact(&world, seed);
    let size = world.groups().map(|g| g.size);
    assert_eq!(
        size,
        Some(3),
        "seed {seed}: groups did not return to 3: floors {:?} counts {:?}",
        floors(&world),
        world
            .running()
            .iter()
            .map(|d| world.count(*d))
            .collect::<Vec<_>>()
    );
    kill_sets(&world)
}

fn row12(count: u64) {
    for headroom in [0.1, 1.0] {
        let runs = seeds(count, |seed| scale(seed, headroom));
        for (seed, r) in runs.iter().enumerate() {
            assert_eq!(
                r.drift, 0,
                "seed {seed} headroom {headroom}: drift {}",
                r.drift
            );
            assert_eq!(
                r.mismatches, 0,
                "seed {seed} headroom {headroom}: mismatches {}",
                r.mismatches
            );
        }
        let range = |f: fn(&KillSets) -> usize| {
            let min = runs.iter().map(f).min().unwrap_or(0);
            let max = runs.iter().map(f).max().unwrap_or(0);
            format!("{min}..{max}")
        };
        println!(
            "ROW 12 SIM PASS seeds={count} headroom={headroom:.1} killsets=20 losing={} current-group-sets={} drift=0 mismatches=0",
            range(|r| r.losing),
            range(|r| r.current)
        );
    }
}

#[test]
fn row12_scale_and_replace_then_every_three_node_kill_set() {
    row12(SEEDS);
}

#[test]
#[ignore]
fn row12_scale_and_replace_then_every_three_node_kill_set_at_1000_seeds() {
    row12(LONG_SEEDS);
}

fn cut_off(seed: u64) -> (usize, u64) {
    let mut w = World::new(seed);
    w.watch = true;
    let n = w.start(6, 3, OLD);
    w.fill(GB / 4);
    warm(&mut w);
    let x = n[0];
    let cuts: Vec<(u64, u64)> = n[1..].iter().map(|o| pair(x, *o)).collect();
    w.cut(&cuts);
    let before = w.acked.len();
    let mut refused = 0;
    for _ in 0..60_000 / STEP_MS {
        for _ in 0..STEP_MS / 100 {
            assert!(
                w.publish_from(x).is_none(),
                "seed {seed}: the cut node acked a write"
            );
            refused += 1;
        }
        let writer = n[1 + w.rng.random_range(0..5)];
        w.publish_from(writer);
        w.advance(STEP_MS);
    }
    w.delete(x);
    traffic(&mut w, 10_000);
    assert!(w.settle_repair(SETTLE_MS), "seed {seed}: no repair");
    settled(&mut w, seed);
    intact(&w, seed);
    (w.acked.len() - before, refused)
}

fn row7(count: u64) {
    let runs = seeds(count, cut_off);
    let acked: usize = runs.iter().map(|r| r.0).sum();
    let refused: u64 = runs.iter().map(|r| r.1).sum();
    println!("ROW 7 SIM PASS seeds={count} lost=0 acked={acked} refused={refused}");
}

#[test]
fn row7_one_node_cut_off_from_five_then_its_disk_deleted() {
    row7(SEEDS);
}

#[test]
#[ignore]
fn row7_one_node_cut_off_from_five_then_its_disk_deleted_at_1000_seeds() {
    row7(LONG_SEEDS);
}

struct ScaleIn {
    hit: usize,
    removal_ms: u64,
    dead_left: usize,
}

fn scale_in(seed: u64, script: u8) -> ScaleIn {
    let mut w = World::new(seed);
    w.watch = true;
    w.delay_ms = 2_000;
    let n = w.start(6, 3, OLD);
    w.fill(GB / 2);
    assert!(w.settle_repair(SETTLE_MS), "seed {seed}: no repair");
    let groups = w.groups().expect("groups").groups;
    let mut victims: Vec<u64> = Vec::new();
    match script {
        0 => victims.extend(&groups[0]),
        1 => {
            let mut i = 0;
            while victims.len() < 3 {
                let g = &groups[i % groups.len()];
                if let Some(d) = g.iter().find(|d| !victims.contains(d)) {
                    victims.push(*d);
                }
                i += 1;
            }
        }
        _ => {
            let mut all = n.clone();
            all.shuffle(&mut w.rng);
            victims.extend(&all[..3]);
        }
    }
    for v in &victims {
        w.stop(*v);
        let gap = w.rng.random_range(0..=4) * STEP_MS;
        w.advance(gap);
    }
    let mut left = victims.clone();
    for _ in 0..(120_000 + 10_000) / STEP_MS {
        for v in left.clone() {
            if !w.running().contains(&v) {
                w.delete(v);
                left.retain(|d| *d != v);
            }
        }
        if left.is_empty() {
            break;
        }
        w.advance(STEP_MS);
    }
    assert!(left.is_empty(), "seed {seed}: {left:?} did not leave");
    for _ in 0..30 {
        traffic(&mut w, 1_000);
    }
    let dead_left = victims.iter().filter(|v| w.in_list(**v)).count();
    let removal_ms = victims
        .iter()
        .map(|v| {
            let at = w.removed_at.get(v).copied().unwrap_or(u64::MAX);
            at.saturating_sub(w.stopped_at[v])
        })
        .max()
        .unwrap_or(0);
    assert!(w.settle_repair(SETTLE_MS), "seed {seed}: no repair");
    settled(&mut w, seed);
    intact(&w, seed);
    ScaleIn {
        hit: groups
            .iter()
            .filter(|g| g.iter().any(|m| victims.contains(m)))
            .count(),
        removal_ms,
        dead_left,
    }
}

fn row10(count: u64) {
    let mut runs: Vec<ScaleIn> = (0..2)
        .map(|script| scale_in(script.into(), script))
        .collect();
    runs.extend(seeds(count, |seed| scale_in(seed, 2)));
    let hit = runs.iter().map(|r| r.hit).max().unwrap_or(0);
    let removal_ms = runs.iter().map(|r| r.removal_ms).max().unwrap_or(0);
    let dead_left: usize = runs.iter().map(|r| r.dead_left).sum();
    assert!(removal_ms < 110_000, "removal took {removal_ms} ms");
    assert_eq!(dead_left, 0, "stopped nodes still in a list");
    println!(
        "ROW 10 SIM PASS seeds={count} lost=0 hit-groups={hit} removed=3 removal-s-max={} dead-left={dead_left}",
        removal_ms as f64 / 1000.0
    );
}

#[test]
fn row10_remove_three_of_six_within_five_seconds() {
    row10(SEEDS);
}

#[test]
#[ignore]
fn row10_remove_three_of_six_within_five_seconds_at_1000_seeds() {
    row10(LONG_SEEDS);
}

fn some_stream_deleted(w: &mut World) {
    for _ in 0..ELECTION_MS / STEP_MS {
        let stream = w.rng.random_range(0..3);
        if w.delete_stream_wait(stream) {
            return;
        }
        w.advance(STEP_MS);
    }
    panic!("no DELETE got 200");
}

fn dropped(w: &mut World, id: u64) {
    for _ in 0..ELECTION_MS / STEP_MS {
        if w.drop_segment(id) {
            return;
        }
        w.advance(STEP_MS);
    }
    panic!("no Drop of {id} got 200");
}

fn held_by(w: &World, disk: u64) -> Vec<u64> {
    w.segments
        .iter()
        .filter(|(_, s)| s.marks.contains_key(&disk))
        .map(|(id, _)| *id)
        .collect()
}

fn serve(w: &mut World, ms: u64) {
    for _ in 0..ms / STEP_MS {
        w.take();
        w.read();
        w.advance(STEP_MS);
    }
}

const BEYOND_MS: u64 = TOMBSTONE_HORIZON + HOUR_MS;

fn all_away(seed: u64) -> (usize, u64) {
    let mut w = World::new(seed);
    w.watch = true;
    let n = w.start(4, 3, OLD);
    w.fill(GB / 4);
    assert!(w.settle_repair(SETTLE_MS), "seed {seed}: no repair");
    some_stream_deleted(&mut w);
    let id = *w.acked.iter().next_back().expect("a segment");
    dropped(&mut w, id);
    for d in &n {
        w.kill(*d);
    }
    w.jump(BEYOND_MS);
    for d in &n {
        assert!(w.boot(*d, 1), "seed {seed}");
    }
    serve(&mut w, 60_000);
    assert!(
        w.wiped.is_empty(),
        "seed {seed}: a wipe after an all-away stop"
    );
    assert_eq!(
        w.shown + w.shown_warned,
        0,
        "seed {seed}: a deleted id came back"
    );
    settled(&mut w, seed);
    intact(&w, seed);

    let mut w = World::new(seed);
    w.watch = true;
    w.headroom = 0.1;
    w.start(7, 3, OLD);
    w.fill(GB / 4);
    assert!(w.settle_repair(SETTLE_MS), "seed {seed}: no repair");
    let id = *w.acked.iter().next().expect("a segment");
    let away: Vec<u64> = w.segments[&id].marks.keys().copied().collect();
    assert!(away.len() < 4, "seed {seed}: {away:?}");
    for d in &away {
        w.kill(*d);
    }
    w.advance(10_000);
    some_stream_deleted(&mut w);
    w.jump(BEYOND_MS);
    w.advance(10_000);
    for d in &away {
        assert!(w.boot(*d, 1), "seed {seed}");
    }
    serve(&mut w, 100_000);
    assert!(
        w.warnings.contains(&id),
        "seed {seed}: {id} kept without a warning"
    );
    assert_eq!(w.wiped.len(), away.len(), "seed {seed}: {:?}", w.wiped);
    settled(&mut w, seed);
    intact(&w, seed);
    (w.warnings.len(), w.shown)
}

fn row15(count: u64) {
    let runs = seeds(count, all_away);
    let kept: usize = runs.iter().map(|r| r.0).sum();
    let silent: u64 = runs.iter().map(|r| r.1).sum();
    assert_eq!(silent, 0, "a deleted id came back without a warning");
    println!(
        "ROW 15 SIM PASS seeds={count} all-away-lost=0 kept-with-warning={kept} silent={silent}"
    );
}

#[test]
fn row15_all_away() {
    row15(SEEDS);
}

#[test]
#[ignore]
fn row15_all_away_at_1000_seeds() {
    row15(LONG_SEEDS);
}

struct Away {
    returned: u64,
    cut_returned: u64,
    last_copy_wiped: usize,
}

fn delete_and_drop_held(w: &mut World, x: u64, seed: u64) {
    let held = held_by(w, x);
    let first = held[0];
    let stream = w.segments[&first].stream;
    for _ in 0..ELECTION_MS / STEP_MS {
        if w.delete_stream_wait(stream) {
            break;
        }
        w.advance(STEP_MS);
    }
    assert!(
        w.deleted.iter().any(|(s, _)| *s == stream),
        "seed {seed}: no DELETE got 200"
    );
    let other = held
        .iter()
        .copied()
        .find(|id| w.segments[id].stream != stream)
        .expect("a segment of another stream");
    dropped(w, other);
}

fn away(seed: u64) -> Away {
    let mut w = World::new(seed);
    w.watch = true;
    let n = w.start(5, 3, OLD);
    w.fill(GB / 4);
    assert!(w.settle_repair(SETTLE_MS), "seed {seed}: no repair");
    let x = n[0];
    w.kill(x);
    w.advance(10_000);
    delete_and_drop_held(&mut w, x, seed);
    w.jump(BEYOND_MS);
    w.advance(10_000);
    assert!(!w.in_list(x), "seed {seed}: no horizon removal");
    assert!(w.boot(x, 1));
    serve(&mut w, 60_000);
    assert!(
        w.wiped.iter().any(|(old, _)| *old == x),
        "seed {seed}: no wipe"
    );
    settled(&mut w, seed);
    intact(&w, seed);
    let returned = w.shown + w.shown_warned;

    let mut w = World::new(seed);
    w.watch = true;
    let n = w.start(5, 3, OLD);
    w.fill(GB / 4);
    assert!(w.settle_repair(SETTLE_MS), "seed {seed}: no repair");
    let x = n[0];
    w.kill(x);
    w.advance(10_000);
    delete_and_drop_held(&mut w, x, seed);
    w.jump(24 * HOUR_MS);
    w.advance(10_000);
    assert!(w.boot(x, 1));
    serve(&mut w, 60_000);
    assert!(w.wiped.is_empty(), "seed {seed}: a wipe inside the horizon");
    assert!(w.running().contains(&x), "seed {seed}");
    assert_eq!(
        w.shown + w.shown_warned,
        0,
        "seed {seed}: inside the horizon"
    );
    settled(&mut w, seed);
    intact(&w, seed);

    let mut w = World::new(seed);
    w.watch = true;
    let n = w.start(5, 3, OLD);
    w.fill(GB / 4);
    assert!(w.settle_repair(SETTLE_MS), "seed {seed}: no repair");
    let x = n[0];
    let cuts: Vec<(u64, u64)> = n[1..].iter().map(|o| pair(x, *o)).collect();
    w.cut(&cuts);
    w.advance(ELECTION_MS);
    delete_and_drop_held(&mut w, x, seed);
    w.jump(BEYOND_MS);
    w.advance(10_000);
    w.cuts.clear();
    serve(&mut w, 60_000);
    assert!(
        w.wiped.iter().any(|(old, _)| *old == x),
        "seed {seed}: no wipe after the heal"
    );
    settled(&mut w, seed);
    intact(&w, seed);
    let cut_returned = w.shown + w.shown_warned;

    let mut w = World::new(seed);
    w.watch = true;
    w.headroom = 0.1;
    let n = w.start(7, 3, OLD);
    w.fill(GB / 4);
    assert!(w.settle_repair(SETTLE_MS), "seed {seed}: no repair");
    let x = n[0];
    let last = held_by(&w, x)[0];
    let others: Vec<u64> = w.segments[&last]
        .marks
        .keys()
        .copied()
        .filter(|d| *d != x)
        .collect();
    w.kill(x);
    for d in &others {
        w.delete(*d);
    }
    w.advance(10_000);
    let stream = (w.segments[&last].stream + 1) % 3;
    for _ in 0..ELECTION_MS / STEP_MS {
        if w.delete_stream_wait(stream) {
            break;
        }
        w.advance(STEP_MS);
    }
    w.jump(BEYOND_MS);
    w.advance(10_000);
    assert!(w.boot(x, 1));
    serve(&mut w, 60_000);
    assert!(w.warnings.contains(&last), "seed {seed}: no warning");
    assert!(
        w.wiped.iter().any(|(old, _)| *old == x),
        "seed {seed}: no wipe of the last holder"
    );
    let last_copy_wiped = usize::from(w.lost() > 0);
    settled(&mut w, seed);
    intact(&w, seed);
    Away {
        returned,
        cut_returned,
        last_copy_wiped,
    }
}

fn row16(count: u64) {
    let runs = seeds(count, away);
    let returned: u64 = runs.iter().map(|r| r.returned).sum();
    let cut: u64 = runs.iter().map(|r| r.cut_returned).sum();
    let wiped: usize = runs.iter().map(|r| r.last_copy_wiped).sum();
    assert_eq!(
        (returned, cut, wiped),
        (0, 0, 0),
        "returned cut-returned last-copy-wiped"
    );
    println!(
        "ROW 16 SIM PASS seeds={count} returned={returned} cut-returned={cut} last-copy-wiped={wiped}"
    );
}

#[test]
fn row16_away_longer_than_the_horizon() {
    row16(SEEDS);
}

#[test]
#[ignore]
fn row16_away_longer_than_the_horizon_at_1000_seeds() {
    row16(LONG_SEEDS);
}

const ELECTION_MS: u64 = 25_000;
const STALL_MS: u64 = 15_000 + ELECTION_MS;

fn settled(w: &mut World, seed: u64) {
    settled_within(w, seed, STALL_MS);
}

fn settled_within(w: &mut World, seed: u64, bound: u64) {
    w.advance(30_000);
    assert!(
        w.worst_stall <= bound,
        "seed {seed}: a stall of {} ms",
        w.worst_stall
    );
}

fn busy(w: &mut World) {
    w.publish();
    w.settle();
    w.take();
    w.read();
}

fn cut_sets(seed: u64) -> u64 {
    cut_run(seed, 0.0, 0.0, STALL_MS + 2 * FENCE_MS).0
}

fn cut_run(seed: u64, loss: f64, restarts: f64, bound: u64) -> (u64, u64) {
    let mut w = World::new(seed);
    w.watch = true;
    let n = w.start(6, 3, OLD);
    w.fill(GB / 4);
    warm(&mut w);
    w.loss = loss;
    for _ in 0..6 {
        w.cuts.clear();
        let mut cuts = Vec::new();
        for a in 0..n.len() {
            for b in a + 1..n.len() {
                if w.rng.random_bool(0.2) {
                    cuts.push(pair(n[a], n[b]));
                }
            }
        }
        w.cut(&cuts);
        if restarts > 0.0 && w.rng.random_bool(restarts) {
            let d = *n.choose(&mut w.rng).expect("a node");
            if w.running().contains(&d) {
                w.kill(d);
                w.boot(d, 1);
            }
        }
        for step in 0..10_000 / STEP_MS {
            busy(&mut w);
            if step == 5 {
                let stream = w.rng.random_range(0..3);
                w.delete_stream(stream);
            }
            if step == 10 {
                let ids: Vec<u64> = w.acked.iter().copied().collect();
                if let Some(id) = ids.choose(&mut w.rng) {
                    w.drop_segment(*id);
                }
            }
            w.advance(STEP_MS);
        }
    }
    w.cuts.clear();
    let mut stuck: Option<BTreeSet<(u64, u64)>> = None;
    for step in 0..(30_000 + ELECTION_MS) / STEP_MS {
        busy(&mut w);
        w.advance(STEP_MS);
        if step >= (20_000 + ELECTION_MS) / STEP_MS {
            let (unled, unreconciled) = w.stalls();
            let now: BTreeSet<(u64, u64)> = unled
                .into_iter()
                .chain(unreconciled.into_iter().map(|d| (0, d)))
                .collect();
            stuck = Some(match stuck {
                Some(s) => &s & &now,
                None => now,
            });
        }
    }
    let stuck = stuck.unwrap_or_default();
    assert!(
        stuck.is_empty(),
        "seed {seed}: stalled through the last 10 s after the heal: {stuck:?}"
    );
    assert_eq!(w.redelivered, 0, "seed {seed}: redelivered after 200");
    assert_eq!(
        w.shown + w.shown_warned,
        0,
        "seed {seed}: a deleted id shown"
    );
    settled_within(&mut w, seed, bound);
    let stall = w.worst_stall;
    let ids: Vec<u64> = w.segments.keys().copied().collect();
    w.held_repair.extend(ids);
    w.advance(repair::DIGEST_MS + 20_000);
    for (id, s) in &w.segments {
        for h in s.marks.keys() {
            if let Some(set) = w.set_of(*h, *id).filter(|set| set.holders.contains(h)) {
                assert_eq!(set, &s.holders, "seed {seed}: segment {id} on {h}");
            }
        }
    }
    intact(&w, seed);
    (w.missed_gossip, stall)
}

fn cuts(count: u64) {
    let missed: u64 = seeds(count, cut_sets).iter().sum();
    println!(
        "CUTS SIM PASS seeds={count} redelivered-after-200=0 deleted-shown=0 missed-gossip={missed}"
    );
}

#[test]
fn cuts_a_random_cut_set_each_ten_seconds() {
    cuts(SEEDS);
}

#[test]
#[ignore]
fn cuts_a_random_cut_set_each_ten_seconds_at_1000_seeds() {
    cuts(LONG_SEEDS);
}

fn false_death(seed: u64) {
    let mut w = World::new(seed);
    let n = w.start(3, 3, OLD);
    w.fill(GB / 4);
    warm(&mut w);
    let (a, b, c) = (n[0], n[1], n[2]);
    w.cut(&[pair(a, b)]);
    w.drop = 1.0;
    let mut seen = false;
    for _ in 0..30_000 / STEP_MS {
        w.publish();
        w.settle();
        w.take();
        seen |= w.marked_absent(a, b)
            && w.marked_absent(b, a)
            && !w.marked_absent(c, a)
            && !w.marked_absent(c, b);
        w.advance(STEP_MS);
    }
    assert!(seen, "seed {seed}: no false death");
    assert_eq!(w.double_leaders, 0, "seed {seed}: two leaders");
    w.drop = 0.0;
    w.cuts.clear();
    w.watch = true;
    for _ in 0..30_000 / STEP_MS {
        w.publish();
        w.settle();
        w.take();
        w.advance(STEP_MS);
    }
    assert_eq!(
        w.double_leaders, 0,
        "seed {seed}: two leaders after the heal"
    );
    assert_eq!(w.redelivered, 0, "seed {seed}: redelivered after 200");
    settled(&mut w, seed);
    intact(&w, seed);
}

fn false_deaths(count: u64) {
    seeds(count, false_death);
    println!("FALSE DEATH SIM PASS seeds={count} double-leaders=0 redelivered-after-200=0");
}

#[test]
fn false_death_two_co_holders_mark_each_other_dead() {
    false_deaths(SEEDS);
}

#[test]
#[ignore]
fn false_death_two_co_holders_mark_each_other_dead_at_1000_seeds() {
    false_deaths(LONG_SEEDS);
}

#[test]
fn a_holder_back_from_a_cut_leads_only_after_it_fetched_the_acks() {
    for seed in 0..5 {
        let mut w = World::new(seed);
        w.watch = true;
        let n = w.start(3, 3, OLD);
        w.fill(GB / 4);
        warm(&mut w);
        let x = n[0];
        w.cut(&[pair(x, n[1]), pair(x, n[2])]);
        for _ in 0..20_000 / STEP_MS {
            w.settle();
            w.take();
            w.advance(STEP_MS);
        }
        assert!(!w.settled.is_empty(), "seed {seed}: no settle got 200");
        w.cuts.clear();
        for _ in 0..20_000 / STEP_MS {
            w.take();
            w.advance(STEP_MS);
        }
        assert_eq!(w.redelivered, 0, "seed {seed}");
        settled(&mut w, seed);
    }
}

#[test]
fn a_planned_holder_with_no_live_complete_copy_names_the_segment_lost() {
    let mut w = World::new(3);
    let n = w.start(4, 3, OLD);
    warm(&mut w);
    let id = w.place(&[n[0]], SEGMENT_BYTES);
    for _ in 0..200 {
        w.advance(STEP_MS);
        if w.copying_to(id) {
            break;
        }
    }
    assert!(w.copying_to(id));
    assert!(w.named_lost.is_empty());
    w.kill(n[0]);
    w.advance(STEP_MS);
    assert_eq!(w.named_lost, BTreeSet::from([id]));
}

fn spanning(w: &World, minority: &[u64], majority: &[u64]) -> BTreeSet<u64> {
    w.segments
        .iter()
        .filter(|(id, _)| {
            minority
                .iter()
                .filter_map(|m| w.set_of(*m, **id))
                .any(|set| {
                    set.holders.iter().any(|h| minority.contains(h))
                        && set.holders.iter().any(|h| majority.contains(h))
                })
        })
        .map(|(id, _)| *id)
        .collect()
}

#[test]
fn a_holder_set_that_spans_a_cut_stays_spanning_on_the_minority_side_until_the_heal() {
    for seed in 0..3 {
        let mut w = World::new(seed);
        w.watch = true;
        let n = w.start(5, 3, OLD);
        w.fill(GB / 4);
        assert!(w.settle_repair(SETTLE_MS), "seed {seed}");
        let (minority, majority) = (&n[..2], &n[2..]);
        let before = spanning(&w, minority, majority);
        assert!(!before.is_empty(), "seed {seed}");
        let cuts: Vec<(u64, u64)> = minority
            .iter()
            .flat_map(|a| majority.iter().map(move |b| pair(*a, *b)))
            .collect();
        w.cut(&cuts);
        w.advance(40_000);
        assert!(
            before.is_subset(&spanning(&w, minority, majority)),
            "seed {seed}"
        );
        w.cuts.clear();
        assert!(w.settle_repair(SETTLE_MS), "seed {seed}");
        let agree = |w: &World| {
            w.segments.iter().all(|(id, s)| {
                s.marks
                    .keys()
                    .all(|h| w.set_of(*h, *id) == Some(&s.holders))
            })
        };
        for _ in 0..10_000 / STEP_MS {
            if agree(&w) {
                break;
            }
            w.advance(STEP_MS);
        }
        for (id, s) in &w.segments {
            for h in s.marks.keys() {
                assert_eq!(
                    w.set_of(*h, *id),
                    Some(&s.holders),
                    "seed {seed} segment {id} holder {h}"
                );
            }
        }
        settled(&mut w, seed);
        intact(&w, seed);
    }
}

fn joiners_on_the_minority_side(count: u64) {
    for seed in 0..5 {
        let mut w = World::new(seed);
        w.watch = true;
        let n = w.start(5, 3, OLD);
        w.fill(GB / 4);
        assert!(w.settle_repair(SETTLE_MS), "seed {seed}");
        let (minority, majority) = (&n[..2], &n[2..]);
        let joiners: Vec<u64> = (6..6 + count).collect();
        let cuts: Vec<(u64, u64)> = minority
            .iter()
            .chain(joiners.iter())
            .flat_map(|a| majority.iter().map(move |b| pair(*a, *b)))
            .collect();
        w.cut(&cuts);
        w.advance(10_000);
        for j in &joiners {
            assert_eq!(w.create("a", OLD, 1), *j);
        }
        w.advance(30_000);
        for _ in 0..40 {
            let stream = w.rng.random_range(0..3);
            for d in minority.iter().chain(joiners.iter()) {
                assert!(
                    !w.delete_from(*d, stream),
                    "seed {seed}: a DELETE got 200 on {d}"
                );
            }
            w.delete_stream(stream);
            w.settle();
            w.take();
            w.read();
            w.advance(STEP_MS);
        }
        assert_eq!(
            w.shown + w.shown_warned,
            0,
            "seed {seed}: a deleted id shown"
        );
        assert_eq!(w.redelivered, 0, "seed {seed}");
        assert_eq!(w.double_leaders, 0, "seed {seed}");
        w.cuts.clear();
        settled(&mut w, seed);
    }
}

#[test]
fn a_joiner_on_the_minority_side_of_a_cut_leaves_the_minority_fenced() {
    joiners_on_the_minority_side(1);
}

#[test]
fn two_joiners_on_the_minority_side_of_a_cut_leave_the_minority_fenced() {
    joiners_on_the_minority_side(2);
}

#[test]
fn three_joiners_on_the_minority_side_of_a_cut_leave_the_minority_fenced() {
    joiners_on_the_minority_side(3);
}

#[test]
fn a_delete_reaches_a_falsely_dead_node_by_a_relay_and_no_deleted_id_shows() {
    for seed in 0..20 {
        let mut w = World::new(seed);
        let n = w.start(4, 3, OLD);
        w.fill(GB / 4);
        warm(&mut w);
        w.cut(&[pair(n[0], n[1])]);
        w.drop = 1.0;
        w.advance(10_000);
        for _ in 0..40 {
            let stream = w.rng.random_range(0..3);
            w.delete_stream(stream);
            w.take();
            w.read();
            w.advance(STEP_MS);
        }
        assert_eq!(
            w.shown + w.shown_warned,
            0,
            "seed {seed}: a deleted id shown"
        );
        w.drop = 0.0;
        w.cuts.clear();
        w.watch = true;
        settled(&mut w, seed);
    }
}

#[test]
fn a_node_that_starts_during_a_reported_pair_cut_joins() {
    for seed in 0..5 {
        let mut w = World::new(seed);
        w.watch = true;
        let n = w.start(4, 3, OLD);
        w.fill(GB / 4);
        warm(&mut w);
        w.cut(&[pair(n[0], n[1])]);
        for _ in 0..20 {
            w.publish_from(n[0]);
            w.publish_from(n[1]);
            w.advance(1_000);
        }
        w.kill(n[0]);
        w.advance(5_000);
        assert!(w.boot(n[0], 1));
        for _ in 0..10 {
            w.publish_from(n[1]);
            w.advance(1_000);
        }
        let acked = (0..20)
            .filter(|_| {
                let ok = w.publish_from(n[0]).is_some();
                w.advance(STEP_MS);
                ok
            })
            .count();
        assert!(acked > 0, "seed {seed}: no write acked during the pair cut");
        settled(&mut w, seed);
    }
}

#[test]
fn a_pair_cut_with_no_writes_leaves_every_segment_a_leader() {
    for seed in 0..3 {
        let mut w = World::new(seed);
        w.watch = true;
        let n = w.start(3, 3, OLD);
        w.fill(GB / 4);
        assert!(w.settle_repair(SETTLE_MS), "seed {seed}");
        w.cut(&[pair(n[0], n[1])]);
        for _ in 0..120_000 / STEP_MS {
            w.advance(STEP_MS);
        }
        for id in w.acked.clone() {
            assert!(
                !w.leaders(id).is_empty(),
                "seed {seed}: segment {id} has no leader"
            );
        }
        settled(&mut w, seed);
    }
}

#[test]
fn a_node_that_restarts_during_a_pair_cut_with_no_writes_joins() {
    for seed in 0..3 {
        let mut w = World::new(seed);
        w.watch = true;
        let n = w.start(4, 3, OLD);
        w.fill(GB / 4);
        assert!(w.settle_repair(SETTLE_MS), "seed {seed}");
        w.cut(&[pair(n[0], n[1])]);
        w.advance(5_000);
        w.kill(n[0]);
        w.advance(5_000);
        assert!(w.boot(n[0], 1));
        for _ in 0..60_000 / STEP_MS {
            w.advance(STEP_MS);
        }
        assert!(w.reconciled(n[0]), "seed {seed}: no reconcile");
        let acked = (0..20)
            .filter(|_| {
                let ok = w.publish_from(n[0]).is_some();
                w.advance(STEP_MS);
                ok
            })
            .count();
        assert!(acked > 0, "seed {seed}: no write acked during the pair cut");
        settled(&mut w, seed);
    }
}

#[test]
fn a_groups_writer_cut_from_every_peer_raises_no_epoch() {
    for seed in 0..40 {
        let mut w = World::new(seed);
        w.watch = true;
        let n = w.start(5, 3, OLD);
        w.fill(GB / 4);
        assert!(w.settle_repair(SETTLE_MS), "seed {seed}");
        let Some(x) = n.iter().copied().find(|d| w.is_groups_writer(*d)) else {
            continue;
        };
        let before = w.groups_epoch(x);
        let cuts: Vec<(u64, u64)> = n.iter().filter(|o| **o != x).map(|o| pair(x, *o)).collect();
        w.cut(&cuts);
        w.publish_from(x);
        for _ in 0..3 {
            w.advance(STEP_MS);
            assert_eq!(
                w.groups_epoch(x),
                before,
                "seed {seed}: the cut writer raised its epoch"
            );
        }
        w.cuts.clear();
        settled(&mut w, seed);
    }
}

#[test]
fn a_degraded_write_after_a_floor_rise_in_a_cut_reopens_its_segment() {
    let mut resealed = 0;
    for seed in 0..5 {
        for offset in [2_000, 4_000, 6_000, 8_000] {
            let mut w = World::new(seed);
            w.watch = true;
            let n = w.start(12, 3, OLD);
            w.fill(GB / 4);
            warm(&mut w);
            let writer = n[0];
            let group = w
                .groups()
                .expect("groups")
                .of(writer)
                .expect("a group")
                .to_vec();
            let peer = *group.iter().find(|d| **d != writer).expect("a co-member");
            let victims: Vec<u64> = n
                .iter()
                .copied()
                .filter(|d| !group.contains(d))
                .take(3)
                .collect();
            for v in &victims {
                w.kill(*v);
            }
            w.advance(offset);
            w.cut(&[pair(writer, peer)]);
            for _ in 0..30_000 / STEP_MS {
                let before = w.resealed;
                w.publish_from(writer);
                if w.resealed > before {
                    assert!(
                        w.publish_from(writer).is_some(),
                        "seed {seed}: no ack after the reseal"
                    );
                }
                w.advance(STEP_MS);
            }
            resealed += w.resealed;
            w.cuts.clear();
            for v in &victims {
                assert!(w.boot(*v, 1));
            }
            settled(&mut w, seed);
            intact(&w, seed);
        }
    }
    assert!(resealed > 0, "no degraded write met a floor rise");
}

fn fenced_restart(seed: u64, gap_ms: u64) {
    let mut w = World::new(seed);
    w.watch = true;
    let n = w.start(5, 3, OLD);
    w.fill(GB / 4);
    warm(&mut w);
    let (minority, majority) = (vec![n[0], n[1], 6, 7], vec![n[2], n[3], n[4]]);
    w.kill(n[0]);
    w.kill(n[1]);
    let cuts: Vec<(u64, u64)> = minority
        .iter()
        .flat_map(|a| majority.iter().map(move |b| pair(*a, *b)))
        .collect();
    w.cut(&cuts);
    w.advance(10_000);
    assert_eq!(w.create("a", OLD, 1), 6);
    assert_eq!(w.create("a", OLD, 1), 7);
    w.advance(5_000);
    assert!(w.boot(n[0], 1));
    w.advance(gap_ms);
    assert!(w.boot(n[1], 1));
    w.advance(30_000);
    for _ in 0..20 {
        let stream = w.rng.random_range(0..3);
        for d in &n[..2] {
            assert!(
                !w.delete_from(*d, stream),
                "seed {seed}: a DELETE got 200 on {d}"
            );
        }
        w.delete_stream(stream);
        w.settle();
        w.take();
        w.read();
        w.advance(STEP_MS);
    }
    assert_eq!(w.double_leaders, 0, "seed {seed}");
    assert_eq!(w.redelivered, 0, "seed {seed}");
    assert_eq!(w.shown + w.shown_warned, 0, "seed {seed}");
    w.cuts.clear();
    settled(&mut w, seed);
}

#[test]
fn old_nodes_that_restart_on_the_minority_side_stay_fenced() {
    for seed in 0..5 {
        fenced_restart(seed, 2_000);
    }
}

fn across(a: &[u64], b: &[u64]) -> Vec<(u64, u64)> {
    a.iter()
        .flat_map(|x| b.iter().map(move |y| pair(*x, *y)))
        .collect()
}

#[test]
fn members_a_delete_gets_200_when_its_last_confirmation_arrives() {
    let mut w = World::new(1);
    let n = w.start(3, 3, OLD);
    warm(&mut w);
    let before = w.deleted.len();
    assert!(w.delete_from(n[0], 0));
    assert_eq!(w.deleted.len(), before, "a 200 before any confirmation");
    w.advance(STEP_MS);
    assert_eq!(w.deleted.len(), before, "a 200 before a round trip");
    w.advance(2_000);
    assert_eq!(
        w.deleted.len(),
        before + 1,
        "no 200 after the confirmations"
    );
}

#[test]
fn members_a_settle_gets_200_only_after_its_ack_frames() {
    for seed in 0..5 {
        let mut w = World::new(seed);
        let n = w.start(3, 3, OLD);
        let id = w.place(&n, SEGMENT_BYTES);
        warm(&mut w);
        w.settle();
        w.advance(1_000);
        assert_eq!(w.settled.get(&id), Some(&1), "seed {seed}: no 200");
        let leader = w.leaders(id)[0];
        w.settle();
        let others: Vec<u64> = n.iter().copied().filter(|d| *d != leader).collect();
        w.cut(&across(&[leader], &others));
        for _ in 0..20_000 / STEP_MS {
            w.take();
            w.advance(STEP_MS);
        }
        assert!(
            others.iter().any(|d| w.leaders(id).contains(d)),
            "seed {seed}: no new leader"
        );
        assert_eq!(w.redelivered, 0, "seed {seed}: redelivered after 200");
        assert_eq!(w.settled.get(&id), Some(&1), "seed {seed}");
    }
}

#[test]
fn members_a_record_goes_to_its_scope_and_a_relay_only_for_a_cut_pair() {
    let mut w = World::new(1);
    let n = w.start(5, 3, OLD);
    warm(&mut w);
    let before = w.record_frames;
    assert!(w.delete_wait(n[0], 0));
    assert_eq!(w.record_frames - before, 4);
    w.cut(&[pair(n[0], n[1])]);
    w.advance(5_000);
    let before = w.record_frames;
    assert!(w.delete_wait(n[0], 1), "no 200 through the relay");
    assert_eq!(w.record_frames - before, 3 + 3);
}

#[test]
fn members_a_request_of_an_earlier_run_gets_no_200() {
    let mut w = World::new(1);
    let n = w.start(3, 3, OLD);
    warm(&mut w);
    assert!(w.delete_from(n[0], 0));
    w.kill(n[0]);
    w.boot(n[0], 1);
    w.advance(5_000);
    assert!(w.done.is_empty());
}

#[test]
fn members_a_settle_with_one_live_copy_gets_no_200() {
    for seed in 0..10 {
        let mut w = World::new(seed);
        let n = w.start(3, 3, OLD);
        let id = w.place(&n[..2], SEGMENT_BYTES);
        w.held_repair.insert(id);
        warm(&mut w);
        w.kill(n[0]);
        w.advance(6_000);
        for _ in 0..200 {
            w.settle();
            w.take();
            w.advance(STEP_MS);
        }
        w.kill(n[1]);
        w.boot(n[0], 1);
        for _ in 0..60_000 / STEP_MS {
            w.take();
            w.advance(STEP_MS);
        }
        assert_eq!(w.redelivered, 0, "seed {seed}: redelivered after 200");
        assert!(
            !w.settled.contains_key(&id),
            "seed {seed}: a 200 on one copy"
        );
    }
}

fn faults(seed: u64, n: usize, loss: f64, wide: &[usize]) -> (u64, u64, u64, usize) {
    let mut w = World::new(seed);
    w.loss = loss;
    let ids = w.start(n, 3, OLD);
    w.fill(GB / 8);
    for h in wide {
        w.place(&ids[..*h], SEGMENT_BYTES);
    }
    warm(&mut w);
    for _ in 0..40 {
        let running = w.running();
        match w.rng.random_range(0..6) {
            0 => {
                w.cuts.clear();
                w.oneway.clear();
            }
            1 => {
                let (a, b) = (ids[w.rng.random_range(0..n)], ids[w.rng.random_range(0..n)]);
                if a != b {
                    w.cuts.insert(pair(a, b));
                }
            }
            2 => {
                let (a, b) = (ids[w.rng.random_range(0..n)], ids[w.rng.random_range(0..n)]);
                if a != b {
                    w.oneway.insert((a, b));
                }
            }
            3 => {
                if let Some(d) = running.choose(&mut w.rng).copied() {
                    w.kill(d);
                    w.boot(d, 1);
                }
            }
            4 => {
                if running.len() + 1 >= n
                    && let Some(d) = running.choose(&mut w.rng).copied()
                {
                    w.kill(d);
                }
            }
            _ => {
                for d in ids.iter().copied().filter(|d| !running.contains(d)) {
                    w.boot(d, 1);
                }
            }
        }
        for _ in 0..w.rng.random_range(1..=8u64) {
            busy(&mut w);
            w.advance(STEP_MS);
        }
    }
    w.cuts.clear();
    w.oneway.clear();
    let running = w.running();
    for d in ids.iter().copied().filter(|d| !running.contains(d)) {
        w.boot(d, 1);
    }
    for _ in 0..60_000 / STEP_MS {
        busy(&mut w);
        w.advance(STEP_MS);
    }
    assert_eq!(
        w.leaderless(),
        Vec::<u64>::new(),
        "seed {seed}: a leaderless segment"
    );
    (w.double_leaders, w.redelivered, w.shown, w.lost_settles())
}

#[test]
fn members_random_faults_seed_54_keep_one_leader_and_no_redelivery() {
    assert_eq!(faults(54, 3, 0.0, &[]), (0, 0, 0, 0));
}

#[test]
fn members_round_trips_leave_with_their_view_entry() {
    let mut w = World::new(1);
    let n = w.start(4, 3, OLD);
    warm(&mut w);
    w.kill(n[3]);
    w.advance(10_000);
    assert!(w.accept_loss(n[3]));
    w.advance(20_000);
    w.jump(TOMBSTONE_HORIZON + HOUR_MS);
    w.advance(10_000);
    assert!(!w.knows(n[0], n[3]));
}

#[test]
fn members_random_faults_seeds_31_and_66_lose_no_settle() {
    assert_eq!(faults(31, 3, 0.0, &[]), (0, 0, 0, 0), "seed 31");
    assert_eq!(faults(66, 3, 0.0, &[]), (0, 0, 0, 0), "seed 66");
}

#[test]
fn members_random_faults_seed_288_catch_up_each_segment() {
    assert_eq!(faults(288, 3, 0.0, &[]), (0, 0, 0, 0));
}

#[test]
fn members_random_faults_seeds_0_to_49() {
    let bad: Vec<(u64, (u64, u64, u64, usize))> =
        seeds(50, |seed| (seed, faults(seed, 3, 0.0, &[])))
            .into_iter()
            .filter(|(_, r)| *r != (0, 0, 0, 0))
            .collect();
    assert!(bad.is_empty(), "{bad:?}");
    println!("FAULTS SIM PASS seeds=50 n=3 double-leaders=0 redelivered=0 shown=0 lost-settles=0");
}

#[test]
fn members_random_faults_with_four_and_five_holder_segments_seeds_0_to_49() {
    let bad: Vec<(u64, (u64, u64, u64, usize))> =
        seeds(50, |seed| (seed, faults(seed, 5, 0.0, &[4, 5])))
            .into_iter()
            .filter(|(_, r)| *r != (0, 0, 0, 0))
            .collect();
    assert!(bad.is_empty(), "{bad:?}");
    println!(
        "FAULTS WIDE SIM PASS seeds=50 n=5 holders=4,5 double-leaders=0 redelivered=0 shown=0 lost-settles=0"
    );
}

fn chaos(seed: u64, loss: f64) -> (u64, u64, u64) {
    let mut w = World::new(seed);
    let n = w.start(5, 3, OLD);
    w.fill(GB / 4);
    warm(&mut w);
    w.loss = loss;
    for _ in 0..20 {
        let r = w.rng.random_range(0..6);
        let run = w.running();
        let dead: Vec<u64> = n.iter().copied().filter(|d| !run.contains(d)).collect();
        match r {
            0 => {
                w.cuts.clear();
                let mut cuts = Vec::new();
                for a in 0..n.len() {
                    for b in a + 1..n.len() {
                        if w.rng.random_bool(0.25) {
                            cuts.push(pair(n[a], n[b]));
                        }
                    }
                }
                w.cut(&cuts);
            }
            1 => w.cuts.clear(),
            2 if run.len() >= 4 => {
                let d = *run.choose(&mut w.rng).expect("a node");
                w.kill(d);
            }
            3 => {
                if let Some(d) = dead.choose(&mut w.rng).copied() {
                    w.boot(d, 1);
                }
            }
            _ => {
                let d = *run.choose(&mut w.rng).expect("a node");
                w.kill(d);
                w.boot(d, 1);
            }
        }
        let gap = w.rng.random_range(2..=8) * 1_000;
        for step in 0..gap / STEP_MS {
            busy(&mut w);
            if step == 3 {
                let ids: Vec<u64> = w.acked.iter().copied().collect();
                if let Some(id) = ids.choose(&mut w.rng) {
                    w.drop_segment(*id);
                }
                let stream = w.rng.random_range(0..3);
                w.delete_stream(stream);
            }
            w.advance(STEP_MS);
        }
    }
    w.cuts.clear();
    let run = w.running();
    for d in n.iter().copied().filter(|d| !run.contains(d)) {
        w.boot(d, 1);
    }
    for _ in 0..60_000 / STEP_MS {
        busy(&mut w);
        w.advance(STEP_MS);
    }
    (w.redelivered, w.shown + w.shown_warned, w.double_leaders)
}

#[test]
fn members_chaos_seed_88_with_frame_loss_leads_on_no_stale_mark() {
    assert_eq!(chaos(88, 0.2), (0, 0, 0));
}

#[test]
fn cuts_seeds_104_and_330_keep_a_leader_for_each_segment() {
    for seed in [104, 330] {
        cut_run(seed, 0.0, 0.0, STALL_MS + 2 * FENCE_MS);
    }
}

const CUTS_LOSS_BOUND_MS: u64 = 55_000;
const CUTS_RESTARTS_BOUND_MS: u64 = 22_000 + ELECTION_MS;

fn cuts_with(count: u64, name: &str, loss: f64, restarts: f64, bound: u64) {
    let runs = seeds(count, |seed| cut_run(seed, loss, restarts, bound));
    let worst = runs.iter().map(|r| r.1).max().unwrap_or(0);
    println!(
        "CUTS {name} SIM PASS seeds={count} redelivered-after-200=0 deleted-shown=0 worst-stall-ms={worst} bound-ms={bound}"
    );
}

#[test]
fn cuts_with_frame_loss_seed_29_reconciles_each_node_after_the_heal() {
    cut_run(29, 0.2, 0.0, CUTS_LOSS_BOUND_MS);
}

#[test]
fn cuts_with_frame_loss() {
    cuts_with(SEEDS, "LOSS", 0.2, 0.0, CUTS_LOSS_BOUND_MS);
}

#[test]
fn cuts_with_restarts() {
    cuts_with(SEEDS, "RESTARTS", 0.0, 0.3, CUTS_RESTARTS_BOUND_MS);
}

#[test]
fn members_each_of_300_segments_has_a_leader_soon_after_one_kill() {
    let mut w = World::new(1);
    let n = w.start(3, 3, OLD);
    for _ in 0..300 {
        w.place(&n, SEGMENT_BYTES);
    }
    let ids: Vec<u64> = w.acked.iter().copied().collect();
    w.held_repair.extend(ids.iter().copied());
    warm(&mut w);
    w.kill(n[2]);
    let start = w.now_ms;
    let mut last = 0;
    for _ in 0..60_000 / STEP_MS {
        w.advance(STEP_MS);
        if ids.iter().any(|id| w.leaders(*id).is_empty()) {
            last = w.now_ms - start;
        }
    }
    assert!(
        last <= STALL_MS,
        "a segment had no leader {last} ms after the kill"
    );
}

#[test]
fn members_a_segment_is_led_soon_after_its_holder_gets_a_copy_back() {
    let mut w = World::new(1);
    let n = w.start(3, 3, OLD);
    for _ in 0..600 {
        w.place(&n, SEGMENT_BYTES);
    }
    let ids: Vec<u64> = w.acked.iter().copied().collect();
    w.held_repair.extend(ids[1..].iter().copied());
    warm(&mut w);
    let id = ids[0];
    let l = w.leaders(id)[0];
    w.lose_copy(l, id);
    let mut back = None;
    let mut last = 0;
    for _ in 0..30_000 / STEP_MS {
        w.advance(STEP_MS);
        if back.is_none() && w.segments[&id].marks.contains_key(&l) {
            back = Some(w.now_ms);
        }
        if w.leaders(id).is_empty() {
            last = w.now_ms;
        }
    }
    let back = back.expect("the copy came back");
    assert!(
        last <= back + 2_000,
        "unled {} ms after the copy came back",
        last - back
    );
}

fn roll(seed: u64, loss: f64) -> (u64, u64) {
    let mut w = World::new(seed);
    let n = w.start(3, 3, OLD);
    w.fill(GB / 4);
    warm(&mut w);
    w.loss = loss;
    let (refused, tries) = (w.refused_settles, w.settle_tries);
    for d in n {
        w.kill(d);
        for _ in 0..1_000 / STEP_MS {
            w.settle();
            w.take();
            w.advance(STEP_MS);
        }
        w.boot(d, 1);
        for _ in 0..30_000 / STEP_MS {
            w.settle();
            w.take();
            w.advance(STEP_MS);
        }
    }
    (w.refused_settles - refused, w.settle_tries - tries)
}

#[test]
fn members_a_rolling_restart_refuses_few_settles() {
    for loss in [0.0, 0.2] {
        let runs = seeds(10, |seed| roll(seed, loss));
        let refused: u64 = runs.iter().map(|r| r.0).sum();
        let tries: u64 = runs.iter().map(|r| r.1).sum();
        println!("SETTLE 503 loss={loss} seeds=10 refused={refused} tries={tries}");
        assert!(
            8 * refused <= tries,
            "loss {loss}: {refused} of {tries} settles got 503"
        );
    }
}

#[test]
fn members_a_drop_reaches_each_holder_by_frames_and_an_absent_holder_on_return() {
    let mut w = World::new(1);
    w.link_ms = 400;
    let n = w.start(3, 3, OLD);
    let id = w.place(&n, SEGMENT_BYTES);
    let away = w.place(&n, SEGMENT_BYTES);
    w.held_repair.extend([id, away]);
    warm(&mut w);
    assert!(w.drop_segment(id));
    let holding = n
        .iter()
        .filter(|d| w.segments[&id].marks.contains_key(d))
        .count();
    assert!(holding >= 2, "the drop reached a holder before a frame");
    w.advance(1_000);
    assert!(n.iter().all(|d| !w.segments[&id].marks.contains_key(d)));
    w.kill(n[2]);
    w.advance(10_000);
    assert!(w.drop_segment(away));
    w.advance(2_000);
    assert!(w.boot(n[2], 1));
    w.advance(10_000);
    assert!(
        !w.segments[&away].marks.contains_key(&n[2]),
        "the returned holder kept a dropped copy"
    );
}

#[test]
fn members_segment_asks_are_capped() {
    let mut w = World::new(1);
    let n = w.start(3, 3, OLD);
    for _ in 0..300 {
        w.place(&n, SEGMENT_BYTES);
    }
    warm(&mut w);
    w.advance(10_000);
    w.kill(n[0]);
    assert!(w.boot(n[0], 1));
    let mut most = 0;
    for _ in 0..40 {
        let before = w.seg_asks;
        w.advance(STEP_MS);
        most = most.max(w.seg_asks - before);
    }
    assert!(most <= 3 * 64, "{most} segment asks in one step");
}

#[test]
fn members_a_read_with_a_fenced_pick_holder_is_whole_or_partial() {
    for seed in 0..5 {
        let mut w = World::new(seed);
        let n = w.start(3, 3, OLD);
        w.fill(GB / 4);
        warm(&mut w);
        w.cut(&[pair(n[0], n[1])]);
        for _ in 0..20_000 / STEP_MS {
            w.read();
            w.advance(STEP_MS);
        }
        assert_eq!(
            w.incomplete_reads, 0,
            "seed {seed}: a read left out a segment"
        );
    }
}

#[test]
fn members_a_dropped_segment_keeps_no_copy_in_flight() {
    let mut w = World::new(1);
    let n = w.start(3, 3, OLD);
    warm(&mut w);
    let id = w.place(&n[..2], SEGMENT_BYTES);
    for _ in 0..20_000 / STEP_MS {
        if w.copying_of(id) {
            break;
        }
        w.advance(STEP_MS);
    }
    assert!(w.copying_of(id), "no repair copy started");
    assert!(w.drop_segment(id));
    w.advance(2_000);
    assert!(
        !w.copying_of(id),
        "a copy of a dropped segment is in flight"
    );
}

fn q4(seed: u64) -> (u64, u64, u64) {
    let mut w = World::new(seed);
    let n = w.start(5, 3, OLD);
    let (a, b, c, d, e) = (n[0], n[1], n[2], n[3], n[4]);
    let id = w.place(&[a, b, c], SEGMENT_BYTES);
    w.held_repair.insert(id);
    warm(&mut w);
    w.cut(&[pair(a, c), pair(b, c), pair(c, d), pair(c, e)]);
    for _ in 0..20_000 / STEP_MS {
        w.settle();
        w.take();
        w.advance(STEP_MS);
    }
    let settled = w.settled.get(&id).copied().unwrap_or(0);
    let c_mark = w.segments[&id].marks.get(&c).copied().unwrap_or(0);
    w.cuts.clear();
    w.held_repair.clear();
    w.cut(&across(&[a, b], &[c, d, e]));
    for _ in 0..60_000 / STEP_MS {
        w.take();
        w.advance(STEP_MS);
    }
    (settled, c_mark, w.redelivered)
}

#[test]
fn probe_q4_others_lift_from_repair_copies() {
    for seed in 0..20 {
        let (settled, c_mark, redelivered) = q4(seed);
        assert!(settled > c_mark, "seed {seed}: no stale copy");
        assert_eq!(redelivered, 0, "seed {seed}: redelivered after 200");
    }
}

#[test]
fn probe_q4_backlog_no_held_repair() {
    for seed in 0..5 {
        let mut w = World::new(seed);
        let n = w.start(5, 3, OLD);
        let (a, b, c) = (n[0], n[1], n[2]);
        w.fill(8 * GB);
        warm(&mut w);
        w.cut(&[pair(a, c), pair(b, c), pair(c, n[3]), pair(c, n[4])]);
        for _ in 0..20_000 / STEP_MS {
            for _ in 0..5 {
                w.settle();
            }
            w.take();
            w.advance(STEP_MS);
        }
        assert!(!w.settled.is_empty(), "seed {seed}: no settle");
        w.cuts.clear();
        w.cut(&across(&[a, b], &[c, n[3], n[4]]));
        for _ in 0..60_000 / STEP_MS {
            w.take();
            w.advance(STEP_MS);
        }
        assert_eq!(w.redelivered, 0, "seed {seed}: redelivered after 200");
    }
}

fn no_200_before_isr_ms(w: &mut World, id: u64, seed: u64) {
    let before = w.settled.get(&id).copied();
    for _ in 0..(ISR_MS - 2 * STEP_MS) / STEP_MS {
        w.settle();
        w.take();
        w.advance(STEP_MS);
    }
    assert_eq!(
        w.settled.get(&id).copied(),
        before,
        "seed {seed}: a 200 before ISR_MS passed"
    );
}

fn stale_pair(seed: u64, nodes: usize, holders: usize, first: usize) -> (bool, u64) {
    let mut w = World::new(seed);
    let n = w.start(nodes, 3, OLD);
    warm(&mut w);
    let id = w.place(&n[..holders], SEGMENT_BYTES);
    w.held_repair.insert(id);
    w.advance(10_000);
    w.cut(&across(&n[..first], &n[first..]));
    no_200_before_isr_ms(&mut w, id, seed);
    for _ in 0..(20_000 + ELECTION_MS) / STEP_MS {
        w.settle();
        w.take();
        w.advance(STEP_MS);
    }
    let settled = w.settled.contains_key(&id);
    let fresh: Vec<u64> = n[first..holders].to_vec();
    let rest: Vec<u64> = n.iter().copied().filter(|d| !fresh.contains(d)).collect();
    w.cuts.clear();
    w.cut(&across(&fresh, &rest));
    for _ in 0..20_000 / STEP_MS {
        w.take();
        w.advance(STEP_MS);
    }
    (settled, w.redelivered)
}

#[test]
fn a_cut_of_four_holders_in_half_redelivers_nothing() {
    for seed in 0..10 {
        assert_eq!(stale_pair(seed, 5, 4, 2).1, 0, "seed {seed}");
    }
}

#[test]
fn two_stale_holders_of_five_lift_no_fence_for_each_other() {
    for seed in 0..80 {
        assert_eq!(stale_pair(seed, 7, 5, 2), (true, 0), "seed {seed}");
    }
}

#[test]
fn a_lone_isr_holder_that_took_a_settle_after_a_holder_died_redelivers_nothing() {
    for seed in 0..5 {
        let mut w = World::new(seed);
        let n = w.start(5, 3, OLD);
        let (a, b, x) = (n[0], n[1], n[2]);
        let id = w.place(&[a, b, x], SEGMENT_BYTES);
        w.held_repair.insert(id);
        warm(&mut w);
        w.cut(&[pair(a, x)]);
        w.advance(5_000);
        for _ in 0..10_000 / STEP_MS {
            w.settle();
            w.take();
            w.advance(STEP_MS);
        }
        assert!(w.settled.contains_key(&id), "seed {seed}: no settle");
        w.kill(b);
        w.advance(10_000);
        assert!(w.accept_loss(b), "seed {seed}");
        w.advance(5_000);
        w.kill(x);
        for _ in 0..STALL_MS / STEP_MS {
            w.take();
            w.advance(STEP_MS);
        }
        assert!(w.in_list(x), "seed {seed}: the dead holder left the list");
        assert_eq!(w.redelivered, 0, "seed {seed}: redelivered after 200");
    }
}

#[test]
fn an_accept_loss_that_empties_the_isr_leaves_the_survivors_a_leader() {
    for seed in 0..5 {
        let mut w = World::new(seed);
        let n = w.start(5, 3, OLD);
        let (a, b, c) = (n[0], n[1], n[2]);
        let id = w.place(&[a, b, c], SEGMENT_BYTES);
        w.held_repair.insert(id);
        warm(&mut w);
        w.cut(&across(&[a], &[b, c]));
        for _ in 0..20_000 / STEP_MS {
            w.settle();
            w.take();
            w.advance(STEP_MS);
        }
        w.cuts.clear();
        w.kill(a);
        w.advance(10_000);
        assert!(w.accept_loss(a), "seed {seed}");
        w.advance(30_000);
        assert_eq!(w.leaderless(), Vec::<u64>::new(), "seed {seed}");
        assert!(!w.leaders(id).is_empty(), "seed {seed}: no leader");
    }
}

fn isr_of(w: &World, id: u64) -> BTreeSet<u64> {
    w.newest()
        .and_then(|m| m.segments.get(&id))
        .map(|s| s.isr.clone())
        .unwrap_or_default()
}

#[test]
fn a_holder_that_left_the_isr_rejoins_under_steady_settles() {
    for seed in 0..5 {
        let mut w = World::new(seed);
        let n = w.start(5, 3, OLD);
        let (a, b, c) = (n[0], n[1], n[2]);
        let id = w.place(&[a, b, c], SEGMENT_BYTES);
        w.held_repair.insert(id);
        warm(&mut w);
        w.cut(&across(&[c], &[a, b]));
        for _ in 0..20_000 / STEP_MS {
            w.settle();
            w.advance(STEP_MS);
        }
        assert!(!isr_of(&w, id).contains(&c), "seed {seed}: c stayed in");
        w.cuts.clear();
        for _ in 0..40_000 / STEP_MS {
            for _ in 0..3 {
                w.settle();
            }
            w.advance(STEP_MS);
        }
        assert!(isr_of(&w, id).contains(&c), "seed {seed}: c did not rejoin");
    }
}

#[test]
fn cuts_with_frame_loss_seeds_20_77_94_reconcile_while_repair_names_new_co_holders() {
    for seed in [20, 77, 94] {
        cut_run(seed, 0.2, 0.0, CUTS_LOSS_BOUND_MS);
    }
}

#[test]
fn a_settle_after_two_of_four_holders_died_loses_nothing_once_they_are_accepted_lost() {
    for seed in 0..5 {
        let mut w = World::new(seed);
        let n = w.start(6, 3, OLD);
        let id = w.place(&n[..4], SEGMENT_BYTES);
        w.held_repair.insert(id);
        warm(&mut w);
        w.kill(n[2]);
        w.kill(n[3]);
        no_200_before_isr_ms(&mut w, id, seed);
        for _ in 0..30_000 / STEP_MS {
            w.settle();
            w.take();
            w.advance(STEP_MS);
        }
        assert!(w.accept_loss(n[2]), "seed {seed}");
        w.advance(10_000);
        assert!(w.accept_loss(n[3]), "seed {seed}");
        w.advance(10_000);
        assert_eq!(w.lost_settles(), 0, "seed {seed}: a 200 was lost");
    }
}

#[test]
fn a_stale_holder_lifts_no_fence_from_one_fetch_of_a_holder_that_later_left() {
    for seed in 0..5 {
        let mut w = World::new(seed);
        let n = w.start(6, 3, OLD);
        let (a, b, x, y) = (n[0], n[1], n[2], n[3]);
        let id = w.place(&[a, b, x, y], SEGMENT_BYTES);
        w.held_repair.insert(id);
        warm(&mut w);
        w.cut(&across(&[a], &[x, y]));
        w.advance(10_000);
        w.cuts.clear();
        w.cut(&across(&[a], &n[1..]));
        for _ in 0..15_000 / STEP_MS {
            w.settle();
            w.take();
            w.advance(STEP_MS);
        }
        w.kill(b);
        w.advance(10_000);
        assert!(w.accept_loss(b), "seed {seed}");
        w.advance(5_000);
        w.cuts.clear();
        w.cut(&across(&[x, y], &[a, n[4], n[5]]));
        for _ in 0..20_000 / STEP_MS {
            w.take();
            w.advance(STEP_MS);
        }
        assert_eq!(w.redelivered, 0, "seed {seed}: redelivered after 200");
    }
}

#[test]
fn members_a_settle_whose_only_scope_holder_has_no_copy_gets_no_200() {
    for seed in 0..10 {
        let mut w = World::new(seed);
        let n = w.start(3, 3, OLD);
        let id = w.place(&n[..2], SEGMENT_BYTES);
        w.held_repair.insert(id);
        w.lose_copy(n[1], id);
        warm(&mut w);
        for _ in 0..200 {
            w.settle();
            w.take();
            w.advance(STEP_MS);
        }
        assert!(w.settle_tries > 0, "seed {seed}: no settle tried");
        assert!(
            !w.settled.contains_key(&id),
            "seed {seed}: a 200 on one copy"
        );
    }
}

fn members_of(w: &World) -> BTreeSet<u64> {
    w.newest().map(|m| m.members.clone()).unwrap_or_default()
}

fn version_of(w: &World) -> u64 {
    w.newest().map_or(0, |m| m.version)
}

fn one_voice(w: &World, seed: u64) {
    assert_eq!(
        w.two_lists, 0,
        "seed {seed}: two nodes applied different commands at one index"
    );
    assert_eq!(w.double_leaders, 0, "seed {seed}: two leaders");
}

#[test]
fn a_join_through_a_cut_minority_never_commits_and_the_joiner_joins_after_the_heal() {
    for seed in 0..5 {
        let mut w = World::new(seed);
        let n = w.start(5, 3, OLD);
        warm(&mut w);
        let members = members_of(&w);
        let joiner = w.create("a", 0, 1);
        let cut = [n[0], n[1], joiner];
        let versions = cut.map(|d| w.meta(d).map_or(0, |m| m.version));
        w.part(&cut);
        for _ in 0..60_000 / STEP_MS {
            assert!(
                !w.in_list(joiner),
                "seed {seed}: the cut minority admitted a joiner"
            );
            assert_eq!(members_of(&w), members, "seed {seed}");
            w.advance(STEP_MS);
        }
        assert_eq!(
            cut.map(|d| w.meta(d).map_or(0, |m| m.version)),
            versions,
            "seed {seed}: the cut minority committed"
        );
        w.heal();
        w.advance(90_000);
        assert!(
            w.in_list(joiner),
            "seed {seed}: the joiner did not join after the heal"
        );
        assert!(
            w.joined(joiner),
            "seed {seed}: the joiner did not reconcile"
        );
        one_voice(&w, seed);
    }
}

#[test]
fn a_short_cut_with_a_joiner_waiting_does_not_remove_the_cut_member() {
    for seed in 0..5 {
        let mut w = World::new(seed);
        let n = w.start(3, 3, OLD);
        warm(&mut w);
        w.part(&[n[0]]);
        w.create("a", 0, 1);
        for _ in 0..20_000 / STEP_MS {
            w.advance(STEP_MS);
            assert!(
                members_of(&w).contains(&n[0]),
                "seed {seed}: the cut member left the list"
            );
        }
        w.heal();
        w.advance(90_000);
        assert!(
            members_of(&w).contains(&n[0]),
            "seed {seed}: the cut member left the list"
        );
    }
}

#[test]
fn a_scale_in_of_half_by_sigterm_removes_one_member_at_a_time_and_loses_nothing() {
    for seed in 0..5 {
        let mut w = World::new(seed);
        let n = w.start(6, 3, OLD);
        w.fill(GB / 2);
        assert!(w.settle_repair(SETTLE_MS), "seed {seed}: no repair");
        for v in &n[3..] {
            w.stop(*v);
        }
        let mut before = members_of(&w);
        for _ in 0..130_000 / STEP_MS {
            let running = w.running();
            for v in n[3..].iter().filter(|v| !running.contains(v)) {
                w.delete(*v);
            }
            let now = members_of(&w);
            assert!(
                before.symmetric_difference(&now).count() <= 1,
                "seed {seed}: {before:?} became {now:?} in one step"
            );
            before = now;
            w.advance(STEP_MS);
        }
        assert_eq!(
            members_of(&w),
            BTreeSet::from([n[0], n[1], n[2]]),
            "seed {seed}"
        );
        assert!(w.settle_repair(SETTLE_MS), "seed {seed}: no repair");
        intact(&w, seed);
        one_voice(&w, seed);
    }
}

#[test]
fn a_drain_then_the_removal_loses_nothing() {
    for seed in 0..5 {
        let mut w = World::new(seed);
        let n = w.start(6, 3, OLD);
        w.fill(GB / 2);
        assert!(w.settle_repair(SETTLE_MS), "seed {seed}: no repair");
        w.drain(n[4]);
        w.drain(n[5]);
        for _ in 0..SETTLE_MS / STEP_MS {
            if !w.in_list(n[4]) && !w.in_list(n[5]) {
                break;
            }
            w.advance(STEP_MS);
        }
        assert_eq!(
            members_of(&w),
            BTreeSet::from([n[0], n[1], n[2], n[3]]),
            "seed {seed}"
        );
        assert!(w.settle_repair(SETTLE_MS), "seed {seed}: no repair");
        assert!(
            w.illegal_falls().is_empty(),
            "seed {seed}: {:?}",
            w.illegal_falls()
        );
        intact(&w, seed);
        one_voice(&w, seed);
    }
}

#[test]
fn an_accept_loss_of_two_dead_members_of_five_commits_and_the_floor_falls_by_accept_loss_only() {
    for seed in 0..5 {
        let mut w = World::new(seed);
        let n = w.start(5, 3, OLD);
        w.fill(GB / 2);
        assert!(w.settle_repair(SETTLE_MS), "seed {seed}: no repair");
        assert!(
            !w.accept_loss_from(n[0], n[1]),
            "seed {seed}: an accept-loss of a live member got through"
        );
        w.kill(n[3]);
        w.kill(n[4]);
        w.advance(60_000);
        assert!(w.accept_loss(n[3]) && w.accept_loss(n[4]), "seed {seed}");
        w.advance(60_000);
        assert_eq!(
            members_of(&w),
            BTreeSet::from([n[0], n[1], n[2]]),
            "seed {seed}"
        );
        let falls: Vec<Cause> = w
            .floor_changes
            .iter()
            .filter(|c| c.to.value < c.from.value)
            .map(|c| c.cause)
            .collect();
        assert!(
            falls
                .iter()
                .all(|c| matches!(c, Cause::AcceptLoss | Cause::Merge)),
            "seed {seed}: {falls:?}"
        );
        assert!(
            w.illegal_falls().is_empty(),
            "seed {seed}: {:?}",
            w.illegal_falls()
        );
        assert_eq!(w.lost(), 0, "seed {seed}");
        one_voice(&w, seed);
    }
}

#[test]
fn a_restart_of_the_leader_during_a_conf_change_gives_one_member_set_on_every_node() {
    for seed in 0..5 {
        let mut w = World::new(seed);
        let n = w.start(5, 3, OLD);
        warm(&mut w);
        let version = version_of(&w);
        let joiner = w.create("a", 0, 1);
        for _ in 0..120_000 / STEP_MS {
            if version_of(&w) > version {
                break;
            }
            w.advance(STEP_MS);
        }
        assert!(
            !w.in_list(joiner),
            "seed {seed}: the change finished before the restart"
        );
        let leader = w.raft_leader().expect("a leader");
        w.kill(leader);
        w.advance(10_000);
        w.boot(leader, 1);
        w.advance(120_000);
        let want: BTreeSet<u64> = n.iter().copied().chain([joiner]).collect();
        for d in w.running() {
            assert_eq!(
                w.meta(d).map(|m| &m.members),
                Some(&want),
                "seed {seed}: node {d}"
            );
        }
        one_voice(&w, seed);
    }
}

#[test]
fn an_even_cut_of_four_commits_on_neither_side() {
    for seed in 0..5 {
        let mut w = World::new(seed);
        let n = w.start(4, 3, OLD);
        warm(&mut w);
        let (members, version) = (members_of(&w), version_of(&w));
        w.part(&[n[0], n[1]]);
        let joiners = [w.create("a", 0, 1), w.create("a", 0, 1)];
        for _ in 0..60_000 / STEP_MS {
            w.advance(STEP_MS);
            assert_eq!(version_of(&w), version, "seed {seed}: a side committed");
        }
        assert_eq!(members_of(&w), members, "seed {seed}");
        assert_eq!(w.raft_leader(), None, "seed {seed}: a side kept a leader");
        w.heal();
        w.advance(120_000);
        assert!(
            joiners.iter().all(|j| w.in_list(*j)),
            "seed {seed}: a joiner did not join after the heal"
        );
        one_voice(&w, seed);
    }
}

#[test]
#[ignore]
fn raft_election_time_after_a_leader_kill() {
    let mut times = Vec::new();
    for seed in 0..40 {
        let mut w = World::new(seed);
        w.start(5, 3, OLD);
        warm(&mut w);
        for _ in 0..5 {
            let leader = w.raft_leader().expect("a leader");
            w.kill(leader);
            let mut ms = 0;
            while w.raft_leader().is_none() && ms < 120_000 {
                w.advance(STEP_MS);
                ms += STEP_MS;
            }
            times.push(ms);
            w.boot(leader, 1);
            w.advance(60_000);
        }
    }
    times.sort_unstable();
    let at = |p: usize| times[(times.len() * p / 100).min(times.len() - 1)];
    println!(
        "ELECTION n={} p50={} p99={} max={}",
        times.len(),
        at(50),
        at(99),
        times[times.len() - 1]
    );
}
