use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

use rand::Rng;
use verdandi_sim::cluster_sim::{HOUR_MS, SEGMENT_BYTES, STEP_MS, World};

const SEEDS: u64 = 100;
const LONG_SEEDS: u64 = 1_000;
const GB: u64 = 1_000_000_000;
const OLD: u64 = 25 * HOUR_MS;
const SIX_HOURS: u64 = 6 * HOUR_MS;
const SAMPLE_GRACE_MS: u64 = 3 * 5_000;
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
    let mut first: Option<(u64, u64)> = None;
    let mut last_loss = 0;
    for _ in 0..10 {
        let running = world.running();
        let victim = running[world.rng.random_range(0..running.len())];
        let zone = ["a", "b", "c"][(victim % 3) as usize];
        world.delete(victim);
        last_loss = world.run_ms;
        world.create(zone, 0, 1);
        for _ in 0..30_000 / STEP_MS {
            world.advance(STEP_MS);
            if first.is_none()
                && let Some(by) = world
                    .running()
                    .into_iter()
                    .find(|d| world.marked_absent(*d, victim))
            {
                first = Some((world.now_ms, by));
            }
        }
        world.publish();
        world.settle();
    }
    let (marked, by) = first.expect("a death was marked");
    let rise = world
        .count_changes
        .iter()
        .find(|c| c.disk == by && c.now_ms >= marked && c.to > c.from)
        .copied()
        .unwrap_or_else(|| panic!("seed {seed}: no count rise on {by}"));
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
        rise_ms: rise.now_ms - marked,
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
    for (seed, falls) in seeds(count, week).into_iter().enumerate() {
        assert!(falls >= 1, "seed {seed}: the floor never fell in the week");
    }
    println!("ROW 18 SIM PASS week losses=13 seeds={count} floor-fell=1");
}

#[test]
fn row18_the_controller_under_a_storm() {
    row18(SEEDS);
}

#[test]
#[ignore]
fn row18_the_controller_under_a_storm_at_1000_seeds() {
    row18(LONG_SEEDS);
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
    let nodes = world.start(3, 3, OLD);
    world.place(&nodes, SEGMENT_BYTES);
    warm(&mut world);
    world.kill(nodes[2]);
    world.settle();
    world.kill(nodes[0]);
    world.kill(nodes[1]);
    assert!(world.boot(nodes[2], 1));
    let receiver = world.create("a", OLD, 1);
    world.advance(30_000);
    assert!(
        world
            .segments
            .values()
            .all(|s| s.marks.contains_key(&receiver))
    );
    assert_eq!(world.lost_settles(), 1);
    world.delete(nodes[0]);
    world.delete(nodes[1]);
    assert_eq!(world.lost_settles(), 1);
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
                    let expected = groups.destination(*id, seg.writer, |d| running.contains(&d));
                    counts[2] += usize::from(expected.iter().all(inside));
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

fn scale(seed: u64) -> KillSets {
    let mut world = World::new(seed);
    world.headroom = 0.1;
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
        world.running().iter().map(|d| world.count(*d)).collect::<Vec<_>>()
    );
    kill_sets(&world)
}

fn row12(count: u64) {
    let runs = seeds(count, scale);
    for (seed, r) in runs.iter().enumerate() {
        assert_eq!(r.drift, 0, "seed {seed}: drift {}", r.drift);
        assert_eq!(r.mismatches, 0, "seed {seed}: mismatches {}", r.mismatches);
    }
    let range = |f: fn(&KillSets) -> usize| {
        let min = runs.iter().map(f).min().unwrap_or(0);
        let max = runs.iter().map(f).max().unwrap_or(0);
        format!("{min}..{max}")
    };
    println!(
        "ROW 12 SIM PASS seeds={count} killsets=20 losing={} current-group-sets={} drift=0 mismatches=0",
        range(|r| r.losing),
        range(|r| r.current)
    );
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
