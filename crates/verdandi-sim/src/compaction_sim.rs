use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use verdandi_core::ids::{MessageId, SegmentId};
use verdandi_core::lease::{Nack, Receipt, Settle};
use verdandi_core::retention::RETAIN_MS;
use verdandi_core::{MAX_FAILURES, SECOND_MS, Timing};
use verdandi_store::engine::{
    Engine, Error, Handle, IDLE_SEAL_MS, NewMessage, OsRandom, SettleBy, Take, Taken, TestClock,
};
use verdandi_store::fs::Fs;
use verdandi_store::recovery::recover;

use crate::fault_fs::{Crash, FaultFs};

const DATA: &str = "/data";

#[derive(Debug, Default)]
pub struct Report {
    pub steps: u64,
    pub crashes: usize,
    pub kept: usize,
}

pub struct Node {
    pub handle: Handle,
    task: JoinHandle<Result<(), Error>>,
}

pub fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

pub fn boot(rt: &Runtime, fs: &FaultFs, clock: &TestClock) -> Node {
    let (engine, handle) = rt
        .block_on(Engine::open(
            fs.clone(),
            Path::new(DATA),
            Arc::new(clock.clone()),
            Box::new(OsRandom),
        ))
        .expect("open");
    let task = rt.spawn(engine.run());
    Node { handle, task }
}

pub fn stop(rt: &Runtime, node: Node) -> Result<(), Error> {
    drop(node.handle);
    rt.block_on(node.task).expect("engine task")
}

pub fn publish(rt: &Runtime, node: &Node, stream: &str, values: &[&[u8]]) -> Vec<MessageId> {
    let messages = values
        .iter()
        .map(|v| NewMessage {
            stream: stream.to_owned(),
            headers: Vec::new(),
            value: v.to_vec(),
            timing: Timing::default(),
        })
        .collect();
    let published = rt
        .block_on(node.handle.publish(messages, None))
        .expect("publish");
    (0..published.count)
        .filter_map(|n| verdandi_core::nth_id(published.first, n))
        .collect()
}

pub fn take(rt: &Runtime, node: &Node, group: &str, stream: &str) -> Vec<MessageId> {
    let taken = rt.block_on(node.handle.take(Take {
        group: group.to_owned(),
        selector: stream.to_owned(),
        limit: 10_000,
        lease_ms: 30_000,
        wait: false,
    }));
    match taken {
        Ok(Taken::Leased { messages, .. }) => messages.iter().map(|d| d.handed.id).collect(),
        _ => Vec::new(),
    }
}

pub fn lease(rt: &Runtime, node: &Node, group: &str, stream: &str) -> (Receipt, Vec<MessageId>) {
    let taken = rt.block_on(node.handle.take(Take {
        group: group.to_owned(),
        selector: stream.to_owned(),
        limit: 10_000,
        lease_ms: 30_000,
        wait: false,
    }));
    match taken {
        Ok(Taken::Leased {
            receipt, messages, ..
        }) => (receipt, messages.iter().map(|d| d.handed.id).collect()),
        _ => (Receipt(0), Vec::new()),
    }
}

fn settle(rt: &Runtime, node: &Node, receipt: Receipt, settle: Settle) {
    rt.block_on(node.handle.settle(receipt, SettleBy::Ids(settle)))
        .expect("settle");
}

fn delete(rt: &Runtime, node: &Node, stream: &str) {
    rt.block_on(node.handle.delete_stream(stream.to_owned()))
        .expect("delete");
}

fn press(rt: &Runtime, node: &Node, fs: &FaultFs, clock: &TestClock, free: u64) {
    fs.set_capacity(fs.used() + free);
    clock.advance(1);
    rt.block_on(node.handle.compact()).expect("compact");
}

pub fn seal(rt: &Runtime, node: &Node, clock: &TestClock) {
    clock.advance(IDLE_SEAL_MS);
    rt.block_on(node.handle.damage()).expect("a tick");
    rt.block_on(node.handle.compact()).expect("the seal lands");
}

struct Scenario {
    fs: FaultFs,
    clock: TestClock,
    unacked: BTreeSet<MessageId>,
}

fn scenario(rt: &Runtime, seed: u64) -> Scenario {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let fs = FaultFs::new(seed, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(rt, &fs, &clock);
    let mut unacked = BTreeSet::new();
    for _ in 0..rng.random_range(2..4) {
        for _ in 0..rng.random_range(1..4) {
            let values: Vec<Vec<u8>> = (0..rng.random_range(1..4))
                .map(|n| format!("keep {n}").into_bytes())
                .collect();
            let refs: Vec<&[u8]> = values.iter().map(|v| &v[..]).collect();
            unacked.extend(publish(rt, &node, "keep", &refs));
            publish(rt, &node, "junk", &[b"junk"]);
            publish(rt, &node, "gone", &[b"gone"]);
        }
        let taken = rt.block_on(node.handle.take(Take {
            group: "g".to_owned(),
            selector: "keep".to_owned(),
            limit: rng.random_range(1..4),
            lease_ms: 30_000,
            wait: false,
        }));
        if let Ok(Taken::Leased {
            receipt, messages, ..
        }) = taken
        {
            let ids: Vec<MessageId> = messages.iter().map(|d| d.handed.id).collect();
            let acked: Vec<MessageId> = ids
                .iter()
                .enumerate()
                .filter(|(n, _)| *n == 0 || rng.random_bool(0.6))
                .map(|(_, id)| *id)
                .collect();
            let settle = SettleBy::Ids(Settle {
                ack: acked.clone(),
                ..Settle::default()
            });
            rt.block_on(node.handle.settle(receipt, settle))
                .expect("ack");
            rt.block_on(node.handle.release(receipt, ids))
                .expect("release");
            for id in acked {
                unacked.remove(&id);
            }
        }
        seal(rt, &node, &clock);
    }
    rt.block_on(node.handle.delete_stream("gone".to_owned()))
        .expect("delete");
    stop(rt, node).expect("clean stop");
    clock.advance(RETAIN_MS + 1);
    Scenario { fs, clock, unacked }
}

fn verify(rt: &Runtime, fs: &FaultFs, clock: &TestClock, unacked: &BTreeSet<MessageId>, at: &str) {
    let node = boot(rt, fs, clock);
    assert!(
        fs.list(Path::new(DATA))
            .expect("list")
            .iter()
            .all(|n| !n.ends_with(".tmp")),
        "{at}: a temporary file is left after recovery"
    );
    let handed = take(rt, &node, "g", "keep");
    let unique: BTreeSet<MessageId> = handed.iter().copied().collect();
    assert_eq!(
        unique.len(),
        handed.len(),
        "{at}: an id is handed out twice"
    );
    assert_eq!(&unique, unacked, "{at}: the unacked messages differ");
    assert!(
        take(rt, &node, "x", "gone").is_empty(),
        "{at}: a deleted message came back"
    );
    stop(rt, node).ok();
}

pub fn crash_at_every_step(seed: u64) -> Report {
    let rt = runtime();
    let base = scenario(&rt, seed);
    let mut report = Report {
        kept: base.unacked.len(),
        ..Report::default()
    };
    let clean = base.fs.snapshot();
    let node = boot(&rt, &clean, &base.clock);
    let before = clean.ops();
    rt.block_on(node.handle.compact()).expect("compact");
    let steps = clean.ops() - before;
    stop(&rt, node).expect("clean stop");
    verify(&rt, &clean, &base.clock, &base.unacked, "no crash");
    for step in 1..=steps + 1 {
        let mode = [Crash::Random, Crash::LoseAll, Crash::Kill][step as usize % 3];
        let fs = base.fs.snapshot();
        let node = boot(&rt, &fs, &base.clock);
        fs.freeze_after(step);
        rt.block_on(node.handle.compact()).ok();
        stop(&rt, node).ok();
        let fs = fs.crash(mode);
        let at = format!("seed {seed} step {step} of {steps} {mode:?}");
        verify(&rt, &fs, &base.clock, &base.unacked, &at);
        report.crashes += 1;
    }
    report.steps = steps;
    report
}

fn owners(fs: &FaultFs) -> BTreeMap<SegmentId, PathBuf> {
    let mut owner = BTreeMap::new();
    recover(&fs.snapshot(), Path::new(DATA), |path, header, _| {
        owner
            .entry(header.segment)
            .or_insert_with(|| path.to_path_buf());
    })
    .expect("recover");
    owner
}

fn compacted(path: &Path) -> bool {
    path.to_string_lossy().contains("cmp-")
}

pub fn two_segment_input_past_64_mib_with_a_crash_at_every_step() -> Report {
    let rt = runtime();
    let fs = FaultFs::new(13, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    let mib = vec![3u8; 1 << 20];
    let values = |n: usize| -> Vec<&[u8]> { (0..n).map(|_| &mib[..]).collect() };
    let mut unacked = BTreeSet::new();
    for _ in 0..2 {
        unacked.extend(publish(&rt, &node, "keep", &values(15)));
        unacked.extend(publish(&rt, &node, "keep", &values(15)));
        publish(&rt, &node, "gone", &values(16));
        publish(&rt, &node, "gone", &values(15));
        seal(&rt, &node, &clock);
    }
    for _ in 0..2 {
        unacked.extend(publish(&rt, &node, "keep", &values(3)));
        publish(&rt, &node, "gone", &values(4));
        publish(&rt, &node, "junk", &values(7));
        seal(&rt, &node, &clock);
    }
    delete(&rt, &node, "junk");
    seal(&rt, &node, &clock);
    delete(&rt, &node, "gone");
    stop(&rt, node).expect("clean stop");
    let before = owners(&fs);
    let merged: Vec<SegmentId> = before
        .iter()
        .filter(|(_, path)| compacted(path))
        .map(|(segment, _)| *segment)
        .collect();
    let clean = fs.snapshot();
    let node = boot(&rt, &clean, &clock);
    let start = clean.ops();
    rt.block_on(node.handle.compact()).expect("compact");
    let steps = clean.ops() - start;
    stop(&rt, node).expect("clean stop");
    let after = owners(&clean);
    let moved: Vec<&PathBuf> = merged.iter().filter_map(|s| after.get(s)).collect();
    assert!(
        moved.len() >= 2,
        "no compacted input with two segments: {before:?}"
    );
    let target: BTreeSet<&PathBuf> = moved.iter().copied().collect();
    assert_eq!(
        target.len(),
        1,
        "the segments of one input went to two outputs: {after:?}"
    );
    assert!(
        !target.contains(&before[&merged[0]]),
        "the input stayed: {after:?}"
    );
    let outputs: BTreeSet<&PathBuf> = after.values().filter(|p| compacted(p)).collect();
    assert_eq!(
        outputs.len(),
        2,
        "the older inputs did not fill a first output: {after:?}"
    );
    verify(&rt, &clean, &clock, &unacked, "no crash");
    let mut report = Report {
        steps,
        kept: unacked.len(),
        ..Report::default()
    };
    for step in 1..=steps + 1 {
        let mode = [Crash::Random, Crash::LoseAll, Crash::Kill][step as usize % 3];
        let base = fs.snapshot();
        let node = boot(&rt, &base, &clock);
        base.freeze_after(step);
        rt.block_on(node.handle.compact()).ok();
        stop(&rt, node).ok();
        let crashed = base.crash(mode);
        let at = format!("step {step} of {steps} {mode:?}");
        verify(&rt, &crashed, &clock, &unacked, &at);
        report.crashes += 1;
    }
    report
}

pub fn pressure_with_a_partial_placement_then_more_passes_and_a_crash() {
    let rt = runtime();
    let fs = FaultFs::new(11, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    let mib = vec![0u8; 1 << 20];
    let values = |n: usize| -> Vec<&[u8]> { (0..n).map(|_| &mib[..]).collect() };
    publish(&rt, &node, "a", &values(20));
    publish(&rt, &node, "a", &values(20));
    publish(&rt, &node, "x", &[b"x"]);
    seal(&rt, &node, &clock);
    publish(&rt, &node, "b", &values(10));
    publish(&rt, &node, "z", &[b"z"]);
    publish(&rt, &node, "y", &values(12));
    seal(&rt, &node, &clock);
    let mut kept = publish(&rt, &node, "c", &values(25));
    publish(&rt, &node, "y", &values(25));
    seal(&rt, &node, &clock);
    delete(&rt, &node, "y");
    seal(&rt, &node, &clock);
    delete(&rt, &node, "x");
    delete(&rt, &node, "z");
    seal(&rt, &node, &clock);
    press(&rt, &node, &fs, &clock, 200 << 20);
    delete(&rt, &node, "a");
    delete(&rt, &node, "b");
    clock.advance(1);
    rt.block_on(node.handle.compact()).expect("compact");
    seal(&rt, &node, &clock);
    stop(&rt, node).expect("clean stop");
    let fs = fs.crash(Crash::LoseAll);
    let node = boot(&rt, &fs, &clock);
    assert!(
        take(&rt, &node, "r", "b").is_empty(),
        "deleted messages of b came back"
    );
    assert!(
        take(&rt, &node, "r", "a").is_empty(),
        "deleted messages of a came back"
    );
    let mut handed = take(&rt, &node, "r", "c");
    handed.sort();
    kept.sort();
    assert_eq!(handed, kept, "the kept messages of c differ");
    stop(&rt, node).expect("clean stop");
}

struct Forgotten {
    fs: FaultFs,
    clock: TestClock,
    held: MessageId,
    forgotten_ms: u64,
}

fn forgotten(rt: &Runtime, seed: u64) -> Forgotten {
    let fs = FaultFs::new(seed, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(rt, &fs, &clock);
    publish(rt, &node, "s", &[b"zero"]);
    let big = vec![1u8; 64 << 10];
    let held = publish(rt, &node, "s", &[&big])[0];
    let (receipt, ids) = lease(rt, &node, "h", "s");
    settle(rt, &node, receipt, acked(&ids[..1]));
    rt.block_on(node.handle.release(receipt, vec![held]))
        .expect("release");
    let (mut receipt, ids) = lease(rt, &node, "g", "s");
    settle(rt, &node, receipt, acked(&ids[..1]));
    for n in 1..=MAX_FAILURES {
        if n == MAX_FAILURES {
            press(rt, &node, &fs, &clock, 200 << 20);
        }
        let nack = Settle {
            nack: vec![Nack {
                id: held,
                delay_ms: Some(0),
                error: None,
            }],
            ..Settle::default()
        };
        settle(rt, &node, receipt, nack);
        if n < MAX_FAILURES {
            receipt = lease(rt, &node, "g", "s").0;
        }
    }
    seal(rt, &node, &clock);
    fs.set_capacity(1 << 40);
    clock.advance(1);
    rt.block_on(node.handle.damage())
        .expect("the pressure ends");
    let forgotten_ms = node.handle.now_ms();
    rt.block_on(node.handle.forget("g".to_owned()))
        .expect("forget");
    clock.advance(8 * 24 * 3600 * SECOND_MS);
    stop(rt, node).expect("clean stop");
    Forgotten {
        fs,
        clock,
        held,
        forgotten_ms,
    }
}

fn acked(ids: &[MessageId]) -> Settle {
    Settle {
        ack: ids.to_vec(),
        ..Settle::default()
    }
}

fn verify_forgotten(rt: &Runtime, fs: &FaultFs, base: &Forgotten, at: &str) {
    let node = boot(rt, fs, &base.clock);
    let handed = take(rt, &node, "g", "s");
    delete(rt, &node, "barrier");
    let dead = take(rt, &node, "x", "s.dead.g");
    assert!(
        dead.iter().all(|id| id.ms() <= base.forgotten_ms),
        "{at}: a second dead copy {dead:?}"
    );
    assert_eq!(handed, [base.held], "{at}: the deleted group came back");
    stop(rt, node).ok();
}

pub fn forget_then_compaction_with_a_crash_at_every_step(seed: u64) -> Report {
    let rt = runtime();
    let base = forgotten(&rt, seed);
    let clean = base.fs.snapshot();
    let node = boot(&rt, &clean, &base.clock);
    let before = clean.ops();
    rt.block_on(node.handle.compact()).expect("compact");
    let steps = clean.ops() - before;
    stop(&rt, node).expect("clean stop");
    verify_forgotten(&rt, &clean, &base, "no crash");
    let mut report = Report {
        steps,
        ..Report::default()
    };
    for step in 1..=steps + 1 {
        let mode = [Crash::Random, Crash::LoseAll, Crash::Kill][step as usize % 3];
        let fs = base.fs.snapshot();
        let node = boot(&rt, &fs, &base.clock);
        fs.freeze_after(step);
        rt.block_on(node.handle.compact()).ok();
        stop(&rt, node).ok();
        let fs = fs.crash(mode);
        verify_forgotten(
            &rt,
            &fs,
            &base,
            &format!("seed {seed} step {step} of {steps} {mode:?}"),
        );
        report.crashes += 1;
    }
    report
}
