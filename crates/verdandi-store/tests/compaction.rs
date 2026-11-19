use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use verdandi_core::Timing;
use verdandi_core::ids::{Address, MessageId, SegmentId};
use verdandi_core::lease::{Receipt, Settle};
use verdandi_core::retention::RETAIN_MS;
use verdandi_sim::fault_fs::{Crash, FaultFile, FaultFs};
use verdandi_store::block::{
    BLOCK_HEADER_LEN, BlockHeader, FILE_HEADER_LEN, FileHeader, FileKind, PAGE, SEGMENT_BYTES,
    align, encode_block,
};
use verdandi_store::compaction::COMPACT_FREE;
use verdandi_store::engine::{
    Engine, Error, IDLE_SEAL_MS, Key, NewMessage, OsRandom, SettleBy, Take, Taken, TestClock,
};
use verdandi_store::fs::{Fs, FsFile, FsStat};
use verdandi_store::log::Log;
use verdandi_store::records::{self, Control, Message};
use verdandi_store::recovery::recover;
use verdandi_store::retention::{RESCAN_MS, SWEEP_MS};

const DATA: &str = "/data";

struct Node {
    handle: verdandi_store::engine::Handle,
    task: JoinHandle<Result<(), Error>>,
}

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

fn boot<F: Fs + Clone>(rt: &Runtime, fs: &F, clock: &TestClock) -> Node {
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

fn stop(rt: &Runtime, node: Node) -> Result<(), Error> {
    drop(node.handle);
    rt.block_on(node.task).expect("engine task")
}

fn message(stream: &str, value: &[u8]) -> NewMessage {
    NewMessage {
        stream: stream.to_owned(),
        headers: Vec::new(),
        value: value.to_vec(),
        timing: Timing::default(),
    }
}

fn publish(rt: &Runtime, node: &Node, stream: &str, value: &[u8]) -> MessageId {
    rt.block_on(node.handle.publish(vec![message(stream, value)], None))
        .expect("publish")
        .first
}

fn take(
    rt: &Runtime,
    node: &Node,
    group: &str,
    stream: &str,
) -> (Receipt, Vec<(MessageId, Address)>) {
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
        }) => (
            receipt,
            messages
                .iter()
                .map(|d| (d.handed.id, d.handed.address))
                .collect(),
        ),
        _ => (Receipt(0), Vec::new()),
    }
}

fn ack(rt: &Runtime, node: &Node, receipt: Receipt, ids: &[MessageId]) {
    let settle = SettleBy::Ids(Settle {
        ack: ids.to_vec(),
        ..Settle::default()
    });
    rt.block_on(node.handle.settle(receipt, settle))
        .expect("ack");
}

fn seal(rt: &Runtime, node: &Node, clock: &TestClock) {
    clock.advance(IDLE_SEAL_MS);
    rt.block_on(node.handle.damage()).expect("a tick");
    rt.block_on(node.handle.compact()).expect("compact");
}

fn compact(rt: &Runtime, node: &Node, clock: &TestClock) {
    clock.advance(1);
    rt.block_on(node.handle.compact()).expect("compact");
}

type Seen = Vec<(PathBuf, BlockHeader, Vec<u8>)>;

fn blocks(fs: &FaultFs) -> Seen {
    let mut seen = Vec::new();
    recover(&fs.snapshot(), Path::new(DATA), |path, header, body| {
        seen.push((path.to_path_buf(), *header, body.to_vec()))
    })
    .expect("recover");
    seen
}

fn files(fs: &FaultFs) -> Vec<String> {
    fs.list(Path::new(DATA)).expect("list")
}

fn controls(seen: &Seen) -> Vec<Control> {
    seen.iter()
        .flat_map(|(_, _, body)| records::decode(body).expect("body").controls)
        .collect()
}

fn values(seen: &Seen) -> Vec<Vec<u8>> {
    seen.iter()
        .flat_map(|(_, _, body)| {
            let body = records::decode(body).expect("body");
            body.runs
                .iter()
                .flat_map(|(entry, bytes)| records::decode_run(bytes, entry.count).expect("run"))
                .map(|m| m.value)
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn compaction_keeps_every_address_and_an_ack_after_it_still_applies() {
    let rt = runtime();
    let fs = FaultFs::new(1, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    for value in [&[b'a'; 4096][..], b"b", b"c"] {
        publish(&rt, &node, "s", value);
    }
    let (receipt, before) = take(&rt, &node, "g", "s");
    ack(&rt, &node, receipt, &[before[0].0]);
    seal(&rt, &node, &clock);
    clock.advance(RETAIN_MS);
    compact(&rt, &node, &clock);
    assert_eq!(values(&blocks(&fs)), [b"b".to_vec(), b"c".to_vec()]);
    let (receipt, after) = take(&rt, &node, "g", "s");
    assert_eq!(after, before[1..]);
    ack(&rt, &node, receipt, &[after[0].0]);
    stop(&rt, node).expect("clean stop");
    let node = boot(&rt, &fs, &clock);
    let (_, restarted) = take(&rt, &node, "g", "s");
    assert_eq!(restarted, before[2..]);
    stop(&rt, node).expect("clean stop");
}

#[test]
fn compaction_carries_the_control_records_that_still_apply_and_drops_the_rest() {
    let rt = runtime();
    let fs = FaultFs::new(2, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    let gone = publish(&rt, &node, "s", &[b'g'; 4096]);
    let held = publish(&rt, &node, "s", b"held");
    let other = publish(&rt, &node, "s", &[b'x'; 4096]);
    let (receipt, _) = take(&rt, &node, "g", "s");
    for id in [held, other, gone] {
        ack(&rt, &node, receipt, &[id]);
        clock.advance(1);
    }
    let (receipt, _) = take(&rt, &node, "h", "s");
    ack(&rt, &node, receipt, &[gone, other]);
    seal(&rt, &node, &clock);
    clock.advance(RETAIN_MS);
    let acks = |controls: &[Control]| -> BTreeSet<(String, u32)> {
        controls
            .iter()
            .filter_map(|c| match c {
                Control::Record(verdandi_core::Record::Ack { group, address, .. }) => {
                    Some((group.clone(), address.position))
                }
                _ => None,
            })
            .collect()
    };
    assert_eq!(acks(&controls(&blocks(&fs))).len(), 5);
    compact(&rt, &node, &clock);
    let after = controls(&blocks(&fs));
    assert_eq!(values(&blocks(&fs)), [b"held".to_vec()]);
    assert_eq!(
        acks(&after),
        [
            ("g".to_owned(), 0),
            ("g".to_owned(), 1),
            ("h".to_owned(), 0),
            ("h".to_owned(), 2)
        ]
        .into_iter()
        .collect()
    );
    let read_sets = after
        .iter()
        .filter(|c| matches!(c, Control::Record(verdandi_core::Record::ReadSet { .. })))
        .count();
    assert_eq!(read_sets, 2);
    stop(&rt, node).expect("clean stop");
    let node = boot(&rt, &fs, &clock);
    assert!(take(&rt, &node, "g", "s").1.is_empty());
    let (_, by_h) = take(&rt, &node, "h", "s");
    assert_eq!(by_h.iter().map(|(id, _)| *id).collect::<Vec<_>>(), [held]);
    stop(&rt, node).expect("clean stop");
}

type Hook = Option<Box<dyn FnOnce(&FaultFs) + Send>>;

#[derive(Clone)]
struct Recorded {
    fs: FaultFs,
    ops: Arc<Mutex<Vec<String>>>,
    fail_remove: Arc<Mutex<bool>>,
    opened: Arc<Mutex<Hook>>,
}

struct RecordedFile {
    file: FaultFile,
    name: String,
    ops: Arc<Mutex<Vec<String>>>,
}

fn name(path: &Path) -> String {
    path.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default()
        .to_owned()
}

impl Recorded {
    fn new(fs: FaultFs) -> Self {
        Self {
            fs,
            ops: Arc::default(),
            fail_remove: Arc::default(),
            opened: Arc::default(),
        }
    }

    fn on_open(&self, hook: impl FnOnce(&FaultFs) + Send + 'static) {
        *self.opened.lock().expect("hook") = Some(Box::new(hook));
    }

    fn note(&self, op: String) {
        self.ops.lock().expect("ops").push(op);
    }

    fn taken(&self) -> Vec<String> {
        std::mem::take(&mut *self.ops.lock().expect("ops"))
    }
}

impl FsFile for RecordedFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.file.read_at(buf, offset)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.file.write_at(buf, offset)
    }

    fn allocate(&self, len: u64) -> io::Result<()> {
        self.file.allocate(len)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.ops
            .lock()
            .expect("ops")
            .push(format!("sync {}", self.name));
        self.file.sync_data()
    }

    fn size(&self) -> io::Result<u64> {
        self.file.size()
    }
}

impl Fs for Recorded {
    type File = RecordedFile;

    fn create(&self, path: &Path) -> io::Result<RecordedFile> {
        self.note(format!("create {}", name(path)));
        Ok(RecordedFile {
            file: self.fs.create(path)?,
            name: name(path),
            ops: Arc::clone(&self.ops),
        })
    }

    fn open(&self, path: &Path) -> io::Result<RecordedFile> {
        let hook = self.opened.lock().expect("hook").take();
        if let Some(hook) = hook {
            hook(&self.fs);
        }
        Ok(RecordedFile {
            file: self.fs.open(path)?,
            name: name(path),
            ops: Arc::clone(&self.ops),
        })
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        self.note("sync_dir".to_owned());
        self.fs.sync_dir(dir)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.note(format!("rename {} {}", name(from), name(to)));
        self.fs.rename(from, to)
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        self.note(format!("remove {}", name(path)));
        if *self.fail_remove.lock().expect("flag") {
            return Err(io::Error::other("injected EIO on remove"));
        }
        self.fs.remove(path)
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
        self.fs.list(dir)
    }

    fn statfs(&self, path: &Path) -> io::Result<FsStat> {
        self.fs.statfs(path)
    }
}

fn two_sealed_segments(rt: &Runtime, node: &Node, clock: &TestClock) -> Vec<MessageId> {
    let mut kept = Vec::new();
    for _ in 0..2 {
        kept.push(publish(rt, node, "keep", b"keep"));
        publish(rt, node, "gone", b"gone");
        clock.advance(IDLE_SEAL_MS);
        rt.block_on(node.handle.damage()).expect("a tick");
        rt.block_on(node.handle.publish(vec![message("keep", b"x")], None))
            .map(|p| kept.push(p.first))
            .expect("publish");
    }
    rt.block_on(node.handle.delete_stream("gone".to_owned()))
        .expect("delete");
    kept
}

#[test]
fn compaction_deletes_its_inputs_only_after_the_output_and_its_directory_are_synced() {
    let rt = runtime();
    let fs = Recorded::new(FaultFs::new(3, 1 << 40));
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    two_sealed_segments(&rt, &node, &clock);
    fs.taken();
    seal(&rt, &node, &clock);
    let ops = fs.taken();
    let at = |prefix: &str| {
        ops.iter()
            .position(|op| op.starts_with(prefix))
            .unwrap_or_else(|| panic!("no {prefix} in {ops:?}"))
    };
    let create = at("create cmp-");
    let temp = ops[create].trim_start_matches("create ").to_owned();
    let synced = at(&format!("sync {temp}"));
    let rename = at(&format!("rename {temp}"));
    let dir_after_rename = rename
        + ops[rename..]
            .iter()
            .position(|op| op == "sync_dir")
            .expect("a directory sync after the rename");
    let first_remove = at("remove ");
    let last_remove = ops
        .iter()
        .rposition(|op| op.starts_with("remove "))
        .expect("a remove");
    assert!(
        create < synced && synced < rename && dir_after_rename < first_remove,
        "{ops:?}"
    );
    assert_eq!(ops[last_remove + 1], "sync_dir", "{ops:?}");
    stop(&rt, node).expect("clean stop");
}

#[test]
fn compaction_after_a_crash_between_the_rename_and_the_delete_gives_no_duplicate_and_no_removed_message()
 {
    let rt = runtime();
    let fs = Recorded::new(FaultFs::new(4, 1 << 40));
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    let mut kept = two_sealed_segments(&rt, &node, &clock);
    *fs.fail_remove.lock().expect("flag") = true;
    clock.advance(IDLE_SEAL_MS);
    rt.block_on(node.handle.damage()).expect("a tick");
    assert_eq!(rt.block_on(node.handle.compact()), Err(Error::Stopped));
    assert_eq!(stop(&rt, node), Err(Error::Stopped));
    let names = files(&fs.fs);
    assert!(names.iter().any(|n| n.starts_with("cmp-")), "{names:?}");
    assert!(
        names.iter().filter(|n| n.starts_with("seg-")).count() >= 2,
        "{names:?}"
    );
    let crashed = fs.fs.crash(Crash::Kill);
    let temp = Path::new(DATA).join("cmp-00000000000000ff.tmp");
    crashed.create(&temp).expect("an orphan temporary file");
    let node = boot(&rt, &crashed, &clock);
    let after = files(&crashed);
    assert!(after.iter().all(|n| !n.ends_with(".tmp")), "{after:?}");
    let newest = names
        .iter()
        .filter(|n| n.starts_with("cmp-"))
        .max()
        .expect("an output");
    assert!(after.contains(newest), "the output lost: {after:?}");
    assert!(after.len() < names.len(), "no loser removed: {after:?}");
    let (_, handed) = take(&rt, &node, "g", "keep");
    let mut handed: Vec<MessageId> = handed.iter().map(|(id, _)| *id).collect();
    handed.sort();
    kept.sort();
    assert_eq!(handed, kept);
    assert!(take(&rt, &node, "h", "gone").1.is_empty());
    let mut seen = BTreeMap::new();
    for (path, header, _) in blocks(&crashed) {
        let first = seen.entry(header.segment).or_insert_with(|| path.clone());
        assert_eq!(*first, path, "segment {} is in two files", header.segment);
    }
    stop(&rt, node).expect("clean stop");
}

#[test]
fn compaction_puts_one_source_segment_in_one_output_with_one_block_per_source_block() {
    let rt = runtime();
    let fs = FaultFs::new(5, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    for _ in 0..3 {
        for value in [b"one".as_slice(), b"two", b"three"] {
            clock.advance(7);
            publish(&rt, &node, "s", value);
            clock.advance(7);
            publish(&rt, &node, "gone", value);
        }
        clock.advance(IDLE_SEAL_MS);
        rt.block_on(node.handle.damage()).expect("a tick");
        publish(&rt, &node, "s", b"next");
    }
    rt.block_on(node.handle.delete_stream("gone".to_owned()))
        .expect("delete");
    let before = blocks(&fs);
    seal(&rt, &node, &clock);
    let after = blocks(&fs);
    let source: BTreeMap<(SegmentId, u64), u32> = before
        .iter()
        .map(|(_, h, _)| ((h.segment, h.commit_ms), h.compaction_version))
        .collect();
    let mut file_of: BTreeMap<SegmentId, PathBuf> = BTreeMap::new();
    let mut copied = 0;
    for (path, header, _) in &after {
        assert_eq!(
            file_of
                .entry(header.segment)
                .or_insert_with(|| path.clone()),
            path
        );
        if !path.to_string_lossy().contains("cmp-") {
            continue;
        }
        copied += 1;
        let version = source
            .get(&(header.segment, header.commit_ms))
            .unwrap_or_else(|| panic!("an output block with no source block: {header:?}"));
        assert_eq!(header.compaction_version, version + 1);
    }
    let count = |seen: &Seen| {
        let mut count: BTreeMap<(SegmentId, u64), usize> = BTreeMap::new();
        for (_, h, _) in seen {
            *count.entry((h.segment, h.commit_ms)).or_default() += 1;
        }
        count
    };
    let sources = count(&before);
    for (pair, n) in count(&after) {
        assert!(
            sources.get(&pair).is_some_and(|m| n <= *m),
            "more output blocks than source blocks for {pair:?}"
        );
    }
    assert!(copied >= 3, "{after:?}");
    stop(&rt, node).expect("clean stop");
}

#[test]
fn compaction_keeps_an_idempotency_key_younger_than_24_h_after_its_messages_leave() {
    let rt = runtime();
    let fs = FaultFs::new(6, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    let key = Key {
        scope: "k".to_owned(),
        key: "order-1".to_owned(),
    };
    let first = rt
        .block_on(
            node.handle
                .publish(vec![message("k", b"v")], Some(key.clone())),
        )
        .expect("publish");
    rt.block_on(node.handle.delete_stream("k".to_owned()))
        .expect("delete");
    seal(&rt, &node, &clock);
    assert!(values(&blocks(&fs)).is_empty());
    stop(&rt, node).expect("clean stop");
    let node = boot(&rt, &fs, &clock);
    let again = rt
        .block_on(node.handle.publish(vec![message("k", b"w")], Some(key)))
        .expect("repeat");
    assert!(again.repeat);
    assert_eq!((again.first, again.count), (first.first, first.count));
    stop(&rt, node).expect("clean stop");
}

#[test]
fn compaction_merges_1000_sealed_files_of_4_kib_whatever_their_removable_ratio() {
    let fs = FaultFs::new(7, 1 << 40);
    let recovered = recover(&fs, Path::new(DATA), |_, _, _| {}).expect("recover");
    let log = Log::start(fs.clone(), 7, recovered);
    for n in 0..1_000u32 {
        let id = MessageId::new(1_000_000 + u64::from(n), 7, 0).expect("id");
        let m = Message {
            position: 0,
            id,
            timing: Timing::default(),
            origin: None,
            headers: Vec::new(),
            value: n.to_le_bytes().to_vec(),
        };
        let (body, _) = records::encode(&[("s", std::slice::from_ref(&m))], &[]);
        log.append(1_000_000, body, SEGMENT_BYTES)
            .blocking_recv()
            .expect("reply")
            .expect("append");
        log.seal().blocking_recv().expect("reply").expect("seal");
    }
    drop(log);
    assert_eq!(files(&fs).len(), 1_000);
    assert!(
        fs.open(&Path::new(DATA).join(&files(&fs)[0]))
            .expect("open")
            .size()
            .expect("size")
            <= 2 * PAGE
    );
    let rt = runtime();
    let clock = TestClock::new(2_000_000);
    let node = boot(&rt, &fs, &clock);
    compact(&rt, &node, &clock);
    let left = files(&fs);
    assert_eq!(left.len(), 1, "{} files", left.len());
    let (_, handed) = take(&rt, &node, "g", "s");
    assert_eq!(handed.len(), 1_000);
    stop(&rt, node).expect("clean stop");
}

#[test]
fn compaction_keeps_a_group_that_acked_once_non_provisional_after_its_acked_data_leaves() {
    let rt = runtime();
    let fs = FaultFs::new(8, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    publish(&rt, &node, "s", &[b'a'; 4096]);
    let (receipt, first) = take(&rt, &node, "g", "s");
    ack(&rt, &node, receipt, &[first[0].0]);
    seal(&rt, &node, &clock);
    clock.advance(RETAIN_MS);
    compact(&rt, &node, &clock);
    assert!(values(&blocks(&fs)).is_empty());
    let pending = publish(&rt, &node, "s", b"pending");
    seal(&rt, &node, &clock);
    clock.advance(RETAIN_MS);
    stop(&rt, node).expect("clean stop");
    let node = boot(&rt, &fs, &clock);
    compact(&rt, &node, &clock);
    let (_, handed) = take(&rt, &node, "g", "s");
    assert_eq!(
        handed.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        [pending]
    );
    stop(&rt, node).expect("clean stop");
}

fn cmp_files(fs: &FaultFs) -> Vec<String> {
    files(fs)
        .into_iter()
        .filter(|n| n.starts_with("cmp-"))
        .collect()
}

fn delete(rt: &Runtime, node: &Node, stream: &str) {
    rt.block_on(node.handle.delete_stream(stream.to_owned()))
        .expect("delete");
}

fn a_compacted_file_of_one_tombstone(rt: &Runtime, node: &Node, clock: &TestClock, fs: &FaultFs) {
    publish(rt, node, "s", b"old");
    seal(rt, node, clock);
    delete(rt, node, "s");
    seal(rt, node, clock);
    assert_eq!(cmp_files(fs).len(), 1, "{:?}", files(fs));
}

#[test]
fn under_pressure_a_compacted_file_whose_control_records_no_longer_apply_goes() {
    let rt = runtime();
    let fs = FaultFs::new(31, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    a_compacted_file_of_one_tombstone(&rt, &node, &clock, &fs);
    fs.set_capacity(fs.used() + (200 << 20));
    compact(&rt, &node, &clock);
    assert!(cmp_files(&fs).is_empty(), "{:?}", files(&fs));
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_compacted_file_whose_control_records_no_longer_apply_goes_at_the_next_rescan() {
    let rt = runtime();
    let fs = FaultFs::new(32, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    a_compacted_file_of_one_tombstone(&rt, &node, &clock, &fs);
    compact(&rt, &node, &clock);
    assert_eq!(cmp_files(&fs).len(), 1, "{:?}", files(&fs));
    clock.advance(RESCAN_MS);
    compact(&rt, &node, &clock);
    assert!(cmp_files(&fs).is_empty(), "{:?}", files(&fs));
    stop(&rt, node).expect("clean stop");
}

#[test]
fn the_hourly_sweep_compacts_old_removable_data_under_the_half_rule() {
    let rt = runtime();
    let fs = FaultFs::new(33, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    publish(&rt, &node, "s", &[b'h'; 4096]);
    let small = publish(&rt, &node, "s", b"a");
    let (receipt, _) = take(&rt, &node, "g", "s");
    ack(&rt, &node, receipt, &[small]);
    seal(&rt, &node, &clock);
    clock.advance(RETAIN_MS);
    compact(&rt, &node, &clock);
    let old = files(&fs);
    clock.advance(SWEEP_MS / 2);
    publish(&rt, &node, "u", &[b'u'; 4096]);
    publish(&rt, &node, "t", b"y");
    delete(&rt, &node, "t");
    seal(&rt, &node, &clock);
    let young: Vec<String> = files(&fs)
        .into_iter()
        .filter(|n| n.starts_with("seg-") && !old.contains(n))
        .collect();
    clock.advance(SWEEP_MS / 2);
    compact(&rt, &node, &clock);
    assert_eq!(cmp_files(&fs).len(), 1, "{:?}", files(&fs));
    assert!(!young.is_empty());
    for name in &young {
        assert!(files(&fs).contains(name), "the sweep rewrote {name}");
    }
    assert_eq!(
        values(&blocks(&fs)),
        [vec![b'h'; 4096], vec![b'u'; 4096], b"y".to_vec()]
    );
    stop(&rt, node).expect("clean stop");
}

fn rewrite(fs: &FaultFs, epoch: u64, version: u32, blocks: &[&(PathBuf, BlockHeader, Vec<u8>)]) {
    let file = fs
        .create(&Path::new(DATA).join(format!("cmp-{epoch:016x}.vdl")))
        .expect("create");
    let header = FileHeader {
        kind: FileKind::Compacted,
        epoch,
    };
    file.write_at(&header.encode(), 0).expect("file header");
    let mut at = PAGE;
    for (_, block, body) in blocks {
        let header = BlockHeader {
            offset: at,
            epoch,
            compaction_version: version,
            ..*block
        };
        let bytes = encode_block(&header, body);
        file.write_at(&bytes, at).expect("block");
        at = align(at + bytes.len() as u64);
    }
    file.sync_data().expect("sync");
}

#[test]
fn compaction_keeps_the_newer_file_of_a_segment_while_an_older_file_still_holds_blocks_of_it() {
    let rt = runtime();
    let fs = FaultFs::new(33, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    publish(&rt, &node, "b", b"deleted");
    seal(&rt, &node, &clock);
    let kept = publish(&rt, &node, "c", &[b'c'; 16384]);
    seal(&rt, &node, &clock);
    stop(&rt, node).expect("clean stop");
    let seen = blocks(&fs);
    let first = seen[0].1.segment;
    let all: Vec<_> = seen.iter().collect();
    let older: Vec<_> = seen.iter().filter(|(_, h, _)| h.segment == first).collect();
    for name in files(&fs) {
        fs.remove(&Path::new(DATA).join(name)).expect("remove");
    }
    rewrite(&fs, 0x100, 1, &all);
    rewrite(&fs, 0x101, 2, &older);
    fs.sync_dir(Path::new(DATA)).expect("sync dir");
    let node = boot(&rt, &fs, &clock);
    fs.set_capacity(fs.used() + COMPACT_FREE + 8192);
    delete(&rt, &node, "b");
    compact(&rt, &node, &clock);
    seal(&rt, &node, &clock);
    stop(&rt, node).expect("clean stop");
    let fs = fs.crash(Crash::LoseAll);
    fs.set_capacity(1 << 40);
    let node = boot(&rt, &fs, &clock);
    assert!(
        take(&rt, &node, "r", "b").1.is_empty(),
        "the deleted message came back"
    );
    compact(&rt, &node, &clock);
    compact(&rt, &node, &clock);
    let left = files(&fs);
    assert!(
        left.iter()
            .all(|n| n != "cmp-0000000000000100.vdl" && n != "cmp-0000000000000101.vdl"),
        "{left:?}"
    );
    let (_, by_r) = take(&rt, &node, "r", "c");
    assert_eq!(by_r.iter().map(|(id, _)| *id).collect::<Vec<_>>(), [kept]);
    stop(&rt, node).expect("clean stop");
}

#[test]
fn compaction_names_every_segment_whose_runs_lie_in_a_damaged_block_with_an_unreadable_header() {
    let rt = runtime();
    let fs = FaultFs::new(34, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    let first = publish(&rt, &node, "a", b"first");
    publish(&rt, &node, "j", &[b'j'; 8192]);
    seal(&rt, &node, &clock);
    publish(&rt, &node, "b", &[b'b'; 16384]);
    publish(&rt, &node, "j", &[b'j'; 20480]);
    seal(&rt, &node, &clock);
    delete(&rt, &node, "j");
    seal(&rt, &node, &clock);
    let merged = cmp_files(&fs);
    assert_eq!(merged.len(), 1, "{:?}", files(&fs));
    assert!(fs.flip_at(&Path::new(DATA).join(&merged[0]), PAGE, 0xff));
    delete(&rt, &node, "b");
    seal(&rt, &node, &clock);
    let recorded = controls(&blocks(&fs));
    let segment = SegmentId {
        incarnation: first.incarnation,
        seq: 0,
    };
    assert!(
        recorded.iter().any(|c| matches!(c, Control::Damage(d)
            if d.segment == segment && (d.start..d.end).contains(&PAGE))),
        "{recorded:?}"
    );
    stop(&rt, node).expect("clean stop");
}

#[test]
fn compaction_names_the_segment_of_a_control_block_in_a_damaged_range_that_holds_a_run_of_another()
{
    let rt = runtime();
    let fs = FaultFs::new(35, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    let first = publish(&rt, &node, "a", b"first");
    stop(&rt, node).expect("clean stop");
    let node = boot(&rt, &fs, &clock);
    delete(&rt, &node, "zz");
    let second = publish(&rt, &node, "j", &[b'j'; 16384]);
    stop(&rt, node).expect("clean stop");
    let run = SegmentId {
        incarnation: first.incarnation,
        seq: 0,
    };
    let control = SegmentId {
        incarnation: second.incarnation,
        seq: 0,
    };
    let seen = blocks(&fs);
    let of = |segment: SegmentId| seen.iter().filter(move |(_, h, _)| h.segment == segment);
    let ordered: Vec<_> = of(run).chain(of(control)).collect();
    assert_eq!(ordered.len(), seen.len());
    let runs = |n: usize| records::decode(&ordered[n].2).expect("body").runs.len();
    let split = of(run).count();
    assert!(runs(split - 1) > 0 && runs(split) == 0, "split at {split}");
    for name in files(&fs) {
        fs.remove(&Path::new(DATA).join(name)).expect("remove");
    }
    rewrite(&fs, 0x100, 1, &ordered);
    fs.sync_dir(Path::new(DATA)).expect("sync dir");
    let node = boot(&rt, &fs, &clock);
    let path = Path::new(DATA).join("cmp-0000000000000100.vdl");
    let starts: Vec<u64> = blocks(&fs)
        .iter()
        .filter(|(p, _, _)| *p == path)
        .map(|(_, h, _)| h.offset)
        .collect();
    assert_eq!(starts.len(), ordered.len(), "{starts:?}");
    let at = starts[split];
    assert!(fs.flip_at(&path, starts[split - 1], 0xff));
    assert!(fs.flip_at(&path, at, 0xff));
    delete(&rt, &node, "j");
    seal(&rt, &node, &clock);
    let recorded = controls(&blocks(&fs));
    assert!(
        recorded.iter().any(|c| matches!(c, Control::Damage(d)
            if d.segment == control && (d.start..d.end).contains(&at))),
        "{recorded:?}"
    );
    stop(&rt, node).expect("clean stop");
}

#[test]
fn under_pressure_a_compacted_file_whose_control_records_all_apply_is_not_written_again() {
    let rt = runtime();
    let fs = FaultFs::new(35, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    let id = publish(&rt, &node, "s", b"acked");
    let (receipt, _) = take(&rt, &node, "g", "s");
    ack(&rt, &node, receipt, &[id]);
    seal(&rt, &node, &clock);
    fs.set_capacity(fs.used() + (200 << 20));
    compact(&rt, &node, &clock);
    let before = cmp_files(&fs);
    assert_eq!(before.len(), 1, "{:?}", files(&fs));
    compact(&rt, &node, &clock);
    assert_eq!(cmp_files(&fs), before);
    stop(&rt, node).expect("clean stop");
}

#[test]
fn compaction_without_pressure_writes_no_output_that_takes_free_space_under_192_mib() {
    let rt = runtime();
    let fs = Recorded::new(FaultFs::new(36, 1 << 40));
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    two_sealed_segments(&rt, &node, &clock);
    fs.on_open(|fs| fs.set_capacity(fs.used() + COMPACT_FREE / 2));
    seal(&rt, &node, &clock);
    assert!(cmp_files(&fs.fs).is_empty(), "{:?}", files(&fs.fs));
    stop(&rt, node).expect("clean stop");
}

fn seg_files(fs: &FaultFs) -> Vec<String> {
    let mut names: Vec<String> = files(fs)
        .into_iter()
        .filter(|n| n.starts_with("seg-"))
        .collect();
    names.sort();
    names
}

#[test]
fn compaction_skips_an_input_whose_damage_record_is_refused_and_deletes_a_later_removable_input() {
    let rt = runtime();
    let fs = FaultFs::new(37, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    publish(&rt, &node, "s", b"kept");
    publish(&rt, &node, "j", &[b'j'; 4096]);
    seal(&rt, &node, &clock);
    publish(&rt, &node, "j", &[b'j'; 4096]);
    seal(&rt, &node, &clock);
    let sealed = seg_files(&fs);
    assert_eq!(sealed.len(), 2, "{sealed:?}");
    delete(&rt, &node, "j");
    let first = Path::new(DATA).join(&sealed[0]);
    let len = fs.open(&first).expect("open").size().expect("size");
    assert!(fs.flip_at(&first, len - 100, 0xff));
    fs.fail_next_write(io::ErrorKind::StorageFull, 0);
    compact(&rt, &node, &clock);
    let left = files(&fs);
    assert!(
        left.contains(&sealed[0]) && !left.contains(&sealed[1]),
        "{left:?}"
    );
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_panic_in_the_compaction_thread_stops_the_node() {
    let rt = runtime();
    let fs = Recorded::new(FaultFs::new(38, 1 << 40));
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    two_sealed_segments(&rt, &node, &clock);
    fs.on_open(|_| panic!("an injected panic in the compaction thread"));
    clock.advance(IDLE_SEAL_MS);
    rt.block_on(node.handle.damage()).expect("a tick");
    assert_eq!(rt.block_on(node.handle.compact()), Err(Error::Stopped));
    assert_eq!(stop(&rt, node), Err(Error::Stopped));
}

#[test]
fn recovery_keeps_no_warning_of_a_losing_file_that_it_deletes() {
    let rt = runtime();
    let fs = Recorded::new(FaultFs::new(39, 1 << 40));
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    two_sealed_segments(&rt, &node, &clock);
    *fs.fail_remove.lock().expect("flag") = true;
    clock.advance(IDLE_SEAL_MS);
    rt.block_on(node.handle.damage()).expect("a tick");
    assert_eq!(rt.block_on(node.handle.compact()), Err(Error::Stopped));
    assert_eq!(stop(&rt, node), Err(Error::Stopped));
    assert_eq!(cmp_files(&fs.fs).len(), 1, "{:?}", files(&fs.fs));
    let input = Path::new(DATA).join(&seg_files(&fs.fs)[0]);
    assert!(fs.fs.flip_at(&input, PAGE + 40, 0xff));
    let crashed = fs.fs.crash(Crash::Kill);
    let node = boot(&rt, &crashed, &clock);
    assert!(!files(&crashed).contains(&name(&input)));
    let (damage, warnings) = rt.block_on(node.handle.damage()).expect("damage");
    assert!(
        damage.is_empty() && warnings.is_empty(),
        "{damage:?} {warnings:?}"
    );
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_compaction_output_never_takes_the_name_of_a_kept_file_with_a_damaged_header() {
    let rt = runtime();
    let fs = FaultFs::new(40, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    two_sealed_segments(&rt, &node, &clock);
    stop(&rt, node).expect("clean stop");
    let garbage = vec![0xab; PAGE as usize];
    let damaged: Vec<PathBuf> = (1..=64u64)
        .map(|epoch| Path::new(DATA).join(format!("cmp-{epoch:016x}.vdl")))
        .collect();
    for path in &damaged {
        let file = fs.create(path).expect("create");
        file.write_at(&garbage, 0).expect("write");
        file.sync_data().expect("sync");
    }
    fs.sync_dir(Path::new(DATA)).expect("sync dir");
    let node = boot(&rt, &fs, &clock);
    seal(&rt, &node, &clock);
    assert_eq!(cmp_files(&fs).len(), damaged.len() + 1, "{:?}", files(&fs));
    for path in &damaged {
        let file = fs.open(path).expect("open");
        let mut data = vec![0; file.size().expect("size") as usize];
        file.read_at(&mut data, 0).expect("read");
        assert!(
            !data.is_empty() && data.iter().all(|b| *b == 0xab),
            "{path:?} was written over"
        );
    }
    stop(&rt, node).expect("clean stop");
}

#[test]
fn compaction_deletes_a_plainly_removable_input_before_it_writes_an_output() {
    let rt = runtime();
    let fs = Recorded::new(FaultFs::new(41, 1 << 40));
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    publish(&rt, &node, "j", &vec![b'j'; 1 << 20]);
    seal(&rt, &node, &clock);
    two_sealed_segments(&rt, &node, &clock);
    delete(&rt, &node, "j");
    fs.on_open(|fs| fs.set_capacity(fs.used() + COMPACT_FREE));
    seal(&rt, &node, &clock);
    assert_eq!(cmp_files(&fs.fs).len(), 1, "{:?}", files(&fs.fs));
    stop(&rt, node).expect("clean stop");
}

#[test]
fn recovery_keeps_no_warning_of_a_malformed_block_in_a_losing_file_that_it_deletes() {
    let rt = runtime();
    let fs = FaultFs::new(42, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    publish(&rt, &node, "s", b"kept");
    seal(&rt, &node, &clock);
    stop(&rt, node).expect("clean stop");
    let seen = blocks(&fs);
    let good: Vec<_> = seen.iter().collect();
    let malformed = (seen[0].0.clone(), seen[0].1, vec![0xab; 64]);
    for name in files(&fs) {
        fs.remove(&Path::new(DATA).join(name)).expect("remove");
    }
    rewrite(&fs, 0x100, 1, &[&malformed]);
    rewrite(&fs, 0x101, 2, &good);
    fs.sync_dir(Path::new(DATA)).expect("sync dir");
    let node = boot(&rt, &fs, &clock);
    assert!(
        !files(&fs).contains(&"cmp-0000000000000100.vdl".to_owned()),
        "{:?}",
        files(&fs)
    );
    let (damage, warnings) = rt.block_on(node.handle.damage()).expect("damage");
    assert!(
        damage.is_empty() && warnings.is_empty(),
        "{damage:?} {warnings:?}"
    );
    stop(&rt, node).expect("clean stop");
}

#[test]
fn under_pressure_after_enospc_the_acked_data_of_the_segment_that_the_log_sealed_goes() {
    let rt = runtime();
    let fs = FaultFs::new(43, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    let id = publish(&rt, &node, "s", b"acked");
    let (receipt, _) = take(&rt, &node, "g", "s");
    ack(&rt, &node, receipt, &[id]);
    let before = seg_files(&fs);
    assert_eq!(before.len(), 1, "{before:?}");
    fs.fail_next_write(io::ErrorKind::StorageFull, 0);
    let refused = rt.block_on(node.handle.publish(vec![message("s", b"late")], None));
    assert_eq!(refused.map(|p| p.first), Err(Error::NoSpace));
    compact(&rt, &node, &clock);
    assert!(!files(&fs).contains(&before[0]), "{:?}", files(&fs));
    stop(&rt, node).expect("clean stop");
}

#[test]
fn recovery_names_both_segments_of_a_damaged_compacted_block_whose_neighbours_disagree() {
    let rt = runtime();
    let fs = FaultFs::new(44, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    let a = publish(&rt, &node, "a", b"one");
    publish(&rt, &node, "a", b"two");
    stop(&rt, node).expect("clean stop");
    let node = boot(&rt, &fs, &clock);
    let b = publish(&rt, &node, "b", b"three");
    stop(&rt, node).expect("clean stop");
    let first = SegmentId {
        incarnation: a.incarnation,
        seq: 0,
    };
    let second = SegmentId {
        incarnation: b.incarnation,
        seq: 0,
    };
    let seen = blocks(&fs);
    let of = |segment: SegmentId| seen.iter().filter(move |(_, h, _)| h.segment == segment);
    let ordered: Vec<_> = of(first).chain(of(second)).collect();
    assert_eq!(ordered.len(), seen.len());
    let split = of(first).count();
    assert!(split >= 2, "split at {split}");
    for name in files(&fs) {
        fs.remove(&Path::new(DATA).join(name)).expect("remove");
    }
    rewrite(&fs, 0x100, 1, &ordered);
    fs.sync_dir(Path::new(DATA)).expect("sync dir");
    let path = Path::new(DATA).join("cmp-0000000000000100.vdl");
    let starts: Vec<u64> = blocks(&fs)
        .iter()
        .filter(|(p, _, _)| *p == path)
        .map(|(_, h, _)| h.offset)
        .collect();
    assert_eq!(starts.len(), ordered.len(), "{starts:?}");
    let at = starts[split - 1];
    assert!(fs.flip_at(&path, at, 0xff));
    let node = boot(&rt, &fs, &clock);
    let (damage, _) = rt.block_on(node.handle.damage()).expect("damage");
    stop(&rt, node).expect("clean stop");
    let named: BTreeSet<SegmentId> = damage
        .iter()
        .filter(|d| (d.start..d.end).contains(&at))
        .map(|d| d.segment)
        .collect();
    assert_eq!(named, BTreeSet::from([first, second]), "{damage:?}");
}

#[test]
fn recovery_names_a_damaged_last_compacted_block_under_the_other_segments_and_not_under_its_own() {
    let rt = runtime();
    let fs = FaultFs::new(45, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    let a = publish(&rt, &node, "a", b"one");
    publish(&rt, &node, "a", b"two");
    stop(&rt, node).expect("clean stop");
    let node = boot(&rt, &fs, &clock);
    let x = publish(&rt, &node, "x", b"three");
    stop(&rt, node).expect("clean stop");
    let first = SegmentId {
        incarnation: a.incarnation,
        seq: 0,
    };
    let only = SegmentId {
        incarnation: x.incarnation,
        seq: 0,
    };
    let seen = blocks(&fs);
    let of = |segment: SegmentId| seen.iter().filter(move |(_, h, _)| h.segment == segment);
    let ordered: Vec<_> = of(first).chain(of(only)).collect();
    assert_eq!(ordered.len(), seen.len());
    assert_eq!(of(only).count(), 1);
    for name in files(&fs) {
        fs.remove(&Path::new(DATA).join(name)).expect("remove");
    }
    rewrite(&fs, 0x100, 1, &ordered);
    fs.sync_dir(Path::new(DATA)).expect("sync dir");
    let path = Path::new(DATA).join("cmp-0000000000000100.vdl");
    let starts: Vec<u64> = blocks(&fs)
        .iter()
        .filter(|(p, _, _)| *p == path)
        .map(|(_, h, _)| h.offset)
        .collect();
    assert_eq!(starts.len(), ordered.len(), "{starts:?}");
    let at = starts[starts.len() - 1];
    assert!(fs.flip_at(&path, at, 0xff));
    let node = boot(&rt, &fs, &clock);
    let (damage, _) = rt.block_on(node.handle.damage()).expect("damage");
    stop(&rt, node).expect("clean stop");
    let named: BTreeSet<SegmentId> = damage
        .iter()
        .filter(|d| (d.start..d.end).contains(&at))
        .map(|d| d.segment)
        .collect();
    assert_eq!(named, BTreeSet::from([first]), "{damage:?}");
}

fn bytes_written(seen: &[&(PathBuf, BlockHeader, Vec<u8>)]) -> u64 {
    let files: BTreeSet<&PathBuf> = seen.iter().map(|(path, _, _)| path).collect();
    let blocks: usize = seen
        .iter()
        .map(|(_, _, body)| BLOCK_HEADER_LEN + body.len())
        .sum();
    (files.len() * FILE_HEADER_LEN + blocks) as u64
}

#[test]
fn the_write_rate_counts_every_byte_of_the_log_blocks_and_of_the_compaction_output() {
    let rt = runtime();
    let fs = FaultFs::new(90, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    publish(&rt, &node, "s", b"kept");
    publish(&rt, &node, "gone", &[b'x'; 4096]);
    delete(&rt, &node, "gone");
    clock.advance(IDLE_SEAL_MS);
    rt.block_on(node.handle.damage()).expect("a tick");
    let before = blocks(&fs);
    compact(&rt, &node, &clock);
    let after = blocks(&fs);
    let outputs: Vec<_> = after
        .iter()
        .filter(|(path, _, _)| name(path).starts_with("cmp-"))
        .collect();
    assert!(!outputs.is_empty(), "{after:?}");
    let written = bytes_written(&before.iter().collect::<Vec<_>>()) + bytes_written(&outputs);
    let status = rt.block_on(node.handle.status()).expect("status");
    assert_eq!(status.write_bytes_per_s, written * 1000 / 60_000);
    stop(&rt, node).expect("clean stop");
}

#[test]
fn an_open_replica_segment_is_not_compacted() {
    let rt = runtime();
    let clock = TestClock::new(1_000_000);
    let (fs_a, fs_b) = (FaultFs::new(5, 1 << 40), FaultFs::new(6, 1 << 40));
    let a = boot(&rt, &fs_a, &clock);
    let mut hook = rt.block_on(a.handle.subscribe()).expect("subscribe");
    for value in [&[b'a'; 4096][..], b"b", b"c"] {
        publish(&rt, &a, "s", value);
    }
    let b = boot(&rt, &fs_b, &clock);
    while let Ok((id, offset, bytes)) = hook.try_recv() {
        rt.block_on(b.handle.replicate(id, offset, bytes))
            .expect("replicate");
    }
    let (receipt, before) = take(&rt, &b, "g", "s");
    assert_eq!(before.len(), 3);
    ack(&rt, &b, receipt, &[before[0].0]);
    clock.advance(IDLE_SEAL_MS + RETAIN_MS);
    rt.block_on(b.handle.damage()).expect("a tick");
    compact(&rt, &b, &clock);
    assert_eq!(values(&blocks(&fs_b)).len(), 3);
    stop(&rt, b).expect("stop b");
    stop(&rt, a).expect("stop a");
}

#[test]
fn an_open_foreign_replica_is_not_compacted_after_a_restart_of_the_holder() {
    let rt = runtime();
    let clock = TestClock::new(1_000_000);
    let (fs_a, fs_b) = (FaultFs::new(5, 1 << 40), FaultFs::new(6, 1 << 40));
    let a = boot(&rt, &fs_a, &clock);
    let mut hook = rt.block_on(a.handle.subscribe()).expect("subscribe");
    for value in [&[b'a'; 4096][..], b"b", b"c"] {
        publish(&rt, &a, "s", value);
    }
    let b = boot(&rt, &fs_b, &clock);
    while let Ok((id, offset, bytes)) = hook.try_recv() {
        rt.block_on(b.handle.replicate(id, offset, bytes))
            .expect("replicate");
    }
    stop(&rt, b).expect("stop b");
    let b = boot(&rt, &fs_b, &clock);
    let open: std::collections::BTreeSet<_> = rt
        .block_on(b.handle.segments())
        .expect("segments")
        .into_iter()
        .collect();
    assert!(!open.is_empty());
    rt.block_on(b.handle.open_foreign(open)).expect("open");
    let (receipt, before) = take(&rt, &b, "g", "s");
    assert_eq!(before.len(), 3);
    ack(&rt, &b, receipt, &[before[0].0]);
    clock.advance(IDLE_SEAL_MS + RETAIN_MS);
    rt.block_on(b.handle.damage()).expect("a tick");
    compact(&rt, &b, &clock);
    assert_eq!(values(&blocks(&fs_b)).len(), 3);
    stop(&rt, b).expect("stop b");
    stop(&rt, a).expect("stop a");
}

#[test]
fn a_snapshot_gives_a_replica_the_control_records_that_it_missed() {
    let rt = runtime();
    let clock = TestClock::new(1_000_000);
    let (fs_a, fs_b) = (FaultFs::new(5, 1 << 40), FaultFs::new(6, 1 << 40));
    let a = boot(&rt, &fs_a, &clock);
    let mut hook = rt.block_on(a.handle.subscribe()).expect("subscribe");
    for value in [b"a", b"b", b"c"] {
        publish(&rt, &a, "s", value);
    }
    let b = boot(&rt, &fs_b, &clock);
    while let Ok((id, offset, bytes)) = hook.try_recv() {
        rt.block_on(b.handle.replicate(id, offset, bytes))
            .expect("replicate");
    }
    let (receipt, got) = take(&rt, &a, "g", "s");
    assert_eq!(got.len(), 3);
    ack(&rt, &a, receipt, &[got[0].0]);
    assert!(
        rt.block_on(b.handle.group_stats("g".to_owned()))
            .expect("stats")
            .is_none()
    );
    let snapshot = rt.block_on(a.handle.snapshot()).expect("snapshot");
    rt.block_on(b.handle.controls(0, snapshot))
        .expect("controls");
    let stats = rt
        .block_on(b.handle.group_stats("g".to_owned()))
        .expect("stats")
        .expect("the group is known");
    assert_eq!(stats.pending, 2);
    stop(&rt, b).expect("stop b");
    stop(&rt, a).expect("stop a");
}
