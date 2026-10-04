use std::collections::BTreeSet;
use std::future::Future;
use std::io;
use std::ops::Bound;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Waker};
use std::thread::{self, ThreadId};
use std::time::Duration;

use tempfile::TempDir;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use verdandi_core::ids::{Address, MessageId, SegmentId};
use verdandi_core::lease::{Bury, Nack, Receipt, Settle, SettleError};
use verdandi_core::retention::RETAIN_MS;
use verdandi_core::{Generation, PROVISIONAL_MS, Reason, Record, Timing, WaiterId, nth_id};
use verdandi_sim::fault_fs::{Crash, FaultFile, FaultFs};
use verdandi_store::block::{BLOCK_HEADER_LEN, FILE_HEADER_LEN, MAX_BLOCK, PAGE, SEGMENT_BYTES};
use verdandi_store::engine::{
    BULK, DEAD_PER_PASS, Delivery, Engine, Error, Handle, IDLE_SEAL_MS, Key, NewMessage, OsRandom,
    PRESSURE_HOLD_MS, Random, SettleBy, Take, Taken, TestClock,
};
use verdandi_store::fs::{Fs, FsFile, FsStat, RealFs};
use verdandi_store::log::{Log, segment_file_name};
use verdandi_store::records::{self, Control, Message, Origin};
use verdandi_store::recovery::recover;

const DATA: &str = "/data";
const LEASE: u64 = 30_000;

struct Node {
    handle: Handle,
    task: JoinHandle<Result<(), Error>>,
}

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

fn boot<F: Fs + Clone>(rt: &Runtime, fs: &F, dir: &Path, clock: &TestClock) -> Node {
    let (engine, handle) = rt
        .block_on(Engine::open(
            fs.clone(),
            dir,
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

fn take(group: &str, selector: &str, limit: usize) -> Take {
    Take {
        group: group.to_owned(),
        selector: selector.to_owned(),
        limit,
        lease_ms: LEASE,
        wait: false,
    }
}

fn leased(taken: Result<Taken, Error>) -> (Receipt, Vec<Delivery>) {
    match taken {
        Ok(Taken::Leased {
            receipt, messages, ..
        }) => (receipt, messages),
        Ok(Taken::Empty) => (Receipt(0), Vec::new()),
        Ok(Taken::Waiting { .. }) => panic!("a take with no wait registered a waiter"),
        Err(e) => panic!("take failed: {e}"),
    }
}

fn ids(deliveries: &[Delivery]) -> Vec<MessageId> {
    deliveries.iter().map(|d| d.handed.id).collect()
}

fn ack(ids: &[MessageId]) -> SettleBy {
    SettleBy::Ids(Settle {
        ack: ids.to_vec(),
        ..Settle::default()
    })
}

fn bury(ids: &[MessageId]) -> SettleBy {
    SettleBy::Ids(Settle {
        dead: ids
            .iter()
            .map(|&id| Bury {
                id,
                error: Some("bad".to_owned()),
            })
            .collect(),
        ..Settle::default()
    })
}

fn publish(rt: &Runtime, node: &Node, stream: &str, values: &[&[u8]]) -> Vec<MessageId> {
    let published = rt
        .block_on(
            node.handle
                .publish(values.iter().map(|v| message(stream, v)).collect(), None),
        )
        .expect("publish");
    (0..published.count)
        .map(|n| verdandi_core::nth_id(published.first, n).expect("id"))
        .collect()
}

fn blocks(fs: &FaultFs) -> Vec<(PathBuf, u64, Vec<u8>)> {
    let mut seen = Vec::new();
    recover(fs, Path::new(DATA), |path, header, body| {
        seen.push((path.to_path_buf(), header.offset, body.to_vec()))
    })
    .expect("recover");
    seen
}

fn stored_messages(body: &[u8]) -> Vec<(String, Message)> {
    let body = records::decode(body).expect("body");
    body.runs
        .iter()
        .flat_map(|(entry, bytes)| {
            records::decode_run(bytes, entry.count)
                .expect("run")
                .into_iter()
                .map(|m| (entry.stream.clone(), m))
        })
        .collect()
}

struct Held<'a>(&'a FaultFs);

impl<'a> Held<'a> {
    fn new(fs: &'a FaultFs) -> Self {
        fs.hold_syncs();
        Self(fs)
    }
}

impl Drop for Held<'_> {
    fn drop(&mut self) {
        self.0.release_syncs();
    }
}

async fn until_sync_held(fs: &FaultFs) {
    while fs.syncs_held() == 0 {
        tokio::task::yield_now().await;
    }
}

#[test]
fn a_message_is_seen_only_after_its_block_syncs() {
    let rt = runtime();
    let fs = FaultFs::new(1, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    rt.block_on(async {
        let _held = Held::new(&fs);
        let handle = node.handle.clone();
        let publish =
            tokio::spawn(async move { handle.publish(vec![message("s", b"x")], None).await });
        until_sync_held(&fs).await;
        let (_, early) = leased(node.handle.take(take("g", "s", 10)).await);
        assert!(early.is_empty(), "a take saw a message before its fsync");
        fs.release_syncs();
        publish.await.expect("join").expect("publish");
        let (_, late) = leased(node.handle.take(take("h", "s", 10)).await);
        assert_eq!(late.len(), 1);
    });
    stop(&rt, node).expect("clean stop");
}

#[test]
fn work_that_comes_during_an_fsync_goes_into_one_next_block() {
    let rt = runtime();
    let fs = FaultFs::new(2, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    rt.block_on(async {
        let _held = Held::new(&fs);
        let mut replies = Vec::new();
        for value in [b"a", b"b", b"c"] {
            let handle = node.handle.clone();
            replies.push(tokio::spawn(async move {
                handle.publish(vec![message("s", value)], None).await
            }));
            until_sync_held(&fs).await;
            for _ in 0..50 {
                tokio::task::yield_now().await;
            }
        }
        fs.release_syncs();
        for reply in replies {
            reply.await.expect("join").expect("publish");
        }
    });
    stop(&rt, node).expect("clean stop");
    let bodies: Vec<usize> = blocks(&fs)
        .iter()
        .map(|(_, _, body)| stored_messages(body).len())
        .collect();
    assert_eq!(bodies, [1, 2]);
}

#[test]
fn an_acked_message_is_never_handed_out_again_after_a_power_loss() {
    let rt = runtime();
    let fs = FaultFs::new(3, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let published = publish(&rt, &node, "s", &[b"a", b"b"]);
    let (receipt, handed) = leased(rt.block_on(node.handle.take(take("g", "s", 10))));
    assert_eq!(ids(&handed), published);
    rt.block_on(node.handle.settle(receipt, ack(&published[..1])))
        .expect("ack");
    let fs = fs.crash(Crash::LoseAll);
    stop(&rt, node).ok();
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let (_, again) = leased(rt.block_on(node.handle.take(take("g", "s", 10))));
    assert_eq!(ids(&again), published[1..]);
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_failed_fsync_stops_the_engine_and_nothing_of_the_block_is_seen() {
    let rt = runtime();
    let fs = FaultFs::new(4, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    fs.fail_next_sync();
    let refused = rt.block_on(node.handle.publish(vec![message("s", b"x")], None));
    assert_eq!(refused, Err(Error::Stopped));
    assert!(matches!(
        rt.block_on(node.handle.take(take("g", "s", 1))),
        Err(Error::Stopped)
    ));
    assert_eq!(rt.block_on(node.task).expect("join"), Err(Error::Stopped));
}

#[test]
fn enospc_abandons_the_block_and_the_engine_goes_on() {
    let rt = runtime();
    let fs = FaultFs::new(5, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    fs.fail_next_write(io::ErrorKind::StorageFull, 10);
    let refused = rt.block_on(node.handle.publish(vec![message("s", b"x")], None));
    assert_eq!(refused, Err(Error::NoSpace));
    let ok = publish(&rt, &node, "s", &[b"y"]);
    let (_, handed) = leased(rt.block_on(node.handle.take(take("g", "s", 10))));
    assert_eq!(ids(&handed), ok);
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_repeat_settle_of_an_unsynced_ack_replies_only_when_the_block_applies() {
    let rt = runtime();
    let fs = FaultFs::new(6, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let published = publish(&rt, &node, "s", &[b"a"]);
    let (receipt, _) = leased(rt.block_on(node.handle.take(take("g", "s", 10))));
    publish(&rt, &node, "barrier", &[b"b"]);
    rt.block_on(async {
        let _held = Held::new(&fs);
        let handle = node.handle.clone();
        let blocker =
            tokio::spawn(async move { handle.publish(vec![message("t", b"z")], None).await });
        until_sync_held(&fs).await;
        let (h1, h2) = (node.handle.clone(), node.handle.clone());
        let first = published.clone();
        let second = published.clone();
        let first = tokio::spawn(async move { h1.settle(receipt, ack(&first)).await });
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        let second = tokio::spawn(async move { h2.settle(receipt, ack(&second)).await });
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        assert!(
            !second.is_finished(),
            "the repeat settle replied before its block"
        );
        fs.release_syncs();
        blocker.await.expect("join").expect("publish");
        assert_eq!(first.await.expect("join"), Ok(()));
        assert_eq!(second.await.expect("join"), Ok(()));
    });
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_repeat_settle_of_an_abandoned_ack_gets_the_same_failure() {
    let rt = runtime();
    let fs = FaultFs::new(7, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let published = publish(&rt, &node, "s", &[b"a"]);
    let (receipt, _) = leased(rt.block_on(node.handle.take(take("g", "s", 10))));
    publish(&rt, &node, "barrier", &[b"b"]);
    rt.block_on(async {
        let _held = Held::new(&fs);
        let handle = node.handle.clone();
        let blocker =
            tokio::spawn(async move { handle.publish(vec![message("t", b"z")], None).await });
        until_sync_held(&fs).await;
        let (h1, h2) = (node.handle.clone(), node.handle.clone());
        let (first, second) = (published.clone(), published.clone());
        let first = tokio::spawn(async move { h1.settle(receipt, ack(&first)).await });
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        let second = tokio::spawn(async move { h2.settle(receipt, ack(&second)).await });
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        fs.fail_next_write(io::ErrorKind::StorageFull, 0);
        fs.release_syncs();
        blocker.await.expect("join").expect("publish");
        assert_eq!(first.await.expect("join"), Err(Error::NoSpace));
        assert_eq!(second.await.expect("join"), Err(Error::NoSpace));
    });
    let retry = rt.block_on(async {
        let settle = node.handle.settle(receipt, ack(&published));
        tokio::time::timeout(Duration::from_secs(5), settle).await
    });
    assert_eq!(retry.expect("the retry replies"), Ok(()));
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_wake_for_a_request_that_has_gone_goes_to_the_next_waiter() {
    let rt = runtime();
    let fs = FaultFs::new(8, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let wait = Take {
        wait: true,
        ..take("g", "s", 1)
    };
    let waiter = |taken: Result<Taken, Error>| match taken {
        Ok(Taken::Waiting { waiter, woken }) => (waiter, woken),
        _ => panic!("expected a waiter"),
    };
    let (_, gone) = waiter(rt.block_on(node.handle.take(wait.clone())));
    drop(gone);
    let (_, woken) = waiter(rt.block_on(node.handle.take(wait)));
    publish(&rt, &node, "s", &[b"a"]);
    let woken = rt.block_on(async { tokio::time::timeout(Duration::from_secs(5), woken).await });
    assert_eq!(woken.expect("the next waiter is woken"), Ok(()));
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_take_whose_reply_cannot_be_sent_releases_its_messages_at_once() {
    let rt = runtime();
    let fs = FaultFs::new(10, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let published = publish(&rt, &node, "s", &[b"a", b"b"]);
    let mut gone = Box::pin(node.handle.take(take("g", "s", 2)));
    let polled = gone.as_mut().poll(&mut Context::from_waker(Waker::noop()));
    assert!(polled.is_pending(), "the take must stop before its reply");
    drop(gone);
    let (_, again) = leased(rt.block_on(node.handle.take(take("g", "s", 2))));
    assert_eq!(ids(&again), published);
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_cancelled_waiter_is_not_woken() {
    let rt = runtime();
    let fs = FaultFs::new(9, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let wait = Take {
        wait: true,
        ..take("g", "s", 1)
    };
    let Ok(Taken::Waiting { waiter, woken }) = rt.block_on(node.handle.take(wait)) else {
        panic!("expected a waiter");
    };
    rt.block_on(node.handle.cancel(waiter)).expect("cancel");
    publish(&rt, &node, "s", &[b"a"]);
    assert!(rt.block_on(woken).is_err());
    let _: WaiterId = waiter;
    stop(&rt, node).expect("clean stop");
}

fn read_one(rt: &Runtime, node: &Node, delivery: &Delivery) -> Option<Message> {
    rt.block_on(node.handle.read(std::slice::from_ref(delivery)))
        .expect("read")
        .remove(0)
}

#[test]
fn a_dead_copy_gets_a_new_id_and_a_dead_copy_of_it_keeps_the_first_origin() {
    let rt = runtime();
    let fs = FaultFs::new(10, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let published = publish(&rt, &node, "s", &[b"payload"]);
    let (receipt, _) = leased(rt.block_on(node.handle.take(take("g", "s", 1))));
    rt.block_on(node.handle.settle(receipt, bury(&published)))
        .expect("dead settle");
    let (receipt, dead) = leased(rt.block_on(node.handle.take(take("g", "s.dead.g", 1))));
    let copy = read_one(&rt, &node, &dead[0]).expect("dead copy");
    assert_ne!(copy.id, published[0]);
    assert_eq!(copy.value, b"payload");
    let origin = copy.origin.expect("origin fields");
    assert_eq!(
        (origin.id, origin.stream.as_str(), origin.reason),
        (published[0], "s", Reason::Dead)
    );
    assert_eq!(origin.error.as_deref(), Some("bad"));
    rt.block_on(node.handle.settle(receipt, bury(&[copy.id])))
        .expect("dead settle of the copy");
    let (_, again) = leased(rt.block_on(node.handle.take(take("h", "s.dead.g", 10))));
    let copies: Vec<Message> = again
        .iter()
        .filter_map(|d| read_one(&rt, &node, d))
        .collect();
    assert_eq!(copies.len(), 2);
    assert!(
        copies
            .iter()
            .all(|c| c.origin.as_ref().map(|o| (o.id, o.stream.as_str()))
                == Some((published[0], "s")))
    );
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_dead_settle_over_32_mib_writes_each_copy_in_one_block_with_its_ack() {
    let rt = runtime();
    let fs = FaultFs::new(11, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let big = vec![7u8; 1 << 20];
    let mut published = Vec::new();
    for _ in 0..40 {
        published.extend(publish(&rt, &node, "s", &[&big]));
    }
    let (receipt, handed) = leased(rt.block_on(node.handle.take(take("g", "s", 40))));
    assert_eq!(handed.len(), 40);
    rt.block_on(node.handle.settle(receipt, bury(&published)))
        .expect("dead settle");
    stop(&rt, node).expect("clean stop");
    let mut with_copies = 0;
    let mut copies = 0;
    for (_, _, body) in blocks(&fs) {
        let decoded = records::decode(&body).expect("body");
        let acked: BTreeSet<Address> = decoded
            .controls
            .iter()
            .filter_map(|c| match c {
                Control::Record(verdandi_core::Record::Ack { address, .. }) => Some(*address),
                _ => None,
            })
            .collect();
        let dead: Vec<(String, Message)> = stored_messages(&body)
            .into_iter()
            .filter(|(s, _)| s == "s.dead.g")
            .collect();
        if dead.is_empty() {
            continue;
        }
        with_copies += 1;
        copies += dead.len();
        assert_eq!(acked.len(), dead.len());
        for (_, copy) in dead {
            let origin = copy.origin.expect("origin");
            let address = handed
                .iter()
                .find(|d| d.handed.id == origin.id)
                .expect("a handed id")
                .handed
                .address;
            assert!(acked.contains(&address), "a dead copy without its ack");
        }
    }
    assert_eq!(copies, 40);
    assert!(with_copies > 1, "one block held every dead copy");
}

#[test]
fn an_idempotency_key_repeat_gets_the_first_reply_also_in_flight_and_after_a_restart() {
    let rt = runtime();
    let fs = FaultFs::new(12, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let key = |scope: &str| {
        Some(Key {
            scope: scope.to_owned(),
            key: "k1".to_owned(),
        })
    };
    let (first, second) = rt.block_on(async {
        let _held = Held::new(&fs);
        let (h1, h2) = (node.handle.clone(), node.handle.clone());
        let k = key("s");
        let first = tokio::spawn(async move {
            h1.publish(vec![message("s", b"a"), message("s", b"b")], k)
                .await
        });
        until_sync_held(&fs).await;
        let k = key("s");
        let second = tokio::spawn(async move { h2.publish(vec![message("s", b"other")], k).await });
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        fs.release_syncs();
        (
            first.await.expect("join").expect("publish"),
            second.await.expect("join").expect("publish"),
        )
    });
    assert!(!first.repeat && second.repeat);
    assert_eq!((first.first, first.count), (second.first, second.count));
    let other = rt
        .block_on(node.handle.publish(vec![message("t", b"c")], key("t")))
        .expect("publish");
    assert!(!other.repeat);
    assert_ne!(other.first, first.first);
    let fs = fs.crash(Crash::LoseAll);
    stop(&rt, node).ok();
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let again = rt
        .block_on(node.handle.publish(vec![message("s", b"z")], key("s")))
        .expect("publish");
    assert!(again.repeat);
    assert_eq!((again.first, again.count), (first.first, 2));
    clock.advance(verdandi_store::engine::KEY_MS);
    let expired = rt
        .block_on(node.handle.publish(vec![message("s", b"z")], key("s")))
        .expect("publish");
    assert!(!expired.repeat);
    stop(&rt, node).expect("clean stop");
}

#[test]
fn the_clock_floor_puts_every_new_id_above_every_id_in_the_log() {
    let rt = runtime();
    let fs = FaultFs::new(13, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let old = publish(&rt, &node, "s", &[b"a"]);
    let tombstone = rt
        .block_on(node.handle.delete_stream("s".to_owned()))
        .expect("delete");
    assert!(tombstone > old[0]);
    stop(&rt, node).expect("clean stop");
    clock.set(5);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let new = publish(&rt, &node, "s", &[b"b"]);
    assert!(new[0] > tombstone, "{} is not above {}", new[0], tombstone);
    let (_, handed) = leased(rt.block_on(node.handle.take(take("g", "s", 10))));
    assert_eq!(ids(&handed), new);
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_tombstone_covers_the_dead_copies_made_before_the_delete() {
    let rt = runtime();
    let fs = FaultFs::new(14, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let published = publish(&rt, &node, "s", &[b"a"]);
    let (receipt, _) = leased(rt.block_on(node.handle.take(take("g", "s", 1))));
    rt.block_on(node.handle.settle(receipt, bury(&published)))
        .expect("dead settle");
    rt.block_on(node.handle.delete_stream("s".to_owned()))
        .expect("delete");
    let (_, dead) = leased(rt.block_on(node.handle.take(take("x", "s.dead.g", 10))));
    assert!(
        dead.is_empty(),
        "a dead copy outlived the delete of its stream"
    );
    let new = publish(&rt, &node, "s", &[b"b"]);
    let (_, fresh) = leased(rt.block_on(node.handle.take(take("y", "s", 10))));
    assert_eq!(ids(&fresh), new);
    stop(&rt, node).expect("clean stop");
}

#[test]
fn the_engine_seals_the_active_segment_after_30_s_with_no_write() {
    let rt = runtime();
    let fs = FaultFs::new(15, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let first = publish(&rt, &node, "s", &[b"a"]);
    let path = Path::new(DATA).join(segment_file_name(
        Address {
            segment: verdandi_core::ids::SegmentId {
                incarnation: first[0].incarnation,
                seq: 0,
            },
            position: 0,
        }
        .segment,
    ));
    let size = || fs.open(&path).expect("open").size().expect("size");
    clock.advance(IDLE_SEAL_MS - 1);
    rt.block_on(node.handle.damage()).expect("tick");
    assert!(size() > 1 << 20, "sealed before 30 s");
    clock.advance(1);
    rt.block_on(async {
        node.handle.damage().await.expect("tick");
        for _ in 0..10_000 {
            if size() < 1 << 20 {
                break;
            }
            tokio::task::yield_now().await;
        }
    });
    assert!(size() < 1 << 20, "not sealed after 30 s with no write");
    publish(&rt, &node, "s", &[b"b"]);
    stop(&rt, node).expect("clean stop");
    let files: BTreeSet<PathBuf> = blocks(&fs).into_iter().map(|(p, _, _)| p).collect();
    assert_eq!(files.len(), 2);
}

#[test]
fn a_flip_at_rest_is_found_by_the_run_crc_never_served_and_recorded() {
    let rt = runtime();
    std::fs::create_dir_all(env!("CARGO_TARGET_TMPDIR")).expect("target tmp dir");
    let tmp = TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).expect("temp dir");
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &RealFs, tmp.path(), &clock);
    let published = publish(&rt, &node, "s", &[b"one", b"two"]);
    let kept = publish(&rt, &node, "t", &[b"three"]);
    let (_, handed) = leased(rt.block_on(node.handle.take(take("g", "s", 10))));
    let location = handed[0].location.clone().expect("location");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(tmp.path().join(segment_file_name(location.start.segment)))
        .expect("open");
    let at = location.run.offset + u64::from(location.run.len) - 1;
    let mut byte = [0];
    file.read_exact_at(&mut byte, at).expect("read");
    file.write_all_at(&[byte[0] ^ 0x40], at).expect("flip");
    let served = rt.block_on(node.handle.read(&handed)).expect("read");
    assert_eq!(served, [None, None]);
    let (_, again) = leased(rt.block_on(node.handle.take(take("h", "s", 10))));
    assert!(again.is_empty(), "a damaged run was handed out again");
    let (_, other) = leased(rt.block_on(node.handle.take(take("h", "t", 10))));
    assert_eq!(ids(&other), kept);
    let named = |damage: &[verdandi_store::records::Damage]| {
        damage
            .iter()
            .any(|d| d.segment == location.start.segment && (d.start..d.end).contains(&at))
    };
    publish(&rt, &node, "barrier", &[b"b"]);
    assert!(named(&rt.block_on(node.handle.damage()).expect("damage").0));
    stop(&rt, node).expect("clean stop");
    let node = boot(&rt, &RealFs, tmp.path(), &clock);
    assert!(named(&rt.block_on(node.handle.damage()).expect("damage").0));
    let (_, after) = leased(rt.block_on(node.handle.take(take("z", "s", 10))));
    assert!(ids(&after).iter().all(|id| !published.contains(id)));
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_bad_tail_is_recorded_as_damage_before_the_seal_cuts_it() {
    let rt = runtime();
    std::fs::create_dir_all(env!("CARGO_TARGET_TMPDIR")).expect("target tmp dir");
    let tmp = TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).expect("temp dir");
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &RealFs, tmp.path(), &clock);
    let published = publish(&rt, &node, "s", &[b"a"]);
    stop(&rt, node).expect("clean stop");
    let name = std::fs::read_dir(tmp.path())
        .expect("list")
        .map(|e| e.expect("entry").path())
        .find(|p| p.extension().is_some_and(|e| e == "vdl"))
        .expect("a segment");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&name)
        .expect("open");
    file.write_all_at(&[0xab; 100], 3 * 4096).expect("garbage");
    drop(file);
    let node = boot(&rt, &RealFs, tmp.path(), &clock);
    let (first, _) = rt.block_on(node.handle.damage()).expect("damage");
    assert!(
        first.iter().any(|d| (d.start..d.end).contains(&(3 * 4096))),
        "{first:?}"
    );
    stop(&rt, node).expect("clean stop");
    assert!(std::fs::metadata(&name).expect("meta").len() < 3 * 4096);
    let node = boot(&rt, &RealFs, tmp.path(), &clock);
    let (second, _) = rt.block_on(node.handle.damage()).expect("damage");
    assert_eq!(first, second);
    let (_, handed) = leased(rt.block_on(node.handle.take(take("g", "s", 10))));
    assert_eq!(ids(&handed), published);
    stop(&rt, node).expect("clean stop");
}

#[test]
fn every_group_control_record_survives_a_restart() {
    let rt = runtime();
    let fs = FaultFs::new(16, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let published = publish(&rt, &node, "s", &[b"a", b"b", b"c"]);
    let (receipt, _) = leased(rt.block_on(node.handle.take(take("g", "s*", 3))));
    let settle = Settle {
        ack: vec![published[0]],
        nack: vec![Nack {
            id: published[1],
            delay_ms: Some(60_000),
            error: None,
        }],
        ..Settle::default()
    };
    rt.block_on(node.handle.settle(receipt, SettleBy::Ids(settle)))
        .expect("settle");
    stop(&rt, node).expect("clean stop");
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let (_, handed) = leased(rt.block_on(node.handle.take(take("g", "s*", 10))));
    assert_eq!(ids(&handed), [published[2]], "ack and nack backoff survive");
    rt.block_on(
        node.handle
            .replay("g".to_owned(), Bound::Excluded(published[0])),
    )
    .expect("replay");
    stop(&rt, node).expect("clean stop");
    clock.advance(60_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let (_, replayed) = leased(rt.block_on(node.handle.take(take("g", "s*", 10))));
    assert_eq!(ids(&replayed), published[1..], "the generation survives");
    rt.block_on(node.handle.forget("g".to_owned()))
        .expect("forget");
    stop(&rt, node).expect("clean stop");
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let (_, fresh) = leased(rt.block_on(node.handle.take(take("g", "s*", 10))));
    assert_eq!(
        ids(&fresh),
        published,
        "a forget starts again at the oldest"
    );
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_settle_with_an_unknown_receipt_is_refused() {
    let rt = runtime();
    let fs = FaultFs::new(17, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let refused = rt.block_on(node.handle.settle(Receipt(1), ack(&[])));
    assert_eq!(refused, Err(Error::Refused(SettleError::UnknownReceipt)));
    stop(&rt, node).expect("clean stop");
}

fn stored_len(stream: &str, values: &[usize]) -> usize {
    let run: Vec<Message> = values
        .iter()
        .enumerate()
        .map(|(n, len)| Message {
            position: n as u32,
            id: MessageId::new(1, 1, n as u16).expect("id"),
            timing: Timing::default(),
            origin: None,
            headers: Vec::new(),
            value: vec![0; *len],
        })
        .collect();
    records::encode(&[(stream, &run)], &[]).0.len()
}

#[test]
fn a_field_over_its_stored_length_is_refused_and_every_block_decodes() {
    let rt = runtime();
    let fs = FaultFs::new(18, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let long = "x".repeat(usize::from(u16::MAX) + 1);
    let with_headers = |headers: Vec<(String, String)>| NewMessage {
        headers,
        ..message("s", b"a")
    };
    let too_long = [
        (
            vec![with_headers(vec![("h".to_owned(), long.clone())])],
            None,
        ),
        (
            vec![with_headers(vec![(long.clone(), "v".to_owned())])],
            None,
        ),
        (
            vec![with_headers(vec![
                (String::new(), String::new());
                usize::from(u16::MAX) + 1
            ])],
            None,
        ),
        (
            vec![message("s", b"a")],
            Some(Key {
                scope: "s".to_owned(),
                key: long.clone(),
            }),
        ),
    ];
    for (messages, key) in too_long {
        let refused = rt.block_on(node.handle.publish(messages, key));
        assert_eq!(refused, Err(Error::TooLong));
    }
    let name = "g".repeat(407);
    let scoped = Some(Key {
        scope: name.clone(),
        key: "k".to_owned(),
    });
    let refused = rt.block_on(node.handle.publish(vec![message("s", b"a")], scoped));
    assert_eq!(refused, Err(Error::BadName));
    for (group, selector) in [(name.as_str(), "s"), ("g", name.as_str())] {
        let taken = rt.block_on(node.handle.take(take(group, selector, 1)));
        assert!(matches!(taken, Err(Error::BadName)), "{group} {selector}");
    }
    let replay = rt.block_on(node.handle.replay(name.clone(), Bound::Unbounded));
    assert_eq!(replay, Err(Error::BadName));
    assert_eq!(
        rt.block_on(node.handle.redrive(name.clone())),
        Err(Error::BadName)
    );
    assert_eq!(
        rt.block_on(node.handle.forget(name.clone())),
        Err(Error::BadName)
    );
    assert_eq!(
        rt.block_on(node.handle.delete_stream(name)),
        Err(Error::BadName)
    );
    let kept = publish(&rt, &node, "s", &[b"kept"]);
    stop(&rt, node).expect("clean stop");
    let stored: Vec<MessageId> = blocks(&fs)
        .iter()
        .flat_map(|(_, _, body)| stored_messages(body))
        .map(|(_, m)| m.id)
        .collect();
    assert_eq!(stored, kept);
}

#[test]
fn a_dead_settle_whose_original_cannot_be_read_gets_an_error() {
    let rt = runtime();
    let fs = FaultFs::new(19, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let published = publish(&rt, &node, "s", &[b"a"]);
    let (receipt, _) = leased(rt.block_on(node.handle.take(take("g", "s", 1))));
    publish(&rt, &node, "barrier", &[b"b"]);
    let _after = fs.crash(Crash::Kill);
    let settled = rt.block_on(node.handle.settle(receipt, bury(&published)));
    assert!(matches!(settled, Err(Error::Unreadable(_))), "{settled:?}");
    stop(&rt, node).ok();
}

#[test]
fn an_expiry_whose_original_cannot_be_read_stops_the_node() {
    let rt = runtime();
    let fs = FaultFs::new(20, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let (_, none) = leased(rt.block_on(node.handle.take(take("g", "s", 1))));
    assert!(none.is_empty());
    let expiring = NewMessage {
        timing: Timing {
            delay_until: None,
            expire_at: Some(2_000),
        },
        ..message("s", b"a")
    };
    rt.block_on(node.handle.publish(vec![expiring], None))
        .expect("publish");
    clock.set(3_000);
    let _after = fs.crash(Crash::Kill);
    rt.block_on(node.handle.take(take("g", "s", 1))).ok();
    let ended =
        rt.block_on(async { tokio::time::timeout(Duration::from_secs(5), node.task).await });
    assert_eq!(
        ended.expect("the node stops").expect("join"),
        Err(Error::Stopped)
    );
}

#[test]
fn a_publish_is_refused_only_when_its_stored_records_are_over_the_block_cap() {
    let rt = runtime();
    let fs = FaultFs::new(21, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let stream = "s".repeat(200);
    let count = 2_000;
    let per = stored_len(&stream, &[0, 0]) - stored_len(&stream, &[0]);
    let room = MAX_BLOCK - BLOCK_HEADER_LEN - (stored_len(&stream, &[0]) - per);
    let value = room / count - per;
    let last = room - (count - 1) * (per + value) - per;
    let batch = |last: usize| -> Vec<NewMessage> {
        (0..count)
            .map(|n| NewMessage {
                value: vec![b'v'; if n + 1 == count { last } else { value }],
                ..message(&stream, b"")
            })
            .collect()
    };
    let over = rt.block_on(node.handle.publish(batch(last + 1), None));
    assert_eq!(over, Err(Error::TooLarge));
    let full = rt
        .block_on(node.handle.publish(batch(last), None))
        .expect("a batch that fills one block is accepted");
    assert_eq!(full.count, count as u32);
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_dead_copy_over_the_block_cap_cuts_its_error_and_the_node_goes_on() {
    let rt = runtime();
    let fs = FaultFs::new(22, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let half = publish(&rt, &node, "s", &[&vec![1u8; 16 << 20]]);
    let (receipt, _) = leased(rt.block_on(node.handle.take(take("g", "s", 1))));
    let long = SettleBy::Ids(Settle {
        dead: vec![Bury {
            id: half[0],
            error: Some("e".repeat(17 << 20)),
        }],
        ..Settle::default()
    });
    rt.block_on(node.handle.settle(receipt, long))
        .expect("the error is cut to fit");
    let (_, dead) = leased(rt.block_on(node.handle.take(take("x", "s.dead.g", 10))));
    let copy = read_one(&rt, &node, &dead[0]).expect("dead copy");
    assert_eq!(copy.value.len(), 16 << 20);
    let error = copy.origin.and_then(|o| o.error).expect("an error");
    assert!(!error.is_empty() && error.len() < 17 << 20);
    publish(&rt, &node, "s", &[b"after"]);
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_read_damage_record_whose_block_is_abandoned_is_written_again() {
    let rt = runtime();
    let fs = FaultFs::new(23, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    publish(&rt, &node, "s", &[b"a"]);
    let (_, mut handed) = leased(rt.block_on(node.handle.take(take("g", "s", 1))));
    publish(&rt, &node, "barrier", &[b"b"]);
    if let Some(location) = handed[0].location.as_mut() {
        location.run.crc ^= 1;
    }
    fs.fail_next_write(io::ErrorKind::StorageFull, 0);
    let served = rt.block_on(node.handle.read(&handed)).expect("read");
    assert_eq!(served, [None]);
    publish(&rt, &node, "barrier", &[b"c"]);
    publish(&rt, &node, "barrier", &[b"d"]);
    let (damage, _) = rt.block_on(node.handle.damage()).expect("damage");
    assert_eq!(damage.len(), 1, "{damage:?}");
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_damaged_range_in_a_compacted_file_is_recorded_with_its_segment() {
    let rt = runtime();
    std::fs::create_dir_all(env!("CARGO_TARGET_TMPDIR")).expect("target tmp dir");
    let tmp = TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).expect("temp dir");
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &RealFs, tmp.path(), &clock);
    let first = publish(&rt, &node, "s", &[b"a"]);
    publish(&rt, &node, "s", &[b"b"]);
    publish(&rt, &node, "s", &[b"c"]);
    stop(&rt, node).expect("clean stop");
    let segment = verdandi_core::ids::SegmentId {
        incarnation: first[0].incarnation,
        seq: 0,
    };
    let compacted = tmp.path().join(format!("cmp-{:016x}.vdl", 99));
    std::fs::rename(tmp.path().join(segment_file_name(segment)), &compacted).expect("rename");
    let at = 2 * PAGE + 60;
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&compacted)
        .expect("open");
    file.write_all_at(&[0xab; 8], at).expect("garbage");
    drop(file);
    let node = boot(&rt, &RealFs, tmp.path(), &clock);
    let (damage, _) = rt.block_on(node.handle.damage()).expect("damage");
    assert!(
        damage
            .iter()
            .any(|d| d.segment == segment && (d.start..d.end).contains(&at)),
        "{damage:?}"
    );
    stop(&rt, node).expect("clean stop");
}

#[derive(Clone)]
struct Counted {
    fs: FaultFs,
    reads: Arc<AtomicUsize>,
    reader: Arc<Mutex<Option<ThreadId>>>,
}

struct CountedFile {
    file: FaultFile,
    reads: Arc<AtomicUsize>,
    reader: Arc<Mutex<Option<ThreadId>>>,
}

impl FsFile for CountedFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        *self.reader.lock().expect("reader") = Some(thread::current().id());
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
        self.file.sync_data()
    }

    fn size(&self) -> io::Result<u64> {
        self.file.size()
    }
}

impl Fs for Counted {
    type File = CountedFile;

    fn create(&self, path: &Path) -> io::Result<CountedFile> {
        Ok(CountedFile {
            file: self.fs.create(path)?,
            reads: Arc::clone(&self.reads),
            reader: Arc::clone(&self.reader),
        })
    }

    fn open(&self, path: &Path) -> io::Result<CountedFile> {
        Ok(CountedFile {
            file: self.fs.open(path)?,
            reads: Arc::clone(&self.reads),
            reader: Arc::clone(&self.reader),
        })
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        self.fs.sync_dir(dir)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.fs.rename(from, to)
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        self.fs.remove(path)
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
        self.fs.list(dir)
    }

    fn statfs(&self, path: &Path) -> io::Result<FsStat> {
        self.fs.statfs(path)
    }
}

fn expiring(stream: &str, value: &[u8], expire_at: u64) -> NewMessage {
    NewMessage {
        timing: Timing {
            delay_until: None,
            expire_at: Some(expire_at),
        },
        ..message(stream, value)
    }
}

#[test]
fn a_publish_is_refused_when_its_dead_copy_cannot_fit_one_block_also_after_a_redrive() {
    let rt = runtime();
    let fs = FaultFs::new(24, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let group = "g".repeat(200);
    let dead_stream = format!("s.dead.{group}");
    let id = MessageId::new(1, 1, 0).expect("id");
    let shell = Message {
        position: 0,
        id,
        timing: Timing::default(),
        origin: Some(Origin {
            id,
            stream: "s".to_owned(),
            reason: Reason::Expired,
            error: None,
        }),
        headers: Vec::new(),
        value: Vec::new(),
    };
    let ack = Control::Record(Record::Ack {
        group: group.clone(),
        generation: Generation {
            counter: 0,
            incarnation: 0,
        },
        address: Address {
            segment: SegmentId {
                incarnation: 0,
                seq: 0,
            },
            position: 0,
        },
        stream: dead_stream.clone(),
    });
    let body = records::encode(&[(&dead_stream, std::slice::from_ref(&shell))], &[ack]);
    let fits = MAX_BLOCK - BLOCK_HEADER_LEN - body.0.len();
    let over = rt.block_on(
        node.handle
            .publish(vec![message("s", &vec![1u8; fits + 1])], None),
    );
    assert_eq!(over, Err(Error::TooLarge));
    let kept = publish(&rt, &node, "s", &[&vec![2u8; fits]]);
    let (receipt, _) = leased(rt.block_on(node.handle.take(take(&group, "s", 1))));
    rt.block_on(node.handle.settle(receipt, bury(&kept)))
        .expect("the dead copy fits one block");
    rt.block_on(node.handle.redrive(group.clone()))
        .expect("redrive");
    let (receipt, redriven) = leased(rt.block_on(node.handle.take(take(&group, "s", 1))));
    assert_eq!(redriven[0].handed.stream, dead_stream);
    rt.block_on(node.handle.settle(receipt, bury(&ids(&redriven))))
        .expect("the dead copy of a redriven copy fits one block");
    let (_, dead) = leased(rt.block_on(node.handle.take(take("x", &dead_stream, 10))));
    assert_eq!(dead.len(), 2);
    for delivery in &dead {
        let copy = read_one(&rt, &node, delivery).expect("dead copy");
        assert_eq!(copy.value.len(), fits);
    }
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_take_or_a_tick_makes_at_most_16_dead_copies_and_reads_each_run_once() {
    let rt = runtime();
    let fs = Counted {
        fs: FaultFs::new(25, 1 << 40),
        reads: Arc::default(),
        reader: Arc::default(),
    };
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let batch = (0..40u8).map(|n| expiring("s", &[n], 2_000)).collect();
    let published = rt
        .block_on(node.handle.publish(batch, None))
        .expect("publish");
    clock.set(3_000);
    fs.reads.store(0, Ordering::SeqCst);
    let (_, none) = leased(rt.block_on(node.handle.take(take("g", "s", 1))));
    assert!(none.is_empty());
    publish(&rt, &node, "barrier", &[b"b"]);
    let (_, first) = leased(rt.block_on(node.handle.take(take("x", "s.dead.g", 100))));
    assert_eq!(first.len(), DEAD_PER_PASS);
    assert_eq!(fs.reads.load(Ordering::SeqCst), 1);
    for _ in 0..2 {
        clock.advance(1);
        publish(&rt, &node, "barrier", &[b"b"]);
    }
    assert_eq!(fs.reads.load(Ordering::SeqCst), 3);
    let (_, all) = leased(rt.block_on(node.handle.take(take("y", "s.dead.g", 100))));
    let origins: BTreeSet<MessageId> = all
        .iter()
        .filter_map(|d| read_one(&rt, &node, d)?.origin.map(|o| o.id))
        .collect();
    let expected: BTreeSet<MessageId> = (0..published.count)
        .map(|n| nth_id(published.first, n).expect("id"))
        .collect();
    assert_eq!(all.len(), expected.len());
    assert_eq!(origins, expected);
    stop(&rt, node).expect("clean stop");
}

#[test]
fn the_engine_reads_no_command_while_its_queued_blocks_are_over_the_cap() {
    let rt = runtime();
    let fs = FaultFs::new(26, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    rt.block_on(async {
        let _held = Held::new(&fs);
        let replies: Vec<_> = (0..6)
            .map(|_| {
                let handle = node.handle.clone();
                tokio::spawn(async move {
                    handle
                        .publish(vec![message("s", &vec![0u8; 16 << 20])], None)
                        .await
                })
            })
            .collect();
        until_sync_held(&fs).await;
        let handle = node.handle.clone();
        let probe = tokio::spawn(async move { handle.damage().await });
        for _ in 0..200 {
            tokio::task::yield_now().await;
        }
        assert!(
            !probe.is_finished(),
            "the engine read a command while over the queue cap"
        );
        fs.release_syncs();
        probe.await.expect("join").expect("damage");
        for reply in replies {
            reply.await.expect("join").expect("publish");
        }
    });
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_client_publish_to_a_dead_or_internal_stream_is_refused() {
    let rt = runtime();
    let fs = FaultFs::new(27, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    for stream in ["orders.dead.billing", "_x"] {
        let refused = rt.block_on(node.handle.publish(vec![message(stream, b"a")], None));
        assert_eq!(refused, Err(Error::BadName), "{stream}");
    }
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_stream_delete_of_a_reserved_name_is_refused_and_of_a_dead_stream_is_not() {
    let rt = runtime();
    let fs = FaultFs::new(61, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    assert_eq!(
        rt.block_on(node.handle.delete_stream("_x".to_owned())),
        Err(Error::BadName)
    );
    rt.block_on(node.handle.delete_stream("s.dead.g".to_owned()))
        .expect("a dead stream delete");
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_damaged_block_in_a_compacted_file_with_no_good_block_is_reported_by_its_file_and_range() {
    let rt = runtime();
    let fs = FaultFs::new(28, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let first = publish(&rt, &node, "s", &[b"a"]);
    stop(&rt, node).expect("clean stop");
    let segment = SegmentId {
        incarnation: first[0].incarnation,
        seq: 0,
    };
    let compacted = Path::new(DATA).join(format!("cmp-{:016x}.vdl", 99));
    fs.rename(
        &Path::new(DATA).join(segment_file_name(segment)),
        &compacted,
    )
    .expect("rename");
    let at = PAGE + 60;
    fs.open(&compacted)
        .expect("open")
        .write_at(&[0xab; 8], at)
        .expect("garbage");
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let (damage, unmapped) = rt.block_on(node.handle.damage()).expect("damage");
    assert!(
        unmapped
            .iter()
            .any(|w| w.file == compacted && w.range.contains(&at)),
        "{damage:?} {unmapped:?}"
    );
    assert!(
        damage.iter().all(|d| !(d.start..d.end).contains(&at)),
        "{damage:?}"
    );
    stop(&rt, node).expect("clean stop");
}

fn named_after_a_header_flip(seed: u64, byte: u64, mask: u8) -> (SegmentId, u64, Vec<SegmentId>) {
    let rt = runtime();
    let fs = FaultFs::new(seed, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let first = publish(&rt, &node, "s", &[b"a"]);
    publish(&rt, &node, "s", &[b"b"]);
    publish(&rt, &node, "s", &[b"c"]);
    clock.advance(IDLE_SEAL_MS);
    rt.block_on(node.handle.damage()).expect("tick");
    publish(&rt, &node, "s", &[b"d"]);
    stop(&rt, node).expect("clean stop");
    let segment = SegmentId {
        incarnation: first[0].incarnation,
        seq: 0,
    };
    let next = SegmentId { seq: 1, ..segment };
    let path = Path::new(DATA).join(segment_file_name(segment));
    let held: Vec<u64> = blocks(&fs)
        .into_iter()
        .filter(|(p, _, _)| *p == path)
        .map(|(_, offset, _)| offset)
        .collect();
    assert_eq!(held.len(), 3, "{held:?}");
    assert!(
        blocks(&fs)
            .iter()
            .any(|(p, _, _)| *p == Path::new(DATA).join(segment_file_name(next))),
        "no live segment {next}"
    );
    let compacted = Path::new(DATA).join(format!("cmp-{:016x}.vdl", 99));
    fs.rename(&path, &compacted).expect("rename");
    let at = held[1] + byte;
    let file = fs.open(&compacted).expect("open");
    let mut old = [0];
    file.read_at(&mut old, at).expect("read");
    file.write_at(&[old[0] ^ mask], at).expect("flip");
    drop(file);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let (damage, unmapped) = rt.block_on(node.handle.damage()).expect("damage");
    stop(&rt, node).expect("clean stop");
    assert!(unmapped.is_empty(), "{unmapped:?}");
    let named = damage
        .iter()
        .filter(|d| (d.start..d.end).contains(&at))
        .map(|d| d.segment)
        .collect();
    (segment, at, named)
}

#[test]
fn a_flip_in_the_incarnation_of_a_compacted_block_names_the_segment_of_its_neighbours() {
    let (segment, at, named) = named_after_a_header_flip(30, 10, 0x01);
    assert_eq!(named, [segment], "flip at {at}");
}

#[test]
fn a_flip_in_the_seq_of_a_compacted_block_does_not_name_another_live_segment() {
    let (segment, at, named) = named_after_a_header_flip(31, 14, 0x01);
    assert_eq!(named, [segment], "flip at {at}");
}

fn dead_copies_after_a_full_disk_and_a_tick(
    seed: u64,
    capacity: u64,
    full: io::ErrorKind,
    later: u64,
) -> usize {
    let rt = runtime();
    let fs = FaultFs::new(seed, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    rt.block_on(node.handle.publish(vec![expiring("s", b"a", 2_000)], None))
        .expect("publish");
    fs.set_capacity(capacity);
    clock.set(3_000);
    fs.fail_next_write(full, 0);
    let (_, none) = leased(rt.block_on(node.handle.take(take("g", "s", 1))));
    assert!(none.is_empty());
    rt.block_on(node.handle.delete_stream("barrier".to_owned()))
        .expect("a barrier");
    clock.advance(later);
    rt.block_on(node.handle.delete_stream("barrier".to_owned()))
        .expect("a barrier");
    let (_, dead) = leased(rt.block_on(node.handle.take(take("x", "s.dead.g", 10))));
    stop(&rt, node).expect("clean stop");
    dead.len()
}

#[test]
fn after_enospc_an_expiry_makes_no_dead_copy_while_free_space_is_under_the_pressure_line() {
    assert_eq!(
        dead_copies_after_a_full_disk_and_a_tick(
            29,
            128 << 20,
            io::ErrorKind::StorageFull,
            PRESSURE_HOLD_MS
        ),
        0
    );
}

#[test]
fn after_enospc_an_expiry_makes_its_dead_copy_once_free_space_is_over_the_pressure_line() {
    assert_eq!(
        dead_copies_after_a_full_disk_and_a_tick(
            30,
            1 << 40,
            io::ErrorKind::StorageFull,
            PRESSURE_HOLD_MS
        ),
        1
    );
}

#[test]
fn after_edquot_the_engine_waits_the_hold_time_before_it_makes_the_dead_copy() {
    assert_eq!(
        dead_copies_after_a_full_disk_and_a_tick(38, 1 << 40, io::ErrorKind::QuotaExceeded, 1),
        0
    );
}

#[test]
fn a_control_record_over_the_block_cap_is_refused_and_the_node_goes_on() {
    let rt = runtime();
    let fs = FaultFs::new(31, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let published = publish(&rt, &node, "s", &[b"a"]);
    let (receipt, _) = leased(rt.block_on(node.handle.take(take("g", "s", 1))));
    let nack = SettleBy::Ids(Settle {
        nack: vec![Nack {
            id: published[0],
            delay_ms: None,
            error: Some("e".repeat(MAX_BLOCK)),
        }],
        ..Settle::default()
    });
    let refused = rt.block_on(node.handle.settle(receipt, nack));
    assert_eq!(refused, Err(Error::TooLarge));
    rt.block_on(node.handle.settle(receipt, ack(&published)))
        .expect("an ack after the refusal");
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_node_starts_on_a_full_disk_and_records_its_recovery_damage_later() {
    let rt = runtime();
    let fs = FaultFs::new(32, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let published = publish(&rt, &node, "s", &[b"a"]);
    stop(&rt, node).expect("clean stop");
    let path = Path::new(DATA).join(segment_file_name(SegmentId {
        incarnation: published[0].incarnation,
        seq: 0,
    }));
    fs.open(&path)
        .expect("open")
        .write_at(&[0xab; 100], 3 * PAGE)
        .expect("garbage");
    fs.fail_next_write(io::ErrorKind::StorageFull, 0);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let waiting = fs.open(&path).expect("open").size().expect("size");
    assert_eq!(
        waiting,
        3 * PAGE + 100,
        "the file keeps its preallocated space"
    );
    clock.advance(1);
    publish(&rt, &node, "barrier", &[b"b"]);
    let (damage, _) = rt.block_on(node.handle.damage()).expect("damage");
    assert!(
        damage
            .iter()
            .any(|d| (d.start..d.end).contains(&(3 * PAGE))),
        "{damage:?}"
    );
    let size = fs.open(&path).expect("open").size().expect("size");
    assert!(size < 3 * PAGE, "the bad tail is not cut: {size}");
    let (_, handed) = leased(rt.block_on(node.handle.take(take("g", "s", 10))));
    assert_eq!(ids(&handed), published);
    stop(&rt, node).expect("clean stop");
}

fn damage_holders(fs: &FaultFs, at: u64) -> Vec<(PathBuf, u64)> {
    blocks(&fs.snapshot())
        .into_iter()
        .filter(|(_, _, body)| {
            records::decode(body)
                .expect("body")
                .controls
                .iter()
                .any(|c| matches!(c, Control::Damage(d) if (d.start..d.end).contains(&at)))
        })
        .map(|(path, offset, _)| (path, offset))
        .collect()
}

#[test]
fn a_damage_record_stays_when_a_flip_hits_one_of_its_two_blocks() {
    let rt = runtime();
    let fs = FaultFs::new(33, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let published = publish(&rt, &node, "s", &[b"a"]);
    stop(&rt, node).expect("clean stop");
    let path = Path::new(DATA).join(segment_file_name(SegmentId {
        incarnation: published[0].incarnation,
        seq: 0,
    }));
    fs.open(&path)
        .expect("open")
        .write_at(&[0xab; 100], 3 * PAGE)
        .expect("garbage");
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    stop(&rt, node).expect("clean stop");
    let holders = damage_holders(&fs, 3 * PAGE);
    let (file, offset) = holders.first().expect("a block with the damage record");
    assert!(fs.flip_at(file, offset + BLOCK_HEADER_LEN as u64, 0x40));
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let (damage, _) = rt.block_on(node.handle.damage()).expect("damage");
    assert!(
        damage
            .iter()
            .any(|d| (d.start..d.end).contains(&(3 * PAGE))),
        "{damage:?}"
    );
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_take_that_defers_dead_copies_writes_its_read_set_record_once() {
    let rt = runtime();
    let fs = FaultFs::new(34, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let batch = (0..40u8).map(|n| expiring("s", &[n], 2_000)).collect();
    rt.block_on(node.handle.publish(batch, None))
        .expect("publish");
    clock.set(3_000);
    rt.block_on(async {
        let _held = Held::new(&fs);
        let handle = node.handle.clone();
        let barrier =
            tokio::spawn(async move { handle.publish(vec![message("barrier", b"b")], None).await });
        until_sync_held(&fs).await;
        for _ in 0..2 {
            let (_, none) = leased(node.handle.take(take("g", "s", 1)).await);
            assert!(none.is_empty());
        }
        fs.release_syncs();
        barrier.await.expect("join").expect("publish");
    });
    publish(&rt, &node, "barrier", &[b"c"]);
    stop(&rt, node).expect("clean stop");
    let read_sets: usize = blocks(&fs.snapshot())
        .iter()
        .map(|(_, _, body)| {
            records::decode(body)
                .expect("body")
                .controls
                .iter()
                .filter(
                    |c| matches!(c, Control::Record(Record::ReadSet { group, .. }) if group == "g"),
                )
                .count()
        })
        .sum();
    assert_eq!(read_sets, 1);
}

#[test]
fn an_expiry_whose_dead_copy_cannot_fit_is_acked_once_and_its_drop_is_recorded() {
    let rt = runtime();
    let fs = Counted {
        fs: FaultFs::new(35, 1 << 40),
        reads: Arc::default(),
        reader: Arc::default(),
    };
    let dir = Path::new(DATA);
    let id = MessageId::new(1_000, 1, 0).expect("id");
    let mut original = Message {
        position: 0,
        id,
        timing: Timing {
            delay_until: None,
            expire_at: Some(2_000),
        },
        origin: None,
        headers: Vec::new(),
        value: Vec::new(),
    };
    let empty = records::encode(&[("s", std::slice::from_ref(&original))], &[]);
    original.value = vec![1; MAX_BLOCK - BLOCK_HEADER_LEN - empty.0.len()];
    let (body, _) = records::encode(&[("s", std::slice::from_ref(&original))], &[]);
    let log = Log::start(
        fs.clone(),
        1,
        recover(&fs, dir, |_, _, _| {}).expect("recover"),
    );
    rt.block_on(log.append(1_000, body, SEGMENT_BYTES))
        .expect("reply")
        .expect("append");
    rt.block_on(log.close());
    let clock = TestClock::new(3_000);
    let node = boot(&rt, &fs, dir, &clock);
    let (_, none) = leased(rt.block_on(node.handle.take(take("g", "s", 1))));
    assert!(none.is_empty());
    publish(&rt, &node, "barrier", &[b"b"]);
    let reads = fs.reads.load(Ordering::SeqCst);
    for _ in 0..3 {
        clock.advance(PRESSURE_HOLD_MS);
        publish(&rt, &node, "barrier", &[b"c"]);
    }
    assert_eq!(
        fs.reads.load(Ordering::SeqCst),
        reads,
        "the dead copy is tried again"
    );
    let dropped = rt.block_on(node.handle.dropped()).expect("dropped");
    let named: Vec<(&str, MessageId)> = dropped.iter().map(|d| (d.group.as_str(), d.id)).collect();
    assert_eq!(named, [("g", id)]);
    stop(&rt, node).expect("clean stop");
    let acks = blocks(&fs.fs.snapshot())
        .iter()
        .flat_map(|(_, _, body)| records::decode(body).expect("body").controls)
        .filter(|c| matches!(c, Control::Record(Record::Ack { group, .. }) if group == "g"))
        .count();
    assert_eq!(acks, 1);
}

#[test]
fn a_dead_settle_over_the_queue_cap_goes_in_steps_and_replies_after_every_dead_copy_is_durable() {
    let rt = runtime();
    let fs = FaultFs::new(36, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let big = vec![3u8; 22 << 20];
    let mut published = Vec::new();
    for _ in 0..3 {
        published.extend(publish(&rt, &node, "s", &[&big]));
    }
    let (receipt, handed) = leased(rt.block_on(node.handle.take(take("g", "s", 3))));
    assert_eq!(handed.len(), 3);
    rt.block_on(node.handle.settle(receipt, bury(&published)))
        .expect("a dead settle over 64 MiB");
    let (_, dead) = leased(rt.block_on(node.handle.take(take("x", "s.dead.g", 10))));
    assert_eq!(dead.len(), 3);
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_read_runs_off_the_caller_thread_and_finds_each_message_of_its_runs() {
    let rt = runtime();
    let fs = Counted {
        fs: FaultFs::new(37, 1 << 40),
        reads: Arc::default(),
        reader: Arc::default(),
    };
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let first = publish(&rt, &node, "s", &[b"a0", b"a1"]);
    let second = publish(&rt, &node, "s", &[b"b0"]);
    let (_, handed) = leased(rt.block_on(node.handle.take(take("g", "s", 10))));
    let delivery = |id: MessageId| {
        handed
            .iter()
            .find(|d| d.handed.id == id)
            .cloned()
            .expect("a handed id")
    };
    let order = [delivery(first[1]), delivery(second[0]), delivery(first[0])];
    let values: Vec<Vec<u8>> = rt
        .block_on(node.handle.read(&order))
        .expect("read")
        .into_iter()
        .map(|m| m.expect("a message").value)
        .collect();
    assert_eq!(values, [b"a1".to_vec(), b"b0".to_vec(), b"a0".to_vec()]);
    let reader = fs.reader.lock().expect("reader").expect("a read");
    assert_ne!(reader, thread::current().id(), "the read ran on the caller");
    stop(&rt, node).expect("clean stop");
}

#[test]
fn an_idempotency_key_whose_scope_is_not_the_stream_is_refused() {
    let rt = runtime();
    let fs = FaultFs::new(39, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let key = |scope: &str| {
        Some(Key {
            scope: scope.to_owned(),
            key: "k".to_owned(),
        })
    };
    let refused = rt.block_on(node.handle.publish(vec![message("s", b"a")], key("t")));
    assert_eq!(refused, Err(Error::BadName));
    rt.block_on(node.handle.publish(vec![message("s", b"a")], key("s")))
        .expect("a key of the stream");
    rt.block_on(
        node.handle
            .publish(vec![message("s", b"a"), message("t", b"b")], key(BULK)),
    )
    .expect("a bulk key");
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_take_whose_read_set_block_gets_enospc_writes_the_read_set_record_again_once() {
    let rt = runtime();
    let fs = FaultFs::new(40, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    publish(&rt, &node, "s", &[b"a"]);
    fs.fail_next_write(io::ErrorKind::StorageFull, 0);
    let (_, first) = leased(rt.block_on(node.handle.take(take("g", "s", 1))));
    assert_eq!(first.len(), 1);
    publish(&rt, &node, "barrier", &[b"b"]);
    for _ in 0..2 {
        leased(rt.block_on(node.handle.take(take("g", "s", 1))));
    }
    publish(&rt, &node, "barrier", &[b"c"]);
    stop(&rt, node).expect("clean stop");
    let read_sets: usize = blocks(&fs.snapshot())
        .iter()
        .map(|(_, _, body)| {
            records::decode(body)
                .expect("body")
                .controls
                .iter()
                .filter(
                    |c| matches!(c, Control::Record(Record::ReadSet { group, .. }) if group == "g"),
                )
                .count()
        })
        .sum();
    assert_eq!(read_sets, 1);
}

#[test]
fn a_provisional_forget_whose_block_gets_enospc_wakes_the_waiters_of_the_group() {
    let rt = runtime();
    let fs = FaultFs::new(41, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let wait = Take {
        wait: true,
        ..take("g", "s", 1)
    };
    let Ok(Taken::Waiting { woken, .. }) = rt.block_on(node.handle.take(wait)) else {
        panic!("expected a waiter");
    };
    publish(&rt, &node, "barrier", &[b"a"]);
    fs.fail_next_write(io::ErrorKind::StorageFull, 0);
    clock.advance(PROVISIONAL_MS);
    let woken = rt.block_on(async { tokio::time::timeout(Duration::from_secs(5), woken).await });
    assert_eq!(woken.expect("the waiter is woken"), Ok(()));
    stop(&rt, node).expect("clean stop");
}

fn waiting(rt: &Runtime, node: &Node) -> tokio::sync::oneshot::Receiver<()> {
    let wait = Take {
        wait: true,
        ..take("g", "s", 10)
    };
    match rt.block_on(node.handle.take(wait)) {
        Ok(Taken::Waiting { woken, .. }) => woken,
        _ => panic!("expected a waiter"),
    }
}

fn asleep(rt: &Runtime, woken: &mut tokio::sync::oneshot::Receiver<()>) -> bool {
    rt.block_on(async { tokio::time::sleep(Duration::from_millis(200)).await });
    matches!(
        woken.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    )
}

#[test]
fn a_release_wakes_a_sleeping_long_poll_waiter_of_the_group() {
    let rt = runtime();
    let fs = FaultFs::new(41, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    publish(&rt, &node, "s", &[b"a", b"b"]);
    let (receipt, handed) = leased(rt.block_on(node.handle.take(take("g", "s", 10))));
    assert_eq!(handed.len(), 2);
    let mut woken = waiting(&rt, &node);
    assert!(asleep(&rt, &mut woken), "woken before the release");
    rt.block_on(node.handle.release(receipt, vec![handed[1].handed.id]))
        .expect("release");
    let wake = rt.block_on(async { tokio::time::timeout(Duration::from_secs(5), woken).await });
    assert_eq!(wake.expect("woken within 5 s"), Ok(()));
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_take_whose_reply_cannot_be_sent_wakes_a_sleeping_long_poll_waiter_of_the_group() {
    let rt = runtime();
    let fs = FaultFs::new(42, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let first = waiting(&rt, &node);
    let mut second = waiting(&rt, &node);
    publish(&rt, &node, "s", &[b"a"]);
    let wake = rt.block_on(async { tokio::time::timeout(Duration::from_secs(5), first).await });
    assert_eq!(wake.expect("the publish wakes the first waiter"), Ok(()));
    assert!(asleep(&rt, &mut second), "woken before the lost reply");
    let mut gone = Box::pin(node.handle.take(take("g", "s", 10)));
    let pending = rt.block_on(async {
        gone.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    });
    assert!(pending);
    drop(gone);
    let wake = rt.block_on(async { tokio::time::timeout(Duration::from_secs(5), second).await });
    assert_eq!(wake.expect("woken within 5 s"), Ok(()));
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_damage_record_that_enospc_refuses_at_each_tick_is_written_once_in_two_blocks() {
    let rt = runtime();
    let fs = FaultFs::new(62, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let published = publish(&rt, &node, "s", &[b"a"]);
    stop(&rt, node).expect("clean stop");
    let path = Path::new(DATA).join(segment_file_name(SegmentId {
        incarnation: published[0].incarnation,
        seq: 0,
    }));
    let file = fs.open(&path).expect("open");
    file.set_len(3 * PAGE).expect("cut the preallocated space");
    file.write_at(&[0xab; 100], 3 * PAGE).expect("garbage");
    fs.set_capacity(fs.used());
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let rounds: Vec<Result<MessageId, Error>> = (0..12)
        .map(|_| {
            clock.advance(1);
            rt.block_on(node.handle.delete_stream("barrier".to_owned()))
        })
        .collect();
    assert!(
        rounds.iter().all(|r| *r == Err(Error::NoSpace)),
        "{rounds:?}"
    );
    fs.set_capacity(1 << 40);
    clock.advance(1);
    rt.block_on(node.handle.delete_stream("barrier".to_owned()))
        .expect("a barrier");
    stop(&rt, node).expect("clean stop");
    let records = blocks(&fs.snapshot())
        .iter()
        .flat_map(|(_, _, body)| records::decode(body).expect("body").controls)
        .filter(|c| matches!(c, Control::Damage(d) if (d.start..d.end).contains(&(3 * PAGE))))
        .count();
    assert_eq!(records, 2);
}

#[test]
fn a_damage_warning_ends_when_its_segment_goes() {
    let rt = runtime();
    let fs = FaultFs::new(63, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    publish(&rt, &node, "s", &[b"a"]);
    let (_, mut handed) = leased(rt.block_on(node.handle.take(take("g", "s", 1))));
    if let Some(location) = handed[0].location.as_mut() {
        location.run.crc ^= 1;
    }
    assert_eq!(
        rt.block_on(node.handle.read(&handed)).expect("read"),
        [None]
    );
    rt.block_on(node.handle.delete_stream("barrier".to_owned()))
        .expect("a barrier");
    assert_eq!(
        rt.block_on(node.handle.damage()).expect("damage").0.len(),
        1
    );
    clock.advance(IDLE_SEAL_MS);
    rt.block_on(node.handle.damage()).expect("a tick");
    rt.block_on(node.handle.compact()).expect("compact");
    let (damage, _) = rt.block_on(node.handle.damage()).expect("damage");
    assert!(damage.is_empty(), "{damage:?}");
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_read_damage_of_a_run_that_compaction_moved_leaves_the_moved_run() {
    let rt = runtime();
    let fs = FaultFs::new(64, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    publish(&rt, &node, "gone", &[&[b'g'; 16384]]);
    let kept = publish(&rt, &node, "s", &[b"kept"]);
    let (_, mut handed) = leased(rt.block_on(node.handle.take(take("g", "s", 1))));
    rt.block_on(node.handle.delete_stream("gone".to_owned()))
        .expect("delete");
    clock.advance(IDLE_SEAL_MS);
    rt.block_on(node.handle.damage()).expect("a tick");
    rt.block_on(node.handle.compact()).expect("compact");
    let names = fs.list(Path::new(DATA)).expect("list");
    assert!(names.iter().any(|n| n.starts_with("cmp-")), "{names:?}");
    if let Some(location) = handed[0].location.as_mut() {
        location.run.crc ^= 1;
    }
    assert_eq!(
        rt.block_on(node.handle.read(&handed)).expect("read"),
        [None]
    );
    rt.block_on(node.handle.delete_stream("barrier".to_owned()))
        .expect("a barrier");
    let (_, again) = leased(rt.block_on(node.handle.take(take("h", "s", 1))));
    assert_eq!(ids(&again), kept);
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_replay_from_a_message_id_stays_the_id_floor_after_its_segment_goes() {
    let rt = runtime();
    let fs = FaultFs::new(65, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let from = publish(&rt, &node, "s", &[b"a"]);
    let (receipt, _) = leased(rt.block_on(node.handle.take(take("g", "s", 1))));
    rt.block_on(node.handle.settle(receipt, ack(&from)))
        .expect("ack");
    rt.block_on(node.handle.replay("g".to_owned(), Bound::Excluded(from[0])))
        .expect("replay");
    clock.advance(RETAIN_MS + IDLE_SEAL_MS);
    rt.block_on(node.handle.damage()).expect("a tick");
    rt.block_on(node.handle.compact()).expect("compact");
    stop(&rt, node).expect("clean stop");
    assert!(
        blocks(&fs)
            .iter()
            .all(|(_, _, body)| stored_messages(body).is_empty()),
        "the segment of the from id is still held"
    );
    clock.set(from[0].ms() - 30_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let new = publish(&rt, &node, "s", &[b"b"]);
    assert!(new[0] > from[0], "{} is not above {}", new[0], from[0]);
    let (_, handed) = leased(rt.block_on(node.handle.take(take("g", "s", 10))));
    assert_eq!(ids(&handed), new);
    stop(&rt, node).expect("clean stop");
}

#[test]
fn a_redrive_while_a_replay_of_the_group_waits_for_its_block_is_refused() {
    let rt = runtime();
    let fs = FaultFs::new(66, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let published = publish(&rt, &node, "s", &[b"a"]);
    let (receipt, _) = leased(rt.block_on(node.handle.take(take("g", "s", 1))));
    rt.block_on(node.handle.settle(receipt, bury(&published)))
        .expect("dead settle");
    let redrive = rt.block_on(async {
        let _held = Held::new(&fs);
        let handle = node.handle.clone();
        let replay = tokio::spawn(async move {
            handle
                .replay("g".to_owned(), Bound::Excluded(published[0]))
                .await
        });
        until_sync_held(&fs).await;
        let handle = node.handle.clone();
        let redrive = tokio::spawn(async move { handle.redrive("g".to_owned()).await });
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        fs.release_syncs();
        replay.await.expect("join").expect("replay");
        redrive.await.expect("join")
    });
    assert_eq!(redrive, Err(Error::Replaying));
    rt.block_on(node.handle.redrive("g".to_owned()))
        .expect("redrive after the replay");
    let (_, again) = leased(rt.block_on(node.handle.take(take("g", "s", 10))));
    assert_eq!(
        again
            .iter()
            .map(|d| d.handed.stream.as_str())
            .collect::<Vec<_>>(),
        ["s.dead.g"]
    );
    stop(&rt, node).expect("clean stop");
}

struct Incarnation(u64);

impl Random for Incarnation {
    fn fill(&mut self, bytes: &mut [u8]) -> Result<(), Error> {
        for (byte, value) in bytes
            .iter_mut()
            .zip(self.0.to_le_bytes().into_iter().cycle())
        {
            *byte = value;
        }
        Ok(())
    }
}

fn boot_as(rt: &Runtime, fs: &FaultFs, clock: &TestClock, incarnation: u64) -> Node {
    let (engine, handle) = rt
        .block_on(Engine::open(
            fs.clone(),
            Path::new(DATA),
            Arc::new(clock.clone()),
            Box::new(Incarnation(incarnation)),
        ))
        .expect("open");
    let task = rt.spawn(engine.run());
    Node { handle, task }
}

#[test]
fn now_after_a_restart_with_a_lower_incarnation_is_after_every_message_in_the_log() {
    let rt = runtime();
    let fs = FaultFs::new(66, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot_as(&rt, &fs, &clock, u64::MAX - 1);
    let old = publish(&rt, &node, "s", &[b"a", b"b"]);
    stop(&rt, node).expect("clean stop");
    let node = boot_as(&rt, &fs, &clock, 1);
    let now = rt.block_on(node.handle.issued()).expect("issued");
    assert!(now > old[1], "{now} is not after {}", old[1]);
    let scan = |from: MessageId| {
        rt.block_on(node.handle.scan("s".to_owned(), Bound::Excluded(from), 10))
            .expect("scan")
    };
    assert_eq!(ids(&scan(now)), []);
    let new = publish(&rt, &node, "s", &[b"c"]);
    assert_eq!(ids(&scan(now)), new);
    stop(&rt, node).expect("clean stop");
}

#[test]
fn the_applied_watch_names_the_streams_of_each_block_with_messages() {
    let rt = runtime();
    let fs = FaultFs::new(67, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let mut applied = node.handle.applied();
    rt.block_on(
        node.handle
            .publish(vec![message("b", b"1"), message("c", b"2")], None),
    )
    .expect("publish");
    let (blocks, mut streams) = applied.borrow_and_update().clone();
    streams.sort();
    assert_eq!((blocks, streams), (1, vec!["b".to_owned(), "c".to_owned()]));
    rt.block_on(node.handle.delete_stream("b".to_owned()))
        .expect("delete");
    assert!(!applied.has_changed().expect("the engine runs"));
    publish(&rt, &node, "a", &[b"3"]);
    assert_eq!(*applied.borrow(), (2, vec!["a".to_owned()]));
    stop(&rt, node).expect("clean stop");
}

#[test]
fn the_write_rate_is_zero_after_the_clock_steps_back_and_counts_from_the_step() {
    let rt = runtime();
    let fs = FaultFs::new(70, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    publish(&rt, &node, "s", &[&[b'x'; 1000]]);
    clock.advance(5_000);
    clock.set(900_000);
    rt.block_on(node.handle.damage()).expect("a tick");
    let status = rt.block_on(node.handle.status()).expect("status");
    assert_eq!(status.write_bytes_per_s, 0);
    publish(&rt, &node, "s", &[&[b'x'; 1000]]);
    clock.advance(1_000);
    rt.block_on(node.handle.damage()).expect("a tick");
    let status = rt.block_on(node.handle.status()).expect("status");
    assert!(
        (10..30).contains(&status.write_bytes_per_s),
        "{}",
        status.write_bytes_per_s
    );
    clock.advance(9_000);
    rt.block_on(node.handle.damage()).expect("a tick");
    let status = rt.block_on(node.handle.status()).expect("status");
    assert!(
        (10..30).contains(&status.write_bytes_per_s),
        "{}",
        status.write_bytes_per_s
    );
    stop(&rt, node).expect("clean stop");
}

#[test]
fn status_holds_the_damage_its_stream_names_the_unmapped_files_and_the_dropped_copies_in_one_reply()
{
    let rt = runtime();
    let fs = FaultFs::new(71, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    publish(&rt, &node, "s", &[b"a"]);
    publish(&rt, &node, "s", &[b"b"]);
    let (_, mut handed) = leased(rt.block_on(node.handle.take(take("g", "s", 1))));
    if let Some(location) = handed[0].location.as_mut() {
        location.run.crc ^= 1;
    }
    assert_eq!(
        rt.block_on(node.handle.read(&handed)).expect("read"),
        [None]
    );
    rt.block_on(node.handle.delete_stream("barrier".to_owned()))
        .expect("a barrier");
    let (damage, unmapped) = rt.block_on(node.handle.damage()).expect("damage");
    let dropped = rt.block_on(node.handle.dropped()).expect("dropped");
    let status = rt.block_on(node.handle.status()).expect("status");
    assert_eq!(status.damage, damage);
    assert_eq!(status.unmapped, unmapped);
    assert_eq!(status.dropped, dropped);
    assert_eq!(status.damage.len(), 1);
    assert_eq!(
        status.damaged.get(&status.damage[0].segment),
        Some(&vec!["s".to_owned()])
    );
    stop(&rt, node).expect("clean stop");
}

#[test]
fn the_write_rate_counts_both_damage_blocks_of_recovery_with_their_headers() {
    let rt = runtime();
    let fs = FaultFs::new(72, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    publish(&rt, &node, "s", &[&[b'a'; 100]]);
    publish(&rt, &node, "s", &[&[b'b'; 100]]);
    stop(&rt, node).expect("clean stop");
    let files = fs.list(Path::new(DATA)).expect("list");
    assert_eq!(files.len(), 1, "{files:?}");
    let path = Path::new(DATA).join(&files[0]);
    assert!(fs.flip_at(&path, PAGE + BLOCK_HEADER_LEN as u64 + 10, 1));
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let status = rt.block_on(node.handle.status()).expect("status");
    assert_eq!(status.damage.len(), 1, "{:?}", status.damage);
    let (body, _) = records::encode(&[], &[Control::Damage(status.damage[0])]);
    let written = FILE_HEADER_LEN + 2 * (BLOCK_HEADER_LEN + body.len());
    assert_eq!(status.write_bytes_per_s, written as u64 * 1000 / 60_000);
    stop(&rt, node).expect("clean stop");
}
