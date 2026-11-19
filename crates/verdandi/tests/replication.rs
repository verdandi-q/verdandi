use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::{TempDir, tempdir_in};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use verdandi::runner::Runner;
use verdandi_core::Record;
use verdandi_core::Timing;
use verdandi_core::ids::{MessageId, SegmentId};
use verdandi_core::lease::{Receipt, Settle};
use verdandi_net::{Net, Wire};
use verdandi_node::Msg;
use verdandi_store::block::{BlockHeader, encode_block};
use verdandi_store::engine::{
    Engine, Error, Handle, NewMessage, OsRandom, SettleBy, Take, Taken, WallClock,
};
use verdandi_store::fs::RealFs;
use verdandi_store::records::{self, Control};

const KEY: &[u8] = b"cluster key";
const BOUND: Duration = Duration::from_secs(120);

struct Member {
    runner: Runner,
    handle: Handle,
    engine: JoinHandle<Result<(), Error>>,
    store: PathBuf,
}

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/c3t5-dirs")
}

fn tempdir() -> TempDir {
    std::fs::create_dir_all(root()).unwrap();
    tempdir_in(root()).unwrap()
}

async fn member(peers: Vec<(u64, String)>, found: bool, dir: &Path) -> Member {
    let store = dir.join("store");
    std::fs::create_dir_all(&store).unwrap();
    let (engine, handle) = Engine::open(RealFs, &store, Arc::new(WallClock), Box::new(OsRandom))
        .await
        .unwrap();
    let engine = tokio::spawn(engine.run());
    let runner = Runner::launch(
        KEY,
        "127.0.0.1:0",
        peers,
        found,
        dir.into(),
        None,
        Some(handle.clone()),
    )
    .await
    .unwrap();
    Member {
        runner,
        handle,
        engine,
        store,
    }
}

async fn cluster(dirs: &[TempDir; 3]) -> [Member; 3] {
    let a = member(Vec::new(), true, dirs[0].path()).await;
    let pa = (a.runner.id(), a.runner.addr().to_owned());
    let b = member(vec![pa.clone()], false, dirs[1].path()).await;
    let pb = (b.runner.id(), b.runner.addr().to_owned());
    let c = member(vec![pa, pb], false, dirs[2].path()).await;
    let all: BTreeSet<u64> = [a.runner.id(), b.runner.id(), c.runner.id()].into();
    timeout(BOUND, async {
        loop {
            let ok = [&a, &b, &c]
                .iter()
                .all(|m| m.runner.members() == all && m.runner.leader().is_some());
            if ok {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the cluster forms");
    [a, b, c]
}

fn message(value: &str) -> NewMessage {
    NewMessage {
        stream: "s".to_owned(),
        headers: Vec::new(),
        value: value.as_bytes().to_vec(),
        timing: Timing::default(),
    }
}

async fn warm_up(handle: &Handle) {
    timeout(BOUND, async {
        while handle.publish(vec![message("warm")], None).await.is_err() {
            sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("the first publish succeeds");
}

async fn taken(handle: &Handle) -> BTreeSet<String> {
    let take = Take {
        group: "g".to_owned(),
        selector: "s".to_owned(),
        limit: 1000,
        lease_ms: 600_000,
        wait: false,
    };
    let got = handle.take(take).await;
    let Ok(Taken::Leased { messages, .. }) = got else {
        return BTreeSet::new();
    };
    handle
        .read(&messages)
        .await
        .unwrap()
        .into_iter()
        .flatten()
        .map(|m| String::from_utf8(m.value).unwrap())
        .collect()
}

async fn leased(handle: &Handle) -> Option<(Receipt, Vec<(String, MessageId)>)> {
    let take = Take {
        group: "g".to_owned(),
        selector: "s".to_owned(),
        limit: 1000,
        lease_ms: 600_000,
        wait: false,
    };
    let Ok(Taken::Leased {
        receipt, messages, ..
    }) = handle.take(take).await
    else {
        return None;
    };
    let read = handle.read(&messages).await.unwrap();
    let got = messages
        .iter()
        .zip(read)
        .filter_map(|(d, m)| Some((String::from_utf8(m?.value).unwrap(), d.handed.id)))
        .collect();
    Some((receipt, got))
}

fn segment_files(dir: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "vdl"))
        .collect()
}

fn same_blocks(writer: &Path, holders: [&Path; 2]) -> bool {
    let files = segment_files(writer);
    !files.is_empty()
        && files.iter().all(|file| {
            let want = std::fs::read(file).unwrap();
            holders.iter().any(|dir| {
                std::fs::read(dir.join(file.file_name().unwrap())).is_ok_and(|got| {
                    let n = got.len().min(want.len());
                    n > 4096 && want[..n] == got[..n]
                })
            })
        })
}

fn finish(members: [Member; 2]) {
    for m in members {
        m.engine.abort();
        drop(m.handle);
        drop(m.runner);
    }
    std::fs::remove_dir(root()).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_publish_needs_two_copies_and_a_holder_serves_after_the_writer_stops() {
    let dirs = [tempdir(), tempdir(), tempdir()];
    let [a, b, c] = cluster(&dirs).await;
    assert!(taken(&a.handle).await.is_empty());
    warm_up(&a.handle).await;
    let wanted: BTreeSet<String> = (0..10).map(|n| format!("m{n}")).collect();
    for value in &wanted {
        a.handle
            .publish(vec![message(value)], None)
            .await
            .expect("two durable copies");
    }
    let holders = [&b, &c];
    timeout(BOUND, async {
        while !same_blocks(&a.store, [&b.store, &c.store]) {
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("a holder holds the same blocks");

    a.runner.stop().await;
    a.engine.abort();
    drop(a.handle);
    let mut found = BTreeSet::new();
    timeout(BOUND, async {
        while !wanted.is_subset(&found) {
            for h in holders {
                found.extend(taken(&h.handle).await);
            }
            sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the holders serve the messages, got {found:?}"));
    assert!(wanted.is_subset(&found));
    finish([b, c]);
    drop(dirs);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_publish_with_no_reachable_holder_is_refused() {
    let dirs = [tempdir(), tempdir(), tempdir()];
    let [a, b, c] = cluster(&dirs).await;
    warm_up(&a.handle).await;
    b.runner.stop().await;
    c.runner.stop().await;
    let started = Instant::now();
    let result = a.handle.publish(vec![message("alone")], None).await;
    assert_eq!(result.map(|_| ()), Err(Error::Unconfirmed));
    assert!(started.elapsed() < Duration::from_secs(30));
    a.engine.abort();
    b.engine.abort();
    c.engine.abort();
    drop(dirs);
    std::fs::remove_dir(root()).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_settled_message_is_not_delivered_again_by_the_new_leader() {
    let dirs = [tempdir(), tempdir(), tempdir()];
    let [a, b, c] = cluster(&dirs).await;
    assert!(taken(&a.handle).await.is_empty());
    warm_up(&a.handle).await;
    let names: Vec<String> = (0..6).map(|n| format!("m{n}")).collect();
    for value in &names {
        a.handle
            .publish(vec![message(value)], None)
            .await
            .expect("two durable copies");
    }
    let (receipt, got) = timeout(BOUND, async {
        let mut all = Vec::new();
        let mut receipt = None;
        while all.len() < names.len() + 1 {
            if let Some((r, more)) = leased(&a.handle).await {
                receipt = Some(r);
                all.extend(more);
            }
            sleep(Duration::from_millis(200)).await;
        }
        (receipt.unwrap(), all)
    })
    .await
    .expect("the leader delivers the messages");
    let settled: BTreeSet<String> = names[..3].iter().cloned().collect();
    let ids: Vec<MessageId> = got
        .iter()
        .filter(|(v, _)| settled.contains(v))
        .map(|(_, id)| *id)
        .collect();
    assert_eq!(ids.len(), 3);
    a.handle
        .settle(
            receipt,
            SettleBy::Ids(Settle {
                ack: ids,
                ..Settle::default()
            }),
        )
        .await
        .expect("every ISR member holds the ack");

    a.runner.stop().await;
    a.engine.abort();
    drop(a.handle);
    let unsettled: BTreeSet<String> = names[3..]
        .iter()
        .cloned()
        .chain(["warm".to_owned()])
        .collect();
    let mut found = BTreeSet::new();
    timeout(BOUND, async {
        while !unsettled.is_subset(&found) {
            for h in [&b, &c] {
                found.extend(taken(&h.handle).await);
            }
            sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the holders serve the unsettled messages, got {found:?}"));
    assert!(
        found.is_disjoint(&settled),
        "a settled message came back: {found:?}"
    );
    finish([b, c]);
    drop(dirs);
}

fn trimmed(file: &Path) -> Vec<u8> {
    let mut bytes = std::fs::read(file).unwrap();
    let end = bytes.iter().rposition(|b| *b != 0).map_or(0, |i| i + 1);
    bytes.truncate(end);
    bytes
}

fn holds_all(writer: &Path, dir: &Path) -> bool {
    let files = segment_files(writer);
    !files.is_empty()
        && files.iter().all(|file| {
            let want = trimmed(file);
            std::fs::read(dir.join(file.file_name().unwrap()))
                .is_ok_and(|got| got.len() >= want.len() && got[..want.len()] == want[..])
        })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn repair_copies_a_segment_with_its_bytes_to_a_new_holder() {
    let dirs = [tempdir(), tempdir(), tempdir()];
    let fourth = tempdir();
    let [a, b, c] = cluster(&dirs).await;
    let peers = [&a, &b, &c]
        .iter()
        .map(|m| (m.runner.id(), m.runner.addr().to_owned()))
        .collect();
    let d = member(peers, false, fourth.path()).await;
    let all: BTreeSet<u64> = [&a, &b, &c, &d].iter().map(|m| m.runner.id()).collect();
    timeout(BOUND, async {
        while !(a.runner.members() == all && d.runner.members() == all) {
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the fourth node joins");
    assert!(taken(&a.handle).await.is_empty());
    warm_up(&a.handle).await;
    for n in 0..10 {
        a.handle
            .publish(vec![message(&format!("m{n}"))], None)
            .await
            .expect("two durable copies");
    }
    let others = [b, c, d];
    let before = timeout(BOUND, async {
        loop {
            let held: Vec<usize> = (0..3)
                .filter(|i| holds_all(&a.store, &others[*i].store))
                .collect();
            if held.len() == 2 {
                return held;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("a holder holds the segment");
    let (receipt, got) = timeout(BOUND, async {
        loop {
            match leased(&a.handle).await {
                Some((r, got)) if got.len() == 11 => return (r, got),
                _ => sleep(Duration::from_millis(200)).await,
            }
        }
    })
    .await
    .expect("the leader delivers the messages");
    let ack: Vec<MessageId> = got[..3].iter().map(|(_, id)| *id).collect();
    a.handle
        .settle(
            receipt,
            SettleBy::Ids(Settle {
                ack,
                ..Settle::default()
            }),
        )
        .await
        .expect("every ISR member holds the ack");
    let held = before[0];
    let mut others: Vec<Option<Member>> = others.into_iter().map(Some).collect();
    let lost = others[held].take().unwrap();
    lost.runner.stop().await;
    lost.engine.abort();
    drop(lost.handle);
    let fresh = timeout(Duration::from_secs(60), async {
        loop {
            let got = (0..3).find(|i| {
                !before.contains(i)
                    && others[*i]
                        .as_ref()
                        .is_some_and(|m| holds_all(&a.store, &m.store))
            });
            if let Some(i) = got {
                return i;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("repair gives the segment to a new holder");
    let writer = a.store.clone();
    let target = others[fresh].as_ref().unwrap().store.clone();
    let new_holder = others[fresh].as_ref().unwrap().handle.clone();
    timeout(Duration::from_secs(30), async {
        loop {
            let stats = new_holder.group_stats("g".to_owned()).await.unwrap();
            if stats.is_some_and(|s| s.pending == 8) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the new holder knows the acks that it missed");
    timeout(Duration::from_secs(60), async {
        let mut n = 0;
        loop {
            if a.handle
                .publish(vec![message(&format!("late{n}"))], None)
                .await
                .is_ok()
                && holds_all(&writer, &target)
            {
                return;
            }
            n += 1;
            sleep(Duration::from_millis(300)).await;
        }
    })
    .await
    .expect("the new holder gets the later blocks");
    a.engine.abort();
    for m in others.into_iter().flatten() {
        m.engine.abort();
    }
    drop(dirs);
    drop(fourth);
    std::fs::remove_dir(root()).ok();
}

async fn halt(m: Member) {
    m.runner.stop().await;
    m.engine.abort();
    let _ = m.engine.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_restarted_cluster_serves_the_messages_of_its_old_segments() {
    let dirs = [tempdir(), tempdir(), tempdir()];
    let [a, b, c] = cluster(&dirs).await;
    assert!(taken(&a.handle).await.is_empty());
    warm_up(&a.handle).await;
    let wanted: BTreeSet<String> = (0..6).map(|n| format!("m{n}")).collect();
    for value in &wanted {
        a.handle
            .publish(vec![message(value)], None)
            .await
            .expect("two durable copies");
    }
    timeout(BOUND, async {
        while !same_blocks(&a.store, [&b.store, &c.store]) {
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("a holder holds the same blocks");
    let ids = [a.runner.id(), b.runner.id(), c.runner.id()];
    halt(a).await;
    halt(b).await;
    halt(c).await;

    let a = member(Vec::new(), false, dirs[0].path()).await;
    let pa = (a.runner.id(), a.runner.addr().to_owned());
    let b = member(vec![pa.clone()], false, dirs[1].path()).await;
    let pb = (b.runner.id(), b.runner.addr().to_owned());
    let c = member(vec![pa, pb], false, dirs[2].path()).await;
    assert_eq!([a.runner.id(), b.runner.id(), c.runner.id()], ids);
    let mut found = BTreeSet::new();
    timeout(BOUND, async {
        while !wanted.is_subset(&found) {
            for m in [&a, &b, &c] {
                found.extend(taken(&m.handle).await);
            }
            sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the restarted cluster serves the messages, got {found:?}"));
    for m in [a, b, c] {
        m.engine.abort();
        drop(m.handle);
        drop(m.runner);
    }
    std::fs::remove_dir(root()).ok();
    drop(dirs);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_control_block_from_a_node_that_is_not_a_member_is_refused() {
    let dirs = [tempdir(), tempdir(), tempdir()];
    let [a, b, c] = cluster(&dirs).await;
    let (outsider, mut rx) = Net::start(KEY, "127.0.0.1:0", u64::MAX - 1).await.unwrap();
    outsider.connect(a.runner.id(), a.runner.addr());
    let tomb = Control::Record(Record::Tombstone {
        stream: "s".to_owned(),
        up_to: MessageId::new(MessageId::MAX_MS, 0, 0).unwrap(),
    });
    let (body, _) = records::encode(&[], &[tomb]);
    let header = BlockHeader {
        segment: SegmentId {
            incarnation: 1,
            seq: 0,
        },
        offset: 0,
        epoch: 0,
        compaction_version: 0,
        commit_ms: 0,
    };
    let ctl = Msg::Ctl {
        id: 1,
        offset: 0,
        bytes: encode_block(&header, &body),
    };
    outsider.send(a.runner.id(), ctl, Wire::default());
    let reply = timeout(Duration::from_secs(5), async {
        while let Some((_, _, msg)) = rx.recv().await {
            if matches!(msg, Msg::BlockOk { .. } | Msg::BlockErr { .. }) {
                return msg;
            }
        }
        std::future::pending().await
    })
    .await;
    outsider.stop();
    assert!(reply.is_err(), "a non-member got an answer");
    for m in [a, b, c] {
        m.engine.abort();
        drop(m.handle);
        drop(m.runner);
    }
    std::fs::remove_dir(root()).ok();
    drop(dirs);
}

fn floor_is(dir: &Path, value: u8) -> bool {
    std::fs::read(dir.join("cluster/floor")).is_ok_and(|b| b.last() == Some(&value))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_publish_confirms_at_the_ack_floor_of_three_copies() {
    let dirs = [tempdir(), tempdir(), tempdir()];
    let [a, b, c] = cluster(&dirs).await;
    timeout(BOUND, async {
        while ![&a, &b, &c].iter().all(|m| floor_is(m.runner.dir(), 3)) {
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("the floor reaches three");
    warm_up(&b.handle).await;
    timeout(BOUND, async {
        let mut n = 0;
        loop {
            let got = b
                .handle
                .publish(vec![message(&format!("w{n}"))], None)
                .await;
            if got.is_ok_and(|g| g.copies >= 3) {
                return;
            }
            n += 1;
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("the copy groups reach three");
    for n in 0..30 {
        let got = b
            .handle
            .publish(vec![message(&format!("f{n}"))], None)
            .await
            .expect("publish");
        assert!(got.copies >= 3, "publish {n} confirmed at {}", got.copies);
    }
    finish([a, c]);
    b.engine.abort();
    drop(b.handle);
    drop(b.runner);
    drop(dirs);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_holder_added_to_an_old_segment_joins_the_isr_and_the_floor_rises() {
    let dirs = [tempdir(), tempdir(), tempdir()];
    let fourth = tempdir();
    let [a, b, c] = cluster(&dirs).await;
    let peers = [&a, &b, &c]
        .iter()
        .map(|m| (m.runner.id(), m.runner.addr().to_owned()))
        .collect();
    let d = member(peers, false, fourth.path()).await;
    let all: BTreeSet<u64> = [&a, &b, &c, &d].iter().map(|m| m.runner.id()).collect();
    timeout(BOUND, async {
        while !(a.runner.members() == all && d.runner.members() == all) {
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the fourth node joins");
    timeout(BOUND, async {
        while ![&a, &b, &c, &d].iter().all(|m| floor_is(m.runner.dir(), 3)) {
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("the floor reaches three");
    warm_up(&a.handle).await;
    let others = [b, c, d];
    timeout(BOUND, async {
        loop {
            let n = (0..3)
                .filter(|i| holds_all(&a.store, &others[*i].store))
                .count();
            if n >= 2 {
                return;
            }
            a.handle.publish(vec![message("fill")], None).await.ok();
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("two holders hold the segment");
    let held = (0..3)
        .find(|i| holds_all(&a.store, &others[*i].store))
        .unwrap();
    let mut others: Vec<Option<Member>> = others.into_iter().map(Some).collect();
    let lost = others[held].take().unwrap();
    lost.runner.stop().await;
    lost.engine.abort();
    drop(lost.handle);
    let start = Instant::now();
    let mut n = 0;
    let at = timeout(Duration::from_secs(90), async {
        loop {
            n += 1;
            let got = a
                .handle
                .publish(vec![message(&format!("r{n}"))], None)
                .await;
            if got.is_ok_and(|g| g.copies >= 3) {
                return start.elapsed();
            }
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("the new holder joins the ISR and the publishes confirm at three");
    println!("confirmed at three after {at:?}");
    a.engine.abort();
    for m in others.into_iter().flatten() {
        m.engine.abort();
    }
    drop(dirs);
    drop(fourth);
    std::fs::remove_dir(root()).ok();
}
