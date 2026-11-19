use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use object_store::ObjectStoreExt;
use tempfile::{TempDir, tempdir_in};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use verdandi::backup::{self, Backup, Source};
use verdandi::runner::{Runner, node_id};
use verdandi_core::Timing;
use verdandi_core::ids::MessageId;
use verdandi_core::ids::SegmentId;
use verdandi_core::lease::{Receipt, Settle};
use verdandi_core::retention::RETAIN_MS;
use verdandi_store::engine::{
    Engine, Error, Handle, NewMessage, OsRandom, SettleBy, Take, Taken, TestClock, WallClock,
};
use verdandi_store::fs::RealFs;

const KEY: &[u8] = b"cluster key";
const BOUND: Duration = Duration::from_secs(120);

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/c4t6a-dirs")
}

fn tempdir() -> TempDir {
    std::fs::create_dir_all(root()).unwrap();
    tempdir_in(root()).unwrap()
}

fn url(dir: &Path) -> String {
    format!("file://{}", dir.canonicalize().unwrap().display())
}

fn message(value: &str) -> NewMessage {
    NewMessage {
        stream: "s".to_owned(),
        headers: Vec::new(),
        value: value.as_bytes().to_vec(),
        timing: Timing::default(),
    }
}

async fn until<T>(what: &str, mut check: impl AsyncFnMut() -> Option<T>) -> T {
    timeout(BOUND, async {
        loop {
            if let Some(found) = check().await {
                return found;
            }
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out: {what}"))
}

async fn object(backup: &Backup, key: &object_store::path::Path) -> Option<Vec<u8>> {
    let got = backup.store.get(key).await.ok()?;
    Some(got.bytes().await.ok()?.to_vec())
}

async fn names(backup: &Backup, dir: &str) -> Vec<String> {
    let base = backup.cluster_key().parent().unwrap().join(dir);
    let listing = backup.store.list_with_delimiter(Some(&base)).await.unwrap();
    listing
        .objects
        .iter()
        .filter_map(|o| o.location.filename().map(str::to_owned))
        .collect()
}

async fn start(dir: &Path, clock: &TestClock) -> (Handle, JoinHandle<Result<(), Error>>) {
    std::fs::create_dir_all(dir).unwrap();
    let (engine, handle) = Engine::open(RealFs, dir, Arc::new(clock.clone()), Box::new(OsRandom))
        .await
        .unwrap();
    (handle, tokio::spawn(engine.run()))
}

async fn lease(handle: &Handle, group: &str, limit: usize) -> (Option<Receipt>, Vec<MessageId>) {
    let take = Take {
        group: group.to_owned(),
        selector: "s".to_owned(),
        limit,
        lease_ms: 3_600_000,
        wait: false,
    };
    let Ok(Taken::Leased {
        receipt, messages, ..
    }) = handle.take(take).await
    else {
        return (None, Vec::new());
    };
    let mut ids: Vec<MessageId> = messages.iter().map(|d| d.handed.id).collect();
    ids.sort();
    (Some(receipt), ids)
}

async fn take_all(handle: &Handle, group: &str, limit: usize) -> Vec<MessageId> {
    lease(handle, group, limit).await.1
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rehydration_takes_the_longest_copy_of_a_segment() {
    let work = tempdir();
    let bucket = work.path().join("bucket");
    std::fs::create_dir_all(&bucket).unwrap();
    let clock = TestClock::new(1_000);
    let (handle, engine) = start(&work.path().join("a"), &clock).await;
    handle.publish(vec![message("one")], None).await.unwrap();
    handle.publish(vec![message("two")], None).await.unwrap();
    clock.advance(31_000);
    let id = until("the segment seals", async || {
        handle.sealed().await.unwrap().first().copied()
    })
    .await;
    let all = take_all(&handle, "probe", 10).await;
    assert_eq!(all.len(), 2);
    let long = std::fs::read(work.path().join("a").join(format!("seg-{id}.vdl"))).unwrap();
    let mut size = [0u8; 4];
    size.copy_from_slice(&long[4096 + 46..4096 + 50]);
    let short = &long[..4096 + 54 + u32::from_le_bytes(size) as usize];
    let backup = Backup::open(&url(&bucket), KEY).unwrap();
    for bytes in [short.to_vec(), long.clone()] {
        let key = backup.segment_key(id, bytes.len() as u64);
        backup.store.put(&key, bytes.into()).await.unwrap();
    }
    backup
        .store
        .put(&backup.cluster_key(), vec![0u8].into())
        .await
        .unwrap();
    engine.abort();
    let (fresh, engine) = start(&work.path().join("b"), &clock).await;
    assert!(backup::rehydrate(&backup, &fresh).await.unwrap());
    assert_eq!(take_all(&fresh, "g", 10).await, all);
    engine.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wiped_node_gets_its_unsettled_messages_back() {
    let work = tempdir();
    let bucket = work.path().join("bucket");
    std::fs::create_dir_all(&bucket).unwrap();
    let clock = TestClock::new(1_000);
    let dir = work.path().join("a");
    let (handle, engine) = start(&dir, &clock).await;
    let published = vec![message("one"), message("two"), message("three")];
    handle.publish(published, None).await.unwrap();
    let all = take_all(&handle, "probe", 10).await;
    assert_eq!(all.len(), 3);
    let backup = Backup::open(&url(&bucket), KEY).unwrap();
    tokio::spawn(backup::run(
        backup.clone(),
        Source::alone(handle.clone(), dir.clone()),
    ));
    let before = until("the first backup", async || {
        clock.advance(31_000);
        let seg = !names(&backup, "seg").await.is_empty();
        let cluster = object(&backup, &backup.cluster_key()).await.is_some();
        let ctl = object(&backup, &backup.control_key(0)).await;
        if seg && cluster { ctl } else { None }
    })
    .await;
    let (receipt, first) = lease(&handle, "g", 1).await;
    let settle = Settle {
        ack: first.clone(),
        ..Settle::default()
    };
    handle
        .settle(receipt.unwrap(), SettleBy::Ids(settle))
        .await
        .unwrap();
    until("the settle reaches the backup", async || {
        let now = object(&backup, &backup.control_key(0)).await?;
        (now != before).then_some(())
    })
    .await;
    engine.abort();
    drop(handle);
    std::fs::remove_dir_all(&dir).unwrap();
    let (fresh, engine) = start(&dir, &clock).await;
    let other = Backup::open(&url(&bucket), b"other key").unwrap();
    assert!(!backup::rehydrate(&other, &fresh).await.unwrap());
    assert!(backup::rehydrate(&backup, &fresh).await.unwrap());
    let want: Vec<MessageId> = all.into_iter().filter(|id| !first.contains(id)).collect();
    assert_eq!(want.len(), 2);
    assert_eq!(take_all(&fresh, "g", 10).await, want);
    engine.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_node_uploads_a_sealed_segment_and_its_control_snapshot() {
    let work = tempdir();
    let store = work.path().join("store");
    let bucket = work.path().join("bucket");
    std::fs::create_dir_all(&store).unwrap();
    std::fs::create_dir_all(&bucket).unwrap();
    let clock = TestClock::new(1_000);
    let (engine, handle) =
        Engine::open(RealFs, &store, Arc::new(clock.clone()), Box::new(OsRandom))
            .await
            .unwrap();
    let engine = tokio::spawn(engine.run());
    handle.publish(vec![message("one")], None).await.unwrap();
    let backup = Backup::open(&url(&bucket), b"").unwrap();
    tokio::spawn(backup::run(
        backup.clone(),
        Source::alone(handle.clone(), store.clone()),
    ));
    sleep(Duration::from_millis(2500)).await;
    let open = handle.segments().await.unwrap();
    assert_eq!(open.len(), 1);
    assert!(handle.sealed().await.unwrap().is_empty());
    assert!(
        object(&backup, &backup.segment_key(open[0], 0))
            .await
            .is_none()
    );
    clock.advance(31_000);
    let id = until("the segment seals", async || {
        handle.sealed().await.unwrap().first().copied()
    })
    .await;
    let want = std::fs::read(store.join(format!("seg-{id}.vdl"))).unwrap();
    let got = until("the segment object", async || {
        object(&backup, &backup.segment_key(id, want.len() as u64)).await
    })
    .await;
    assert_eq!(got, want);
    until("the control object", async || {
        object(&backup, &backup.control_key(0)).await
    })
    .await;
    assert_eq!(backup.uploaded(), BTreeSet::from([id]));
    drop(handle);
    engine.abort();
}

struct Member {
    runner: Runner,
    handle: Handle,
    engine: JoinHandle<Result<(), Error>>,
    store: PathBuf,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn in_a_cluster_exactly_one_holder_uploads_each_sealed_segment() {
    let dirs = [tempdir(), tempdir(), tempdir(), tempdir()];
    let bucket = dirs[3].path().join("bucket");
    std::fs::create_dir_all(&bucket).unwrap();
    let a = member(Vec::new(), true, dirs[0].path()).await;
    let pa = (a.runner.id(), a.runner.addr().to_owned());
    let b = member(vec![pa.clone()], false, dirs[1].path()).await;
    let pb = (b.runner.id(), b.runner.addr().to_owned());
    let c = member(vec![pa, pb], false, dirs[2].path()).await;
    let all: BTreeSet<u64> = [a.runner.id(), b.runner.id(), c.runner.id()].into();
    until("the cluster forms", async || {
        [&a, &b, &c]
            .iter()
            .all(|m| m.runner.members() == all && m.runner.leader().is_some())
            .then_some(())
    })
    .await;
    let members = [a, b, c];
    for m in &members {
        until("a publish succeeds", async || {
            m.handle.publish(vec![message("x")], None).await.ok()
        })
        .await;
    }
    let backups: Vec<Backup> = members
        .iter()
        .map(|m| {
            let backup = Backup::open(&url(&bucket), KEY).unwrap();
            m.runner
                .backup(backup.clone(), m.handle.clone(), m.store.clone());
            backup
        })
        .collect();
    let sealed = until("every copy seals and uploads", async || {
        let mut sealed = BTreeSet::new();
        for m in &members {
            sealed.extend(m.handle.sealed().await.unwrap());
        }
        let sent: BTreeSet<SegmentId> = backups.iter().flat_map(Backup::uploaded).collect();
        (sealed.len() >= 3 && sent == sealed).then_some(sealed)
    })
    .await;
    sleep(Duration::from_secs(5)).await;
    let counts: usize = backups.iter().map(|b| b.uploaded().len()).sum();
    assert_eq!(counts, sealed.len(), "a segment went up twice");
    for id in &sealed {
        let names = names(&backups[0], "seg").await;
        assert!(
            names.iter().any(|n| n.starts_with(&id.to_string())),
            "{id} is missing"
        );
    }
    for m in &members {
        until("the control object", async || {
            object(&backups[0], &backups[0].control_key(m.runner.id())).await
        })
        .await;
    }
    until("the cluster record", async || {
        let bytes = object(&backups[0], &backups[0].cluster_key()).await?;
        let record: backup::ClusterRecord = postcard::from_bytes(&bytes).ok()?;
        (record.members.iter().copied().collect::<BTreeSet<_>>() == all).then_some(())
    })
    .await;
    for m in members {
        m.engine.abort();
        drop(m.handle);
        drop(m.runner);
    }
    std::fs::remove_dir(root()).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_waits_for_the_upload_of_a_segment() {
    let work = tempdir();
    let bucket = work.path().join("bucket");
    std::fs::create_dir_all(&bucket).unwrap();
    let clock = TestClock::new(1_000);
    let dir = work.path().join("a");
    let (handle, engine) = start(&dir, &clock).await;
    handle.stored(BTreeSet::new()).await.unwrap();
    handle
        .publish(vec![message("one"), message("two")], None)
        .await
        .unwrap();
    let (receipt, ids) = lease(&handle, "g", 10).await;
    let settle = Settle {
        ack: ids,
        ..Settle::default()
    };
    handle
        .settle(receipt.unwrap(), SettleBy::Ids(settle))
        .await
        .unwrap();
    clock.advance(31_000);
    let id = until("the segment seals", async || {
        handle.sealed().await.unwrap().first().copied()
    })
    .await;
    let want = std::fs::read(dir.join(format!("seg-{id}.vdl"))).unwrap();
    clock.advance(RETAIN_MS);
    for _ in 0..5 {
        handle.compact().await.unwrap();
        sleep(Duration::from_millis(200)).await;
    }
    assert!(dir.join(format!("seg-{id}.vdl")).exists());
    let backup = Backup::open(&url(&bucket), KEY).unwrap();
    tokio::spawn(backup::run(
        backup.clone(),
        Source::alone(handle.clone(), dir.clone()),
    ));
    let got = until("the segment object", async || {
        object(&backup, &backup.segment_key(id, want.len() as u64)).await
    })
    .await;
    assert_eq!(got, want);
    until("compaction runs after the upload", async || {
        handle.compact().await.unwrap();
        (!dir.join(format!("seg-{id}.vdl")).exists()).then_some(())
    })
    .await;
    engine.abort();
}

fn seg_files(store: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(store)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with("seg-"))
        })
        .collect()
}

async fn restart(peers: Vec<(u64, String)>, found: bool, dir: &Path, backup: &Backup) -> Member {
    let m = member(peers, found, dir).await;
    m.runner
        .backup(backup.clone(), m.handle.clone(), m.store.clone());
    m
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_holder_that_lost_its_segment_fetches_it_from_the_backup() {
    let dirs = [tempdir(), tempdir(), tempdir(), tempdir()];
    let bucket = dirs[3].path().join("bucket");
    std::fs::create_dir_all(&bucket).unwrap();
    let backup = Backup::open(&url(&bucket), KEY).unwrap();
    let a = member(Vec::new(), true, dirs[0].path()).await;
    let pa = (a.runner.id(), a.runner.addr().to_owned());
    let b = member(vec![pa.clone()], false, dirs[1].path()).await;
    let pb = (b.runner.id(), b.runner.addr().to_owned());
    let c = member(vec![pa, pb], false, dirs[2].path()).await;
    let all: BTreeSet<u64> = [a.runner.id(), b.runner.id(), c.runner.id()].into();
    until("the cluster forms", async || {
        [&a, &b, &c]
            .iter()
            .all(|m| m.runner.members() == all && m.runner.leader().is_some())
            .then_some(())
    })
    .await;
    let wanted: BTreeSet<String> = (0..4).map(|n| format!("m{n}")).collect();
    for value in &wanted {
        until("a publish succeeds", async || {
            a.handle.publish(vec![message(value)], None).await.ok()
        })
        .await;
    }
    let members = [a, b, c];
    for m in &members {
        m.runner
            .backup(backup.clone(), m.handle.clone(), m.store.clone());
    }
    until("the segment is in the backup", async || {
        let held = members.iter().flat_map(|m| seg_files(&m.store)).count();
        let stored = names(&backup, "seg").await;
        (held >= 2 && !stored.is_empty()).then_some(())
    })
    .await;
    let ids = members.each_ref().map(|m| m.runner.id());
    for m in members {
        m.runner.stop().await;
        m.engine.abort();
        let _ = m.engine.await;
    }
    for dir in &dirs[..3] {
        for file in seg_files(&dir.path().join("store")) {
            std::fs::remove_file(file).unwrap();
        }
    }
    let a = restart(Vec::new(), false, dirs[0].path(), &backup).await;
    let pa = (a.runner.id(), a.runner.addr().to_owned());
    let b = restart(vec![pa.clone()], false, dirs[1].path(), &backup).await;
    let pb = (b.runner.id(), b.runner.addr().to_owned());
    let c = restart(vec![pa, pb], false, dirs[2].path(), &backup).await;
    assert_eq!([a.runner.id(), b.runner.id(), c.runner.id()], ids);
    let mut found = BTreeSet::new();
    until("the nodes serve the fetched messages", async || {
        for m in [&a, &b, &c] {
            let (_, got) = lease(&m.handle, "g", 10).await;
            found.extend(got);
        }
        (found.len() == wanted.len()).then_some(())
    })
    .await;
    assert!([&a, &b, &c].iter().any(|m| !seg_files(&m.store).is_empty()));
    for m in [a, b, c] {
        m.engine.abort();
        drop(m.handle);
        drop(m.runner);
    }
    std::fs::remove_dir(root()).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_object_gives_an_error_and_no_adopt() {
    let work = tempdir();
    let bucket = work.path().join("bucket");
    std::fs::create_dir_all(&bucket).unwrap();
    let clock = TestClock::new(1_000);
    let (handle, engine) = start(&work.path().join("a"), &clock).await;
    handle.publish(vec![message("one")], None).await.unwrap();
    clock.advance(31_000);
    let id = until("the segment seals", async || {
        handle.sealed().await.unwrap().first().copied()
    })
    .await;
    let long = std::fs::read(work.path().join("a").join(format!("seg-{id}.vdl"))).unwrap();
    let mut size = [0u8; 4];
    size.copy_from_slice(&long[4096 + 46..4096 + 50]);
    let cut = &long[..4096 + 54 + u32::from_le_bytes(size) as usize - 3];
    let backup = Backup::open(&url(&bucket), KEY).unwrap();
    let key = backup.segment_key(id, cut.len() as u64);
    backup.store.put(&key, cut.to_vec().into()).await.unwrap();
    backup
        .store
        .put(&backup.cluster_key(), vec![0u8].into())
        .await
        .unwrap();
    engine.abort();
    let (fresh, engine) = start(&work.path().join("b"), &clock).await;
    assert!(backup::rehydrate(&backup, &fresh).await.is_err());
    let adopted = Arc::new(AtomicBool::new(false));
    let mut source = Source::alone(fresh.clone(), work.path().join("b"));
    source.wanted = Box::new(move || BTreeSet::from([node_id(id)]));
    let flag = adopted.clone();
    source.fetched = Box::new(move |_| flag.store(true, Ordering::SeqCst));
    let task = tokio::spawn(backup::run(backup.clone(), source));
    sleep(Duration::from_secs(9)).await;
    task.abort();
    assert!(!adopted.load(Ordering::SeqCst));
    engine.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shorter_object_does_not_let_the_writer_compact() {
    let work = tempdir();
    let bucket = work.path().join("bucket");
    std::fs::create_dir_all(&bucket).unwrap();
    let clock = TestClock::new(1_000);
    let dir = work.path().join("a");
    let (handle, engine) = start(&dir, &clock).await;
    handle.stored(BTreeSet::new()).await.unwrap();
    handle
        .publish(vec![message("one"), message("two")], None)
        .await
        .unwrap();
    let (receipt, ids) = lease(&handle, "g", 10).await;
    let settle = Settle {
        ack: ids,
        ..Settle::default()
    };
    handle
        .settle(receipt.unwrap(), SettleBy::Ids(settle))
        .await
        .unwrap();
    clock.advance(31_000);
    let id = until("the segment seals", async || {
        handle.sealed().await.unwrap().first().copied()
    })
    .await;
    let want = std::fs::read(dir.join(format!("seg-{id}.vdl"))).unwrap();
    clock.advance(RETAIN_MS);
    let backup = Backup::open(&url(&bucket), KEY).unwrap();
    let short = want[..want.len() / 2].to_vec();
    let short_key = backup.segment_key(id, short.len() as u64);
    backup.store.put(&short_key, short.into()).await.unwrap();
    let mut source = Source::alone(handle.clone(), dir.clone());
    source.mine = Box::new(|_| false);
    tokio::spawn(backup::run(backup.clone(), source));
    sleep(Duration::from_secs(4)).await;
    for _ in 0..5 {
        handle.compact().await.unwrap();
        sleep(Duration::from_millis(200)).await;
    }
    assert!(dir.join(format!("seg-{id}.vdl")).exists());
    let full_key = backup.segment_key(id, want.len() as u64);
    backup.store.put(&full_key, want.into()).await.unwrap();
    until("compaction runs after the full object", async || {
        handle.compact().await.unwrap();
        (!dir.join(format!("seg-{id}.vdl")).exists()).then_some(())
    })
    .await;
    engine.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_with_control_state_does_not_rehydrate() {
    let work = tempdir();
    let bucket = work.path().join("bucket");
    std::fs::create_dir_all(&bucket).unwrap();
    let clock = TestClock::new(1_000);
    let dir = work.path().join("a");
    let (handle, engine) = start(&dir, &clock).await;
    let backup = Backup::open(&url(&bucket), KEY).unwrap();
    backup
        .store
        .put(&backup.cluster_key(), vec![0u8].into())
        .await
        .unwrap();
    std::fs::create_dir_all(dir.join("cluster")).unwrap();
    std::fs::write(dir.join("cluster").join("control"), b"x").unwrap();
    assert!(!backup::restore(&backup, &handle, &dir, true).await.unwrap());
    std::fs::remove_file(dir.join("cluster").join("control")).unwrap();
    assert!(backup::restore(&backup, &handle, &dir, true).await.unwrap());
    engine.abort();
}

#[tokio::test]
async fn a_restore_retries_five_times_and_then_gives_up() {
    let calls = std::sync::atomic::AtomicU32::new(0);
    let tick = Duration::from_millis(1);
    let failed: anyhow::Result<()> = backup::with_retry(5, tick, || async {
        calls.fetch_add(1, Ordering::SeqCst);
        anyhow::bail!("down")
    })
    .await;
    assert!(failed.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 5);
    let calls = std::sync::atomic::AtomicU32::new(0);
    let worked = backup::with_retry(5, tick, || async {
        if calls.fetch_add(1, Ordering::SeqCst) < 2 {
            anyhow::bail!("down")
        }
        Ok(true)
    })
    .await;
    assert!(worked.unwrap());
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}
