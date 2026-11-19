use std::collections::BTreeSet;
use std::time::Duration;

use tempfile::{TempDir, tempdir_in};
use tokio::time::{sleep, timeout};
use verdandi::runner::Runner;

const KEY: &[u8] = b"cluster key";
const BOUND: Duration = Duration::from_secs(60);

fn tempdir() -> TempDir {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/c3t2-dirs");
    std::fs::create_dir_all(&root).unwrap();
    tempdir_in(root).unwrap()
}

fn cleanup() {
    let _ = std::fs::remove_dir(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/c3t2-dirs"),
    );
}

async fn converge(runners: &[&Runner], all: &BTreeSet<u64>) {
    timeout(BOUND, async {
        loop {
            let leaders: BTreeSet<Option<u64>> = runners.iter().map(|r| r.leader()).collect();
            let one =
                leaders.len() == 1 && leaders.iter().all(|l| l.is_some_and(|l| all.contains(&l)));
            if one && runners.iter().all(|r| &r.members() == all) {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "no convergence: {:?}",
            runners
                .iter()
                .map(|r| (r.leader(), r.members()))
                .collect::<Vec<_>>()
        )
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_runners_elect_one_leader_and_join() {
    let dirs = [tempdir(), tempdir(), tempdir()];
    let a = Runner::start(KEY, "127.0.0.1:0", Vec::new(), true, dirs[0].path().into())
        .await
        .unwrap();
    let pa = (a.id(), a.addr().to_owned());
    let b = Runner::start(
        KEY,
        "127.0.0.1:0",
        vec![pa.clone()],
        false,
        dirs[1].path().into(),
    )
    .await
    .unwrap();
    let pb = (b.id(), b.addr().to_owned());
    let c = Runner::start(
        KEY,
        "127.0.0.1:0",
        vec![pa, pb],
        false,
        dirs[2].path().into(),
    )
    .await
    .unwrap();
    let all: BTreeSet<u64> = [a.id(), b.id(), c.id()].into();
    converge(&[&a, &b, &c], &all).await;
    drop((a, b, c));
    drop(dirs);
    cleanup();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_runner_keeps_its_id_and_state_and_rejoins() {
    let dirs = [tempdir(), tempdir(), tempdir()];
    let a = Runner::start(KEY, "127.0.0.1:0", Vec::new(), true, dirs[0].path().into())
        .await
        .unwrap();
    let pa = (a.id(), a.addr().to_owned());
    let b = Runner::start(
        KEY,
        "127.0.0.1:0",
        vec![pa.clone()],
        false,
        dirs[1].path().into(),
    )
    .await
    .unwrap();
    let pb = (b.id(), b.addr().to_owned());
    let c = Runner::start(
        KEY,
        "127.0.0.1:0",
        vec![pa.clone(), pb.clone()],
        false,
        dirs[2].path().into(),
    )
    .await
    .unwrap();
    let all: BTreeSet<u64> = [a.id(), b.id(), c.id()].into();
    converge(&[&a, &b, &c], &all).await;
    let (id, applied) = (c.id(), c.applied());
    assert!(applied > 0);
    c.stop().await;
    let c = Runner::start(
        KEY,
        "127.0.0.1:0",
        vec![pa, pb],
        true,
        dirs[2].path().into(),
    )
    .await
    .unwrap();
    assert_eq!(c.id(), id);
    assert!(c.applied() >= applied);
    assert_eq!(c.members(), all);
    converge(&[&a, &b, &c], &all).await;
    let leaders = [&a, &b, &c]
        .iter()
        .filter(|r| r.leader() == Some(r.id()))
        .count();
    assert_eq!(leaders, 1);
    drop((a, b, c));
    drop(dirs);
    cleanup();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fresh_dir_gets_a_new_id_and_an_old_dir_keeps_its_own() {
    let (one, two) = (tempdir(), tempdir());
    let a = Runner::start(KEY, "127.0.0.1:0", Vec::new(), true, one.path().into())
        .await
        .unwrap();
    let id = a.id();
    a.stop().await;
    let b = Runner::start(KEY, "127.0.0.1:0", Vec::new(), true, one.path().into())
        .await
        .unwrap();
    let c = Runner::start(KEY, "127.0.0.1:0", Vec::new(), true, two.path().into())
        .await
        .unwrap();
    assert_eq!(b.id(), id);
    assert_ne!(c.id(), id);
    drop((b, c, one, two));
    cleanup();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_truncated_state_file_stops_the_start() {
    let dir = tempdir();
    let a = Runner::start(KEY, "127.0.0.1:0", Vec::new(), true, dir.path().into())
        .await
        .unwrap();
    a.stop().await;
    let file = dir.path().join("cluster/control");
    let bytes = std::fs::read(&file).unwrap();
    assert!(bytes.len() > 2);
    std::fs::write(&file, &bytes[..bytes.len() / 2]).unwrap();
    let again = Runner::start(KEY, "127.0.0.1:0", Vec::new(), true, dir.path().into()).await;
    assert!(again.is_err());
    drop(dir);
    cleanup();
}
