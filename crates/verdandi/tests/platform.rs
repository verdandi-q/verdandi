use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::Duration;

use tempfile::{TempDir, tempdir_in};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use verdandi::runner::Runner;
use verdandi_core::Timing;
use verdandi_store::engine::{Engine, Error, Handle, NewMessage, OsRandom, Take, Taken, WallClock};
use verdandi_store::fs::RealFs;

const KEY: &[u8] = b"cluster key";
const BOUND: Duration = Duration::from_secs(60);

struct Member {
    runner: Runner,
    handle: Handle,
    engine: JoinHandle<Result<(), Error>>,
}

fn tempdir() -> TempDir {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/c4t2-dirs");
    std::fs::create_dir_all(&root).unwrap();
    tempdir_in(root).unwrap()
}

fn fake(spot: Arc<AtomicBool>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut buf = [0u8; 2048];
            let n = stream.read(&mut buf).unwrap_or(0);
            let head = String::from_utf8_lossy(&buf[..n]).into_owned();
            let line = head.lines().next().unwrap_or("");
            let body = if line.starts_with("PUT /latest/api/token") {
                Some("tok")
            } else if !head.contains("X-aws-ec2-metadata-token: tok")
                && !head.contains("x-aws-ec2-metadata-token: tok")
            {
                None
            } else if line.contains("placement/availability-zone") {
                Some("eu-west-1a")
            } else if line.contains("/instance-type") {
                Some("m7g.large")
            } else if line.contains("/instance-life-cycle") {
                Some("spot")
            } else if line.contains("spot/instance-action") && spot.load(Relaxed) {
                Some("{\"action\":\"terminate\"}")
            } else if line.contains("target-lifecycle-state") {
                Some("InService")
            } else {
                None
            };
            let reply = match body {
                Some(b) => format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{b}",
                    b.len()
                ),
                None => "HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                    .to_owned(),
            };
            let _ = stream.write_all(reply.as_bytes());
        }
    });
    url
}

async fn member(peers: Vec<(u64, String)>, found: bool, dir: &Path, meta: &str) -> Member {
    let store = dir.join("store");
    std::fs::create_dir_all(&store).unwrap();
    let (engine, handle) = Engine::open(RealFs, &store, Arc::new(WallClock), Box::new(OsRandom))
        .await
        .unwrap();
    let engine = tokio::spawn(engine.run());
    let runner = Runner::launch_with(
        KEY,
        "127.0.0.1:0",
        peers,
        found,
        dir.into(),
        None,
        Some(handle.clone()),
        meta,
        verdandi_store::block::VERSION,
    )
    .await
    .unwrap();
    Member {
        runner,
        handle,
        engine,
    }
}

fn message(value: &str) -> NewMessage {
    NewMessage {
        stream: "s".to_owned(),
        headers: Vec::new(),
        value: value.as_bytes().to_vec(),
        timing: Timing::default(),
    }
}

async fn take(handle: &Handle) -> bool {
    let take = Take {
        group: "g".to_owned(),
        selector: "s".to_owned(),
        limit: 1000,
        lease_ms: 600_000,
        wait: false,
    };
    matches!(handle.take(take).await, Ok(Taken::Leased { .. }))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_spot_notice_starts_the_copy_away_while_the_node_serves() {
    let spot = Arc::new(AtomicBool::new(false));
    let url = fake(spot.clone());
    let dirs = [tempdir(), tempdir(), tempdir()];
    let off = "http://127.0.0.1:1";
    let a = member(Vec::new(), true, dirs[0].path(), &url).await;
    let pa = (a.runner.id(), a.runner.addr().to_owned());
    let b = member(vec![pa.clone()], false, dirs[1].path(), off).await;
    let pb = (b.runner.id(), b.runner.addr().to_owned());
    let c = member(vec![pa, pb], false, dirs[2].path(), off).await;
    let all: BTreeSet<u64> = [a.runner.id(), b.runner.id(), c.runner.id()].into();
    timeout(BOUND, async {
        while ![&a, &b, &c]
            .iter()
            .all(|m| m.runner.members() == all && m.runner.leader().is_some())
        {
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the cluster forms");
    timeout(BOUND, async {
        while a.handle.publish(vec![message("warm")], None).await.is_err() {
            sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("the first publish succeeds");
    for n in 0..5 {
        a.handle
            .publish(vec![message(&format!("m{n}"))], None)
            .await
            .expect("two durable copies");
    }
    timeout(BOUND, async {
        while !b.runner.leaders().contains(&a.runner.id()) {
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("the writer leads the segment");
    sleep(Duration::from_secs(10)).await;
    assert!(b.runner.leaders().contains(&a.runner.id()));
    spot.store(true, Relaxed);
    timeout(BOUND, async {
        while b.runner.leaders().contains(&a.runner.id()) {
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("the segment leader moves away from the signalled node");
    assert!(take(&a.handle).await, "the node still serves a take");
    for m in [a, b, c] {
        m.engine.abort();
        drop(m.handle);
        drop(m.runner);
    }
    drop(dirs);
}
