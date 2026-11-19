use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tempfile::{TempDir, tempdir_in};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use verdandi::runner::{Runner, own_id};
use verdandi_http::Node;
use verdandi_store::engine::{Engine, OsRandom, WallClock};
use verdandi_store::fs::RealFs;

const KEY: &[u8] = b"cluster key";
const BOUND: Duration = Duration::from_secs(120);
const IPS: [&str; 3] = ["127.0.0.1", "127.0.0.2", "127.0.0.3"];

struct Member {
    runner: Runner,
    engine: JoinHandle<Result<(), verdandi_store::engine::Error>>,
    http: JoinHandle<std::io::Result<()>>,
    url: String,
}

fn tempdir() -> TempDir {
    let root: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/c3t7-dirs");
    std::fs::create_dir_all(&root).unwrap();
    tempdir_in(root).unwrap()
}

async fn member(ip: &str, peers: Vec<(u64, String)>, found: bool, dir: &Path) -> Member {
    let me = own_id(dir).unwrap();
    let store = dir.join("store");
    std::fs::create_dir_all(&store).unwrap();
    let (engine, handle) =
        Engine::open_as(RealFs, &store, Arc::new(WallClock), Box::new(OsRandom), me)
            .await
            .unwrap();
    let engine = tokio::spawn(engine.run());
    let runner = Runner::launch(
        KEY,
        &format!("{ip}:7946"),
        peers,
        found,
        dir.into(),
        None,
        Some(handle.clone()),
    )
    .await
    .unwrap();
    let node = Node::default();
    node.set(handle).ok().unwrap();
    let listener = TcpListener::bind(format!("{ip}:8080")).await.unwrap();
    let http = tokio::spawn(verdandi_http::serve_with(
        listener,
        node,
        Some(Arc::new(runner.cluster(KEY))),
        std::future::pending(),
    ));
    Member {
        runner,
        engine,
        http,
        url: format!("http://{ip}:8080"),
    }
}

async fn cluster(dirs: &[TempDir; 3]) -> [Member; 3] {
    let a = member(IPS[0], Vec::new(), true, dirs[0].path()).await;
    let pa = (a.runner.id(), a.runner.addr().to_owned());
    let b = member(IPS[1], vec![pa.clone()], false, dirs[1].path()).await;
    let pb = (b.runner.id(), b.runner.addr().to_owned());
    let c = member(IPS[2], vec![pa, pb], false, dirs[2].path()).await;
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
    [a, b, c]
}

async fn take(client: &Client, url: &str) -> Option<Value> {
    let reply = client
        .get(format!("{url}/s/s?group=g&limit=100&lease=2"))
        .send()
        .await
        .unwrap();
    match reply.status() {
        StatusCode::OK => Some(serde_json::from_slice(&reply.bytes().await.unwrap()).unwrap()),
        StatusCode::NO_CONTENT => None,
        other => panic!("take gave {other}"),
    }
}

fn values(reply: &Value) -> BTreeSet<u64> {
    reply["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["value"]["n"].as_u64().unwrap())
        .collect()
}

async fn publish(client: &Client, url: &str, n: u64) -> StatusCode {
    client
        .post(format!("{url}/s/s"))
        .header("content-type", "application/json")
        .header("idempotency-key", format!("n{n}"))
        .body(json!({ "n": n }).to_string())
        .send()
        .await
        .unwrap()
        .status()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn http_routes_across_three_nodes_and_a_lone_node_refuses_a_publish() {
    if TcpListener::bind("0.0.0.0:8080").await.is_err() {
        panic!("ABORTED port 8080 is busy");
    }
    let dirs = [tempdir(), tempdir(), tempdir()];
    let [a, b, c] = cluster(&dirs).await;
    let client = Client::new();
    assert!(take(&client, &a.url).await.is_none());
    timeout(BOUND, async {
        while !matches!(
            publish(&client, &a.url, 0).await,
            StatusCode::CREATED | StatusCode::OK
        ) {
            sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("the first publish succeeds");
    for n in 1..5 {
        assert_eq!(publish(&client, &a.url, n).await, StatusCode::CREATED);
    }

    let wanted: BTreeSet<u64> = (0..5).collect();
    let got = timeout(BOUND, async {
        loop {
            if let Some(reply) = take(&client, &c.url).await {
                return reply;
            }
            sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("a take at C gets the messages from the node that has work");
    assert_eq!(values(&got), wanted);
    let receipt = got["receipt"].as_str().unwrap().to_owned();
    let first = got["messages"][0]["id"].as_str().unwrap().to_owned();
    let acked = got["messages"][0]["value"]["n"].as_u64().unwrap();

    let settled = client
        .post(format!("{}/r/{receipt}", b.url))
        .header("content-type", "application/json")
        .body(json!({ "ack": [first] }).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(settled.status(), StatusCode::OK);

    sleep(Duration::from_secs(4)).await;
    let again = timeout(BOUND, async {
        loop {
            if let Some(reply) = take(&client, &c.url).await {
                return reply;
            }
            sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("the unsettled messages come again");
    let mut left = wanted.clone();
    left.remove(&acked);
    assert_eq!(values(&again), left);

    let read = client
        .get(format!("{}/s/s?limit=100", c.url))
        .send()
        .await
        .unwrap();
    assert!(read.headers().get("x-vd-partial").is_none());
    let read: Value = serde_json::from_slice(&read.bytes().await.unwrap()).unwrap();
    assert_eq!(values(&read), wanted);

    let [b_stop, c_stop] = [b, c];
    for m in [&b_stop, &c_stop] {
        m.http.abort();
    }
    for m in [b_stop, c_stop] {
        m.runner.stop().await;
        m.engine.abort();
    }
    let started = Instant::now();
    assert_eq!(
        publish(&client, &a.url, 99).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(started.elapsed() < Duration::from_secs(30));
    a.http.abort();
    a.engine.abort();
    drop(dirs);
    std::fs::remove_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/c3t7-dirs")).ok();
}
