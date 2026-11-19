use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tempfile::{TempDir, tempdir_in};
use tokio::net::TcpListener;
use tokio::time::{sleep, timeout};
use verdandi::runner::{Runner, own_id};
use verdandi_http::Node;
use verdandi_store::engine::{Engine, OsRandom, WallClock};
use verdandi_store::fs::RealFs;

const KEY: &[u8] = b"cluster key";
const BOUND: Duration = Duration::from_secs(120);
const IPS: [&str; 3] = ["127.0.0.1", "127.0.0.2", "127.0.0.3"];

fn tempdir() -> TempDir {
    let root: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/c4t1-dirs");
    std::fs::create_dir_all(&root).unwrap();
    tempdir_in(root).unwrap()
}

async fn member(ip: &str, peers: Vec<(u64, String)>, found: bool, dir: &Path) -> Runner {
    let me = own_id(dir).unwrap();
    let store = dir.join("store");
    std::fs::create_dir_all(&store).unwrap();
    let (engine, handle) =
        Engine::open_as(RealFs, &store, Arc::new(WallClock), Box::new(OsRandom), me)
            .await
            .unwrap();
    tokio::spawn(engine.run());
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
    tokio::spawn(verdandi_http::serve_with(
        listener,
        node,
        Some(Arc::new(runner.cluster(KEY))),
        std::future::pending(),
    ));
    runner
}

async fn status(client: &Client, request: reqwest::RequestBuilder) -> (StatusCode, String) {
    let _ = client;
    let reply = request.send().await.unwrap();
    (reply.status(), reply.text().await.unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_stopping_node_is_not_ready_refuses_publishes_and_still_settles() {
    if TcpListener::bind("0.0.0.0:8080").await.is_err() {
        panic!("ABORTED port 8080 is busy");
    }
    let dirs = [tempdir(), tempdir(), tempdir()];
    let a = member(IPS[0], Vec::new(), true, dirs[0].path()).await;
    let pa = (a.id(), a.addr().to_owned());
    let b = member(IPS[1], vec![pa.clone()], false, dirs[1].path()).await;
    let pb = (b.id(), b.addr().to_owned());
    let c = member(IPS[2], vec![pa, pb], false, dirs[2].path()).await;
    let all: BTreeSet<u64> = [a.id(), b.id(), c.id()].into();
    timeout(BOUND, async {
        while ![&a, &b, &c]
            .iter()
            .all(|r| r.members() == all && r.leader().is_some())
        {
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the cluster forms");
    let client = Client::new();
    let url = format!("http://{}:8080", IPS[0]);
    let publish = |n: u64| {
        status(
            &client,
            client
                .post(format!("{url}/s/s"))
                .header("content-type", "application/json")
                .body(json!({ "n": n }).to_string()),
        )
    };
    timeout(BOUND, async {
        while publish(0).await.0 != StatusCode::CREATED {
            sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("the first publish succeeds");
    let ready = || status(&client, client.get(format!("{url}/readyz")));
    timeout(BOUND, async {
        while ready().await.0 != StatusCode::OK {
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("the node is ready");
    let (code, body) = timeout(BOUND, async {
        loop {
            let got = status(
                &client,
                client.get(format!("{url}/s/s?group=g&limit=10&lease=60")),
            )
            .await;
            if got.0 == StatusCode::OK {
                return got;
            }
            sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("a take");
    assert_eq!(code, StatusCode::OK);
    let taken: Value = serde_json::from_str(&body).unwrap();
    let receipt = taken["receipt"].as_str().unwrap().to_owned();
    let id = taken["messages"][0]["id"].as_str().unwrap().to_owned();

    let shutting = a.shutdown();
    tokio::pin!(shutting);
    tokio::select! {
        biased;
        () = &mut shutting => panic!("the node left before the checks"),
        () = async {
            sleep(Duration::from_millis(300)).await;
            let (code, body) = publish(1).await;
            assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE, "{body}");
            assert!(body.contains("stopping"), "{body}");
            assert_eq!(ready().await.0, StatusCode::SERVICE_UNAVAILABLE);
            let settled = client
                .post(format!("{url}/r/{receipt}"))
                .header("content-type", "application/json")
                .body(json!({ "ack": [id] }).to_string())
                .send()
                .await
                .unwrap();
            assert_eq!(settled.status(), StatusCode::OK);
        } => {}
    }
}
