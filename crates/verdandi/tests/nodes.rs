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
    let root: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/c4t3-dirs");
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
async fn the_node_routes_need_the_key_and_a_drained_node_leaves_after_its_copies_move() {
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
    timeout(BOUND, async {
        loop {
            let sent = client
                .post(format!("{url}/s/s"))
                .header("content-type", "application/json")
                .body(json!({ "n": 0 }).to_string());
            if status(&client, sent).await.0 == StatusCode::CREATED {
                return;
            }
            sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("the first publish succeeds");
    let key = String::from_utf8(KEY.to_vec()).unwrap();
    let call = |method: reqwest::Method, path: String, token: Option<&str>| {
        let mut request = client.request(method, format!("{url}{path}"));
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        status(&client, request)
    };
    let get = reqwest::Method::GET;
    let post = reqwest::Method::POST;
    let id_a = a.id();
    let id_c = timeout(BOUND, async {
        loop {
            for id in [b.id(), c.id()] {
                let (_, body) = call(get.clone(), format!("/n/{id}"), Some(&key)).await;
                let info: Value = serde_json::from_str(&body).unwrap_or_default();
                if info["segments"].as_u64().unwrap_or(0) > 0 {
                    return id;
                }
            }
            sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("a segment names a second holder");
    for token in [None, Some("wrong")] {
        let (code, _) = call(get.clone(), format!("/n/{id_c}"), token).await;
        assert_eq!(code, StatusCode::UNAUTHORIZED);
        let (code, _) = call(post.clone(), format!("/n/{id_c}/drain"), token).await;
        assert_eq!(code, StatusCode::UNAUTHORIZED);
    }
    let (code, body) = call(get.clone(), format!("/n/{id_c}"), Some(&key)).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    let before: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(before["member"], true);
    assert_eq!(before["live"], true);
    let held = before["segments"].as_u64().unwrap();
    assert!(held > 0, "{body}");
    let (code, _) = call(
        post.clone(),
        format!("/n/{id_c}/accept-loss?confirm={id_c}"),
        Some(&key),
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT);
    let (code, _) = call(
        post.clone(),
        format!("/n/{id_c}/accept-loss?confirm={id_a}"),
        Some(&key),
    )
    .await;
    assert_eq!(code, StatusCode::BAD_REQUEST);
    let (code, body) = call(post.clone(), format!("/n/{id_c}/drain"), Some(&key)).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    timeout(BOUND, async {
        while a.members().contains(&id_c) {
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("the drained node leaves the members");
    let (_, body) = call(get, format!("/n/{id_a}"), Some(&key)).await;
    let after: Value = serde_json::from_str(&body).unwrap();
    assert!(after["segments"].as_u64().unwrap() >= held, "{body}");
}
