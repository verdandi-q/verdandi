// allow: each test target uses a part of the shared kit
#[allow(dead_code)]
mod kit;

use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, OnceLock};

use kit::{START_MS, call, listen, scratch};
use reqwest::{Client, StatusCode};
use verdandi_http::{Cluster, Health};
use verdandi_store::engine::{Engine, OsRandom, TestClock};
use verdandi_store::fs::RealFs;

#[tokio::test]
async fn readyz_needs_every_held_segment_and_no_stop_and_a_stop_refuses_publishes_only() {
    let dir = scratch();
    let held = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let (h, s) = (held.clone(), stop.clone());
    let cluster = Cluster::new(
        b"key".to_vec(),
        Box::new(|_| Vec::new()),
        Box::new(|| "{}".into()),
    )
    .with_health(Health {
        held: Box::new(move || h.load(SeqCst)),
        stopping: Box::new(move || s.load(SeqCst)),
    });
    let node = Arc::new(OnceLock::new());
    let (listener, url) = listen().await;
    tokio::spawn(verdandi_http::serve_with(
        listener,
        Arc::clone(&node),
        Some(Arc::new(cluster)),
        std::future::pending(),
    ));
    let (engine, handle) = Engine::open(
        RealFs,
        dir.path(),
        Arc::new(TestClock::new(START_MS)),
        Box::new(OsRandom),
    )
    .await
    .expect("open");
    tokio::spawn(engine.run());
    node.set(handle).ok().expect("the node is set once");
    let http = Client::new();
    let status = |path: &str| {
        let request = http.get(format!("{url}{path}"));
        async move { call(request).await.status }
    };
    assert_eq!(status("/readyz").await, StatusCode::SERVICE_UNAVAILABLE);
    held.store(true, SeqCst);
    assert_eq!(status("/readyz").await, StatusCode::OK);
    let publish = || call(http.post(format!("{url}/s/x")).body("1"));
    assert_eq!(publish().await.status, StatusCode::CREATED);
    stop.store(true, SeqCst);
    assert_eq!(status("/readyz").await, StatusCode::SERVICE_UNAVAILABLE);
    let refused = publish().await;
    assert_eq!(refused.status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        refused.body.to_string().contains("stopping"),
        "{:?}",
        refused.body
    );
    assert_eq!(status("/s/x?group=g").await, StatusCode::OK);
}
