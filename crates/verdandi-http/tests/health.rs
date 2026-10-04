// allow: each test target uses a part of the shared kit
#[allow(dead_code)]
mod kit;

use std::sync::{Arc, OnceLock};

use kit::{START_MS, call, listen, refused, scratch, start, strings};
use reqwest::{Client, StatusCode};
use verdandi_store::engine::{Engine, OsRandom, TestClock};
use verdandi_store::fs::RealFs;

#[tokio::test]
async fn livez_answers_at_once_and_readyz_gives_503_until_recovery_ends() {
    let dir = scratch();
    let node = Arc::new(OnceLock::new());
    let (listener, url) = listen().await;
    let server = tokio::spawn(verdandi_http::serve(
        listener,
        Arc::clone(&node),
        std::future::pending(),
    ));
    let http = Client::new();
    let get = |path: &str| http.get(format!("{url}{path}"));
    assert_eq!(call(get("/livez")).await.status, StatusCode::OK);
    refused(&call(get("/readyz")).await, StatusCode::SERVICE_UNAVAILABLE);
    refused(
        &call(get("/s/x?group=g")).await,
        StatusCode::SERVICE_UNAVAILABLE,
    );
    let (engine, handle) = Engine::open(
        RealFs,
        dir.path(),
        Arc::new(TestClock::new(START_MS)),
        Box::new(OsRandom),
    )
    .await
    .expect("open");
    let engine = tokio::spawn(engine.run());
    node.set(handle).ok().expect("the node is set once");
    assert_eq!(call(get("/readyz")).await.status, StatusCode::OK);
    assert_eq!(call(get("/livez")).await.status, StatusCode::OK);
    server.abort();
    drop(http);
    drop(node);
    engine.abort();
}

#[tokio::test]
async fn get_root_names_a_damaged_file_by_its_file_name_only() {
    let dir = scratch();
    let name = "cmp-0000000000000063.vdl";
    std::fs::write(dir.path().join(name), [0xab; 32]).expect("garbage");
    let node = start(dir.path(), &TestClock::new(START_MS)).await;
    let root = call(node.get("/")).await;
    let warnings = strings(&root.body["warnings"]);
    assert!(
        warnings.contains(&format!("damaged log range: file {name} bytes 0..19")),
        "{warnings:?}"
    );
    node.stop().await;
}

#[tokio::test]
async fn every_path_but_livez_and_readyz_gets_503_with_next_during_recovery() {
    let (listener, url) = listen().await;
    let server = tokio::spawn(verdandi_http::serve(
        listener,
        Arc::new(OnceLock::new()),
        std::future::pending(),
    ));
    let http = Client::new();
    for request in [
        http.get(format!("{url}/")),
        http.get(format!("{url}/nope")),
        http.delete(format!("{url}/g/x")),
        http.put(format!("{url}/s/x")),
        http.post(format!("{url}/s/x")).body("1"),
    ] {
        refused(&call(request).await, StatusCode::SERVICE_UNAVAILABLE);
    }
    server.abort();
}
