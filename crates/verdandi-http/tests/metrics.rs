// allow: each test target uses a part of the shared kit
#[allow(dead_code)]
mod kit;

use kit::{boot, call};
use reqwest::StatusCode;
use serde_json::json;

fn array(count: usize) -> String {
    format!("[{}]", vec!["1"; count].join(","))
}

async fn settled(node: &kit::Node) {
    call(node.post("/s/s", array(300))).await;
    let got = call(node.get("/s/s?group=g&limit=60")).await;
    let receipt = got.body["receipt"].as_str().expect("receipt").to_owned();
    let acks: Vec<String> = got.body["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .map(|m| m["id"].as_str().expect("id").to_owned())
        .collect();
    let done = call(node.post(&format!("/r/{receipt}"), json!({"ack": acks}).to_string())).await;
    assert_eq!(done.status, StatusCode::OK, "{}", done.body);
}

#[tokio::test]
async fn group_json_has_drain_rate_and_desired_consumers_over_a_60_s_window() {
    let (_dir, node) = boot().await;
    settled(&node).await;
    let stats = call(node.get("/g/g")).await;
    assert_eq!(stats.body["pending"], 240, "{}", stats.body);
    assert_eq!(stats.body["drain_rate"], 1.0, "{}", stats.body);
    assert_eq!(stats.body["desired_consumers"], 4, "{}", stats.body);
    node.clock.advance(61_000);
    let later = call(node.get("/g/g")).await;
    assert_eq!(later.body["drain_rate"], 0.0, "{}", later.body);
    assert_eq!(later.body["desired_consumers"], 240, "{}", later.body);
    node.stop().await;
}

#[tokio::test]
async fn metrics_is_prometheus_text_with_per_group_pending() {
    let (_dir, node) = boot().await;
    settled(&node).await;
    let response = node.get("/metrics").send().await.expect("send");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["content-type"],
        "text/plain; version=0.0.4"
    );
    let text = response.text().await.expect("text");
    let mut typed = Vec::new();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let (name, kind) = rest.split_once(' ').expect("name and kind");
            assert_eq!(kind, "gauge");
            typed.push(name.to_owned());
        } else if !line.starts_with('#') {
            let (series, value) = line.rsplit_once(' ').expect("name and value");
            value.parse::<f64>().expect("a number");
            let name = series.split('{').next().expect("name");
            assert!(typed.iter().any(|t| t == name), "no TYPE for {name}");
            assert!(!series.contains('{') || series.ends_with('}'), "{line}");
        }
    }
    assert!(
        text.lines()
            .any(|l| l == "verdandi_group_pending{group=\"g\"} 240"),
        "{text}"
    );
    assert!(
        text.contains("verdandi_group_drain_rate{group=\"g\"} 1\n"),
        "{text}"
    );
    assert!(text.contains("verdandi_live_members 1\n"), "{text}");
    assert!(text.contains("verdandi_disk_used_bytes "), "{text}");
    node.stop().await;
}

#[tokio::test]
async fn root_has_a_scale_hint_that_is_null_when_nothing_is_wrong() {
    let (_dir, node) = boot().await;
    let root = call(node.get("/")).await;
    assert!(root.body.get("scale_hint").is_some(), "{}", root.body);
    assert!(root.body["scale_hint"].is_null(), "{}", root.body);
    node.stop().await;
}

#[tokio::test]
async fn scale_hint_and_live_members_follow_the_cluster_callback() {
    use std::sync::{Arc, OnceLock};
    use verdandi_http::Cluster;
    use verdandi_store::engine::{Engine, OsRandom, TestClock};
    use verdandi_store::fs::RealFs;

    let dir = kit::scratch();
    let cluster = Cluster::new(
        b"key".to_vec(),
        Box::new(|_| Vec::new()),
        Box::new(|| "{}".into()),
    )
    .with_scale(Box::new(|| (1, 2)));
    let node = Arc::new(OnceLock::new());
    let (listener, url) = kit::listen().await;
    tokio::spawn(verdandi_http::serve_with(
        listener,
        Arc::clone(&node),
        Some(Arc::new(cluster)),
        std::future::pending(),
    ));
    let (engine, handle) = Engine::open(
        RealFs,
        dir.path(),
        Arc::new(TestClock::new(kit::START_MS)),
        Box::new(OsRandom),
    )
    .await
    .expect("open");
    tokio::spawn(engine.run());
    node.set(handle).ok().expect("the node is set once");
    let http = reqwest::Client::new();
    let root = call(http.get(format!("{url}/"))).await;
    assert_eq!(
        root.body["scale_hint"], "fewer live nodes than the ack floor",
        "{}",
        root.body
    );
    let text = http
        .get(format!("{url}/metrics"))
        .send()
        .await
        .expect("send")
        .text()
        .await
        .expect("text");
    assert!(text.contains("verdandi_live_members 1\n"), "{text}");
}

#[tokio::test]
async fn scale_hint_names_a_heavy_node_from_the_cluster() {
    use std::sync::{Arc, OnceLock};
    use verdandi_http::Cluster;
    use verdandi_store::engine::{Engine, OsRandom, TestClock};
    use verdandi_store::fs::RealFs;

    let dir = kit::scratch();
    let cluster = Cluster::new(
        b"key".to_vec(),
        Box::new(|_| Vec::new()),
        Box::new(|| "{}".into()),
    )
    .with_scale(Box::new(|| (3, 2)))
    .with_pressure(Box::new(|| (Some(0xab), None)));
    let node = Arc::new(OnceLock::new());
    let (listener, url) = kit::listen().await;
    tokio::spawn(verdandi_http::serve_with(
        listener,
        Arc::clone(&node),
        Some(Arc::new(cluster)),
        std::future::pending(),
    ));
    let (engine, handle) = Engine::open(
        RealFs,
        dir.path(),
        Arc::new(TestClock::new(kit::START_MS)),
        Box::new(OsRandom),
    )
    .await
    .expect("open");
    tokio::spawn(engine.run());
    node.set(handle).ok().expect("the node is set once");
    let http = reqwest::Client::new();
    let root = call(http.get(format!("{url}/"))).await;
    assert_eq!(
        root.body["scale_hint"], "node 00000000000000ab fills faster than its peers",
        "{}",
        root.body
    );
}

#[tokio::test]
async fn copies_show_the_controller_count_target_and_floor() {
    use std::sync::{Arc, OnceLock};
    use verdandi_http::Cluster;
    use verdandi_store::engine::{Engine, OsRandom, TestClock};
    use verdandi_store::fs::RealFs;

    let dir = kit::scratch();
    let cluster = Cluster::new(
        b"key".to_vec(),
        Box::new(|_| Vec::new()),
        Box::new(|| "{}".into()),
    )
    .with_copies(Box::new(|| (4, 5, 3)));
    let node = Arc::new(OnceLock::new());
    let (listener, url) = kit::listen().await;
    tokio::spawn(verdandi_http::serve_with(
        listener,
        Arc::clone(&node),
        Some(Arc::new(cluster)),
        std::future::pending(),
    ));
    let (engine, handle) = Engine::open(
        RealFs,
        dir.path(),
        Arc::new(TestClock::new(kit::START_MS)),
        Box::new(OsRandom),
    )
    .await
    .expect("open");
    tokio::spawn(engine.run());
    node.set(handle).ok().expect("the node is set once");
    let root = call(reqwest::Client::new().get(format!("{url}/"))).await;
    let copies = &root.body["copies"];
    assert_eq!(
        (
            copies["ack_count"].as_u64(),
            copies["target"].as_u64(),
            copies["floor"].as_u64()
        ),
        (Some(4), Some(5), Some(3)),
        "{}",
        root.body
    );
}
