// allow: each test target uses a part of the shared kit
#[allow(dead_code)]
mod kit;

use std::future::Future;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::task::Poll;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::Request;
use kit::{START_MS, boot, call, ids, read_until, refused, scratch, taken};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::sync::watch;
use tower_service::Service;
use verdandi_store::engine::{Engine, OsRandom, Take, Taken, TestClock};
use verdandi_store::fs::RealFs;

const ZERO_ID: &str = "000000000000-0000000000000000-0000";
const ZERO_RECEIPT: &str = "r_00000000000000000000000000000000";

#[tokio::test]
async fn take_limit_and_max_bytes_cut_the_batch_and_give_at_least_one_message() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post(
        "/s/t",
        "[1000000001,1000000002,1000000003,1000000004,1000000005]",
    ))
    .await);
    let limited = call(node.get("/s/t?group=g&limit=2")).await;
    assert_eq!(taken(&limited), published[..2]);
    let cut = call(node.get("/s/t?group=h&max_bytes=15")).await;
    assert_eq!(taken(&cut), published[..1]);
    let rest = call(node.get("/s/t?group=h")).await;
    assert_eq!(taken(&rest), published[1..]);
    let one = call(node.get("/s/t?group=k&max_bytes=1")).await;
    assert_eq!(taken(&one), published[..1]);
    let padded = ids(&call(
        node.post("/s/p", "[1,2]")
            .header("X-Msg-Pad", "p".repeat(100)),
    )
    .await);
    let headers = call(node.get("/s/p?group=g&max_bytes=200")).await;
    assert_eq!(taken(&headers), padded[..1]);
    node.stop().await;
}

#[tokio::test]
async fn take_max_bytes_counts_the_reply_bytes_so_base64_values_cut_the_batch_sooner() {
    let (_dir, node) = boot().await;
    let mut published = Vec::new();
    for byte in [0xfe, 0xff] {
        published.extend(ids(&call(
            node.post("/s/b", vec![byte; 400])
                .header("Content-Type", "application/octet-stream"),
        )
        .await));
    }
    let cut = call(node.get("/s/b?group=g&max_bytes=1000")).await;
    assert_eq!(taken(&cut), published[..1], "{}", cut.body);
    let rest = call(node.get("/s/b?group=g")).await;
    assert_eq!(taken(&rest), published[1..]);
    node.stop().await;
}

#[tokio::test]
async fn take_over_a_cap_or_under_a_minimum_gets_400_with_next() {
    let (_dir, node) = boot().await;
    for path in [
        "/s/t?group=g&limit=0",
        "/s/t?group=g&lease=43201",
        "/s/t?group=g&wait=61",
        "/s/t?group=g&limit=10001",
        "/s/t?group=g&wait=soon",
        "/s/t?group=a.dead.b",
        "/s/t?group=g&limt=5",
        "/s/t?group=g&max_bytes=16777217",
    ] {
        refused(&call(node.get(path)).await, StatusCode::BAD_REQUEST);
    }
    let dead = call(node.get("/s/t?group=a.dead.b")).await;
    let error = dead.body["error"].as_str().expect("error");
    assert!(error.contains("group"), "{error}");
    let at_cap = call(node.get("/s/t?group=g&lease=43200&limit=10000")).await;
    assert_eq!(at_cap.status, StatusCode::NO_CONTENT);
    node.stop().await;
}

#[tokio::test]
async fn take_with_wait_returns_early_on_a_publish() {
    let (_dir, node) = boot().await;
    let started = Instant::now();
    let publish = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        call(node.post("/s/w", "1")).await
    };
    let (take, published) = tokio::join!(call(node.get("/s/w?group=g&wait=30")), publish);
    assert_eq!(take.status, StatusCode::OK);
    assert_eq!(taken(&take), ids(&published));
    assert!(started.elapsed() < Duration::from_secs(10));
    node.stop().await;
}

#[tokio::test]
async fn take_with_wait_ends_in_204_with_a_next_link() {
    let (_dir, node) = boot().await;
    let started = Instant::now();
    let take = call(node.get("/s/w?group=g&wait=1&limit=7&lease=90&max_bytes=4096")).await;
    assert_eq!(take.status, StatusCode::NO_CONTENT);
    assert!(started.elapsed() >= Duration::from_secs(1));
    let link = take.headers["link"].to_str().expect("text");
    assert_eq!(
        link,
        r#"</s/w?group=g&limit=7&lease=90&max_bytes=4096&wait=60>; rel="next""#
    );
    node.stop().await;
}

#[tokio::test]
async fn settle_smoke_an_ack_by_id_gets_200_and_a_second_take_does_not_see_the_id() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/s", "[1,2]")).await);
    let take = call(node.get("/s/s?group=g")).await;
    assert_eq!(taken(&take), published);
    let receipt = take.body["receipt"].as_str().expect("receipt");
    let ack = call(node.post(
        &format!("/r/{receipt}"),
        json!({ "ack": [published[0]] }).to_string(),
    ))
    .await;
    assert_eq!(ack.status, StatusCode::OK);
    let leased = call(node.get("/s/s?group=g")).await;
    assert_eq!(leased.status, StatusCode::NO_CONTENT);
    node.clock.advance(31_000);
    let again = call(node.get("/s/s?group=g")).await;
    assert_eq!(taken(&again), published[1..]);
    node.stop().await;
}

#[tokio::test]
async fn settle_refusals_change_nothing_and_a_repeat_gets_200() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/s", "1")).await);
    let first = call(node.get("/s/s?group=g")).await;
    let receipt = first.body["receipt"].as_str().expect("receipt");
    let ack = json!({ "ack": [published[0]] }).to_string();
    let unknown = call(node.post(&format!("/r/{ZERO_RECEIPT}"), ack.clone())).await;
    refused(&unknown, StatusCode::CONFLICT);
    let both = json!({ "ack": [published[0], ZERO_ID] }).to_string();
    refused(
        &call(node.post(&format!("/r/{receipt}"), both)).await,
        StatusCode::CONFLICT,
    );
    node.clock.advance(31_000);
    let second = call(node.get("/s/s?group=g")).await;
    assert_eq!(taken(&second), published);
    let path = format!("/r/{}", second.body["receipt"].as_str().expect("receipt"));
    refused(
        &call(node.post("/r/r_bad", ack.clone())).await,
        StatusCode::BAD_REQUEST,
    );
    let reply = json!({ "reply": { "id": published[0], "value": 1 } }).to_string();
    refused(
        &call(node.post(&path, reply)).await,
        StatusCode::BAD_REQUEST,
    );
    let nack = json!({ "nack": [{ "id": published[0], "delay": 43_201 }] }).to_string();
    refused(&call(node.post(&path, nack)).await, StatusCode::BAD_REQUEST);
    let many = json!({ "ack": vec![ZERO_ID; 10_001] }).to_string();
    refused(&call(node.post(&path, many)).await, StatusCode::BAD_REQUEST);
    let extend = json!({ "extend": 43_201 }).to_string();
    refused(
        &call(node.post(&path, extend)).await,
        StatusCode::BAD_REQUEST,
    );
    let extended = call(node.post(&path, json!({ "extend": 120 }).to_string())).await;
    assert_eq!(extended.status, StatusCode::OK);
    node.clock.advance(31_000);
    assert_eq!(call(node.delete(&path)).await.status, StatusCode::OK);
    assert_eq!(call(node.delete(&path)).await.status, StatusCode::OK);
    assert_eq!(call(node.post(&path, ack)).await.status, StatusCode::OK);
    node.clock.advance(120_000);
    let none = call(node.get("/s/s?group=g")).await;
    assert_eq!(none.status, StatusCode::NO_CONTENT);
    node.stop().await;
}

#[tokio::test]
async fn a_take_reply_holds_at_most_16_mib_of_values_and_releases_the_rest() {
    let (_dir, node) = boot().await;
    let value = format!("\"{}\"", "v".repeat(999_998));
    let batch = format!("[{}]", vec![value; 9].join(","));
    let mut published = ids(&call(node.post("/s/big", batch.clone())).await);
    published.extend(ids(&call(node.post("/s/big", batch)).await));
    assert_eq!(published.len(), 18);
    let first = call(node.get("/s/big?group=g&limit=100")).await;
    assert_eq!(taken(&first), published[..16]);
    let rest = call(node.get("/s/big?group=g")).await;
    assert_eq!(taken(&rest), published[16..]);
    node.stop().await;
}

fn holder(dir: &Path, marker: &[u8]) -> (PathBuf, Vec<u8>, usize) {
    for entry in std::fs::read_dir(dir).expect("data dir") {
        let path = entry.expect("entry").path();
        let data = std::fs::read(&path).expect("read");
        if let Some(at) = data.windows(marker.len()).position(|w| w == marker) {
            return (path, data, at);
        }
    }
    panic!("no file holds the marker");
}

fn corrupt(dir: &Path, marker: &[u8]) {
    let (path, _, at) = holder(dir, marker);
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open");
    file.write_all_at(b"#", at as u64).expect("flip");
}

#[tokio::test]
async fn a_take_whose_every_message_is_unreadable_gets_204() {
    let (dir, node) = boot().await;
    let marker = "unreadable-value-marker";
    let published = call(node.post("/s/u", marker)).await;
    assert_eq!(published.status, StatusCode::CREATED);
    corrupt(dir.path(), marker.as_bytes());
    let take = call(node.get("/s/u?group=g")).await;
    assert_eq!(take.status, StatusCode::NO_CONTENT, "{}", take.body);
    node.stop().await;
}

#[tokio::test]
async fn a_head_with_a_group_gets_400_and_leases_nothing() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/h", "1")).await);
    let head = call(reqwest::Client::new().head(format!("{}/s/h?group=g", node.url))).await;
    assert_eq!(head.status, StatusCode::BAD_REQUEST);
    let take = call(node.get("/s/h?group=g")).await;
    assert_eq!(taken(&take), published);
    node.stop().await;
}

#[tokio::test]
async fn an_unreadable_message_is_not_in_the_receipt_of_the_take() {
    let (dir, node) = boot().await;
    let marker = "unreadable-value-marker";
    let lost = ids(&call(node.post("/s/u", marker)).await);
    let kept = ids(&call(node.post("/s/u", "1")).await);
    corrupt(dir.path(), marker.as_bytes());
    let take = call(node.get("/s/u?group=g")).await;
    assert_eq!(taken(&take), kept);
    let receipt = take.body["receipt"].as_str().expect("receipt");
    let ack = json!({ "ack": lost }).to_string();
    refused(
        &call(node.post(&format!("/r/{receipt}"), ack)).await,
        StatusCode::CONFLICT,
    );
    node.stop().await;
}

#[tokio::test]
async fn a_take_whose_read_fails_releases_every_leased_message() {
    let (dir, node) = boot().await;
    let marker = "io-error-value-marker";
    let first = call(node.get("/s/e?group=g")).await;
    assert_eq!(first.status, StatusCode::NO_CONTENT);
    let published = ids(&call(node.post("/s/e", marker)).await);
    let (path, data, at) = holder(dir.path(), marker.as_bytes());
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open");
    file.set_len(at as u64).expect("cut");
    refused(
        &call(node.get("/s/e?group=g")).await,
        StatusCode::SERVICE_UNAVAILABLE,
    );
    file.write_all_at(&data[at..], at as u64).expect("restore");
    let again = call(node.get("/s/e?group=g")).await;
    assert_eq!(again.status, StatusCode::OK);
    assert_eq!(taken(&again), published);
    node.stop().await;
}

#[tokio::test]
async fn a_lease_or_an_extend_of_0_gets_400_with_next() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/z", "1")).await);
    refused(
        &call(node.get("/s/z?group=g&lease=0")).await,
        StatusCode::BAD_REQUEST,
    );
    let take = call(node.get("/s/z?group=g")).await;
    assert_eq!(taken(&take), published);
    let path = format!("/r/{}", take.body["receipt"].as_str().expect("receipt"));
    refused(
        &call(node.post(&path, json!({ "extend": 0 }).to_string())).await,
        StatusCode::BAD_REQUEST,
    );
    let ack = json!({ "ack": published }).to_string();
    assert_eq!(call(node.post(&path, ack)).await.status, StatusCode::OK);
    node.stop().await;
}

fn local(method: &str, path: &str, body: String) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .header("host", "127.0.0.1")
        .body(Body::from(body))
        .expect("request")
}

async fn json_of(response: axum::response::Response) -> (u16, Value) {
    let status = response.status().as_u16();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body)
}

#[tokio::test]
async fn a_take_that_stops_after_its_lease_and_before_its_reply_releases_its_messages() {
    let dir = scratch();
    let (engine, handle) = Engine::open(
        RealFs,
        dir.path(),
        Arc::new(TestClock::new(START_MS)),
        Box::new(OsRandom),
    )
    .await
    .expect("open");
    let engine = tokio::spawn(engine.run());
    let (_stop, stopping) = watch::channel(false);
    let mut app = verdandi_http::router(Arc::new(OnceLock::from(handle)), stopping);
    let value = format!("\"{}\"", "v".repeat(1_000_000));
    let (status, published) = json_of(
        app.call(local("POST", "/s/d", value))
            .await
            .expect("publish"),
    )
    .await;
    assert_eq!(status, 201);
    let mut take = Box::pin(app.call(local("GET", "/s/d?group=g", String::new())));
    let first = std::future::poll_fn(|cx| Poll::Ready(take.as_mut().poll(cx).is_pending())).await;
    assert!(first, "the take waits for the engine");
    let (status, _) = json_of(
        app.call(local("GET", "/", String::new()))
            .await
            .expect("root"),
    )
    .await;
    assert_eq!(status, 200);
    let second = std::future::poll_fn(|cx| Poll::Ready(take.as_mut().poll(cx).is_pending())).await;
    assert!(second, "the take reads its leased message");
    drop(take);
    tokio::task::yield_now().await;
    let (status, again) = json_of(
        app.call(local("GET", "/s/d?group=g", String::new()))
            .await
            .expect("take"),
    )
    .await;
    assert_eq!(status, 200, "{again}");
    assert_eq!(again["messages"][0]["id"], published["ids"][0]);
    engine.abort();
}

#[tokio::test]
async fn settle_ack_nack_and_dead_touch_only_the_named_ids_and_the_dead_copy_keeps_its_fields() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/s", "[1,2,3,4]")).await);
    let take = call(node.get("/s/s?group=g")).await;
    let receipt = take.body["receipt"].as_str().expect("receipt");
    let settled = call(
        node.post(
            &format!("/r/{receipt}"),
            json!({
                "ack": [published[0]],
                "nack": [{"id": published[1], "error": "timeout"}],
                "dead": [{"id": published[2], "error": "bad payload"}],
            })
            .to_string(),
        ),
    )
    .await;
    assert_eq!(settled.status, StatusCode::OK, "{}", settled.body);
    let early = call(node.get("/s/s?group=g")).await;
    assert_eq!(early.status, StatusCode::NO_CONTENT);
    node.clock.advance(1_000);
    let nacked = call(node.get("/s/s?group=g")).await;
    assert_eq!(taken(&nacked), published[1..2]);
    assert_eq!(nacked.body["messages"][0]["attempt"], 2);
    let dead = call(node.get("/s/s.dead.g")).await;
    assert_eq!(taken(&dead).len(), 1, "{}", dead.body);
    let copy = &dead.body["messages"][0];
    assert_eq!(copy["value"], 3);
    assert_eq!(
        copy["original"],
        json!({"id": published[2], "stream": "s", "reason": "dead", "error": "bad payload"})
    );
    node.clock.advance(30_000);
    let rest = call(node.get("/s/s?group=g")).await;
    assert_eq!(taken(&rest), [published[1].clone(), published[3].clone()]);
    node.stop().await;
}

#[tokio::test]
async fn settle_dead_of_40_messages_of_1_mb_gives_40_dead_copies_once_the_reply_is_200() {
    let (_dir, node) = boot().await;
    let value = format!("\"{}\"", "v".repeat(1_000_000));
    let mut published = Vec::new();
    for _ in 0..40 {
        published.extend(ids(&call(node.post("/s/big", value.clone())).await));
    }
    let Taken::Leased {
        receipt, messages, ..
    } = node
        .handle
        .take(Take {
            group: "g".to_owned(),
            selector: "big".to_owned(),
            limit: 40,
            lease_ms: 600_000,
            wait: false,
        })
        .await
        .expect("take")
    else {
        panic!("the take leased nothing");
    };
    assert_eq!(messages.len(), 40);
    let dead: Vec<Value> = published
        .iter()
        .map(|id| json!({"id": id, "error": "too big"}))
        .collect();
    let settled = call(node.post(
        &format!("/r/{receipt}"),
        json!({ "dead": dead }).to_string(),
    ))
    .await;
    assert_eq!(settled.status, StatusCode::OK, "{}", settled.body);
    let mut copies = Vec::new();
    let mut path = "/s/big.dead.g".to_owned();
    loop {
        let page = call(node.get(&path)).await;
        let ids = taken(&page);
        if ids.is_empty() {
            break;
        }
        for message in page.body["messages"].as_array().expect("messages") {
            assert_eq!(message["original"]["reason"], "dead");
            assert_eq!(message["original"]["error"], "too big");
            copies.push(message["original"]["id"].as_str().expect("id").to_owned());
        }
        path = page.body["next"].as_str().expect("next").to_owned();
    }
    assert_eq!(copies, published);
    let stats = call(node.get("/g/g")).await;
    assert_eq!(stats.body["dead"], 40);
    assert_eq!(stats.body["in_flight"], 0);
    node.stop().await;
}

#[tokio::test]
async fn retry_nack_backoff_starts_at_1_s_and_doubles_and_a_nack_delay_replaces_it() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/s", "1")).await);
    let nack = |body: Value| {
        let node = &node;
        async move {
            let take = call(node.get("/s/s?group=g")).await;
            let receipt = take.body["receipt"].as_str().expect("receipt");
            let reply = call(node.post(&format!("/r/{receipt}"), body.to_string())).await;
            assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
            take.body["messages"][0]["attempt"].clone()
        }
    };
    let id = &published[0];
    assert_eq!(nack(json!({"nack": [{"id": id}]})).await, 1);
    for (wait_ms, attempt) in [(1_000, 2), (2_000, 3)] {
        node.clock.advance(wait_ms - 1);
        assert_eq!(
            call(node.get("/s/s?group=g")).await.status,
            StatusCode::NO_CONTENT
        );
        node.clock.advance(1);
        assert_eq!(nack(json!({"nack": [{"id": id}]})).await, attempt);
    }
    node.clock.advance(3_999);
    assert_eq!(
        call(node.get("/s/s?group=g")).await.status,
        StatusCode::NO_CONTENT
    );
    node.clock.advance(1);
    assert_eq!(nack(json!({"nack": [{"id": id, "delay": 30}]})).await, 4);
    node.clock.advance(29_999);
    assert_eq!(
        call(node.get("/s/s?group=g")).await.status,
        StatusCode::NO_CONTENT
    );
    node.clock.advance(1);
    let due = call(node.get("/s/s?group=g")).await;
    assert_eq!(taken(&due), published);
    assert_eq!(due.body["messages"][0]["attempt"], 5);
    node.stop().await;
}

#[tokio::test]
async fn retry_the_first_lease_expiry_is_free_and_the_fifth_failure_goes_dead_by_itself() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/s", "\"p\"")).await);
    call(node.get("/s/s?group=g")).await;
    node.clock.advance(31_000);
    let mut attempt = 0;
    for failure in 1..=5 {
        let take = call(node.get("/s/s?group=g")).await;
        assert_eq!(taken(&take), published, "failure {failure}");
        attempt = take.body["messages"][0]["attempt"]
            .as_u64()
            .expect("attempt");
        let receipt = take.body["receipt"].as_str().expect("receipt");
        let nack =
            json!({"nack": [{"id": published[0], "delay": 0, "error": format!("boom {failure}")}]});
        let reply = call(node.post(&format!("/r/{receipt}"), nack.to_string())).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        if failure == 4 {
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(taken(&call(node.get("/s/s.dead.g")).await).is_empty());
        }
    }
    assert_eq!(attempt, 6, "the free expiry counted as a delivery");
    node.clock.advance(1);
    let dead = read_until(&node, "/s/s.dead.g", 1).await;
    assert_eq!(
        dead.body["messages"][0]["original"],
        json!({"id": published[0], "stream": "s", "reason": "attempts", "error": "boom 5"})
    );
    node.clock.advance(600_000);
    assert_eq!(
        call(node.get("/s/s?group=g")).await.status,
        StatusCode::NO_CONTENT
    );
    node.stop().await;
}
