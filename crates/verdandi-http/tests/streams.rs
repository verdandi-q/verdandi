// allow: each test target uses a part of the shared kit
#[allow(dead_code)]
mod kit;

use std::time::Duration;

use kit::{START_MS, boot, call, ids, read_until, refused, start, taken};
use reqwest::StatusCode;
use serde_json::json;
use verdandi_core::ids::MessageId;
use verdandi_store::engine::{IDLE_SEAL_MS, TestClock};

fn holds(dir: &std::path::Path, needle: &[u8]) -> bool {
    std::fs::read_dir(dir)
        .expect("list")
        .map(|e| std::fs::read(e.expect("entry").path()).expect("read"))
        .any(|bytes| bytes.windows(needle.len()).any(|w| w == needle))
}

#[tokio::test]
async fn delete_stream_hides_every_older_message_and_compaction_removes_them() {
    let (dir, node) = boot().await;
    call(node.post("/s/s", "old-value-1")).await;
    call(node.post("/s/s", "old-value-2")).await;
    refused(&call(node.delete("/s/s")).await, StatusCode::BAD_REQUEST);
    refused(
        &call(node.delete("/s/s?confirm=t")).await,
        StatusCode::BAD_REQUEST,
    );
    let deleted = call(node.delete("/s/s?confirm=s")).await;
    assert_eq!(deleted.status, StatusCode::OK, "{}", deleted.body);
    let new = call(node.post("/s/s", "new-value")).await;
    assert_eq!(new.status, StatusCode::CREATED);
    let take = call(node.get("/s/s?group=g")).await;
    assert_eq!(taken(&take), ids(&new));
    assert!(holds(dir.path(), b"old-value-1"));
    node.clock.advance(IDLE_SEAL_MS + 1);
    let gone = tokio::time::timeout(Duration::from_secs(10), async {
        while holds(dir.path(), b"old-value-1") || holds(dir.path(), b"old-value-2") {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        gone.is_ok(),
        "compaction did not remove the deleted messages"
    );
    assert!(holds(dir.path(), b"new-value"));
    let again = call(node.post("/s/s", "after-compaction")).await;
    assert_eq!(again.status, StatusCode::CREATED);
    node.stop().await;
}

#[tokio::test]
async fn delete_stream_then_a_restart_with_the_clock_behind_gives_new_ids_above_the_tombstone() {
    let (dir, node) = boot().await;
    call(node.post("/s/s", "before")).await;
    node.clock.advance(60_000);
    let deleted = call(node.delete("/s/s?confirm=s")).await;
    let up_to: MessageId = deleted.body["up_to"]
        .as_str()
        .expect("up_to")
        .parse()
        .expect("an id");
    node.stop().await;
    let node = start(dir.path(), &TestClock::new(START_MS - 3_600_000)).await;
    let new = call(node.post("/s/s", "after")).await;
    let id: MessageId = ids(&new)[0].parse().expect("an id");
    assert!(id > up_to, "{id} is not above the tombstone {up_to}");
    let take = call(node.get("/s/s?group=g")).await;
    assert_eq!(taken(&take), ids(&new));
    node.stop().await;
}

#[tokio::test]
async fn delete_stream_of_a_reserved_name_gets_400_and_of_a_dead_stream_gets_200() {
    let (_dir, node) = boot().await;
    refused(
        &call(node.delete("/s/_x?confirm=_x")).await,
        StatusCode::BAD_REQUEST,
    );
    let deleted = call(node.delete("/s/s.dead.g?confirm=s.dead.g")).await;
    assert_eq!(deleted.status, StatusCode::OK, "{}", deleted.body);
    assert_eq!(deleted.body["deleted"], "s.dead.g");
    node.stop().await;
}

#[tokio::test]
async fn delete_stream_hides_older_messages_from_plain_reads_and_shows_new_ones() {
    let (_dir, node) = boot().await;
    call(node.post("/s/s", "old-1")).await;
    let old = ids(&call(node.post("/s/s", "old-2")).await);
    let first = call(node.get("/s/s?group=g")).await;
    let receipt = first.body["receipt"].as_str().expect("receipt");
    let buried = call(node.post(
        &format!("/r/{receipt}"),
        json!({"dead": [{"id": old[0], "error": "e"}]}).to_string(),
    ))
    .await;
    assert_eq!(buried.status, StatusCode::OK, "{}", buried.body);
    assert_eq!(taken(&call(node.get("/s/s")).await).len(), 2);
    assert_eq!(
        read_until(&node, "/s/s.dead.g", 1).await.status,
        StatusCode::OK
    );
    let deleted = call(node.delete("/s/s?confirm=s")).await;
    assert_eq!(deleted.status, StatusCode::OK, "{}", deleted.body);
    for path in ["/s/s", "/s/s?from=-7d", "/s/s.dead.g"] {
        let read = call(node.get(path)).await;
        assert!(taken(&read).is_empty(), "{path}: {}", read.body);
    }
    let new = call(node.post("/s/s", "new")).await;
    assert_eq!(taken(&call(node.get("/s/s")).await), ids(&new));
    node.stop().await;
}
