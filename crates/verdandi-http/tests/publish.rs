// allow: each test target uses a part of the shared kit
#[allow(dead_code)]
mod kit;

use std::time::{Duration, UNIX_EPOCH};

use kit::{boot, call, ids, read_until, refused, start, taken};
use reqwest::StatusCode;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn counter(id: &str) -> u16 {
    u16::from_str_radix(&id[id.len() - 4..], 16).expect("a hex counter")
}

async fn status_line(url: &str, request: &str) -> String {
    let address = url.trim_start_matches("http://");
    let mut stream = TcpStream::connect(address).await.expect("connect");
    stream.write_all(request.as_bytes()).await.expect("write");
    let mut reply = Vec::new();
    let mut chunk = [0; 1024];
    while !reply.windows(2).any(|w| w == b"\r\n") {
        let n = stream.read(&mut chunk).await.expect("read");
        assert!(n > 0, "the server closed with no status line");
        reply.extend_from_slice(&chunk[..n]);
    }
    let text = String::from_utf8_lossy(&reply).into_owned();
    text.lines().next().unwrap_or_default().to_owned()
}

#[tokio::test]
async fn publish_one_gives_201_one_id_one_copy_and_a_next_take_link() {
    let (_dir, node) = boot().await;
    let reply = call(node.post("/s/orders", r#"{"order":1}"#)).await;
    assert_eq!(reply.status, StatusCode::CREATED);
    assert_eq!(ids(&reply).len(), 1);
    assert_eq!(reply.body["copies"], 1);
    assert_eq!(reply.body["next"]["take"], "/s/orders?group={group}");
    let take = call(node.get("/s/orders?group=g")).await;
    assert_eq!(taken(&take), ids(&reply));
    assert_eq!(take.body["messages"][0]["value"], json!({"order": 1}));
    node.stop().await;
}

#[tokio::test]
async fn publish_batch_is_one_atomic_batch_that_keeps_the_bytes_of_each_element() {
    let (_dir, node) = boot().await;
    let reply = call(node.post("/s/b", r#"[{"a": 1.50}, "x", 3]"#)).await;
    assert_eq!(reply.status, StatusCode::CREATED);
    assert_eq!(ids(&reply).len(), 3);
    let take = call(node.get("/s/b?group=g")).await;
    assert_eq!(taken(&take), ids(&reply));
    let text = String::from_utf8(take.bytes).expect("UTF-8");
    assert!(text.contains(r#""value":{"a": 1.50}"#), "{text}");
    assert_eq!(take.body["messages"][1]["value"], "x");
    assert_eq!(take.body["messages"][2]["value"], 3);
    refused(
        &call(node.post("/s/b", "[]")).await,
        StatusCode::BAD_REQUEST,
    );
    let large = format!("[{}]", vec!["1"; 4_300].join(","));
    let over = call(
        node.post("/s/c", large)
            .header("X-Msg-Pad", "p".repeat(8_000)),
    )
    .await;
    refused(&over, StatusCode::PAYLOAD_TOO_LARGE);
    let items = format!("[{}]", vec![r#"{"stream":"c","value":1}"#; 4_300].join(","));
    let bulk = call(
        node.post("/bulk", items)
            .header("X-Msg-Pad", "p".repeat(8_000)),
    )
    .await;
    refused(&bulk, StatusCode::PAYLOAD_TOO_LARGE);
    let after = ids(&call(node.post("/s/a", "1")).await);
    assert_eq!(counter(&after[0]), counter(&ids(&reply)[2]) + 1);
    let none = call(node.get("/s/c?group=g")).await;
    assert_eq!(none.status, StatusCode::NO_CONTENT);
    node.stop().await;
}

#[tokio::test]
async fn bulk_publishes_each_item_to_its_named_stream_with_an_id_each() {
    let (_dir, node) = boot().await;
    let body = r#"[{"stream":"a","value":1},{"stream":"b","value":{"x":2},"headers":{"X-Msg-K":"v","X-Other":"no"}}]"#;
    let reply = call(node.post("/bulk", body)).await;
    assert_eq!(reply.status, StatusCode::CREATED);
    let published = ids(&reply);
    assert_eq!(published.len(), 2);
    let a = call(node.get("/s/a?group=g")).await;
    assert_eq!(taken(&a), published[..1]);
    assert_eq!(a.body["messages"][0]["value"], 1);
    let b = call(node.get("/s/b?group=g")).await;
    assert_eq!(taken(&b), published[1..]);
    assert_eq!(b.body["messages"][0]["value"], json!({"x": 2}));
    assert_eq!(b.body["messages"][0]["headers"], json!({"x-msg-k": "v"}));
    node.stop().await;
}

#[tokio::test]
async fn idempotency_key_repeats_get_200_with_the_first_body_per_scope_and_after_a_restart() {
    let (dir, node) = boot().await;
    let keyed = |path: &str, key: &str, body: &str| {
        node.post(path, body.to_owned())
            .header("Idempotency-Key", key)
    };
    let first = call(keyed("/s/a", "k1", "1")).await;
    assert_eq!(first.status, StatusCode::CREATED);
    let again = call(keyed("/s/a", "k1", "2")).await;
    assert_eq!(again.status, StatusCode::OK);
    assert_eq!(again.bytes, first.bytes);
    let other = call(keyed("/s/b", "k1", "1")).await;
    assert_eq!(other.status, StatusCode::CREATED);
    assert_ne!(ids(&other), ids(&first));
    let bulk = call(keyed("/bulk", "k1", r#"[{"stream":"a","value":1}]"#)).await;
    assert_eq!(bulk.status, StatusCode::CREATED);
    assert_ne!(ids(&bulk), ids(&first));
    let (x, y) = tokio::join!(
        call(keyed("/s/c", "k2", "1")),
        call(keyed("/s/c", "k2", "1"))
    );
    let mut statuses = [x.status, y.status];
    statuses.sort();
    assert_eq!(statuses, [StatusCode::OK, StatusCode::CREATED]);
    assert_eq!(ids(&x), ids(&y));
    let clock = node.clock.clone();
    node.stop().await;
    let node = start(dir.path(), &clock).await;
    let after = call(node.post("/s/a", "3").header("Idempotency-Key", "k1")).await;
    assert_eq!(after.status, StatusCode::OK);
    assert_eq!(after.bytes, first.bytes);
    node.stop().await;
}

#[tokio::test]
async fn idempotency_key_of_a_batch_of_a_million_messages_repeats_the_whole_reply() {
    let (_dir, node) = boot().await;
    let batch = format!("[{}]", vec!["1"; 1_000_000].join(","));
    let keyed = || {
        node.post("/s/m", batch.clone())
            .header("Idempotency-Key", "million")
    };
    let first = call(keyed()).await;
    assert_eq!(first.status, StatusCode::CREATED, "{}", first.body);
    assert_eq!(ids(&first).len(), 1_000_000);
    let again = call(keyed()).await;
    assert_eq!(again.status, StatusCode::OK);
    assert_eq!(again.bytes, first.bytes);
    node.stop().await;
}

#[tokio::test]
async fn headers_that_travel_come_back_on_take_and_others_are_dropped() {
    let (_dir, node) = boot().await;
    let reply = call(
        node.post("/s/h", "hello")
            .header("Content-Type", "text/plain; charset=utf-8")
            .header(
                "traceparent",
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            )
            .header("tracestate", "a=1")
            .header("ce-type", "order.created")
            .header("X-Msg-Who", "me")
            .header("X-Other", "dropped"),
    )
    .await;
    assert_eq!(reply.status, StatusCode::CREATED);
    let take = call(node.get("/s/h?group=g")).await;
    let message = &take.body["messages"][0];
    assert_eq!(
        message["headers"],
        json!({
            "content-type": "text/plain; charset=utf-8",
            "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            "tracestate": "a=1",
            "ce-type": "order.created",
            "x-msg-who": "me",
        })
    );
    assert_eq!(message["value"], "hello");
    let over = call(
        node.post("/s/h", "x")
            .header("X-Msg-Pad", "p".repeat(9_000)),
    )
    .await;
    refused(&over, StatusCode::BAD_REQUEST);
    node.stop().await;
}

#[tokio::test]
async fn delay_takes_an_rfc_3339_time_with_any_offset_or_a_lowercase_z() {
    let (_dir, node) = boot().await;
    let due = kit::START_MS + 30_000;
    let utc = |ms: u64| {
        humantime::format_rfc3339_millis(UNIX_EPOCH + Duration::from_millis(ms)).to_string()
    };
    let times = [
        utc(due + 7_200_000).replace('Z', "+02:00"),
        utc(due - 19_800_000).replace('Z', "-05:30"),
        utc(due).replace('Z', "z").replace('T', "t"),
    ];
    let mut published = Vec::new();
    for time in &times {
        let path = format!("/s/d?delay={}", time.replace('+', "%2B"));
        let reply = call(node.post(&path, "1")).await;
        assert_eq!(reply.status, StatusCode::CREATED, "{time}: {}", reply.body);
        published.extend(ids(&reply));
    }
    node.clock.advance(29_999);
    let early = call(node.get("/s/d?group=g")).await;
    assert_eq!(early.status, StatusCode::NO_CONTENT);
    node.clock.advance(1);
    assert_eq!(taken(&call(node.get("/s/d?group=g")).await), published);
    node.stop().await;
}

#[tokio::test]
async fn delay_hides_the_message_from_every_take_until_its_time() {
    let (_dir, node) = boot().await;
    let seconds = call(node.post("/s/d?delay=30", "1")).await;
    let duration = call(node.post("/s/d?delay=30s", "2")).await;
    assert_eq!(seconds.status, StatusCode::CREATED);
    assert_eq!(duration.status, StatusCode::CREATED);
    let early = call(node.get("/s/d?group=g")).await;
    assert_eq!(early.status, StatusCode::NO_CONTENT);
    node.clock.advance(29_999);
    let still = call(node.get("/s/d?group=g")).await;
    assert_eq!(still.status, StatusCode::NO_CONTENT);
    node.clock.advance(1);
    let due = call(node.get("/s/d?group=g")).await;
    assert_eq!(taken(&due), [ids(&seconds), ids(&duration)].concat());
    refused(
        &call(node.post("/s/d?delay=soon", "3")).await,
        StatusCode::BAD_REQUEST,
    );
    refused(
        &call(node.post("/s/d?tll=30", "3")).await,
        StatusCode::BAD_REQUEST,
    );
    node.stop().await;
}

#[tokio::test]
async fn too_large_a_message_over_1_mib_gets_413_that_names_the_claim_check_pattern() {
    let (_dir, node) = boot().await;
    let fits = call(node.post("/s/t", vec![b'a'; 1 << 20])).await;
    assert_eq!(fits.status, StatusCode::CREATED);
    let over = call(node.post("/s/t", vec![b'a'; (1 << 20) + 1])).await;
    refused(&over, StatusCode::PAYLOAD_TOO_LARGE);
    let body = call(node.post("/s/t", vec![b'a'; (16 << 20) + 1])).await;
    refused(&body, StatusCode::PAYLOAD_TOO_LARGE);
    let next = over.body["next"].as_str().expect("next");
    assert!(next.contains("claim-check"), "{next}");
    let next = body.body["next"].as_str().expect("next");
    assert!(
        next.contains("split the batch") && !next.contains("claim-check"),
        "{next}"
    );
    node.stop().await;
}

#[tokio::test]
async fn cross_origin_smoke_a_foreign_text_post_and_a_foreign_host_get_403() {
    let (_dir, node) = boot().await;
    let foreign = call(
        node.post("/s/x", "a")
            .header("Content-Type", "text/plain")
            .header("Origin", "http://evil.example"),
    )
    .await;
    refused(&foreign, StatusCode::FORBIDDEN);
    let none = call(node.get("/s/x?group=g")).await;
    assert_eq!(none.status, StatusCode::NO_CONTENT);
    for site in ["same-site", "cross-site"] {
        let take = call(node.get("/s/x?group=g").header("Sec-Fetch-Site", site)).await;
        refused(&take, StatusCode::FORBIDDEN);
        let post = call(node.post("/s/x", "a").header("Sec-Fetch-Site", site)).await;
        refused(&post, StatusCode::FORBIDDEN);
    }
    for site in ["same-origin", "none"] {
        let take = call(node.get("/s/x?group=g").header("Sec-Fetch-Site", site)).await;
        assert_eq!(take.status, StatusCode::NO_CONTENT);
    }
    let host = call(node.get("/readyz").header("Host", "evil.example")).await;
    refused(&host, StatusCode::FORBIDDEN);
    node.stop().await;
}

#[tokio::test]
async fn a_body_that_breaks_off_gets_400_not_413() {
    let (_dir, node) = boot().await;
    let request =
        "POST /s/x HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n\r\n";
    let line = status_line(&node.url, request).await;
    assert!(line.starts_with("HTTP/1.1 400"), "{line}");
    node.stop().await;
}

#[tokio::test]
async fn a_body_that_does_not_arrive_in_60_s_gets_408() {
    let (_dir, node) = boot().await;
    let request = "POST /s/x HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 10\r\n\r\n1";
    tokio::time::pause();
    let line = tokio::time::timeout(Duration::from_secs(120), status_line(&node.url, request))
        .await
        .expect("a reply before 120 s");
    tokio::time::resume();
    assert!(line.starts_with("HTTP/1.1 408"), "{line}");
    node.stop().await;
}

#[tokio::test]
async fn a_bulk_over_the_block_cap_gets_413_before_the_rest_of_the_array_is_read() {
    let (_dir, node) = boot().await;
    let items = vec![r#"{"stream":"c","value":1}"#; 4_300].join(",");
    let over = call(
        node.post("/bulk", format!("[{items},x]"))
            .header("X-Msg-Pad", "p".repeat(8_000)),
    )
    .await;
    refused(&over, StatusCode::PAYLOAD_TOO_LARGE);
    node.stop().await;
}

#[tokio::test]
async fn a_repeated_travelling_header_name_gets_400_and_stores_nothing() {
    let (_dir, node) = boot().await;
    let twice = call(
        node.post("/s/r", "1")
            .header("X-Msg-Tag", "a")
            .header("X-Msg-Tag", "b"),
    )
    .await;
    refused(&twice, StatusCode::BAD_REQUEST);
    let item = r#"[{"stream":"r","value":1,"headers":{"X-Msg-Tag":"a","x-msg-tag":"b"}}]"#;
    refused(
        &call(node.post("/bulk", item)).await,
        StatusCode::BAD_REQUEST,
    );
    let shared = call(
        node.post(
            "/bulk",
            r#"[{"stream":"r","value":1,"headers":{"x-msg-tag":"b"}}]"#,
        )
        .header("X-Msg-Tag", "a"),
    )
    .await;
    refused(&shared, StatusCode::BAD_REQUEST);
    let none = call(node.get("/s/r?group=g")).await;
    assert_eq!(none.status, StatusCode::NO_CONTENT);
    node.stop().await;
}

#[tokio::test]
async fn bulk_item_headers_that_are_not_visible_ascii_get_400() {
    let (_dir, node) = boot().await;
    for item in [
        r#"[{"stream":"c","value":1,"headers":{"traceparent":"\u0000\r\nX: y"}}]"#,
        r#"[{"stream":"c","value":1,"headers":{"x-msg-\u0001":"v"}}]"#,
    ] {
        refused(
            &call(node.post("/bulk", item)).await,
            StatusCode::BAD_REQUEST,
        );
    }
    let none = call(node.get("/s/c?group=g")).await;
    assert_eq!(none.status, StatusCode::NO_CONTENT);
    node.stop().await;
}

#[tokio::test]
async fn an_unknown_path_or_method_gets_a_json_error_with_next() {
    let (_dir, node) = boot().await;
    refused(&call(node.get("/nope")).await, StatusCode::NOT_FOUND);
    refused(&call(node.delete("/g/x/y")).await, StatusCode::NOT_FOUND);
    let put = reqwest::Client::new().put(format!("{}/s/x", node.url));
    refused(&call(put).await, StatusCode::METHOD_NOT_ALLOWED);
    node.stop().await;
}

#[tokio::test]
async fn a_bulk_item_header_named_twice_in_its_object_gets_400_and_stores_nothing() {
    let (_dir, node) = boot().await;
    let item = r#"[{"stream":"r","value":1,"headers":{"x-msg-tag":"a","x-msg-tag":"b"}}]"#;
    refused(
        &call(node.post("/bulk", item)).await,
        StatusCode::BAD_REQUEST,
    );
    let none = call(node.get("/s/r?group=g")).await;
    assert_eq!(none.status, StatusCode::NO_CONTENT);
    node.stop().await;
}

#[tokio::test]
async fn a_bulk_item_header_name_that_is_not_a_token_or_is_a_bare_prefix_gets_400() {
    let (_dir, node) = boot().await;
    for name in ["x-msg-a b", "x-msg-a:b", "x-msg-a\\tb", "ce-", "x-msg-", ""] {
        let item = format!(r#"[{{"stream":"c","value":1,"headers":{{"{name}":"v"}}}}]"#);
        let reply = call(node.post("/bulk", item)).await;
        assert_eq!(
            reply.status,
            StatusCode::BAD_REQUEST,
            "{name}: {}",
            reply.body
        );
        refused(&reply, StatusCode::BAD_REQUEST);
    }
    let none = call(node.get("/s/c?group=g")).await;
    assert_eq!(none.status, StatusCode::NO_CONTENT);
    node.stop().await;
}

#[tokio::test]
async fn a_request_header_that_is_a_bare_prefix_gets_400_and_stores_nothing() {
    let (_dir, node) = boot().await;
    for name in ["ce-", "x-msg-"] {
        for path in ["/s/q", "/bulk"] {
            let body = if path == "/bulk" {
                r#"[{"stream":"q","value":1}]"#
            } else {
                "1"
            };
            let reply = call(node.post(path, body).header(name, "v")).await;
            assert_eq!(
                reply.status,
                StatusCode::BAD_REQUEST,
                "{path} {name}: {}",
                reply.body
            );
            refused(&reply, StatusCode::BAD_REQUEST);
        }
    }
    let none = call(node.get("/s/q?group=g")).await;
    assert_eq!(none.status, StatusCode::NO_CONTENT);
    node.stop().await;
}

#[tokio::test]
async fn a_repeated_content_type_or_idempotency_key_gets_400_and_stores_nothing() {
    let (_dir, node) = boot().await;
    let types = call(
        node.post("/s/q", "1")
            .header("Content-Type", "application/json")
            .header("Content-Type", "text/plain"),
    )
    .await;
    refused(&types, StatusCode::BAD_REQUEST);
    let keys = call(
        node.post("/s/q", "1")
            .header("Idempotency-Key", "a")
            .header("Idempotency-Key", "b"),
    )
    .await;
    refused(&keys, StatusCode::BAD_REQUEST);
    let bulk = call(
        node.post("/bulk", r#"[{"stream":"q","value":1}]"#)
            .header("Idempotency-Key", "a")
            .header("Idempotency-Key", "b"),
    )
    .await;
    refused(&bulk, StatusCode::BAD_REQUEST);
    let none = call(node.get("/s/q?group=g")).await;
    assert_eq!(none.status, StatusCode::NO_CONTENT);
    node.stop().await;
}

#[tokio::test]
async fn disk_full_429_names_the_holding_groups_and_the_streams_with_no_group() {
    let fs = verdandi_sim::fault_fs::FaultFs::new(1, 1 << 40);
    let clock = verdandi_store::engine::TestClock::new(kit::START_MS);
    let node = kit::start_on(fs.clone(), std::path::Path::new("/data"), &clock).await;
    let first = call(node.post("/s/s", "a")).await;
    call(node.post("/s/s", "b")).await;
    call(node.post("/s/t", "c")).await;
    let take = call(node.get("/s/s?group=g&limit=1")).await;
    assert_eq!(taken(&take), ids(&first));
    let receipt = take.body["receipt"].as_str().expect("receipt").to_owned();
    let ack = json!({ "ack": ids(&first) }).to_string();
    let acked = call(node.post(&format!("/r/{receipt}"), ack)).await;
    assert_eq!(acked.status, StatusCode::OK);
    fs.set_capacity(fs.used() + (1 << 20));
    node.clock.advance(1);
    let full = call(node.post("/s/s", "d")).await;
    refused(&full, StatusCode::TOO_MANY_REQUESTS);
    let error = full.body["error"].as_str().expect("error");
    let next = full.body["next"].as_str().expect("next");
    assert!(error.contains("groups g "), "{error}");
    assert!(next.contains("DELETE /g/g"), "{next}");
    assert!(next.contains("DELETE /s/t?confirm=t"), "{next}");
    assert!(!next.contains("DELETE /s/s"), "{next}");
    node.stop().await;
}

#[tokio::test]
async fn ttl_message_is_not_delivered_after_expiry_and_is_dead_once_with_its_original_fields() {
    let (_dir, node) = boot().await;
    let first = call(node.get("/s/s?group=g")).await;
    assert_eq!(first.status, StatusCode::NO_CONTENT);
    let original = ids(&call(node.post("/s/s?ttl=10", "\"payload\"")).await);
    refused(
        &call(node.post("/s/s?ttl=soon", "1")).await,
        StatusCode::BAD_REQUEST,
    );
    node.clock.advance(11_000);
    let late = call(node.get("/s/s?group=g")).await;
    assert_eq!(late.status, StatusCode::NO_CONTENT);
    let dead = read_until(&node, "/s/s.dead.g", 1).await;
    let copy = &dead.body["messages"][0];
    assert_eq!(copy["stream"], "s.dead.g");
    assert_eq!(copy["value"], "payload");
    assert_ne!(copy["id"], original[0]);
    assert_eq!(
        copy["original"],
        json!({"id": original[0], "stream": "s", "reason": "expired"})
    );
    for _ in 0..4 {
        node.clock.advance(5_000);
        let again = call(node.get("/s/s?group=g")).await;
        assert_eq!(again.status, StatusCode::NO_CONTENT);
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    let once = call(node.get("/s/s.dead.g")).await;
    assert_eq!(taken(&once).len(), 1, "{}", once.body);
    node.stop().await;
}
