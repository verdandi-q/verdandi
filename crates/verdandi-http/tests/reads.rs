// allow: each test target uses a part of the shared kit
#[allow(dead_code)]
mod kit;

use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, UNIX_EPOCH};

use kit::{Node, START_MS, boot, call, ids, refused, scratch, start_as, start_on, taken};
use reqwest::{Client, Method, Response, StatusCode};
use serde_json::{Value, json};
use tokio::sync::oneshot;
use verdandi_sim::fault_fs::FaultFs;
use verdandi_store::engine::{Engine, Error, OsRandom, Random, TestClock};
use verdandi_store::fs::RealFs;

struct Incarnation(u64);

impl Random for Incarnation {
    fn fill(&mut self, bytes: &mut [u8]) -> Result<(), Error> {
        for (byte, value) in bytes
            .iter_mut()
            .zip(self.0.to_le_bytes().into_iter().cycle())
        {
            *byte = value;
        }
        Ok(())
    }
}

struct Frames {
    response: Response,
    buf: String,
}

impl Frames {
    fn new(response: Response) -> Self {
        Self {
            response,
            buf: String::new(),
        }
    }

    async fn next_within(&mut self, end: &str, wait: Duration) -> Option<String> {
        loop {
            if let Some(at) = self.buf.find(end) {
                let frame = self.buf[..at].to_owned();
                self.buf.drain(..at + end.len());
                return Some(frame);
            }
            let chunk = tokio::time::timeout(wait, self.response.chunk())
                .await
                .expect("no frame in time")
                .expect("chunk")?;
            self.buf
                .push_str(std::str::from_utf8(&chunk).expect("text"));
        }
    }

    async fn next(&mut self, end: &str) -> Option<String> {
        self.next_within(end, Duration::from_secs(10)).await
    }

    async fn silent(&mut self, end: &str) -> bool {
        tokio::time::timeout(Duration::from_millis(400), self.next(end))
            .await
            .is_err()
    }
}

async fn follow(node: &Node, path: &str) -> Frames {
    let response = node.get(path).send().await.expect("send");
    assert_eq!(response.status(), StatusCode::OK);
    Frames::new(response)
}

async fn events(node: &Node, path: &str, last: Option<&str>) -> Frames {
    let mut request = node.get(path).header("Accept", "text/event-stream");
    if let Some(last) = last {
        request = request.header("Last-Event-ID", last);
    }
    let response = request.send().await.expect("send");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    Frames::new(response)
}

fn event(frame: &str) -> (String, Value) {
    let field = |name: &str| {
        frame
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .unwrap_or_else(|| panic!("no {name} in {frame:?}"))
            .to_owned()
    };
    (
        field("id: "),
        serde_json::from_str(&field("data: ")).expect("JSON data"),
    )
}

fn rfc3339(ms: u64) -> String {
    humantime::format_rfc3339_millis(UNIX_EPOCH + Duration::from_millis(ms)).to_string()
}

fn after(ms: u64) -> String {
    format!("{:012x}-ffffffffffffffff-ffff", ms - 1)
}

fn line(text: &str) -> Value {
    serde_json::from_str(text).expect("one JSON line")
}

fn corrupt(dir: &Path, marker: &[u8]) {
    for entry in std::fs::read_dir(dir).expect("data dir") {
        let path = entry.expect("entry").path();
        let data = std::fs::read(&path).expect("read");
        if let Some(at) = data.windows(marker.len()).position(|w| w == marker) {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .expect("open");
            file.write_all_at(b"#", at as u64).expect("flip");
            return;
        }
    }
    panic!("no file holds the marker");
}

#[tokio::test]
async fn read_gives_the_oldest_messages_with_limit_and_a_next_link_that_resumes_after_the_last_id()
{
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/s", "[1,2,3,4,5]")).await);
    let first = call(node.get("/s/s?limit=2")).await;
    assert_eq!(first.status, StatusCode::OK, "{}", first.body);
    assert_eq!(taken(&first), published[..2]);
    let message = &first.body["messages"][0];
    assert_eq!(message["stream"], "s");
    assert_eq!(message["value"], 1);
    assert_eq!(message["headers"], json!({}));
    assert!(message["at"].is_string());
    assert!(message.get("attempt").is_none(), "{message}");
    let next = first.body["next"].as_str().expect("next");
    assert_eq!(next, format!("/s/s?from={}&limit=2", published[1]));
    let second = call(node.get(next)).await;
    assert_eq!(taken(&second), published[2..4]);
    let third = call(node.get(second.body["next"].as_str().expect("next"))).await;
    assert_eq!(taken(&third), published[4..]);
    let last = call(node.get(third.body["next"].as_str().expect("next"))).await;
    assert_eq!(last.status, StatusCode::OK);
    assert!(taken(&last).is_empty());
    assert_eq!(last.body["next"], third.body["next"]);
    let all = call(node.get("/s/s")).await;
    assert_eq!(taken(&all), published);
    assert_eq!(
        all.body["next"],
        format!("/s/s?from={}&limit=100", published[4])
    );
    let unread = call(node.get("/s/nothing")).await;
    assert_eq!(unread.status, StatusCode::OK);
    assert_eq!(
        unread.body,
        json!({"messages": [], "next": "/s/nothing?limit=100"})
    );
    node.stop().await;
}

#[tokio::test]
async fn read_from_a_time_a_relative_age_now_or_a_message_id() {
    let (_dir, node) = boot().await;
    let a = ids(&call(node.post("/s/s", "\"a\"")).await);
    node.clock.advance(60_000);
    let b = ids(&call(node.post("/s/s", "\"b\"")).await);
    node.clock.advance(60_000);
    let c = ids(&call(node.post("/s/s", "\"c\"")).await);
    let at = |ms: u64| format!("/s/s?from={}", rfc3339(ms));
    let from_time = call(node.get(&at(START_MS + 60_000))).await;
    assert_eq!(taken(&from_time), [b.clone(), c.clone()].concat());
    let offset = rfc3339(START_MS + 60_000 + 7_200_000).replace('Z', "%2B02:00");
    let with_offset = call(node.get(&format!("/s/s?from={offset}"))).await;
    assert_eq!(taken(&with_offset), [b.clone(), c.clone()].concat());
    let lower = call(node.get(&format!("/s/s?from={}", rfc3339(START_MS).to_lowercase()))).await;
    assert_eq!(taken(&lower), [a, b.clone(), c.clone()].concat());
    let relative = call(node.get("/s/s?from=-90s")).await;
    assert_eq!(taken(&relative), [b.clone(), c.clone()].concat());
    let after_id = call(node.get(&format!("/s/s?from={}", b[0]))).await;
    assert_eq!(taken(&after_id), c);
    let now = call(node.get("/s/s?from=now")).await;
    assert!(taken(&now).is_empty());
    let resume = now.body["next"].as_str().expect("next");
    assert_eq!(resume, format!("/s/s?from={}&limit=100", c[0]));
    node.clock.advance(1_000);
    let d = ids(&call(node.post("/s/s", "\"d\"")).await);
    assert_eq!(taken(&call(node.get(resume)).await), d);
    let future = call(node.get("/s/s?from=2099-01-01T00:00:00%2B02:00&limit=7")).await;
    assert!(taken(&future).is_empty());
    assert_eq!(
        future.body["next"],
        format!("/s/s?from={}&limit=7", after(4_070_901_600_000))
    );
    node.stop().await;
}

#[tokio::test]
async fn read_from_now_after_a_restart_with_a_lower_incarnation_skips_every_old_message() {
    let dir = scratch();
    let clock = TestClock::new(START_MS);
    let old = start_as(
        RealFs,
        dir.path(),
        &clock,
        Box::new(Incarnation(u64::MAX - 1)),
    )
    .await;
    call(old.post("/s/s", "1")).await;
    old.stop().await;
    let node = start_as(RealFs, dir.path(), &clock, Box::new(Incarnation(1))).await;
    let now = call(node.get("/s/s?from=now")).await;
    assert!(taken(&now).is_empty(), "{}", now.body);
    assert_eq!(
        now.body["next"],
        format!("/s/s?from={}&limit=100", after(START_MS + 1))
    );
    node.stop().await;
}

#[tokio::test]
async fn read_gives_json_text_and_base64_values_with_the_travelling_headers() {
    let (_dir, node) = boot().await;
    let pretty = ids(&call(node.post("/s/v", "{\n  \"a\": [1, 2]\n}")).await);
    let text = ids(&call(
        node.post("/s/v", "plain words")
            .header("Content-Type", "text/plain")
            .header("X-Msg-Tag", "t1"),
    )
    .await);
    let bytes = ids(&call(
        node.post("/s/v", vec![0xff, 0xfe, 0x00])
            .header("Content-Type", "application/octet-stream"),
    )
    .await);
    let read = call(node.get("/s/v")).await;
    assert_eq!(taken(&read), [pretty, text, bytes].concat());
    let messages = &read.body["messages"];
    assert_eq!(messages[0]["value"], json!({"a": [1, 2]}));
    assert_eq!(messages[1]["value"], "plain words");
    assert_eq!(messages[1]["headers"]["x-msg-tag"], "t1");
    assert_eq!(messages[2]["value_base64"], "//4A");
    assert!(messages[2].get("value").is_none());
    node.stop().await;
}

#[tokio::test]
async fn read_with_a_last_event_id_that_is_not_text_gets_400_naming_the_header() {
    let (_dir, node) = boot().await;
    let bad = reqwest::header::HeaderValue::from_bytes(&[0xff, 0xfe]).expect("header value");
    let reply = call(node.get("/s/s").header("Last-Event-ID", bad)).await;
    refused(&reply, StatusCode::BAD_REQUEST);
    for field in ["error", "next"] {
        let text = reply.body[field].as_str().expect(field);
        assert!(text.contains("Last-Event-ID"), "{field}: {text}");
    }
    node.stop().await;
}

#[tokio::test]
async fn read_with_a_bad_request_gets_400_with_next() {
    let (_dir, node) = boot().await;
    for path in [
        "/s/o.*",
        "/s/a..b",
        "/s/s?limit=10001",
        "/s/s?limit=0",
        "/s/s?limit=x",
        "/s/s?from=garbage",
        "/s/s?from=",
        "/s/s?wait=5",
        "/s/s?lease=5",
        "/s/s?max_bytes=5",
        "/s/s?follow=yes",
        "/s/s?cursor=1",
        "/s/s?group=g&from=now",
        "/s/s?group=g&follow",
    ] {
        refused(&call(node.get(path)).await, StatusCode::BAD_REQUEST);
    }
    let pattern = call(node.get("/s/o.*")).await;
    assert!(
        pattern.body["next"]
            .as_str()
            .expect("next")
            .contains("read each stream, or take as a group")
    );
    node.stop().await;
}

#[tokio::test]
async fn read_leases_nothing_and_creates_no_group() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/s", "[1,2]")).await);
    call(node.get("/s/s")).await;
    call(node.get("/s/s?from=-1d&limit=1")).await;
    assert_eq!(call(node.get("/g/g")).await.status, StatusCode::NOT_FOUND);
    let take = call(node.get("/s/s?group=g")).await;
    assert_eq!(taken(&take), published);
    assert_eq!(take.body["messages"][0]["attempt"], 1);
    let again = call(node.get("/s/s")).await;
    assert_eq!(taken(&again), published);
    assert_eq!(
        call(node.get("/s/s?group=g")).await.status,
        StatusCode::NO_CONTENT
    );
    node.stop().await;
}

#[tokio::test]
async fn read_of_a_dead_stream_by_name_is_allowed() {
    let (_dir, node) = boot().await;
    let none = call(node.get("/s/s.dead.g")).await;
    assert_eq!(none.status, StatusCode::OK);
    assert!(taken(&none).is_empty());
    let reserved = call(node.get("/s/_x")).await;
    assert_eq!(reserved.status, StatusCode::OK);
    node.stop().await;
}

#[tokio::test]
async fn read_pages_stop_at_16_mib_and_the_next_link_gives_the_rest() {
    let (_dir, node) = boot().await;
    let value = format!("\"{}\"", "v".repeat(900_000));
    let mut published = Vec::new();
    for _ in 0..20 {
        published.extend(ids(&call(node.post("/s/big", value.clone())).await));
    }
    let first = call(node.get("/s/big")).await;
    assert_eq!(first.status, StatusCode::OK);
    let count = taken(&first).len();
    assert!(
        (1..20).contains(&count),
        "the first page holds {count} messages"
    );
    assert_eq!(taken(&first), published[..count]);
    let rest = call(node.get(first.body["next"].as_str().expect("next"))).await;
    assert_eq!(taken(&rest), published[count..]);
    node.stop().await;
}

#[tokio::test]
async fn read_next_link_of_an_empty_page_holds_the_resolved_id_of_from() {
    let (_dir, node) = boot().await;
    let page = call(node.get("/s/s?from=-1h%2030m&limit=5")).await;
    assert_eq!(page.status, StatusCode::OK, "{}", page.body);
    assert!(taken(&page).is_empty());
    let next = page.body["next"].as_str().expect("next");
    assert_eq!(
        next,
        format!("/s/s?from={}&limit=5", after(START_MS - 5_400_000))
    );
    let published = ids(&call(node.post("/s/s", "1")).await);
    assert_eq!(taken(&call(node.get(next)).await), published);
    node.stop().await;
}

#[tokio::test]
async fn read_pages_count_the_reply_bytes_so_base64_values_stop_at_16_mib() {
    let (_dir, node) = boot().await;
    let mut published = Vec::new();
    for n in 0..16u8 {
        let value = vec![0xff - n; 1_000_000];
        published.extend(ids(&call(
            node.post("/s/bin", value)
                .header("Content-Type", "application/octet-stream"),
        )
        .await));
    }
    let first = call(node.get("/s/bin")).await;
    assert_eq!(first.status, StatusCode::OK);
    assert!(
        first.bytes.len() <= 16 << 20,
        "the page is {} bytes",
        first.bytes.len()
    );
    let count = taken(&first).len();
    assert_eq!(taken(&first), published[..count]);
    let rest = call(node.get(first.body["next"].as_str().expect("next"))).await;
    assert_eq!(taken(&rest), published[count..]);
    node.stop().await;
}

#[tokio::test]
async fn read_skips_a_message_whose_run_is_damaged_and_still_moves_on() {
    let (dir, node) = boot().await;
    let marker = "damaged-read-marker";
    let lost = ids(&call(node.post("/s/u", marker)).await);
    let kept = ids(&call(node.post("/s/u", "1")).await);
    corrupt(dir.path(), marker.as_bytes());
    let page = call(node.get("/s/u?limit=1")).await;
    assert!(taken(&page).is_empty(), "{}", page.body);
    assert_eq!(page.body["next"], format!("/s/u?from={}&limit=1", lost[0]));
    assert_eq!(
        taken(&call(node.get(page.body["next"].as_str().expect("next"))).await),
        kept
    );
    for _ in 0..2 {
        let read = call(node.get("/s/u")).await;
        assert_eq!(taken(&read), kept, "{}", read.body);
    }
    let warned = tokio::time::timeout(Duration::from_secs(10), async {
        while !call(node.get("/")).await.body["warnings"]
            .to_string()
            .contains("damaged log range")
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(warned.is_ok(), "the damaged range has no warning");
    node.stop().await;
}

#[tokio::test]
async fn follow_streams_the_history_then_each_new_message_as_one_json_line() {
    let (_dir, node) = boot().await;
    let old = ids(&call(node.post("/s/s", "{\n  \"pretty\": [1,\n 2]\n}")).await);
    let mut lines = follow(&node, "/s/s?follow").await;
    let first = lines.next("\n").await.expect("a line");
    let first: Value = serde_json::from_str(&first).expect("one JSON line");
    assert_eq!(first["id"], old[0]);
    assert_eq!(first["value"], json!({"pretty": [1, 2]}));
    assert!(lines.silent("\n").await, "nothing else is there yet");
    let new = ids(&call(node.post("/s/s", "[7,8]")).await);
    for id in &new {
        let line = lines.next("\n").await.expect("a line");
        let line: Value = serde_json::from_str(&line).expect("JSON");
        assert_eq!(line["id"], *id);
    }
    drop(lines);
    node.stop().await;
}

#[tokio::test]
async fn follow_with_a_small_limit_gives_every_page_in_order() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/s", "[1,2,3,4,5]")).await);
    let mut lines = follow(&node, "/s/s?follow&limit=2").await;
    for id in &published {
        assert_eq!(line(&lines.next("\n").await.expect("a line"))["id"], *id);
    }
    assert!(lines.silent("\n").await);
    let new = ids(&call(node.post("/s/s", "[6,7,8]")).await);
    for id in &new {
        assert_eq!(line(&lines.next("\n").await.expect("a line"))["id"], *id);
    }
    drop(lines);
    node.stop().await;
}

#[tokio::test]
async fn follow_hides_the_messages_before_a_stream_delete() {
    let (_dir, node) = boot().await;
    call(node.post("/s/s", "[1,2]")).await;
    assert_eq!(
        call(node.delete("/s/s?confirm=s")).await.status,
        StatusCode::OK
    );
    let kept = ids(&call(node.post("/s/s", "3")).await);
    let mut lines = follow(&node, "/s/s?follow").await;
    assert_eq!(
        line(&lines.next("\n").await.expect("a line"))["id"],
        kept[0]
    );
    assert!(lines.silent("\n").await);
    drop(lines);
    node.stop().await;
}

#[tokio::test]
async fn follow_skips_a_message_whose_run_is_damaged_and_still_moves_on() {
    let (dir, node) = boot().await;
    let marker = "damaged-follow-marker";
    call(node.post("/s/u", marker)).await;
    let kept = ids(&call(node.post("/s/u", "1")).await);
    corrupt(dir.path(), marker.as_bytes());
    let mut lines = follow(&node, "/s/u?follow&limit=1").await;
    assert_eq!(
        line(&lines.next("\n").await.expect("a line"))["id"],
        kept[0]
    );
    let new = ids(&call(node.post("/s/u", "2")).await);
    assert_eq!(line(&lines.next("\n").await.expect("a line"))["id"], new[0]);
    drop(lines);
    node.stop().await;
}

#[tokio::test]
async fn follow_response_is_ndjson_and_from_now_skips_the_history() {
    let (_dir, node) = boot().await;
    call(node.post("/s/s", "1")).await;
    let response = node.get("/s/s?from=now&follow").send().await.expect("send");
    assert_eq!(response.headers()["content-type"], "application/x-ndjson");
    let mut lines = Frames::new(response);
    assert!(lines.silent("\n").await);
    let new = ids(&call(node.post("/s/s", "2")).await);
    let line = lines.next("\n").await.expect("a line");
    let line: Value = serde_json::from_str(&line).expect("JSON");
    assert_eq!(line["id"], new[0]);
    drop(lines);
    node.stop().await;
}

#[tokio::test]
async fn follow_leases_nothing_and_changes_no_state() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/s", "[1,2]")).await);
    let mut lines = follow(&node, "/s/s?follow").await;
    for _ in &published {
        lines.next("\n").await.expect("a line");
    }
    assert_eq!(call(node.get("/g/g")).await.status, StatusCode::NOT_FOUND);
    let take = call(node.get("/s/s?group=g")).await;
    assert_eq!(taken(&take), published);
    assert_eq!(take.body["messages"][0]["attempt"], 1);
    drop(lines);
    node.stop().await;
}

#[tokio::test]
async fn follow_sse_gives_the_message_id_as_the_event_id_and_resumes_by_last_event_id() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/s", "[1,2,3]")).await);
    let mut stream = events(&node, "/s/s?follow", None).await;
    for (n, id) in published.iter().enumerate() {
        let (event_id, data) = event(&stream.next("\n\n").await.expect("an event"));
        assert_eq!(event_id, *id);
        assert_eq!(data["id"], *id);
        assert_eq!(data["value"], n + 1);
    }
    drop(stream);
    let mut resumed = events(&node, "/s/s?from=now&follow", Some(&published[0])).await;
    let start = resumed.next("\n\n").await.expect("the start event");
    assert_eq!(start, format!("id: {}", published[0]));
    for id in &published[1..] {
        let (event_id, _) = event(&resumed.next("\n\n").await.expect("an event"));
        assert_eq!(event_id, *id);
    }
    assert!(resumed.silent("\n\n").await);
    let new = ids(&call(node.post("/s/s", "4")).await);
    let (event_id, data) = event(&resumed.next("\n\n").await.expect("an event"));
    assert_eq!(event_id, new[0]);
    assert_eq!(data["value"], 4);
    drop(resumed);
    node.stop().await;
}

#[tokio::test]
async fn follow_sse_from_now_sends_the_resolved_from_as_the_first_event_id_so_a_reconnect_misses_nothing()
 {
    let (_dir, node) = boot().await;
    let old = ids(&call(node.post("/s/s", "1")).await);
    let mut stream = events(&node, "/s/s?from=now&follow", None).await;
    let start = stream.next("\n\n").await.expect("the start event");
    assert_eq!(start, format!("id: {}", old[0]));
    assert!(stream.silent("\n\n").await);
    drop(stream);
    let gap = ids(&call(node.post("/s/s", "2")).await);
    let last = start.strip_prefix("id: ").expect("an id");
    let mut resumed = events(&node, "/s/s?from=now&follow", Some(last)).await;
    resumed.next("\n\n").await.expect("the start event");
    let (id, data) = event(&resumed.next("\n\n").await.expect("an event"));
    assert_eq!(id, gap[0]);
    assert_eq!(data["value"], 2);
    drop(resumed);
    node.stop().await;
}

#[tokio::test]
async fn follow_sse_without_follow_is_a_plain_read() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/s", "1")).await);
    let read = call(node.get("/s/s").header("Accept", "text/event-stream")).await;
    assert_eq!(read.status, StatusCode::OK);
    assert_eq!(taken(&read), published);
    node.stop().await;
}

#[tokio::test]
async fn follow_sse_sends_keep_alive_comments_while_idle() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/s", "1")).await);
    let mut stream = events(&node, "/s/s?follow", None).await;
    let (id, _) = event(&stream.next("\n\n").await.expect("an event"));
    assert_eq!(id, published[0]);
    tokio::time::pause();
    let comment = stream
        .next_within("\n\n", Duration::from_secs(60))
        .await
        .expect("a keep-alive");
    tokio::time::resume();
    assert!(comment.starts_with(':'), "{comment:?}");
    drop(stream);
    node.stop().await;
}

#[tokio::test]
async fn follow_shows_no_message_before_its_block_is_durable() {
    let fs = FaultFs::new(7, 1 << 40);
    let clock = TestClock::new(START_MS);
    let node = start_on(fs.clone(), Path::new("/data"), &clock).await;
    let mut lines = follow(&node, "/s/s?follow").await;
    let mut stream = events(&node, "/s/s?follow", None).await;
    fs.hold_syncs();
    let publish = tokio::spawn(call(node.post("/s/s", "1")));
    tokio::time::timeout(Duration::from_secs(10), async {
        while fs.syncs_held() == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the block waits for its sync");
    assert!(taken(&call(node.get("/s/s")).await).is_empty());
    assert!(lines.silent("\n").await);
    assert!(stream.silent("\n\n").await);
    fs.release_syncs();
    let published = ids(&publish.await.expect("join"));
    assert_eq!(taken(&call(node.get("/s/s")).await), published);
    let line: Value = serde_json::from_str(&lines.next("\n").await.expect("a line")).expect("JSON");
    assert_eq!(line["id"], published[0]);
    let (id, _) = event(&stream.next("\n\n").await.expect("an event"));
    assert_eq!(id, published[0]);
    drop((lines, stream));
    node.stop().await;
}

#[tokio::test]
async fn follow_stops_cleanly_when_the_client_disconnects() {
    let (_dir, node) = boot().await;
    call(node.post("/s/s", "1")).await;
    let mut lines = follow(&node, "/s/s?follow").await;
    lines.next("\n").await.expect("a line");
    let mut stream = events(&node, "/s/s?follow", None).await;
    stream.next("\n\n").await.expect("an event");
    let idle = follow(&node, "/s/idle?follow").await;
    drop((lines, stream, idle));
    node.stop().await;
}

#[tokio::test]
async fn follow_ends_at_a_stop_like_a_long_poll_does() {
    let dir = scratch();
    let clock = TestClock::new(START_MS);
    let (engine, handle) = Engine::open(RealFs, dir.path(), Arc::new(clock), Box::new(OsRandom))
        .await
        .expect("open");
    let engine = tokio::spawn(engine.run());
    let (listener, url) = kit::listen().await;
    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(verdandi_http::serve(
        listener,
        Arc::new(OnceLock::from(handle)),
        async move {
            stopped.await.ok();
        },
    ));
    let client = Client::new();
    let published = client
        .post(format!("{url}/s/s"))
        .body("1")
        .send()
        .await
        .expect("publish");
    assert_eq!(published.status(), StatusCode::CREATED);
    let mut lines = Frames::new(
        client
            .get(format!("{url}/s/s?follow"))
            .send()
            .await
            .expect("send"),
    );
    lines.next("\n").await.expect("a line");
    let mut stream = Frames::new(
        client
            .get(format!("{url}/s/s?follow"))
            .header("Accept", "text/event-stream")
            .send()
            .await
            .expect("send"),
    );
    stream.next("\n\n").await.expect("an event");
    let mut waiting = Frames::new(
        client
            .get(format!("{url}/s/empty?follow"))
            .send()
            .await
            .expect("send"),
    );
    assert!(waiting.silent("\n").await);
    stop.send(()).expect("stop");
    assert!(lines.next("\n").await.is_none());
    assert!(stream.next("\n\n").await.is_none());
    assert!(waiting.next("\n").await.is_none());
    drop(client);
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .expect("the server ends")
        .expect("join")
        .expect("clean stop");
    tokio::time::timeout(Duration::from_secs(10), engine)
        .await
        .expect("the engine ends")
        .expect("join")
        .expect("clean stop");
}

#[tokio::test]
async fn cross_origin_plain_reads_are_allowed_with_cors_headers_for_get_and_head_only() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/s", "1")).await);
    let read = call(node.get("/s/s").header("Origin", "http://evil.example")).await;
    assert_eq!(read.status, StatusCode::OK);
    assert_eq!(taken(&read), published);
    assert_eq!(read.headers["access-control-allow-origin"], "*");
    let site = call(
        node.get("/s/s")
            .header("Origin", "http://evil.example")
            .header("Sec-Fetch-Site", "cross-site"),
    )
    .await;
    assert_eq!(site.status, StatusCode::OK);
    let client = Client::new();
    let head = client
        .head(format!("{}/s/s", node.url))
        .header("Origin", "http://evil.example")
        .send()
        .await
        .expect("head");
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(head.headers()["access-control-allow-origin"], "*");
    let preflight = client
        .request(Method::OPTIONS, format!("{}/s/s", node.url))
        .header("Origin", "http://evil.example")
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await
        .expect("preflight");
    assert!(preflight.status().is_success(), "{}", preflight.status());
    let allowed = preflight.headers()["access-control-allow-methods"]
        .to_str()
        .expect("text")
        .to_owned();
    assert!(
        allowed.contains("GET") && allowed.contains("HEAD"),
        "{allowed}"
    );
    for method in ["POST", "PUT", "DELETE", "PATCH"] {
        assert!(!allowed.contains(method), "{allowed}");
    }
    drop((head, preflight, client));
    node.stop().await;
}

#[tokio::test]
async fn cross_origin_writes_and_takes_get_403_with_next_and_change_nothing() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/s", "1")).await);
    refused(
        &call(
            node.post("/s/s", "2")
                .header("Content-Type", "text/plain")
                .header("Origin", "http://evil.example"),
        )
        .await,
        StatusCode::FORBIDDEN,
    );
    refused(
        &call(
            node.delete("/s/s?confirm=s")
                .header("Sec-Fetch-Site", "cross-site"),
        )
        .await,
        StatusCode::FORBIDDEN,
    );
    refused(
        &call(
            node.get("/s/s?group=g")
                .header("Origin", "http://evil.example"),
        )
        .await,
        StatusCode::FORBIDDEN,
    );
    assert_eq!(taken(&call(node.get("/s/s")).await), published);
    assert_eq!(call(node.get("/g/g")).await.status, StatusCode::NOT_FOUND);
    node.stop().await;
}

#[tokio::test]
async fn cross_origin_host_other_than_localhost_or_an_ip_literal_gets_403_with_next() {
    let (_dir, node) = boot().await;
    refused(
        &call(node.get("/s/s").header("Host", "evil.example")).await,
        StatusCode::FORBIDDEN,
    );
    refused(
        &call(node.get("/s/s").header("Host", "evil.example:8080")).await,
        StatusCode::FORBIDDEN,
    );
    for host in ["localhost:8080", "127.0.0.1:8080", "[::1]:8080"] {
        let read = call(node.get("/s/s").header("Host", host)).await;
        assert_eq!(read.status, StatusCode::OK, "{host}: {}", read.body);
    }
    node.stop().await;
}
