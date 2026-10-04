// allow: each test target uses a part of the shared kit
#[allow(dead_code)]
mod kit;

use std::ops::Bound;
use std::time::{Duration, UNIX_EPOCH};

use kit::{Node, Reply, START_MS, boot, call, ids, refused, start, taken};
use reqwest::StatusCode;
use serde_json::{Value, json};
use verdandi_core::Reason;
use verdandi_core::ids::MessageId;
use verdandi_store::engine::{Take, Taken, TestClock};
use verdandi_store::records::Message;

const DAY_MS: u64 = 24 * 3600 * 1000;

async fn stats(node: &Node, group: &str) -> Reply {
    call(node.get(&format!("/g/{group}"))).await
}

async fn take(node: &Node, path: &str) -> Reply {
    call(node.get(path)).await
}

fn receipt(reply: &Reply) -> String {
    reply.body["receipt"].as_str().expect("receipt").to_owned()
}

async fn settle(node: &Node, receipt: &str, body: Value) -> Reply {
    call(node.post(&format!("/r/{receipt}"), body.to_string())).await
}

async fn put(node: &Node, group: &str, body: &str) -> Reply {
    call(node.put(&format!("/g/{group}"), body.to_owned())).await
}

async fn dead_copies(node: &Node, stream: &str, probe: &str) -> Vec<Message> {
    let taken = node
        .handle
        .take(Take {
            group: probe.to_owned(),
            selector: stream.to_owned(),
            limit: 100,
            lease_ms: 1000,
            wait: false,
        })
        .await
        .expect("probe take");
    let Taken::Leased { messages, .. } = taken else {
        return Vec::new();
    };
    node.handle
        .read(&messages)
        .await
        .expect("probe read")
        .into_iter()
        .map(|m| m.expect("a readable copy"))
        .collect()
}

fn rfc3339(ms: u64) -> String {
    humantime::format_rfc3339_millis(UNIX_EPOCH + Duration::from_millis(ms)).to_string()
}

async fn forgotten(node: &Node, group: &str) {
    let gone = tokio::time::timeout(Duration::from_secs(10), async {
        while stats(node, group).await.status != StatusCode::NOT_FOUND {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(gone.is_ok(), "the group {group} was not forgotten");
}

#[tokio::test]
async fn provisional_group_counts_pending_in_flight_and_oldest_age_until_the_first_ack() {
    let (_dir, node) = boot().await;
    refused(&stats(&node, "g").await, StatusCode::NOT_FOUND);
    let published = ids(&call(node.post("/s/s", "[1,2,3]")).await);
    node.clock.advance(5000);
    let first = take(&node, "/s/s?group=g&limit=2").await;
    assert_eq!(taken(&first), published[..2]);
    let leased = stats(&node, "g").await;
    assert_eq!(leased.status, StatusCode::OK, "{}", leased.body);
    assert_eq!(
        leased.body,
        json!({"pending": 1, "in_flight": 2, "dead": 0, "oldest_age_s": 5.0, "provisional": true})
    );
    let acked = settle(&node, &receipt(&first), json!({"ack": [published[0]]})).await;
    assert_eq!(acked.status, StatusCode::OK);
    assert_eq!(
        stats(&node, "g").await.body,
        json!({"pending": 1, "in_flight": 1, "dead": 0, "oldest_age_s": 5.0, "provisional": false})
    );
    node.stop().await;
}

#[tokio::test]
async fn provisional_group_starts_at_the_oldest_held_message() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/s", "[1,2,3]")).await);
    let first = take(&node, "/s/s?group=late&limit=100").await;
    assert_eq!(taken(&first), published);
    node.stop().await;
}

#[tokio::test]
async fn provisional_group_with_no_ack_is_forgotten_after_24_hours_and_an_acking_group_is_not() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/s", "[1,2]")).await);
    take(&node, "/s/s?group=idle&limit=1").await;
    let busy = take(&node, "/s/s?group=busy&limit=1").await;
    settle(&node, &receipt(&busy), json!({"ack": [published[0]]})).await;
    node.clock.advance(DAY_MS + 1);
    forgotten(&node, "idle").await;
    let kept = stats(&node, "busy").await;
    assert_eq!(kept.status, StatusCode::OK, "{}", kept.body);
    assert_eq!(kept.body["provisional"], json!(false));
    let again = take(&node, "/s/s?group=idle&limit=100").await;
    assert_eq!(taken(&again), published);
    node.stop().await;
}

#[tokio::test]
async fn forget_ends_the_group_and_a_new_take_starts_at_the_oldest_held_message() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/s", "[1,2]")).await);
    let first = take(&node, "/s/s?group=g&limit=100").await;
    let old = receipt(&first);
    settle(&node, &old, json!({"ack": published})).await;
    assert_eq!(
        take(&node, "/s/s?group=g").await.status,
        StatusCode::NO_CONTENT
    );
    let deleted = call(node.delete("/g/g")).await;
    assert_eq!(deleted.status, StatusCode::OK, "{}", deleted.body);
    assert_eq!(deleted.body, json!({"deleted": "g"}));
    refused(&stats(&node, "g").await, StatusCode::NOT_FOUND);
    let again = take(&node, "/s/s?group=g&limit=100").await;
    assert_eq!(taken(&again), published);
    refused(
        &call(node.delete(&format!("/r/{old}"))).await,
        StatusCode::CONFLICT,
    );
    refused(
        &call(node.delete("/g/never-taken")).await,
        StatusCode::NOT_FOUND,
    );
    refused(
        &call(node.delete("/g/a.dead.b")).await,
        StatusCode::BAD_REQUEST,
    );
    node.stop().await;
}

#[tokio::test]
async fn replay_from_a_time_delivers_again_from_that_time() {
    let (_dir, node) = boot().await;
    let a = ids(&call(node.post("/s/s", "1")).await);
    node.clock.advance(10_000);
    let bc = ids(&call(node.post("/s/s", "[2,3]")).await);
    let all = take(&node, "/s/s?group=g").await;
    assert_eq!(taken(&all), [a.clone(), bc.clone()].concat());
    let acked = call(node.delete(&format!("/r/{}", receipt(&all)))).await;
    assert_eq!(acked.status, StatusCode::OK);
    assert_eq!(
        take(&node, "/s/s?group=g").await.status,
        StatusCode::NO_CONTENT
    );
    let body = json!({"from": rfc3339(START_MS + 10_000)}).to_string();
    let replayed = put(&node, "g", &body).await;
    assert_eq!(replayed.status, StatusCode::OK, "{}", replayed.body);
    let again = take(&node, "/s/s?group=g").await;
    assert_eq!(taken(&again), bc);
    node.stop().await;
}

#[tokio::test]
async fn replay_from_now_a_relative_time_or_a_message_id_means_after_that_point() {
    let (_dir, node) = boot().await;
    let a = ids(&call(node.post("/s/s", "1")).await);
    node.clock.advance(10_000);
    let b = ids(&call(node.post("/s/s", "2")).await);
    node.clock.advance(1000);
    let both = [a.clone(), b.clone()].concat();
    let cases = [
        ("now".to_owned(), vec![]),
        ("-5s".to_owned(), b.clone()),
        ("-1d".to_owned(), both.clone()),
        (a[0].clone(), b.clone()),
        (b[0].clone(), vec![]),
        (rfc3339(START_MS), both),
        (rfc3339(START_MS + 10_000), b),
    ];
    for (n, (from, expected)) in cases.into_iter().enumerate() {
        let group = format!("r{n}");
        let replayed = put(&node, &group, &json!({"from": from}).to_string()).await;
        assert_eq!(replayed.status, StatusCode::OK, "{from}: {}", replayed.body);
        let got = take(&node, &format!("/s/s?group={group}")).await;
        if expected.is_empty() {
            assert_eq!(got.status, StatusCode::NO_CONTENT, "from {from}");
        } else {
            assert_eq!(taken(&got), expected, "from {from}");
        }
    }
    node.stop().await;
}

#[tokio::test]
async fn replay_makes_a_settle_on_the_old_generation_get_409() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/s", "[1,2]")).await);
    let old = take(&node, "/s/s?group=g").await;
    let replayed = put(&node, "g", r#"{"from":"-1d"}"#).await;
    assert_eq!(replayed.status, StatusCode::OK, "{}", replayed.body);
    refused(
        &call(node.delete(&format!("/r/{}", receipt(&old)))).await,
        StatusCode::CONFLICT,
    );
    let new = take(&node, "/s/s?group=g").await;
    assert_eq!(taken(&new), published);
    let acked = call(node.delete(&format!("/r/{}", receipt(&new)))).await;
    assert_eq!(acked.status, StatusCode::OK);
    node.stop().await;
}

#[tokio::test]
async fn replay_two_at_once_settle_on_one_winner() {
    let (_dir, node) = boot().await;
    let p = ids(&call(node.post("/s/s", "[1,2,3,4]")).await);
    take(&node, "/s/s?group=g").await;
    let from = |n: usize| Bound::Excluded(p[n].parse::<MessageId>().expect("id"));
    let (x, y) = tokio::join!(
        node.handle.replay("g".to_owned(), from(0)),
        node.handle.replay("g".to_owned(), from(1))
    );
    assert_eq!((x, y), (Ok(()), Ok(())));
    let got = take(&node, "/s/s?group=g").await;
    assert_eq!(taken(&got), p[2..], "the higher generation wins");
    let acked = call(node.delete(&format!("/r/{}", receipt(&got)))).await;
    assert_eq!(acked.status, StatusCode::OK);
    assert_eq!(
        take(&node, "/s/s?group=g").await.status,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        stats(&node, "g").await.body,
        json!({"pending": 0, "in_flight": 0, "dead": 0, "oldest_age_s": 0.0, "provisional": false})
    );
    node.stop().await;
}

#[tokio::test]
async fn group_routes_with_an_invalid_group_name_get_400_that_names_the_group() {
    let (_dir, node) = boot().await;
    for name in ["a.dead.b", "a*", "a..b"] {
        let replies = [
            stats(&node, name).await,
            put(&node, name, r#"{"from":"now"}"#).await,
            call(node.delete(&format!("/g/{name}"))).await,
        ];
        for reply in replies {
            refused(&reply, StatusCode::BAD_REQUEST);
            let error = reply.body["error"].as_str().expect("error");
            assert!(error.contains("group"), "{name}: {error}");
            assert!(!error.contains("stream"), "{name}: {error}");
        }
    }
    node.stop().await;
}

#[tokio::test]
async fn replay_and_redrive_with_a_bad_request_get_400_with_next() {
    let (_dir, node) = boot().await;
    for (group, body) in [
        ("g", ""),
        ("g", "{}"),
        ("g", "[]"),
        ("g", r#"{"from":"soon"}"#),
        ("g", r#"{"from":"-7x"}"#),
        ("g", r#"{"from":5}"#),
        ("g", r#"{"from":"now","redrive":"dead"}"#),
        ("g", r#"{"redrive":"live"}"#),
        ("g", r#"{"from":"now","extra":1}"#),
        ("a.dead.b", r#"{"from":"now"}"#),
        ("a.dead.b", r#"{"redrive":"dead"}"#),
    ] {
        let reply = put(&node, group, body).await;
        refused(&reply, StatusCode::BAD_REQUEST);
    }
    node.stop().await;
}

#[tokio::test]
async fn redrive_delivers_dead_copies_again_and_a_second_death_lands_in_the_same_dead_stream() {
    let (_dir, node) = boot().await;
    let original = ids(&call(node.post("/s/s", "payload")).await);
    let first = take(&node, "/s/s?group=g").await;
    let buried = settle(
        &node,
        &receipt(&first),
        json!({"dead": [{"id": original[0], "error": "bad payload"}]}),
    )
    .await;
    assert_eq!(buried.status, StatusCode::OK, "{}", buried.body);
    assert_eq!(
        stats(&node, "g").await.body,
        json!({"pending": 0, "in_flight": 0, "dead": 1, "oldest_age_s": 0.0, "provisional": false})
    );
    let copies = dead_copies(&node, "s.dead.g", "probe1").await;
    assert_eq!(copies.len(), 1);
    let origin = copies[0].origin.clone().expect("origin");
    assert_eq!(origin.id.to_string(), original[0]);
    assert_eq!(origin.stream, "s");
    assert_eq!(origin.reason, Reason::Dead);
    assert_eq!(origin.error.as_deref(), Some("bad payload"));
    assert_eq!(copies[0].value, b"payload");
    assert_eq!(
        take(&node, "/s/s?group=g").await.status,
        StatusCode::NO_CONTENT
    );
    let redriven = put(&node, "g", r#"{"redrive":"dead"}"#).await;
    assert_eq!(redriven.status, StatusCode::OK, "{}", redriven.body);
    assert_eq!(
        stats(&node, "g").await.body["pending"],
        json!(1),
        "the dead copy is pending again"
    );
    let again = take(&node, "/s/s?group=g").await;
    assert_eq!(again.body["messages"][0]["stream"], json!("s.dead.g"));
    assert_eq!(again.body["messages"][0]["value"], json!("payload"));
    let second = taken(&again);
    let buried = settle(
        &node,
        &receipt(&again),
        json!({"dead": [{"id": second[0], "error": "still bad"}]}),
    )
    .await;
    assert_eq!(buried.status, StatusCode::OK, "{}", buried.body);
    let copies = dead_copies(&node, "s.dead.g", "probe2").await;
    assert_eq!(copies.len(), 2);
    let origin = copies[1].origin.clone().expect("origin");
    assert_eq!(origin.id.to_string(), original[0]);
    assert_eq!(origin.stream, "s");
    assert_eq!(origin.error.as_deref(), Some("still bad"));
    assert_eq!(
        stats(&node, "g").await.body["dead"],
        json!(1),
        "only the new dead copy counts"
    );
    assert_eq!(
        take(&node, "/s/s?group=g").await.status,
        StatusCode::NO_CONTENT
    );
    node.stop().await;
}

#[tokio::test]
async fn redrive_with_no_dead_copies_changes_nothing() {
    let (_dir, node) = boot().await;
    call(node.post("/s/s", "1")).await;
    take(&node, "/s/s?group=g").await;
    let before = stats(&node, "g").await.body;
    let redriven = put(&node, "g", r#"{"redrive":"dead"}"#).await;
    assert_eq!(redriven.status, StatusCode::OK, "{}", redriven.body);
    assert_eq!(stats(&node, "g").await.body, before);
    node.stop().await;
}

#[tokio::test]
async fn redrive_a_pattern_never_matches_a_dead_stream() {
    let (_dir, node) = boot().await;
    let original = ids(&call(node.post("/s/s", "payload")).await);
    let first = take(&node, "/s/s?group=g").await;
    settle(
        &node,
        &receipt(&first),
        json!({"dead": [{"id": original[0]}]}),
    )
    .await;
    let wide = take(&node, "/s/*?group=other&limit=100").await;
    assert_eq!(taken(&wide), original);
    let star = take(&node, "/s/*?group=g").await;
    assert_eq!(star.status, StatusCode::NO_CONTENT);
    let exact = take(&node, "/s/s.dead.g?group=reader").await;
    assert_eq!(exact.body["messages"][0]["stream"], json!("s.dead.g"));
    node.stop().await;
}

#[tokio::test]
async fn redrive_client_publish_to_a_dead_or_reserved_name_gets_400() {
    let (_dir, node) = boot().await;
    for path in ["/s/a.dead.g", "/s/_a", "/s/a.dead.g.dead.h"] {
        refused(&call(node.post(path, "x")).await, StatusCode::BAD_REQUEST);
    }
    refused(
        &call(node.post("/bulk", r#"[{"stream":"a.dead.g","value":1}]"#)).await,
        StatusCode::BAD_REQUEST,
    );
    node.stop().await;
}

#[tokio::test]
async fn the_429_next_names_group_and_stream_deletes_that_both_resolve() {
    let fs = verdandi_sim::fault_fs::FaultFs::new(1, 1 << 40);
    let clock = TestClock::new(START_MS);
    let node = kit::start_on(fs.clone(), std::path::Path::new("/data"), &clock).await;
    let first = call(node.post("/s/s", "a")).await;
    call(node.post("/s/s", "b")).await;
    call(node.post("/s/t", "c")).await;
    let got = take(&node, "/s/s?group=g&limit=1").await;
    let acked = settle(&node, &receipt(&got), json!({"ack": ids(&first)})).await;
    assert_eq!(acked.status, StatusCode::OK);
    fs.set_capacity(fs.used() + (1 << 20));
    node.clock.advance(1);
    let full = call(node.post("/s/s", "d")).await;
    refused(&full, StatusCode::TOO_MANY_REQUESTS);
    let next = full.body["next"].as_str().expect("next");
    let routes: Vec<&str> = next
        .split_once(": ")
        .expect("a list of calls")
        .1
        .split(", ")
        .collect();
    assert_eq!(routes, ["DELETE /g/g", "DELETE /s/t?confirm=t"]);
    for route in routes {
        let path = route.strip_prefix("DELETE ").expect("a DELETE");
        let reply = call(node.delete(path)).await;
        assert_eq!(reply.status, StatusCode::OK, "{route}: {}", reply.body);
    }
    node.stop().await;
}

#[tokio::test]
async fn replay_from_a_time_takes_any_rfc_3339_offset_and_a_lowercase_z() {
    let (_dir, node) = boot().await;
    call(node.post("/s/s", "1")).await;
    node.clock.advance(10_000);
    let b = ids(&call(node.post("/s/s", "2")).await);
    let at = START_MS + 10_000;
    let times = [
        rfc3339(at + 7_200_000).replace('Z', "+02:00"),
        rfc3339(at - 19_800_000).replace('Z', "-05:30"),
        rfc3339(at).replace('Z', "z").replace('T', "t"),
    ];
    for (n, from) in times.into_iter().enumerate() {
        let group = format!("o{n}");
        let replayed = put(&node, &group, &json!({"from": from}).to_string()).await;
        assert_eq!(replayed.status, StatusCode::OK, "{from}: {}", replayed.body);
        let got = take(&node, &format!("/s/s?group={group}")).await;
        assert_eq!(taken(&got), b, "from {from}");
    }
    refused(
        &put(&node, "g", r#"{"from":"2026-10-03T16:00:00+24:00"}"#).await,
        StatusCode::BAD_REQUEST,
    );
    node.stop().await;
}

#[tokio::test]
async fn replay_from_now_after_a_clock_step_back_replays_no_acked_message() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/s", "1")).await);
    let first = take(&node, "/s/s?group=g").await;
    let acked = settle(&node, &receipt(&first), json!({"ack": published})).await;
    assert_eq!(acked.status, StatusCode::OK, "{}", acked.body);
    node.clock.set(START_MS - 60_000);
    let replayed = put(&node, "g", r#"{"from":"now"}"#).await;
    assert_eq!(replayed.status, StatusCode::OK, "{}", replayed.body);
    assert_eq!(
        take(&node, "/s/s?group=g").await.status,
        StatusCode::NO_CONTENT
    );
    node.stop().await;
}

#[tokio::test]
async fn replay_from_now_then_a_restart_with_the_clock_behind_delivers_new_messages() {
    let (dir, node) = boot().await;
    let replayed = put(&node, "g", r#"{"from":"now"}"#).await;
    assert_eq!(replayed.status, StatusCode::OK, "{}", replayed.body);
    node.stop().await;
    let node = start(dir.path(), &TestClock::new(START_MS - 100_000)).await;
    let published = ids(&call(node.post("/s/s", "1")).await);
    let got = take(&node, "/s/s?group=g").await;
    assert_eq!(got.status, StatusCode::OK, "{}", got.body);
    assert_eq!(taken(&got), published);
    node.stop().await;
}

#[tokio::test]
async fn redrive_after_a_replay_from_now_delivers_the_dead_copies_made_before_it() {
    let (_dir, node) = boot().await;
    let original = ids(&call(node.post("/s/s", "payload")).await);
    let first = take(&node, "/s/s?group=g").await;
    let buried = settle(
        &node,
        &receipt(&first),
        json!({"dead": [{"id": original[0]}]}),
    )
    .await;
    assert_eq!(buried.status, StatusCode::OK, "{}", buried.body);
    node.clock.advance(1000);
    let replayed = put(&node, "g", r#"{"from":"now"}"#).await;
    assert_eq!(replayed.status, StatusCode::OK, "{}", replayed.body);
    let redriven = put(&node, "g", r#"{"redrive":"dead"}"#).await;
    assert_eq!(redriven.status, StatusCode::OK, "{}", redriven.body);
    let counted = stats(&node, "g").await.body;
    assert_eq!(
        (&counted["pending"], &counted["dead"]),
        (&json!(1), &json!(0)),
        "{counted}"
    );
    let again = take(&node, "/s/s?group=g").await;
    assert_eq!(
        again.body["messages"][0]["stream"],
        json!("s.dead.g"),
        "{}",
        again.body
    );
    node.stop().await;
}

#[tokio::test]
async fn group_stats_age_follows_the_oldest_unacked_message_and_a_stream_delete_clears_it() {
    let (_dir, node) = boot().await;
    let a = ids(&call(node.post("/s/s", "1")).await);
    node.clock.advance(5000);
    call(node.post("/s/s", "2")).await;
    node.clock.advance(5000);
    let first = take(&node, "/s/s?group=g&limit=1").await;
    assert_eq!(taken(&first), a);
    assert_eq!(stats(&node, "g").await.body["oldest_age_s"], json!(10.0));
    let acked = settle(&node, &receipt(&first), json!({"ack": a})).await;
    assert_eq!(acked.status, StatusCode::OK, "{}", acked.body);
    assert_eq!(stats(&node, "g").await.body["oldest_age_s"], json!(5.0));
    let deleted = call(node.delete("/s/s?confirm=s")).await;
    assert_eq!(deleted.status, StatusCode::OK, "{}", deleted.body);
    assert_eq!(
        stats(&node, "g").await.body,
        json!({"pending": 0, "in_flight": 0, "dead": 0, "oldest_age_s": 0.0, "provisional": false})
    );
    node.stop().await;
}

#[tokio::test]
async fn redrive_and_forget_of_an_unknown_group_get_404_and_a_replay_creates_it() {
    let (_dir, node) = boot().await;
    refused(
        &put(&node, "never", r#"{"redrive":"dead"}"#).await,
        StatusCode::NOT_FOUND,
    );
    refused(&call(node.delete("/g/never")).await, StatusCode::NOT_FOUND);
    let created = put(&node, "never", r#"{"from":"-1d"}"#).await;
    assert_eq!(created.status, StatusCode::OK, "{}", created.body);
    assert_eq!(stats(&node, "never").await.status, StatusCode::OK);
    let redriven = put(&node, "never", r#"{"redrive":"dead"}"#).await;
    assert_eq!(redriven.status, StatusCode::OK, "{}", redriven.body);
    let deleted = call(node.delete("/g/never")).await;
    assert_eq!(deleted.status, StatusCode::OK, "{}", deleted.body);
    refused(&call(node.delete("/g/never")).await, StatusCode::NOT_FOUND);
    node.stop().await;
}

#[tokio::test]
async fn group_writes_from_another_site_get_403() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/s", "1")).await);
    let leased = take(&node, "/s/s?group=g").await;
    for request in [
        node.put("/g/g", r#"{"from":"now"}"#)
            .header("Origin", "http://evil.example"),
        node.put("/g/g", r#"{"redrive":"dead"}"#)
            .header("Sec-Fetch-Site", "cross-site"),
        node.delete("/g/g").header("Origin", "http://evil.example"),
        node.delete("/g/g").header("Sec-Fetch-Site", "cross-site"),
    ] {
        refused(&call(request).await, StatusCode::FORBIDDEN);
    }
    let acked = settle(&node, &receipt(&leased), json!({"ack": published})).await;
    assert_eq!(acked.status, StatusCode::OK, "{}", acked.body);
    node.stop().await;
}
