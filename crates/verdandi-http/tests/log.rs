// allow: each test target uses a part of the shared kit
#[allow(dead_code)]
mod kit;

use kit::{boot, call, capture_log, ids};
use reqwest::StatusCode;
use serde_json::json;

fn settles(lines: &[String]) -> Vec<&String> {
    lines
        .iter()
        .filter(|line| line.contains("verdandi_http::settle: settle "))
        .collect()
}

#[tokio::test]
async fn a_request_gives_one_info_line_with_method_uri_and_status() {
    let (_dir, node) = boot().await;
    let (_log, lines) = capture_log();
    assert_eq!(call(node.get("/livez")).await.status, StatusCode::OK);
    assert_eq!(
        call(node.post("/s/a", "1")).await.status,
        StatusCode::CREATED
    );
    assert_eq!(
        call(node.get("/nope?x=1")).await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        lines.lines(),
        [
            "INFO verdandi_http: request method=GET uri=/livez status=200",
            "INFO verdandi_http: request method=POST uri=/s/a status=201",
            "INFO verdandi_http: request method=GET uri=/nope?x=1 status=404",
        ]
    );
    node.stop().await;
}

#[tokio::test]
async fn a_refused_request_is_logged_with_its_status() {
    let (_dir, node) = boot().await;
    let (_log, lines) = capture_log();
    let refused = call(node.get("/livez").header("Host", "example.com")).await;
    assert_eq!(refused.status, StatusCode::FORBIDDEN);
    assert_eq!(
        lines.lines(),
        ["INFO verdandi_http: request method=GET uri=/livez status=403"]
    );
    node.stop().await;
}

#[tokio::test]
async fn a_settle_logs_its_kinds_and_ids_before_its_request_line() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/s", "[1,2,3]")).await);
    let take = call(node.get("/s/s?group=g")).await;
    let receipt = take.body["receipt"].as_str().expect("receipt").to_owned();
    let (_log, lines) = capture_log();
    let extend =
        call(node.post(&format!("/r/{receipt}"), json!({ "extend": 5 }).to_string())).await;
    assert_eq!(extend.status, StatusCode::OK, "{}", extend.body);
    let mixed = call(node.post(
        &format!("/r/{receipt}"),
        json!({ "ack": [published[0]], "nack": [{ "id": published[1] }] }).to_string(),
    ))
    .await;
    assert_eq!(mixed.status, StatusCode::OK, "{}", mixed.body);
    assert_eq!(
        lines.lines(),
        [
            format!(
                "INFO verdandi_http::settle: settle receipt={receipt} kinds=extend count=0 ids="
            ),
            format!("INFO verdandi_http: request method=POST uri=/r/{receipt} status=200"),
            format!(
                "INFO verdandi_http::settle: settle receipt={receipt} kinds=ack,nack count=2 ids={},{}",
                published[0], published[1]
            ),
            format!("INFO verdandi_http: request method=POST uri=/r/{receipt} status=200"),
        ]
    );
    node.stop().await;
}

#[tokio::test]
async fn a_settle_of_many_ids_logs_the_count_and_the_first_10_ids() {
    let (_dir, node) = boot().await;
    let published = ids(&call(node.post("/s/s", "[1,2,3,4,5,6,7,8,9,10,11,12]")).await);
    let take = call(node.get("/s/s?group=g&limit=12")).await;
    let receipt = take.body["receipt"].as_str().expect("receipt").to_owned();
    let (_log, lines) = capture_log();
    let ack = call(node.post(
        &format!("/r/{receipt}"),
        json!({ "ack": published }).to_string(),
    ))
    .await;
    assert_eq!(ack.status, StatusCode::OK, "{}", ack.body);
    assert_eq!(
        settles(&lines.lines()),
        [&format!(
            "INFO verdandi_http::settle: settle receipt={receipt} kinds=ack count=12 ids={}",
            published[..10].join(",")
        )]
    );
    node.stop().await;
}

#[tokio::test]
async fn an_ack_of_the_whole_take_logs_the_kind_ack_all() {
    let (_dir, node) = boot().await;
    call(node.post("/s/s", "1")).await;
    let take = call(node.get("/s/s?group=g")).await;
    let receipt = take.body["receipt"].as_str().expect("receipt").to_owned();
    let (_log, lines) = capture_log();
    assert_eq!(
        call(node.delete(&format!("/r/{receipt}"))).await.status,
        StatusCode::OK
    );
    let lines = lines.lines();
    assert_eq!(
        settles(&lines),
        [&format!(
            "INFO verdandi_http::settle: settle receipt={receipt} kinds=ack_all ids="
        )]
    );
    node.stop().await;
}

#[tokio::test]
async fn a_failed_settle_logs_no_settle_line() {
    let (_dir, node) = boot().await;
    let (_log, lines) = capture_log();
    let stale = format!("/r/r_{:032x}", 7);
    let reply = call(node.post(&stale, json!({ "ack": [] }).to_string())).await;
    assert_eq!(reply.status, StatusCode::CONFLICT);
    assert!(settles(&lines.lines()).is_empty());
    node.stop().await;
}
