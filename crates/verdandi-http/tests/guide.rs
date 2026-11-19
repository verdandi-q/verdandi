// allow: each test target uses a part of the shared kit
#[allow(dead_code)]
mod kit;

use std::collections::BTreeSet;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use kit::{Node, START_MS, boot, call, ids, listen, refused, scratch, start, strings, taken};
use reqwest::{Client, Method, StatusCode};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use verdandi_core::Timing;
use verdandi_core::ids::MessageId;
use verdandi_http::{NEXT_STEP, OPEN_MODE};
use verdandi_store::block::{BLOCK_HEADER_LEN, MAX_BLOCK, SEGMENT_BYTES};
use verdandi_store::engine::TestClock;
use verdandi_store::log::Log;
use verdandi_store::records::{self, Message};
use verdandi_store::recovery::recover;

struct Text {
    status: StatusCode,
    content_type: String,
    body: String,
}

async fn text(node: &Node, accept: Option<&str>) -> Text {
    let mut request = node.get("/");
    if let Some(accept) = accept {
        request = request.header("Accept", accept);
    }
    let response = request.send().await.expect("send");
    Text {
        status: response.status(),
        content_type: response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned(),
        body: response.text().await.expect("body"),
    }
}

async fn guide(node: &Node) -> String {
    let reply = text(node, Some("text/plain")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    reply.body
}

async fn rate(node: &Node) -> u64 {
    call(node.get("/")).await.body["node"]["write_bytes_per_s"]
        .as_u64()
        .expect("a write rate")
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

struct Curl {
    method: Method,
    url: String,
    headers: Vec<(String, String)>,
    body: Option<String>,
}

fn words(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut word = String::new();
    let mut quote = None;
    let mut open = false;
    for c in line.chars() {
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (Some(_), c) => word.push(c),
            (None, '\'' | '"') => {
                quote = Some(c);
                open = true;
            }
            (None, c) if c.is_whitespace() => {
                if open || !word.is_empty() {
                    out.push(std::mem::take(&mut word));
                    open = false;
                }
            }
            (None, c) => word.push(c),
        }
    }
    if open || !word.is_empty() {
        out.push(word);
    }
    out
}

fn curl(line: &str) -> Curl {
    let mut words = words(line).into_iter();
    assert_eq!(words.next().as_deref(), Some("curl"), "{line}");
    let (mut method, mut url, mut body) = (None, None, None);
    let mut headers = Vec::new();
    while let Some(word) = words.next() {
        match word.as_str() {
            "-X" => method = words.next(),
            "-d" => body = words.next(),
            "-N" => {}
            "-H" => {
                let header = words.next().expect("a header");
                let (name, value) = header.split_once(": ").expect("name: value");
                headers.push((name.to_owned(), value.to_owned()));
            }
            _ => url = Some(word),
        }
    }
    if body.is_some()
        && !headers
            .iter()
            .any(|(n, _)| n.eq_ignore_ascii_case("content-type"))
    {
        headers.push((
            "Content-Type".to_owned(),
            "application/x-www-form-urlencoded".to_owned(),
        ));
    }
    let method = method.unwrap_or_else(|| if body.is_some() { "POST" } else { "GET" }.to_owned());
    Curl {
        method: method.parse().expect("a method"),
        url: url.expect("a url"),
        headers,
        body,
    }
}

async fn run(call: &Curl) -> (StatusCode, Value, String) {
    let mut request = Client::new().request(call.method.clone(), &call.url);
    for (name, value) in &call.headers {
        request = request.header(name, value);
    }
    if let Some(body) = &call.body {
        request = request.body(body.clone());
    }
    let response = request.send().await.expect("send");
    let status = response.status();
    let text = response.text().await.expect("body");
    let json = serde_json::from_str(&text).unwrap_or(Value::Null);
    (status, json, text)
}

fn section<'a>(guide: &'a str, heading: &str) -> Vec<&'a str> {
    guide
        .lines()
        .skip_while(|line| *line != heading)
        .skip(1)
        .take_while(|line| !line.is_empty())
        .filter(|line| line.starts_with("  curl "))
        .map(str::trim)
        .collect()
}

fn curl_lines(guide: &str) -> Vec<&str> {
    guide
        .lines()
        .filter(|line| line.starts_with("  curl "))
        .map(str::trim)
        .collect()
}

#[tokio::test]
async fn root_json_gives_node_copies_streams_groups_warnings_and_links() {
    let (_dir, node) = boot().await;
    call(node.post("/s/orders", r#"{"order":1}"#)).await;
    call(node.get("/s/orders?group=billing")).await;
    let root = call(node.get("/")).await;
    assert_eq!(root.status, StatusCode::OK);
    let body = &root.body;
    let total = body["node"]["disk_total_bytes"].as_u64().expect("total");
    let free = body["node"]["disk_free_bytes"].as_u64().expect("free");
    assert!(total > 0 && free <= total, "{total} {free}");
    assert!(body["node"]["write_bytes_per_s"].is_u64(), "{body}");
    assert_eq!(
        body["copies"],
        json!({"ack_count": 1, "target": 1, "floor": 1, "why": "one node"})
    );
    assert_eq!(body["streams"], json!(["orders"]));
    assert_eq!(
        body["groups"],
        json!([{"name": "billing", "provisional": true}])
    );
    let warnings: Vec<String> = strings(&body["warnings"])
        .into_iter()
        .filter(|w| !w.starts_with("disk space low: ") && !w.starts_with("disk full in about "))
        .collect();
    assert_eq!(warnings, [OPEN_MODE], "{body}");
    for link in [
        "guide", "openapi", "publish", "take", "ack", "settle", "read",
    ] {
        assert!(body["links"][link].is_string(), "{link}: {body}");
    }
    node.stop().await;
}

#[tokio::test]
async fn root_drops_a_forgotten_group_and_keeps_a_deleted_stream_listed_until_compaction() {
    let (_dir, node) = boot().await;
    call(node.post("/s/orders", "1")).await;
    call(node.get("/s/orders?group=billing")).await;
    call(node.delete("/g/billing")).await;
    let root = call(node.get("/")).await;
    assert_eq!(root.body["groups"], json!([]), "{}", root.body);
    call(node.delete("/s/orders?confirm=orders")).await;
    node.clock.advance(1);
    let root = call(node.get("/")).await;
    assert_eq!(root.body["streams"], json!(["orders"]), "{}", root.body);
    let text = guide(&node).await;
    let phrase = "GET / lists the stream and its dead streams until compaction removes their data.";
    assert!(text.contains(phrase), "the guide lacks: {phrase}");
    node.stop().await;
}

#[tokio::test]
async fn root_gives_the_guide_for_text_plain_or_html_without_json_and_json_otherwise() {
    let (_dir, node) = boot().await;
    let cases = [
        (Some("text/plain"), true),
        (Some("text/html"), true),
        (Some("text/html,application/xhtml+xml"), true),
        (Some("text/plain, application/json"), false),
        (Some("application/json"), false),
        (Some("*/*"), false),
        (None, false),
    ];
    for (accept, guide) in cases {
        let reply = text(&node, accept).await;
        assert_eq!(reply.status, StatusCode::OK, "{accept:?}");
        if guide {
            assert!(reply.content_type.starts_with("text/plain"), "{accept:?}");
            assert!(reply.body.starts_with("VERDANDI GUIDE"), "{accept:?}");
        } else {
            assert!(
                reply.content_type.starts_with("application/json"),
                "{accept:?}"
            );
            assert!(
                serde_json::from_str::<Value>(&reply.body).is_ok(),
                "{accept:?}"
            );
        }
    }
    node.stop().await;
}

#[tokio::test]
async fn guide_uses_the_host_of_the_request_in_every_curl_line() {
    let (_dir, node) = boot().await;
    let own = guide(&node).await;
    let host = node.url.trim_start_matches("http://");
    assert!(!own.contains("{host}"), "{own}");
    let lines = curl_lines(&own);
    assert!(!lines.is_empty());
    for line in &lines {
        assert!(line.contains(&format!("http://{host}/")), "{line}");
    }
    let named = node
        .get("/")
        .header("Accept", "text/plain")
        .header("Host", "localhost:8080")
        .send()
        .await
        .expect("send")
        .text()
        .await
        .expect("body");
    assert!(named.contains("http://localhost:8080/s/orders"), "{named}");
    assert!(!named.contains(host), "{named}");
    node.stop().await;
}

#[tokio::test]
async fn root_names_a_damaged_file_by_file_name_and_never_gives_a_path() {
    let dir = scratch();
    let name = "cmp-0000000000000063.vdl";
    std::fs::write(dir.path().join(name), [0xab; 32]).expect("garbage");
    let node = start(dir.path(), &TestClock::new(START_MS)).await;
    let json = call(node.get("/")).await;
    let path = dir.path().to_string_lossy().into_owned();
    let raw = json.body.to_string();
    assert!(raw.contains(name), "{raw}");
    assert!(!raw.contains(&path), "{raw}");
    let plain = guide(&node).await;
    assert!(!plain.contains(&path), "{plain}");
    node.stop().await;
}

#[tokio::test]
async fn root_warning_names_the_streams_held_in_a_damaged_segment() {
    let (dir, node) = boot().await;
    call(node.post("/s/u", "damaged-read-marker")).await;
    call(node.post("/s/u", "1")).await;
    corrupt(dir.path(), b"damaged-read-marker");
    call(node.get("/s/u")).await;
    let found = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let warnings = strings(&call(node.get("/")).await.body["warnings"]);
            if let Some(w) = warnings.iter().find(|w| w.starts_with("damaged log range")) {
                return w.clone();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the damaged range has no warning");
    assert!(
        found.ends_with("; streams held in this segment: u"),
        "{found}"
    );
    node.stop().await;
}

#[tokio::test]
async fn root_warns_of_a_dropped_dead_copy_and_names_its_stream_and_group() {
    let fs = verdandi_sim::fault_fs::FaultFs::new(35, 1 << 40);
    let dir = Path::new("/data");
    let id = MessageId::new(1_000, 1, 0).expect("id");
    let mut original = Message {
        position: 0,
        id,
        timing: Timing {
            delay_until: None,
            expire_at: Some(2_000),
        },
        origin: None,
        headers: Vec::new(),
        value: Vec::new(),
    };
    let empty = records::encode(&[("s", std::slice::from_ref(&original))], &[]);
    original.value = vec![1; MAX_BLOCK - BLOCK_HEADER_LEN - empty.0.len()];
    let (body, _) = records::encode(&[("s", std::slice::from_ref(&original))], &[]);
    let log = Log::start(
        fs.clone(),
        1,
        recover(&fs, dir, |_, _, _| {}).expect("recover"),
    );
    log.append(1_000, body, SEGMENT_BYTES)
        .await
        .expect("reply")
        .expect("append");
    log.close().await;
    let node = kit::start_on(fs.clone(), dir, &TestClock::new(3_000)).await;
    assert_eq!(
        call(node.get("/s/s?group=g&limit=1")).await.status,
        StatusCode::NO_CONTENT
    );
    let found = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let warnings = strings(&call(node.get("/")).await.body["warnings"]);
            if let Some(w) = warnings.iter().find(|w| w.starts_with("dead copy dropped")) {
                return w.clone();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the dropped dead copy has no warning");
    assert_eq!(
        found,
        format!("dead copy dropped: message {id} of stream s for group g")
    );
    call(node.post("/s/t", vec![b'v'; 100 << 10])).await;
    fs.set_capacity(fs.used() + rate(&node).await * 3600);
    let warnings = strings(&call(node.get("/")).await.body["warnings"]);
    let order: Vec<Option<usize>> = warnings
        .iter()
        .map(|w| {
            [
                OPEN_MODE,
                "dead copy dropped: ",
                "disk space low: ",
                "disk full in about ",
            ]
            .iter()
            .position(|p| w.starts_with(p))
        })
        .collect();
    assert_eq!(order, [Some(0), Some(1), Some(2), Some(3)], "{warnings:?}");
    node.stop().await;
}

#[tokio::test]
async fn write_rate_divides_by_at_least_one_window_after_the_start() {
    let (_dir, node) = boot().await;
    assert_eq!(rate(&node).await, 0);
    call(node.post("/s/s", "x".repeat(1000))).await;
    node.clock.advance(1_000);
    let early = rate(&node).await;
    assert!((10..30).contains(&early), "{early}");
    node.clock.advance(9_000);
    let measured = rate(&node).await;
    assert!((10..30).contains(&measured), "{measured}");
    node.stop().await;
}

#[tokio::test]
async fn write_rate_falls_to_zero_when_the_writes_are_older_than_two_windows() {
    let (_dir, node) = boot().await;
    call(node.post("/s/s", "x".repeat(1000))).await;
    node.clock.advance(10_000);
    assert!(rate(&node).await > 0);
    for _ in 0..3 {
        node.clock.advance(61_000);
        rate(&node).await;
    }
    assert_eq!(rate(&node).await, 0);
    node.stop().await;
}

#[tokio::test]
async fn openapi_is_version_3_and_lists_exactly_the_m1_paths_and_methods() {
    let (_dir, node) = boot().await;
    let doc = call(node.get("/openapi.json")).await;
    assert_eq!(doc.status, StatusCode::OK);
    assert!(
        doc.body["openapi"]
            .as_str()
            .expect("version")
            .starts_with("3.")
    );
    assert!(doc.body["info"]["title"].is_string());
    let listed: BTreeSet<(String, String)> = doc.body["paths"]
        .as_object()
        .expect("paths")
        .iter()
        .flat_map(|(path, item)| {
            item.as_object()
                .expect("a path item")
                .keys()
                .map(|method| (method.to_uppercase(), path.clone()))
                .collect::<Vec<_>>()
        })
        .collect();
    let m1 = [
        ("GET", "/"),
        ("GET", "/openapi.json"),
        ("GET", "/livez"),
        ("GET", "/readyz"),
        ("GET", "/s/{stream}"),
        ("POST", "/s/{stream}"),
        ("DELETE", "/s/{stream}"),
        ("POST", "/bulk"),
        ("POST", "/r/{receipt}"),
        ("DELETE", "/r/{receipt}"),
        ("GET", "/g/{group}"),
        ("PUT", "/g/{group}"),
        ("DELETE", "/g/{group}"),
    ];
    let expected: BTreeSet<(String, String)> = m1
        .iter()
        .map(|(m, p)| ((*m).to_owned(), (*p).to_owned()))
        .collect();
    assert_eq!(listed, expected);
    node.stop().await;
}

fn refs(value: &Value, found: &mut BTreeSet<String>) {
    match value {
        Value::Object(map) => {
            for (key, inner) in map {
                match (key.as_str(), inner) {
                    ("$ref", Value::String(target)) => {
                        found.insert(target.clone());
                    }
                    _ => refs(inner, found),
                }
            }
        }
        Value::Array(items) => items.iter().for_each(|item| refs(item, found)),
        _ => {}
    }
}

#[tokio::test]
async fn openapi_resolves_every_ref_and_gives_each_operation_a_summary_and_responses() {
    let (_dir, node) = boot().await;
    let doc = call(node.get("/openapi.json")).await.body;
    let mut found = BTreeSet::new();
    refs(&doc, &mut found);
    assert!(!found.is_empty());
    for target in &found {
        let pointer = target.trim_start_matches('#');
        assert!(doc.pointer(pointer).is_some(), "{target} does not resolve");
    }
    for (path, item) in doc["paths"].as_object().expect("paths") {
        for (method, operation) in item.as_object().expect("an item") {
            assert!(operation["summary"].is_string(), "{method} {path}");
            let responses = operation["responses"].as_object().expect("responses");
            assert!(!responses.is_empty(), "{method} {path}");
        }
    }
    for schema in [
        "ErrorBody",
        "Published",
        "Leased",
        "SettleRequest",
        "Status",
    ] {
        assert!(doc["components"]["schemas"][schema].is_object(), "{schema}");
    }
    node.stop().await;
}

#[tokio::test]
async fn openapi_documents_only_routes_that_the_server_serves() {
    let (_dir, node) = boot().await;
    let doc = call(node.get("/openapi.json")).await.body;
    let receipt = format!("r_{:032x}", 1);
    for (path, item) in doc["paths"].as_object().expect("paths") {
        let url = path
            .replace("{stream}", "x")
            .replace("{group}", "g")
            .replace("{receipt}", &receipt);
        for method in item.as_object().expect("an item").keys() {
            let method: Method = method.to_uppercase().parse().expect("a method");
            let reply = Client::new()
                .request(method.clone(), format!("{}{url}", node.url))
                .send()
                .await
                .expect("send");
            assert_ne!(
                reply.status(),
                StatusCode::METHOD_NOT_ALLOWED,
                "{method} {path}"
            );
            let body = reply.text().await.expect("body");
            assert!(!body.contains("no route has this path"), "{method} {path}");
        }
    }
    node.stop().await;
}

async fn raw(url: &str, request: &str) -> (u16, Value) {
    let mut stream = TcpStream::connect(url.trim_start_matches("http://"))
        .await
        .expect("connect");
    stream.write_all(request.as_bytes()).await.expect("write");
    let mut reply = Vec::new();
    stream.read_to_end(&mut reply).await.expect("read");
    let text = String::from_utf8_lossy(&reply).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").expect("a head and a body");
    let status = head
        .split(' ')
        .nth(1)
        .expect("a status")
        .parse()
        .expect("a number");
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

#[tokio::test]
async fn errors_have_next_on_every_4xx_and_5xx_body() {
    let (_dir, node) = boot().await;
    let receipt = format!("r_{:032x}", 1);
    let http = Client::new();
    let cases = [
        (node.get("/s/x?group=g&limit=0"), StatusCode::BAD_REQUEST),
        (node.post("/s/orders.dead.g", "1"), StatusCode::BAD_REQUEST),
        (node.post("/s/x?ttl=soon", "1"), StatusCode::BAD_REQUEST),
        (node.post("/bulk", "[]"), StatusCode::BAD_REQUEST),
        (node.post("/r/not-a-receipt", "{}"), StatusCode::BAD_REQUEST),
        (node.put("/g/g", "{}"), StatusCode::BAD_REQUEST),
        (node.delete("/s/x"), StatusCode::BAD_REQUEST),
        (node.delete("/s/_vd?confirm=_vd"), StatusCode::BAD_REQUEST),
        (
            node.get("/s/x?from=now&follow=1&group=g"),
            StatusCode::BAD_REQUEST,
        ),
        (
            node.get("/").header("Host", "evil.example"),
            StatusCode::FORBIDDEN,
        ),
        (
            node.post("/s/x", "1")
                .header("Origin", "http://evil.example")
                .header("Content-Type", "text/plain"),
            StatusCode::FORBIDDEN,
        ),
        (node.post("/", "1"), StatusCode::METHOD_NOT_ALLOWED),
        (node.delete("/"), StatusCode::METHOD_NOT_ALLOWED),
        (node.get("/nowhere"), StatusCode::NOT_FOUND),
        (node.get("/g/nothing"), StatusCode::NOT_FOUND),
        (node.delete("/g/nothing"), StatusCode::NOT_FOUND),
        (
            http.request(Method::PATCH, format!("{}/s/x", node.url)),
            StatusCode::METHOD_NOT_ALLOWED,
        ),
        (
            node.post(&format!("/r/{receipt}"), "{}"),
            StatusCode::CONFLICT,
        ),
        (node.delete(&format!("/r/{receipt}")), StatusCode::CONFLICT),
        (
            node.post("/s/x", vec![b'a'; (1 << 20) + 1]),
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
    ];
    for (request, status) in cases {
        let request = request.build().expect("a request");
        let label = format!("{} {}", request.method(), request.url());
        let reply = call(reqwest::RequestBuilder::from_parts(http.clone(), request)).await;
        refused(&reply, status);
        assert!(reply.status.is_client_error(), "{label}");
    }
    drop(http);
    node.stop().await;
}

#[tokio::test]
async fn errors_have_next_while_the_node_recovers() {
    let node = Arc::new(OnceLock::new());
    let (listener, url) = listen().await;
    let server = tokio::spawn(verdandi_http::serve(
        listener,
        Arc::clone(&node),
        std::future::pending(),
    ));
    let http = Client::new();
    for path in ["/", "/openapi.json", "/s/x?group=g", "/g/g", "/nowhere"] {
        let reply = call(http.get(format!("{url}{path}"))).await;
        refused(&reply, StatusCode::SERVICE_UNAVAILABLE);
    }
    let reply = call(http.post(format!("{url}/"))).await;
    refused(&reply, StatusCode::SERVICE_UNAVAILABLE);
    server.abort();
}

#[tokio::test]
async fn errors_have_next_when_the_body_does_not_arrive_in_60_s() {
    let (_dir, node) = boot().await;
    let request =
        "POST /s/x HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nContent-Length: 10\r\n\r\n1";
    tokio::time::pause();
    let (status, body) = tokio::time::timeout(Duration::from_secs(120), raw(&node.url, request))
        .await
        .expect("a reply before 120 s");
    tokio::time::resume();
    assert_eq!(status, 408);
    assert!(
        body["error"].is_string() && body["next"].is_string(),
        "{body}"
    );
    node.stop().await;
}

#[tokio::test]
async fn errors_have_next_when_the_disk_is_full_of_unacked_work() {
    let fs = verdandi_sim::fault_fs::FaultFs::new(1, 1 << 40);
    let clock = TestClock::new(START_MS);
    let node = kit::start_on(fs.clone(), Path::new("/data"), &clock).await;
    call(node.post("/s/s", "a")).await;
    call(node.get("/s/s?group=g&limit=1")).await;
    fs.set_capacity(fs.used() + (1 << 20));
    node.clock.advance(1);
    let full = call(node.post("/s/s", "d")).await;
    refused(&full, StatusCode::TOO_MANY_REQUESTS);
    node.stop().await;
}

#[tokio::test]
async fn publish_and_take_replies_carry_next() {
    let (_dir, node) = boot().await;
    let published = call(node.post("/s/orders", "{}")).await;
    assert_eq!(published.body["next"]["take"], "/s/orders?group={group}");
    let bulk = call(node.post("/bulk", r#"[{"stream":"a","value":1}]"#)).await;
    assert!(bulk.body["next"]["take"].is_string(), "{}", bulk.body);
    let take = call(node.get("/s/orders?group=g")).await;
    assert_eq!(taken(&take), ids(&published));
    let receipt = take.body["receipt"].as_str().expect("receipt");
    assert_eq!(take.body["next"]["ack"], format!("DELETE /r/{receipt}"));
    assert_eq!(take.body["next"]["settle"], format!("POST /r/{receipt}"));
    node.stop().await;
}

#[tokio::test]
async fn guide_walk_publishes_takes_extends_and_acks_using_only_the_guide_text() {
    let (_dir, node) = boot().await;
    let text = guide(&node).await;
    let first = |heading: &str| -> String {
        section(&text, heading)
            .first()
            .unwrap_or_else(|| panic!("no curl line under {heading}"))
            .to_string()
    };
    let (status, published, _) = run(&curl(&first("PUBLISH"))).await;
    assert_eq!(status, StatusCode::CREATED, "{published}");
    assert_eq!(published["copies"], 1);
    let id = published["ids"][0].as_str().expect("an id").to_owned();
    let (status, taken, _) = run(&curl(&first("TAKE"))).await;
    assert_eq!(status, StatusCode::OK, "{taken}");
    assert_eq!(taken["messages"][0]["id"], id);
    let receipt = taken["receipt"].as_str().expect("a receipt").to_owned();
    let with_receipt = |heading: &str| first(heading).replace("RECEIPT", &receipt);
    let (status, extended, _) = run(&curl(&with_receipt("EXTEND"))).await;
    assert_eq!(status, StatusCode::OK, "{extended}");
    let (status, acked, _) = run(&curl(&with_receipt("ACK"))).await;
    assert_eq!(status, StatusCode::OK, "{acked}");
    let (status, group, _) = run(&curl(&first("GROUPS"))).await;
    assert_eq!(status, StatusCode::OK, "{group}");
    assert_eq!(group["pending"], 0, "{group}");
    assert_eq!(group["in_flight"], 0, "{group}");
    node.stop().await;
}

async fn lease_after_the_first_end(extend: bool) -> (StatusCode, StatusCode) {
    let (_dir, node) = boot().await;
    let text = guide(&node).await;
    let line = |heading: &str| section(&text, heading)[0].to_owned();
    run(&curl(&line("PUBLISH"))).await;
    let (_, taken, _) = run(&curl(&line("TAKE"))).await;
    let receipt = taken["receipt"].as_str().expect("a receipt").to_owned();
    let mut extended = StatusCode::OK;
    if extend {
        extended = run(&curl(&line("EXTEND").replace("RECEIPT", &receipt)))
            .await
            .0;
    }
    node.clock.advance(31_000);
    let again = call(node.get("/s/orders?group=billing")).await;
    node.stop().await;
    (extended, again.status)
}

#[tokio::test]
async fn guide_walk_extend_keeps_the_lease_past_its_first_end() {
    assert_eq!(
        lease_after_the_first_end(true).await,
        (StatusCode::OK, StatusCode::NO_CONTENT)
    );
}

#[tokio::test]
async fn guide_walk_without_extend_loses_the_lease_at_its_first_end() {
    assert_eq!(lease_after_the_first_end(false).await.1, StatusCode::OK);
}

async fn lease(node: &Node, count: usize) -> (String, Vec<String>) {
    for n in 0..count {
        call(node.post("/s/orders", format!(r#"{{"n":{n}}}"#))).await;
    }
    let reply = call(node.get(&format!("/s/orders?group=billing&limit={count}"))).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    let receipt = reply.body["receipt"].as_str().expect("a receipt");
    (receipt.to_owned(), taken(&reply))
}

#[tokio::test]
async fn guide_every_curl_line_gives_a_2xx_with_real_data() {
    let (_dir, node) = boot().await;
    let text = guide(&node).await;
    let lines = curl_lines(&text);
    assert!(lines.len() >= 10, "{}", lines.len());
    for line in lines {
        let mut line = line.to_owned();
        if line.contains("RECEIPT") {
            let (receipt, ids) = lease(&node, 3).await;
            line = line.replace("RECEIPT", &receipt);
            for (n, id) in ids.iter().enumerate() {
                line = line.replace(&format!("ID{}", n + 1), id);
            }
        }
        let call = curl(&line);
        if line.contains("follow") {
            let mut request = Client::new().request(call.method.clone(), &call.url);
            for (name, value) in &call.headers {
                request = request.header(name, value);
            }
            let status = request.send().await.expect("send").status();
            assert!(status.is_success(), "{line}: {status}");
            continue;
        }
        let (status, _, body) = run(&call).await;
        assert!(status.is_success(), "{line}: {status} {body}");
    }
    node.stop().await;
}

#[tokio::test]
async fn accept_values_match_without_regard_to_case() {
    let (_dir, node) = boot().await;
    let reply = text(&node, Some("TEXT/PLAIN")).await;
    assert!(
        reply.content_type.starts_with("text/plain"),
        "{}",
        reply.body
    );
    assert!(reply.body.starts_with("VERDANDI GUIDE"), "{}", reply.body);
    let json = text(&node, Some("Text/Plain, APPLICATION/JSON")).await;
    assert!(json.content_type.starts_with("application/json"));
    node.stop().await;
}

#[tokio::test]
async fn root_warns_early_when_the_disk_fills_within_24_hours_and_when_under_20_percent_is_free() {
    let fs = verdandi_sim::fault_fs::FaultFs::new(2, 1 << 40);
    let clock = TestClock::new(START_MS);
    let node = kit::start_on(fs.clone(), Path::new("/data"), &clock).await;
    call(node.post("/s/s", vec![b'v'; 100 << 10])).await;
    let early = |warnings: &[String], prefix: &str| {
        warnings
            .iter()
            .filter(|w| w.starts_with(prefix) && w.ends_with(NEXT_STEP))
            .count()
    };
    fs.set_capacity(fs.used() * 2);
    let warnings = strings(&call(node.get("/")).await.body["warnings"]);
    assert_eq!(warnings[0], OPEN_MODE);
    assert_eq!(early(&warnings, "disk full in about "), 1, "{warnings:?}");
    assert_eq!(early(&warnings, "disk space low: "), 0, "{warnings:?}");
    let per_s = rate(&node).await;
    fs.set_capacity(fs.used() + per_s * 2 * 3600);
    let warnings = strings(&call(node.get("/")).await.body["warnings"]);
    let full: Vec<&String> = warnings
        .iter()
        .filter(|w| w.starts_with("disk full in about "))
        .collect();
    assert_eq!(full.len(), 1, "{warnings:?}");
    assert!(
        full[0].contains(" hours at the current write rate"),
        "{warnings:?}"
    );
    fs.set_capacity(fs.used() + per_s * 25 * 3600);
    let warnings = strings(&call(node.get("/")).await.body["warnings"]);
    assert_eq!(early(&warnings, "disk full in about "), 0, "{warnings:?}");
    fs.set_capacity(fs.used() + fs.used() / 9);
    let warnings = strings(&call(node.get("/")).await.body["warnings"]);
    assert_eq!(early(&warnings, "disk space low: "), 1, "{warnings:?}");
    fs.set_capacity(fs.used() + (1 << 20));
    let warnings = strings(&call(node.get("/")).await.body["warnings"]);
    let full = warnings
        .iter()
        .find(|w| w.starts_with("disk full in about "));
    assert!(
        full.is_some_and(|w| w.contains(" minutes at the current write rate")),
        "{warnings:?}"
    );
    fs.set_capacity(fs.used() + per_s / 2);
    let warnings = strings(&call(node.get("/")).await.body["warnings"]);
    assert!(
        warnings
            .iter()
            .any(|w| w.starts_with("disk full in about 1 minute at ")),
        "{warnings:?}"
    );
    fs.set_capacity(fs.used());
    let warnings = strings(&call(node.get("/")).await.body["warnings"]);
    assert_eq!(
        early(&warnings, "disk full, 0 bytes free; "),
        1,
        "{warnings:?}"
    );
    assert_eq!(early(&warnings, "disk full in about "), 0, "{warnings:?}");
    node.stop().await;
}

#[tokio::test]
async fn root_keeps_its_warnings_and_gives_null_disk_fields_when_the_disk_cannot_be_read() {
    let (dir, node) = boot().await;
    let moved = dir.path().with_extension("away");
    std::fs::rename(dir.path(), &moved).expect("move the data dir");
    let root = call(node.get("/")).await;
    std::fs::rename(&moved, dir.path()).expect("move the data dir back");
    assert_eq!(root.status, StatusCode::OK, "{}", root.body);
    assert!(
        root.body["node"]["disk_total_bytes"].is_null(),
        "{}",
        root.body
    );
    assert!(
        root.body["node"]["disk_free_bytes"].is_null(),
        "{}",
        root.body
    );
    assert_eq!(root.body["warnings"], json!([OPEN_MODE]));
    node.stop().await;
}

#[tokio::test]
async fn guide_limits_match_what_the_server_enforces() {
    let (_dir, node) = boot().await;
    let text = guide(&node).await;
    for phrase in [
        "A body has at most 16 MiB. A message has at most 1 MiB.",
        "joined by dots. It has at most 200 bytes.",
        "A name does not start with _ and does not hold .dead. Its last part is not dead.",
        "delay: seconds, a duration such as 30s, or an RFC 3339 time.",
        "ttl: seconds, or a duration such as 30s.",
        "They have at most 8192 bytes in all, names and values together.",
        "so one big message can pass this limit.",
        "Each list holds at most 10000 items.",
        "extend: 1 to 43200 seconds. delay: 0 to 43200 seconds.",
        "curl -N \"http://127.0.0.1:",
        "/s/orders?from=now&follow\"",
        "409 the receipt is stale or does not hold the id, the group changed, or a replay is not yet durable. Read next.",
        "413 a message is over 1 MiB, a batch is over 32 MiB, or the request is over 16 MiB. Read next.",
        "503 the node is starting or stopping, or the disk is full. Retry later.",
        "Every error body from a route is JSON:",
    ] {
        assert!(text.contains(phrase), "the guide lacks: {phrase}");
    }
    let post = |path: String, body: String| call(node.post(&path, body));
    let big = post("/s/x".to_owned(), "a".repeat((16 << 20) + 1)).await;
    assert_eq!(big.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert!(
        big.body["error"]
            .as_str()
            .is_some_and(|e| e.contains("16 MiB"))
    );
    let travelling = |value: usize| node.post("/s/x", "1").header("X-Msg-A", "v".repeat(value));
    assert_eq!(call(travelling(8185)).await.status, StatusCode::CREATED);
    assert_eq!(call(travelling(8186)).await.status, StatusCode::BAD_REQUEST);
    for (name, status) in [
        ("_x", StatusCode::BAD_REQUEST),
        ("a.dead.b", StatusCode::BAD_REQUEST),
        ("dead", StatusCode::BAD_REQUEST),
        ("jobs.dead", StatusCode::BAD_REQUEST),
        (&"a".repeat(201), StatusCode::BAD_REQUEST),
        (&"a".repeat(200), StatusCode::CREATED),
        ("a_b-C.d9", StatusCode::CREATED),
    ] {
        assert_eq!(
            post(format!("/s/{name}"), "1".to_owned()).await.status,
            status,
            "{name}"
        );
    }
    for (query, status) in [
        ("delay=30", StatusCode::CREATED),
        ("delay=30s", StatusCode::CREATED),
        ("delay=2030-01-01T00:00:00Z", StatusCode::CREATED),
        ("ttl=3600", StatusCode::CREATED),
        ("ttl=1h", StatusCode::CREATED),
        ("ttl=2030-01-01T00:00:00Z", StatusCode::BAD_REQUEST),
    ] {
        assert_eq!(
            post(format!("/s/q?{query}"), "1".to_owned()).await.status,
            status,
            "{query}"
        );
    }
    let published = call(node.post("/s/m", "a".repeat(600))).await;
    call(node.post("/s/m", "b".repeat(600))).await;
    let one = call(node.get("/s/m?group=g&max_bytes=1")).await;
    assert_eq!(
        one.body["messages"].as_array().map(Vec::len),
        Some(1),
        "{}",
        one.body
    );
    let id = ids(&published)[0].clone();
    let receipt = format!("r_{:032x}", 1);
    let settle = |body: Value| call(node.post(&format!("/r/{receipt}"), body.to_string()));
    for (body, status) in [
        (json!({"ack": vec![&id; 10_000]}), StatusCode::CONFLICT),
        (json!({"ack": vec![&id; 10_001]}), StatusCode::BAD_REQUEST),
        (json!({"extend": 43_200}), StatusCode::CONFLICT),
        (json!({"extend": 43_201}), StatusCode::BAD_REQUEST),
        (json!({"extend": 0}), StatusCode::BAD_REQUEST),
        (
            json!({"nack": [{"id": id, "delay": 43_200}]}),
            StatusCode::CONFLICT,
        ),
        (
            json!({"nack": [{"id": id, "delay": 43_201}]}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"nack": [{"id": id, "delay": 0}]}),
            StatusCode::CONFLICT,
        ),
    ] {
        assert_eq!(
            settle(body.clone()).await.status,
            status,
            "{:.60}",
            body.to_string()
        );
    }
    node.stop().await;
}

fn resolved<'a>(doc: &'a Value, schema: &'a Value) -> &'a Value {
    match schema["$ref"].as_str() {
        Some(r) => &doc["components"]["schemas"][r.rsplit('/').next().expect("a name")],
        None => schema,
    }
}

fn any_json(schema: &Value) -> bool {
    schema
        .get("type")
        .is_none_or(|t| t != "object" && t != "array")
        && schema.get("properties").is_none()
}

fn header_names(operation: &Value) -> BTreeSet<String> {
    operation["parameters"]
        .as_array()
        .expect("parameters")
        .iter()
        .filter(|p| p["in"] == "header")
        .map(|p| p["name"].as_str().expect("a name").to_ascii_lowercase())
        .collect()
}

#[tokio::test]
async fn openapi_publish_bodies_and_bulk_values_accept_any_json() {
    let (_dir, node) = boot().await;
    let doc = call(node.get("/openapi.json")).await.body;
    let content =
        &doc["paths"]["/s/{stream}"]["post"]["requestBody"]["content"]["application/json"];
    assert!(any_json(resolved(&doc, &content["schema"])), "{content}");
    let item = &doc["components"]["schemas"]["BulkItem"]["properties"]["value"];
    assert!(any_json(resolved(&doc, item)), "{item}");
    node.stop().await;
}

#[tokio::test]
async fn openapi_documents_the_headers_that_the_server_reads() {
    let (_dir, node) = boot().await;
    let doc = call(node.get("/openapi.json")).await.body;
    let travelling = ["traceparent", "tracestate"];
    for (path, method, names) in [
        (
            "/s/{stream}",
            "post",
            [&["idempotency-key"][..], &travelling].concat(),
        ),
        (
            "/bulk",
            "post",
            [&["idempotency-key"][..], &travelling].concat(),
        ),
        ("/s/{stream}", "get", vec!["last-event-id"]),
    ] {
        let found = header_names(&doc["paths"][path][method]);
        let want: BTreeSet<String> = names.iter().map(|n| (*n).to_owned()).collect();
        assert_eq!(found, want, "{method} {path}");
    }
    for (path, method, words) in [
        ("/s/{stream}", "post", &["ce-", "X-Msg-"][..]),
        ("/bulk", "post", &["ce-", "X-Msg-"]),
        ("/s/{stream}", "get", &["Accept: text/event-stream"]),
    ] {
        let text = doc["paths"][path][method]["description"]
            .as_str()
            .unwrap_or_default();
        for word in words {
            assert!(text.contains(word), "{method} {path}: {text}");
        }
    }
    node.stop().await;
}

#[tokio::test]
async fn openapi_marks_the_confirm_of_a_stream_delete_as_required() {
    let (_dir, node) = boot().await;
    let doc = call(node.get("/openapi.json")).await.body;
    let confirm = doc["paths"]["/s/{stream}"]["delete"]["parameters"]
        .as_array()
        .expect("parameters")
        .iter()
        .find(|p| p["name"] == "confirm")
        .expect("a confirm parameter");
    assert_eq!(confirm["required"], true, "{confirm}");
    node.stop().await;
}

#[tokio::test]
async fn name_errors_and_the_guide_say_that_the_last_part_cannot_be_dead() {
    let (_dir, node) = boot().await;
    for name in ["dead", "jobs.dead"] {
        let reply = call(node.post(&format!("/s/{name}"), "1")).await;
        assert_eq!(reply.status, StatusCode::BAD_REQUEST, "{name}");
        let next = reply.body["next"].as_str().expect("next");
        assert!(next.contains("last part"), "{next}");
    }
    node.stop().await;
}

fn keys(value: &serde_json::Value) -> BTreeSet<String> {
    value
        .as_object()
        .expect("an object")
        .keys()
        .cloned()
        .collect()
}

#[tokio::test]
async fn guide_take_reply_has_the_keys_of_a_real_take_reply() {
    let (_dir, node) = boot().await;
    let text = guide(&node).await;
    let example = text
        .lines()
        .find_map(|l| l.strip_prefix("The reply is 200: {\"receipt\""))
        .map(|rest| format!("{{\"receipt\"{rest}"))
        .expect("a take reply line");
    let example: serde_json::Value = serde_json::from_str(&example).expect("valid json");
    call(node.post("/s/orders", "{}")).await;
    let real = call(node.get("/s/orders?group=g")).await.body;
    assert_eq!(keys(&example), keys(&real));
    assert_eq!(
        keys(&example["messages"][0]),
        keys(&real["messages"][0]),
        "{real}"
    );
    assert!(
        text.contains("value_base64"),
        "the guide lacks value_base64"
    );
    node.stop().await;
}

#[tokio::test]
async fn a_batch_over_the_block_cap_gets_413_inside_the_body_limit() {
    let (_dir, node) = boot().await;
    let body = format!("[{}1]", "1,".repeat((16 << 20) / 2 - 2));
    let reply = call(node.post("/s/x", body)).await;
    assert_eq!(
        reply.status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "{}",
        reply.body
    );
    node.stop().await;
}

#[tokio::test]
async fn openapi_413_of_publish_and_bulk_names_every_cause_and_the_disk_fields_are_required() {
    let (_dir, node) = boot().await;
    let doc = call(node.get("/openapi.json")).await.body;
    for path in ["/s/{stream}", "/bulk"] {
        let text = doc["paths"][path]["post"]["responses"]["413"]["description"]
            .as_str()
            .expect("a 413 description");
        assert!(
            text.contains("1 MiB") && text.contains("16 MiB") && text.contains("32 MiB"),
            "{path}: {text}"
        );
    }
    let required = &doc["components"]["schemas"]["Disk"]["required"];
    for field in ["disk_total_bytes", "disk_free_bytes", "write_bytes_per_s"] {
        assert!(required.to_string().contains(field), "{field}: {required}");
    }
    node.stop().await;
}

#[tokio::test]
async fn openapi_lists_403_for_every_route_and_408_for_every_body_read() {
    let (_dir, node) = boot().await;
    let doc = call(node.get("/openapi.json")).await.body;
    for (path, item) in doc["paths"].as_object().expect("paths") {
        for (method, operation) in item.as_object().expect("an item") {
            assert!(
                operation["responses"]["403"].is_object(),
                "{method} {path} lacks 403"
            );
        }
    }
    for (path, method) in [("/r/{receipt}", "post"), ("/g/{group}", "put")] {
        assert!(
            doc["paths"][path][method]["responses"]["408"].is_object(),
            "{method} {path} lacks 408"
        );
    }
    for path in ["/livez", "/readyz"] {
        let reply = call(node.get(path).header("Host", "evil.example")).await;
        refused(&reply, StatusCode::FORBIDDEN);
    }
    node.stop().await;
}

#[tokio::test]
async fn guide_states_the_retry_rules_and_a_later_lease_end_waits_before_the_message_comes_back() {
    let (_dir, node) = boot().await;
    let text = guide(&node).await;
    for phrase in [
        "The first lease end of a message is free. Each later lease end is a failure.",
        "A failure is a nack, or a lease end that is not free.",
        "After a failure, the message waits 1 second. The wait doubles at each failure, to at most 300 seconds.",
        "At the fifth failure, the message goes dead.",
    ] {
        assert!(text.contains(phrase), "the guide lacks: {phrase}");
    }
    call(node.post("/s/orders", "1")).await;
    let take = || call(node.get("/s/orders?group=billing&lease=1"));
    assert_eq!(take().await.status, StatusCode::OK);
    node.clock.advance(1_000);
    assert_eq!(take().await.status, StatusCode::OK);
    node.clock.advance(1_000);
    assert_eq!(take().await.status, StatusCode::NO_CONTENT);
    node.clock.advance(1_000);
    tokio::time::timeout(Duration::from_secs(10), async {
        while take().await.status != StatusCode::OK {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the message did not come back after its wait");
    node.stop().await;
}

fn props(doc: &Value, schema: &Value) -> BTreeSet<String> {
    keys(&resolved(doc, schema)["properties"])
}

#[tokio::test]
async fn openapi_gives_the_take_reply_or_the_read_page_and_names_each_message_field() {
    let (_dir, node) = boot().await;
    let doc = call(node.get("/openapi.json")).await.body;
    let schema = resolved(
        &doc,
        &doc["paths"]["/s/{stream}"]["get"]["responses"]["200"]["content"]["application/json"]["schema"],
    );
    let shapes: Vec<BTreeSet<String>> = schema["oneOf"]
        .as_array()
        .unwrap_or_else(|| panic!("no oneOf: {schema}"))
        .iter()
        .map(|s| props(&doc, s))
        .collect();
    let set =
        |names: &[&str]| -> BTreeSet<String> { names.iter().map(|n| (*n).to_owned()).collect() };
    assert_eq!(
        shapes,
        [
            set(&["receipt", "lease_until", "messages", "next"]),
            set(&["messages", "next"])
        ]
    );
    let page = resolved(&doc, &schema["oneOf"][1]);
    assert_eq!(page["properties"]["next"]["type"], "string", "{page}");
    let message = &page["properties"]["messages"]["items"];
    let fields = set(&[
        "id",
        "stream",
        "at",
        "attempt",
        "headers",
        "value",
        "value_base64",
        "original",
    ]);
    assert_eq!(props(&doc, message), fields);
    let leased = resolved(&doc, &schema["oneOf"][0]);
    assert_eq!(
        props(&doc, &leased["properties"]["messages"]["items"]),
        fields
    );
    let original = &resolved(&doc, message)["properties"]["original"];
    let original = original
        .get("oneOf")
        .and_then(|o| o.as_array())
        .and_then(|o| o.iter().find(|s| s.get("$ref").is_some()))
        .unwrap_or(original);
    assert_eq!(
        props(&doc, original),
        set(&["id", "stream", "reason", "error"])
    );
    call(node.post("/s/orders", "{}")).await;
    let read = call(node.get("/s/orders")).await.body;
    assert_eq!(keys(&read), set(&["messages", "next"]));
    let taken = call(node.get("/s/orders?group=g")).await.body;
    for real in [&read["messages"][0], &taken["messages"][0]] {
        assert!(keys(real).is_subset(&fields), "{real}");
    }
    node.stop().await;
}

#[tokio::test]
async fn openapi_gives_the_range_of_each_take_and_settle_number_and_the_server_enforces_it() {
    let (_dir, node) = boot().await;
    let doc = call(node.get("/openapi.json")).await.body;
    let parameters = doc["paths"]["/s/{stream}"]["get"]["parameters"]
        .as_array()
        .expect("parameters");
    for (name, min, max) in [
        ("limit", 1, 10_000),
        ("max_bytes", 0, 16_777_216),
        ("lease", 1, 43_200),
        ("wait", 0, 60),
    ] {
        let parameter = parameters
            .iter()
            .find(|p| p["name"] == name)
            .unwrap_or_else(|| panic!("no {name}"));
        assert_eq!(parameter["schema"]["minimum"], min, "{parameter}");
        assert_eq!(parameter["schema"]["maximum"], max, "{parameter}");
        assert!(
            parameter["description"]
                .as_str()
                .is_some_and(|d| !d.is_empty()),
            "{parameter}"
        );
        let take = |value: i64| call(node.get(&format!("/s/x?group=g&{name}={value}")));
        assert_ne!(
            take(min).await.status,
            StatusCode::BAD_REQUEST,
            "{name}={min}"
        );
        assert_eq!(
            take(max + 1).await.status,
            StatusCode::BAD_REQUEST,
            "{name}={}",
            max + 1
        );
        if min > 0 {
            assert_eq!(
                take(min - 1).await.status,
                StatusCode::BAD_REQUEST,
                "{name}={}",
                min - 1
            );
        }
    }
    let schemas = &doc["components"]["schemas"];
    for (schema, field, min) in [("SettleRequest", "extend", 1), ("NackRequest", "delay", 0)] {
        let value = &schemas[schema]["properties"][field];
        assert_eq!(value["minimum"], min, "{schema}.{field}: {value}");
        assert_eq!(value["maximum"], 43_200, "{schema}.{field}: {value}");
    }
    node.stop().await;
}

#[tokio::test]
async fn root_gives_the_time_until_the_disk_is_full_and_null_when_the_rate_is_0() {
    let (_dir, node) = boot().await;
    let idle = call(node.get("/")).await.body;
    assert_eq!(idle["node"]["write_bytes_per_s"], 0, "{idle}");
    assert_eq!(
        idle["node"].get("time_until_full_s"),
        Some(&Value::Null),
        "{idle}"
    );
    call(node.post("/s/s", "x".repeat(10_000))).await;
    node.clock.advance(1_000);
    let busy = call(node.get("/")).await.body;
    let free = busy["node"]["disk_free_bytes"].as_u64().expect("free");
    let rate = busy["node"]["write_bytes_per_s"].as_u64().expect("rate");
    assert!(rate > 0, "{busy}");
    assert_eq!(busy["node"]["time_until_full_s"], free / rate, "{busy}");
    node.stop().await;
}

#[tokio::test]
async fn root_sends_no_cors_headers_and_openapi_keeps_them() {
    let (_dir, node) = boot().await;
    for accept in ["application/json", "text/plain"] {
        let root = node
            .get("/")
            .header("Origin", "http://evil.example")
            .header("Accept", accept)
            .send()
            .await
            .expect("send");
        assert_eq!(root.status(), StatusCode::OK, "{accept}");
        assert!(
            root.headers().get("access-control-allow-origin").is_none(),
            "{accept}: {:?}",
            root.headers()
        );
    }
    let doc = call(
        node.get("/openapi.json")
            .header("Origin", "http://evil.example"),
    )
    .await;
    assert_eq!(doc.headers["access-control-allow-origin"], "*");
    let text = guide(&node).await;
    let phrase = "In open mode, a web page in your browser can read the streams of this node.";
    assert!(text.contains(phrase), "the guide lacks: {phrase}");
    node.stop().await;
}

#[tokio::test]
async fn root_replies_vary_by_accept() {
    let (_dir, node) = boot().await;
    for accept in ["application/json", "text/plain"] {
        let root = node
            .get("/")
            .header("Accept", accept)
            .send()
            .await
            .expect("send");
        let vary = root
            .headers()
            .get_all("vary")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .collect::<Vec<_>>()
            .join(",")
            .to_ascii_lowercase();
        assert!(vary.contains("accept"), "{accept}: {vary:?}");
    }
    node.stop().await;
}

#[tokio::test]
async fn the_guide_answers_while_the_node_recovers_and_the_json_status_waits() {
    let node = Arc::new(OnceLock::new());
    let (listener, url) = listen().await;
    let server = tokio::spawn(verdandi_http::serve(
        listener,
        Arc::clone(&node),
        std::future::pending(),
    ));
    let http = Client::new();
    let plain = http
        .get(format!("{url}/"))
        .header("Accept", "text/plain")
        .send()
        .await
        .expect("send");
    assert_eq!(plain.status(), StatusCode::OK);
    assert!(
        plain
            .text()
            .await
            .expect("body")
            .starts_with("VERDANDI GUIDE")
    );
    let json = call(http.get(format!("{url}/"))).await;
    refused(&json, StatusCode::SERVICE_UNAVAILABLE);
    server.abort();
}

#[tokio::test]
async fn route_errors_point_to_the_guide_first_and_name_every_route() {
    let (_dir, node) = boot().await;
    let http = Client::new();
    for request in [
        node.get("/nowhere"),
        http.request(Method::PATCH, format!("{}/s/x", node.url)),
        http.request(Method::PUT, format!("{}/", node.url)),
    ] {
        let reply = call(request).await;
        let next = reply.body["next"].as_str().expect("next");
        assert!(
            next.starts_with("read the guide: GET / with Accept: text/plain"),
            "{next}"
        );
        for route in [
            "GET /,",
            "GET /openapi.json",
            "GET /livez",
            "GET /readyz",
            "GET /s/{stream}?from=",
        ] {
            assert!(next.contains(route), "{route}: {next}");
        }
    }
    drop(http);
    node.stop().await;
}
