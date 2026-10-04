// allow: each test target uses a part of the shared kit
#[allow(dead_code)]
mod kit;

use kit::{boot, call, refused};
use reqwest::StatusCode;

const GROWTH_MAX: u64 = 96 << 20;

fn status(field: &str) -> u64 {
    let text = std::fs::read_to_string("/proc/self/status").expect("status");
    let line = text
        .lines()
        .find(|l| l.starts_with(field))
        .expect("a status field");
    let kib: u64 = line[field.len()..]
        .trim()
        .trim_end_matches(" kB")
        .parse()
        .expect("a number");
    kib << 10
}

fn reset_peak() -> u64 {
    std::fs::write("/proc/self/clear_refs", "5").expect("reset the peak");
    status("VmRSS:")
}

#[tokio::test]
async fn a_16_mib_array_of_tiny_elements_is_refused_without_a_vector_of_every_element() {
    let (_dir, node) = boot().await;
    let publish = format!("[1{}]", ",1".repeat((16 << 20) / 2 - 2));
    let before = reset_peak();
    refused(
        &call(node.post("/s/m", publish)).await,
        StatusCode::PAYLOAD_TOO_LARGE,
    );
    let growth = status("VmHWM:").saturating_sub(before);
    assert!(growth < GROWTH_MAX, "publish grew {growth} bytes");
    let one = call(node.post("/s/m", "1")).await;
    assert_eq!(one.status, StatusCode::CREATED);
    let leased = call(node.get("/s/m?group=g")).await;
    let receipt = leased.body["receipt"].as_str().expect("receipt");
    let settle = format!(r#"{{"ack":["a"{}]}}"#, r#","a""#.repeat((16 << 20) / 4 - 4));
    let before = reset_peak();
    refused(
        &call(node.post(&format!("/r/{receipt}"), settle)).await,
        StatusCode::BAD_REQUEST,
    );
    let growth = status("VmHWM:").saturating_sub(before);
    assert!(growth < GROWTH_MAX, "settle grew {growth} bytes");
    node.stop().await;
}
