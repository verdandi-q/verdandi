use std::path::Path;
use std::time::Duration;

use reqwest::{Client, StatusCode};
use verdandi_sim::fault_fs::FaultFs;
use verdandi_store::engine::TestClock;

use crate::kit::{DATA, boot, ready, runtime, send, serve};

#[test]
fn i07_a_waiting_take_answers_with_the_message_published_during_its_wait() {
    let rt = runtime();
    let fs = FaultFs::new(7, 1 << 40);
    let node = boot(&rt, &fs, Path::new(DATA), &TestClock::new(1_000));
    let url = serve(&rt, ready(&node));
    let http = Client::new();
    let take = send(http.get(format!("{url}/s/w?group=g&wait=30")));
    let publish = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        send(http.post(format!("{url}/s/w")).body("1")).await
    };
    let ((status, taken), (_, published)) = rt.block_on(async { tokio::join!(take, publish) });
    assert_eq!(status, StatusCode::OK);
    assert_eq!(taken["messages"][0]["id"], published["ids"][0]);
}
