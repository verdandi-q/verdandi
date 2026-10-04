use std::path::Path;

use reqwest::{Client, StatusCode};
use verdandi_sim::fault_fs::FaultFs;
use verdandi_store::engine::TestClock;

use crate::kit::{DATA, boot, ready, runtime, send, serve};

#[test]
fn i04_a_publish_and_a_take_need_no_call_that_creates_a_stream_or_a_group() {
    let rt = runtime();
    let fs = FaultFs::new(4, 1 << 40);
    let node = boot(&rt, &fs, Path::new(DATA), &TestClock::new(1_000));
    let url = serve(&rt, ready(&node));
    let http = Client::new();
    let (status, published) = rt.block_on(send(http.post(format!("{url}/s/never.seen")).body("1")));
    assert_eq!(status, StatusCode::CREATED);
    let (status, taken) = rt.block_on(send(http.get(format!("{url}/s/never.seen?group=new"))));
    assert_eq!(status, StatusCode::OK);
    assert_eq!(taken["messages"][0]["id"], published["ids"][0]);
}
