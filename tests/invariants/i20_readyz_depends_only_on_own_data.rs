use std::path::Path;
use std::sync::{Arc, OnceLock};

use reqwest::{Client, StatusCode};
use verdandi_sim::fault_fs::FaultFs;
use verdandi_store::engine::TestClock;

use crate::kit::{DATA, boot, runtime, send, serve};

#[test]
fn i20_readyz_gives_200_once_the_node_holds_its_own_data_with_no_peer() {
    let rt = runtime();
    let node = Arc::new(OnceLock::new());
    let url = serve(&rt, Arc::clone(&node));
    let http = Client::new();
    let (before, _) = rt.block_on(send(http.get(format!("{url}/readyz"))));
    assert_eq!(before, StatusCode::SERVICE_UNAVAILABLE);
    let fs = FaultFs::new(20, 1 << 40);
    let engine = boot(&rt, &fs, Path::new(DATA), &TestClock::new(1_000));
    node.set(engine.handle.clone()).ok().expect("set once");
    let (after, _) = rt.block_on(send(http.get(format!("{url}/readyz"))));
    assert_eq!(after, StatusCode::OK);
}
