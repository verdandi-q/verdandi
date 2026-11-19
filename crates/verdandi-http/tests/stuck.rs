// allow: each test target uses a part of the shared kit
#[allow(dead_code)]
mod kit;

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use kit::{call, listen};
use reqwest::{Client, StatusCode};
use verdandi_store::fs::Syncing;

#[tokio::test]
async fn livez_gives_503_while_a_disk_write_is_stuck_over_5_s() {
    let (listener, url) = listen().await;
    tokio::spawn(verdandi_http::serve(
        listener,
        Arc::new(OnceLock::new()),
        std::future::pending(),
    ));
    let http = Client::new();
    let livez = || call(http.get(format!("{url}/livez")));
    let syncing = Syncing::begin();
    assert_eq!(livez().await.status, StatusCode::OK);
    tokio::time::sleep(Duration::from_millis(5_300)).await;
    assert_eq!(livez().await.status, StatusCode::SERVICE_UNAVAILABLE);
    drop(syncing);
    assert_eq!(livez().await.status, StatusCode::OK);
}
