use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use reqwest::StatusCode;
use tokio::net::TcpListener;
use verdandi_http::{Cluster, Node};
use verdandi_net::{Hello, Reply, open_members, seal, seal_members};

const KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn sealed(key: &[u8], sent_ms: u64) -> Vec<u8> {
    let hello = Hello {
        node: 7,
        addr: "10.0.0.7:7946".into(),
        incarnation: 1,
        sent_ms,
        cluster: None,
    };
    seal(key, &hello).unwrap()
}

async fn start(cluster: Option<Arc<Cluster>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(verdandi_http::serve_with(
        listener,
        Node::default(),
        cluster,
        std::future::pending(),
    ));
    url
}

fn cluster() -> Arc<Cluster> {
    let reply = seal_members(
        KEY,
        &Reply {
            cluster: None,
            members: vec![(1, "10.0.0.1:7946".into(), true)],
        },
    )
    .unwrap();
    Arc::new(Cluster::new(
        KEY.to_vec(),
        Box::new(move |_| reply.clone()),
        Box::new(|| "{}".into()),
    ))
}

async fn post(url: &str, body: Vec<u8>) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{url}/s/_vd.hello"))
        .header("host", "lb.example")
        .body(body)
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn a_valid_hello_gets_the_sealed_members_and_a_repeat_is_refused() {
    let url = start(Some(cluster())).await;
    let body = sealed(KEY, now_ms());
    let reply = post(&url, body.clone()).await;
    assert_eq!(reply.status(), StatusCode::OK);
    let members = open_members(KEY, &reply.bytes().await.unwrap())
        .unwrap()
        .members;
    assert_eq!(members, vec![(1, "10.0.0.1:7946".to_owned(), true)]);
    assert_eq!(post(&url, body).await.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn a_stale_future_foreign_or_large_hello_is_refused() {
    let url = start(Some(cluster())).await;
    let now = now_ms();
    for sent in [now - 31_000, now + 31_000] {
        let reply = post(&url, sealed(KEY, sent)).await;
        assert_eq!(reply.status(), StatusCode::BAD_REQUEST);
    }
    let foreign = sealed(b"fedcba9876543210fedcba9876543210", now);
    assert_eq!(post(&url, foreign).await.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        post(&url, vec![7; 100]).await.status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        post(&url, vec![7; 5000]).await.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

#[tokio::test]
async fn open_mode_has_no_hello_route_and_no_status() {
    let url = start(None).await;
    assert_eq!(
        post(&url, sealed(KEY, now_ms())).await.status(),
        StatusCode::FORBIDDEN
    );
    let status = reqwest::get(format!("{url}/s/_vd.status")).await.unwrap();
    assert_ne!(status.status(), StatusCode::OK);
}
