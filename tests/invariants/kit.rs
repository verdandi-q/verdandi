use std::path::Path;
use std::sync::{Arc, OnceLock};

use reqwest::{RequestBuilder, StatusCode};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use verdandi_core::Timing;
use verdandi_core::ids::MessageId;
use verdandi_store::engine::{
    Delivery, Engine, Error, Handle, NewMessage, OsRandom, Take, Taken, TestClock,
};
use verdandi_store::fs::Fs;

pub const DATA: &str = "/data";

pub struct Node {
    pub handle: Handle,
    pub task: JoinHandle<Result<(), Error>>,
}

pub fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

pub fn boot<F: Fs + Clone>(rt: &Runtime, fs: &F, dir: &Path, clock: &TestClock) -> Node {
    let (engine, handle) = rt
        .block_on(Engine::open(
            fs.clone(),
            dir,
            Arc::new(clock.clone()),
            Box::new(OsRandom),
        ))
        .expect("open");
    let task = rt.spawn(engine.run());
    Node { handle, task }
}

pub fn stop(rt: &Runtime, node: Node) -> Result<(), Error> {
    drop(node.handle);
    rt.block_on(node.task).expect("engine task")
}

pub fn publish(rt: &Runtime, node: &Node, stream: &str, value: &[u8]) -> Result<MessageId, Error> {
    let message = NewMessage {
        stream: stream.to_owned(),
        headers: Vec::new(),
        value: value.to_vec(),
        timing: Timing::default(),
    };
    rt.block_on(node.handle.publish(vec![message], None))
        .map(|p| p.first)
}

pub fn take(rt: &Runtime, node: &Node, group: &str, stream: &str) -> Vec<Delivery> {
    let taken = rt.block_on(node.handle.take(Take {
        group: group.to_owned(),
        selector: stream.to_owned(),
        limit: 100,
        lease_ms: 30_000,
        wait: false,
    }));
    match taken {
        Ok(Taken::Leased { messages, .. }) => messages,
        _ => Vec::new(),
    }
}

pub fn ids(deliveries: &[Delivery]) -> Vec<MessageId> {
    deliveries.iter().map(|d| d.handed.id).collect()
}

pub fn serve(rt: &Runtime, node: verdandi_http::Node) -> String {
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("address"));
        tokio::spawn(verdandi_http::serve(listener, node, std::future::pending()));
        url
    })
}

pub fn ready(node: &Node) -> verdandi_http::Node {
    Arc::new(OnceLock::from(node.handle.clone()))
}

pub async fn send(request: RequestBuilder) -> (StatusCode, Value) {
    let response = request.send().await.expect("send");
    let status = response.status();
    let bytes = response.bytes().await.expect("body");
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("a JSON body")
    };
    (status, body)
}
