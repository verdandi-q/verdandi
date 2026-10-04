use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use reqwest::header::HeaderMap;
use reqwest::{Body, Client, RequestBuilder, StatusCode};
use serde_json::Value;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::Level;
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::fmt::MakeWriter;
use verdandi_store::engine::{Engine, Error, Handle, OsRandom, Random, TestClock};
use verdandi_store::fs::{Fs, RealFs};

pub const START_MS: u64 = 1_790_000_000_000;

pub struct Node {
    pub url: String,
    pub clock: TestClock,
    pub handle: Handle,
    http: Client,
    server: JoinHandle<std::io::Result<()>>,
    engine: JoinHandle<Result<(), Error>>,
}

pub struct Reply {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub bytes: Vec<u8>,
    pub body: Value,
}

pub fn scratch() -> TempDir {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/tmp");
    std::fs::create_dir_all(&root).expect("scratch dir");
    TempDir::new_in(root).expect("temp dir")
}

pub async fn listen() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("address"));
    (listener, url)
}

pub async fn start(dir: &Path, clock: &TestClock) -> Node {
    start_on(RealFs, dir, clock).await
}

pub async fn start_on<F: Fs + Clone>(fs: F, dir: &Path, clock: &TestClock) -> Node {
    start_as(fs, dir, clock, Box::new(OsRandom)).await
}

pub async fn start_as<F: Fs + Clone>(
    fs: F,
    dir: &Path,
    clock: &TestClock,
    random: Box<dyn Random>,
) -> Node {
    let (engine, handle) = Engine::open(fs, dir, Arc::new(clock.clone()), random)
        .await
        .expect("open");
    let engine = tokio::spawn(engine.run());
    let probe = handle.clone();
    let (listener, url) = listen().await;
    let server = tokio::spawn(verdandi_http::serve(
        listener,
        Arc::new(OnceLock::from(handle)),
        std::future::pending(),
    ));
    Node {
        url,
        clock: clock.clone(),
        handle: probe,
        http: Client::new(),
        server,
        engine,
    }
}

pub async fn boot() -> (TempDir, Node) {
    let dir = scratch();
    let node = start(dir.path(), &TestClock::new(START_MS)).await;
    (dir, node)
}

impl Node {
    pub fn get(&self, path: &str) -> RequestBuilder {
        self.http.get(format!("{}{path}", self.url))
    }

    pub fn post(&self, path: &str, body: impl Into<Body>) -> RequestBuilder {
        self.http.post(format!("{}{path}", self.url)).body(body)
    }

    pub fn put(&self, path: &str, body: impl Into<Body>) -> RequestBuilder {
        self.http.put(format!("{}{path}", self.url)).body(body)
    }

    pub fn delete(&self, path: &str) -> RequestBuilder {
        self.http.delete(format!("{}{path}", self.url))
    }

    pub async fn stop(self) {
        self.server.abort();
        drop(self.http);
        drop(self.handle);
        let stopped = tokio::time::timeout(Duration::from_secs(10), self.engine)
            .await
            .expect("the engine stops when the server goes");
        stopped.expect("join").expect("clean stop");
    }
}

pub async fn call(request: RequestBuilder) -> Reply {
    let response = request.send().await.expect("send");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response.bytes().await.expect("body").to_vec();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("a JSON body")
    };
    Reply {
        status,
        headers,
        bytes,
        body,
    }
}

pub fn strings(value: &Value) -> Vec<String> {
    value
        .as_array()
        .expect("an array")
        .iter()
        .map(|v| v.as_str().expect("a string").to_owned())
        .collect()
}

pub fn ids(reply: &Reply) -> Vec<String> {
    strings(&reply.body["ids"])
}

pub fn taken(reply: &Reply) -> Vec<String> {
    reply.body["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .map(|m| m["id"].as_str().expect("an id").to_owned())
        .collect()
}

pub fn refused(reply: &Reply, status: StatusCode) {
    assert_eq!(reply.status, status, "body: {}", reply.body);
    assert!(reply.body["error"].is_string(), "no error: {}", reply.body);
    assert!(reply.body["next"].is_string(), "no next: {}", reply.body);
}

pub async fn read_until(node: &Node, path: &str, count: usize) -> Reply {
    let found = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let reply = call(node.get(path)).await;
            if taken(&reply).len() >= count {
                return reply;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    found.expect("the messages did not appear")
}

#[derive(Clone, Default)]
pub struct LogBuffer(Arc<Mutex<Vec<u8>>>);

impl LogBuffer {
    pub fn lines(&self) -> Vec<String> {
        let bytes = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        String::from_utf8_lossy(&bytes)
            .lines()
            .map(|line| line.trim_start().to_owned())
            .collect()
    }
}

impl io::Write for LogBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl MakeWriter<'_> for LogBuffer {
    type Writer = LogBuffer;

    fn make_writer(&self) -> LogBuffer {
        self.clone()
    }
}

pub fn capture_log() -> (DefaultGuard, LogBuffer) {
    let buffer = LogBuffer::default();
    let guard = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_max_level(Level::INFO)
            .without_time()
            .with_writer(buffer.clone())
            .finish(),
    );
    (guard, buffer)
}
