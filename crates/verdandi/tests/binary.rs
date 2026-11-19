use std::cell::RefCell;
use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use reqwest::{Client, StatusCode};
use serde_json::Value;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use verdandi_http::OPEN_MODE;

const URL: &str = "http://127.0.0.1:8080";
const CLUSTER: [&str; 2] = ["VERDANDI_CLUSTER_KEY", "VERDANDI_CLUSTER_URL"];

static PORT: Mutex<()> = Mutex::new(());

struct Running(Child);

impl Drop for Running {
    fn drop(&mut self) {
        self.0.kill().ok();
        self.0.wait().ok();
    }
}

impl Running {
    fn kill(&mut self) -> ExitStatus {
        self.0.kill().expect("kill");
        self.0.wait().expect("wait")
    }

    fn exit(&mut self) -> ExitStatus {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.0.try_wait().expect("try wait") {
                return status;
            }
            assert!(Instant::now() < deadline, "the binary did not exit");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn stderr(mut self) -> String {
        self.0.kill().ok();
        self.0.wait().expect("wait");
        let mut text = String::new();
        self.0
            .stderr
            .take()
            .expect("stderr")
            .read_to_string(&mut text)
            .expect("read stderr");
        text
    }
}

fn port() -> MutexGuard<'static, ()> {
    PORT.lock().unwrap_or_else(PoisonError::into_inner)
}

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

fn scratch() -> TempDir {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/tmp");
    std::fs::create_dir_all(&root).expect("scratch dir");
    TempDir::new_in(root).expect("temp dir")
}

fn spawn(dir: &Path, env: &[(&str, &str)]) -> Running {
    let child = Command::new(env!("CARGO_BIN_EXE_verdandi"))
        .current_dir(dir)
        .envs(env.iter().copied())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the binary");
    Running(child)
}

async fn start(dir: &Path, env: &[(&str, &str)]) -> (Running, Client) {
    if TcpListener::bind("0.0.0.0:8080").await.is_err() {
        panic!("ABORTED port 8080 is busy");
    }
    let mut running = spawn(dir, env);
    let http = Client::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = running.0.try_wait().expect("try wait") {
            panic!("the binary exited with {status}");
        }
        let ready = http.get(format!("{URL}/readyz")).send().await;
        if ready.is_ok_and(|r| r.status() == StatusCode::OK) {
            return (running, http);
        }
        assert!(Instant::now() < deadline, "the binary is not ready");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn json(response: reqwest::Response) -> (StatusCode, Value) {
    let status = response.status();
    let bytes = response.bytes().await.expect("body");
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("a JSON body")
    };
    (status, body)
}

fn strings(value: &Value) -> impl Iterator<Item = String> + '_ {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str().map(str::to_owned))
}

async fn drain(http: &Client, group: &str) -> BTreeSet<String> {
    let mut seen = BTreeSet::new();
    loop {
        let reply = http
            .get(format!("{URL}/s/load?group={group}&limit=10000"))
            .send()
            .await
            .expect("take");
        let (status, body) = json(reply).await;
        if status == StatusCode::NO_CONTENT {
            return seen;
        }
        assert_eq!(status, StatusCode::OK, "{body}");
        for message in body["messages"].as_array().expect("messages") {
            seen.insert(message["id"].as_str().expect("id").to_owned());
        }
    }
}

#[test]
fn open_mode_warns_on_stderr_and_in_get_root_at_each_start() {
    let _port = port();
    let dir = scratch();
    let rt = runtime();
    for _ in 0..2 {
        let (running, http) = rt.block_on(start(dir.path(), &[]));
        let (status, root) = rt.block_on(async {
            json(http.get(format!("{URL}/")).send().await.expect("get /")).await
        });
        assert_eq!(status, StatusCode::OK);
        assert!(strings(&root["warnings"]).any(|w| w == OPEN_MODE), "{root}");
        assert!(OPEN_MODE.contains("any web page can read"), "{OPEN_MODE}");
        let stderr = running.stderr();
        assert!(stderr.contains(OPEN_MODE), "{stderr}");
    }
}

#[test]
fn open_mode_refuses_to_start_with_a_cluster_variable() {
    let _port = port();
    let dir = scratch();
    for name in CLUSTER {
        let mut running = spawn(dir.path(), &[(name, "x")]);
        let status = running.exit();
        assert!(!status.success(), "{name}: {status}");
        let stderr = running.stderr();
        assert!(stderr.contains(name), "{stderr}");
        assert!(!stderr.contains(OPEN_MODE), "{stderr}");
    }
    assert!(!dir.path().join("data").exists());
}

#[test]
fn a_bad_backup_url_stops_the_start_and_names_the_variable() {
    let _port = port();
    let dir = scratch();
    for bad in ["x", "ftp://host/path"] {
        let mut running = spawn(dir.path(), &[("VERDANDI_BACKUP_URL", bad)]);
        let status = running.exit();
        assert!(!status.success(), "{bad}: {status}");
        let stderr = running.stderr();
        assert!(
            stderr.contains("VERDANDI_BACKUP_URL is not valid"),
            "{stderr}"
        );
    }
    assert!(!dir.path().join("data").exists());
}

#[test]
fn sigterm_ends_each_long_poll_closes_the_engine_and_exits_0() {
    let _port = port();
    let dir = scratch();
    let rt = runtime();
    let (mut running, http) = rt.block_on(start(dir.path(), &[]));
    let (status, body) = rt.block_on(async {
        json(
            http.post(format!("{URL}/s/t"))
                .body("1")
                .send()
                .await
                .expect("publish"),
        )
        .await
    });
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let published: Vec<String> = strings(&body["ids"]).collect();
    let pid = running.0.id().to_string();
    let started = Instant::now();
    let ((status, body), term) = rt.block_on(async {
        let poll = async {
            json(
                http.get(format!("{URL}/s/w?group=g&wait=60"))
                    .send()
                    .await
                    .expect("take"),
            )
            .await
        };
        let term = async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            Command::new("kill")
                .args(["-TERM", &pid])
                .status()
                .expect("kill")
        };
        tokio::join!(poll, term)
    });
    assert!(term.success(), "{term}");
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert!(body["next"].is_string(), "{body}");
    assert!(started.elapsed() < Duration::from_secs(10));
    let status = running.exit();
    assert!(status.success(), "{status}");
    drop(running);
    let (_running, http) = rt.block_on(start(dir.path(), &[]));
    let ((_, taken), (_, root)) = rt.block_on(async {
        let take = http.get(format!("{URL}/s/t?group=g")).send();
        let taken = json(take.await.expect("take")).await;
        let root = json(http.get(format!("{URL}/")).send().await.expect("get /")).await;
        (taken, root)
    });
    let ids: Vec<String> = taken["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|m| m["id"].as_str().map(str::to_owned))
        .collect();
    assert_eq!(ids, published);
    let warnings: Vec<String> = strings(&root["warnings"])
        .filter(|w| !w.starts_with("disk space low: ") && !w.starts_with("disk full in about "))
        .collect();
    assert_eq!(warnings, [OPEN_MODE], "{root}");
}

#[test]
fn sigterm_exits_0_within_the_stop_deadline_while_a_follow_client_reads_nothing() {
    let _port = port();
    let dir = scratch();
    let rt = runtime();
    let (mut running, http) = rt.block_on(start(dir.path(), &[]));
    rt.block_on(async {
        for n in 0..32u8 {
            let reply = http
                .post(format!("{URL}/s/big"))
                .header("Content-Type", "application/octet-stream")
                .body(vec![n; 1_000_000])
                .send()
                .await
                .expect("publish");
            assert_eq!(reply.status(), StatusCode::CREATED);
        }
    });
    let mut stalled = TcpStream::connect("127.0.0.1:8080").expect("connect");
    stalled
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("read timeout");
    stalled
        .write_all(b"GET /s/big?follow HTTP/1.1\r\nHost: 127.0.0.1:8080\r\n\r\n")
        .expect("request");
    let mut head = Vec::new();
    let mut chunk = [0u8; 65536];
    while !head.windows(2).any(|pair| pair == b"}\n") {
        let read = stalled.read(&mut chunk).expect("the first line arrives");
        assert!(read > 0, "the stream ended before the first line");
        head.extend_from_slice(&chunk[..read]);
    }
    std::thread::sleep(Duration::from_millis(500));
    let term = Command::new("kill")
        .args(["-TERM", &running.0.id().to_string()])
        .status()
        .expect("kill");
    let sent = Instant::now();
    assert!(term.success(), "{term}");
    let status = running.exit();
    let took = sent.elapsed();
    assert!(status.success(), "{status}");
    assert!(took >= Duration::from_secs(5), "the exit took {took:?}");
    assert!(took < Duration::from_secs(8), "{took:?}");
    drop(stalled);
}

#[test]
fn sigterm_during_recovery_exits_0_without_serving() {
    let _port = port();
    let dir = scratch();
    let data = dir.path().join("data");
    std::fs::create_dir(&data).expect("data dir");
    for epoch in 1..=128u64 {
        let file = std::fs::File::create(data.join(format!("cmp-{epoch:016x}.vdl"))).expect("file");
        file.set_len(64 << 20).expect("sparse length");
    }
    let rt = runtime();
    if rt.block_on(TcpListener::bind("0.0.0.0:8080")).is_err() {
        panic!("ABORTED port 8080 is busy");
    }
    let mut running = spawn(dir.path(), &[]);
    let http = Client::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = running.0.try_wait().expect("try wait") {
            panic!("the binary exited with {status}");
        }
        let ready = rt.block_on(http.get(format!("{URL}/readyz")).send());
        if ready.is_ok_and(|r| r.status() == StatusCode::SERVICE_UNAVAILABLE) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the binary did not start recovery"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let term = Command::new("kill")
        .args(["-TERM", &running.0.id().to_string()])
        .status()
        .expect("kill");
    assert!(term.success(), "{term}");
    let status = running.exit();
    assert!(status.success(), "{status}");
    let stderr = running.stderr();
    assert!(!stderr.lines().any(|l| l.ends_with(": ready")), "{stderr}");
}

fn profile(env: &[(&str, &str)]) -> (usize, String) {
    let dir = scratch();
    let rt = runtime();
    let (running, _http) = rt.block_on(start(dir.path(), env));
    let tasks = Path::new("/proc")
        .join(running.0.id().to_string())
        .join("task");
    let threads = std::fs::read_dir(tasks).expect("tasks").count();
    let log = running
        .stderr()
        .lines()
        .filter(|l| !l.contains("uri=/readyz"))
        .collect::<Vec<_>>()
        .join("\n");
    (threads, log)
}

#[test]
fn env_ignored_tokio_and_log_variables_change_no_thread_and_no_log_line() {
    let _port = port();
    let plain = profile(&[]);
    let tuned = profile(&[("TOKIO_WORKER_THREADS", "1"), ("RUST_LOG", "trace")]);
    assert_eq!(plain, tuned);
}

#[test]
fn sigkill_restart_keeps_every_201_id_and_redelivers_every_unacked_one() {
    let _port = port();
    let dir = scratch();
    let rt = runtime();
    let (mut running, http) = rt.block_on(start(dir.path(), &[]));
    let published = RefCell::new(BTreeSet::new());
    let acked = RefCell::new(BTreeSet::new());
    let unsure = RefCell::new(BTreeSet::new());
    let publisher = |n: usize| {
        let (http, published) = (&http, &published);
        async move {
            for i in 0.. {
                let Ok(reply) = http
                    .post(format!("{URL}/s/load"))
                    .body(format!("[{n},{i}]"))
                    .send()
                    .await
                else {
                    return;
                };
                if reply.status() != StatusCode::CREATED {
                    return;
                }
                let Ok(bytes) = reply.bytes().await else {
                    return;
                };
                let body: Value = serde_json::from_slice(&bytes).expect("a JSON body");
                published.borrow_mut().extend(strings(&body["ids"]));
            }
        }
    };
    let consumer = async {
        loop {
            let Ok(reply) = http
                .get(format!("{URL}/s/load?group=g&limit=50&wait=1"))
                .send()
                .await
            else {
                return;
            };
            let Ok(bytes) = reply.bytes().await else {
                return;
            };
            let Ok(body) = serde_json::from_slice::<Value>(&bytes) else {
                continue;
            };
            let ids: Vec<String> = body["messages"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|m| m["id"].as_str().map(str::to_owned))
                .collect();
            let receipt = body["receipt"].as_str().unwrap_or_default();
            unsure.borrow_mut().extend(ids.iter().cloned());
            let Ok(ack) = http.delete(format!("{URL}/r/{receipt}")).send().await else {
                return;
            };
            for id in &ids {
                unsure.borrow_mut().remove(id);
            }
            if ack.status() == StatusCode::OK {
                acked.borrow_mut().extend(ids);
            }
        }
    };
    let killer = async {
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        running.kill()
    };
    rt.block_on(async {
        tokio::join!(
            publisher(0),
            publisher(1),
            publisher(2),
            publisher(3),
            consumer,
            killer
        )
    });
    drop(running);
    let published = published.into_inner();
    let acked = acked.into_inner();
    let unsure = unsure.into_inner();
    assert!(!published.is_empty() && !acked.is_empty());
    let (_running, http) = rt.block_on(start(dir.path(), &[]));
    let stored = rt.block_on(drain(&http, "check"));
    let missing: Vec<_> = published.difference(&stored).collect();
    assert!(missing.is_empty(), "lost 201 ids: {missing:?}");
    let redelivered = rt.block_on(drain(&http, "g"));
    let unacked: BTreeSet<_> = published
        .iter()
        .filter(|id| !acked.contains(*id) && !unsure.contains(*id))
        .cloned()
        .collect();
    let missing: Vec<_> = unacked.difference(&redelivered).collect();
    assert!(missing.is_empty(), "unacked ids not delivered: {missing:?}");
}

const KEY: &str = "0123456789abcdef0123456789abcdef";

fn spawn_node(dir: &Path, url: &str, ip: &str) -> Running {
    let log = std::fs::File::create(dir.join("stderr.log")).expect("log file");
    let child = Command::new(env!("CARGO_BIN_EXE_verdandi"))
        .current_dir(dir)
        .env("VERDANDI_CLUSTER_KEY", KEY)
        .env("VERDANDI_CLUSTER_URL", url)
        .env("POD_IP", ip)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
        .spawn()
        .expect("spawn the binary");
    Running(child)
}

async fn status(http: &Client, ip: &str) -> Option<(u64, Option<u64>, BTreeSet<u64>)> {
    let reply = http
        .get(format!("http://{ip}:8080/s/_vd.status"))
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .ok()?;
    let body: Value = serde_json::from_slice(&reply.bytes().await.ok()?).ok()?;
    let members = body["members"].as_array()?.iter().filter_map(Value::as_u64);
    Some((
        body["id"].as_u64()?,
        body["leader"].as_u64(),
        members.collect(),
    ))
}

async fn until<T>(what: &str, secs: u64, mut probe: impl AsyncFnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if let Some(found) = probe().await {
            return found;
        }
        assert!(Instant::now() < deadline, "timeout: {what}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[test]
fn three_processes_form_one_cluster_and_one_leaves_on_sigterm() {
    let _port = port();
    let rt = runtime();
    let dirs = [scratch(), scratch(), scratch()];
    let ips = ["127.0.0.1", "127.0.0.2", "127.0.0.3"];
    let url = format!("http://{}:8080", ips[0]);
    let mut nodes: Vec<Running> = (0..3)
        .map(|i| spawn_node(dirs[i].path(), &url, ips[i]))
        .collect();
    let client = Client::new();
    let logs = || {
        dirs.iter()
            .map(|d| std::fs::read_to_string(d.path().join("stderr.log")).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n-----\n")
    };
    let ids = rt.block_on(until("three members and one leader", 150, async || {
        let mut all = Vec::new();
        for ip in &ips {
            all.push(status(&client, ip).await?);
        }
        let ids: BTreeSet<u64> = all.iter().map(|s| s.0).collect();
        let leader = all[0].1?;
        let one = all.iter().all(|s| s.1 == Some(leader) && s.2 == ids);
        (ids.len() == 3 && ids.contains(&leader) && one).then_some((ids, leader, all))
    }));
    let (ids, leader, all) = ids;
    let Some(at) = all.iter().position(|s| s.0 != leader) else {
        panic!("no follower\n{}", logs());
    };
    let gone = all[at].0;
    let pid = nodes[at].0.id().to_string();
    let signalled = Command::new("kill").args(["-TERM", &pid]).status();
    assert!(signalled.expect("kill").success());
    let deadline = Instant::now() + Duration::from_secs(150);
    let exit = loop {
        if let Some(status) = nodes[at].0.try_wait().expect("try wait") {
            break status;
        }
        assert!(Instant::now() < deadline, "no exit\n{}", logs());
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(exit.success(), "{exit}\n{}", logs());
    let rest: BTreeSet<u64> = ids.iter().copied().filter(|i| *i != gone).collect();
    rt.block_on(until("the node left Meta.members", 60, async || {
        for ip in ips
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != at)
            .map(|(_, p)| p)
        {
            let s = status(&client, ip).await?;
            if s.2 != rest {
                return None;
            }
        }
        Some(())
    }));
}
