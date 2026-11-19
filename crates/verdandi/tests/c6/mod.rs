use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::TcpListener as StdListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use reqwest::{Client, StatusCode};
use serde_json::Value;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::sleep;

pub const KEY: &str = "0123456789abcdef0123456789abcdef";
pub const STREAM: &str = "c6";
const LB_IP: &str = "127.0.0.200";

pub struct Node {
    pub ip: String,
    pub dir: PathBuf,
    url: String,
    backup: Option<String>,
    child: Option<Child>,
    keep: bool,
    pub restarts: usize,
    pub up: Arc<AtomicBool>,
    pub pin: Arc<AtomicBool>,
    ready: Arc<AtomicBool>,
}

impl Node {
    pub fn start(&mut self) {
        assert!(self.child.is_none(), "{} is running", self.ip);
        std::fs::create_dir_all(&self.dir).expect("node dir");
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join("stderr.log"))
            .expect("log file");
        let mut command = Command::new(env!("CARGO_BIN_EXE_verdandi"));
        command
            .current_dir(&self.dir)
            .env("VERDANDI_CLUSTER_KEY", KEY)
            .env("VERDANDI_CLUSTER_URL", &self.url)
            .env("POD_IP", &self.ip)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(log));
        if let Some(backup) = &self.backup {
            command.env("VERDANDI_BACKUP_URL", backup);
        }
        self.child = Some(command.spawn().expect("spawn the binary"));
        self.keep = true;
    }

    pub fn alive(&mut self) -> bool {
        self.child
            .as_mut()
            .is_some_and(|c| c.try_wait().expect("try wait").is_none())
    }

    pub fn kill9(&mut self) {
        self.keep = false;
        if let Some(mut child) = self.child.take() {
            child.kill().expect("kill");
            child.wait().expect("wait");
        }
    }

    pub fn signal(&mut self, name: &str) {
        self.keep &= name != "TERM";
        let pid = self.child.as_ref().expect("a running node").id();
        let sent = Command::new("kill")
            .args([&format!("-{name}"), &pid.to_string()])
            .status()
            .expect("kill");
        assert!(sent.success(), "kill -{name} {pid}: {sent}");
    }

    pub async fn wait_exit(&mut self, secs: u64) -> ExitStatus {
        self.keep = false;
        let deadline = Instant::now() + Duration::from_secs(secs);
        loop {
            let child = self.child.as_mut().expect("a running node");
            if let Some(status) = child.try_wait().expect("try wait") {
                self.child = None;
                return status;
            }
            assert!(Instant::now() < deadline, "{} did not exit", self.ip);
            sleep(Duration::from_millis(100)).await;
        }
    }

    pub fn log(&self) -> String {
        std::fs::read_to_string(self.dir.join("stderr.log")).unwrap_or_default()
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.kill9();
    }
}

pub struct Cluster {
    pub nodes: Vec<Node>,
    pub lb: String,
    pub root: PathBuf,
    pub http: Client,
}

type Switch = (String, Arc<AtomicBool>, Arc<AtomicBool>);

async fn forward(listener: TcpListener, nodes: Vec<Switch>) {
    let turn = AtomicUsize::new(0);
    loop {
        let Ok((mut from, _)) = listener.accept().await else {
            return;
        };
        let pick = |need_ready: bool| -> Vec<&str> {
            nodes
                .iter()
                .filter(|(_, up, ready)| up.load(Relaxed) && (!need_ready || ready.load(Relaxed)))
                .map(|(ip, ..)| ip.as_str())
                .collect()
        };
        let mut live = pick(true);
        if live.is_empty() {
            live = pick(false);
        }
        if live.is_empty() {
            continue;
        }
        let ip = live[turn.fetch_add(1, Relaxed) % live.len()].to_owned();
        tokio::spawn(async move {
            if let Ok(mut onward) = TcpStream::connect((ip.as_str(), 8080)).await {
                let _ = tokio::io::copy_bidirectional(&mut from, &mut onward).await;
            }
        });
    }
}

async fn probe(ip: String, ready: Arc<AtomicBool>, pin: Arc<AtomicBool>) {
    let http = Client::new();
    loop {
        let reply = http
            .get(format!("http://{ip}:8080/readyz"))
            .timeout(Duration::from_secs(1))
            .send()
            .await;
        let ok = reply.is_ok_and(|r| r.status() == StatusCode::OK);
        ready.store(ok || pin.load(Relaxed), Relaxed);
        sleep(Duration::from_millis(500)).await;
    }
}

pub async fn status(http: &Client, ip: &str) -> Option<(u64, BTreeSet<u64>)> {
    let reply = http
        .get(format!("http://{ip}:8080/s/_vd.status"))
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .ok()?;
    let body: Value = serde_json::from_slice(&reply.bytes().await.ok()?).ok()?;
    let members = body["members"].as_array()?.iter().filter_map(Value::as_u64);
    Some((body["id"].as_u64()?, members.collect()))
}

impl Cluster {
    pub async fn new(row: &str, count: u8, backup: bool) -> Cluster {
        let target = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/c6");
        let root = target.join(row);
        std::fs::remove_dir_all(&root).ok();
        std::fs::create_dir_all(&root).expect("row dir");
        let root = root.canonicalize().expect("row dir");
        let bucket = backup.then(|| {
            std::fs::create_dir_all(root.join("bucket")).expect("bucket");
            format!("file://{}", root.join("bucket").display())
        });
        let listener = TcpListener::bind((LB_IP, 0)).await.expect("bind the lb");
        let lb = format!("http://{}", listener.local_addr().expect("lb address"));
        let mut nodes = Vec::new();
        let mut switches = Vec::new();
        for n in 1..=count {
            let ip = format!("127.0.0.{n}");
            assert!(
                StdListener::bind((ip.as_str(), 8080)).is_ok(),
                "ABORTED {ip}:8080 is busy"
            );
            let up = Arc::new(AtomicBool::new(true));
            let ready = Arc::new(AtomicBool::new(false));
            let pin = Arc::new(AtomicBool::new(false));
            switches.push((ip.clone(), up.clone(), Arc::clone(&ready)));
            tokio::spawn(probe(ip.clone(), ready.clone(), pin.clone()));
            nodes.push(Node {
                dir: root.join(format!("n{n}")),
                ip,
                url: lb.clone(),
                backup: bucket.clone(),
                child: None,
                keep: false,
                restarts: 0,
                up,
                pin,
                ready,
            });
        }
        tokio::spawn(forward(listener, switches));
        let http = Client::builder()
            .pool_max_idle_per_host(0)
            .timeout(Duration::from_secs(8))
            .build()
            .expect("client");
        Cluster {
            nodes,
            lb,
            root,
            http,
        }
    }

    pub fn start_all(&mut self) {
        self.nodes.iter_mut().for_each(Node::start);
    }

    pub fn restarts(&self) -> usize {
        self.nodes.iter().map(|n| n.restarts).sum()
    }

    pub async fn start_staged(&mut self) {
        self.nodes[0].start();
        self.wait_members(1, 90).await;
        self.nodes.iter_mut().skip(1).for_each(Node::start);
    }

    pub fn running(&mut self) -> Vec<String> {
        for node in &mut self.nodes {
            if node.keep && node.child.is_some() && !node.alive() {
                node.child = None;
                node.restarts += 1;
                node.start();
            }
        }
        self.nodes
            .iter_mut()
            .filter_map(|n| n.alive().then(|| n.ip.clone()))
            .collect()
    }

    pub fn logs(&self) -> String {
        self.nodes
            .iter()
            .map(|n| {
                let log = n.log();
                let tail: Vec<&str> = log.lines().rev().take(12).collect();
                format!(
                    "--- {}\n{}",
                    n.ip,
                    tail.into_iter().rev().collect::<Vec<_>>().join("\n")
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub async fn try_members(&mut self, want: usize, secs: u64) -> Result<Duration, String> {
        let started = Instant::now();
        loop {
            let ips = self.running();
            let mut sets = Vec::new();
            for ip in &ips {
                if let Some(s) = status(&self.http, ip).await {
                    sets.push(s.1);
                }
            }
            let agree = sets.iter().all(|m| m.len() == want && *m == sets[0]);
            if !ips.is_empty() && sets.len() == ips.len() && agree {
                return Ok(started.elapsed());
            }
            if started.elapsed() >= Duration::from_secs(secs) {
                let sizes: Vec<usize> = sets.iter().map(BTreeSet::len).collect();
                return Err(format!("not {want} members in {secs} s: sizes {sizes:?}"));
            }
            sleep(Duration::from_millis(250)).await;
        }
    }

    pub async fn wait_members(&mut self, want: usize, secs: u64) -> Duration {
        match self.try_members(want, secs).await {
            Ok(took) => took,
            Err(e) => panic!("{e}\n{}", self.logs()),
        }
    }

    pub async fn wait_ready(&mut self, secs: u64) {
        let started = Instant::now();
        loop {
            let ips = self.running();
            let mut all = !ips.is_empty();
            for ip in &ips {
                all &= self.ready(ip).await;
            }
            if all {
                return;
            }
            assert!(
                started.elapsed() < Duration::from_secs(secs),
                "not ready in {secs} s\n{}",
                self.logs()
            );
            sleep(Duration::from_millis(250)).await;
        }
    }

    pub async fn ready(&self, ip: &str) -> bool {
        let reply = self
            .http
            .get(format!("http://{ip}:8080/readyz"))
            .timeout(Duration::from_secs(2))
            .send()
            .await;
        reply.is_ok_and(|r| r.status() == StatusCode::OK)
    }

    pub fn dirs(&self) -> Vec<PathBuf> {
        self.nodes.iter().map(|n| n.dir.join("data")).collect()
    }

    pub fn clean(mut self) {
        self.nodes.iter_mut().for_each(Node::kill9);
        std::fs::remove_dir_all(&self.root).ok();
    }
}

#[derive(Default, Clone)]
pub struct Record {
    pub acked: BTreeMap<String, String>,
    pub settled: BTreeSet<String>,
    pub sent: usize,
}

pub struct Load {
    pub rec: Arc<Mutex<Record>>,
    stop: Arc<AtomicBool>,
    tasks: Vec<JoinHandle<()>>,
}

async fn publisher(
    http: Client,
    lb: String,
    rec: Arc<Mutex<Record>>,
    stop: Arc<AtomicBool>,
    p: usize,
    pad: usize,
    pause: u64,
) {
    let filler = "x".repeat(pad);
    let mut seq = 0u64;
    while !stop.load(Relaxed) {
        seq += 1;
        let key = format!("{p}:{seq}");
        rec.lock().unwrap().sent += 1;
        let reply = http
            .post(format!("{lb}/s/{STREAM}"))
            .body(format!("c6m:{key};{filler}"))
            .send()
            .await;
        let mut ok = false;
        if let Ok(reply) = reply
            && matches!(reply.status(), StatusCode::OK | StatusCode::CREATED)
            && let Ok(bytes) = reply.bytes().await
            && let Ok(body) = serde_json::from_slice::<Value>(&bytes)
        {
            ok = true;
            let mut rec = rec.lock().unwrap();
            for id in body["ids"].as_array().into_iter().flatten() {
                if let Some(id) = id.as_str() {
                    rec.acked.insert(id.to_owned(), key.clone());
                }
            }
        }
        sleep(Duration::from_millis(if ok { pause } else { 100 })).await;
    }
}

async fn consumer(http: Client, lb: String, rec: Arc<Mutex<Record>>, stop: Arc<AtomicBool>) {
    while !stop.load(Relaxed) {
        let reply = http
            .get(format!("{lb}/s/{STREAM}?group=g&limit=20&wait=1&lease=20"))
            .send()
            .await;
        let mut worked = false;
        if let Ok(reply) = reply
            && reply.status() == StatusCode::OK
            && let Ok(bytes) = reply.bytes().await
            && let Ok(body) = serde_json::from_slice::<Value>(&bytes)
            && let Some(receipt) = body["receipt"].as_str()
        {
            let ids: Vec<&str> = body["messages"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|m| m["id"].as_str())
                .step_by(2)
                .collect();
            let settle = http
                .post(format!("{lb}/r/{receipt}"))
                .body(serde_json::json!({ "ack": ids }).to_string())
                .send()
                .await;
            worked = true;
            if settle.is_ok_and(|r| r.status() == StatusCode::OK) {
                rec.lock()
                    .unwrap()
                    .settled
                    .extend(ids.iter().map(|id| (*id).to_owned()));
            }
        }
        if !worked {
            sleep(Duration::from_millis(200)).await;
        }
    }
}

impl Load {
    pub fn start(c: &Cluster, publishers: usize, consumers: usize, pad: usize, pause: u64) -> Load {
        let rec: Arc<Mutex<Record>> = Arc::default();
        let stop = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::new();
        for p in 0..publishers {
            tasks.push(tokio::spawn(publisher(
                c.http.clone(),
                c.lb.clone(),
                rec.clone(),
                stop.clone(),
                p,
                pad,
                pause,
            )));
        }
        for _ in 0..consumers {
            tasks.push(tokio::spawn(consumer(
                c.http.clone(),
                c.lb.clone(),
                rec.clone(),
                stop.clone(),
            )));
        }
        Load { rec, stop, tasks }
    }

    pub fn snapshot(&self) -> Record {
        self.rec.lock().unwrap().clone()
    }

    pub async fn lost_so_far(&self, c: &mut Cluster) -> Vec<String> {
        let snap = self.snapshot();
        let lost = missing(c, &snap).await;
        let settled = self.snapshot().settled;
        lost.into_iter()
            .filter(|id| !settled.contains(id))
            .collect()
    }

    pub fn acked(&self) -> usize {
        self.rec.lock().unwrap().acked.len()
    }

    pub async fn stop(self) -> Record {
        self.stop.store(true, Relaxed);
        for task in self.tasks {
            task.await.expect("load task");
        }
        std::mem::take(&mut *self.rec.lock().unwrap())
    }
}

async fn history(http: &Client, base: &str) -> Option<BTreeSet<String>> {
    let mut seen = BTreeSet::new();
    let mut url = format!("{base}/s/{STREAM}?limit=100");
    loop {
        let reply = http
            .get(&url)
            .timeout(Duration::from_secs(60))
            .send()
            .await
            .ok()?;
        if reply.status() == StatusCode::NO_CONTENT {
            return Some(seen);
        }
        if reply.status() != StatusCode::OK {
            return None;
        }
        let body: Value = serde_json::from_slice(&reply.bytes().await.ok()?).ok()?;
        let messages = body["messages"].as_array()?;
        if messages.is_empty() {
            return Some(seen);
        }
        seen.extend(
            messages
                .iter()
                .filter_map(|m| m["id"].as_str().map(str::to_owned)),
        );
        url = format!("{base}{}", body["next"].as_str()?);
    }
}

pub async fn readable(c: &mut Cluster) -> BTreeSet<String> {
    for _ in 0..30 {
        for ip in c.running() {
            if let Some(seen) = history(&c.http, &format!("http://{ip}:8080")).await {
                return seen;
            }
        }
        sleep(Duration::from_secs(2)).await;
    }
    panic!("no node served a history read\n{}", c.logs());
}

async fn drain(c: &Cluster, group: &str, into: &mut BTreeSet<String>) {
    for _ in 0..200 {
        let reply = c
            .http
            .get(format!(
                "{}/s/{STREAM}?group={group}&limit=1000&lease=3600",
                c.lb
            ))
            .send()
            .await;
        let Ok(reply) = reply else {
            sleep(Duration::from_millis(500)).await;
            continue;
        };
        if reply.status() != StatusCode::OK {
            return;
        }
        let Ok(bytes) = reply.bytes().await else {
            continue;
        };
        let body: Value = serde_json::from_slice(&bytes).expect("a JSON body");
        into.extend(
            body["messages"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|m| m["id"].as_str().map(str::to_owned)),
        );
    }
}

pub async fn missing(c: &mut Cluster, rec: &Record) -> Vec<String> {
    let want: Vec<&String> = rec
        .acked
        .keys()
        .filter(|id| !rec.settled.contains(*id))
        .collect();
    let mut have = readable(c).await;
    let mut lost: Vec<&&String> = want.iter().filter(|id| !have.contains(**id)).collect();
    if !lost.is_empty() {
        drain(c, "check", &mut have).await;
        lost = want.iter().filter(|id| !have.contains(**id)).collect();
    }
    lost.into_iter().map(|id| (**id).clone()).collect()
}

pub async fn no_loss(c: &mut Cluster, rec: &Record) {
    let lost = missing(c, rec).await;
    assert!(
        lost.is_empty(),
        "LOSS: {} acked unsettled messages are neither readable nor takeable, first {:?}\n{}",
        lost.len(),
        lost.iter()
            .take(5)
            .map(|id| &rec.acked[id])
            .collect::<Vec<_>>(),
        c.logs()
    );
}

pub async fn takeable(c: &Cluster, group: &str) -> BTreeSet<String> {
    let mut got = BTreeSet::new();
    drain(c, group, &mut got).await;
    got
}

pub fn tokens(dir: &Path, out: &mut HashSet<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            tokens(&path, out);
        } else if let Ok(bytes) = std::fs::read(&path) {
            scan(&bytes, out);
        }
    }
}

fn scan(bytes: &[u8], out: &mut HashSet<String>) {
    let tag = b"c6m:";
    let mut at = 0;
    while let Some(found) = bytes[at..].windows(tag.len()).position(|w| w == tag) {
        let start = at + found + tag.len();
        let end = bytes[start..]
            .iter()
            .take(24)
            .position(|b| *b == b';')
            .map(|n| start + n);
        if let Some(end) = end
            && bytes[start..end]
                .iter()
                .all(|b| b.is_ascii_digit() || *b == b':')
        {
            out.insert(String::from_utf8_lossy(&bytes[start..end]).into_owned());
        }
        at = start;
    }
}

pub fn below_floor(c: &Cluster, rec: &Record) -> Vec<String> {
    let mut copies: HashMap<String, usize> = HashMap::new();
    for dir in c.dirs() {
        let mut found = HashSet::new();
        tokens(&dir, &mut found);
        for key in found {
            *copies.entry(key).or_default() += 1;
        }
    }
    let keys: BTreeSet<&String> = rec.acked.values().collect();
    keys.into_iter()
        .filter(|k| copies.get(*k).copied().unwrap_or(0) < 2)
        .cloned()
        .collect()
}
