use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use reqwest::{Client, Response, StatusCode};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const URL: &str = "http://127.0.0.1:8080";
const BATCH: usize = 50;
const BATCHES_PER_SECOND: u64 = 10;
const PAD: usize = 980;
const CONSUMERS: usize = 3;
const NACK_ONE_IN: u64 = 200;
const RSS_MAX: u64 = 4_000_000_000;
const RSS_EVERY: Duration = Duration::from_secs(5);
const DRAIN_MAX: Duration = Duration::from_secs(180);
const STALL_GRACE: Duration = Duration::from_secs(60);
const HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const WAIT: u8 = 0;
const BETWEEN: u8 = 1;
const SETTLE: u8 = 2;

pub struct Config {
    pub duration: Duration,
    pub kill_every: Duration,
    pub require_phases: bool,
}

struct Rec {
    published: bool,
    delivered: bool,
    acked: Option<Instant>,
}

#[derive(Default)]
struct Ledger {
    ids: HashMap<String, Rec>,
    seqs: Vec<bool>,
    redelivered_after_ack: u64,
}

struct Ctx {
    http: Client,
    ledger: Mutex<Ledger>,
    failure: Mutex<Option<String>>,
    producer_done: AtomicBool,
    empty_after_producer: AtomicU64,
    settles: AtomicU64,
    conflicts: AtomicU64,
}

impl Ctx {
    fn fail(&self, message: String) {
        self.failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert(message);
    }

    fn check(&self) {
        if let Some(message) = self
            .failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            panic!("{message}");
        }
    }

    fn ledger(&self) -> std::sync::MutexGuard<'_, Ledger> {
        self.ledger.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn settled(&self, status: StatusCode) {
        self.settles.fetch_add(1, Ordering::Relaxed);
        if status == StatusCode::CONFLICT {
            self.conflicts.fetch_add(1, Ordering::Relaxed);
        }
    }
}

struct Running(Child);

impl Drop for Running {
    fn drop(&mut self) {
        self.0.kill().ok();
        self.0.wait().ok();
    }
}

struct Rng(u64);

impl Rng {
    fn new(salt: u64) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos() as u64;
        Rng((nanos ^ salt.wrapping_mul(0x9E37_79B9_7F4A_7C15)) | 1)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n
    }
}

struct Slot {
    task: JoinHandle<()>,
    phase: Arc<AtomicU8>,
}

async fn reply(response: Response) -> Result<(StatusCode, Value), String> {
    let status = response.status();
    let bytes = response.bytes().await.map_err(|e| format!("body: {e}"))?;
    if bytes.is_empty() {
        return Ok((status, Value::Null));
    }
    let body = serde_json::from_slice(&bytes).map_err(|e| format!("{status} body: {e}"))?;
    Ok((status, body))
}

async fn send(request: reqwest::RequestBuilder) -> Result<(StatusCode, Value), String> {
    reply(request.send().await.map_err(|e| format!("request: {e}"))?).await
}

fn ids_of(body: &Value) -> Vec<String> {
    body["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|m| m["id"].as_str().map(str::to_owned))
        .collect()
}

async fn produce(ctx: Arc<Ctx>, duration: Duration) {
    if let Err(message) = publish_all(&ctx, duration).await {
        ctx.fail(message);
    }
}

async fn publish_all(ctx: &Ctx, duration: Duration) -> Result<(), String> {
    let end = Instant::now() + duration;
    let mut tick = tokio::time::interval(Duration::from_millis(1000 / BATCHES_PER_SECOND));
    let pad = "x".repeat(PAD);
    let mut seq = 0usize;
    while Instant::now() < end {
        tick.tick().await;
        let values: Vec<String> = (seq..seq + BATCH)
            .map(|n| format!(r#"{{"seq":{n},"pad":"{pad}"}}"#))
            .collect();
        let request = ctx
            .http
            .post(format!("{URL}/s/soak"))
            .header("Content-Type", "application/json")
            .body(format!("[{}]", values.join(",")));
        let (status, body) = send(request).await?;
        if status != StatusCode::CREATED {
            return Err(format!("publish gave {status} {body}"));
        }
        let ids: Vec<String> = body["ids"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|id| id.as_str().map(str::to_owned))
            .collect();
        if ids.len() != BATCH {
            return Err(format!("publish gave {} ids for {BATCH}", ids.len()));
        }
        let mut ledger = ctx.ledger();
        for id in ids {
            let rec = ledger.ids.entry(id.clone()).or_insert(Rec {
                published: false,
                delivered: false,
                acked: None,
            });
            if rec.published {
                return Err(format!("id {id} was published twice"));
            }
            rec.published = true;
        }
        seq += BATCH;
    }
    Ok(())
}

async fn consumer(ctx: Arc<Ctx>, phase: Arc<AtomicU8>, salt: u64) {
    if let Err(message) = consume(&ctx, &phase, salt).await {
        ctx.fail(message);
    }
}

fn record_delivery(ctx: &Ctx, body: &Value, sent: Instant) {
    let mut ledger = ctx.ledger();
    for message in body["messages"].as_array().into_iter().flatten() {
        let Some(id) = message["id"].as_str() else {
            continue;
        };
        let rec = ledger.ids.entry(id.to_owned()).or_insert(Rec {
            published: false,
            delivered: false,
            acked: None,
        });
        rec.delivered = true;
        let after_ack = rec.acked.is_some_and(|acked| acked <= sent);
        if after_ack {
            ledger.redelivered_after_ack += 1;
        }
        if let Some(seq) = message["value"]["seq"].as_u64() {
            let seq = seq as usize;
            if ledger.seqs.len() <= seq {
                ledger.seqs.resize(seq + 1, false);
            }
            ledger.seqs[seq] = true;
        }
    }
}

fn record_acks(ctx: &Ctx, ids: &[String]) {
    let now = Instant::now();
    let mut ledger = ctx.ledger();
    for id in ids {
        if let Some(rec) = ledger.ids.get_mut(id) {
            rec.acked.get_or_insert(now);
        }
    }
}

async fn consume(ctx: &Ctx, phase: &AtomicU8, salt: u64) -> Result<(), String> {
    let mut rng = Rng::new(salt);
    loop {
        phase.store(WAIT, Ordering::Relaxed);
        let sent = Instant::now();
        let request = ctx
            .http
            .get(format!("{URL}/s/soak?group=soak&limit=10&lease=5&wait=5"));
        let (status, body) = send(request).await?;
        match status {
            StatusCode::NO_CONTENT => {
                if ctx.producer_done.load(Ordering::SeqCst) {
                    ctx.empty_after_producer.fetch_add(1, Ordering::SeqCst);
                }
                continue;
            }
            StatusCode::OK => {}
            other => return Err(format!("take gave {other} {body}")),
        }
        record_delivery(ctx, &body, sent);
        phase.store(BETWEEN, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(rng.below(30))).await;
        let receipt = body["receipt"].as_str().ok_or("take gave no receipt")?;
        let ids = ids_of(&body);
        let nacked: Vec<&String> = ids.iter().filter(|_| rng.below(NACK_ONE_IN) == 0).collect();
        phase.store(SETTLE, Ordering::Relaxed);
        let acked: Vec<String> = ids
            .iter()
            .filter(|id| !nacked.contains(id))
            .cloned()
            .collect();
        let url = format!("{URL}/r/{receipt}");
        let mode = rng.below(3);
        let request = if !nacked.is_empty() || mode == 0 {
            let nacks: Vec<Value> = nacked.iter().map(|id| json!({"id": id})).collect();
            ctx.http
                .post(&url)
                .body(json!({"ack": acked, "nack": nacks}).to_string())
        } else {
            if mode == 2 {
                let (status, body) =
                    send(ctx.http.post(&url).body(json!({"extend": 5}).to_string())).await?;
                ctx.settled(status);
                if status != StatusCode::OK && status != StatusCode::CONFLICT {
                    return Err(format!("extend gave {status} {body}"));
                }
            }
            ctx.http.delete(&url)
        };
        let (status, body) = send(request).await?;
        ctx.settled(status);
        match status {
            StatusCode::OK => record_acks(ctx, &acked),
            StatusCode::CONFLICT => {}
            other => return Err(format!("settle gave {other} {body}")),
        }
    }
}

fn spawn_consumer(ctx: &Arc<Ctx>, rng: &mut Rng) -> Slot {
    let phase = Arc::new(AtomicU8::new(WAIT));
    let task = tokio::spawn(consumer(
        Arc::clone(ctx),
        Arc::clone(&phase),
        rng.below(u64::MAX),
    ));
    Slot { task, phase }
}

async fn kill_one(slots: &mut [Slot], ctx: &Arc<Ctx>, rng: &mut Rng, hits: &mut [u64; 3]) {
    let at = rng.below(slots.len() as u64) as usize;
    let target = rng.below(3) as u8;
    let until = Instant::now() + Duration::from_secs(3);
    while slots[at].phase.load(Ordering::Relaxed) != target && Instant::now() < until {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    hits[usize::from(slots[at].phase.load(Ordering::Relaxed))] += 1;
    slots[at].task.abort();
    if let Err(error) = (&mut slots[at].task).await
        && !error.is_cancelled()
    {
        ctx.fail(format!("consumer task: {error}"));
    }
    slots[at] = spawn_consumer(ctx, rng);
}

fn client() -> Client {
    Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .expect("http client")
}

fn rss(pid: u32) -> Result<u64, String> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status"))
        .map_err(|e| format!("read status of the child: {e}"))?;
    let kb = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|rest| {
            rest.trim()
                .trim_end_matches("kB")
                .trim()
                .parse::<u64>()
                .ok()
        })
        .ok_or("the child has no VmRSS")?;
    Ok(kb * 1024)
}

fn watch(ctx: &Ctx, pid: u32, next: &mut Instant, deadline: Instant) {
    ctx.check();
    assert!(
        Instant::now() < deadline,
        "ABORTED stall: the run has not ended after its duration, the drain limit and {} s",
        STALL_GRACE.as_secs()
    );
    if Instant::now() < *next {
        return;
    }
    *next = Instant::now() + RSS_EVERY;
    let bytes = rss(pid).unwrap_or_else(|e| panic!("{e}"));
    assert!(bytes <= RSS_MAX, "ABORTED rss bytes={bytes}");
}

struct Group {
    pending: u64,
    in_flight: u64,
    dead: u64,
}

async fn group_counts(ctx: &Ctx) -> Result<Group, String> {
    let (status, body) = send(ctx.http.get(format!("{URL}/g/soak"))).await?;
    if status != StatusCode::OK {
        return Err(format!("group status gave {status} {body}"));
    }
    let count = |name: &str| {
        body[name]
            .as_u64()
            .ok_or_else(|| format!("no {name} in {body}"))
    };
    Ok(Group {
        pending: count("pending")?,
        in_flight: count("in_flight")?,
        dead: count("dead")?,
    })
}

async fn dead_count(ctx: &Ctx, deadline: Instant) -> Result<u64, String> {
    let mut path = "/s/soak.dead.soak?limit=1000".to_owned();
    let mut count = 0;
    loop {
        if Instant::now() >= deadline {
            return Err(format!(
                "ABORTED stall: the dead read has not ended after {count} messages"
            ));
        }
        let (status, body) = send(ctx.http.get(format!("{URL}{path}"))).await?;
        match status {
            StatusCode::NOT_FOUND => return Ok(count),
            StatusCode::OK => {}
            other => return Err(format!("dead read gave {other} {body}")),
        }
        let page = body["messages"].as_array().map_or(0, Vec::len);
        if page == 0 {
            return Ok(count);
        }
        count += page as u64;
        path = body["next"]
            .as_str()
            .ok_or_else(|| format!("dead read has no next in {body}"))?
            .to_owned();
    }
}

async fn settled_dead(ctx: &Ctx, deadline: Instant) -> Result<(Group, u64), String> {
    let mut agreed = 0;
    loop {
        let group = group_counts(ctx).await?;
        let dead = dead_count(ctx, deadline).await?;
        if group.dead == dead {
            agreed += 1;
            if agreed == 2 {
                return Ok((group, dead));
            }
        } else {
            agreed = 0;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "ABORTED dead copies did not converge group_dead={} dead={dead}",
                group.dead
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn start(dir: &Path) -> Running {
    if TcpListener::bind("0.0.0.0:8080").await.is_err() {
        panic!("ABORTED port 8080 is busy");
    }
    let child = Command::new(env!("CARGO_BIN_EXE_verdandi"))
        .current_dir(dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn the binary");
    let mut running = Running(child);
    let http = client();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = running.0.try_wait().expect("try wait") {
            panic!("the binary exited with {status}");
        }
        let ready = http.get(format!("{URL}/readyz")).send().await;
        if ready.is_ok_and(|r| r.status() == StatusCode::OK) {
            return running;
        }
        assert!(Instant::now() < deadline, "the binary is not ready");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

struct Totals {
    published: u64,
    delivered: u64,
    unknown: u64,
    seq_missing: u64,
    redelivered_after_ack: u64,
    lost: u64,
    unacked: u64,
}

fn totals(ctx: &Ctx) -> Totals {
    let ledger = ctx.ledger();
    let published = ledger.ids.values().filter(|r| r.published).count() as u64;
    let delivered = ledger.ids.values().filter(|r| r.delivered).count() as u64;
    let lost = ledger
        .ids
        .values()
        .filter(|r| r.published && !r.delivered)
        .count() as u64;
    let unacked = ledger
        .ids
        .values()
        .filter(|r| r.published && r.acked.is_none())
        .count() as u64;
    let unknown = ledger.ids.values().filter(|r| !r.published).count() as u64;
    let seq_missing = (0..published as usize)
        .filter(|seq| !ledger.seqs.get(*seq).copied().unwrap_or(false))
        .count() as u64;
    Totals {
        published,
        delivered,
        unknown,
        seq_missing,
        redelivered_after_ack: ledger.redelivered_after_ack,
        lost,
        unacked,
    }
}

async fn drive(config: &Config, dir: &Path) {
    let running = start(dir).await;
    let pid = running.0.id();
    let ctx = Arc::new(Ctx {
        http: client(),
        ledger: Mutex::new(Ledger::default()),
        failure: Mutex::new(None),
        producer_done: AtomicBool::new(false),
        empty_after_producer: AtomicU64::new(0),
        settles: AtomicU64::new(0),
        conflicts: AtomicU64::new(0),
    });
    let mut rng = Rng::new(0);
    let started = Instant::now();
    let mut next_rss = started + RSS_EVERY;
    let deadline = started + config.duration + DRAIN_MAX + STALL_GRACE;
    let producer = tokio::spawn(produce(Arc::clone(&ctx), config.duration));
    let mut slots: Vec<Slot> = (0..CONSUMERS)
        .map(|_| spawn_consumer(&ctx, &mut rng))
        .collect();
    let mut next_kill = config.kill_every;
    let last_kill = config.duration.saturating_sub(config.kill_every);
    let mut kills = 0u64;
    let mut hits = [0u64; 3];
    while !producer.is_finished() {
        tokio::time::sleep(Duration::from_millis(10)).await;
        watch(&ctx, pid, &mut next_rss, deadline);
        if next_kill <= last_kill && started.elapsed() >= next_kill {
            kill_one(&mut slots, &ctx, &mut rng, &mut hits).await;
            kills += 1;
            next_kill += config.kill_every;
        }
    }
    producer.await.expect("producer task");
    ctx.check();
    ctx.producer_done.store(true, Ordering::SeqCst);
    let drain_end = Instant::now() + DRAIN_MAX;
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        watch(&ctx, pid, &mut next_rss, deadline);
        if ctx.empty_after_producer.load(Ordering::SeqCst) > 0 {
            let counts = group_counts(&ctx).await.unwrap_or_else(|e| panic!("{e}"));
            if (counts.pending == 0 && counts.in_flight == 0) || Instant::now() > drain_end {
                break;
            }
            ctx.empty_after_producer.store(0, Ordering::SeqCst);
        } else if Instant::now() > drain_end {
            break;
        }
    }
    for slot in &slots {
        slot.task.abort();
    }
    ctx.check();
    let (group, dead) = settled_dead(&ctx, deadline)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let settles = ctx.settles.load(Ordering::Relaxed);
    let conflicts = ctx.conflicts.load(Ordering::Relaxed);
    let Group {
        pending,
        in_flight,
        dead: group_dead,
    } = group;
    let duration_s = started.elapsed().as_secs();
    let t = totals(&ctx);
    let unacked = t.unacked.saturating_sub(dead);
    let expected_kills =
        (config.duration.as_secs() / config.kill_every.as_secs()).saturating_sub(1);
    eprintln!(
        "kill phases wait={} between={} settle={}",
        hits[usize::from(WAIT)],
        hits[usize::from(BETWEEN)],
        hits[usize::from(SETTLE)]
    );
    let passed = t.lost == 0
        && t.unknown == 0
        && t.seq_missing == 0
        && t.delivered == t.published
        && pending == 0
        && in_flight == 0
        && dead == 0
        && group_dead == dead
        && conflicts * 100 <= settles
        && t.redelivered_after_ack == 0
        && unacked <= 10 * (hits[usize::from(SETTLE)] + CONSUMERS as u64)
        && kills == expected_kills
        && (!config.require_phases || hits.iter().all(|&hit| hit > 0))
        && duration_s >= config.duration.as_secs();
    if !passed {
        panic!(
            "M1 SOAK FAIL published={} delivered={} dead={dead} group_dead={group_dead} pending={pending} in_flight={in_flight} lost={} unknown={} seq_missing={} redelivered_after_ack={} unacked={unacked} settles={settles} conflicts={conflicts} kills={kills} expected_kills={expected_kills} phases={hits:?} duration_s={duration_s}",
            t.published, t.delivered, t.lost, t.unknown, t.seq_missing, t.redelivered_after_ack
        );
    }
    println!(
        "M1 SOAK PASS published={} delivered={} dead=0 pending=0 in_flight=0 lost=0 redelivered_after_ack=0 unacked={unacked} settles={settles} conflicts={conflicts} kills={kills} duration_s={duration_s}",
        t.published, t.delivered
    );
}

pub fn run(config: &Config) {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/tmp/m1-soak");
    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("clear the soak dir");
    }
    std::fs::create_dir_all(&dir).expect("create the soak dir");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(drive(config, &dir));
}
