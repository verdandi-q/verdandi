use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::hash::{BuildHasher, Hasher};
use std::io::{Error, ErrorKind, Result, Write};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::backup::{self, Backup, ClusterRecord, Source};
use protobuf::Message;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc::{Receiver, UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval};
use tracing::{debug, error, warn};
use verdandi_control::{Control, Meta, Saved};
use verdandi_core::Record;
use verdandi_core::cluster::controller::{
    self, Controller, Floor, KEYED_MIN_COPIES, SOAK_MS, START,
};
use verdandi_core::cluster::placement::HolderSet;
use verdandi_core::cluster::repair::DIGEST_MS;
use verdandi_core::cluster::view::{
    Body, GOSSIP_MS, LIFECYCLE, REBALANCE, SAMPLE_MS, SIGTERM, SPOT, View,
};
use verdandi_core::ids::SegmentId;
use verdandi_http::{Cluster, Health, NodeInfo, Op, Peers, Reply};
use verdandi_net::{
    ClusterId, Hello, Net, Reply as Welcome, Wire, open_members, seal, seal_members,
};
use verdandi_node::{
    Cmd, Effect, Inbound, Input, Job, Knobs, LEASE_MS, Msg, Node, Output, STEP_MS, Sense, Store,
    expired, node_sample,
};
use verdandi_store::block::{align, decode_bytes};
use verdandi_store::engine::Handle;
use verdandi_store::records::{self, Control as Stored};

pub const METADATA_URL: &str = "http://169.254.169.254";
const METADATA_POLL: Duration = Duration::from_secs(5);
const METADATA_TIMEOUT: Duration = Duration::from_millis(100);
const FANOUT: usize = 3;
const CONFIRM: Duration = Duration::from_secs(5);
const HELLO_MS: u64 = 10_000;
const HELLO_TIMEOUT: Duration = Duration::from_secs(5);
const KNOWN_FOR: Duration = Duration::from_secs(25);
const FOUND_ROUNDS: u32 = 2;
const FIND_MS: u64 = 1_000;
const QUIET: Duration = Duration::from_secs(3);
const SPLIT_LOG: Duration = Duration::from_secs(60);
const COPY_WAIT: Duration = Duration::from_secs(3);
const COPY_TRIES: u32 = 5;
const COPY_EXTRA: Duration = Duration::from_secs(30);

pub fn own_id(dir: &Path) -> Result<u64> {
    let me = once(&dir.join("node-id"), random_id)?;
    if me == 0 {
        return Err(Error::new(ErrorKind::InvalidData, "node id is zero"));
    }
    Ok(me)
}

pub struct Discovery {
    pub url: String,
    pub ip: IpAddr,
}

struct Disco {
    url: String,
    key: Vec<u8>,
    restart: u64,
    reached: Arc<AtomicU32>,
    may_found: bool,
}

#[derive(Default)]
struct Seen {
    leader: Option<u64>,
    members: BTreeSet<u64>,
    applied: u64,
    known: BTreeMap<u64, (String, bool, Instant)>,
    fresh_at: Option<Instant>,
    addrs: BTreeMap<u64, String>,
    leaders: BTreeSet<u64>,
    stop: bool,
    left: bool,
    held: bool,
    committed: u16,
    soak_left_ms: Option<u64>,
    live: usize,
    floor: u8,
    count: u8,
    target: u8,
    holders: BTreeMap<u64, Vec<u64>>,
    fetch: BTreeSet<u64>,
    cluster: Option<ClusterId>,
    heard: Vec<(u64, ClusterId)>,
    split: bool,
    heavy: Option<u64>,
    until_full_s: Option<u64>,
}

impl Seen {
    fn learn(&mut self, id: u64, at: String, member: bool) {
        if self
            .known
            .get(&id)
            .is_none_or(|k| k.2.elapsed() >= KNOWN_FOR)
        {
            self.fresh_at = Some(Instant::now());
        }
        self.known.insert(id, (at, member, Instant::now()));
    }
}

const FORMAT_MAX: u16 = verdandi_store::block::VERSION;

pub struct Runner {
    me: u64,
    addr: String,
    dir: PathBuf,
    seen: Arc<Mutex<Seen>>,
    net: Net,
    task: JoinHandle<()>,
    ended: Arc<Notify>,
    ops: UnboundedSender<Platform>,
    uploader: Mutex<Option<JoinHandle<()>>>,
}

struct PanicSignal(Arc<Notify>);

impl Drop for PanicSignal {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.notify_one();
        }
    }
}

#[derive(Serialize, Deserialize)]
struct SavedFile {
    hard_state: Vec<u8>,
    snapshot: Vec<u8>,
    entries: Vec<Vec<u8>>,
    conf: Vec<u8>,
    applied: u64,
    meta: Vec<u8>,
}

impl SavedFile {
    fn new(saved: &Saved) -> Result<SavedFile> {
        let bytes = |m: &dyn Message| m.write_to_bytes().map_err(Error::other);
        Ok(SavedFile {
            hard_state: bytes(&saved.hard_state)?,
            snapshot: bytes(&saved.snapshot)?,
            entries: saved
                .entries
                .iter()
                .map(|e| bytes(e))
                .collect::<Result<_>>()?,
            conf: bytes(&saved.conf)?,
            applied: saved.applied,
            meta: saved.meta.encode(),
        })
    }

    fn saved(&self) -> Result<Saved> {
        let bad = |what: &str| Error::new(ErrorKind::InvalidData, format!("bad saved {what}"));
        Ok(Saved {
            hard_state: <HardState as Message>::parse_from_bytes(&self.hard_state)
                .map_err(|_| bad("hard state"))?,
            snapshot: <Snapshot as Message>::parse_from_bytes(&self.snapshot)
                .map_err(|_| bad("snapshot"))?,
            entries: self
                .entries
                .iter()
                .map(|e| <Entry as Message>::parse_from_bytes(e).map_err(|_| bad("entry")))
                .collect::<Result<_>>()?,
            conf: <ConfState as Message>::parse_from_bytes(&self.conf)
                .map_err(|_| bad("conf state"))?,
            applied: self.applied,
            meta: Meta::decode(&self.meta).ok_or_else(|| bad("meta"))?,
        })
    }
}

fn too_new(committed: u16, format_max: u16) -> Error {
    Error::new(
        ErrorKind::InvalidData,
        format!("the cluster format is {committed}; this build reads up to {format_max}"),
    )
}

fn put<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = postcard::to_allocvec(value).map_err(Error::other)?;
    let tmp = path.with_extension("tmp");
    let mut file = File::create(&tmp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    std::fs::rename(&tmp, path)?;
    let parent = path.parent().unwrap_or(Path::new("."));
    File::open(parent)?.sync_all()
}

fn get<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let (value, rest) = postcard::take_from_bytes(&bytes).map_err(|e| {
        Error::new(
            ErrorKind::InvalidData,
            format!("corrupt state file {}: {e}", path.display()),
        )
    })?;
    if !rest.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!("corrupt state file {}", path.display()),
        ));
    }
    Ok(Some(value))
}

struct Seg {
    holders: Vec<u64>,
    backlog: Vec<(u64, Vec<u8>)>,
}

struct Pend {
    sid: SegmentId,
    copies: BTreeSet<u64>,
    failed: BTreeSet<u64>,
    need: BTreeSet<u64>,
    ctl: BTreeSet<u64>,
    ctl_ok: BTreeSet<u64>,
    deadline: Instant,
}

impl Pend {
    fn confirmed(&self, want: usize) -> bool {
        self.copies.len() >= want
            && self
                .need
                .iter()
                .all(|n| self.copies.contains(n) || self.ctl_ok.contains(n))
    }
}

struct Transfer {
    sid: SegmentId,
    next: u64,
    after: u64,
    sent: Option<(u64, Instant)>,
    fails: u32,
    epoch: Option<u64>,
}

struct Data {
    handle: Handle,
    copies: BTreeMap<(u64, u64), Transfer>,
    extra: BTreeMap<(u64, u64), Instant>,
    hook: UnboundedReceiver<(SegmentId, u64, Vec<u8>)>,
    segs: BTreeMap<SegmentId, Seg>,
    ids: BTreeMap<u64, SegmentId>,
    pend: BTreeMap<(u64, u64), Pend>,
    led: Option<BTreeSet<SegmentId>>,
    led_until: u64,
    gaps: BTreeSet<u64>,
}

pub fn node_id(segment: SegmentId) -> u64 {
    let mut hash = Sha256::new();
    hash.update(segment.incarnation.to_be_bytes());
    hash.update(segment.seq.to_be_bytes());
    let digest = hash.finalize();
    let mut low = [0u8; 8];
    low.copy_from_slice(&digest[24..]);
    u64::from_be_bytes(low)
}

async fn next_commit(data: &mut Option<Data>) -> Option<(SegmentId, u64, Vec<u8>)> {
    match data {
        Some(data) => data.hook.recv().await,
        None => std::future::pending().await,
    }
}

enum Platform {
    Facts {
        zone: String,
        instance_type: String,
        spot: bool,
    },
    Signal(u8),
    Fetched(SegmentId),
    Ask(Op, oneshot::Sender<Reply>),
}

async fn imds(client: &reqwest::Client, base: &str, token: &str, path: &str) -> Option<String> {
    let reply = client
        .get(format!("{base}/latest/meta-data/{path}"))
        .header("X-aws-ec2-metadata-token", token)
        .send()
        .await
        .ok()?;
    if !reply.status().is_success() {
        return None;
    }
    reply.text().await.ok()
}

async fn poll_metadata(base: String, tx: UnboundedSender<Platform>) {
    let Ok(client) = reqwest::Client::builder().timeout(METADATA_TIMEOUT).build() else {
        return;
    };
    let base = base.trim_end_matches('/').to_owned();
    let (mut known, mut sent) = (false, 0u8);
    let mut timer = interval(METADATA_POLL);
    loop {
        tokio::select! {
            _ = tx.closed() => return,
            _ = timer.tick() => {}
        }
        let token = client
            .put(format!("{base}/latest/api/token"))
            .header("X-aws-ec2-metadata-token-ttl-seconds", "60")
            .send()
            .await;
        let token = match token {
            Ok(reply) if reply.status().is_success() => reply.text().await.ok(),
            _ => None,
        };
        let Some(token) = token else {
            debug!("no platform metadata service");
            continue;
        };
        if !known {
            let zone = imds(&client, &base, &token, "placement/availability-zone").await;
            let kind = imds(&client, &base, &token, "instance-type").await;
            let life = imds(&client, &base, &token, "instance-life-cycle").await;
            if let Some(zone) = zone {
                known = true;
                let facts = Platform::Facts {
                    zone,
                    instance_type: kind.unwrap_or_default(),
                    spot: life.is_some_and(|l| l.trim() == "spot"),
                };
                if tx.send(facts).is_err() {
                    return;
                }
            }
        }
        for (bit, path) in [
            (SPOT, "spot/instance-action"),
            (REBALANCE, "events/recommendations/rebalance"),
            (LIFECYCLE, "autoscaling/target-lifecycle-state"),
        ] {
            if sent & bit != 0 {
                continue;
            }
            let found = imds(&client, &base, &token, path)
                .await
                .is_some_and(|v| bit != LIFECYCLE || v.trim() != "InService");
            if found {
                sent |= bit;
                if tx.send(Platform::Signal(bit)).is_err() {
                    return;
                }
            }
        }
    }
}

struct Driver {
    platform: UnboundedReceiver<Platform>,
    me: u64,
    data: Option<Data>,
    node: Node,
    net: Net,
    disco: Option<Disco>,
    stopping: bool,
    rx: Receiver<(u64, Wire, Msg)>,
    peers: Vec<u64>,
    seen: Arc<Mutex<Seen>>,
    origin_ms: u64,
    alive_base: u64,
    started: Instant,
    tick: u64,
    cdir: PathBuf,
    floor: Floor,
    dropped: BTreeMap<u64, u64>,
    epochs: BTreeMap<u64, u64>,
    wishes: BTreeMap<(u64, u64, u64), HolderSet>,
    lacking: BTreeMap<u64, u64>,
    boot_held: BTreeSet<u64>,
    warned: Option<Instant>,
    foreign_warned: Option<Instant>,
}

fn caught_up(since: &mut BTreeMap<u64, u64>, lacking: BTreeSet<u64>, run: u64) -> bool {
    since.retain(|id, _| lacking.contains(id));
    for id in lacking {
        since.entry(id).or_insert(run);
    }
    since.values().all(|at| run < at + DIGEST_MS)
}

fn random_id() -> u64 {
    std::hash::RandomState::new().build_hasher().finish().max(1)
}

fn wall_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(0)
}

fn once<T: Serialize + DeserializeOwned>(path: &Path, make: impl FnOnce() -> T) -> Result<T> {
    if let Some(value) = get(path)? {
        return Ok(value);
    }
    let value = make();
    put(path, &value)?;
    Ok(value)
}

impl Driver {
    // allow: the builder takes the full node setup
    #[allow(clippy::too_many_arguments)]
    async fn build(
        key: &[u8],
        listen: &str,
        peers: Vec<(u64, String)>,
        found: bool,
        dir: PathBuf,
        discovery: Option<Discovery>,
        engine: Option<Handle>,
        format_max: u16,
    ) -> Result<Driver> {
        let cdir = dir.join("cluster");
        std::fs::create_dir_all(&cdir)?;
        let me = own_id(&dir)?;
        let restart = get::<u64>(&dir.join("restart"))?.unwrap_or(0) + 1;
        put(&dir.join("restart"), &restart)?;
        let part = |name: &str| cdir.join(name);
        let (raft, saved) = match get::<SavedFile>(&part("control"))? {
            Some(file) => {
                let saved = file.saved()?;
                (Control::restore(me, saved.clone()), saved)
            }
            None => {
                let raft = if found {
                    Control::found(me)
                } else {
                    Control::restore(me, Saved::default())
                };
                let raft = raft.map_err(Error::other)?;
                let saved = raft.save().map_err(Error::other)?;
                put(&part("control"), &SavedFile::new(&saved)?)?;
                if found {
                    put(&part("cluster"), &(me, wall_ms()))?;
                }
                (Ok(raft), saved)
            }
        };
        let raft = raft.map_err(Error::other)?;
        let cluster = get::<ClusterId>(&part("cluster"))?;
        let applied = saved.applied;
        let members = saved.meta.members.clone();
        let floor = get::<(u64, u8)>(&part("floor"))?
            .map_or(START, |(epoch, value)| Floor { epoch, value });
        let committed = get::<u16>(&part("committed"))?.unwrap_or(1);
        if committed > format_max {
            return Err(too_new(committed, format_max));
        }
        let alive_base = get::<u64>(&part("alive"))?.unwrap_or(0);
        let join_ms = once(&part("join"), || alive_base)?;
        let member = get::<Vec<u8>>(&part("member"))?;
        let store = get::<Store>(&part("store"))?.unwrap_or_default();
        let dropped = get::<BTreeMap<u64, u64>>(&part("dropped"))?.unwrap_or_default();
        let epochs = get::<BTreeMap<u64, u64>>(&part("epochs"))?.unwrap_or_default();
        let (net, rx) = Net::start(key, listen, me).await?;
        net.set_cluster(cluster);
        for (id, addr) in &peers {
            net.connect(*id, addr);
        }
        let addr = match &discovery {
            Some(d) => {
                let port = net
                    .addr()
                    .parse::<SocketAddr>()
                    .map_err(Error::other)?
                    .port();
                SocketAddr::new(d.ip, port).to_string()
            }
            None => net.addr().to_owned(),
        };
        let origin_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_millis()),
        )
        .unwrap_or(0);
        let body = Body {
            address: addr,
            format_max,
            committed,
            join_ms,
            alive_ms: alive_base,
            host: me,
            ..Body::default()
        };
        let (view, count) = match member {
            None => (View::new(me, restart, body, origin_ms, alive_base), 0),
            Some(bytes) => View::restored(me, restart, body, origin_ms, alive_base, &bytes)
                .ok_or_else(|| Error::new(ErrorKind::InvalidData, "bad saved member state"))?,
        };
        let mut ctl = Controller::new(true, floor, committed);
        ctl.count = count;
        let phase_ms = (me % (SAMPLE_MS / STEP_MS)) * STEP_MS;
        let mut node = Node::new(
            me,
            origin_ms,
            alive_base,
            format_max,
            phase_ms,
            view,
            ctl,
            raft,
            Arc::new(saved),
            store,
        );
        node.headroom = 1.0;
        node.epochs = epochs.clone();
        node.dropped = dropped.clone();
        let held: Vec<u64> = node
            .raft
            .meta()
            .segments
            .iter()
            .filter(|(_, s)| s.holders.holders.contains(&me))
            .map(|(id, _)| *id)
            .collect();
        node.booting = held.iter().copied().collect();
        let on_disk = match &engine {
            Some(handle) => handle.segments().await.map_err(Error::other)?,
            None => Vec::new(),
        };
        for &id in &held {
            if on_disk.iter().any(|sid| node_id(*sid) == id) {
                node.marks.insert(id, 0);
            }
            node.catch_up(origin_ms, id);
        }
        let may_found = discovery.is_some() && members.is_empty() && cluster.is_none();
        let disco = discovery.map(|d| Disco {
            url: d.url,
            key: key.to_vec(),
            restart,
            reached: Arc::new(AtomicU32::new(0)),
            may_found,
        });
        let seen = Arc::new(Mutex::new(Seen {
            members,
            applied,
            committed,
            live: 1,
            floor: 1,
            count: 1,
            target: 1,
            cluster,
            ..Seen::default()
        }));
        let data = match engine {
            Some(handle) => {
                let hook = handle.subscribe_held().await.map_err(Error::other)?;
                let led_until = handle.now_ms() + LEASE_MS;
                handle
                    .lead(BTreeSet::new(), led_until)
                    .await
                    .map_err(Error::other)?;
                let ids = on_disk
                    .iter()
                    .filter(|sid| held.contains(&node_id(**sid)))
                    .map(|sid| (node_id(*sid), *sid))
                    .collect();
                Some(Data {
                    handle,
                    copies: BTreeMap::new(),
                    extra: BTreeMap::new(),
                    hook,
                    segs: BTreeMap::new(),
                    ids,
                    pend: BTreeMap::new(),
                    led: Some(BTreeSet::new()),
                    led_until,
                    gaps: BTreeSet::new(),
                })
            }
            None => None,
        };
        Ok(Driver {
            platform: unbounded_channel().1,
            me,
            data,
            node,
            net,
            disco,
            stopping: false,
            rx,
            peers: peers.into_iter().map(|(id, _)| id).collect(),
            seen: seen.clone(),
            origin_ms,
            alive_base,
            started: Instant::now(),
            tick: 0,
            cdir,
            floor,
            dropped,
            epochs,
            wishes: BTreeMap::new(),
            lacking: BTreeMap::new(),
            boot_held: held.iter().copied().collect(),
            warned: None,
            foreign_warned: None,
        })
    }
}

impl Runner {
    pub async fn start(
        key: &[u8],
        listen: &str,
        peers: Vec<(u64, String)>,
        found: bool,
        dir: PathBuf,
    ) -> Result<Runner> {
        Runner::launch(key, listen, peers, found, dir, None, None).await
    }

    pub async fn launch(
        key: &[u8],
        listen: &str,
        peers: Vec<(u64, String)>,
        found: bool,
        dir: PathBuf,
        discovery: Option<Discovery>,
        engine: Option<Handle>,
    ) -> Result<Runner> {
        Runner::launch_with(
            key,
            listen,
            peers,
            found,
            dir,
            discovery,
            engine,
            METADATA_URL,
            FORMAT_MAX,
        )
        .await
    }

    // allow: the launcher takes the full node setup and the metadata URL
    #[allow(clippy::too_many_arguments)]
    pub async fn launch_with(
        key: &[u8],
        listen: &str,
        peers: Vec<(u64, String)>,
        found: bool,
        dir: PathBuf,
        discovery: Option<Discovery>,
        engine: Option<Handle>,
        metadata: &str,
        format_max: u16,
    ) -> Result<Runner> {
        let mut driver = Driver::build(
            key,
            listen,
            peers,
            found,
            dir.clone(),
            discovery,
            engine,
            format_max,
        )
        .await?;
        let (tx, rx) = unbounded_channel();
        driver.platform = rx;
        let ops = tx.clone();
        tokio::spawn(poll_metadata(metadata.to_owned(), tx));
        let (me, seen, net) = (driver.me, driver.seen.clone(), driver.net.clone());
        let addr = driver.node.view.own().body.address.clone();
        let ended = Arc::new(Notify::new());
        let signal = ended.clone();
        let task = tokio::spawn(async move {
            let _panic = PanicSignal(signal.clone());
            if let Err(e) = driver.run().await {
                error!("the node stops: {e}");
                signal.notify_one();
            }
        });
        Ok(Runner {
            me,
            addr,
            dir,
            seen,
            net,
            task,
            ended,
            ops,
            uploader: Mutex::new(None),
        })
    }

    pub fn backup(&self, backup: Backup, handle: Handle, dir: PathBuf) {
        let me = self.me;
        let (holding, leading, wanting) = (self.seen.clone(), self.seen.clone(), self.seen.clone());
        let ops = self.ops.clone();
        let mine = move |segment| {
            let seen = holding.lock().unwrap();
            seen.holders
                .get(&node_id(segment))
                .is_some_and(|h| backup::pick(segment, h) == Some(me))
        };
        let record = move || {
            let seen = leading.lock().unwrap();
            (seen.leader == Some(me)).then(|| ClusterRecord {
                members: seen.members.iter().copied().collect(),
                format: seen.committed,
            })
        };
        let source = Source {
            handle,
            dir,
            me,
            mine: Box::new(mine),
            record: Box::new(record),
            wanted: Box::new(move || wanting.lock().unwrap().fetch.clone()),
            fetched: Box::new(move |sid| {
                let _ = ops.send(Platform::Fetched(sid));
            }),
        };
        let task = tokio::spawn(backup::run(backup, source));
        if let Some(old) = self.uploader.lock().unwrap().replace(task) {
            old.abort();
        }
    }

    pub fn cluster(&self, key: &[u8]) -> Cluster {
        let (seen, me, addr, sealing) =
            (self.seen.clone(), self.me, self.addr.clone(), key.to_vec());
        let hello = move |hello: Hello| {
            let mut s = seen.lock().unwrap();
            if let Some(theirs) = hello.cluster
                && s.heard.len() < verdandi_net::MEMBERS_MAX
            {
                s.heard.push((hello.node, theirs));
            }
            if hello.node != me && hello.node != 0 {
                let flag = s.known.get(&hello.node).is_some_and(|k| k.1);
                s.learn(hello.node, hello.addr, flag);
            }
            let mut list = vec![(me, addr.clone(), !s.members.is_empty())];
            for (id, (at, flag, seen_at)) in &s.known {
                if seen_at.elapsed() < KNOWN_FOR && list.len() < verdandi_net::MEMBERS_MAX {
                    list.push((*id, at.clone(), *flag || s.members.contains(id)));
                }
            }
            let reply = Welcome {
                cluster: s.cluster,
                members: list,
            };
            seal_members(&sealing, &reply).unwrap_or_default()
        };
        let seen = self.seen.clone();
        let me = self.me;
        let status = move || {
            let s = seen.lock().unwrap();
            let leader = s.leader.map_or("null".to_owned(), |l| l.to_string());
            let members: Vec<String> = s.members.iter().map(u64::to_string).collect();
            let soak = s.soak_left_ms.map_or("null".to_owned(), |t| t.to_string());
            format!(
                "{{\"id\":{me},\"leader\":{leader},\"members\":[{}],\"format\":{},\"soak_left_ms\":{soak}}}",
                members.join(","),
                s.committed
            )
        };
        let seen = self.seen.clone();
        let me = self.me;
        let peers = move || {
            let s = seen.lock().unwrap();
            Peers {
                me,
                addrs: s.addrs.clone(),
                leaders: s.leaders.clone(),
            }
        };
        let (held, stopping) = (self.seen.clone(), self.seen.clone());
        let health = Health {
            held: Box::new(move || {
                let s = held.lock().unwrap();
                s.held && !s.split
            }),
            stopping: Box::new(move || stopping.lock().unwrap().stop),
        };
        let scaled = self.seen.clone();
        let scale = move || {
            let s = scaled.lock().unwrap();
            (s.live, s.floor)
        };
        let counted = self.seen.clone();
        let copies = move || {
            let s = counted.lock().unwrap();
            (s.count, s.target, s.floor)
        };
        let pressed = self.seen.clone();
        let pressure = move || {
            let s = pressed.lock().unwrap();
            (s.heavy, s.until_full_s)
        };
        let ops = self.ops.clone();
        let ask = move |op| {
            let (tx, rx) = oneshot::channel();
            let _ = ops.send(Platform::Ask(op, tx));
            rx
        };
        Cluster::new(key.to_vec(), Box::new(hello), Box::new(status))
            .with_peers(Box::new(peers))
            .with_health(health)
            .with_ask(Box::new(ask))
            .with_scale(Box::new(scale))
            .with_copies(Box::new(copies))
            .with_pressure(Box::new(pressure))
    }

    pub async fn failed(&self) {
        self.ended.notified().await;
    }

    pub async fn shutdown(&self) {
        self.seen.lock().unwrap().stop = true;
        while !self.seen.lock().unwrap().left {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    pub async fn stop(mut self) {
        self.task.abort();
        if let Some(uploader) = self.uploader.lock().unwrap().take() {
            uploader.abort();
        }
        let _ = (&mut self.task).await;
        self.net.stop();
    }

    pub fn id(&self) -> u64 {
        self.me
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn dir(&self) -> &PathBuf {
        &self.dir
    }

    pub fn leader(&self) -> Option<u64> {
        self.seen.lock().unwrap().leader
    }

    pub fn members(&self) -> BTreeSet<u64> {
        self.seen.lock().unwrap().members.clone()
    }

    pub fn leaders(&self) -> BTreeSet<u64> {
        self.seen.lock().unwrap().leaders.clone()
    }

    pub fn applied(&self) -> u64 {
        self.seen.lock().unwrap().applied
    }
}

impl Drop for Runner {
    fn drop(&mut self) {
        self.task.abort();
        if let Some(uploader) = self.uploader.lock().unwrap().take() {
            uploader.abort();
        }
    }
}

impl Driver {
    async fn run(mut self) -> Result<()> {
        let mut timer = interval(Duration::from_millis(STEP_MS));
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = timer.tick() => self.on_tick().await?,
                Some(block) = next_commit(&mut self.data) => self.on_commit(block).await?,
                Some(found) = self.platform.recv() => self.on_platform(found)?,
                got = self.rx.recv() => {
                    let Some(first) = got else { return Ok(()) };
                    let mut batch = vec![first];
                    while let Ok(more) = self.rx.try_recv() {
                        batch.push(more);
                    }
                    self.on_frames(batch).await?;
                }
            }
        }
    }

    fn on_platform(&mut self, found: Platform) -> Result<()> {
        match found {
            Platform::Signal(bit) => self.step(vec![Cmd::Signal(bit)], Vec::new()),
            Platform::Fetched(sid) => {
                let id = node_id(sid);
                let writer = self
                    .node
                    .raft
                    .meta()
                    .segments
                    .get(&id)
                    .map(|s| s.holders.writer);
                if let Some(data) = &mut self.data {
                    data.ids.insert(id, sid);
                    data.gaps.remove(&id);
                }
                let (born, _) = self.clock();
                let adopt = Cmd::Adopt {
                    id,
                    mark: 0,
                    born,
                    writer,
                    unnamed: None,
                    named: Vec::new(),
                };
                self.step(vec![adopt], Vec::new())
            }
            Platform::Ask(op, reply) => {
                let answer = self.ask(op)?;
                let _ = reply.send(answer);
                Ok(())
            }
            Platform::Facts {
                zone,
                instance_type,
                spot,
            } => {
                let (now, run) = self.clock();
                let mut body = self.node.view.latest().clone();
                if spot {
                    body.spot_pool = format!("{zone}/{instance_type}");
                }
                body.zone = zone;
                body.instance_type = instance_type;
                self.node.view.set_body(body, now, run);
                Ok(())
            }
        }
    }

    fn ask(&mut self, op: Op) -> Result<Reply> {
        Ok(match op {
            Op::Info(disk) => Reply::Info(self.info(disk)),
            Op::Drain => {
                self.step(vec![Cmd::Drain], Vec::new())?;
                Reply::Done
            }
            Op::Accept(disk) => {
                let effects = self.step_fx(vec![Cmd::AcceptLoss { disk }], Vec::new())?;
                let ok = effects.iter().any(|e| matches!(e, Effect::Accepted(true)));
                let members = &self.node.raft.meta().members;
                let live = members
                    .iter()
                    .filter(|m| **m == self.me || self.node.view.live().any(|(d, _)| d == **m))
                    .count();
                Reply::Accept {
                    ok,
                    quorum: live * 2 > members.len(),
                }
            }
        })
    }

    fn info(&self, disk: u64) -> Option<NodeInfo> {
        let meta = self.node.raft.meta();
        let member = meta.members.contains(&disk);
        let body = self.node.view.entry(disk).map(|e| &e.body);
        if body.is_none() && !member {
            return None;
        }
        let signals = if disk == self.me {
            self.node.signals
        } else {
            body.map_or(0, |b| b.signals)
        };
        Some(NodeInfo {
            id: disk,
            address: body.map_or_else(String::new, |b| b.address.clone()),
            member,
            live: disk == self.me || self.node.view.live().any(|(d, _)| d == disk),
            draining: body.is_some_and(|b| b.drained),
            stopping: signals & (SIGTERM | LIFECYCLE) != 0,
            segments: meta
                .segments
                .values()
                .filter(|s| s.holders.holders.contains(&disk))
                .count(),
        })
    }

    fn clock(&self) -> (u64, u64) {
        let run = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        (self.origin_ms + run, self.alive_base + run)
    }

    fn step(&mut self, cmds: Vec<Cmd>, frames: Vec<Inbound>) -> Result<()> {
        self.step_fx(cmds, frames).map(drop)
    }

    fn step_fx(&mut self, cmds: Vec<Cmd>, frames: Vec<Inbound>) -> Result<Vec<Effect>> {
        let (now, run) = self.clock();
        let input = Input {
            now,
            run,
            cmds,
            frames,
            partial: false,
            pairs: BTreeSet::new(),
        };
        let mut frames = Vec::new();
        let out = self.node.step(input, &mut |to, msg, version| {
            frames.push((to, msg, version));
            true
        });
        let version = self.node.raft.meta().version;
        let Output {
            effects,
            frames: direct,
            ..
        } = out;
        let (answers, effects): (Vec<Effect>, Vec<Effect>) = effects.into_iter().partition(|e| {
            matches!(
                e,
                Effect::Picked(_)
                    | Effect::Stored { .. }
                    | Effect::RefusedWrite { .. }
                    | Effect::Copy { .. }
                    | Effect::Accepted(_)
            )
        });
        frames.extend(direct.into_iter().map(|(to, msg)| (to, msg, version)));
        let net = self.net.clone();
        self.deliver(effects, frames, now, &mut |to, msg, wire| {
            net.send(to, msg, wire)
        })?;
        let mut seen = self.seen.lock().unwrap();
        seen.leader = self.node.raft.leader();
        let meta = self.node.raft.meta();
        seen.members = meta.members.clone();
        seen.leaders = meta.segments.values().map(|s| s.leader).collect();
        let live: BTreeSet<u64> = self.node.view.live().map(|(d, _)| d).collect();
        let me = self.me;
        seen.holders = meta
            .segments
            .iter()
            .filter(|(_, s)| s.holders.holders.contains(&me))
            .map(|(id, s)| {
                let up = |h: &u64| *h == me || live.contains(h);
                let isr: Vec<u64> = s.isr.iter().copied().filter(up).collect();
                let all = s.holders.holders.iter().copied().filter(up);
                (*id, if isr.is_empty() { all.collect() } else { isr })
            })
            .collect();
        seen.fetch = meta
            .segments
            .iter()
            .filter(|(id, s)| {
                s.holders.holders.contains(&me)
                    && !self.node.marks.contains_key(*id)
                    && self.node.holders_of(**id).is_empty()
            })
            .map(|(id, _)| *id)
            .collect();
        let lacking: BTreeSet<u64> = meta
            .segments
            .iter()
            .filter(|(id, _)| {
                self.boot_held.contains(*id)
                    && !self.node.marks.contains_key(*id)
                    && !self.node.holders_of(**id).is_empty()
            })
            .map(|(id, _)| *id)
            .collect();
        seen.held = caught_up(&mut self.lacking, lacking, run);
        seen.addrs = self
            .node
            .view
            .live()
            .filter(|(id, _)| meta.members.contains(id))
            .map(|(id, e)| (id, e.body.address.clone()))
            .collect();
        Ok(answers)
    }

    fn deliver(
        &mut self,
        effects: Vec<Effect>,
        frames: Vec<(u64, Msg, u64)>,
        now: u64,
        send: &mut dyn FnMut(u64, Msg, Wire),
    ) -> Result<()> {
        self.apply(effects)?;
        for (to, msg, version) in frames {
            if to != self.me {
                send(
                    to,
                    msg,
                    Wire {
                        sent_ms: now,
                        version,
                    },
                );
            }
        }
        Ok(())
    }

    fn part(&self, name: &str) -> PathBuf {
        self.cdir.join(name)
    }

    fn apply(&mut self, effects: Vec<Effect>) -> Result<()> {
        for effect in effects {
            match effect {
                Effect::Saved(saved) => {
                    put(&self.part("control"), &SavedFile::new(&saved)?)?;
                    self.seen.lock().unwrap().applied = saved.applied;
                    let to = saved.meta.floor;
                    if to != self.floor {
                        put(&self.part("floor"), &(to.epoch, to.value))?;
                        self.floor = to;
                    }
                }
                Effect::Persist(store) => put(&self.part("store"), &store)?,
                Effect::Epoch { id, epoch } => {
                    let held = self.epochs.entry(id).or_insert(0);
                    *held = (*held).max(epoch);
                    put(&self.part("epochs"), &self.epochs)?;
                }
                Effect::Dropped { id, run } | Effect::Tomb { id, run } => {
                    self.dropped.insert(id, run);
                    put(&self.part("dropped"), &self.dropped)?;
                }
                Effect::Expired { alive } => {
                    self.dropped.retain(|_, at| !expired(*at, alive));
                    put(&self.part("dropped"), &self.dropped)?;
                }
                Effect::Wish { by, set } => {
                    self.wishes
                        .entry((set.segment, set.epoch, by))
                        .or_insert(set);
                }
                Effect::WishDone { key } => self.wishes.retain(|k, _| (k.0, k.1) != key),
                Effect::RaftRefused => warn!("the control plane refused a frame"),
                Effect::Left { own, .. } => {
                    let (now, _) = self.clock();
                    let version = self.node.raft.meta().version;
                    let mut to = self.node.gossip_peers();
                    to.extend(self.peers.iter().copied());
                    to.sort_unstable();
                    to.dedup();
                    for peer in to.into_iter().filter(|p| *p != self.me) {
                        let wire = Wire {
                            sent_ms: now,
                            version,
                        };
                        self.net.send(peer, Msg::Gossip(own.clone()), wire);
                    }
                    self.seen.lock().unwrap().left = true;
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn on_frames(&mut self, batch: Vec<(u64, Wire, Msg)>) -> Result<()> {
        let (now, _) = self.clock();
        let mut frames = Vec::new();
        for (from, wire, msg) in batch {
            match msg {
                Msg::Block {
                    id,
                    segment,
                    offset,
                    bytes,
                } => self.on_block(from, id, segment, offset, bytes).await?,
                Msg::Ctl { id, offset, bytes } => self.on_ctl(from, id, offset, bytes).await?,
                Msg::BlockOk { id, offset } => self.on_ack(from, id, offset, true).await?,
                Msg::BlockErr { id, offset } => self.on_ack(from, id, offset, false).await?,
                Msg::Handed {
                    id,
                    mark,
                    writer,
                    unnamed,
                    named,
                } => {
                    if self.data.as_ref().is_some_and(|d| d.ids.contains_key(&id)) {
                        let (born, _) = self.clock();
                        let adopt = Cmd::Adopt {
                            id,
                            mark,
                            born,
                            writer,
                            unnamed,
                            named,
                        };
                        self.step(vec![adopt], Vec::new())?;
                    }
                }
                msg => frames.push(Inbound {
                    from,
                    sent: wire.sent_ms.min(now),
                    version: wire.version,
                    msg,
                }),
            }
        }
        self.step(Vec::new(), frames)
    }

    fn wire(&self) -> Wire {
        let (now, _) = self.clock();
        Wire {
            sent_ms: now,
            version: self.node.raft.meta().version,
        }
    }

    async fn on_block(
        &mut self,
        from: u64,
        id: u64,
        segment: (u64, u32),
        offset: u64,
        bytes: Vec<u8>,
    ) -> Result<()> {
        let Some(data) = &self.data else {
            return Ok(());
        };
        if !self.node.raft.meta().members.contains(&from) {
            return Ok(());
        }
        let sid = SegmentId {
            incarnation: segment.0,
            seq: segment.1,
        };
        let stored = node_id(sid) == id
            && data
                .handle
                .replicate(sid, offset, bytes)
                .await
                .inspect_err(|e| warn!("a replica block was refused: {e}"))
                .is_ok();
        let (adopt_now, gap) = match self.data.as_mut() {
            Some(d) if stored => (d.ids.insert(id, sid).is_none() || d.gaps.remove(&id), false),
            Some(d) => (false, d.ids.contains_key(&id) && d.gaps.insert(id)),
            None => (false, false),
        };
        if gap {
            self.step(vec![Cmd::Gap { id }], Vec::new())?;
        }
        if adopt_now {
            let (born, _) = self.clock();
            let adopt = Cmd::Adopt {
                id,
                mark: 0,
                born,
                writer: Some(from),
                unnamed: None,
                named: Vec::new(),
            };
            self.step(vec![adopt], Vec::new())?;
        }
        let reply = if stored {
            Msg::BlockOk { id, offset }
        } else {
            Msg::BlockErr { id, offset }
        };
        self.net.send(from, reply, self.wire());
        Ok(())
    }

    async fn on_ctl(&mut self, from: u64, id: u64, offset: u64, bytes: Vec<u8>) -> Result<()> {
        let Some(data) = &self.data else {
            return Ok(());
        };
        if !self.node.raft.meta().members.contains(&from) {
            return Ok(());
        }
        let stored = data
            .handle
            .controls(offset, bytes)
            .await
            .inspect_err(|e| warn!("a control block was refused: {e}"))
            .is_ok();
        let reply = if stored {
            Msg::BlockOk { id, offset }
        } else {
            Msg::BlockErr { id, offset }
        };
        self.net.send(from, reply, self.wire());
        Ok(())
    }

    fn ctl_targets(&self, bytes: &[u8], offset: u64) -> (BTreeSet<u64>, BTreeSet<u64>) {
        let Ok((_, body)) = decode_bytes(bytes, offset) else {
            return (BTreeSet::new(), BTreeSet::new());
        };
        let Ok(body) = records::decode(body) else {
            return (BTreeSet::new(), BTreeSet::new());
        };
        let meta = self.node.raft.meta();
        let (mut isr, mut all) = (BTreeSet::new(), BTreeSet::new());
        for control in &body.controls {
            let Stored::Record(record) = control else {
                continue;
            };
            match record {
                Record::Ack { address, .. } | Record::Failure { address, .. } => {
                    if let Some(seg) = meta.segments.get(&node_id(address.segment)) {
                        isr.extend(seg.isr.iter().copied());
                    }
                }
                Record::Generation { .. }
                | Record::Forget { .. }
                | Record::ReadSet { .. }
                | Record::Tombstone { .. }
                | Record::Redrive { .. } => all.extend(meta.members.iter().copied()),
                Record::Messages { .. } => {}
            }
        }
        isr.remove(&self.me);
        all.remove(&self.me);
        (isr, all)
    }

    async fn on_ack(&mut self, from: u64, id: u64, offset: u64, ok: bool) -> Result<()> {
        if self.copy_ack(from, id, offset, ok).await? {
            return Ok(());
        }
        let Some(data) = &mut self.data else {
            return Ok(());
        };
        let Some(pend) = data.pend.get_mut(&(id, offset)) else {
            return Ok(());
        };
        let Some(seg) = data.segs.get(&pend.sid) else {
            return Ok(());
        };
        if from == self.me {
            return Ok(());
        }
        let mut refill = None;
        if seg.holders.contains(&from) {
            if ok {
                pend.copies.insert(from);
            } else {
                pend.failed.insert(from);
                refill = Some(pend.sid);
            }
        } else if pend.ctl.contains(&from) && ok {
            pend.ctl_ok.insert(from);
        } else {
            return Ok(());
        }
        if let Some(sid) = refill
            && self.start_copy(id, from, sid, None)
        {
            self.pump((id, from)).await?;
        }
        self.settle_block(id, offset).await
    }

    fn start_copy(&mut self, id: u64, to: u64, sid: SegmentId, epoch: Option<u64>) -> bool {
        let Some(data) = &mut self.data else {
            return false;
        };
        if data.copies.contains_key(&(id, to)) {
            return false;
        }
        data.copies.insert(
            (id, to),
            Transfer {
                sid,
                next: 0,
                after: 0,
                sent: None,
                fails: 0,
                epoch,
            },
        );
        true
    }

    async fn copy_ack(&mut self, from: u64, id: u64, offset: u64, ok: bool) -> Result<bool> {
        let key = (id, from);
        let Some(data) = &mut self.data else {
            return Ok(false);
        };
        let Some(copy) = data.copies.get_mut(&key) else {
            return Ok(false);
        };
        if copy.sent.map(|s| s.0) != Some(offset) {
            return Ok(false);
        }
        copy.sent = None;
        if ok {
            copy.next = copy.after;
            copy.fails = 0;
        } else {
            copy.next = 0;
            copy.fails += 1;
        }
        if copy.fails >= COPY_TRIES {
            data.copies.remove(&key);
        } else {
            self.pump(key).await?;
        }
        Ok(true)
    }

    async fn pump(&mut self, key: (u64, u64)) -> Result<()> {
        let (id, to) = key;
        let wire = self.wire();
        let Some(data) = &mut self.data else {
            return Ok(());
        };
        let Some(copy) = data.copies.get_mut(&key) else {
            return Ok(());
        };
        let sid = copy.sid;
        let blocks = match data.handle.blocks(sid, copy.next).await {
            Ok(blocks) => blocks,
            Err(e) => {
                warn!("a copy cannot read its segment: {e}");
                data.copies.remove(&key);
                return Ok(());
            }
        };
        let Some((last, block)) = blocks.last() else {
            let done = data.copies.remove(&key);
            return match done {
                Some(done) => self.hand_off(key, done).await,
                None => Ok(()),
            };
        };
        copy.after = align(last + block.len() as u64);
        copy.sent = Some((*last, Instant::now()));
        for (offset, bytes) in blocks {
            let block = Msg::Block {
                id,
                segment: (sid.incarnation, sid.seq),
                offset,
                bytes,
            };
            self.net.send(to, block, wire);
        }
        Ok(())
    }

    async fn hand_off(&mut self, (id, to): (u64, u64), done: Transfer) -> Result<()> {
        let Some(epoch) = done.epoch else {
            return Ok(());
        };
        if let Some(data) = &self.data {
            match data.handle.snapshot().await {
                Ok(bytes) => {
                    let ctl = Msg::Ctl {
                        id,
                        offset: 0,
                        bytes,
                    };
                    self.net.send(to, ctl, self.wire());
                }
                Err(e) => warn!("a control snapshot cannot be read: {e}"),
            }
        }
        let mark = self.node.marks.get(&id).copied().unwrap_or(0);
        let handoff = Cmd::Handoff {
            id,
            to,
            epoch,
            mark,
        };
        let copied = self
            .step_fx(vec![handoff], Vec::new())?
            .into_iter()
            .find_map(|e| match e {
                Effect::Copy {
                    writer,
                    unnamed,
                    named,
                } => Some(Msg::Handed {
                    id,
                    mark,
                    writer,
                    unnamed,
                    named,
                }),
                _ => None,
            });
        if let (Some(frame), Some(data)) = (copied, &mut self.data) {
            data.extra.insert((id, to), Instant::now());
            self.net.send(to, frame, self.wire());
        }
        Ok(())
    }

    async fn repair(&mut self) -> Result<()> {
        let (now, _) = self.clock();
        let Some(data) = &self.data else {
            return Ok(());
        };
        let Some(pass) = self.node.pass() else {
            return Ok(());
        };
        let open: BTreeSet<u64> = data.segs.keys().map(|s| node_id(*s)).collect();
        let none = BTreeSet::new();
        let knobs = Knobs {
            open: &open,
            held: &none,
        };
        let mut jobs: Vec<Job> = self
            .node
            .marks
            .keys()
            .filter(|id| data.ids.contains_key(*id))
            .filter_map(|id| self.node.plan(now, *id, &pass, &knobs).0)
            .collect();
        if self.node.signals != 0 || self.node.view.own().body.drained {
            let (_, run) = self.clock();
            jobs.extend(self.node.drains(run, &knobs));
        }
        let mut starts = Vec::new();
        for job in jobs {
            let Some(sid) = data.ids.get(&job.segment) else {
                continue;
            };
            for to in job.to.iter().copied().take(job.need) {
                let key = (job.segment, to);
                if !data.copies.contains_key(&key) && !data.extra.contains_key(&key) {
                    starts.push((key, *sid, job.epoch));
                }
            }
        }
        for ((id, to), sid, epoch) in starts {
            if self.start_copy(id, to, sid, Some(epoch)) {
                self.pump((id, to)).await?;
            }
        }
        Ok(())
    }

    async fn copy_timeouts(&mut self) -> Result<()> {
        let Some(data) = &mut self.data else {
            return Ok(());
        };
        let mut again = Vec::new();
        data.copies.retain(|key, copy| {
            if copy.sent.is_none_or(|(_, at)| at.elapsed() < COPY_WAIT) {
                return true;
            }
            copy.sent = None;
            copy.fails += 1;
            again.push(*key);
            copy.fails < COPY_TRIES
        });
        for key in again {
            self.pump(key).await?;
        }
        Ok(())
    }

    async fn settle_block(&mut self, id: u64, offset: u64) -> Result<()> {
        let Some(data) = &self.data else {
            return Ok(());
        };
        let Some(pend) = data.pend.get(&(id, offset)) else {
            return Ok(());
        };
        let holders = data.segs.get(&pend.sid).map_or(0, |s| s.holders.len());
        let (copies, failed) = (pend.copies.len(), pend.failed.len());
        let min = usize::from(KEYED_MIN_COPIES);
        let stored = if pend.confirmed(self.want(pend.sid)) {
            if self.node.set_of(id).is_none() {
                return Ok(());
            }
            let mut sealed = self.step_fx(vec![Cmd::Seal { reached: copies }], Vec::new())?;
            if sealed
                .iter()
                .any(|e| matches!(e, Effect::RefusedWrite { resealed: true }))
            {
                sealed = self.step_fx(vec![Cmd::Seal { reached: copies }], Vec::new())?;
            }
            sealed.iter().any(|e| matches!(e, Effect::Stored { .. }))
        } else if holders.saturating_sub(failed) < min {
            false
        } else {
            return Ok(());
        };
        self.finish(id, offset, if stored { copies } else { 0 })
            .await
    }

    fn want(&self, sid: SegmentId) -> usize {
        let isr = self
            .node
            .raft
            .meta()
            .segments
            .get(&node_id(sid))
            .map_or(0, |m| m.isr.len());
        usize::from(self.node.ctl.floor.value)
            .min(isr)
            .max(usize::from(KEYED_MIN_COPIES))
    }

    async fn finish(&mut self, id: u64, offset: u64, copies: usize) -> Result<()> {
        let Some(data) = &mut self.data else {
            return Ok(());
        };
        if let Some(pend) = data.pend.remove(&(id, offset)) {
            data.handle
                .confirm(pend.sid, offset, u32::try_from(copies).unwrap_or(u32::MAX))
                .await
                .map_err(Error::other)?;
        }
        Ok(())
    }

    async fn on_commit(&mut self, (sid, offset, bytes): (SegmentId, u64, Vec<u8>)) -> Result<()> {
        let (me, id) = (self.me, node_id(sid));
        let Some(data) = &mut self.data else {
            return Ok(());
        };
        data.ids.insert(id, sid);
        data.segs
            .entry(sid)
            .or_insert_with(|| Seg {
                holders: Vec::new(),
                backlog: Vec::new(),
            })
            .backlog
            .push((offset, bytes));
        data.pend.insert(
            (id, offset),
            Pend {
                sid,
                copies: BTreeSet::from([me]),
                failed: BTreeSet::new(),
                need: BTreeSet::new(),
                ctl: BTreeSet::new(),
                ctl_ok: BTreeSet::new(),
                deadline: Instant::now() + CONFIRM,
            },
        );
        self.send_backlogs()
    }

    fn send_backlogs(&mut self) -> Result<()> {
        let Some(data) = &self.data else {
            return Ok(());
        };
        let waiting: Vec<SegmentId> = data
            .segs
            .iter()
            .filter(|(_, s)| !s.backlog.is_empty())
            .map(|(sid, _)| *sid)
            .collect();
        for sid in waiting {
            self.place(sid)?;
            let wire = self.wire();
            let me = self.me;
            let Some(seg) = self.data.as_mut().and_then(|d| d.segs.get_mut(&sid)) else {
                continue;
            };
            if seg.holders.is_empty() {
                continue;
            }
            let id = node_id(sid);
            let holders = seg.holders.clone();
            let backlog = std::mem::take(&mut seg.backlog);
            let targets: BTreeSet<u64> = self.data.as_ref().map_or_else(BTreeSet::new, |d| {
                holders
                    .iter()
                    .copied()
                    .chain(
                        d.extra
                            .range((id, 0)..=(id, u64::MAX))
                            .map(|((_, t), _)| *t),
                    )
                    .filter(|t| *t != me && !d.copies.contains_key(&(id, *t)))
                    .collect()
            });
            for (offset, bytes) in backlog {
                let (isr, all) = self.ctl_targets(&bytes, offset);
                let ctl: BTreeSet<u64> = isr
                    .union(&all)
                    .copied()
                    .filter(|t| !holders.contains(t))
                    .collect();
                if let Some(pend) = self
                    .data
                    .as_mut()
                    .and_then(|d| d.pend.get_mut(&(id, offset)))
                {
                    pend.need = isr;
                    pend.ctl = ctl.clone();
                }
                for target in &ctl {
                    let frame = Msg::Ctl {
                        id,
                        offset,
                        bytes: bytes.clone(),
                    };
                    self.net.send(*target, frame, wire);
                }
                for holder in &targets {
                    let block = Msg::Block {
                        id,
                        segment: (sid.incarnation, sid.seq),
                        offset,
                        bytes: bytes.clone(),
                    };
                    self.net.send(*holder, block, wire);
                }
            }
        }
        Ok(())
    }

    fn place(&mut self, sid: SegmentId) -> Result<()> {
        let (me, id) = (self.me, node_id(sid));
        if self
            .data
            .as_ref()
            .and_then(|d| d.segs.get(&sid))
            .is_none_or(|s| !s.holders.is_empty())
        {
            return Ok(());
        }
        let picked = self
            .step_fx(vec![Cmd::Place { id }], Vec::new())?
            .into_iter()
            .find_map(|e| match e {
                Effect::Picked(holders) => Some(holders),
                _ => None,
            })
            .unwrap_or_default();
        if picked.len() < usize::from(KEYED_MIN_COPIES) || !picked.contains(&me) {
            return Ok(());
        }
        let set = HolderSet {
            segment: id,
            epoch: 1,
            writer: me,
            holders: picked.iter().copied().collect(),
        };
        self.wishes.insert((id, 1, me), set);
        let (born, _) = self.clock();
        let adopt = Cmd::Adopt {
            id,
            mark: 0,
            born,
            writer: Some(me),
            unnamed: None,
            named: Vec::new(),
        };
        self.step(vec![adopt], Vec::new())?;
        if let Some(seg) = self.data.as_mut().and_then(|d| d.segs.get_mut(&sid)) {
            seg.holders = picked;
        }
        Ok(())
    }

    async fn data_tick(&mut self) -> Result<()> {
        self.send_backlogs()?;
        let (now, _) = self.clock();
        let Some(data) = &self.data else {
            return Ok(());
        };
        let ready: Vec<(u64, u64)> = data
            .pend
            .iter()
            .filter(|(_, p)| p.confirmed(self.want(p.sid)))
            .map(|(k, _)| *k)
            .collect();
        for (id, offset) in ready {
            self.settle_block(id, offset).await?;
        }
        let Some(data) = &mut self.data else {
            return Ok(());
        };
        let late: Vec<(u64, u64)> = data
            .pend
            .iter()
            .filter(|(_, p)| p.deadline <= Instant::now())
            .map(|(k, _)| *k)
            .collect();
        for (id, offset) in late {
            self.finish(id, offset, 0).await?;
        }
        self.copy_timeouts().await?;
        let Some(data) = &mut self.data else {
            return Ok(());
        };
        let meta = self.node.raft.meta();
        for (sid, seg) in &mut data.segs {
            if let Some(m) = meta.segments.get(&node_id(*sid))
                && m.holders.holders.contains(&self.me)
            {
                seg.holders = m.holders.holders.iter().copied().collect();
            }
        }
        data.extra.retain(|(id, to), at| {
            at.elapsed() < COPY_EXTRA
                && !meta
                    .segments
                    .get(id)
                    .is_some_and(|m| m.holders.holders.contains(to))
        });
        let led: BTreeSet<SegmentId> = self
            .node
            .raft
            .meta()
            .segments
            .keys()
            .filter(|id| self.node.leads(now, **id))
            .filter_map(|id| data.ids.get(id).copied())
            .collect();
        let mut newest: BTreeMap<u64, SegmentId> = BTreeMap::new();
        for (id, sid) in &data.ids {
            let Some(seg) = meta.segments.get(id) else {
                continue;
            };
            let writer = seg.holders.writer;
            if writer == self.me || !meta.members.contains(&writer) {
                continue;
            }
            let top = newest.entry(sid.incarnation).or_insert(*sid);
            if sid.seq > top.seq {
                *top = *sid;
            }
        }
        let renew = data.handle.now_ms() + LEASE_MS;
        if data.led.as_ref() != Some(&led) || renew >= data.led_until + LEASE_MS / 2 {
            data.led = Some(led.clone());
            data.led_until = renew;
            data.handle.lead(led, renew).await.map_err(Error::other)?;
            data.handle
                .open_foreign(newest.into_values().collect())
                .await
                .map_err(Error::other)?;
        }
        Ok(())
    }

    fn gossip_targets(&self) -> Vec<u64> {
        let mut all = self.node.gossip_peers();
        let extra: Vec<u64> = self
            .peers
            .iter()
            .copied()
            .filter(|p| !all.contains(p))
            .collect();
        all.extend(extra);
        if all.is_empty() {
            return all;
        }
        let start = usize::try_from(self.tick).unwrap_or(0) % all.len();
        all.rotate_left(start);
        all.truncate(FANOUT);
        all
    }

    fn leave_when_done(&mut self) -> Result<()> {
        let stop = self.seen.lock().unwrap().stop;
        if stop && !self.stopping {
            self.stopping = true;
            self.step(vec![Cmd::Stop], Vec::new())?;
        }
        let (_, run) = self.clock();
        let none = BTreeSet::new();
        let knobs = Knobs {
            open: &none,
            held: &none,
        };
        if self.stopping && self.node.stop_done(run, &knobs) {
            self.step(vec![Cmd::Leave { remaining: 0 }], Vec::new())?;
        }
        Ok(())
    }

    async fn discover(&mut self) -> Result<()> {
        let Some(d) = &self.disco else {
            return Ok(());
        };
        let now = Instant::now();
        let known: Vec<(u64, String)> = {
            let mut seen = self.seen.lock().unwrap();
            for (id, entry) in self.node.view.live().filter(|(id, _)| *id != self.me) {
                let at = &entry.body.address;
                seen.known.entry(id).or_insert((at.clone(), false, now)).2 = now;
            }
            seen.known
                .iter()
                .map(|(id, (at, _, _))| (*id, at.clone()))
                .collect()
        };
        for (id, addr) in known {
            self.net.connect(id, &addr);
            if !self.peers.contains(&id) {
                self.peers.push(id);
            }
        }
        let alone = self.node.raft.meta().members.is_empty();
        let every = if d.may_found && alone {
            FIND_MS
        } else {
            HELLO_MS
        };
        if self.tick % (every / STEP_MS) == 1 {
            tokio::spawn(hello_round(
                d.url.clone(),
                d.key.clone(),
                self.node.view.own().body.address.clone(),
                (self.me, d.restart),
                self.seen.clone(),
                d.reached.clone(),
            ));
        }
        if d.may_found && alone && d.reached.load(Relaxed) >= FOUND_ROUNDS {
            let seen = self.seen.lock().unwrap();
            let quiet = seen.fresh_at.is_none_or(|at| at.elapsed() >= QUIET);
            let lowest = seen
                .known
                .iter()
                .filter(|(_, (_, _, at))| at.elapsed() < KNOWN_FOR)
                .all(|(id, (_, member, _))| !member && self.me < *id);
            drop(seen);
            if quiet && lowest {
                self.found()?;
            }
        }
        self.merge().await
    }

    async fn merge(&mut self) -> Result<()> {
        let heard = std::mem::take(&mut self.seen.lock().unwrap().heard);
        let members = self.node.raft.meta().members.clone();
        for (node, theirs) in heard {
            let mine = self.seen.lock().unwrap().cluster;
            match mine {
                None if members.contains(&node) => {
                    put(&self.part("cluster"), &theirs)?;
                    self.seen.lock().unwrap().cluster = Some(theirs);
                    self.net.set_cluster(Some(theirs));
                }
                Some(mine) if theirs < mine => {
                    let handle = self.data.as_ref().map(|d| d.handle.clone());
                    let busy = !self.node.raft.meta().segments.is_empty();
                    if !holds_data(busy, handle).await? {
                        std::fs::remove_dir_all(&self.cdir)?;
                        return Err(Error::other(
                            "this cluster lost to another cluster; the control state is wiped; restart to join",
                        ));
                    }
                    self.seen.lock().unwrap().split = true;
                }
                _ => {}
            }
        }
        let split = self.seen.lock().unwrap().split;
        if split && self.warned.is_none_or(|at| at.elapsed() >= SPLIT_LOG) {
            error!(
                "two clusters share this load balancer; this cluster holds data and will not merge"
            );
            self.warned = Some(Instant::now());
        }
        let foreign = self.net.foreign();
        if foreign > 0
            && self
                .foreign_warned
                .is_none_or(|at| at.elapsed() >= SPLIT_LOG)
        {
            warn!("dropped {foreign} frames from another cluster");
            self.foreign_warned = Some(Instant::now());
        }
        Ok(())
    }

    fn found(&mut self) -> Result<()> {
        let raft = Control::found(self.me).map_err(Error::other)?;
        let saved = raft.save().map_err(Error::other)?;
        put(&self.part("control"), &SavedFile::new(&saved)?)?;
        self.node.raft = raft;
        self.node.saved = Arc::new(saved);
        let id = (self.me, wall_ms());
        put(&self.part("cluster"), &id)?;
        self.seen.lock().unwrap().cluster = Some(id);
        self.net.set_cluster(Some(id));
        if let Some(d) = &mut self.disco {
            d.may_found = false;
        }
        Ok(())
    }

    async fn on_tick(&mut self) -> Result<()> {
        if self.seen.lock().unwrap().left {
            return Ok(());
        }
        self.tick += 1;
        self.discover().await?;
        let (now, run) = self.clock();
        self.node.view_tick(now, run);
        self.step(vec![Cmd::Tick, Cmd::Ping], Vec::new())?;
        self.node.timers(now);
        self.step(vec![Cmd::Guard], Vec::new())?;
        self.node.prune(now);
        let mut cmds = vec![
            Cmd::SegAsks,
            Cmd::Fresh,
            Cmd::Beats,
            Cmd::IsrAsks,
            Cmd::ResendCopied,
            Cmd::ResendDrops,
            Cmd::Govern,
            Cmd::Lead,
        ];
        if self.tick.is_multiple_of(GOSSIP_MS / STEP_MS) {
            cmds.push(Cmd::Gossip {
                to: self.gossip_targets(),
            });
        }
        cmds.extend(self.wishes.values().cloned().map(|set| Cmd::Wish { set }));
        self.step(cmds, Vec::new())?;
        if self.tick % (SAMPLE_MS / STEP_MS) == self.node.phase_ms / STEP_MS {
            self.sample(now, run).await?;
        }
        self.data_tick().await?;
        if self.tick.is_multiple_of(DIGEST_MS / STEP_MS) {
            self.repair().await?;
        }
        self.leave_when_done()
    }

    async fn sample(&mut self, now: u64, run: u64) -> Result<()> {
        let (cap, used) = match &self.data {
            Some(data) => disk_use(&data.handle).await?,
            None => (None, 0),
        };
        let sense = Sense {
            now,
            run,
            cap,
            used,
            headroom: 1.0,
            sizes: Default::default(),
        };
        let sampled = node_sample(&mut self.node, &sense);
        if sampled.refused {
            return Err(too_new(sampled.committed, self.node.format_max));
        }
        let next = sampled.committed + 1;
        let live: Vec<u64> = self.node.view.live().map(|(d, _)| d).collect();
        let soaking = live.iter().all(|d| {
            self.node
                .view
                .entry(*d)
                .is_some_and(|e| e.body.format_max >= next)
        });
        let soak_left_ms = soaking
            .then(|| {
                let least = live.iter().map(|d| self.node.view.alive_for(*d, run)).min();
                SOAK_MS.saturating_sub(least.unwrap_or(0))
            })
            .filter(|_| !live.is_empty());
        {
            let mut seen = self.seen.lock().unwrap();
            seen.committed = sampled.committed;
            seen.soak_left_ms = soak_left_ms;
            seen.live = live.len();
            seen.floor = self.floor.value;
            seen.count = self.node.ctl.count;
            seen.target = controller::target(seen.count, live.len().max(1), self.node.headroom);
            seen.heavy = self.node.heavy_node();
            seen.until_full_s = self
                .node
                .view
                .live()
                .map(|(_, e)| &e.body)
                .filter(|b| b.fill_rate < b.disk_total.saturating_sub(b.disk_free))
                .filter_map(|b| {
                    b.disk_free
                        .saturating_mul(SAMPLE_MS / 1000)
                        .checked_div(b.fill_rate)
                })
                .min();
        }
        let mut cmds = Vec::new();
        if let Some(to) = sampled.decided.filter(|_| sampled.acts) {
            let cause = if to.epoch > sampled.held.epoch {
                verdandi_node::Cause::Quiet
            } else {
                verdandi_node::Cause::Rise
            };
            cmds.push(Cmd::Floor { to, cause });
        }
        cmds.push(Cmd::WriteGroups);
        put(&self.part("member"), &sampled.member)?;
        put(&self.part("committed"), &sampled.committed)?;
        put(&self.part("alive"), &run)?;
        self.step(cmds, Vec::new())
    }
}

async fn holds_data(busy: bool, handle: Option<Handle>) -> Result<bool> {
    let Some(handle) = handle.filter(|_| !busy) else {
        return Ok(busy);
    };
    let segments = handle.segments().await.map_err(Error::other)?;
    let status = handle.status().await.map_err(Error::other)?;
    Ok(!segments.is_empty() || !status.streams.is_empty())
}

async fn disk_use(handle: &Handle) -> Result<(Option<u64>, u64)> {
    let status = handle.status().await.map_err(Error::other)?;
    Ok(status.disk.map_or((None, 0), |d| {
        (Some(d.total), d.total.saturating_sub(d.free))
    }))
}

async fn hello_round(
    url: String,
    key: Vec<u8>,
    addr: String,
    (node, incarnation): (u64, u64),
    seen: Arc<Mutex<Seen>>,
    reached: Arc<AtomicU32>,
) {
    let sent_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(0);
    let hello = Hello {
        node,
        addr,
        incarnation,
        sent_ms,
        cluster: seen.lock().unwrap().cluster,
    };
    let Ok(body) = seal(&key, &hello) else {
        return;
    };
    let client = reqwest::Client::builder()
        .timeout(HELLO_TIMEOUT)
        .pool_max_idle_per_host(0)
        .build();
    let Ok(client) = client else {
        return;
    };
    let target = format!("{}/s/_vd.hello", url.trim_end_matches('/'));
    let reply = match client.post(target).body(body).send().await {
        Ok(reply) if reply.status().is_success() => reply.bytes().await,
        Ok(reply) => {
            warn!("hello refused with {}", reply.status());
            return;
        }
        Err(e) if e.is_timeout() => {
            reached.fetch_add(1, Relaxed);
            return;
        }
        Err(e) => {
            warn!("hello failed: {e}");
            return;
        }
    };
    let Some(reply) = reply.ok().and_then(|b| open_members(&key, &b).ok()) else {
        warn!("hello reply is not valid");
        return;
    };
    let mut seen = seen.lock().unwrap();
    if let (Some(theirs), Some(first)) = (reply.cluster, reply.members.first()) {
        seen.heard.push((first.0, theirs));
    }
    for (id, at, member) in reply
        .members
        .into_iter()
        .filter(|m| m.0 != node && m.0 != 0)
    {
        seen.learn(id, at, member);
    }
    reached.fetch_add(1, Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn saved_is_written_before_the_first_frame_leaves() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/c3t3-dirs");
        std::fs::create_dir_all(&root).unwrap();
        let dir = tempfile::tempdir_in(root).unwrap();
        let mut driver = Driver::build(
            b"cluster key",
            "127.0.0.1:0",
            Vec::new(),
            true,
            dir.path().into(),
            None,
            None,
            FORMAT_MAX,
        )
        .await
        .unwrap();
        let path = driver.part("control");
        let saved = Saved::default();
        let want = postcard::to_allocvec(&SavedFile::new(&saved).unwrap()).unwrap();
        assert_ne!(std::fs::read(&path).unwrap(), want);
        let mut at_send = None;
        driver
            .deliver(
                vec![Effect::Saved(Arc::new(saved))],
                vec![(9, Msg::Lose(1), 1)],
                0,
                &mut |_, _, _| {
                    at_send.get_or_insert(std::fs::read(&path).unwrap());
                },
            )
            .unwrap();
        driver.net.stop();
        assert_eq!(at_send.unwrap(), want);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_sample_feeds_disk_use_and_it_grows_after_a_write() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/prec6-dirs");
        std::fs::create_dir_all(&root).unwrap();
        let dir = tempfile::tempdir_in(root).unwrap();
        let me = own_id(dir.path()).unwrap();
        let store = dir.path().join("store");
        std::fs::create_dir_all(&store).unwrap();
        let (engine, handle) = verdandi_store::engine::Engine::open_as(
            verdandi_store::fs::RealFs,
            &store,
            Arc::new(verdandi_store::engine::WallClock),
            Box::new(verdandi_store::engine::OsRandom),
            me,
        )
        .await
        .unwrap();
        tokio::spawn(engine.run());
        let mut driver = Driver::build(
            b"cluster key",
            "127.0.0.1:0",
            Vec::new(),
            true,
            dir.path().into(),
            None,
            Some(handle.clone()),
            FORMAT_MAX,
        )
        .await
        .unwrap();
        let (now, run) = driver.clock();
        driver.sample(now, run).await.unwrap();
        let first = driver.node.view.latest().clone();
        assert!(first.disk_total > 0);
        let mut file = File::create(dir.path().join("ballast")).unwrap();
        file.write_all(&vec![7u8; 16 << 20]).unwrap();
        file.sync_all().unwrap();
        driver
            .sample(now + SAMPLE_MS, run + SAMPLE_MS)
            .await
            .unwrap();
        let second = driver.node.view.latest().clone();
        assert!(
            second.holds > first.holds,
            "{} {}",
            second.holds,
            first.holds
        );
        assert_eq!(second.disk_total, first.disk_total);
        driver.net.stop();
    }

    #[test]
    fn a_lacking_segment_blocks_ready_only_after_one_repair_period() {
        let mut since = BTreeMap::new();
        assert!(caught_up(&mut since, BTreeSet::from([1]), 100));
        assert!(caught_up(
            &mut since,
            BTreeSet::from([1]),
            100 + DIGEST_MS - 1
        ));
        assert!(!caught_up(&mut since, BTreeSet::from([1]), 100 + DIGEST_MS));
        assert!(caught_up(&mut since, BTreeSet::new(), 100 + DIGEST_MS));
        assert!(since.is_empty());
    }
}
