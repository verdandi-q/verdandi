use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Result;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use verdandi_node::Msg;

use crate::link::{Link, Reader, Writer};
use crate::{ClusterId, MAX_FRAME};

const QUEUE: usize = 1024;
const INBOUND: usize = 4096;
const STEP: Duration = Duration::from_secs(5);
const PING: Duration = Duration::from_secs(1);
const RETRY_MIN: Duration = Duration::from_millis(100);
const RETRY_MAX: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Wire {
    pub sent_ms: u64,
    pub version: u64,
}

#[derive(Serialize, Deserialize)]
enum Env {
    Direct(Option<ClusterId>, Wire, Msg),
    Relay(Option<ClusterId>, u64, u64, Wire, Msg),
    Ping,
}

#[derive(Default)]
struct Queue {
    items: VecDeque<Env>,
    head: u64,
}

#[derive(Default)]
struct Retry {
    at: Option<Instant>,
    delay: Duration,
}

struct Active {
    serial: u64,
    opener: u64,
    close: Arc<Notify>,
}

#[derive(Default)]
struct Peer {
    queue: Mutex<Queue>,
    wake: Notify,
    addr: Mutex<Option<String>>,
    active: Mutex<Option<Active>>,
    retry: Mutex<Retry>,
    dialing: AtomicBool,
}

impl Peer {
    fn linked(&self) -> bool {
        self.active.lock().unwrap().is_some()
    }

    fn backing_off(&self) -> bool {
        self.retry
            .lock()
            .unwrap()
            .at
            .is_some_and(|at| Instant::now() < at)
    }

    fn failed(&self) {
        let mut retry = self.retry.lock().unwrap();
        retry.delay = (retry.delay * 2).clamp(RETRY_MIN, RETRY_MAX);
        retry.at = Some(Instant::now() + retry.delay);
    }

    fn proved(&self, good: &AtomicBool) {
        if !good.swap(true, Relaxed) {
            *self.retry.lock().unwrap() = Retry::default();
        }
    }

    fn idle(&self) -> bool {
        self.addr.lock().unwrap().is_none()
            && !self.linked()
            && self.queue.lock().unwrap().items.is_empty()
    }
}

struct Inner {
    key: Vec<u8>,
    me: u64,
    addr: String,
    peers: Mutex<HashMap<u64, Arc<Peer>>>,
    blocked: Mutex<HashSet<u64>>,
    dropped: AtomicU64,
    foreign: AtomicU64,
    cluster: Mutex<Option<ClusterId>>,
    serials: AtomicU64,
    accept: Mutex<Option<JoinHandle<()>>>,
    tx: mpsc::Sender<(u64, Wire, Msg)>,
}

#[derive(Clone)]
pub struct Net(Arc<Inner>);

impl Net {
    pub async fn start(
        key: &[u8],
        listen: &str,
        me: u64,
    ) -> Result<(Net, mpsc::Receiver<(u64, Wire, Msg)>)> {
        let listener = TcpListener::bind(listen).await?;
        let (tx, rx) = mpsc::channel(INBOUND);
        let net = Net(Arc::new(Inner {
            key: key.to_vec(),
            me,
            addr: listener.local_addr()?.to_string(),
            peers: Mutex::default(),
            blocked: Mutex::default(),
            dropped: AtomicU64::new(0),
            foreign: AtomicU64::new(0),
            cluster: Mutex::default(),
            serials: AtomicU64::new(0),
            accept: Mutex::default(),
            tx,
        }));
        let accepting = net.clone();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    sleep(Duration::from_millis(50)).await;
                    continue;
                };
                let net = accepting.clone();
                tokio::spawn(async move {
                    let (key, me, addr) = (&net.0.key, net.0.me, &net.0.addr);
                    if let Ok(Ok(link)) = timeout(STEP, Link::accept(key, stream, me, addr)).await {
                        net.run(link, false);
                    }
                });
            }
        });
        *net.0.accept.lock().unwrap() = Some(handle);
        Ok((net, rx))
    }

    pub fn stop(&self) {
        if let Some(handle) = self.0.accept.lock().unwrap().take() {
            handle.abort();
        }
        let ids: Vec<u64> = self.0.peers.lock().unwrap().keys().copied().collect();
        for id in ids {
            self.kill(id);
        }
    }

    pub fn addr(&self) -> &str {
        &self.0.addr
    }

    pub fn connect(&self, id: u64, addr: &str) {
        *self.0.peer(id).addr.lock().unwrap() = Some(addr.to_owned());
    }

    pub fn dropped(&self) -> u64 {
        self.0.dropped.load(Relaxed)
    }

    pub fn foreign(&self) -> u64 {
        self.0.foreign.load(Relaxed)
    }

    pub fn set_cluster(&self, id: Option<ClusterId>) {
        *self.0.cluster.lock().unwrap() = id;
    }

    pub fn linked(&self, id: u64) -> bool {
        self.0.linked_peer(id).is_some()
    }

    pub fn block(&self, id: u64) {
        self.0.blocked.lock().unwrap().insert(id);
        self.kill(id);
    }

    pub fn kill(&self, id: u64) {
        if let Some(peer) = self.0.peers.lock().unwrap().get(&id)
            && let Some(active) = peer.active.lock().unwrap().as_ref()
        {
            active.close.notify_one();
        }
    }

    pub fn send(&self, to: u64, msg: Msg, wire: Wire) {
        let peer = self.0.peers.lock().unwrap().get(&to).cloned();
        let Some(peer) = peer.filter(|p| {
            !self.0.is_blocked(to)
                && (p.linked() || (p.addr.lock().unwrap().is_some() && !p.backing_off()))
        }) else {
            self.0.relay(to, msg, wire);
            return;
        };
        let cluster = *self.0.cluster.lock().unwrap();
        self.0.push(&peer, Env::Direct(cluster, wire, msg));
        if !peer.linked() {
            self.dial(to, peer);
        }
    }

    fn dial(&self, to: u64, peer: Arc<Peer>) {
        if peer.dialing.swap(true, Relaxed) {
            return;
        }
        let net = self.clone();
        tokio::spawn(async move {
            let wait = peer.retry.lock().unwrap().at;
            if let Some(at) = wait {
                tokio::time::sleep_until(at.into()).await;
            }
            let linked = net.dial_once(to, &peer).await;
            if !linked {
                peer.failed();
            }
            peer.dialing.store(false, Relaxed);
            if !linked && !peer.linked() {
                let lost: Vec<Env> = peer.queue.lock().unwrap().items.drain(..).collect();
                for env in lost {
                    if let Env::Direct(_, wire, msg) = env {
                        net.0.relay(to, msg, wire);
                    }
                }
            }
        });
    }

    async fn dial_once(&self, to: u64, peer: &Peer) -> bool {
        let addr = peer.addr.lock().unwrap().clone();
        let Some(addr) = addr.filter(|_| !self.0.is_blocked(to)) else {
            return false;
        };
        let (key, me, mine) = (&self.0.key, self.0.me, &self.0.addr);
        match timeout(STEP, Link::connect(key, addr, me, mine)).await {
            Ok(Ok(link)) if link.peer().0 == to => self.run(link, true),
            _ => false,
        }
    }

    fn run(&self, link: Link, dialed: bool) -> bool {
        let (id, addr) = link.peer();
        if id == self.0.me || self.0.is_blocked(id) {
            return false;
        }
        let peer = self.0.peer(id);
        if !addr.is_empty() {
            peer.addr
                .lock()
                .unwrap()
                .get_or_insert_with(|| addr.to_owned());
        }
        let opener = if dialed { self.0.me } else { id };
        let low = self.0.me.min(id);
        let serial = self.0.serials.fetch_add(1, Relaxed);
        let close = Arc::new(Notify::new());
        let good = Arc::new(AtomicBool::new(false));
        let kept = {
            let mut active = peer.active.lock().unwrap();
            let loses = active
                .as_ref()
                .is_some_and(|cur| cur.opener == low && opener != low);
            if !loses {
                if let Some(cur) = active.as_ref() {
                    cur.close.notify_one();
                }
                *active = Some(Active {
                    serial,
                    opener,
                    close: close.clone(),
                });
            }
            !loses
        };
        let (rd, wr) = link.split();
        let net = self.clone();
        if !kept {
            drop(wr);
            tokio::spawn(async move { net.0.read_loop(id, rd, &peer, &good).await });
            return true;
        }
        tokio::spawn(async move {
            let mut read = std::pin::pin!(net.0.read_loop(id, rd, &peer, &good));
            let write = async {
                tokio::select! {
                    () = close.notified() => true,
                    () = net.0.write_loop(&peer, wr, &good) => false,
                }
            };
            tokio::select! {
                () = &mut read => net.gone(id, serial, &peer, &good),
                graceful = write => {
                    net.gone(id, serial, &peer, &good);
                    if graceful {
                        read.await;
                    }
                }
            }
        });
        true
    }

    fn gone(&self, id: u64, serial: u64, peer: &Arc<Peer>, good: &AtomicBool) {
        {
            let mut active = peer.active.lock().unwrap();
            if active.as_ref().is_none_or(|a| a.serial != serial) {
                return;
            }
            *active = None;
        }
        if !good.load(Relaxed) {
            peer.failed();
        }
        if !peer.queue.lock().unwrap().items.is_empty() {
            self.dial(id, peer.clone());
        } else if peer.idle() {
            let mut peers = self.0.peers.lock().unwrap();
            if peers
                .get(&id)
                .is_some_and(|p| Arc::ptr_eq(p, peer) && p.idle())
            {
                peers.remove(&id);
            }
        }
    }
}

impl Inner {
    fn peer(&self, id: u64) -> Arc<Peer> {
        self.peers.lock().unwrap().entry(id).or_default().clone()
    }

    fn linked_peer(&self, id: u64) -> Option<Arc<Peer>> {
        self.peers
            .lock()
            .unwrap()
            .get(&id)
            .filter(|p| p.linked())
            .cloned()
    }

    fn is_blocked(&self, id: u64) -> bool {
        self.blocked.lock().unwrap().contains(&id)
    }

    fn push(&self, peer: &Peer, env: Env) {
        {
            let mut queue = peer.queue.lock().unwrap();
            if queue.items.len() >= QUEUE {
                queue.items.pop_front();
                queue.head += 1;
                self.dropped.fetch_add(1, Relaxed);
            }
            queue.items.push_back(env);
        }
        peer.wake.notify_one();
    }

    fn relay(&self, to: u64, msg: Msg, wire: Wire) {
        let via: Vec<Arc<Peer>> = if msg.relayable() {
            self.peers
                .lock()
                .unwrap()
                .iter()
                .filter(|(id, p)| **id != to && p.linked())
                .map(|(_, p)| p.clone())
                .collect()
        } else {
            Vec::new()
        };
        let cluster = *self.cluster.lock().unwrap();
        if via.is_empty() {
            self.dropped.fetch_add(1, Relaxed);
        }
        for peer in via {
            self.push(&peer, Env::Relay(cluster, self.me, to, wire, msg.clone()));
        }
    }

    async fn read_loop(&self, from: u64, mut rd: Reader, peer: &Peer, good: &AtomicBool) {
        while let Ok(bytes) = rd.recv_bytes(MAX_FRAME).await {
            peer.proved(good);
            if !self.inbound(from, &bytes).await {
                return;
            }
        }
    }

    async fn write_loop(&self, peer: &Peer, mut wr: Writer, good: &AtomicBool) {
        let ping = postcard::to_allocvec(&Env::Ping).expect("an Env always encodes");
        loop {
            let next = {
                let queue = peer.queue.lock().unwrap();
                queue.items.front().map(|env| {
                    (
                        postcard::to_allocvec(env).expect("an Env always encodes"),
                        queue.head,
                    )
                })
            };
            let Some((bytes, head)) = next else {
                tokio::select! {
                    () = peer.wake.notified() => {}
                    () = sleep(PING) => {
                        if wr.send_bytes(&ping).await.is_err() {
                            return;
                        }
                        peer.proved(good);
                    }
                }
                continue;
            };
            if bytes.len() > MAX_FRAME {
                self.dropped.fetch_add(1, Relaxed);
            } else if wr.send_bytes(&bytes).await.is_err() {
                return;
            } else {
                peer.proved(good);
            }
            let mut queue = peer.queue.lock().unwrap();
            if queue.head == head {
                queue.items.pop_front();
                queue.head += 1;
            }
        }
    }

    async fn inbound(&self, from: u64, bytes: &[u8]) -> bool {
        let Ok(env) = postcard::from_bytes::<Env>(bytes) else {
            return false;
        };
        let theirs = match &env {
            Env::Direct(c, ..) | Env::Relay(c, ..) => *c,
            Env::Ping => None,
        };
        let mine = *self.cluster.lock().unwrap();
        if mine.is_some() && theirs.is_some() && mine != theirs {
            self.foreign.fetch_add(1, Relaxed);
            return true;
        }
        match env {
            Env::Ping => true,
            Env::Direct(_, wire, msg) => self.tx.send((from, wire, msg)).await.is_ok(),
            Env::Relay(_, origin, to, wire, msg) if to == self.me => {
                !msg.relayable() || self.tx.send((origin, wire, msg)).await.is_ok()
            }
            Env::Relay(cluster, origin, to, wire, msg) => {
                if msg.relayable()
                    && let Some(peer) = self.linked_peer(to)
                {
                    self.push(&peer, Env::Relay(cluster, origin, to, wire, msg));
                }
                true
            }
        }
    }
}
