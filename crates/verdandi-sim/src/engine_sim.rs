use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rand::seq::IndexedRandom;
use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use verdandi_core::ids::{Address, MessageId, SegmentId};
use verdandi_core::lease::{Bury, Nack, Receipt, Settle};
use verdandi_core::{MAX_FAILURES, Record, Timing};
use verdandi_store::block::{BLOCK_HEADER_LEN, FILE_HEADER_LEN};
use verdandi_store::engine::{
    Clock, Delivery, Engine, Error, Handle, IDLE_SEAL_MS, Key, NewMessage, Published, Random,
    SettleBy, Take, Taken, TestClock,
};
use verdandi_store::records::{self, Control, Damage};
use verdandi_store::recovery::recover;

use crate::fault_fs::{Crash, FaultFs};

const DATA: &str = "/data";
const GROUPS: [&str; 2] = ["g", "h"];
const ROUNDS: usize = 4;
const STEPS: usize = 30;
const LEASE: u64 = 60_000;

#[derive(Debug, Default)]
pub struct Report {
    pub published: usize,
    pub acked: usize,
    pub crashes: usize,
    pub flips: usize,
    pub named: usize,
    pub dead: usize,
    pub served: usize,
    pub repeats: usize,
}

struct Seeded(ChaCha8Rng);

impl Random for Seeded {
    fn fill(&mut self, bytes: &mut [u8]) -> Result<(), Error> {
        self.0.fill_bytes(bytes);
        Ok(())
    }
}

struct Block {
    path: PathBuf,
    segment: SegmentId,
    winner: bool,
    range: Range<u64>,
    ids: Vec<MessageId>,
    acks: Vec<(String, Address)>,
    failures: Vec<(String, Address)>,
    damage: Vec<Damage>,
    keys: Vec<String>,
}

#[derive(Default)]
struct Scan {
    blocks: Vec<Block>,
    messages: BTreeMap<MessageId, (String, Vec<u8>)>,
    addresses: BTreeMap<Address, MessageId>,
    acked: BTreeSet<(String, Address)>,
    failures: BTreeMap<(String, Address), u32>,
    dead: BTreeMap<(String, MessageId), usize>,
}

impl Scan {
    fn read(fs: &FaultFs) -> Scan {
        let mut scan = Scan::default();
        let mut seen = Vec::new();
        recover(&fs.snapshot(), Path::new(DATA), |path, header, body| {
            seen.push((path.to_path_buf(), *header, body.to_vec()));
        })
        .expect("scan");
        let mut winners: BTreeMap<SegmentId, ((u32, u64), &PathBuf)> = BTreeMap::new();
        for (path, header, _) in &seen {
            let key = (header.compaction_version, header.epoch);
            if winners
                .get(&header.segment)
                .is_none_or(|(best, _)| key > *best)
            {
                winners.insert(header.segment, (key, path));
            }
        }
        for (path, header, body) in &seen {
            let winner = winners
                .get(&header.segment)
                .is_some_and(|(_, best)| *best == path);
            let end = header.offset + (BLOCK_HEADER_LEN + body.len()) as u64;
            let body = records::decode(body).expect("a good block decodes");
            let mut block = Block {
                path: path.clone(),
                segment: header.segment,
                winner,
                range: header.offset..end,
                ids: Vec::new(),
                acks: Vec::new(),
                failures: Vec::new(),
                damage: Vec::new(),
                keys: Vec::new(),
            };
            for (entry, bytes) in body.runs.iter().filter(|_| winner) {
                for m in records::decode_run(bytes, entry.count).expect("a good run decodes") {
                    let address = Address {
                        segment: header.segment,
                        position: m.position,
                    };
                    if let Some(origin) = &m.origin {
                        *scan
                            .dead
                            .entry((entry.stream.clone(), origin.id))
                            .or_default() += 1;
                    }
                    block.ids.push(m.id);
                    scan.addresses.insert(address, m.id);
                    let id = m.id;
                    let twice = scan.messages.insert(id, (entry.stream.clone(), m.value));
                    assert!(twice.is_none(), "id {id} is stored twice");
                }
            }
            for control in &body.controls {
                match control {
                    Control::Record(Record::Ack { group, address, .. }) => {
                        block.acks.push((group.clone(), *address));
                        scan.acked.insert((group.clone(), *address));
                    }
                    Control::Record(Record::Failure {
                        group,
                        address,
                        count,
                        ..
                    }) => {
                        block.failures.push((group.clone(), *address));
                        let kept = scan.failures.entry((group.clone(), *address)).or_default();
                        *kept = (*kept).max(*count);
                    }
                    Control::Damage(damage) => block.damage.push(*damage),
                    Control::Key { key, .. } => block.keys.push(key.clone()),
                    _ => {}
                }
            }
            scan.blocks.push(block);
        }
        scan
    }

    fn acked_by(&self, group: &str, id: MessageId) -> bool {
        self.acked
            .iter()
            .any(|(g, a)| g == group && self.addresses.get(a) == Some(&id))
    }
}

#[derive(Default)]
struct Acks {
    tried: BTreeMap<(String, MessageId), u64>,
    durable: BTreeMap<(String, MessageId), u64>,
}

impl Acks {
    fn attempt(&mut self, group: &str, id: MessageId, settle: u64) {
        self.tried.entry((group.to_owned(), id)).or_insert(settle);
    }

    fn landed(&mut self, group: &str, id: MessageId, settle: u64) {
        self.durable.entry((group.to_owned(), id)).or_insert(settle);
    }

    fn found(&mut self, scan: &Scan) {
        for (group, address) in &scan.acked {
            let Some(id) = scan.addresses.get(address) else {
                continue;
            };
            let key = (group.clone(), *id);
            if let Some(settle) = self.tried.get(&key).copied() {
                self.durable.entry(key).or_insert(settle);
            }
        }
    }

    fn removable(
        &self,
        id: MessageId,
        buried: &dyn Fn(&str) -> bool,
        lost: &BTreeSet<(String, MessageId)>,
    ) -> bool {
        let at = |g: &str| self.durable.get(&(g.to_owned(), id)).copied();
        let last = GROUPS.iter().filter_map(|g| at(g)).max().unwrap_or(0);
        GROUPS.iter().all(|g| {
            let first = self
                .durable
                .iter()
                .filter(|(key, _)| key.0 == *g && !lost.contains(*key))
                .map(|(_, at)| *at)
                .min();
            at(g).is_some() || buried(g) || first.is_none_or(|first| first > last)
        })
    }
}

fn tag_of(value: &[u8]) -> Option<(u64, u32)> {
    let text = std::str::from_utf8(value).ok()?;
    let mut parts = text.split(':');
    let tag = parts.next()?.parse().ok()?;
    parts.next()?;
    Some((tag, parts.next()?.parse().ok()?))
}

fn ids_of(published: &Published) -> Vec<MessageId> {
    (0..published.count)
        .filter_map(|n| verdandi_core::nth_id(published.first, n))
        .collect()
}

struct Node {
    handle: Handle,
    task: JoinHandle<Result<(), Error>>,
}

struct Sim {
    seed: u64,
    rng: ChaCha8Rng,
    rt: Runtime,
    fs: FaultFs,
    clock: TestClock,
    tag: u64,
    batches: BTreeMap<u64, u32>,
    acked_publishes: BTreeSet<u64>,
    expiry: BTreeMap<u64, u64>,
    published: BTreeMap<u64, Vec<MessageId>>,
    settles: u64,
    acks: Acks,
    acked: BTreeSet<(String, MessageId)>,
    nacked: BTreeSet<(String, MessageId)>,
    keys: BTreeMap<String, (String, u64, Published)>,
    leases: Vec<(String, Receipt, Vec<MessageId>)>,
    lost: BTreeSet<MessageId>,
    lost_tags: BTreeSet<u64>,
    lost_keys: BTreeSet<String>,
    lost_acks: BTreeSet<(String, Address)>,
    lost_acked: BTreeSet<(String, MessageId)>,
    lost_nacked: BTreeSet<(String, MessageId)>,
    repeated: BTreeSet<u64>,
    named: Vec<(SegmentId, u64, usize)>,
    flipped: BTreeSet<(PathBuf, u64)>,
    report: Report,
}

impl Sim {
    fn new(seed: u64) -> Self {
        Self {
            seed,
            rng: ChaCha8Rng::seed_from_u64(seed),
            rt: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime"),
            fs: FaultFs::new(seed, 1 << 40),
            clock: TestClock::new(1_000_000),
            tag: 0,
            batches: BTreeMap::new(),
            acked_publishes: BTreeSet::new(),
            expiry: BTreeMap::new(),
            published: BTreeMap::new(),
            settles: 0,
            acks: Acks::default(),
            acked: BTreeSet::new(),
            nacked: BTreeSet::new(),
            keys: BTreeMap::new(),
            leases: Vec::new(),
            lost: BTreeSet::new(),
            lost_tags: BTreeSet::new(),
            lost_keys: BTreeSet::new(),
            lost_acks: BTreeSet::new(),
            lost_acked: BTreeSet::new(),
            lost_nacked: BTreeSet::new(),
            repeated: BTreeSet::new(),
            named: Vec::new(),
            flipped: BTreeSet::new(),
            report: Report::default(),
        }
    }

    fn boot(&mut self) -> Node {
        let (engine, handle) = self
            .rt
            .block_on(Engine::open(
                self.fs.clone(),
                Path::new(DATA),
                Arc::new(self.clock.clone()),
                Box::new(Seeded(ChaCha8Rng::seed_from_u64(self.rng.random()))),
            ))
            .unwrap_or_else(|e| panic!("seed {}: open: {e}", self.seed));
        let task = self.rt.spawn(engine.run());
        Node { handle, task }
    }

    fn lose(&mut self, scan: &Scan, block: usize) {
        let block = &scan.blocks[block];
        self.flipped.insert((block.path.clone(), block.range.start));
        self.lost_keys.extend(block.keys.iter().cloned());
        for id in &block.ids {
            self.lost.insert(*id);
            if let Some((tag, _)) = scan.messages.get(id).and_then(|(_, v)| tag_of(v)) {
                self.lost_tags.insert(tag);
            }
        }
        for (group, address) in &block.acks {
            self.lost_acks.insert((group.clone(), *address));
            if let Some(id) = scan.addresses.get(address) {
                self.lost_acked.insert((group.clone(), *id));
            }
        }
        for (group, address) in &block.failures {
            if let Some(id) = scan.addresses.get(address) {
                self.lost_nacked.insert((group.clone(), *id));
            }
        }
        let kept = |d: &Damage| {
            scan.blocks.iter().any(|b| {
                !self.flipped.contains(&(b.path.clone(), b.range.start)) && b.damage.contains(d)
            })
        };
        let lost: Vec<Damage> = block.damage.iter().filter(|d| !kept(d)).copied().collect();
        self.named.retain(|(segment, at, _)| {
            !lost
                .iter()
                .any(|d| d.segment == *segment && (d.start..d.end).contains(at))
        });
    }

    fn flip(&mut self, scan: &Scan) -> Option<(SegmentId, u64)> {
        let n = self.rng.random_range(0..scan.blocks.len().max(1));
        let block = scan.blocks.get(n)?;
        let header = self.rng.random_bool(0.1);
        let range = if header {
            0..FILE_HEADER_LEN as u64
        } else {
            block.range.clone()
        };
        let at = self.rng.random_range(range);
        let mask = self.rng.random_range(1..=u8::MAX);
        if !self.fs.flip_at(&block.path, at, mask) {
            return None;
        }
        self.report.flips += 1;
        if !header {
            self.lose(scan, n);
        }
        if !block.winner {
            return None;
        }
        Some((block.segment, at))
    }

    fn check(&mut self, scan: &Scan, round: usize) {
        let seed = self.seed;
        let mut present: BTreeMap<u64, u32> = BTreeMap::new();
        for (stream, value) in scan.messages.values() {
            if stream == "a" || stream == "t" {
                let (tag, count) = tag_of(value).expect("a tagged value");
                assert_eq!(
                    self.batches.get(&tag),
                    Some(&count),
                    "seed {seed}: unknown batch"
                );
                *present.entry(tag).or_default() += 1;
            }
        }
        for (tag, n) in &present {
            assert_eq!(
                Some(n),
                self.batches.get(tag),
                "seed {seed} round {round}: batch {tag} is in part"
            );
        }
        for tag in &self.repeated {
            assert!(
                !present.contains_key(tag),
                "seed {seed} round {round}: the repeat {tag} stored its message"
            );
        }
        for id in scan.messages.keys() {
            assert!(
                !self.lost.contains(id),
                "seed {seed} round {round}: the lost message {id} shows again"
            );
        }
        self.acks.found(scan);
        let now = self.clock.now_ms();
        let removable = |tag: &u64| match (self.expiry.get(tag), self.published.get(tag)) {
            (Some(expired), _) => *expired <= now,
            (None, Some(ids)) => ids.iter().all(|id| {
                let buried = |g: &str| scan.dead.contains_key(&(format!("a.dead.{g}"), *id));
                self.acks.removable(*id, &buried, &self.lost_acked)
            }),
            (None, None) => false,
        };
        for tag in &self.acked_publishes {
            assert!(
                present.contains_key(tag) || self.lost_tags.contains(tag) || removable(tag),
                "seed {seed} round {round}: acked publish {tag} lost"
            );
        }
        let durable = |settled: &mut dyn Iterator<Item = &(String, Address)>| {
            settled
                .filter_map(|(g, a)| Some((g.clone(), *scan.addresses.get(a)?)))
                .collect::<BTreeSet<(String, MessageId)>>()
        };
        let acked = durable(&mut scan.acked.iter());
        let failed = durable(&mut scan.failures.keys());
        for (kind, settled, durable, lost) in [
            ("ack or bury", &self.acked, &acked, &self.lost_acked),
            ("nack", &self.nacked, &failed, &self.lost_nacked),
        ] {
            for (group, id) in settled {
                let key = (group.clone(), *id);
                assert!(
                    durable.contains(&key)
                        || self.lost.contains(id)
                        || lost.contains(&key)
                        || !scan.messages.contains_key(id),
                    "seed {seed} round {round}: a 200 {kind} of {id} by {group} is not durable"
                );
            }
        }
        for ((stream, origin), copies) in &scan.dead {
            assert!(
                *copies <= 1 || self.lost.contains(origin),
                "seed {seed}: {copies} dead copies of {origin} in {stream}"
            );
        }
    }

    fn audit(&mut self, node: &Node, scan: &Scan, live_flip: bool, round: usize) {
        let seed = self.seed;
        let (damage, _) = self.rt.block_on(node.handle.damage()).expect("damage");
        for (segment, at, flipped) in &self.named {
            let held = scan
                .addresses
                .iter()
                .any(|(a, id)| a.segment == *segment && !self.lost.contains(id));
            assert!(
                !held
                    || damage
                        .iter()
                        .any(|d| d.segment == *segment && (d.start..d.end).contains(at)),
                "seed {seed} round {round}: the flip at {at} of {segment} in round {flipped} is not named"
            );
        }
        self.report.named = self.report.named.max(self.named.len());
        for group in GROUPS {
            let mut handed = BTreeSet::new();
            loop {
                let taken = self.rt.block_on(node.handle.take(Take {
                    group: group.to_owned(),
                    selector: "a".to_owned(),
                    limit: 1_000,
                    lease_ms: LEASE,
                    wait: false,
                }));
                let Ok(Taken::Leased {
                    receipt, messages, ..
                }) = taken
                else {
                    break;
                };
                self.served(node, &messages, scan, live_flip);
                let ids: Vec<MessageId> = messages.iter().map(|d| d.handed.id).collect();
                handed.extend(ids.iter().copied());
                self.leases.push((group.to_owned(), receipt, ids));
            }
            for (address, id) in &scan.addresses {
                if self.lost.contains(id) || scan.messages[id].0 != "a" {
                    continue;
                }
                let key = (group.to_owned(), *address);
                let dead = scan.failures.get(&key).is_some_and(|c| *c >= MAX_FAILURES);
                if scan.acked.contains(&key) {
                    assert!(
                        !handed.contains(id) || self.lost_acks.contains(&key),
                        "seed {seed}: {id} handed to {group} after a durable ack"
                    );
                } else if !dead {
                    assert!(
                        handed.contains(id),
                        "seed {seed}: {id} not delivered to {group} after recovery"
                    );
                }
            }
        }
    }

    fn served(&mut self, node: &Node, messages: &[Delivery], scan: &Scan, live_flip: bool) {
        let seed = self.seed;
        let read = self.rt.block_on(node.handle.read(messages)).expect("read");
        for (delivery, message) in messages.iter().zip(read) {
            match message {
                Some(m) => {
                    if let Some((_, value)) = scan.messages.get(&m.id) {
                        assert_eq!(&m.value, value, "seed {seed}: a damaged value was served");
                    }
                    self.report.served += 1;
                }
                None => assert!(
                    live_flip,
                    "seed {seed}: {} has no readable record",
                    delivery.handed.id
                ),
            }
        }
    }

    fn publish(&mut self, node: &Node) -> JoinHandle<(u64, Option<Key>, Result<Published, Error>)> {
        self.tag += 1;
        let tag = self.tag;
        let count = self.rng.random_range(1..=4u32);
        let ttl = self.rng.random_bool(0.3);
        let stream = if ttl { "t" } else { "a" };
        let now = self.clock.now_ms();
        let messages: Vec<NewMessage> = (0..count)
            .map(|n| NewMessage {
                stream: stream.to_owned(),
                headers: Vec::new(),
                value: format!("{tag}:{n}:{count}").into_bytes(),
                timing: Timing {
                    delay_until: None,
                    expire_at: ttl.then_some(now + self.rng.random_range(1..20_000)),
                },
            })
            .collect();
        self.batches.insert(tag, count);
        if let Some(expiry) = messages.iter().filter_map(|m| m.timing.expire_at).max() {
            self.expiry.insert(tag, expiry);
        }
        let key = self.rng.random_bool(0.3).then(|| Key {
            scope: stream.to_owned(),
            key: format!("k{tag}"),
        });
        let handle = node.handle.clone();
        self.rt.spawn(async move {
            let result = handle.publish(messages, key.clone()).await;
            (tag, key, result)
        })
    }

    fn repeat(&mut self, node: &Node) {
        let seed = self.seed;
        for (key, (scope, tag, first)) in self.keys.clone() {
            self.tag += 1;
            self.batches.insert(self.tag, 1);
            let message = NewMessage {
                stream: scope.clone(),
                headers: Vec::new(),
                value: format!("{}:0:1", self.tag).into_bytes(),
                timing: Timing::default(),
            };
            let again = self
                .rt
                .block_on(node.handle.publish(
                    vec![message],
                    Some(Key {
                        scope: scope.clone(),
                        key: key.clone(),
                    }),
                ))
                .unwrap_or_else(|e| panic!("seed {seed}: repeat of {key}: {e}"));
            if !again.repeat {
                assert!(
                    self.lost_tags.contains(&tag) || self.lost_keys.contains(&key),
                    "seed {seed}: key {key} of a durable publish is not a repeat"
                );
                self.acked_publishes.insert(self.tag);
                self.published.insert(self.tag, ids_of(&again));
                self.keys.insert(key, (scope, self.tag, again));
                continue;
            }
            assert_eq!(
                (again.first, again.count),
                (first.first, first.count),
                "seed {seed}: key {key} repeat gives another reply"
            );
            self.repeated.insert(self.tag);
            self.report.repeats += 1;
        }
    }

    fn step(&mut self, node: &Node) {
        match self.rng.random_range(0..10) {
            0..3 => {
                let replies: Vec<_> = (0..self.rng.random_range(1..4))
                    .map(|_| self.publish(node))
                    .collect();
                for reply in replies {
                    let (tag, key, result) = self.rt.block_on(reply).expect("join");
                    if let Ok(published) = result {
                        self.acked_publishes.insert(tag);
                        self.published.insert(tag, ids_of(&published));
                        self.report.published += 1;
                        if let Some(Key { scope, key }) = key {
                            self.keys.insert(key, (scope, tag, published));
                        }
                    }
                }
            }
            3..5 => {
                let group = *GROUPS.choose(&mut self.rng).expect("a group");
                let taken = self.rt.block_on(node.handle.take(Take {
                    group: group.to_owned(),
                    selector: "a".to_owned(),
                    limit: self.rng.random_range(1..6),
                    lease_ms: LEASE,
                    wait: false,
                }));
                if let Ok(Taken::Leased {
                    receipt, messages, ..
                }) = taken
                {
                    let ids: Vec<MessageId> = messages.iter().map(|d| d.handed.id).collect();
                    for id in &ids {
                        let key = (group.to_owned(), *id);
                        assert!(
                            !self.acked.contains(&key) || self.lost_acked.contains(&key),
                            "seed {}: {id} handed to {group} again after a 200 ack",
                            self.seed
                        );
                    }
                    self.leases.push((group.to_owned(), receipt, ids));
                }
            }
            5..8 => self.settle(node),
            8 => {
                self.rt
                    .block_on(node.handle.take(Take {
                        group: "x".to_owned(),
                        selector: "t".to_owned(),
                        limit: 10,
                        lease_ms: LEASE,
                        wait: false,
                    }))
                    .ok();
            }
            _ => {
                let by = if self.rng.random_bool(0.2) {
                    IDLE_SEAL_MS + 1
                } else {
                    self.rng.random_range(1..5_000)
                };
                self.clock.advance(by);
                self.rt.block_on(node.handle.damage()).ok();
            }
        }
    }

    fn settle(&mut self, node: &Node) {
        if self.leases.is_empty() {
            return;
        }
        let at = self.rng.random_range(0..self.leases.len());
        let (group, receipt, ids) = self.leases.swap_remove(at);
        let mut settle = Settle::default();
        for id in &ids {
            match self.rng.random_range(0..10) {
                0..6 => settle.ack.push(*id),
                6..8 => settle.nack.push(Nack {
                    id: *id,
                    delay_ms: None,
                    error: None,
                }),
                8 => settle.dead.push(Bury {
                    id: *id,
                    error: None,
                }),
                _ => {}
            }
        }
        let mut acks = settle.ack.clone();
        acks.extend(settle.dead.iter().map(|b| b.id));
        let nacks: Vec<MessageId> = settle.nack.iter().map(|n| n.id).collect();
        self.settles += 1;
        for id in &acks {
            self.acks.attempt(&group, *id, self.settles);
        }
        let replied = self
            .rt
            .block_on(node.handle.settle(receipt, SettleBy::Ids(settle)))
            .is_ok();
        let synced = (!replied && !acks.is_empty()).then(|| {
            let fs = self.fs.snapshot().crash(Crash::LoseAll);
            fs.set_fault_rates(0, 0);
            Scan::read(&fs)
        });
        for id in &acks {
            if synced
                .as_ref()
                .is_none_or(|scan| scan.acked_by(&group, *id))
            {
                self.acks.landed(&group, *id, self.settles);
            }
        }
        if replied {
            self.report.acked += acks.len();
            self.acked
                .extend(acks.into_iter().map(|id| (group.clone(), id)));
            self.nacked
                .extend(nacks.into_iter().map(|id| (group.clone(), id)));
        }
    }

    fn round(&mut self, round: usize) {
        let scan = Scan::read(&self.fs);
        self.check(&scan, round);
        for _ in 0..self.rng.random_range(0..3) {
            if let Some((segment, at)) = self.flip(&scan) {
                self.named.push((segment, at, round));
            }
        }
        self.clock.advance(10 * 60_000);
        let node = self.boot();
        let live = self.rng.random_bool(0.3).then(|| self.flip(&scan));
        self.audit(&node, &scan, live.is_some(), round);
        self.named
            .extend(live.flatten().map(|(segment, at)| (segment, at, round)));
        self.repeat(&node);
        self.fs
            .set_fault_rates(self.rng.random_range(0..20), self.rng.random_range(0..5));
        if self.rng.random_bool(0.5) {
            self.fs.set_read_fault_rate(self.rng.random_range(1..100));
        }
        if self.rng.random_bool(0.6) {
            self.fs.freeze_after(self.rng.random_range(5..300));
        }
        for _ in 0..STEPS {
            self.step(&node);
        }
        self.leases.clear();
        let mode = match self.rng.random_range(0..10) {
            0..6 => Crash::Random,
            6..8 => Crash::LoseAll,
            _ => Crash::Kill,
        };
        self.fs = self.fs.crash(mode);
        self.fs.set_fault_rates(0, 0);
        self.report.crashes += 1;
        drop(node.handle);
        self.rt.block_on(node.task).expect("engine task").ok();
    }
}

pub fn run(seed: u64) -> Report {
    let mut sim = Sim::new(seed);
    for round in 0..ROUNDS {
        sim.round(round);
    }
    let scan = Scan::read(&sim.fs);
    sim.check(&scan, ROUNDS);
    sim.report.dead = scan.dead.len();
    sim.report
}

pub fn batch_atomic(seed: u64) -> usize {
    let mut sim = Sim::new(seed);
    let mut checked = 0;
    for round in 0..ROUNDS {
        let node = sim.boot();
        sim.fs.freeze_after(sim.rng.random_range(3..60));
        let replies: Vec<_> = (0..sim.rng.random_range(1..8))
            .map(|_| {
                sim.tag += 1;
                let (tag, count) = (sim.tag, sim.rng.random_range(2..=50u32));
                sim.batches.insert(tag, count);
                let messages = (0..count)
                    .map(|n| NewMessage {
                        stream: "a".to_owned(),
                        headers: Vec::new(),
                        value: format!("{tag}:{n}:{count}").into_bytes(),
                        timing: Timing::default(),
                    })
                    .collect();
                let handle = node.handle.clone();
                sim.rt
                    .spawn(async move { (tag, handle.publish(messages, None).await.is_ok()) })
            })
            .collect();
        for reply in replies {
            let (tag, ok) = sim.rt.block_on(reply).expect("join");
            if ok {
                sim.acked_publishes.insert(tag);
            }
        }
        sim.fs = sim.fs.crash(if sim.rng.random_bool(0.5) {
            Crash::Random
        } else {
            Crash::LoseAll
        });
        drop(node.handle);
        sim.rt.block_on(node.task).expect("engine task").ok();
        let scan = Scan::read(&sim.fs);
        sim.check(&scan, round);
        checked += scan.messages.len();
    }
    checked
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(counter: u16) -> MessageId {
        MessageId::new(1_000, 1, counter).expect("an id")
    }

    #[test]
    fn a_message_acked_by_h_while_g_had_only_a_failed_ack_is_removable() {
        let (m, x) = (id(0), id(1));
        let mut acks = Acks::default();
        acks.attempt("g", x, 2);
        acks.attempt("h", m, 5);
        acks.landed("h", m, 5);
        acks.attempt("g", x, 10);
        acks.landed("g", x, 10);
        assert!(acks.removable(m, &|_| false, &BTreeSet::new()));
    }

    #[test]
    fn a_failed_ack_of_g_does_not_make_a_message_removable_once_g_has_acked() {
        let (m, x) = (id(0), id(1));
        let mut acks = Acks::default();
        acks.attempt("g", x, 3);
        acks.landed("g", x, 3);
        acks.attempt("h", m, 5);
        acks.landed("h", m, 5);
        acks.attempt("g", m, 6);
        assert!(!acks.removable(m, &|_| false, &BTreeSet::new()));
    }
}
