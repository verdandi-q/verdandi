use std::collections::{BTreeMap, BTreeSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use verdandi_node::*;
pub use verdandi_node::{Cause, FENCE_MS, ISR_MS, STEP_MS};

use rand::seq::{IndexedRandom, SliceRandom};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use verdandi_control::{Control, Meta, Saved};
use verdandi_core::cluster::controller::{
    self, Controller, Floor, QUIET_PERIOD, START, confirmed, refuse,
};
use verdandi_core::cluster::placement::{Groups, HolderSet};
use verdandi_core::cluster::repair::{self, DIGEST_MS, Limiter};
use verdandi_core::cluster::view::{
    Body, FAIL_MS, GOSSIP_MS, JOIN_MS, LIFECYCLE, REBALANCE, SAMPLE_MS, SIGTERM, SPOT,
    STOP_BUDGET_MS, State, Update, View,
};

pub const SEGMENT_BYTES: u64 = 64 << 20;
pub const HOUR_MS: u64 = 3_600_000;
const FANOUT: usize = 3;
const HELLO_MS: u64 = 10_000;
const FOUNDING_STEPS: u64 = 600;
const FLAP_MS: u64 = 13_500;
const SLACK_MS: u64 = FAIL_MS + 2 * SAMPLE_MS + GOSSIP_MS;
const STALL_WINDOW_MS: u64 = 60_000;

#[derive(Clone, Copy, Debug)]
pub struct FloorChange {
    pub run_ms: u64,
    pub disk: u64,
    pub from: Floor,
    pub to: Floor,
    pub cause: Cause,
    pub debt_ok: bool,
    pub majority: bool,
    pub counts_ok: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LossKind {
    Kill,
    Left,
    Signal,
}

#[derive(Clone, Debug)]
pub struct WorldLoss {
    pub disk: u64,
    pub run_ms: u64,
    pub kind: LossKind,
    pub side: BTreeSet<u64>,
}

#[derive(Clone, Debug)]
pub struct HolderChange {
    pub run_ms: u64,
    pub from: HolderSet,
    pub to: HolderSet,
    pub copies: BTreeSet<u64>,
    pub current: bool,
    pub least: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct CountChange {
    pub now_ms: u64,
    pub run_ms: u64,
    pub disk: u64,
    pub from: u8,
    pub to: u8,
}

struct Frame {
    at: u64,
    sent: u64,
    from: u64,
    to: u64,
    version: u64,
    msg: Msg,
}

struct Disk {
    restart: u64,
    floor: Floor,
    committed: u16,
    alive_ms: u64,
    zone: String,
    join_ms: u64,
    member: Option<Vec<u8>>,
    control: Arc<Saved>,
    store: Store,
    dropped: BTreeMap<u64, u64>,
    epochs: BTreeMap<u64, u64>,
}

pub struct Segment {
    pub writer: u64,
    pub holders: HolderSet,
    pub bytes: u64,
    pub format: u16,
    pub born_run_ms: u64,
    pub born_ms: u64,
    pub lost_run_ms: u64,
    pub marks: BTreeMap<u64, u64>,
    pub born: BTreeSet<u64>,
    pub stream: u64,
    copied: BTreeMap<u64, u64>,
}

pub struct World {
    pub rng: ChaCha8Rng,
    pub now_ms: u64,
    pub run_ms: u64,
    pub drop: f64,
    pub loss: f64,
    pub slow_ms: u64,
    pub link_ms: u64,
    pub delay_ms: u64,
    pub cuts: BTreeSet<(u64, u64)>,
    pub oneway: BTreeSet<(u64, u64)>,
    pub held_repair: BTreeSet<u64>,
    pub open: BTreeSet<u64>,
    pub pairs: BTreeSet<(u64, u64)>,
    pub capacity: BTreeMap<u64, u64>,
    pub drained: BTreeSet<u64>,
    pub segments: BTreeMap<u64, Segment>,
    pub acked: BTreeSet<u64>,
    pub settled: BTreeMap<u64, u64>,
    pub floor_changes: Vec<FloorChange>,
    pub count_changes: Vec<CountChange>,
    pub refused: u64,
    pub losses: Vec<WorldLoss>,
    pub boots: Vec<(u64, u64)>,
    pub holder_changes: Vec<HolderChange>,
    pub zones_known: bool,
    pub headroom: f64,
    pub signals: BTreeMap<u64, (u64, String)>,
    pub splits: u64,
    pub merges: u64,
    pub deleted: Vec<(u64, u64)>,
    pub dropped: BTreeSet<u64>,
    pub drops_done: BTreeSet<u64>,
    pub warnings: BTreeSet<u64>,
    pub wiped: Vec<(u64, u64)>,
    pub named_lost: BTreeSet<u64>,
    pub refused_writes: u64,
    pub redelivered: u64,
    pub double_leaders: u64,
    pub shown: u64,
    pub shown_warned: u64,
    pub missed_gossip: u64,
    pub resealed: u64,
    pub two_lists: u64,
    pub raft_refused: u64,
    pub asks_doubled: u64,
    pub records_max: usize,
    pub record_frames: u64,
    pub seg_asks: u64,
    pub settle_tries: u64,
    pub partial_reads: u64,
    pub incomplete_reads: u64,
    pub refused_settles: u64,
    pub commit_ms: u64,
    pub stopped_at: BTreeMap<u64, u64>,
    pub removed_at: BTreeMap<u64, u64>,
    pub exits: BTreeMap<u64, u64>,
    pub records: Vec<(u64, u64)>,
    pub done: Vec<(u64, u64, u64)>,
    pub drain_commits: Vec<(u64, u64)>,
    open_requests: BTreeMap<u64, u64>,
    drop_waits: BTreeMap<u64, (u64, u64)>,
    drop_meta: BTreeMap<u64, u64>,
    next_request: u64,
    wishes: BTreeMap<(u64, u64, u64), (u64, HolderSet)>,
    cut_ms: BTreeMap<(u64, u64), u64>,
    linked_ms: BTreeMap<(u64, u64), u64>,
    applied_ms: BTreeMap<(u64, u64), u64>,
    terms: BTreeMap<u64, u64>,
    applied_log: BTreeMap<u64, u64>,
    pub watch: bool,
    pub worst_stall: u64,
    stalled: BTreeMap<(u64, u64), Vec<u64>>,
    disks: BTreeMap<u64, Disk>,
    nodes: BTreeMap<u64, Node>,
    sides: BTreeMap<u64, bool>,
    queue: Vec<Frame>,
    copying: BTreeMap<(u64, u64), u64>,
    sends: BTreeMap<u64, Limiter>,
    takes: BTreeMap<u64, Limiter>,
    queued: Vec<Job>,
    dirty: bool,
    next_disk: u64,
    next_segment: u64,
    changed_run_ms: u64,
    decisions: BTreeMap<Floor, FloorChange>,
    cursor: usize,
}

impl World {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: ChaCha8Rng::seed_from_u64(seed),
            now_ms: 0,
            run_ms: 0,
            drop: 0.0,
            loss: 0.0,
            slow_ms: 0,
            link_ms: 200,
            delay_ms: 200,
            cuts: BTreeSet::new(),
            oneway: BTreeSet::new(),
            held_repair: BTreeSet::new(),
            open: BTreeSet::new(),
            pairs: BTreeSet::new(),
            capacity: BTreeMap::new(),
            drained: BTreeSet::new(),
            segments: BTreeMap::new(),
            acked: BTreeSet::new(),
            settled: BTreeMap::new(),
            floor_changes: Vec::new(),
            count_changes: Vec::new(),
            refused: 0,
            losses: Vec::new(),
            boots: Vec::new(),
            holder_changes: Vec::new(),
            zones_known: true,
            headroom: 1.0,
            signals: BTreeMap::new(),
            splits: 0,
            merges: 0,
            deleted: Vec::new(),
            dropped: BTreeSet::new(),
            drops_done: BTreeSet::new(),
            warnings: BTreeSet::new(),
            wiped: Vec::new(),
            named_lost: BTreeSet::new(),
            refused_writes: 0,
            redelivered: 0,
            double_leaders: 0,
            shown: 0,
            shown_warned: 0,
            missed_gossip: 0,
            resealed: 0,
            two_lists: 0,
            raft_refused: 0,
            asks_doubled: 0,
            records_max: 0,
            record_frames: 0,
            seg_asks: 0,
            settle_tries: 0,
            partial_reads: 0,
            incomplete_reads: 0,
            refused_settles: 0,
            commit_ms: 0,
            stopped_at: BTreeMap::new(),
            removed_at: BTreeMap::new(),
            exits: BTreeMap::new(),
            records: Vec::new(),
            done: Vec::new(),
            drain_commits: Vec::new(),
            open_requests: BTreeMap::new(),
            drop_waits: BTreeMap::new(),
            drop_meta: BTreeMap::new(),
            next_request: 1,
            wishes: BTreeMap::new(),
            cut_ms: BTreeMap::new(),
            linked_ms: BTreeMap::new(),
            applied_ms: BTreeMap::new(),
            terms: BTreeMap::new(),
            applied_log: BTreeMap::new(),
            watch: false,
            worst_stall: 0,
            stalled: BTreeMap::new(),
            disks: BTreeMap::new(),
            nodes: BTreeMap::new(),
            sides: BTreeMap::new(),
            queue: Vec::new(),
            copying: BTreeMap::new(),
            sends: BTreeMap::new(),
            takes: BTreeMap::new(),
            queued: Vec::new(),
            dirty: true,
            next_disk: 1,
            next_segment: 1,
            changed_run_ms: 0,
            decisions: BTreeMap::new(),
            cursor: 0,
        }
    }

    pub fn running(&self) -> Vec<u64> {
        self.nodes.keys().copied().collect()
    }

    pub fn disks(&self) -> Vec<u64> {
        self.disks.keys().copied().collect()
    }

    pub fn floor(&self, disk: u64) -> Option<Floor> {
        self.disks.get(&disk).map(|d| d.floor)
    }

    pub fn count(&self, disk: u64) -> Option<u8> {
        self.nodes.get(&disk).map(|n| n.ctl.count)
    }

    pub fn committed(&self, disk: u64) -> Option<u16> {
        self.disks.get(&disk).map(|d| d.committed)
    }

    pub fn leaders(&self, id: u64) -> Vec<u64> {
        self.running()
            .into_iter()
            .filter(|n| self.leads(*n, id))
            .collect()
    }

    pub fn meta(&self, disk: u64) -> Option<&Meta> {
        self.nodes.get(&disk).map(|n| n.raft.meta())
    }

    pub fn newest(&self) -> Option<&Meta> {
        self.disks
            .values()
            .map(|d| &d.control)
            .max_by_key(|c| c.applied)
            .map(|c| &c.meta)
    }

    pub fn raft_leader(&self) -> Option<u64> {
        self.nodes
            .iter()
            .filter(|(_, n)| n.raft.is_leader())
            .max_by_key(|(_, n)| n.raft.term())
            .map(|(d, _)| *d)
    }

    pub fn in_list(&self, disk: u64) -> bool {
        self.newest().is_some_and(|m| m.members.contains(&disk))
    }

    pub fn adopted_ms(&self, disk: u64) -> Option<u64> {
        self.nodes.get(&disk).map(|n| n.adopted_ms)
    }

    pub fn joined(&self, disk: u64) -> bool {
        self.nodes.get(&disk).is_some_and(|n| n.joined)
    }

    pub fn missed_records(&self) -> usize {
        self.nodes
            .iter()
            .filter(|(d, n)| n.reconciled && led(n, **d, self.now_ms, HEARD_MS))
            .map(|(d, _)| {
                self.records
                    .iter()
                    .filter(|(origin, seq)| {
                        let disk = &self.disks[d];
                        !disk.store.records.contains_key(&(*origin, *seq))
                            && disk.store.marks.get(origin).copied().unwrap_or(0) < *seq
                    })
                    .count()
            })
            .sum()
    }

    pub fn reconciled(&self, disk: u64) -> bool {
        self.nodes.get(&disk).is_some_and(|n| n.reconciled)
    }

    pub fn reconciling(&self, disk: u64) -> bool {
        self.nodes
            .get(&disk)
            .is_some_and(|n| n.reconcile_ms.is_some())
    }

    pub fn groups_epoch(&self, disk: u64) -> Option<u64> {
        groups_of(self.nodes.get(&disk)?).map(|g| g.epoch)
    }

    pub fn is_groups_writer(&self, disk: u64) -> bool {
        self.nodes.get(&disk).is_some_and(|n| n.raft.is_leader())
    }

    fn majority(&self, h: u64) -> bool {
        self.nodes.contains_key(&h)
            && self
                .nodes
                .values()
                .map(|n| n.raft.meta())
                .max_by_key(|m| m.version)
                .is_some_and(|m| {
                    m.members.contains(&h)
                        && confirmed(
                            m.members.iter().filter(|x| self.link(h, **x)).count(),
                            m.members.len(),
                        )
                        && self.raft_leader().is_none_or(|l| self.link(h, l))
                })
    }

    pub fn stalls(&self) -> (BTreeSet<(u64, u64)>, BTreeSet<u64>) {
        let running = self.running();
        let mut unled = BTreeSet::new();
        for id in self.acked.iter().filter(|id| !self.dropped.contains(id)) {
            let seg = &self.segments[id];
            let meta = self.newest().and_then(|m| m.segments.get(id));
            let holders: Vec<u64> = meta
                .iter()
                .flat_map(|m| &m.holders.holders)
                .copied()
                .filter(|h| self.nodes.contains_key(h) && seg.marks.contains_key(h))
                .collect();
            let calm = |h: u64| {
                holders.iter().all(|o| {
                    self.cut_ms
                        .get(&pair(h, *o))
                        .is_none_or(|t| self.now_ms >= t + JOIN_MS)
                })
            };
            let ready: Vec<u64> = holders
                .iter()
                .copied()
                .filter(|h| self.majority(*h) && calm(*h))
                .collect();
            let isr = meta.map(|m| m.isr.clone()).unwrap_or_default();
            let ready: Vec<u64> = ready.into_iter().filter(|h| isr.contains(h)).collect();
            let alone = holders.len() < usize::from(controller::KEYED_MIN_COPIES);
            if !alone && !ready.is_empty() && self.leaders(*id).is_empty() {
                unled.extend(ready.into_iter().map(|h| (*id, h)));
            }
        }
        let unreconciled = running
            .iter()
            .copied()
            .filter(|d| self.majority(*d) && !self.nodes[d].reconciled)
            .collect();
        (unled, unreconciled)
    }

    pub fn leaderless(&self) -> Vec<u64> {
        let Some(m) = self.newest() else {
            return Vec::new();
        };
        m.segments
            .iter()
            .filter(|(id, s)| {
                !self.dropped.contains(id)
                    && s.isr.is_disjoint(&m.members)
                    && s.holders.holders.iter().any(|h| {
                        self.nodes.get(h).is_some_and(|n| n.reconciled)
                            && self
                                .segments
                                .get(id)
                                .is_some_and(|g| g.marks.contains_key(h))
                    })
            })
            .map(|(id, _)| *id)
            .collect()
    }

    pub fn marked_absent(&self, by: u64, disk: u64) -> bool {
        self.nodes.get(&by).is_some_and(|n| {
            n.view
                .entry(disk)
                .is_some_and(|e| e.head.state != State::Alive)
        })
    }

    pub fn create(&mut self, zone: &str, alive_ms: u64, format_max: u16) -> u64 {
        let disk = self.next_disk;
        self.next_disk += 1;
        self.disks.insert(
            disk,
            Disk {
                restart: 0,
                floor: START,
                committed: 1,
                alive_ms,
                zone: zone.to_owned(),
                join_ms: self.run_ms,
                member: None,
                control: Arc::new(Saved::default()),
                store: Store::default(),
                dropped: BTreeMap::new(),
                epochs: BTreeMap::new(),
            },
        );
        if !self.sides.is_empty() {
            let side = self.rng.random_bool(0.5);
            self.sides.insert(disk, side);
            self.cut(&self.crossing());
        }
        self.boot(disk, format_max);
        disk
    }

    pub fn start(&mut self, n: usize, zones: usize, alive_ms: u64) -> Vec<u64> {
        let names = ["a", "b", "c", "d"];
        let ids: Vec<u64> = (0..n)
            .map(|i| {
                let zone = if zones == 0 { "" } else { names[i % zones] };
                self.create(zone, alive_ms, 1)
            })
            .collect();
        if let Some(first) = ids.first().copied() {
            let found = Control::found(first).expect("the founding state restores");
            let saved = found.save().expect("the founding state saves");
            if let (Some(node), Some(disk)) =
                (self.nodes.get_mut(&first), self.disks.get_mut(&first))
            {
                node.raft = found;
                disk.control = Arc::new(saved);
                node.saved = disk.control.clone();
            }
            let all: BTreeSet<u64> = ids.iter().copied().collect();
            for _ in 0..FOUNDING_STEPS {
                if self.newest().is_some_and(|m| m.members == all) && self.grouped() {
                    break;
                }
                self.advance(STEP_MS);
            }
            for node in self.nodes.values_mut() {
                node.reconciled = false;
                node.reconcile_ms = None;
            }
        }
        ids
    }

    fn body(&self, disk: u64, format_max: u16) -> Body {
        let d = &self.disks[&disk];
        let (host, prefix) = self.signals.get(&disk).cloned().unwrap_or_else(|| {
            let net = if self.zones_known {
                disk.to_string()
            } else {
                d.zone.clone()
            };
            (disk, format!("10.{net}.0.0/24"))
        });
        Body {
            address: format!("10.0.{}.{}:8080", disk / 250, disk % 250),
            join_ms: d.join_ms,
            alive_ms: d.alive_ms,
            format_max,
            committed: d.committed,
            zone: if self.zones_known {
                d.zone.clone()
            } else {
                String::new()
            },
            host,
            prefix,
            holds: self.holds(disk),
            ..Body::default()
        }
    }

    pub fn boot(&mut self, disk: u64, format_max: u16) -> bool {
        let Some(d) = self.disks.get_mut(&disk) else {
            return false;
        };
        if refuse(d.committed, format_max) {
            self.refused += 1;
            return false;
        }
        d.restart += 1;
        let (restart, floor, committed, run) = (d.restart, d.floor, d.committed, d.alive_ms);
        let member = d.member.clone();
        let raft =
            Control::restore(disk, (*d.control).clone()).expect("the saved control state restores");
        let body = self.body(disk, format_max);
        let phase_ms = self.rng.random_range(0..SAMPLE_MS / STEP_MS) * STEP_MS;
        let (view, count) = match member {
            None => (View::new(disk, restart, body, self.now_ms, run), 0),
            Some(bytes) => View::restored(disk, restart, body, self.now_ms, run, &bytes)
                .expect("the saved member state decodes"),
        };
        let mut ctl = Controller::new(true, floor, committed);
        ctl.count = count;
        let mut node = Node::new(
            disk,
            self.now_ms,
            run,
            format_max,
            phase_ms,
            view,
            ctl,
            raft,
            self.disks[&disk].control.clone(),
            self.disks[&disk].store.clone(),
        );
        node.headroom = self.headroom;
        node.epochs = self.disks[&disk].epochs.clone();
        node.dropped = self.disks[&disk].dropped.clone();
        for (id, s) in self
            .segments
            .iter()
            .filter(|(_, s)| s.marks.contains_key(&disk))
        {
            node.writers.insert(*id, s.writer);
            node.marks.insert(*id, s.marks[&disk]);
            node.born.insert(*id, s.born_ms);
            if s.holders.holders.contains(&disk) {
                node.named
                    .extend(s.holders.holders.iter().map(|h| ((*id, *h), s.born_ms)));
            }
        }
        self.nodes.insert(disk, node);
        self.sends.insert(disk, Limiter::new(self.now_ms));
        self.takes.insert(disk, Limiter::new(self.now_ms));
        self.boots.push((disk, self.run_ms));
        self.changed_run_ms = self.run_ms;
        let seeds: Vec<u64> = self
            .nodes
            .keys()
            .copied()
            .filter(|p| *p != disk && self.link(disk, *p))
            .collect();
        if let Some(peer) = seeds.choose(&mut self.rng).copied() {
            let hello = self.nodes[&peer]
                .view
                .answer(&self.nodes[&disk].view.digest());
            let now = self.now_ms;
            if let Some(node) = self.nodes.get_mut(&disk) {
                node.view.merge(&hello, now, run);
                let seen = node
                    .view
                    .live()
                    .map(|(_, e)| e.body.committed)
                    .max()
                    .unwrap_or(0);
                if refuse(seen, format_max) {
                    self.nodes.remove(&disk);
                    self.refused += 1;
                    return false;
                }
            }
            self.exchange(disk, peer);
        }
        let ids: Vec<u64> = self
            .segments
            .iter()
            .filter(|(id, s)| {
                s.marks.contains_key(&disk)
                    || self
                        .set_of(disk, **id)
                        .is_some_and(|set| set.holders.contains(&disk))
            })
            .map(|(id, _)| *id)
            .collect();
        if let Some(node) = self.nodes.get_mut(&disk) {
            node.booting = ids.iter().copied().collect();
        }
        for id in ids {
            let now = self.now_ms;
            if let Some(node) = self.nodes.get_mut(&disk) {
                node.catch_up(now, id);
            }
        }
        self.drained.remove(&disk);
        true
    }

    pub fn lose_copy(&mut self, disk: u64, id: u64) {
        self.set_mark(disk, id, None);
    }

    fn set_mark(&mut self, n: u64, id: u64, mark: Option<u64>) {
        if let Some(seg) = self.segments.get_mut(&id) {
            match mark {
                Some(m) => seg.marks.insert(n, m),
                None => seg.marks.remove(&n),
            };
        }
        if let Some(node) = self.nodes.get_mut(&n) {
            match mark {
                Some(m) => node.marks.insert(id, m),
                None => node.marks.remove(&id),
            };
        }
    }

    fn record(&mut self, disk: u64, kind: LossKind) {
        if self.holds(disk) > 0 && !self.drained.contains(&disk) {
            self.losses.push(WorldLoss {
                disk,
                run_ms: self.run_ms,
                kind,
                side: self.side(disk),
            });
        }
    }

    pub fn copying_to(&self, id: u64) -> bool {
        self.copying.keys().any(|(segment, _)| *segment == id)
    }

    pub fn kill(&mut self, disk: u64) {
        self.record(disk, LossKind::Kill);
        self.down(disk);
    }

    fn down(&mut self, disk: u64) {
        if let Some(node) = self.nodes.remove(&disk) {
            self.refused_settles += node.settles.len() as u64;
        }
        self.copying.retain(|(_, to), _| *to != disk);
        self.changed_run_ms = self.run_ms;
        for segment in self.segments.values_mut() {
            if segment.marks.contains_key(&disk) {
                segment.lost_run_ms = self.run_ms;
            }
        }
    }

    pub fn cut(&mut self, pairs: &[(u64, u64)]) {
        self.cuts.extend(pairs.iter().copied());
        self.changed_run_ms = self.run_ms;
    }

    fn crossing(&self) -> Vec<(u64, u64)> {
        self.sides
            .iter()
            .flat_map(|(a, x)| {
                self.sides
                    .iter()
                    .filter(move |(_, y)| x != *y)
                    .map(move |(b, _)| pair(*a, *b))
            })
            .collect()
    }

    pub fn part(&mut self, minority: &[u64]) {
        self.sides = self
            .disks
            .keys()
            .map(|d| (*d, minority.contains(d)))
            .collect();
        self.cut(&self.crossing());
    }

    pub fn heal(&mut self) {
        for cut in self.crossing() {
            self.cuts.remove(&cut);
        }
        self.sides.clear();
        self.oneway.clear();
    }

    pub fn knows(&self, by: u64, disk: u64) -> bool {
        self.nodes
            .get(&by)
            .is_some_and(|n| n.view.entry(disk).is_some())
    }

    pub fn delete(&mut self, disk: u64) {
        self.kill(disk);
        self.disks.remove(&disk);
        for segment in self.segments.values_mut() {
            segment.marks.remove(&disk);
        }
    }

    fn step_node(&mut self, disk: u64, now: u64, cmds: Vec<Cmd>) -> Vec<Effect> {
        let mut effects = Vec::new();
        for cmd in cmds {
            match cmd {
                Cmd::Signal(signal) => {
                    if signal != SIGTERM {
                        self.record(disk, LossKind::Signal);
                    }
                    effects.extend(self.run_node(disk, Input::cmd(now, self.run_of(disk), cmd)));
                }
                Cmd::Drain => {
                    self.drained.insert(disk);
                    effects.extend(self.run_node(disk, Input::cmd(now, self.run_of(disk), cmd)));
                }
                cmd => effects.extend(self.run_node(disk, Input::cmd(now, self.run_of(disk), cmd))),
            }
        }
        effects
    }

    fn run_node(&mut self, disk: u64, mut input: Input) -> Vec<Effect> {
        input.pairs = self.pairs.clone();
        let Some(mut node) = self.nodes.remove(&disk) else {
            return Vec::new();
        };
        let out = node.step(input, &mut |to, msg, version| {
            self.send_at(disk, to, msg, version)
        });
        self.nodes.insert(disk, node);
        self.absorb(disk, &out.effects);
        for (to, msg) in out.frames {
            self.send(disk, to, msg);
        }
        let mut effects = out.effects;
        for effect in &mut effects {
            if let Effect::Wipe {
                members,
                kept,
                erased,
            } = effect
            {
                self.wipe(disk, members, std::mem::take(kept), std::mem::take(erased));
            }
        }
        effects
    }

    fn cmd(&mut self, disk: u64, cmd: Cmd) -> Vec<Effect> {
        let input = Input::cmd(self.now_ms, self.run_of(disk), cmd);
        self.run_node(disk, input)
    }

    fn absorb(&mut self, disk: u64, effects: &[Effect]) {
        for effect in effects {
            match effect {
                Effect::Refused => self.refused_settles += 1,
                Effect::RaftRefused => self.raft_refused += 1,
                Effect::DropDone { id } => {
                    self.drop_waits.remove(id);
                    self.drops_done.insert(*id);
                }
                Effect::Settled {
                    id,
                    next,
                    started_ms,
                } => {
                    let settled = self.settled.entry(*id).or_insert(*next);
                    *settled = (*settled).max(*next);
                    self.applied_ms.insert((*id, disk), *started_ms);
                }
                Effect::Mark { id, mark } => {
                    if let Some(seg) = self.segments.get_mut(id) {
                        seg.marks.insert(disk, *mark);
                    }
                }
                Effect::Unmark { id } => {
                    if let Some(seg) = self.segments.get_mut(id) {
                        seg.marks.remove(&disk);
                    }
                }
                Effect::Epoch { id, epoch } => {
                    if let Some(d) = self.disks.get_mut(&disk) {
                        let held = d.epochs.entry(*id).or_insert(0);
                        *held = (*held).max(*epoch);
                    }
                }
                Effect::Replicated { id, sent } => {
                    self.applied_ms.insert((*id, disk), *sent);
                }
                Effect::Dropped { id, run } => {
                    if let Some(d) = self.disks.get_mut(&disk) {
                        d.dropped.insert(*id, *run);
                    }
                    self.copying.retain(|(s, _), _| s != id);
                }
                Effect::Wish { by, set } => self.wish(*by, set.clone()),
                Effect::Led { term } => {
                    if *self.terms.entry(*term).or_insert(disk) != disk {
                        self.double_leaders += 1;
                    }
                }
                Effect::Tomb { id, run } => {
                    if let Some(d) = self.disks.get_mut(&disk) {
                        d.dropped.insert(*id, *run);
                    }
                }
                Effect::RefusedWrite { resealed } => {
                    self.refused_writes += 1;
                    self.resealed += u64::from(*resealed);
                }
                Effect::Stopped => {
                    self.stopped_at.entry(disk).or_insert(self.now_ms);
                }
                Effect::WishDone { key } => {
                    self.wishes.retain(|k, _| (k.0, k.1) != *key);
                }
                Effect::DrainCommit => self.drain_commits.push((disk, self.run_ms)),
                Effect::Decided { from, to, cause } => self.decide(disk, *from, *to, *cause),
                Effect::Saved(control) => self.saved(disk, control.clone()),
                Effect::Persist(store) => {
                    if let Some(d) = self.disks.get_mut(&disk) {
                        d.store = store.clone();
                    }
                }
                Effect::Expired { alive } => {
                    if let Some(d) = self.disks.get_mut(&disk) {
                        d.dropped.retain(|_, at| !expired(*at, *alive));
                    }
                }
                Effect::RequestGone { id } => {
                    self.open_requests.remove(id);
                }
                Effect::RequestDone {
                    id,
                    key,
                    stream,
                    upto,
                } => {
                    self.open_requests.remove(id);
                    self.deleted.push((*stream, *upto));
                    self.records.push(*key);
                    self.done.push((*id, disk, self.now_ms));
                }
                _ => {}
            }
        }
    }

    fn decide(&mut self, disk: u64, from: Floor, to: Floor, cause: Cause) {
        let (run, settled) = (self.run_ms, self.run_ms >= self.changed_run_ms + SLACK_MS);
        let side = self.side(disk);
        let (debt_ok, majority, counts_ok) = match cause {
            Cause::Rise | Cause::Quiet => (
                self.debt_truth(to.value, &side),
                self.listed(disk),
                !settled || self.counts_ok(&side, to.value),
            ),
            _ => (true, true, true),
        };
        self.decisions.insert(
            to,
            FloorChange {
                run_ms: run,
                disk,
                from,
                to,
                cause,
                debt_ok,
                majority,
                counts_ok,
            },
        );
    }

    fn saved(&mut self, disk: u64, control: Arc<Saved>) {
        let (now, run) = (self.now_ms, self.run_ms);
        let Some(d) = self.disks.get_mut(&disk) else {
            return;
        };
        let old = std::mem::replace(&mut d.control, control.clone());
        if control.meta.floor != old.meta.floor {
            let (from, to) = (old.meta.floor, control.meta.floor);
            d.floor = to;
            let change = match self.decisions.remove(&to) {
                Some(made) => FloorChange { disk, from, ..made },
                None => FloorChange {
                    run_ms: run,
                    disk,
                    from,
                    to,
                    cause: Cause::Merge,
                    debt_ok: true,
                    majority: true,
                    counts_ok: true,
                },
            };
            self.floor_changes.push(change);
        }
        for entry in old
            .entries
            .iter()
            .filter(|e| e.index <= control.snapshot.get_metadata().index)
            .chain(control.entries.iter())
            .filter(|e| e.index > old.applied && e.index <= control.applied)
        {
            let mut h = DefaultHasher::new();
            (entry.term, entry.data.as_ref(), entry.context.as_ref()).hash(&mut h);
            let hash = h.finish();
            if *self.applied_log.entry(entry.index).or_insert(hash) != hash {
                self.two_lists += 1;
            }
        }
        if control.meta.version == old.meta.version {
            return;
        }
        self.commit_ms = now;
        for set in control.meta.segments.values().map(|s| &s.holders) {
            let before = old.meta.segments.get(&set.segment).map(|b| &b.holders);
            if before == Some(set) {
                continue;
            }
            let Some(seg) = self.segments.get_mut(&set.segment) else {
                continue;
            };
            if seg.holders.epoch < set.epoch {
                let from = std::mem::replace(&mut seg.holders, set.clone());
                self.log(from, set.clone());
            }
        }
        for d in old.meta.members.difference(&control.meta.members) {
            self.removed_at.entry(*d).or_insert(now);
        }
    }

    fn each(&mut self, cmd: Cmd) {
        for disk in self.running() {
            self.cmd(disk, cmd.clone());
        }
    }

    fn partial(&self, disk: u64, id: u64) -> bool {
        self.copying.contains_key(&(id, disk))
    }

    pub fn signal(&mut self, disk: u64, signal: u8) {
        self.step_node(disk, self.now_ms, vec![Cmd::Signal(signal)]);
    }

    pub fn stop(&mut self, disk: u64) {
        self.step_node(disk, self.now_ms, vec![Cmd::Stop]);
    }

    pub fn drain(&mut self, disk: u64) {
        self.step_node(disk, self.now_ms, vec![Cmd::Drain]);
    }

    pub fn take(&mut self) {
        self.do_take();
    }

    pub fn read(&mut self) {
        self.do_read();
    }

    pub fn settle(&mut self) {
        let candidates: Vec<u64> = self
            .acked
            .iter()
            .copied()
            .filter(|s| {
                self.segments
                    .get(s)
                    .is_some_and(|seg| self.live_copies(seg) > 0)
            })
            .collect();
        let Some(id) = candidates.choose(&mut self.rng).copied() else {
            return;
        };
        let leaders: Vec<u64> = self
            .running()
            .into_iter()
            .filter(|n| self.leads(*n, id) && self.live_reconciled(*n, *n))
            .collect();
        let Some(leader) = leaders.choose(&mut self.rng).copied() else {
            return;
        };
        self.settle_tries += 1;
        let sid = self.next_request;
        self.next_request += 1;
        self.step_node(leader, self.now_ms, vec![Cmd::Settle { id, sid }]);
    }

    fn remaining(&self, disk: u64) -> usize {
        let others = self
            .nodes
            .keys()
            .filter(|h| **h != disk && self.serving(**h))
            .count();
        let floor = self
            .nodes
            .get(&disk)
            .map_or(0, |n| usize::from(n.ctl.floor.value))
            .min(others);
        self.segments
            .values()
            .filter(|s| s.marks.contains_key(&disk))
            .filter(|s| {
                s.marks
                    .keys()
                    .filter(|h| **h != disk && self.serving(**h))
                    .count()
                    < floor
            })
            .count()
    }

    fn leave(&mut self, disk: u64) {
        self.sample(disk);
        let remaining = self.remaining(disk);
        let drained = self.drained.contains(&disk);
        if remaining > 0 && !drained {
            self.record(disk, LossKind::Left);
        }
        let now = self.now_ms;
        let cmd = Cmd::Leave {
            remaining: remaining as u32,
        };
        let Some(Effect::Left { restart, own }) = self
            .cmd(disk, cmd)
            .into_iter()
            .find(|e| matches!(e, Effect::Left { .. }))
        else {
            return;
        };
        for peer in self.running() {
            if peer != disk && self.sends(disk, peer) {
                let at = now + self.rng.random_range(0..=self.delay_ms);
                self.gossip_to(disk, peer, at, own.clone());
                self.cmd(peer, Cmd::Departed { disk, restart, at });
            }
        }
        self.exits.insert(disk, now);
        self.down(disk);
    }

    pub fn accept_loss(&mut self, disk: u64) -> bool {
        let running = self.running();
        let Some(origin) = running
            .iter()
            .copied()
            .find(|d| self.live_reconciled(*d, *d))
            .or(running.first().copied())
        else {
            return false;
        };
        self.accept_loss_from(origin, disk)
    }

    pub fn accept_loss_from(&mut self, origin: u64, disk: u64) -> bool {
        for _ in 0..=REPLACE_MS / STEP_MS {
            let effects = self.step_node(origin, self.now_ms, vec![Cmd::AcceptLoss { disk }]);
            if effects.iter().any(|e| matches!(e, Effect::Accepted(true))) {
                return true;
            }
            if !self.nodes.get(&origin).is_some_and(|n| n.view.absent(disk)) {
                return false;
            }
            self.step(STEP_MS);
        }
        false
    }

    fn serving(&self, disk: u64) -> bool {
        self.nodes
            .get(&disk)
            .is_some_and(|n| n.signals == 0 && !n.view.own().body.drained)
    }

    fn run_of(&self, disk: u64) -> u64 {
        self.disks.get(&disk).map_or(0, |d| d.alive_ms)
    }

    pub fn holds(&self, disk: u64) -> u64 {
        self.segments
            .values()
            .filter(|s| s.marks.contains_key(&disk) && s.holders.holders.contains(&disk))
            .map(|s| s.bytes)
            .sum()
    }

    pub fn live_copies(&self, segment: &Segment) -> usize {
        self.served(segment).count()
    }

    pub fn place(&mut self, holders: &[u64], bytes: u64) -> u64 {
        let format = self.nodes.get(&holders[0]).map_or(1, |n| n.ctl.committed);
        self.store(holders[0], holders, bytes, format)
    }

    fn store(&mut self, writer: u64, holders: &[u64], bytes: u64, format: u16) -> u64 {
        let id = self.next_segment;
        self.next_segment += 1;
        let named: BTreeSet<u64> = holders.iter().copied().collect();
        for h in holders {
            self.queue.push(Frame {
                at: self.now_ms,
                sent: self.now_ms,
                from: writer,
                to: *h,
                version: 0,
                msg: Msg::Named(id, named.clone()),
            });
        }
        let set = HolderSet {
            segment: id,
            epoch: 1,
            writer,
            holders: holders.iter().copied().collect(),
        };
        self.wish(writer, set.clone());
        self.segments.insert(
            id,
            Segment {
                writer,
                copied: holders.iter().map(|h| (*h, self.now_ms)).collect(),
                holders: set,
                bytes,
                format,
                born_run_ms: self.run_ms,
                born_ms: self.now_ms,
                lost_run_ms: 0,
                marks: holders.iter().map(|h| (*h, 0)).collect(),
                born: holders.iter().copied().collect(),
                stream: id % STREAMS,
            },
        );
        for h in holders {
            let adopt = Cmd::Adopt {
                id,
                mark: 0,
                born: self.now_ms,
                writer: Some(writer),
                unnamed: None,
                named: Vec::new(),
            };
            self.cmd(*h, adopt);
        }
        self.acked.insert(id);
        id
    }

    pub fn fill(&mut self, bytes_per_node: u64) {
        let nodes = self.running();
        let floor = 3.min(nodes.len());
        let segments = bytes_per_node * nodes.len() as u64 / floor as u64 / SEGMENT_BYTES;
        for i in 0..segments as usize {
            let holders: Vec<u64> = (0..floor).map(|k| nodes[(i + k) % nodes.len()]).collect();
            self.place(&holders, SEGMENT_BYTES);
        }
    }

    pub fn publish(&mut self) -> Option<u64> {
        let writers: Vec<u64> = self
            .running()
            .into_iter()
            .filter(|d| self.serving(*d))
            .collect();
        let writer = *writers.choose(&mut self.rng)?;
        self.publish_from(writer)
    }

    pub fn publish_from(&mut self, writer: u64) -> Option<u64> {
        let id = self.next_segment;
        let picked = self
            .cmd(writer, Cmd::Place { id })
            .into_iter()
            .find_map(|e| match e {
                Effect::Picked(holders) => Some(holders),
                _ => None,
            })?;
        let reached: Vec<u64> = picked
            .into_iter()
            .filter(|h| *h == writer || self.link(writer, *h))
            .collect();
        let cmd = Cmd::Seal {
            reached: reached.len(),
        };
        let format = self.cmd(writer, cmd).into_iter().find_map(|e| match e {
            Effect::Stored { format } => Some(format),
            _ => None,
        })?;
        self.count_domains(writer);
        Some(self.store(writer, &reached, SEGMENT_BYTES, format))
    }

    pub fn set_of(&self, n: u64, id: u64) -> Option<&HolderSet> {
        self.nodes.get(&n)?.set_of(id)
    }

    fn leads(&self, n: u64, id: u64) -> bool {
        self.nodes
            .get(&n)
            .is_some_and(|node| node.leads(self.now_ms, id))
    }

    fn wish(&mut self, by: u64, set: HolderSet) {
        self.wishes
            .entry((set.segment, set.epoch, by))
            .or_insert((by, set));
    }

    fn open_wishes(&mut self) {
        let keys: Vec<(u64, u64, u64)> = self.wishes.keys().copied().collect();
        for key in keys {
            let Some((by, set)) = self.wishes.get(&key).cloned() else {
                continue;
            };
            if !self.nodes.contains_key(&by) {
                self.wishes.remove(&key);
                continue;
            }
            self.cmd(by, Cmd::Wish { set });
        }
    }

    fn sends(&self, from: u64, to: u64) -> bool {
        from == to
            || (self.nodes.contains_key(&to)
                && !self.cuts.contains(&pair(from, to))
                && !self.oneway.contains(&(from, to)))
    }

    fn relays(&self, from: u64, to: u64) -> bool {
        !self.sends(from, to)
            && self
                .nodes
                .keys()
                .any(|r| self.sends(from, *r) && self.sends(*r, to))
    }

    fn link(&self, from: u64, to: u64) -> bool {
        self.sends(from, to) && (from == to || !self.oneway.contains(&(to, from)))
    }

    fn live_reconciled(&self, by: u64, disk: u64) -> bool {
        self.nodes
            .get(&by)
            .is_some_and(|node| node.live_of(self.now_ms, disk))
    }

    fn show(&mut self, n: u64, id: u64) {
        let Some(seg) = self.segments.get(&id) else {
            return;
        };
        if !seg.marks.contains_key(&n) || self.disks[&n].store.erased.contains(&id) {
            return;
        }
        let gone = self.drops_done.contains(&id)
            || self.drop_waits.get(&id).is_some_and(|(by, _)| {
                self.nodes
                    .get(by)
                    .and_then(|n| n.drop_waits.get(&id))
                    .is_some_and(BTreeSet::is_empty)
                    && self
                        .drop_meta
                        .get(&id)
                        .is_some_and(|t| self.now_ms >= t + HEARD_MS + LEASE_MS)
            })
            || self
                .deleted
                .iter()
                .any(|(stream, upto)| seg.stream == *stream && id <= *upto);
        if gone && self.warnings.contains(&id) {
            self.shown_warned += 1;
        } else if gone {
            self.shown += 1;
        }
    }

    fn do_take(&mut self) {
        let ids: Vec<u64> = self.acked.iter().copied().collect();
        for id in ids {
            let leaders = self
                .running()
                .into_iter()
                .filter(|n| self.leads(*n, id))
                .count();
            if leaders > 1 {
                self.double_leaders += 1;
            }
            for n in self.running() {
                for effect in self.step_node(n, self.now_ms, vec![Cmd::Take { id }]) {
                    if let Effect::Served { by, id, mark } = effect {
                        if self.settled.get(&id).is_some_and(|s| mark < *s) {
                            self.redelivered += 1;
                        }
                        self.show(by, id);
                    }
                }
            }
        }
    }

    fn do_read(&mut self) {
        let Some(reader) = self.running().choose(&mut self.rng).copied() else {
            return;
        };
        let ids: Vec<u64> = self.acked.iter().copied().collect();
        for id in ids {
            let mut asked = Vec::new();
            for effect in self.step_node(reader, self.now_ms, vec![Cmd::Read]) {
                if let Effect::Ask { peers } = effect {
                    asked = peers;
                }
            }
            let mut open = Vec::new();
            let near: Vec<u64> = asked
                .iter()
                .copied()
                .filter(|p| self.link(reader, *p))
                .collect();
            for p in near {
                for effect in self.step_node(p, self.now_ms, vec![Cmd::Probe { id }]) {
                    if let Effect::Open { by } = effect {
                        open.push(by);
                    }
                }
            }
            let cmd = Cmd::Pick { id, asked, open };
            for effect in self.step_node(reader, self.now_ms, vec![cmd]) {
                match effect {
                    Effect::Served { by, id, .. } => self.show(by, id),
                    Effect::Unserved { id, partial } => {
                        self.partial_reads += u64::from(partial);
                        if self.readable(reader, id) {
                            self.incomplete_reads += 1;
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    fn readable(&self, reader: u64, id: u64) -> bool {
        let seg = &self.segments[&id];
        let deleted = self
            .deleted
            .iter()
            .any(|(stream, upto)| seg.stream == *stream && id <= *upto);
        let set = self.newest().and_then(|m| m.segments.get(&id));
        !self.dropped.contains(&id)
            && !deleted
            && seg.marks.keys().any(|p| {
                self.nodes.contains_key(p)
                    && self.link(reader, *p)
                    && set.is_some_and(|m| m.holders.holders.contains(p))
            })
    }

    pub fn copying_of(&self, id: u64) -> bool {
        self.copying.keys().any(|(s, _)| *s == id)
    }

    pub fn delete_stream(&mut self, stream: u64) -> bool {
        let Some(origin) = self.running().choose(&mut self.rng).copied() else {
            return false;
        };
        self.delete_from(origin, stream)
    }

    pub fn delete_from(&mut self, origin: u64, stream: u64) -> bool {
        self.delete_start(origin, stream).is_some()
    }

    pub fn delete_stream_wait(&mut self, stream: u64) -> bool {
        let Some(origin) = self.running().choose(&mut self.rng).copied() else {
            return false;
        };
        self.delete_wait(origin, stream)
    }

    pub fn delete_wait(&mut self, origin: u64, stream: u64) -> bool {
        let Some(id) = self.delete_start(origin, stream) else {
            return false;
        };
        while self.open_requests.contains_key(&id) {
            self.advance(STEP_MS);
        }
        self.done.iter().any(|d| d.0 == id)
    }

    fn delete_start(&mut self, origin: u64, stream: u64) -> Option<u64> {
        let (id, upto) = (self.next_request, self.next_segment - 1);
        let effects = self.step_node(origin, self.now_ms, vec![Cmd::Delete { id, stream, upto }]);
        if !effects.iter().any(|e| matches!(e, Effect::Announced)) {
            return None;
        }
        self.next_request += 1;
        self.open_requests.insert(id, origin);
        Some(id)
    }

    pub fn drop_segment(&mut self, id: u64) -> bool {
        if !self.segments.contains_key(&id) {
            return false;
        }
        let mut decider = None;
        for n in self.running() {
            if self
                .cmd(n, Cmd::DropStart { id })
                .iter()
                .any(|e| matches!(e, Effect::Dropping))
            {
                decider = Some(n);
                break;
            }
        }
        let Some(decider) = decider else {
            return false;
        };
        self.dropped.insert(id);
        let restart = self.disks[&decider].restart;
        self.drop_waits.insert(id, (decider, restart));
        self.drops_settle();
        true
    }

    fn drops_settle(&mut self) {
        let now = self.now_ms;
        for id in self.drop_waits.keys() {
            let emptied = self.nodes.values().any(|n| {
                n.raft
                    .meta()
                    .segments
                    .get(id)
                    .is_some_and(|m| m.holders.epoch > 0 && m.holders.holders.is_empty())
            });
            if emptied {
                self.drop_meta.entry(*id).or_insert(now);
            }
        }
        let ids: Vec<u64> = self.drop_waits.keys().copied().collect();
        for id in ids {
            let (by, restart) = self.drop_waits[&id];
            if !self.nodes.contains_key(&by)
                || self.disks.get(&by).is_none_or(|d| d.restart != restart)
            {
                self.drop_waits.remove(&id);
                self.drops_done.insert(id);
            } else {
                self.step_node(by, now, vec![Cmd::DropCheck { id }]);
            }
        }
    }

    fn count_domains(&mut self, writer: u64) {
        let node = &self.nodes[&writer];
        let labels: BTreeMap<u64, u64> = match &node.domains {
            Some(d) => d.of.clone(),
            None => node.view.live().map(|(d, _)| (d, 0)).collect(),
        };
        let zone = |d: &u64| self.disks.get(d).map(|x| x.zone.as_str()).unwrap_or("");
        let (mut split, mut merged) = (false, false);
        for (a, la) in &labels {
            for (b, lb) in labels.range(a + 1..) {
                if zone(a).is_empty() || zone(b).is_empty() {
                    continue;
                }
                split |= zone(a) == zone(b) && la != lb;
                merged |= zone(a) != zone(b) && la == lb;
            }
        }
        self.splits += u64::from(split);
        self.merges += u64::from(merged);
    }

    pub fn zone(&self, disk: u64) -> String {
        self.disks
            .get(&disk)
            .map(|d| d.zone.clone())
            .unwrap_or_default()
    }

    pub fn agreed(&self) -> bool {
        let running: BTreeSet<u64> = self.nodes.keys().copied().collect();
        let results: BTreeSet<Option<u64>> = self
            .nodes
            .values()
            .map(|n| {
                n.domains
                    .as_ref()
                    .filter(|d| d.of.keys().copied().collect::<BTreeSet<u64>>() == running)
                    .map(|d| d.hash)
            })
            .collect();
        results.len() == 1 && !results.contains(&None)
    }

    pub fn groups(&self) -> Option<Groups> {
        self.newest()
            .map(|m| m.groups.clone())
            .filter(|g| g.epoch > 0)
    }

    fn jobs(&self, cursor: Option<usize>) -> (Vec<Job>, Vec<(u64, HolderSet)>) {
        let mut copies = Vec::new();
        let mut drops = Vec::new();
        let per: BTreeMap<u64, Pass> = self
            .nodes
            .iter()
            .filter_map(|(n, node)| Some((*n, node.pass()?)))
            .collect();
        let knobs = Knobs {
            open: &self.open,
            held: &self.held_repair,
        };
        let ids: Vec<&u64> = self.segments.keys().collect();
        let scan: Vec<usize> = match cursor {
            Some(c) => repair::pass(ids.len(), c).collect(),
            None => (0..ids.len()).collect(),
        };
        for i in scan {
            let id = *ids[i];
            if self.held_repair.contains(&id) {
                continue;
            }
            let holders: Vec<u64> = self
                .nodes
                .iter()
                .filter(|(_, n)| n.marks.contains_key(&id))
                .map(|(d, _)| *d)
                .collect();
            for n in holders {
                let (Some(node), Some(pass)) = (self.nodes.get(&n), per.get(&n)) else {
                    continue;
                };
                let (copy, drop) = node.plan(self.now_ms, id, pass, &knobs);
                copies.extend(copy);
                drops.extend(drop);
            }
        }
        for (disk, node) in &self.nodes {
            if node.signals != 0 || node.view.own().body.drained {
                copies.extend(node.drains(self.run_of(*disk), &knobs));
            }
        }
        copies.sort_by_key(|j| (j.copies, j.segment));
        (copies, drops)
    }

    pub fn repaired(&self) -> bool {
        for (id, seg) in &self.segments {
            assert!(
                !seg.marks.keys().any(|h| self.nodes.contains_key(h))
                    || self.served(seg).next().is_some(),
                "segment {id} has a live copy and no live named holder: {:?}",
                seg.holders
            );
        }
        let (copies, drops) = self.jobs(None);
        let member = |d: &u64| self.newest().is_some_and(|m| m.members.contains(d));
        let named = self.segments.iter().all(|(id, seg)| {
            let set = self.newest().and_then(|m| m.segments.get(id));
            seg.marks.keys().all(|h| {
                !self.nodes.contains_key(h)
                    || !member(h)
                    || set.is_some_and(|m| m.holders.holders.contains(h))
            })
        });
        self.grouped()
            && named
            && !copies.iter().any(|j| member(&j.from))
            && !drops.iter().any(|(by, _)| member(by))
    }

    fn grouped(&self) -> bool {
        let Some(members) = self.newest().map(|m| &m.members) else {
            return false;
        };
        let voters: BTreeMap<&u64, &Node> = self
            .nodes
            .iter()
            .filter(|(d, _)| members.contains(*d))
            .collect();
        let records: BTreeSet<Option<&Groups>> = voters.values().map(|n| groups_of(n)).collect();
        records.len() == 1
            && !records.contains(&None)
            && voters.values().all(|n| n.next_groups().is_none())
    }

    pub fn settle_repair(&mut self, max_ms: u64) -> bool {
        self.advance(30_000);
        for _ in 0..max_ms / DIGEST_MS {
            if self.repaired() {
                return true;
            }
            self.advance(DIGEST_MS);
        }
        self.repaired()
    }

    fn log(&mut self, from: HolderSet, to: HolderSet) {
        let copies = self.segments[&to.segment]
            .marks
            .keys()
            .copied()
            .filter(|h| self.nodes.contains_key(h))
            .collect();
        let groups = self.groups();
        let current = groups.as_ref().is_some_and(|g| g.current(&to.holders));
        let back = from
            .holders
            .iter()
            .filter(|h| self.disks.contains_key(h))
            .count();
        let least = groups.as_ref().map_or(0, |g| back.min(usize::from(g.size)));
        self.holder_changes.push(HolderChange {
            run_ms: self.run_ms,
            from,
            to,
            copies,
            current,
            least,
        });
    }

    fn repair(&mut self) {
        let now = self.now_ms;
        if self.dirty || now.is_multiple_of(DIGEST_MS) {
            let (copies, drops) = self.jobs(Some(self.cursor));
            self.cursor = self.cursor.wrapping_add(repair::REPAIR_SCAN_SEGMENTS);
            self.queued = copies;
            self.dirty = !drops.is_empty() || self.nodes.values().any(Node::confirming);
            self.drop_copies(drops);
        }
        let queued = std::mem::take(&mut self.queued);
        self.copy(&queued, now);
        self.queued = queued;
        let planned: Vec<(u64, u64)> = self.copying.keys().copied().collect();
        for (id, to) in planned {
            let Some(seg) = self.segments.get(&id) else {
                continue;
            };
            let set = self.set_of(to, id).unwrap_or(&seg.holders);
            if self.nodes.contains_key(&to)
                && no_holder(
                    to,
                    &set.holders,
                    |h| self.link(to, h),
                    |h| seg.marks.contains_key(&h),
                )
            {
                self.named_lost.insert(id);
            }
        }
    }

    fn drop_copies(&mut self, drops: Vec<(u64, HolderSet)>) {
        for (by, keep) in drops {
            self.wish(by, keep);
        }
    }

    fn copy(&mut self, queued: &[Job], now: u64) {
        for job in queued {
            let Some(seg) = self.segments.get(&job.segment) else {
                continue;
            };
            if !self.nodes.contains_key(&job.from)
                || !seg.marks.contains_key(&job.from)
                || self.disks[&job.from].dropped.contains_key(&job.segment)
            {
                continue;
            }
            let bytes = seg.bytes;
            let mut newer = None;
            for t in &job.to {
                if !self.open(job.from, *t) {
                    continue;
                }
                let offer = Cmd::Offer {
                    id: job.segment,
                    epoch: job.epoch,
                };
                if let Some(Effect::Newer { epoch }) = self.cmd(*t, offer).pop() {
                    newer = Some((*t, epoch));
                    break;
                }
            }
            if let Some((target, theirs)) = newer {
                self.copying.remove(&(job.segment, target));
                let stale = Cmd::Stale {
                    id: job.segment,
                    epoch: theirs,
                };
                self.cmd(job.from, stale);
                continue;
            }
            let mut active: Vec<u64> = job
                .to
                .iter()
                .copied()
                .filter(|t| {
                    self.copying.contains_key(&(job.segment, *t)) && self.open(job.from, *t)
                })
                .take(job.need)
                .collect();
            for t in &job.to {
                if active.len() >= job.need {
                    break;
                }
                if !active.contains(t)
                    && self.open(job.from, *t)
                    && self.takes.get_mut(t).is_some_and(|l| l.available(now) > 0)
                {
                    active.push(*t);
                }
            }
            for to in active {
                let (Some(out), Some(inn)) =
                    (self.sends.get_mut(&job.from), self.takes.get_mut(&to))
                else {
                    continue;
                };
                let room = out.available(now).min(inn.available(now));
                if room == 0 {
                    continue;
                }
                let left = self.copying.entry((job.segment, to)).or_insert(bytes);
                let step = room.min(*left);
                out.spend(step);
                inn.spend(step);
                *left -= step;
                if *left > 0 {
                    continue;
                }
                self.copying.remove(&(job.segment, to));
                self.dirty = true;
                if self.disks[&to].dropped.contains_key(&job.segment) {
                    continue;
                }
                let mark = self.segments[&job.segment].marks[&job.from];
                let sender_erased = self.disks[&job.from].store.erased.contains(&job.segment);
                self.cmd(
                    to,
                    Cmd::Erase {
                        id: job.segment,
                        sender_erased,
                    },
                );
                let handoff = Cmd::Handoff {
                    id: job.segment,
                    to,
                    epoch: job.epoch,
                    mark,
                };
                let Some(Effect::Copy {
                    writer,
                    unnamed,
                    named,
                    ..
                }) = self
                    .cmd(job.from, handoff)
                    .into_iter()
                    .find(|e| matches!(e, Effect::Copy { .. }))
                else {
                    continue;
                };
                let Some(seg) = self.segments.get_mut(&job.segment) else {
                    continue;
                };
                seg.marks.insert(to, mark);
                seg.copied.insert(to, self.now_ms);
                let born = seg.born_ms;
                let adopt = Cmd::Adopt {
                    id: job.segment,
                    mark,
                    born,
                    writer,
                    unnamed,
                    named,
                };
                self.cmd(to, adopt);
            }
        }
    }

    fn open(&self, from: u64, to: u64) -> bool {
        self.serving(to) && self.link(from, to)
    }

    fn finish_stops(&mut self) {
        let stopping: Vec<u64> = self
            .nodes
            .iter()
            .filter(|(_, n)| n.stop_run_ms.is_some())
            .map(|(d, _)| *d)
            .collect();
        for disk in stopping {
            if self.nodes[&disk].stop_done(
                self.run_of(disk),
                &Knobs {
                    open: &self.open,
                    held: &self.held_repair,
                },
            ) {
                self.leave(disk);
            }
        }
    }

    fn answer_settles(&mut self) {
        let mut open: Vec<(u64, u64)> = self
            .nodes
            .iter()
            .flat_map(|(d, n)| n.settles.keys().map(|sid| (*sid, *d)))
            .collect();
        open.sort_unstable();
        for (sid, leader) in open {
            self.step_node(leader, self.now_ms, vec![Cmd::Answer { sid }]);
        }
    }

    fn served<'a>(&'a self, seg: &'a Segment) -> impl Iterator<Item = (&'a u64, &'a u64)> + 'a {
        seg.marks
            .iter()
            .filter(|(h, _)| self.nodes.contains_key(h) && seg.holders.holders.contains(h))
    }

    pub fn lost(&self) -> usize {
        self.acked
            .iter()
            .filter(|s| !self.dropped.contains(s))
            .filter(|s| {
                self.segments
                    .get(s)
                    .is_none_or(|seg| self.served(seg).next().is_none())
            })
            .count()
    }

    pub fn lost_settles(&self) -> usize {
        self.settled
            .iter()
            .filter(|(s, _)| !self.dropped.contains(s))
            .filter(|(s, n)| {
                self.segments
                    .get(s)
                    .and_then(|seg| self.served(seg).map(|(_, m)| *m).max())
                    .is_none_or(|m| m < **n)
            })
            .count()
    }

    pub fn unreadable(&self) -> usize {
        self.segments
            .values()
            .flat_map(|s| s.marks.keys().map(move |h| (h, s.format)))
            .filter(|(h, format)| self.nodes.get(h).is_some_and(|n| n.format_max < *format))
            .count()
    }

    pub fn max_format(&self) -> u16 {
        self.segments.values().map(|s| s.format).max().unwrap_or(1)
    }

    fn debt_truth(&self, count: u8, side: &BTreeSet<u64>) -> bool {
        let recent = self.run_ms.saturating_sub(SLACK_MS);
        self.segments.values().all(|s| {
            let live = self.live_copies(s);
            s.born_run_ms > recent
                || s.lost_run_ms > recent
                || !s
                    .marks
                    .keys()
                    .any(|h| side.contains(h) && s.holders.holders.contains(h))
                || live >= usize::from(count)
        })
    }

    pub fn side(&self, disk: u64) -> BTreeSet<u64> {
        let mut seen = BTreeSet::from([disk]);
        let mut todo = vec![disk];
        while let Some(a) = todo.pop() {
            for b in self.nodes.keys() {
                if self.link(a, *b) && seen.insert(*b) {
                    todo.push(*b);
                }
            }
        }
        seen
    }

    pub fn listed(&self, disk: u64) -> bool {
        self.nodes.contains_key(&disk)
            && self.newest().is_some_and(|m| {
                let linked = m
                    .members
                    .iter()
                    .filter(|x| {
                        **x == disk
                            || self
                                .linked_ms
                                .get(&pair(disk, **x))
                                .is_some_and(|t| self.now_ms < t + FENCE_MS)
                            || self.relays(disk, **x)
                    })
                    .count();
                m.members.contains(&disk) && confirmed(linked, m.members.len())
            })
    }

    fn counts_ok(&self, side: &BTreeSet<u64>, count: u8) -> bool {
        let recent = self.run_ms.saturating_sub(SLACK_MS);
        side.iter().all(|p| {
            self.nodes.get(p).is_some_and(|n| n.ctl.count <= count)
                || self.count_changes.iter().any(|c| {
                    c.disk == *p && c.run_ms > recent && (c.from <= count || c.to <= count)
                })
        })
    }

    fn counted(&self, loss: &WorldLoss) -> bool {
        let start = self
            .boots
            .iter()
            .filter(|(d, r)| *d == loss.disk && *r <= loss.run_ms)
            .map(|(_, r)| *r)
            .max()
            .unwrap_or(0);
        let again = loss.kind != LossKind::Kill
            && self
                .losses
                .iter()
                .any(|l| l.disk == loss.disk && l.run_ms >= start && l.run_ms < loss.run_ms);
        let back = match loss.kind {
            LossKind::Kill => FLAP_MS,
            LossKind::Left => STOP_BUDGET_MS + JOIN_MS,
            LossKind::Signal => 0,
        };
        !again
            && !self
                .boots
                .iter()
                .any(|(d, r)| *d == loss.disk && *r > loss.run_ms && *r <= loss.run_ms + back)
    }

    pub fn illegal_falls(&self) -> Vec<FloorChange> {
        let mut legal = BTreeSet::from([START]);
        let mut bad = Vec::new();
        for change in &self.floor_changes {
            let (from, to) = (change.from, change.to);
            let next = to.epoch == from.epoch + 1 && to.value < from.value;
            let ok = to > from
                && match change.cause {
                    Cause::Merge => legal.contains(&to),
                    Cause::Rise => to.epoch == from.epoch,
                    Cause::Drain => {
                        next && self
                            .drain_commits
                            .iter()
                            .any(|(d, at)| *d == change.disk && *at <= change.run_ms)
                    }
                    Cause::AcceptLoss => next,
                    Cause::Quiet => {
                        let first = change.run_ms.saturating_sub(QUIET_PERIOD);
                        let last = change.run_ms.saturating_sub(SLACK_MS);
                        next && change.debt_ok
                            && change.majority
                            && change.counts_ok
                            && !self.losses.iter().any(|l| {
                                let at = match l.kind {
                                    LossKind::Left => l.run_ms + STOP_BUDGET_MS + JOIN_MS,
                                    LossKind::Kill | LossKind::Signal => l.run_ms,
                                };
                                at > first
                                    && at <= last
                                    && l.side.contains(&change.disk)
                                    && self.counted(l)
                            })
                    }
                };
            if ok {
                legal.insert(to);
            } else {
                bad.push(*change);
            }
        }
        bad
    }

    pub fn advance(&mut self, ms: u64) {
        for _ in 0..ms / STEP_MS {
            self.step(STEP_MS);
        }
    }

    pub fn jump(&mut self, run_ms: u64) {
        self.run_ms += run_ms;
        let running = self.running();
        for disk in running {
            if let Some(d) = self.disks.get_mut(&disk) {
                d.alive_ms += run_ms;
            }
        }
    }

    pub fn quiet_hours(&mut self, hours: u64) {
        for _ in 0..hours {
            self.jump(HOUR_MS - 2 * SAMPLE_MS);
            self.advance(2 * SAMPLE_MS);
        }
    }

    fn step(&mut self, dt: u64) {
        self.now_ms += dt;
        if !self.nodes.is_empty() {
            self.run_ms += dt;
        }
        let running = self.running();
        for disk in &running {
            if let Some(d) = self.disks.get_mut(disk) {
                d.alive_ms += dt;
            }
        }
        let now = self.now_ms;
        for c in &self.cuts {
            self.cut_ms.insert(*c, now);
        }
        for (a, b) in &self.oneway {
            self.cut_ms.insert(pair(*a, *b), now);
        }
        for a in &running {
            for b in running.iter().filter(|b| *b > a) {
                if self.link(*a, *b) {
                    self.linked_ms.insert((*a, *b), now);
                }
            }
        }
        let (mut due, later): (Vec<Frame>, Vec<Frame>) = std::mem::take(&mut self.queue)
            .into_iter()
            .partition(|f| f.at <= now);
        self.queue = later;
        due.sort_by_key(|f| f.at);
        for frame in due {
            self.receive(frame);
        }
        for disk in &running {
            let run = self.run_of(*disk);
            if let Some(node) = self.nodes.get_mut(disk) {
                node.view_tick(now, run);
            }
        }
        for disk in &running {
            if self.rng.random_range(0..4) != 0 {
                self.cmd(*disk, Cmd::Tick);
            }
        }
        self.each(Cmd::Ping);
        for node in self.nodes.values_mut() {
            node.timers(now);
        }
        for disk in &running {
            self.cmd(*disk, Cmd::Guard);
        }
        for node in self.nodes.values_mut() {
            node.prune(now);
        }
        self.open_wishes();
        self.each(Cmd::SegAsks);
        self.each(Cmd::Fresh);
        self.each(Cmd::Beats);
        self.each(Cmd::IsrAsks);
        self.each(Cmd::ResendCopied);
        self.drops_settle();
        self.each(Cmd::ResendDrops);
        self.answer_requests();
        self.answer_settles();
        for disk in &running {
            self.cmd(*disk, Cmd::Govern);
            self.cmd(*disk, Cmd::Lead);
        }
        if now.is_multiple_of(GOSSIP_MS) {
            for disk in &running {
                self.gossip(*disk);
            }
        }
        let running = self.running();
        if now.is_multiple_of(HELLO_MS) && running.len() > 1 {
            let mut order = running.clone();
            order.shuffle(&mut self.rng);
            for disk in &order {
                let peers: Vec<u64> = running.iter().copied().filter(|p| p != disk).collect();
                if let Some(peer) = peers.choose(&mut self.rng).copied() {
                    self.hello(*disk, peer);
                }
            }
        }
        for disk in &running {
            let due = self
                .nodes
                .get(disk)
                .is_some_and(|n| now % SAMPLE_MS == n.phase_ms);
            if due {
                self.sample(*disk);
            }
        }
        self.repair();
        self.finish_stops();
        if self.watch && now.is_multiple_of(GOSSIP_MS) && now >= self.commit_ms + 2 * FENCE_MS {
            let (unled, unreconciled) = self.stalls();
            let current: BTreeSet<(u64, u64)> = unled
                .into_iter()
                .chain(unreconciled.into_iter().map(|d| (0, d)))
                .collect();
            for k in current {
                self.stalled.entry(k).or_default().push(now);
            }
            for times in self.stalled.values_mut() {
                times.retain(|t| now < t + STALL_WINDOW_MS);
                let stalled = times.len().saturating_sub(1) as u64 * GOSSIP_MS;
                self.worst_stall = self.worst_stall.max(stalled);
            }
            self.stalled.retain(|_, times| !times.is_empty());
        }
    }

    fn gossip_to(&mut self, from: u64, to: u64, at: u64, updates: Vec<Update>) {
        self.queue.push(Frame {
            at,
            sent: self.now_ms,
            from,
            to,
            version: 0,
            msg: Msg::Gossip(updates),
        });
    }

    fn send(&mut self, from: u64, to: u64, msg: Msg) -> bool {
        let Some(version) = self.nodes.get(&from).map(|n| n.raft.meta().version) else {
            return false;
        };
        self.send_at(from, to, msg, version)
    }

    pub fn inject(&mut self, from: u64, to: u64, msg: Msg) {
        self.queue.push(Frame {
            at: self.now_ms,
            sent: self.now_ms,
            from,
            to,
            version: 0,
            msg,
        });
    }

    fn send_at(&mut self, from: u64, to: u64, msg: Msg, version: u64) -> bool {
        let protocol = !matches!(msg, Msg::Gossip(..) | Msg::Ping(_) | Msg::Pong(..));
        let relayed = msg.relayable() && self.relays(from, to);
        if !(relayed || self.sends(from, to))
            || (protocol && self.loss > 0.0 && self.rng.random_bool(self.loss))
        {
            return false;
        }
        let slow = if matches!(msg, Msg::Raft(_)) {
            self.slow_ms
        } else {
            0
        };
        let hops = if relayed { 2 } else { 1 };
        let at = self.now_ms + slow + hops * self.rng.random_range(1..=self.link_ms.max(1));
        match &msg {
            Msg::RecordsAsk(..) => {
                self.asks_doubled += u64::from(self.queue.iter().any(|q| {
                    (q.from, q.to, matches!(q.msg, Msg::RecordsAsk(..))) == (from, to, true)
                        || (q.from, q.to, matches!(q.msg, Msg::Records(..))) == (to, from, true)
                }));
            }
            Msg::Records(records, ..) => self.records_max = self.records_max.max(records.len()),
            Msg::Record(..) => self.record_frames += 1,
            Msg::SegAsk(..) => self.seg_asks += 1,
            _ => {}
        }
        self.queue.push(Frame {
            at,
            sent: self.now_ms,
            from,
            to,
            version,
            msg,
        });
        true
    }

    fn receive(&mut self, f: Frame) {
        if !self.sends(f.from, f.to) && !(f.msg.relayable() && self.relays(f.from, f.to)) {
            return;
        }
        let (to, now, run) = (f.to, self.now_ms, self.run_of(f.to));
        if !self.nodes.contains_key(&to) {
            return;
        }
        let msg = f.msg;
        let partial = match &msg {
            Msg::SegAsk(id, _) => self.partial(to, *id),
            _ => false,
        };
        let dropped = matches!(msg, Msg::Dropped(_));
        let input = Input {
            now,
            run,
            cmds: Vec::new(),
            frames: vec![Inbound {
                from: f.from,
                sent: f.sent,
                version: f.version,
                msg,
            }],
            partial,
            pairs: BTreeSet::new(),
        };
        self.run_node(to, input);
        if dropped {
            self.drops_settle();
        }
    }

    fn answer_requests(&mut self) {
        let open: Vec<(u64, u64)> = self.open_requests.iter().map(|(i, o)| (*i, *o)).collect();
        for (id, origin) in open {
            if self.nodes.contains_key(&origin) {
                self.cmd(origin, Cmd::AnswerRequest { id });
            } else {
                self.open_requests.remove(&id);
            }
        }
    }

    pub fn held_state(&self, disk: u64) -> usize {
        self.nodes
            .get(&disk)
            .map_or(0, |n| n.votes.len() + n.peers.len() + n.seen_records.len())
    }

    fn sense(&self, disk: u64) -> Sense {
        Sense {
            now: self.now_ms,
            run: self.run_of(disk),
            cap: self.capacity.get(&disk).copied(),
            used: self.holds(disk),
            headroom: self.headroom,
            sizes: self
                .segments
                .iter()
                .filter(|(_, s)| s.marks.contains_key(&disk))
                .map(|(id, s)| (*id, s.bytes))
                .collect(),
        }
    }

    fn wipe(
        &mut self,
        old: u64,
        members: &[u64],
        kept: Vec<(u64, u64, Option<u64>, u64)>,
        erased: BTreeSet<u64>,
    ) {
        let (zone, format_max) = (self.zone(old), self.nodes[&old].format_max);
        for seg in self.segments.values_mut() {
            seg.marks.remove(&old);
        }
        self.down(old);
        self.disks.remove(&old);
        let new = self.create(&zone, 0, format_max);
        for m in members {
            self.cmd(new, Cmd::AskRecords { peer: *m });
        }
        for (id, mark, epoch, born) in kept {
            if erased.contains(&id) {
                self.cmd(
                    new,
                    Cmd::Erase {
                        id,
                        sender_erased: true,
                    },
                );
            }
            let (Some(seg), Some(epoch)) = (self.segments.get_mut(&id), epoch) else {
                continue;
            };
            seg.marks.insert(new, mark);
            seg.copied.insert(new, self.now_ms);
            let writer = Some(seg.writer);
            let adopt = Cmd::Adopt {
                id,
                mark,
                born,
                writer,
                unnamed: Some((epoch, Some(old))),
                named: Vec::new(),
            };
            self.cmd(new, adopt);
            self.warnings.insert(id);
        }
        self.wiped.push((old, new));
    }

    fn gossip(&mut self, disk: u64) {
        let Some(node) = self.nodes.get(&disk) else {
            return;
        };
        let mut peers = node.gossip_peers();
        peers.shuffle(&mut self.rng);
        peers.truncate(FANOUT);
        for peer in peers {
            self.exchange(disk, peer);
        }
    }

    fn trade(&self, from: u64, to: u64) -> Option<(Vec<Update>, Vec<Update>)> {
        let (a, b) = (self.nodes.get(&from)?, self.nodes.get(&to)?);
        let ask = a.view.digest();
        let (to_a, reply) = (b.view.answer(&ask), b.view.digest());
        Some((to_a, a.view.answer(&reply)))
    }

    fn exchange(&mut self, from: u64, to: u64) {
        if self.cuts.contains(&pair(from, to)) {
            self.missed_gossip += 1;
            return;
        }
        let Some((to_a, to_b)) = self.trade(from, to) else {
            return;
        };
        for (source, target, updates) in [(to, from, to_a), (from, to, to_b)] {
            if !self.sends(source, target) || self.rng.random_bool(self.drop) {
                self.missed_gossip += 1;
                continue;
            }
            let at = self.now_ms + self.rng.random_range(0..=self.delay_ms);
            self.gossip_to(source, target, at, updates);
        }
    }

    fn hello(&mut self, from: u64, to: u64) {
        if !self.link(from, to) {
            return;
        }
        let Some((to_a, to_b)) = self.trade(from, to) else {
            return;
        };
        let now = self.now_ms;
        for (target, updates) in [(from, to_a), (to, to_b)] {
            let run = self.run_of(target);
            if let Some(node) = self.nodes.get_mut(&target) {
                node.view.merge(&updates, now, run);
            }
        }
    }

    fn sample(&mut self, disk: u64) {
        let sense = self.sense(disk);
        let Some(node) = self.nodes.get_mut(&disk) else {
            return;
        };
        let Sampled {
            before,
            count,
            decided,
            acts,
            held,
            committed,
            member,
            refused,
        } = node_sample(node, &sense);
        let now = sense.now;
        if before != count {
            self.count_changes.push(CountChange {
                now_ms: now,
                run_ms: self.run_ms,
                disk,
                from: before,
                to: count,
            });
        }
        if let Some(to) = decided.filter(|_| acts) {
            let cause = if to.epoch > held.epoch {
                Cause::Quiet
            } else {
                Cause::Rise
            };
            self.cmd(disk, Cmd::Floor { to, cause });
        }
        if let Some(d) = self.disks.get_mut(&disk) {
            d.member = Some(member);
            if !refused {
                d.committed = committed;
            }
        }
        if refused {
            self.refused += 1;
            self.down(disk);
        } else {
            self.cmd(disk, Cmd::WriteGroups);
        }
    }
}

pub fn mix(seed: u64, events: usize) -> World {
    let mut w = World::new(seed);
    w.watch = true;
    w.drop = 0.01;
    w.delay_ms = 800;
    let n = w.rng.random_range(4..=8);
    let zones = w.rng.random_range(0..=3);
    w.start(n, zones, 25 * HOUR_MS);
    w.fill(1 << 30);
    w.advance(60_000);
    let mut down: Vec<(u64, u64)> = Vec::new();
    for _ in 0..events {
        let running = w.running();
        let pick = running.choose(&mut w.rng).copied();
        let later = w.rng.random_range(30_000..900_000);
        let signal = [SPOT, REBALANCE, LIFECYCLE][w.rng.random_range(0..3)];
        match (w.rng.random_range(0..14), pick) {
            (0, _) => {
                w.publish();
            }
            (1, _) => w.settle(),
            (2, Some(d)) if running.len() > 2 => {
                w.kill(d);
                down.push((d, w.now_ms + later));
            }
            (3, Some(d)) if running.len() > 2 => {
                w.stop(d);
                down.push((d, w.now_ms + later.max(200_000)));
            }
            (4, Some(d)) => w.signal(d, signal),
            (5, Some(d)) if running.len() > 2 => {
                w.delete(d);
                w.create("", 0, 1);
            }
            (6, _) => {
                for d in &running {
                    w.kill(*d);
                    down.push((*d, w.now_ms + later));
                }
            }
            (7, Some(d)) => {
                if let Some(other) = running.choose(&mut w.rng).copied().filter(|o| *o != d) {
                    let cut = pair(d, other);
                    if !w.cuts.remove(&cut) {
                        w.cut(&[cut]);
                    }
                }
            }
            (8, _) => {
                let ids: Vec<u64> = w.segments.keys().copied().collect();
                if let Some(id) = ids.choose(&mut w.rng).copied()
                    && !w.held_repair.remove(&id)
                {
                    w.held_repair.insert(id);
                }
            }
            (9, Some(d)) if running.len() > 3 => w.drain(d),
            (10, _) => {
                let gone: Vec<u64> = w
                    .disks()
                    .into_iter()
                    .filter(|d| !w.nodes.contains_key(d))
                    .collect();
                if let Some(d) = gone.choose(&mut w.rng).copied() {
                    w.accept_loss(d);
                }
            }
            (11, _) => {
                let hours = w.rng.random_range(1..=7);
                w.quiet_hours(hours);
            }
            (12, _) => {
                w.create("", 0, 1);
            }
            (13, _) if running.len() >= 3 => {
                if w.sides.is_empty() {
                    let mut disks = w.disks();
                    disks.shuffle(&mut w.rng);
                    let k = w.rng.random_range(1..=(disks.len() - 1) / 2);
                    w.part(&disks[..k]);
                } else {
                    w.heal();
                }
            }
            _ => {}
        }
        let wait = w.rng.random_range(1..=30) * 1_000;
        w.advance(wait);
        let now = w.now_ms;
        let due: Vec<u64> = down
            .iter()
            .filter(|(_, at)| *at <= now)
            .map(|(d, _)| *d)
            .collect();
        down.retain(|(_, at)| *at > now);
        for d in due {
            w.boot(d, 1);
        }
    }
    w.heal();
    w.cuts.clear();
    w.held_repair.clear();
    w.quiet_hours(7);
    w.advance(60_000);
    w
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_from_an_older_raft_term_is_not_kept() {
        let mut w = World::new(1);
        let ids = w.start(3, 3, 25 * HOUR_MS);
        w.advance(30_000);
        let to = ids[0];
        let term = w.nodes[&to].raft.term();
        assert!(term > 0);
        let frame = |w: &World, term| Frame {
            at: 0,
            sent: w.now_ms,
            from: ids[1],
            to,
            version: 0,
            msg: Msg::Fresh(0, term),
        };
        w.nodes.get_mut(&to).expect("node").fresh.clear();
        let old = frame(&w, term - 1);
        w.receive(old);
        assert!(w.nodes[&to].fresh.is_empty());
        let current = frame(&w, term);
        w.receive(current);
        assert_eq!(w.nodes[&to].fresh.len(), 1);
    }
}
