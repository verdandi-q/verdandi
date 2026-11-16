use std::collections::{BTreeMap, BTreeSet};
use std::hash::{DefaultHasher, Hash, Hasher};

use raft::eraftpb::{ConfChangeType, Message};
use rand::seq::{IndexedRandom, SliceRandom};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use verdandi_control::{Command, Control, Meta, Saved};
use verdandi_core::cluster::controller::{self, Controller, Floor, QUIET_PERIOD, START, refuse};
use verdandi_core::cluster::domains::{self, Domains};
use verdandi_core::cluster::fence::{self, Answer, Block, FENCE_MS, Fate, Known, Marks, confirmed};
use verdandi_core::cluster::placement::{self, Candidate, Groups, HolderSet};
use verdandi_core::cluster::repair::{self, DIGEST_MS, Held, Limiter, Step, Want};
use verdandi_core::cluster::steer::{self, Hold};
use verdandi_core::cluster::view::{
    Body, FAIL_MS, GOSSIP_MS, HEARTBEAT_MS, Head, JOIN_MS, LIFECYCLE, REBALANCE, SAMPLE_MS,
    SIGTERM, SPOT, STOP_BUDGET_MS, State, TOMBSTONE_HORIZON, Update, View,
};

pub const STEP_MS: u64 = HEARTBEAT_MS;
pub const SEGMENT_BYTES: u64 = 64 << 20;
pub const HOUR_MS: u64 = 3_600_000;
const FANOUT: usize = 3;
const HELLO_MS: u64 = 10_000;
const REPLACE_MS: u64 = 30_000;
const REPLICATE_MS: u64 = 5_000;
const FOUNDING_STEPS: u64 = 600;
const HEARD_MS: u64 = 10 * STEP_MS;
const LEASE_MS: u64 = 3_000;
const FLAP_MS: u64 = 13_500;
const SLACK_MS: u64 = FAIL_MS + 2 * SAMPLE_MS + GOSSIP_MS;
const STREAMS: u64 = 3;
const STALL_WINDOW_MS: u64 = 60_000;
const STOPS: u8 = SIGTERM | LIFECYCLE;
const RECORDS_MAX: usize = 64;
const FENCES_MAX: usize = 64;
const SEG_ASKS_MAX: usize = 64;
const DROPS_MAX: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cause {
    Rise,
    Quiet,
    Merge,
    Drain,
    AcceptLoss,
}

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

struct Job {
    copies: usize,
    segment: u64,
    from: u64,
    need: usize,
    to: Vec<u64>,
}

#[derive(Clone, Copy, Debug)]
pub struct CountChange {
    pub now_ms: u64,
    pub run_ms: u64,
    pub disk: u64,
    pub from: u8,
    pub to: u8,
}

#[derive(Clone, Debug)]
enum Msg {
    Gossip(Vec<Update>),
    Ping(Head),
    Pong(u64, Head),
    Raft(Message),
    Lose(u64),
    Record(u64, u64, Rec, BTreeSet<u64>),
    Ack(u64, u64, u64, u64),
    Acked(u64, bool, u64),
    Wish(HolderSet),
    SegAsk(u64, u64),
    Seg(u64, Option<u64>, Known, u64),
    Beat(u64, u64),
    Copied(u64, u64, u64),
    Beaten(u64, u64, u64),
    Drop(u64),
    Dropped(u64),
    FenceAsk(u64, BTreeSet<u64>, u64),
    FenceIs(u64, u64, BTreeSet<u64>, Marks, bool, BTreeSet<u64>),
    Confirm(u64, u64, u64, u64),
    VoteAsk(u64),
    Vote(BTreeSet<u64>, u64),
    MarksAsk,
    MarksIs(Marks, Marks, BTreeSet<(u64, u64)>),
    RecordsAsk(Marks, Option<(u64, u64)>),
    Records(Vec<((u64, u64), u64, u64)>, Marks, bool),
}

#[derive(Clone, Debug)]
enum Rec {
    Delete((u64, u64), u64, u64),
}

fn groups_of(node: &Node) -> Option<&Groups> {
    Some(&node.raft.meta().groups).filter(|g| g.epoch > 0)
}

struct Request {
    origin: u64,
    restart: u64,
    rec: Rec,
    started_ms: u64,
    scope: BTreeSet<u64>,
    confirmed: BTreeSet<u64>,
    newest: u64,
}

#[derive(Default)]
struct Lease {
    epoch: u64,
    from: BTreeSet<u64>,
    synced_ms: Option<u64>,
    beats: BTreeMap<u64, u64>,
}

struct Settle {
    leader: u64,
    epoch: u64,
    restart: u64,
    segment: u64,
    next: u64,
    started_ms: u64,
    scope: BTreeSet<u64>,
    sets: Vec<BTreeSet<u64>>,
    acked: BTreeMap<u64, bool>,
}

struct Frame {
    at: u64,
    sent: u64,
    from: u64,
    to: u64,
    version: u64,
    msg: Msg,
}

type Vote = (BTreeSet<u64>, u64);
type PeerMarks = (Marks, Marks, u64, u64, BTreeSet<(u64, u64)>);
type FenceAnswer = (BTreeSet<u64>, Marks, u64, BTreeSet<u64>);
type SegAnswer = (Known, Marks, u64);
type Lift = (u64, Option<(u64, State)>, u64);
type Queued = ((u64, u64), u64);

struct Disk {
    restart: u64,
    floor: Floor,
    committed: u16,
    alive_ms: u64,
    zone: String,
    join_ms: u64,
    member: Option<Vec<u8>>,
    control: Saved,
    records: BTreeMap<(u64, u64), (u64, u64, u64)>,
    marks: Marks,
    forgot: Marks,
    sequence: u64,
    dropped: BTreeMap<u64, u64>,
    erased: BTreeSet<u64>,
    epochs: BTreeMap<u64, u64>,
}

struct Node {
    view: View,
    ctl: Controller,
    domains: Option<Domains>,
    format_max: u16,
    signals: u8,
    phase_ms: u64,
    boot_run_ms: u64,
    proposed_groups: Option<(u64, u64)>,
    stop_run_ms: Option<u64>,
    heavy: BTreeMap<u64, (Hold, u64)>,
    trips: BTreeMap<u64, (u64, u64)>,
    full: bool,
    reconciled: bool,
    joined: bool,
    fences: BTreeMap<u64, BTreeSet<u64>>,
    unreachable: BTreeMap<u64, Option<u64>>,
    opened: Option<Floor>,
    proposed: Option<(u64, Floor)>,
    fall: Option<Cause>,
    raft: Control,
    heard_ms: Option<u64>,
    adopted_ms: u64,
    lose: BTreeSet<u64>,
    booted_ms: u64,
    proposed_segs: BTreeSet<(u64, u64, u64, bool)>,
    lease: BTreeMap<u64, Lease>,
    changing: BTreeMap<u64, HolderSet>,
    fence_answers: BTreeMap<(u64, u64), FenceAnswer>,
    seen_records: BTreeMap<u64, u64>,
    records_asked: BTreeMap<u64, u64>,
    new_member: bool,
    booting: BTreeSet<u64>,
    votes: BTreeMap<u64, Vote>,
    vote_ms: Option<u64>,
    peers: BTreeMap<u64, PeerMarks>,
    reconcile_ms: Option<u64>,
    late: BTreeMap<u64, u64>,
    named: BTreeMap<(u64, u64), u64>,
    caught: BTreeMap<(u64, u64), u64>,
    lifts: BTreeMap<(u64, u64), Lift>,
    seg_answers: BTreeMap<(u64, u64), SegAnswer>,
    seg_queue: BTreeMap<(u64, u64), u64>,
    seg_asked: BTreeMap<(u64, u64), u64>,
    drops: BTreeMap<u64, BTreeSet<u64>>,
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
    requests: BTreeMap<u64, Request>,
    settles: BTreeMap<u64, Settle>,
    drop_waits: BTreeMap<u64, (u64, u64, BTreeSet<u64>)>,
    next_request: u64,
    wishes: BTreeMap<(u64, u64), (u64, HolderSet)>,
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

fn heard(node: &Node, id: u64, m: &verdandi_control::Segment) -> bool {
    let from = node
        .lease
        .get(&id)
        .filter(|l| l.epoch == m.leader_epoch)
        .map_or(0, |l| l.from.len());
    fence::confirmed(from + 1, m.holders.holders.len())
}

fn synced(node: &Node, id: u64, m: &verdandi_control::Segment) -> bool {
    m.leader_epoch == 1 || heard(node, id, m)
}

fn enough_in(set: &BTreeSet<u64>, leader: u64, stored: &BTreeSet<u64>) -> bool {
    let copies = set
        .iter()
        .filter(|h| **h == leader || stored.contains(h))
        .count();
    fence::enough(copies, set.len())
}

fn fingerprint(set: &HolderSet) -> u64 {
    let mut h = DefaultHasher::new();
    (set.epoch, set.writer, &set.holders).hash(&mut h);
    h.finish()
}

fn pair(a: u64, b: u64) -> (u64, u64) {
    (a.min(b), a.max(b))
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
            requests: BTreeMap::new(),
            settles: BTreeMap::new(),
            drop_waits: BTreeMap::new(),
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

    pub fn acts(&self, disk: u64) -> bool {
        self.nodes.get(&disk).is_some_and(|n| !n.full)
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
            .filter(|(_, n)| !n.full && n.reconciled)
            .map(|(d, _)| {
                self.records
                    .iter()
                    .filter(|(origin, seq)| {
                        let disk = &self.disks[d];
                        !disk.records.contains_key(&(*origin, *seq))
                            && disk.marks.get(origin).copied().unwrap_or(0) < *seq
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
            && self.newest().is_some_and(|m| {
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
            let holders: Vec<u64> = seg
                .holders
                .holders
                .iter()
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
            let alone = holders.len() < usize::from(controller::KEYED_MIN_COPIES)
                || seg.holders.holders.iter().any(|x| {
                    let pre: Vec<u64> = holders
                        .iter()
                        .copied()
                        .filter(|h| {
                            calm(*h)
                                && self.linked_ms.get(&pair(*h, *x)).is_some_and(|t| {
                                    seg.copied.get(h).is_some_and(|c| c < t)
                                        && self.nodes[h].booted_ms < *t
                                })
                        })
                        .collect();
                    let missed = |h: &u64| {
                        let since = self
                            .linked_ms
                            .get(&pair(*h, *x))
                            .map_or(0, |t| t + 2 * FENCE_MS);
                        self.applied_ms.get(&(*id, *h)).is_none_or(|t| {
                            *t <= since
                                || *t <= self.nodes[h].booted_ms
                                || seg.copied.get(h).is_some_and(|c| t <= c)
                        })
                    };
                    !self.nodes.contains_key(x)
                        && self.in_list(*x)
                        && ready.iter().all(missed)
                        && !pre.iter().any(|a| {
                            fence::quorum(
                                pre.iter().filter(|b| *b != a && self.link(*a, **b)).count(),
                                seg.holders.holders.len(),
                            )
                        })
                });
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

    pub fn full_fence(&self, disk: u64) -> bool {
        self.nodes.get(&disk).is_some_and(|n| n.full)
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
                control: Saved::default(),
                records: BTreeMap::new(),
                marks: Marks::new(),
                forgot: Marks::new(),
                sequence: 0,
                dropped: BTreeMap::new(),
                erased: BTreeSet::new(),
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
                disk.control = saved;
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
            Control::restore(disk, d.control.clone()).expect("the saved control state restores");
        let body = self.body(disk, format_max);
        let phase_ms = self.rng.random_range(0..SAMPLE_MS / STEP_MS) * STEP_MS;
        let (view, count) = match member {
            None => (View::new(disk, restart, body, self.now_ms, run), 0),
            Some(bytes) => View::restored(disk, restart, body, self.now_ms, run, &bytes)
                .expect("the saved member state decodes"),
        };
        let mut ctl = Controller::new(true, floor, committed);
        ctl.count = count;
        self.nodes.insert(
            disk,
            Node {
                view,
                ctl,
                domains: None,
                format_max,
                signals: 0,
                phase_ms,
                boot_run_ms: run,
                proposed_groups: None,
                stop_run_ms: None,
                heavy: BTreeMap::new(),
                trips: BTreeMap::new(),
                full: true,
                reconciled: false,
                joined: false,
                fences: BTreeMap::new(),
                unreachable: BTreeMap::new(),
                opened: None,
                proposed: None,
                fall: None,
                raft,
                heard_ms: None,
                adopted_ms: self.now_ms,
                lose: BTreeSet::new(),
                booted_ms: self.now_ms,
                proposed_segs: BTreeSet::new(),
                lease: BTreeMap::new(),
                changing: BTreeMap::new(),
                fence_answers: BTreeMap::new(),
                seen_records: BTreeMap::new(),
                records_asked: BTreeMap::new(),
                new_member: false,
                booting: BTreeSet::new(),
                votes: BTreeMap::new(),
                vote_ms: None,
                peers: BTreeMap::new(),
                reconcile_ms: None,
                late: BTreeMap::new(),
                named: BTreeMap::new(),
                caught: BTreeMap::new(),
                lifts: BTreeMap::new(),
                seg_answers: BTreeMap::new(),
                seg_queue: BTreeMap::new(),
                seg_asked: BTreeMap::new(),
                drops: BTreeMap::new(),
            },
        );
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
            self.catch_up(disk, id);
            let dropped = self
                .set_of(disk, id)
                .is_some_and(|set| !set.holders.contains(&disk));
            if dropped && let Some(seg) = self.segments.get_mut(&id) {
                seg.marks.remove(&disk);
            }
        }
        self.drained.remove(&disk);
        true
    }

    fn record(&mut self, disk: u64, kind: LossKind) {
        let held = self
            .nodes
            .get(&disk)
            .is_some_and(|n| n.view.own().body.holds > 0);
        if held && !self.drained.contains(&disk) {
            self.losses.push(WorldLoss {
                disk,
                run_ms: self.run_ms,
                kind,
                side: self.side(disk),
            });
        }
    }

    pub fn kill(&mut self, disk: u64) {
        self.record(disk, LossKind::Kill);
        self.down(disk);
    }

    fn down(&mut self, disk: u64) {
        self.nodes.remove(&disk);
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

    pub fn signal(&mut self, disk: u64, signal: u8) {
        let (now, run) = (self.now_ms, self.run_of(disk));
        if signal != SIGTERM {
            self.record(disk, LossKind::Signal);
        }
        if let Some(node) = self.nodes.get_mut(&disk) {
            node.signals |= signal;
            let mut body = node.view.own().body.clone();
            body.signals = node.signals;
            node.view.set_body(body, now, run);
        }
    }

    pub fn stop(&mut self, disk: u64) {
        self.signal(disk, SIGTERM);
        let deadline = self.run_ms + STOP_BUDGET_MS;
        if let Some(node) = self.nodes.get_mut(&disk) {
            node.stop_run_ms.get_or_insert(deadline);
            self.stopped_at.entry(disk).or_insert(self.now_ms);
        }
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
        let remaining = self.remaining(disk);
        let drained = self.drained.contains(&disk);
        if remaining > 0 && !drained {
            self.record(disk, LossKind::Left);
        }
        let (now, run) = (self.now_ms, self.run_of(disk));
        let Some(node) = self.nodes.get_mut(&disk) else {
            return;
        };
        node.view.leave(remaining as u32, now, run);
        let restart = node.view.own().head.restart;
        let own = node
            .view
            .answer(&[])
            .into_iter()
            .filter(|u| u.disk == disk)
            .collect::<Vec<_>>();
        for peer in self.running() {
            if peer != disk && self.sends(disk, peer) {
                let at = now + self.rng.random_range(0..=self.delay_ms);
                self.gossip_to(disk, peer, at, own.clone());
                if let Some(p) = self.nodes.get_mut(&peer) {
                    p.trips.insert(disk, (restart, at));
                }
            }
        }
        self.exits.insert(disk, now);
        self.down(disk);
    }

    pub fn drain(&mut self, disk: u64) {
        self.drained.insert(disk);
        let (now, run) = (self.now_ms, self.run_of(disk));
        if let Some(node) = self.nodes.get_mut(&disk) {
            node.view.drained(disk, now, run);
        }
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
        let Some(node) = self.nodes.get_mut(&origin) else {
            return false;
        };
        if node.view.entry(disk).is_none() {
            return !node.raft.meta().members.contains(&disk);
        }
        if !node.view.absent(disk) {
            return false;
        }
        node.lose.insert(disk);
        true
    }

    fn propose_floor(&mut self, disk: u64, to: Floor, cause: Cause) {
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
        let Some(node) = self.nodes.get_mut(&disk) else {
            return;
        };
        let key = (node.raft.term(), to);
        if node.proposed == Some(key) {
            return;
        }
        node.proposed = Some(key);
        let from = node.raft.meta().floor;
        self.raft_refused += u64::from(node.raft.propose(&Command::Floor(to)).is_err());
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
        self.flush(disk);
    }

    fn fall(&mut self, disk: u64) {
        let Some(node) = self.nodes.get_mut(&disk) else {
            return;
        };
        let Some(cause) = node.fall else {
            return;
        };
        if !node.raft.is_leader() {
            node.fall = None;
            return;
        }
        match node.ctl.lower() {
            Some(to) => {
                node.ctl.floor = node.raft.meta().floor;
                self.propose_floor(disk, to, cause);
            }
            None => node.fall = None,
        }
    }

    fn floor_applied(&mut self, disk: u64, from: Floor, to: Floor) {
        let Some(node) = self.nodes.get_mut(&disk) else {
            return;
        };
        node.ctl.floor = to;
        node.proposed = None;
        if let Some(d) = self.disks.get_mut(&disk) {
            d.floor = to;
        }
        let change = match self.decisions.remove(&to) {
            Some(made) => FloorChange { disk, from, ..made },
            None => FloorChange {
                run_ms: self.run_ms,
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

    fn debt(&self, disk: u64, count: u8) -> u64 {
        self.segments
            .values()
            .filter(|s| {
                s.marks.contains_key(&disk)
                    && s.holders.holders.contains(&disk)
                    && self.live_copies(s) < usize::from(count)
            })
            .map(|s| s.bytes)
            .sum()
    }

    pub fn place(&mut self, holders: &[u64], bytes: u64) -> u64 {
        self.store(holders[0], holders, holders, bytes)
    }

    fn store(&mut self, writer: u64, holders: &[u64], stored: &[u64], bytes: u64) -> u64 {
        let id = self.next_segment;
        self.next_segment += 1;
        let format = self.nodes.get(&writer).map_or(1, |n| n.ctl.committed);
        for h in stored {
            if let Some(node) = self.nodes.get_mut(h) {
                node.named
                    .extend(holders.iter().map(|c| ((id, *c), self.now_ms)));
            }
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
                copied: stored.iter().map(|h| (*h, self.now_ms)).collect(),
                holders: set,
                bytes,
                format,
                born_run_ms: self.run_ms,
                born_ms: self.now_ms,
                lost_run_ms: 0,
                marks: stored.iter().map(|h| (*h, 0)).collect(),
                born: holders.iter().copied().collect(),
                stream: id % STREAMS,
            },
        );
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
        let node = &self.nodes[&writer];
        if !node.joined || !node.raft.meta().members.contains(&writer) {
            self.refused_writes += 1;
            return None;
        }
        let groups = groups_of(node)?;
        let late: Vec<u64> = node.unreachable.keys().copied().collect();
        let mut holders = groups.new_holders(writer, self.next_segment, &late);
        let confirmed: BTreeSet<u64> = holders
            .iter()
            .copied()
            .filter(|h| *h != writer && self.link(writer, *h))
            .collect();
        let floor = node.ctl.floor;
        let opened = node.opened.unwrap_or(floor);
        match fence::block(
            writer,
            &holders,
            &confirmed,
            |h| node.view.absent(h),
            true,
            usize::from(groups.size),
            opened,
            floor,
        ) {
            Block::Ack(_) => {
                holders.retain(|h| *h == writer || !node.view.absent(*h));
                if let Some(node) = self.nodes.get_mut(&writer) {
                    node.opened = Some(floor);
                }
                let mut stored = vec![writer];
                stored.extend(&confirmed);
                self.count_domains(writer);
                Some(self.store(writer, &holders, &stored, SEGMENT_BYTES))
            }
            Block::Refuse(late) => {
                self.refused_writes += 1;
                self.resealed += u64::from(
                    late.is_empty()
                        && opened != floor
                        && confirmed.len() + 1 >= usize::from(controller::KEYED_MIN_COPIES),
                );
                if let Some(node) = self.nodes.get_mut(&writer) {
                    node.unreachable.extend(late.into_iter().map(|h| (h, None)));
                    node.opened = None;
                }
                None
            }
            Block::Wait => None,
        }
    }

    pub fn set_of(&self, n: u64, id: u64) -> Option<&HolderSet> {
        self.nodes
            .get(&n)?
            .raft
            .meta()
            .segments
            .get(&id)
            .map(|s| &s.holders)
            .filter(|h| h.epoch > 0)
    }

    fn epoch_of(&self, n: u64, id: u64) -> u64 {
        self.nodes
            .get(&n)
            .and_then(|node| node.raft.meta().segments.get(&id))
            .map_or(0, |m| m.leader_epoch)
    }

    fn open_epoch(&self, n: u64, id: u64) -> u64 {
        let held = self
            .disks
            .get(&n)
            .and_then(|d| d.epochs.get(&id))
            .copied()
            .unwrap_or(0);
        held.max(self.epoch_of(n, id))
    }

    fn hold_epoch(&mut self, n: u64, id: u64, epoch: u64) -> u64 {
        if let Some(d) = self.disks.get_mut(&n) {
            let held = d.epochs.entry(id).or_insert(0);
            *held = (*held).max(epoch);
        }
        self.open_epoch(n, id)
    }

    fn leads(&self, n: u64, id: u64) -> bool {
        let (Some(node), Some(seg)) = (self.nodes.get(&n), self.segments.get(&id)) else {
            return false;
        };
        let Some(m) = node.raft.meta().segments.get(&id) else {
            return false;
        };
        let holders = &m.holders.holders;
        m.leader == n
            && holders.contains(&n)
            && seg.marks.contains_key(&n)
            && self.open_epoch(n, id) <= m.leader_epoch
            && node.lease.get(&id).is_some_and(|l| {
                let beats = holders
                    .iter()
                    .filter(|h| {
                        **h != n && l.beats.get(h).is_some_and(|t| self.now_ms < t + LEASE_MS)
                    })
                    .count();
                l.epoch == m.leader_epoch
                    && fence::confirmed(beats + 1, holders.len())
                    && (m.leader_epoch == 1
                        || l.synced_ms
                            .is_some_and(|t| self.now_ms >= t + LEASE_MS + STEP_MS))
            })
    }

    fn send_beats(&mut self) {
        let now = self.now_ms;
        let mut beats = Vec::new();
        for (n, node) in &self.nodes {
            for (id, m) in &node.raft.meta().segments {
                if m.leader == *n && m.holders.holders.contains(n) && synced(node, *id, m) {
                    beats.push((*n, *id, m.leader_epoch, m.holders.holders.clone()));
                }
            }
        }
        for (n, id, epoch, holders) in beats {
            if let Some(node) = self.nodes.get_mut(&n) {
                let lease = node.lease.entry(id).or_default();
                if lease.epoch != epoch {
                    *lease = Lease {
                        epoch,
                        ..Lease::default()
                    };
                }
                lease.synced_ms.get_or_insert(now);
            }
            for h in holders.into_iter().filter(|h| *h != n) {
                self.send(n, h, Msg::Beat(id, epoch));
            }
        }
    }

    fn add_holder(&mut self, leader: u64, own: &HolderSet, new: u64) {
        if [3,4,5].contains(&own.segment) { eprintln!("ADD {} leader={leader} seg={} own={:?} new={new}", self.now_ms, own.segment, own); }
        let mut holders = own.holders.clone();
        holders.insert(new);
        let set = own.with(leader, holders);
        if let Some(node) = self.nodes.get_mut(&leader) {
            node.changing.insert(own.segment, set.clone());
        }
        self.wish(leader, set);
    }

    fn wish(&mut self, by: u64, set: HolderSet) {
        self.wishes
            .entry((set.segment, set.epoch))
            .or_insert((by, set));
    }

    fn open_wishes(&mut self) {
        let keys: Vec<(u64, u64)> = self.wishes.keys().copied().collect();
        for key in keys {
            let applied = self.nodes.values().any(|n| {
                n.raft
                    .meta()
                    .segments
                    .get(&key.0)
                    .is_some_and(|s| s.holders.epoch >= key.1)
            });
            let (by, set) = self.wishes[&key].clone();
            if applied {
                self.wishes.remove(&key);
                continue;
            }
            let Some(sender) = Some(by).filter(|b| self.nodes.contains_key(b)).or_else(|| {
                set.holders
                    .iter()
                    .copied()
                    .find(|h| self.nodes.contains_key(h))
            }) else {
                continue;
            };
            let node = &self.nodes[&sender];
            if node.raft.is_leader() {
                self.propose_segment(sender, Command::Holders(set));
            } else if let Some(leader) = node.raft.leader() {
                self.send(sender, leader, Msg::Wish(set));
            }
        }
    }

    fn propose_segment(&mut self, disk: u64, command: Command) {
        let (segment, epoch, leader) = match &command {
            Command::Holders(h) => (h.segment, h.epoch, false),
            Command::Leader { segment, epoch, .. } => (*segment, *epoch, true),
            _ => return,
        };
        let Some(node) = self.nodes.get_mut(&disk).filter(|n| n.raft.is_leader()) else {
            return;
        };
        if !node
            .proposed_segs
            .insert((node.raft.term(), segment, epoch, leader))
        {
            return;
        }
        self.raft_refused += u64::from(node.raft.propose(&command).is_err());
        self.flush(disk);
        if let Command::Holders(h) = command
            && h.epoch == 1
        {
            self.propose_segment(
                disk,
                Command::Leader {
                    segment,
                    epoch: 1,
                    disk: h.writer,
                },
            );
        }
    }

    fn lead_segments(&mut self, disk: u64) {
        let Some(node) = self.nodes.get(&disk).filter(|n| n.raft.is_leader()) else {
            return;
        };
        let pairs = self.reported(disk);
        let copy = |id: u64, h: u64| self.segments[&id].marks.contains_key(&h);
        let usable =
            |id: u64, h: u64| self.serving(h) && self.live_reconciled(disk, h) && copy(id, h);
        let picks: Vec<Command> = node
            .raft
            .meta()
            .segments
            .iter()
            .filter(|(id, s)| {
                s.holders.epoch > 0
                    && !(s.holders.holders.contains(&s.leader)
                        && usable(**id, s.leader)
                        && !s.holders.holders.iter().any(|o| {
                            *o != s.leader && pairs.contains(&placement::pair(s.leader, *o))
                        }))
            })
            .filter_map(|(id, s)| {
                let leader = placement::leader(0, &s.holders, |h| usable(*id, h), &pairs)?;
                Some(Command::Leader {
                    segment: *id,
                    epoch: s.leader_epoch + 1,
                    disk: leader,
                })
            })
            .collect();
        for command in picks {
            self.propose_segment(disk, command);
        }
    }

    fn catch_up(&mut self, disk: u64, id: u64) {
        let Some(own) = self.set_of(disk, id) else {
            return;
        };
        let peers: Vec<u64> = own.holders.iter().copied().filter(|h| *h != disk).collect();
        for p in peers {
            self.ask_seg(disk, id, p);
        }
    }

    fn ask_seg(&mut self, disk: u64, id: u64, peer: u64) {
        let now = self.now_ms;
        if let Some(node) = self.nodes.get_mut(&disk)
            && node
                .seg_asked
                .get(&(id, peer))
                .is_none_or(|t| now >= t + STEP_MS)
        {
            node.seg_queue.entry((id, peer)).or_insert(now);
        }
    }

    fn send_seg_asks(&mut self) {
        let now = self.now_ms;
        let mut asks = Vec::new();
        let ids: Vec<u64> = self.nodes.keys().copied().collect();
        for n in ids {
            let node = &self.nodes[&n];
            let (mut due, far): (Vec<Queued>, Vec<Queued>) = node
                .seg_queue
                .iter()
                .map(|(k, t)| (*k, *t))
                .partition(|((_, p), _)| self.near(n, *p));
            due.sort_by_key(|(k, t)| (*t, *k));
            let due: Vec<(u64, u64)> = due.into_iter().take(SEG_ASKS_MAX).map(|(k, _)| k).collect();
            let far: Vec<(u64, u64)> = far.into_iter().map(|(k, _)| k).collect();
            let Some(node) = self.nodes.get_mut(&n) else {
                continue;
            };
            for key in far {
                node.seg_queue.remove(&key);
            }
            for key in due {
                node.seg_queue.remove(&key);
                node.seg_asked.insert(key, now);
                asks.push((n, key));
            }
        }
        for (n, (id, peer)) in asks {
            let epoch = self.epoch_of(n, id);
            self.send(n, peer, Msg::SegAsk(id, epoch));
        }
    }

    fn sends(&self, from: u64, to: u64) -> bool {
        from == to
            || (self.nodes.contains_key(&to)
                && !self.cuts.contains(&pair(from, to))
                && !self.oneway.contains(&(from, to)))
    }

    fn link(&self, from: u64, to: u64) -> bool {
        self.sends(from, to) && (from == to || !self.oneway.contains(&(to, from)))
    }

    fn live_reconciled(&self, by: u64, disk: u64) -> bool {
        match self.nodes.get(&by) {
            Some(node) if by == disk => node.reconciled && !node.full,
            Some(node) => node
                .view
                .entry(disk)
                .is_some_and(|e| e.head.state == State::Alive && e.body.reconciled),
            None => false,
        }
    }

    fn reported(&self, by: u64) -> BTreeSet<(u64, u64)> {
        let Some(node) = self.nodes.get(&by) else {
            return self.pairs.clone();
        };
        let mut pairs = self.pairs.clone();
        for (disk, entry) in node.view.live() {
            pairs.extend(entry.body.unreachable.iter().map(|u| pair(disk, *u)));
        }
        pairs.retain(|(a, b)| !node.view.absent(*a) && !node.view.absent(*b));
        pairs
    }

    fn show(&mut self, n: u64, id: u64) {
        let Some(seg) = self.segments.get(&id) else {
            return;
        };
        if !seg.marks.contains_key(&n) || self.disks[&n].erased.contains(&id) {
            return;
        }
        let gone = self.drops_done.contains(&id)
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

    pub fn take(&mut self) {
        let ids: Vec<u64> = self.acked.iter().copied().collect();
        for id in ids {
            let leaders: Vec<u64> = self
                .running()
                .into_iter()
                .filter(|n| self.leads(*n, id))
                .collect();
            if leaders.len() > 1 {
                self.double_leaders += 1;
            }
            let serving: Vec<u64> = leaders
                .into_iter()
                .filter(|n| self.live_reconciled(*n, *n))
                .collect();
            for n in serving {
                let mark = self.segments[&id].marks[&n];
                if self.settled.get(&id).is_some_and(|s| mark < *s) {
                    self.redelivered += 1;
                }
                self.show(n, id);
            }
        }
    }

    pub fn read(&mut self) {
        let Some(reader) = self.running().choose(&mut self.rng).copied() else {
            return;
        };
        let ids: Vec<u64> = self.acked.iter().copied().collect();
        for id in ids {
            let seg = &self.segments[&id];
            let asked: Vec<u64> = seg
                .marks
                .keys()
                .copied()
                .filter(|p| self.link(reader, *p) && self.live_reconciled(reader, *p))
                .collect();
            let open: Vec<u64> = asked
                .iter()
                .copied()
                .filter(|p| {
                    self.live_reconciled(*p, *p)
                        && self.set_of(*p, id).is_some_and(|s| s.holders.contains(p))
                        && !self.segment_fenced(
                            *p,
                            id,
                            &mut self.nodes[p].fences.get(&id).cloned().unwrap_or_default(),
                        )
                })
                .collect();
            let mut parts: Vec<u64> = open
                .iter()
                .copied()
                .filter(|p| self.picks(*p, id))
                .collect();
            if parts.is_empty() {
                parts.extend(placement::pick(&[id], open));
            }
            let partial = parts.is_empty() && !asked.is_empty();
            self.partial_reads += u64::from(partial);
            if parts.is_empty() && !partial && self.readable(reader, id) {
                self.incomplete_reads += 1;
            }
            for p in parts {
                self.show(p, id);
            }
        }
    }

    fn readable(&self, reader: u64, id: u64) -> bool {
        let seg = &self.segments[&id];
        let deleted = self
            .deleted
            .iter()
            .any(|(stream, upto)| seg.stream == *stream && id <= *upto);
        !self.dropped.contains(&id)
            && !deleted
            && seg.marks.keys().any(|p| {
                self.link(reader, *p)
                    && self.live_reconciled(reader, *p)
                    && self.set_of(*p, id).is_some_and(|s| s.holders.contains(p))
            })
    }

    pub fn copying_of(&self, id: u64) -> bool {
        self.copying.keys().any(|(s, _)| *s == id)
    }

    fn picks(&self, p: u64, id: u64) -> bool {
        let seg = &self.segments[&id];
        self.set_of(p, id).is_some_and(|set| {
            placement::pick(
                &[id],
                set.holders
                    .iter()
                    .copied()
                    .filter(|h| seg.marks.contains_key(h) && self.live_reconciled(p, *h)),
            ) == Some(p)
        })
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
        while self.requests.contains_key(&id) {
            self.advance(STEP_MS);
        }
        self.done.iter().any(|d| d.0 == id)
    }

    fn delete_start(&mut self, origin: u64, stream: u64) -> Option<u64> {
        if !self.live_reconciled(origin, origin) {
            return None;
        }
        let d = self.disks.get_mut(&origin)?;
        d.sequence += 1;
        let key = (origin, d.sequence);
        let upto = self.next_segment - 1;
        Some(self.announce(origin, Rec::Delete(key, stream, upto)))
    }

    fn apply(&mut self, to: u64, key: (u64, u64), stream: u64, upto: u64) {
        let covered: Vec<u64> = self
            .segments
            .iter()
            .filter(|(id, s)| s.stream == stream && **id <= upto && s.marks.contains_key(&to))
            .map(|(id, _)| *id)
            .collect();
        let Some(d) = self.disks.get_mut(&to) else {
            return;
        };
        let at = d.alive_ms;
        d.records.entry(key).or_insert((stream, upto, at));
        let mark = d.marks.entry(key.0).or_insert(0);
        while d.records.contains_key(&(key.0, *mark + 1)) {
            *mark += 1;
        }
        d.erased.extend(covered);
    }

    pub fn drop_segment(&mut self, id: u64) -> bool {
        let Some(seg) = self.segments.get(&id) else {
            return false;
        };
        let decider = self.running().into_iter().find(|n| {
            seg.marks.contains_key(n)
                && self.live_reconciled(*n, *n)
                && !self.segment_fenced(
                    *n,
                    id,
                    &mut self.nodes[n].fences.get(&id).cloned().unwrap_or_default(),
                )
                && self.picks(*n, id)
        });
        let Some(decider) = decider else {
            return false;
        };
        let view = &self.nodes[&decider].view;
        let Some(set) = self.set_of(decider, id) else {
            return false;
        };
        let co: BTreeSet<u64> = set
            .holders
            .iter()
            .copied()
            .filter(|h| *h != decider)
            .collect();
        if co
            .iter()
            .any(|h| !view.absent(*h) && !self.near(decider, *h))
        {
            return false;
        }
        let waits: BTreeSet<u64> = co.iter().copied().filter(|h| !view.absent(*h)).collect();
        self.apply_drop(decider, id);
        self.dropped.insert(id);
        let restart = self.disks[&decider].restart;
        self.drop_waits.insert(id, (decider, restart, waits));
        self.send_drops(decider, id, co);
        self.drops_settle();
        true
    }

    fn drops_settle(&mut self) {
        let done: Vec<u64> = self
            .drop_waits
            .iter()
            .filter(|(_, (by, restart, waits))| {
                waits.is_empty()
                    || !self.nodes.contains_key(by)
                    || self.disks.get(by).is_none_or(|d| d.restart != *restart)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in done {
            self.drop_waits.remove(&id);
            self.drops_done.insert(id);
        }
    }

    fn apply_drop(&mut self, h: u64, id: u64) -> bool {
        if self
            .disks
            .get(&h)
            .is_none_or(|d| d.dropped.contains_key(&id))
        {
            return false;
        }
        if let Some(s) = self.segments.get_mut(&id) {
            s.marks.remove(&h);
        }
        if let Some(d) = self.disks.get_mut(&h) {
            d.dropped.insert(id, d.alive_ms);
        }
        self.copying.retain(|(s, _), _| *s != id);
        true
    }

    fn send_drops(&mut self, from: u64, id: u64, targets: BTreeSet<u64>) {
        let Some(node) = self.nodes.get(&from) else {
            return;
        };
        let now: Vec<u64> = targets
            .iter()
            .copied()
            .filter(|t| !node.view.absent(*t))
            .collect();
        if let Some(node) = self.nodes.get_mut(&from) {
            node.drops.entry(id).or_default().extend(targets);
        }
        for t in now {
            self.send(from, t, Msg::Drop(id));
        }
    }

    fn resend_drops(&mut self) {
        self.drops_settle();
        let mut sends = Vec::new();
        for (n, node) in self.nodes.iter_mut() {
            let dropped = &self.disks[n].dropped;
            node.drops.retain(|id, _| dropped.contains_key(id));
            let members = &node.raft.meta().members;
            for targets in node.drops.values_mut() {
                targets.retain(|t| members.contains(t));
            }
            node.drops.retain(|_, targets| !targets.is_empty());
            let view = &node.view;
            sends.extend(
                node.drops
                    .iter()
                    .flat_map(|(id, ts)| ts.iter().map(move |t| (*n, *t, *id)))
                    .filter(|(_, t, _)| !view.absent(*t))
                    .take(DROPS_MAX),
            );
        }
        for (n, t, id) in sends {
            self.send(n, t, Msg::Drop(id));
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

    fn reach(&self, from: u64, to: u64) -> bool {
        from == to || self.nodes.get(&from).is_some_and(|n| !n.view.absent(to))
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

    fn ring_of(&self, disk: u64) -> (Vec<u64>, BTreeMap<u64, u64>) {
        let node = &self.nodes[&disk];
        let members = &node.raft.meta().members;
        let domain: BTreeMap<u64, u64> = node
            .domains
            .as_ref()
            .map(|d| d.of.clone())
            .unwrap_or_default();
        let candidates: Vec<Candidate> = node
            .view
            .live()
            .filter(|(d, e)| {
                e.body.signals == 0
                    && !e.body.drained
                    && (e.body.reconciled || e.head.restart > 1)
                    && members.contains(d)
            })
            .map(|(d, e)| Candidate {
                disk: d,
                domain: domain.get(&d).copied().unwrap_or(u64::MAX),
                join_ms: e.body.join_ms,
                heavy: node
                    .heavy
                    .get(&d)
                    .filter(|(hold, _)| hold.heavy)
                    .map(|(_, level)| *level),
            })
            .collect();
        (placement::ring(&candidates, node.ctl.floor.value), domain)
    }

    fn next_groups(&self, disk: u64) -> Option<Groups> {
        let node = self.nodes.get(&disk)?;
        let (ring, domain) = self.ring_of(disk);
        placement::next(
            groups_of(node),
            &ring,
            &domain,
            &self.reported(disk),
            node.ctl.floor.value,
        )
    }

    fn write_groups(&mut self, disk: u64) {
        let Some(node) = self.nodes.get(&disk) else {
            return;
        };
        if !node.raft.is_leader() {
            return;
        }
        let started = self.run_of(disk) - node.boot_run_ms;
        if groups_of(node).is_none() && !placement::ready(node.domains.is_some(), started) {
            return;
        }
        let Some(made) = self.next_groups(disk) else {
            return;
        };
        let Some(node) = self.nodes.get_mut(&disk) else {
            return;
        };
        let key = (node.raft.term(), made.epoch);
        if node.proposed_groups == Some(key) {
            return;
        }
        node.proposed_groups = Some(key);
        self.raft_refused += u64::from(node.raft.propose(&Command::Groups(made)).is_err());
        self.flush(disk);
    }

    fn jobs(&self, cursor: Option<usize>) -> (Vec<Job>, Vec<(u64, HolderSet)>) {
        let mut copies = Vec::new();
        let mut drops = Vec::new();
        let known: BTreeSet<u64> = self.disks.keys().copied().collect();
        let per: BTreeMap<u64, (&Groups, BTreeSet<u64>, Want)> = self
            .nodes
            .iter()
            .filter_map(|(n, node)| {
                let groups = groups_of(node)?;
                let live = known
                    .iter()
                    .copied()
                    .filter(|d| self.reach(*n, *d))
                    .collect();
                let members: BTreeSet<u64> = groups.groups.iter().flatten().copied().collect();
                let want = Want {
                    target: usize::from(controller::target(
                        groups.size,
                        members.len(),
                        self.headroom,
                    )),
                    cold: groups.size < node.ctl.floor.value.max(node.ctl.count),
                };
                Some((*n, (groups, live, want)))
            })
            .collect();
        let ids: Vec<&u64> = self.segments.keys().collect();
        let scan: Vec<usize> = match cursor {
            Some(c) => repair::pass(ids.len(), c).collect(),
            None => (0..ids.len()).collect(),
        };
        for i in scan {
            let id = ids[i];
            let seg = &self.segments[id];
            if self.held_repair.contains(id) {
                continue;
            }
            let complete = |_: u64, d: u64| seg.marks.contains_key(&d);
            let leader = seg.marks.keys().copied().find(|n| self.leads(*n, *id));
            let mut senders: BTreeMap<Vec<u64>, Option<u64>> = BTreeMap::new();
            for n in seg.marks.keys() {
                let Some((groups, live_set, want)) = per.get(n) else {
                    continue;
                };
                let live = |d: u64| live_set.contains(&d);
                let Some(set) = self.set_of(*n, *id) else {
                    continue;
                };
                let ready: Vec<u64> = set
                    .holders
                    .iter()
                    .copied()
                    .filter(|d| live(*d) && complete(*id, *d) && self.live_reconciled(*n, *d))
                    .collect();
                let sender = leader.or_else(|| {
                    *senders
                        .entry(ready)
                        .or_insert_with_key(|r| repair::sender(set, |d| r.contains(&d)))
                });
                let leads = leader == Some(*n);
                if sender != Some(*n) && !leads {
                    continue;
                }
                let held = Held {
                    writer: seg.writer,
                    bytes: seg.bytes,
                    sealed: !self.open.contains(id),
                    set,
                };
                match repair::plan(&held, groups, live, complete, *want) {
                    Step::Copy { need, to } if sender == Some(*n) => copies.push(Job {
                        copies: seg.marks.keys().filter(|h| live(**h)).count(),
                        segment: *id,
                        from: *n,
                        need,
                        to,
                    }),
                    Step::Drop(keep) if leads => drops.push((*n, set.with(*n, keep))),
                    Step::Copy { .. } | Step::Drop(_) => {}
                    Step::Idle => {}
                }
            }
        }
        for (disk, node) in &self.nodes {
            if node.signals != 0 || node.view.own().body.drained {
                copies.extend(self.drains(*disk, node));
            }
        }
        copies.sort_by_key(|j| (j.copies, j.segment));
        (copies, drops)
    }

    fn drains(&self, disk: u64, node: &Node) -> Vec<Job> {
        let Some(next) = self.next_groups(disk).or_else(|| groups_of(node).cloned()) else {
            return Vec::new();
        };
        let serving = |d: u64| {
            d != disk
                && self.reach(disk, d)
                && node.view.entry(d).is_some_and(|e| e.body.signals == 0)
        };
        let complete = |s: u64, d: u64| {
            self.segments
                .get(&s)
                .is_some_and(|x| x.marks.contains_key(&d))
        };
        let held: Vec<Held> = self
            .segments
            .iter()
            .filter(|(id, s)| !self.held_repair.contains(id) && s.marks.contains_key(&disk))
            .filter_map(|(id, s)| {
                let set = self
                    .set_of(disk, *id)
                    .filter(|set| set.holders.contains(&disk))?;
                Some(Held {
                    writer: s.writer,
                    bytes: s.bytes,
                    sealed: !self.open.contains(id),
                    set,
                })
            })
            .collect();
        let deadline = node.stop_run_ms.unwrap_or(u64::MAX);
        repair::self_drain(disk, &held, &next, serving, complete, self.run_ms, deadline)
            .into_iter()
            .map(|(segment, to)| Job {
                copies: self.segments[&segment]
                    .marks
                    .keys()
                    .filter(|h| serving(**h))
                    .count(),
                segment,
                from: disk,
                need: to.len(),
                to,
            })
            .collect()
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
        self.grouped() && copies.is_empty() && drops.is_empty()
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
            && voters.keys().all(|d| self.next_groups(**d).is_none())
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
            self.dirty = !drops.is_empty();
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
                && fence::no_holder(
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
            if keep.holders.iter().all(|h| self.nodes.contains_key(h)) {
                self.wish(by, keep);
            }
        }
    }

    fn copy(&mut self, queued: &[Job], now: u64) {
        for job in queued {
            let Some(seg) = self.segments.get(&job.segment) else {
                continue;
            };
            if !self.nodes.contains_key(&job.from) || !seg.marks.contains_key(&job.from) {
                continue;
            }
            let bytes = seg.bytes;
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
            if now % 5000 == 0 && now >= 95000 && now <= 105000 {
                let sr = self.sends.get_mut(&job.from).map(|l| l.available(now));
                let tk: Vec<_> = job.to.iter().map(|t| (*t, self.takes.get_mut(t).map(|l| l.available(now)))).collect();
                eprintln!("DBG {now} from={} seg={} need={} to={:?} active={:?} copying={:?} sends={:?} takes={:?} serving={:?} link={:?}", job.from, job.segment, job.need, job.to, active, self.copying.iter().filter(|(k,_)|k.0==job.segment).collect::<Vec<_>>(), sr, tk, job.to.iter().map(|t| self.serving(*t)).collect::<Vec<_>>(), job.to.iter().map(|t| self.link(3,*t)).collect::<Vec<_>>());
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
                if now >= 100000 && now <= 130000 && now % 5000 == 0 { eprintln!("SPEND {now} from={} seg={} to={} step={} left={}", job.from, job.segment, to, step, *left); }
                out.spend(step);
                inn.spend(step);
                *left -= step;
                if *left > 0 {
                    continue;
                }
                self.copying.remove(&(job.segment, to));
                self.dirty = true;
                let mark = self.segments[&job.segment].marks[&job.from];
                let stream = self.segments[&job.segment].stream;
                let covered = self.disks[&to]
                    .records
                    .values()
                    .any(|(s, upto, _)| *s == stream && job.segment <= *upto);
                if (covered || self.disks[&job.from].erased.contains(&job.segment))
                    && let Some(d) = self.disks.get_mut(&to)
                {
                    d.erased.insert(job.segment);
                }
                let own = self.set_of(job.from, job.segment).cloned();
                let Some(seg) = self.segments.get_mut(&job.segment) else {
                    continue;
                };
                seg.marks.insert(to, mark);
                seg.copied.insert(to, self.now_ms);
                let Some(own) = own else {
                    continue;
                };
                let co: BTreeSet<u64> = own.holders.iter().copied().filter(|h| *h != to).collect();
                let named: Vec<((u64, u64), u64)> = self.nodes[&job.from]
                    .named
                    .iter()
                    .filter(|((id, h), _)| *id == job.segment && co.contains(h))
                    .filter(|_| !self.nodes[&job.from].fences.contains_key(&job.segment))
                    .map(|(k, t)| (*k, *t))
                    .collect();
                if let Some(node) = self.nodes.get_mut(&to) {
                    node.fences.insert(job.segment, co);
                    node.caught.retain(|(id, _), _| *id != job.segment);
                    node.named.extend(named);
                }
                if !own.holders.contains(&to) {
                    if self.leads(job.from, job.segment) {
                        self.add_holder(job.from, &own, to);
                    } else if let Some(leader) = self.nodes[&job.from]
                        .raft
                        .meta()
                        .segments
                        .get(&job.segment)
                        .map(|m| m.leader)
                    {
                        self.send(job.from, leader, Msg::Copied(job.segment, to, mark));
                    }
                }
                self.catch_up(to, job.segment);
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
            let node = &self.nodes[&disk];
            let done = node.stop_run_ms.is_some_and(|t| self.run_ms >= t)
                || (self.drains(disk, node).is_empty()
                    && !node.raft.meta().members.contains(&disk));
            if done {
                self.leave(disk);
            }
        }
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
        let seg = &self.segments[&id];
        let next = seg.marks[&leader] + 1;
        let Some(current) = self.set_of(leader, id) else {
            return;
        };
        let node = &self.nodes[&leader];
        let sets: Vec<BTreeSet<u64>> = std::iter::once(&current.holders)
            .chain(
                node.changing
                    .get(&id)
                    .filter(|c| c.epoch > current.epoch)
                    .map(|c| &c.holders),
            )
            .cloned()
            .collect();
        let scope: BTreeSet<u64> = sets
            .iter()
            .flatten()
            .copied()
            .filter(|h| *h != leader && !self.nodes[&leader].view.absent(*h))
            .collect();
        if !sets.iter().all(|set| enough_in(set, leader, &scope)) {
            self.refused_settles += 1;
            return;
        }
        if let Some(mark) = self
            .segments
            .get_mut(&id)
            .and_then(|s| s.marks.get_mut(&leader))
        {
            *mark = next;
        }
        let sid = self.next_request;
        self.next_request += 1;
        let epoch = self.epoch_of(leader, id);
        for h in &scope {
            self.send(leader, *h, Msg::Ack(sid, id, next, epoch));
        }
        self.settles.insert(
            sid,
            Settle {
                leader,
                epoch,
                restart: self.disks[&leader].restart,
                segment: id,
                next,
                started_ms: self.now_ms,
                scope,
                sets,
                acked: BTreeMap::new(),
            },
        );
    }

    fn answer_settles(&mut self) {
        let now = self.now_ms;
        let ids: Vec<u64> = self.settles.keys().copied().collect();
        for sid in ids {
            let s = &self.settles[&sid];
            let gone = !self.nodes.contains_key(&s.leader)
                || self.disks[&s.leader].restart != s.restart
                || now >= s.started_ms + REPLICATE_MS;
            if !gone {
                let view = &self.nodes[&s.leader].view;
                let scope: BTreeSet<u64> = s
                    .scope
                    .iter()
                    .copied()
                    .filter(|h| s.acked.contains_key(h) || !view.absent(*h))
                    .collect();
                let open: Vec<u64> = scope
                    .iter()
                    .copied()
                    .filter(|h| !s.acked.contains_key(h))
                    .collect();
                let (leader, segment, next, epoch) = (s.leader, s.segment, s.next, s.epoch);
                let stored: BTreeSet<u64> = s
                    .acked
                    .iter()
                    .filter(|(_, c)| **c)
                    .map(|(h, _)| *h)
                    .collect();
                if s.sets.iter().all(|set| enough_in(set, leader, &scope))
                    && !open.is_empty()
                    && !s.sets.iter().all(|set| enough_in(set, leader, &stored))
                {
                    if let Some(s) = self.settles.get_mut(&sid) {
                        s.scope = scope;
                    }
                    for h in open {
                        self.send(leader, h, Msg::Ack(sid, segment, next, epoch));
                    }
                    continue;
                }
            }
            let Some(s) = self.settles.remove(&sid) else {
                continue;
            };
            let stored: BTreeSet<u64> = s
                .acked
                .iter()
                .filter(|(_, c)| **c)
                .map(|(h, _)| *h)
                .collect();
            if gone
                || !self.leads(s.leader, s.segment)
                || self.epoch_of(s.leader, s.segment) != s.epoch
                || !s.sets.iter().all(|set| enough_in(set, s.leader, &stored))
            {
                self.refused_settles += 1;
            } else {
                let settled = self.settled.entry(s.segment).or_insert(s.next);
                *settled = (*settled).max(s.next);
                self.applied_ms.insert((s.segment, s.leader), s.started_ms);
                if let Some(node) = self.nodes.get_mut(&s.leader) {
                    node.caught.insert((s.segment, s.leader), s.started_ms);
                }
            }
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
                let list: Vec<u64> = node.raft.meta().members.iter().copied().collect();
                node.view.tick(now, run, &list);
            }
        }
        for disk in &running {
            if self.rng.random_range(0..4) != 0 {
                self.drive(*disk, Control::tick);
            }
        }
        self.pings();
        self.timers();
        let mut held: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
        if now.is_multiple_of(GOSSIP_MS) {
            for (id, seg) in &self.segments {
                for h in seg.marks.keys() {
                    held.entry(*h).or_default().push(*id);
                }
            }
        }
        for disk in &running {
            self.guard(*disk, held.remove(disk).unwrap_or_default());
        }
        self.prune();
        self.ask_fences();
        self.open_wishes();
        self.send_seg_asks();
        self.send_beats();
        self.resend_drops();
        self.answer_requests();
        self.answer_settles();
        for disk in &running {
            self.govern(*disk);
            self.lead_segments(*disk);
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

    fn send(&mut self, from: u64, to: u64, msg: Msg) {
        let protocol = !matches!(msg, Msg::Gossip(..) | Msg::Ping(_) | Msg::Pong(..));
        if !self.sends(from, to) || (protocol && self.loss > 0.0 && self.rng.random_bool(self.loss))
        {
            return;
        }
        let Some(node) = self.nodes.get(&from) else {
            return;
        };
        let version = node.raft.meta().version;
        let slow = if matches!(msg, Msg::Raft(_)) {
            self.slow_ms
        } else {
            0
        };
        let at = self.now_ms + slow + self.rng.random_range(1..=self.link_ms.max(1));
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
    }

    fn pings(&mut self) {
        for a in self.running() {
            let node = &self.nodes[&a];
            let head = node.view.own().head;
            let mut targets: BTreeSet<u64> = node.view.digest().iter().map(|l| l.disk).collect();
            targets.extend(node.raft.meta().members.iter().copied());
            targets.remove(&a);
            for b in targets {
                self.send(a, b, Msg::Ping(head));
            }
        }
    }

    fn timers(&mut self) {
        let now = self.now_ms;
        for (me, node) in self.nodes.iter_mut() {
            let start = if node.full { 0 } else { now };
            for line in node.view.digest() {
                if line.disk == *me {
                    continue;
                }
                let restart = line.head.restart;
                let trip = node.trips.entry(line.disk).or_insert((restart, start));
                if trip.0 < restart {
                    *trip = (restart, trip.1.max(now));
                }
            }
        }
    }

    fn beat(&mut self, to: u64, from: u64, head: Head) {
        let (now, run) = (self.now_ms, self.run_of(to));
        if let Some(node) = self.nodes.get_mut(&to) {
            node.view.merge(
                &[Update {
                    disk: from,
                    head,
                    body: None,
                }],
                now,
                run,
            );
        }
    }

    fn receive(&mut self, f: Frame) {
        if !self.sends(f.from, f.to) {
            return;
        }
        let (to, now, run) = (f.to, self.now_ms, self.run_of(f.to));
        let Some(node) = self.nodes.get_mut(&to) else {
            return;
        };
        if let Msg::Gossip(updates) = &f.msg {
            node.view.merge(updates, now, run);
            return;
        }
        match f.msg {
            Msg::Gossip(..) => {}
            Msg::Ping(head) => {
                self.beat(to, f.from, head);
                let head = self.nodes[&to].view.own().head;
                self.send(to, f.from, Msg::Pong(f.sent, head));
            }
            Msg::Pong(sent, head) => {
                self.beat(to, f.from, head);
                if let Some(node) = self.nodes.get_mut(&to) {
                    let trip = node.trips.entry(f.from).or_insert((head.restart, 0));
                    *trip = (trip.0.max(head.restart), trip.1.max(sent));
                }
            }
            Msg::Raft(message) => {
                let Some(node) = self.nodes.get_mut(&to) else {
                    return;
                };
                match node.raft.step(message) {
                    Ok(()) | Err(raft::Error::StepPeerNotFound | raft::Error::ProposalDropped) => {}
                    Err(e) => panic!("the control plane refused a step: {e}"),
                }
                if node.raft.leader() == Some(f.from) {
                    node.heard_ms = Some(now);
                }
                self.flush(to);
            }
            Msg::Lose(disk) => {
                if let Some(node) = self
                    .nodes
                    .get_mut(&to)
                    .filter(|n| n.view.absent(disk) && n.raft.meta().members.contains(&disk))
                {
                    node.lose.insert(disk);
                }
            }
            Msg::Record(id, origin, rec, cut) => {
                self.take_record(to, f.from, id, origin, rec, cut);
            }
            Msg::Ack(sid, id, next, epoch) => {
                let held = self.hold_epoch(to, id, epoch);
                if held > epoch {
                    self.send(to, f.from, Msg::Acked(sid, false, held));
                    return;
                }
                let mark = self
                    .segments
                    .get_mut(&id)
                    .and_then(|s| s.marks.get_mut(&to));
                let copy = mark.is_some();
                if let Some(mark) = mark {
                    *mark = (*mark).max(next);
                }
                if copy {
                    self.applied_ms.insert((id, to), f.sent);
                }
                if let Some(node) = self.nodes.get_mut(&to).filter(|_| copy) {
                    node.caught.insert((id, to), f.sent);
                }
                self.send(to, f.from, Msg::Acked(sid, copy, epoch));
            }
            Msg::Wish(set) => self.propose_segment(to, Command::Holders(set)),
            Msg::SegAsk(id, epoch) => {
                let held = self.hold_epoch(to, id, epoch);
                if let Some(seg) = self.segments.get(&id) {
                    let mark = seg.marks.get(&to).copied();
                    let known = self.known(to, id);
                    self.send(to, f.from, Msg::Seg(id, mark, known, held));
                }
            }
            Msg::Seg(id, mark, known, epoch) => {
                let Some(seg) = self.segments.get_mut(&id) else {
                    return;
                };
                if let (Some(m), Some(own)) = (mark, seg.marks.get_mut(&to)) {
                    *own = (*own).max(m);
                }
                let answer = Marks::from([(0, mark.unwrap_or(0))]);
                if let Some(node) = self.nodes.get_mut(&to) {
                    node.seg_answers.insert((id, f.from), (known, answer, now));
                    if mark.is_some() {
                        node.caught.insert((id, f.from), f.sent);
                    }
                    let current = node
                        .raft
                        .meta()
                        .segments
                        .get(&id)
                        .filter(|m| m.leader == to && m.leader_epoch == epoch);
                    if current.is_some() && mark.is_some() {
                        let lease = node.lease.entry(id).or_default();
                        if lease.epoch != epoch {
                            *lease = Lease {
                                epoch,
                                ..Lease::default()
                            };
                        }
                        lease.from.insert(f.from);
                    }
                }
                let booting = self
                    .nodes
                    .get_mut(&to)
                    .is_some_and(|n| n.booting.remove(&id));
                let dropped = self
                    .set_of(to, id)
                    .is_some_and(|s| !s.holders.contains(&to));
                if booting
                    && dropped
                    && let Some(seg) = self.segments.get_mut(&id)
                {
                    seg.marks.remove(&to);
                }
            }
            Msg::Beat(id, epoch) => {
                if self.hold_epoch(to, id, epoch) == epoch {
                    self.send(to, f.from, Msg::Beaten(id, epoch, f.sent));
                }
            }
            Msg::Copied(id, new, mark) => {
                if [3,4,5].contains(&id) { eprintln!("COPIED {} at={to} seg={id} new={new} own={:?} leads={} meta={:?}", self.now_ms, self.set_of(to,id), self.leads(to,id), self.nodes[&to].raft.meta().segments.get(&id).map(|m| (m.leader, m.leader_epoch, m.holders.holders.clone(), m.holders.epoch))); }
                let own = self
                    .set_of(to, id)
                    .filter(|s| !s.holders.contains(&new))
                    .cloned();
                let caught = self
                    .segments
                    .get(&id)
                    .and_then(|s| s.marks.get(&to))
                    .is_some_and(|m| mark >= *m);
                if let Some(own) = own
                    && caught
                    && self.leads(to, id)
                {
                    self.add_holder(to, &own, new);
                }
            }
            Msg::Beaten(id, epoch, sent) => {
                if let Some(lease) = self
                    .nodes
                    .get_mut(&to)
                    .and_then(|n| n.lease.get_mut(&id))
                    .filter(|l| l.epoch == epoch)
                {
                    let beat = lease.beats.entry(f.from).or_insert(0);
                    *beat = (*beat).max(sent);
                }
            }
            Msg::Drop(id) => {
                if self.apply_drop(to, id) {
                    let named: BTreeSet<u64> = self
                        .set_of(to, id)
                        .map(|s| s.holders.clone())
                        .unwrap_or_default();
                    let targets = named
                        .into_iter()
                        .filter(|h| *h != to && *h != f.from)
                        .collect();
                    self.send_drops(to, id, targets);
                }
                self.send(to, f.from, Msg::Dropped(id));
            }
            Msg::Dropped(id) => {
                if let Some(ts) = self.nodes.get_mut(&to).and_then(|n| n.drops.get_mut(&id)) {
                    ts.remove(&f.from);
                }
                if let Some((by, _, waits)) = self.drop_waits.get_mut(&id)
                    && *by == to
                {
                    waits.remove(&f.from);
                }
                self.drops_settle();
            }
            Msg::Acked(sid, copy, epoch) => {
                if let Some(s) = self.settles.get_mut(&sid)
                    && s.leader == to
                {
                    s.acked.insert(f.from, copy && epoch == s.epoch);
                }
            }
            Msg::FenceAsk(id, xs, sent) => {
                let named = self.set_of(to, id);
                if self.segments.contains_key(&id)
                    && named.is_none_or(|s| s.holders.contains(&f.from))
                {
                    let node = &self.nodes[&to];
                    let absent: BTreeSet<u64> = xs
                        .iter()
                        .copied()
                        .filter(|x| node.view.absent(*x))
                        .collect();
                    let lifted = self.lifted_of(to, id, &absent);
                    let marks = self.marks_of(to, id);
                    let dropped = self.disks[&to].dropped.contains_key(&id);
                    self.send(
                        to,
                        f.from,
                        Msg::FenceIs(id, sent, absent, marks, dropped, lifted),
                    );
                }
            }
            Msg::FenceIs(id, sent, absent, marks, dropped, lifted) => {
                if let Some(node) = self.nodes.get_mut(&to) {
                    node.fence_answers
                        .insert((id, f.from), (absent, marks, sent, lifted));
                }
                if dropped
                    && self
                        .segments
                        .get(&id)
                        .is_some_and(|s| s.marks.contains_key(&to))
                {
                    self.apply_drop(to, id);
                }
            }
            Msg::Confirm(id, origin, by, version) => {
                if to != origin {
                    self.send(to, origin, Msg::Confirm(id, origin, by, version));
                } else if let Some(r) = self.requests.get_mut(&id) {
                    r.confirmed.insert(by);
                    r.newest = r.newest.max(version);
                }
            }
            Msg::VoteAsk(sent) => {
                let node = &self.nodes[&to];
                let absent = node
                    .raft
                    .meta()
                    .members
                    .iter()
                    .copied()
                    .filter(|m| node.view.absent(*m))
                    .collect();
                self.send(to, f.from, Msg::Vote(absent, sent));
            }
            Msg::Vote(absent, sent) => {
                if let Some(node) = self.nodes.get_mut(&to) {
                    node.votes.insert(f.from, (absent, sent));
                }
            }
            Msg::MarksAsk => {
                let d = &self.disks[&to];
                let (marks, forgot) = (d.marks.clone(), d.forgot.clone());
                let reported = self.reported(to);
                self.send(to, f.from, Msg::MarksIs(marks, forgot, reported));
            }
            Msg::MarksIs(marks, forgot, reported) => {
                if let Some(node) = self.nodes.get_mut(&to) {
                    node.peers
                        .insert(f.from, (marks, forgot, f.version, now, reported));
                }
            }
            Msg::RecordsAsk(marks, after) => {
                let d = &self.disks[&to];
                let mut records: Vec<((u64, u64), u64, u64)> = d
                    .records
                    .iter()
                    .filter(|(key, _)| after.is_none_or(|a| **key > a))
                    .filter(|((origin, seq), _)| *seq > marks.get(origin).copied().unwrap_or(0))
                    .map(|(key, (stream, upto, _))| (*key, *stream, *upto))
                    .take(RECORDS_MAX + 1)
                    .collect();
                let more = records.len() > RECORDS_MAX;
                records.truncate(RECORDS_MAX);
                let marks = d.marks.clone();
                self.send(to, f.from, Msg::Records(records, marks, more));
            }
            Msg::Records(records, marks, more) => {
                let last = records.last().map(|r| r.0);
                for (key, stream, upto) in records {
                    self.apply(to, key, stream, upto);
                }
                if let Some(node) = self.nodes.get_mut(&to) {
                    node.records_asked.remove(&f.from);
                }
                if more {
                    let own = self.disks[&to].marks.clone();
                    self.ask_records(to, f.from, own, last);
                } else if let Some(t) = self.disks.get_mut(&to) {
                    for (origin, mark) in marks {
                        let own = t.marks.entry(origin).or_insert(0);
                        *own = (*own).max(mark);
                    }
                }
            }
        }
    }

    fn ask_records(&mut self, by: u64, peer: u64, marks: Marks, after: Option<(u64, u64)>) {
        let now = self.now_ms;
        let Some(node) = self.nodes.get_mut(&by) else {
            return;
        };
        if node
            .records_asked
            .get(&peer)
            .is_some_and(|t| now < t + 2 * GOSSIP_MS)
        {
            return;
        }
        node.records_asked.insert(peer, now);
        self.send(by, peer, Msg::RecordsAsk(marks, after));
    }

    fn take_record(
        &mut self,
        to: u64,
        from: u64,
        id: u64,
        origin: u64,
        rec: Rec,
        cut: BTreeSet<u64>,
    ) {
        let now = self.now_ms;
        let Some(node) = self.nodes.get_mut(&to) else {
            return;
        };
        if node.seen_records.insert(id, now).is_some() {
            return;
        }
        let peers: Vec<u64> = if to == origin {
            self.requests
                .get(&id)
                .map(|r| r.scope.iter().copied().collect())
                .unwrap_or_default()
        } else if from == origin {
            cut.iter()
                .copied()
                .filter(|p| *p != to && *p != origin)
                .collect()
        } else {
            Vec::new()
        };
        let version = node.raft.meta().version;
        match &rec {
            Rec::Delete(k, stream, upto) => self.apply(to, *k, *stream, *upto),
        }
        if to != origin {
            self.send(to, from, Msg::Confirm(id, origin, to, version));
        }
        for p in peers {
            self.send(to, p, Msg::Record(id, origin, rec.clone(), cut.clone()));
        }
    }

    fn announce(&mut self, origin: u64, rec: Rec) -> u64 {
        let id = self.next_request;
        self.next_request += 1;
        let now = self.now_ms;
        let node = &self.nodes[&origin];
        let meta = node.raft.meta();
        let mut scope: BTreeSet<u64> = meta.members.clone();
        scope.remove(&origin);
        let cut: BTreeSet<u64> = node
            .unreachable
            .keys()
            .copied()
            .filter(|p| scope.contains(p))
            .collect();
        let newest = meta.version;
        self.requests.insert(
            id,
            Request {
                origin,
                restart: self.disks[&origin].restart,
                rec: rec.clone(),
                started_ms: now,
                scope,
                confirmed: BTreeSet::new(),
                newest,
            },
        );
        self.take_record(origin, origin, id, origin, rec, cut);
        id
    }

    fn answer_requests(&mut self) {
        let now = self.now_ms;
        let ids: Vec<u64> = self.requests.keys().copied().collect();
        for id in ids {
            let r = &self.requests[&id];
            let origin = r.origin;
            let Some(node) = self.nodes.get(&origin) else {
                self.requests.remove(&id);
                continue;
            };
            if now >= r.started_ms + REPLICATE_MS || self.disks[&origin].restart != r.restart {
                self.requests.remove(&id);
                continue;
            }
            let meta = node.raft.meta();
            if r.newest > meta.version {
                continue;
            }
            let fresh = |v: &Vote| v.1 >= r.started_ms;
            let open: Vec<u64> = r
                .scope
                .iter()
                .chain(meta.members.iter())
                .copied()
                .filter(|p| *p != origin && !r.confirmed.contains(p))
                .collect();
            let settled = open.iter().all(|p| {
                let yes = meta
                    .members
                    .iter()
                    .filter(|v| {
                        if **v == origin {
                            node.view.absent(*p)
                        } else {
                            node.votes
                                .get(v)
                                .is_some_and(|vote| fresh(vote) && vote.0.contains(p))
                        }
                    })
                    .count();
                confirmed(yes, meta.members.len())
            });
            if !settled {
                let asked = node.vote_ms.is_some_and(|t| t >= r.started_ms);
                if !asked && now >= r.started_ms + STEP_MS {
                    let voters: Vec<u64> = meta
                        .members
                        .iter()
                        .copied()
                        .filter(|v| *v != origin)
                        .collect();
                    if let Some(node) = self.nodes.get_mut(&origin) {
                        node.vote_ms = Some(now);
                    }
                    for v in voters {
                        self.send(origin, v, Msg::VoteAsk(now));
                    }
                }
                continue;
            }
            let Some(r) = self.requests.remove(&id) else {
                continue;
            };
            match r.rec {
                Rec::Delete(key, stream, upto) => {
                    self.deleted.push((stream, upto));
                    self.records.push(key);
                    self.done.push((id, origin, now));
                }
            }
        }
    }

    fn prune(&mut self) {
        let now = self.now_ms;
        for node in self.nodes.values_mut() {
            let view = &node.view;
            node.trips.retain(|d, _| view.entry(*d).is_some());
            node.fence_answers.retain(|_, a| now < a.2 + FENCE_MS);
            node.votes.retain(|_, v| now < v.1 + REPLICATE_MS);
            node.peers.retain(|_, p| now < p.3 + 2 * GOSSIP_MS);
            node.records_asked.retain(|_, t| now < *t + 2 * GOSSIP_MS);
            let start = node.reconcile_ms;
            node.seg_answers
                .retain(|_, a| start.is_some_and(|t| a.2 >= t));
            node.seg_asked.retain(|_, t| now < *t + STEP_MS);
            node.seen_records.retain(|_, t| now < *t + REPLICATE_MS);
        }
    }

    pub fn fenced_segments(&self, disk: u64) -> Vec<u64> {
        self.nodes
            .get(&disk)
            .map(|n| n.fences.keys().copied().collect())
            .unwrap_or_default()
    }

    pub fn trips_to(&self, disk: u64) -> Vec<u64> {
        self.nodes
            .get(&disk)
            .map(|n| n.trips.keys().copied().collect())
            .unwrap_or_default()
    }

    pub fn held_state(&self, disk: u64) -> usize {
        self.nodes
            .get(&disk)
            .map_or(0, |n| n.votes.len() + n.peers.len() + n.seen_records.len())
    }

    fn ask_fences(&mut self) {
        let now = self.now_ms;
        let mut asks = Vec::new();
        for (n, node) in &self.nodes {
            let list = node.raft.meta();
            let ids: Vec<u64> = self
                .segments
                .iter()
                .filter(|(id, s)| {
                    node.fences.contains_key(id)
                        || s.marks.contains_key(n)
                            && self.set_of(*n, **id).is_some_and(|set| {
                                set.holders.contains(n)
                                    && set.holders.iter().any(|h| h != n && !self.near(*n, *h))
                            })
                })
                .map(|(id, _)| *id)
                .collect();
            let skip = (now / STEP_MS) as usize * FENCES_MAX % ids.len().max(1);
            for id in ids
                .iter()
                .cycle()
                .skip(skip)
                .take(ids.len().min(FENCES_MAX))
            {
                let Some(set) = self.set_of(*n, *id) else {
                    continue;
                };
                let co: BTreeSet<u64> = set.holders.iter().copied().filter(|h| h != n).collect();
                let live = co.iter().filter(|c| !node.view.absent(**c)).count();
                let alone = !fence::quorum(live, set.holders.len());
                let mut targets = co.clone();
                if alone {
                    targets.extend(list.members.iter().copied().filter(|m| m != n));
                }
                asks.extend(targets.into_iter().map(|t| (*n, t, *id, co.clone())));
            }
        }
        for (n, t, id, co) in asks {
            self.send(n, t, Msg::FenceAsk(id, co, now));
        }
    }

    fn drive(&mut self, disk: u64, act: impl FnOnce(&mut Control)) {
        if let Some(node) = self.nodes.get_mut(&disk) {
            act(&mut node.raft);
        }
        self.flush(disk);
    }

    fn change(&mut self, disk: u64, kind: ConfChangeType, member: u64) {
        let Some(node) = self.nodes.get_mut(&disk) else {
            return;
        };
        self.raft_refused += u64::from(node.raft.propose_conf_change(kind, member).is_err());
        self.flush(disk);
    }

    fn flush(&mut self, disk: u64) {
        let Some(node) = self.nodes.get_mut(&disk) else {
            return;
        };
        let out = node.raft.drain().expect("the control plane drains");
        let saved = node.raft.save().expect("the control plane saves");
        let term = node.raft.is_leader().then(|| node.raft.term());
        let Some(d) = self.disks.get_mut(&disk) else {
            return;
        };
        let old = std::mem::replace(&mut d.control, saved);
        self.applied(disk, &old);
        if let Some(term) = term
            && *self.terms.entry(term).or_insert(disk) != disk
        {
            self.double_leaders += 1;
        }
        for message in out {
            let to = message.to;
            self.send(disk, to, Msg::Raft(message));
        }
    }

    fn applied(&mut self, disk: u64, old: &Saved) {
        let (now, run) = (self.now_ms, self.run_ms);
        let floor = self.disks[&disk].control.meta.floor;
        if floor != old.meta.floor {
            self.floor_applied(disk, old.meta.floor, floor);
        }
        let control = &self.disks[&disk].control;
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
        let changed: Vec<(Option<HolderSet>, HolderSet)> = control
            .meta
            .segments
            .values()
            .filter_map(|s| {
                let before = old
                    .meta
                    .segments
                    .get(&s.holders.segment)
                    .map(|b| &b.holders);
                (before != Some(&s.holders)).then(|| (before.cloned(), s.holders.clone()))
            })
            .collect();
        for (before, set) in changed {
            let Some(seg) = self.segments.get_mut(&set.segment) else {
                continue;
            };
            if before.is_some_and(|b| b.holders.contains(&disk)) && !set.holders.contains(&disk) {
                seg.marks.remove(&disk);
            }
            if seg.holders.epoch < set.epoch {
                let from = std::mem::replace(&mut seg.holders, set.clone());
                self.log(from, set);
            }
        }
        let control = &self.disks[&disk].control;
        let gone: Vec<u64> = old
            .meta
            .members
            .difference(&control.meta.members)
            .copied()
            .collect();
        let joined = !old.meta.members.contains(&disk) && control.meta.members.contains(&disk);
        for d in &gone {
            self.removed_at.entry(*d).or_insert(now);
        }
        let Some(node) = self.nodes.get_mut(&disk) else {
            return;
        };
        node.new_member |= joined;
        node.adopted_ms = now;
        if !node.raft.is_leader() {
            return;
        }
        let lost = gone.iter().any(|d| node.lose.contains(d));
        node.lose.retain(|d| !gone.contains(d));
        let drained = gone.iter().any(|d| {
            node.view
                .entry(*d)
                .is_some_and(|e| e.body.drained && e.body_key.0 == e.head.restart)
        });
        if lost {
            node.fall = Some(Cause::AcceptLoss);
        } else if drained {
            node.fall = Some(Cause::Drain);
            self.drain_commits.push((disk, run));
        }
    }

    fn govern(&mut self, disk: u64) {
        let Some(node) = self.nodes.get_mut(&disk) else {
            return;
        };
        if node.raft.is_leader() {
            let members = &node.raft.meta().members;
            let removed: Vec<u64> = node
                .lose
                .iter()
                .copied()
                .filter(|d| !members.contains(d))
                .collect();
            node.lose.retain(|d| !removed.contains(d));
            if !removed.is_empty() {
                node.fall = Some(Cause::AcceptLoss);
            }
        }
        self.fall(disk);
        let Some(node) = self.nodes.get(&disk) else {
            return;
        };
        let (view, meta, run) = (&node.view, node.raft.meta(), self.run_of(disk));
        if !node.raft.is_leader() {
            if let Some(leader) = node.raft.leader() {
                let lose: Vec<u64> = node.lose.iter().copied().collect();
                for d in lose {
                    self.send(disk, leader, Msg::Lose(d));
                }
            }
            return;
        }
        let flags = |d: u64| {
            view.entry(d).map_or((false, false, 0), |e| {
                let current = e.body_key.0 == e.head.restart;
                (
                    current && e.body.signals & STOPS != 0,
                    current && e.body.drained,
                    e.body.remaining,
                )
            })
        };
        let waiting = view.live().map(|(d, _)| d).any(|d| {
            let (stop, drained, _) = flags(d);
            !stop && !drained && (node.raft.learners().contains(&d) || !meta.members.contains(&d))
        });
        let drops: Vec<Command> = meta
            .segments
            .values()
            .filter_map(|s| {
                let kept: BTreeSet<u64> = s
                    .holders
                    .holders
                    .iter()
                    .copied()
                    .filter(|h| meta.members.contains(h))
                    .collect();
                (!kept.is_empty() && kept.len() < s.holders.holders.len())
                    .then(|| Command::Holders(s.holders.with(disk, kept)))
            })
            .collect();
        let needed = |m: u64| {
            meta.segments.iter().any(|(id, s)| {
                let set: Vec<u64> = s
                    .holders
                    .holders
                    .iter()
                    .copied()
                    .filter(|h| meta.members.contains(h))
                    .collect();
                let copies = set
                    .iter()
                    .filter(|h| {
                        **h != m
                            && self.live_reconciled(disk, **h)
                            && self.segments[id].marks.contains_key(h)
                    })
                    .count();
                set.contains(&m) && !fence::confirmed(copies, set.len())
            })
        };
        let remove = meta.members.iter().copied().find(|m| {
            let (stop, drained, remaining) = flags(*m);
            if view.absent(*m) {
                view.entry(*m).is_some()
                    && (stop && !needed(*m)
                        || drained && !needed(*m)
                        || node.lose.contains(m)
                        || waiting && view.absent_since(*m).is_some_and(|t| run >= t + REPLACE_MS)
                        || view
                            .absent_since(*m)
                            .is_some_and(|t| run >= t + TOMBSTONE_HORIZON))
            } else {
                (stop || drained && remaining == 0) && !needed(*m)
            }
        });
        let promote = node.raft.learners().iter().copied().find(|l| {
            let (stop, drained, _) = flags(*l);
            !view.absent(*l)
                && !stop
                && !drained
                && view.entry(*l).is_some_and(|e| e.body.epoch == meta.version)
        });
        let add = view.live().map(|(d, _)| d).find(|d| {
            let (stop, drained, _) = flags(*d);
            !meta.members.contains(d) && !node.raft.learners().contains(d) && !stop && !drained
        });
        let change = remove
            .map(|d| (ConfChangeType::RemoveNode, d))
            .or(promote.map(|d| (ConfChangeType::AddNode, d)))
            .or(add.map(|d| (ConfChangeType::AddLearnerNode, d)));
        if let Some((kind, member)) = change.filter(|_| !node.raft.pending_conf()) {
            self.change(disk, kind, member);
        }
        for command in drops {
            self.propose_segment(disk, command);
        }
    }

    fn marks_of(&self, disk: u64, id: u64) -> Marks {
        let mark = self.segments[&id].marks.get(&disk).copied().unwrap_or(0);
        Marks::from([(0, mark)])
    }

    fn known(&self, disk: u64, id: u64) -> Known {
        let seg = &self.segments[&id];
        if self.disks[&disk].dropped.contains_key(&id) {
            Known::Dropped
        } else if seg.marks.contains_key(&disk) && self.live_reconciled(disk, disk) {
            Known::Copy
        } else if seg.marks.contains_key(&disk)
            || self.copying.contains_key(&(id, disk))
            || self
                .set_of(disk, id)
                .unwrap_or(&seg.holders)
                .holders
                .contains(&disk)
        {
            Known::Partial
        } else {
            Known::Unknown
        }
    }

    fn status(&self, disk: u64, body: &mut Body) {
        let node = &self.nodes[&disk];
        body.reconciled = node.reconciled;
        body.unreachable = node.unreachable.keys().copied().collect();
        body.epoch = node.raft.meta().version;
        if body.drained {
            body.remaining = self.remaining(disk) as u32;
        }
    }

    fn guard(&mut self, disk: u64, held: Vec<u64>) {
        let now = self.now_ms;
        let run = self.run_of(disk);
        self.expire(disk);
        let Some(node) = self.nodes.get_mut(&disk) else {
            return;
        };
        let members = &node.raft.meta().members;
        let member = members.contains(&disk);
        let led =
            |within: u64| node.raft.is_leader() || node.heard_ms.is_some_and(|t| now < t + within);
        let full = !(member && led(HEARD_MS));
        if !node.raft.is_leader() {
            node.lose.retain(|d| members.contains(d));
        }
        if !member || !led(2 * HEARD_MS) {
            node.reconciled = false;
        }
        let node = &self.nodes[&disk];
        let peers: Vec<u64> = node
            .view
            .digest()
            .iter()
            .map(|l| l.disk)
            .filter(|p| *p != disk)
            .collect();
        let near: BTreeSet<u64> = peers
            .iter()
            .copied()
            .filter(|p| self.near(disk, *p))
            .collect();
        let named: BTreeSet<(u64, u64)> = held
            .iter()
            .filter_map(|id| self.set_of(disk, *id).map(|s| (*id, s)))
            .flat_map(|(id, s)| s.holders.iter().map(move |h| (id, *h)))
            .collect();
        let unopened: BTreeSet<u64> = held
            .iter()
            .copied()
            .filter(|id| self.set_of(disk, *id).is_none())
            .collect();
        if let Some(node) = self.nodes.get_mut(&disk) {
            node.late
                .retain(|p, _| !near.contains(p) && peers.contains(p));
            for p in peers.iter().filter(|p| !near.contains(p)) {
                node.late.entry(*p).or_insert(now);
            }
            if !held.is_empty() {
                node.named
                    .retain(|k, _| named.contains(k) || unopened.contains(&k.0));
                for k in named {
                    node.named.entry(k).or_insert(now);
                }
            }
        }
        let node = &self.nodes[&disk];
        let stale: BTreeSet<u64> = node
            .trips
            .keys()
            .copied()
            .filter(|p| !self.near(disk, *p))
            .collect();
        let mut fences = node.fences.clone();
        let mut behind: Vec<u64> = Vec::new();
        let node = &self.nodes[&disk];
        let mut lifts = Vec::new();
        for id in held {
            let Some(set) = self.set_of(disk, id) else {
                continue;
            };
            let unseen = |h: &u64| *h != disk && (stale.contains(h) || !node.trips.contains_key(h));
            if !set.holders.contains(&disk)
                || (!fences.contains_key(&id) && !set.holders.iter().any(unseen))
            {
                continue;
            }
            let mut lost = fences.remove(&id).unwrap_or_default();
            let mut was = lost.clone();
            was.extend(
                set.holders
                    .iter()
                    .copied()
                    .filter(|h| *h != disk && !near.contains(h)),
            );
            if self.segment_fenced(disk, id, &mut lost) {
                behind.push(id);
                fences.insert(id, lost.clone());
            }
            lifts.extend(
                was.difference(&lost)
                    .map(|x| ((id, *x), self.lift_key(disk, id, *x))),
            );
        }
        let kept: Vec<(u64, u64)> = node
            .lifts
            .iter()
            .filter(|((id, x), key)| {
                self.segments
                    .get(id)
                    .is_some_and(|s| s.marks.contains_key(&disk))
                    && self.lift_key(disk, *id, *x) == **key
            })
            .map(|(k, _)| *k)
            .collect();
        if let Some(node) = self.nodes.get_mut(&disk) {
            node.lifts.retain(|k, _| kept.contains(k));
            node.lifts.extend(lifts);
        }
        let node = &self.nodes[&disk];
        let unsynced: Vec<u64> = node
            .raft
            .meta()
            .segments
            .iter()
            .filter(|(id, m)| m.leader == disk && !synced(node, **id, m))
            .map(|(id, _)| *id)
            .collect();
        for id in behind.into_iter().chain(unsynced) {
            self.catch_up(disk, id);
        }
        let node = &self.nodes[&disk];
        let live: Vec<u64> = node
            .view
            .live()
            .map(|(p, _)| p)
            .filter(|p| *p != disk)
            .collect();
        let young = now < node.booted_ms + FENCE_MS;
        let fresh: BTreeSet<u64> = node
            .unreachable
            .keys()
            .chain(&live)
            .copied()
            .filter(|p| young || self.near(disk, *p))
            .collect();
        let Some(node) = self.nodes.get_mut(&disk) else {
            return;
        };
        fence::unreachable(&mut node.unreachable, &live, |p| fresh.contains(&p), now);
        fences.retain(|id, _| {
            self.segments
                .get(id)
                .is_some_and(|s| s.marks.contains_key(&disk))
        });
        let Some(node) = self.nodes.get_mut(&disk) else {
            return;
        };
        node.fences = fences;
        node.full = full;
        if !full && !node.reconciled {
            self.reconcile(disk);
        }
        let Some(node) = self.nodes.get(&disk) else {
            return;
        };
        let mut body = node.view.own().body.clone();
        self.status(disk, &mut body);
        if let Some(node) = self.nodes.get_mut(&disk)
            && node.view.own().body != body
        {
            node.view.set_body(body, now, run);
        }
    }

    fn near(&self, by: u64, peer: u64) -> bool {
        self.nodes[&by]
            .trips
            .get(&peer)
            .is_some_and(|(_, at)| self.now_ms < at + FENCE_MS)
    }

    fn lift_key(&self, disk: u64, id: u64, x: u64) -> Lift {
        let node = &self.nodes[&disk];
        let set = self.set_of(disk, id).map_or(0, fingerprint);
        let view = node.view.entry(x).map(|e| (e.head.restart, e.head.state));
        (node.late.get(&x).copied().unwrap_or(0), view, set)
    }

    fn segment_fenced(&self, disk: u64, id: u64, lost: &mut BTreeSet<u64>) -> bool {
        let node = &self.nodes[&disk];
        let (view, list) = (&node.view, node.raft.meta());
        let Some(set) = self.set_of(disk, id) else {
            return false;
        };
        let co: Vec<u64> = set.holders.iter().copied().filter(|h| *h != disk).collect();
        let holders = set.holders.len();
        let late: Vec<u64> = co
            .iter()
            .copied()
            .filter(|h| !self.near(disk, *h))
            .collect();
        lost.retain(|x| co.contains(x));
        if late.is_empty() && lost.is_empty() {
            return false;
        }
        let own = self.marks_of(disk, id);
        let answer = |c: u64, x: u64| {
            node.fence_answers
                .get(&(id, c))
                .filter(|a| self.now_ms < a.2 + FENCE_MS)
                .map(|a| Answer {
                    absent: a.0.contains(&x),
                    marks: a.1.clone(),
                })
        };
        fence::fenced(lost, &late, |x| {
            if node.lifts.get(&(id, x)) == Some(&self.lift_key(disk, id, x)) {
                return true;
            }
            let from_x = answer(x, x);
            let late = node.late.get(&x);
            let witness = |c: u64| {
                late.is_some_and(|l| node.named.get(&(id, c)).is_some_and(|t| t < l))
                    || node
                        .fence_answers
                        .get(&(id, c))
                        .is_some_and(|a| self.now_ms < a.2 + FENCE_MS && a.3.contains(&x))
            };
            let others: Vec<Option<Answer>> = co
                .iter()
                .filter(|c| **c != x && !view.absent(**c) && witness(**c))
                .map(|c| answer(*c, x))
                .collect();
            let alone = from_x.is_none() && !fence::quorum(others.len(), holders);
            let missed = late.is_none_or(|l| {
                let after = |c: &u64| node.caught.get(&(id, *c)).is_some_and(|t| t >= l);
                let fetched = co
                    .iter()
                    .filter(|c| after(c) && node.named.get(&(id, **c)).is_some_and(|t| t < l))
                    .count();
                !after(&disk) && !fence::quorum(fetched, holders)
            });
            if alone && (missed || !others.is_empty()) && list.members.contains(&x) {
                return false;
            }
            let members: Vec<Answer> = view
                .live()
                .filter(|(d, _)| alone && list.members.contains(d) && *d != disk && *d != x)
                .filter_map(|(d, _)| answer(d, x))
                .collect();
            let set = if alone { list.members.len() } else { 0 };
            fence::lifted(
                &own,
                from_x.as_ref(),
                view.absent(x),
                &others.iter().map(Option::as_ref).collect::<Vec<_>>(),
                holders,
                &members.iter().collect::<Vec<_>>(),
                set,
            )
        })
    }

    fn lifted_of(&self, disk: u64, id: u64, xs: &BTreeSet<u64>) -> BTreeSet<u64> {
        let node = &self.nodes[&disk];
        let seg = &self.segments[&id];
        let Some(set) = self.set_of(disk, id) else {
            return BTreeSet::new();
        };
        if !seg.marks.contains_key(&disk) || !set.holders.contains(&disk) {
            return BTreeSet::new();
        }
        let mut lost = node.fences.get(&id).cloned().unwrap_or_default();
        self.segment_fenced(disk, id, &mut lost);
        xs.iter()
            .copied()
            .filter(|x| set.holders.contains(x) && node.late.contains_key(x) && !lost.contains(x))
            .collect()
    }

    fn expire(&mut self, disk: u64) {
        let Some(d) = self.disks.get_mut(&disk) else {
            return;
        };
        let alive = d.alive_ms;
        let gone: Vec<(u64, u64)> = d
            .records
            .iter()
            .filter(|(_, (_, _, at))| fence::expired(*at, alive))
            .map(|(key, _)| *key)
            .collect();
        for (origin, seq) in gone {
            d.records.remove(&(origin, seq));
            let forgot = d.forgot.entry(origin).or_insert(0);
            *forgot = (*forgot).max(seq);
        }
        d.dropped.retain(|_, at| !fence::expired(*at, alive));
    }

    fn reconcile(&mut self, disk: u64) {
        let now = self.now_ms;
        let Some(node) = self.nodes.get_mut(&disk) else {
            return;
        };
        let start = *node.reconcile_ms.get_or_insert(now);
        let node = &self.nodes[&disk];
        let list = node.raft.meta();
        let members: Vec<u64> = node
            .view
            .live()
            .map(|(d, _)| d)
            .filter(|d| *d != disk && list.members.contains(d))
            .collect();
        let stale: Vec<u64> = members
            .iter()
            .copied()
            .filter(|m| {
                node.peers
                    .get(m)
                    .is_none_or(|p| p.3 < start || now >= p.3 + GOSSIP_MS)
            })
            .collect();
        for m in stale {
            self.send(disk, m, Msg::MarksAsk);
        }
        let node = &self.nodes[&disk];
        let held: Vec<u64> = self
            .segments
            .iter()
            .filter(|(_, s)| s.marks.contains_key(&disk) && s.born_ms < start)
            .map(|(id, _)| *id)
            .collect();
        let mut fates: Vec<(u64, Vec<Option<Known>>)> = Vec::new();
        let mut asks = Vec::new();
        for id in &held {
            let own = self.marks_of(disk, *id);
            let co: Vec<u64> = self
                .set_of(disk, *id)
                .iter()
                .flat_map(|s| s.holders.iter().copied())
                .filter(|h| *h != disk && !node.view.absent(*h))
                .collect();
            let mut known = Vec::new();
            for c in co {
                let a = node
                    .seg_answers
                    .get(&(*id, c))
                    .filter(|a| a.2 >= start && fence::reached(&own, &a.1));
                if a.is_none() && self.near(disk, c) {
                    asks.push((*id, c));
                }
                known.push(a.map(|a| a.0));
            }
            fates.push((*id, known));
        }
        let behind = asks
            .iter()
            .any(|k| node.named.get(k).is_none_or(|t| *t <= start));
        for (id, c) in asks {
            self.ask_seg(disk, id, c);
        }
        let node = &self.nodes[&disk];
        let list = node.raft.meta();
        let fresh = |t: u64| t >= start && now < t + 2 * GOSSIP_MS;
        let answer = |m: u64| node.peers.get(&m).filter(|p| fresh(p.3)).cloned();
        let own_pairs = self.reported(disk);
        let cut = |m: u64| {
            let votes = list
                .members
                .iter()
                .filter(|v| {
                    let pairs = if **v == disk {
                        Some(&own_pairs)
                    } else {
                        node.peers.get(v).filter(|p| fresh(p.3)).map(|p| &p.4)
                    };
                    pairs.is_some_and(|p| p.contains(&pair(disk, m)))
                })
                .count();
            confirmed(votes, list.members.len())
        };
        if members
            .iter()
            .any(|m| answer(*m).is_none() && (self.near(disk, *m) || !cut(*m)))
        {
            return;
        }
        let answers: Vec<(u64, PeerMarks)> = members
            .iter()
            .filter_map(|m| answer(*m).map(|a| (*m, a)))
            .collect();
        if answers
            .iter()
            .any(|(_, a)| node.new_member && a.2 < list.version)
        {
            return;
        }
        let own = &self.disks[&disk];
        let wipe = answers
            .iter()
            .any(|(_, a)| !fence::reached(&own.marks, &a.1));
        let node = &self.nodes[&disk];
        let new_disk = node.view.own().head.restart == 1 && !node.joined;
        if wipe && (!new_disk || !held.is_empty()) {
            if behind {
                return;
            }
            let kept: Vec<u64> = fates
                .iter()
                .filter(|(_, known)| {
                    let all: Vec<Known> = known.iter().flatten().copied().collect();
                    known.contains(&None) || fence::fate(true, &all) == Fate::Last
                })
                .map(|(id, _)| *id)
                .chain(
                    self.segments
                        .iter()
                        .filter(|(_, s)| s.marks.contains_key(&disk) && s.born_ms >= start)
                        .map(|(id, _)| *id),
                )
                .collect();
            let members: Vec<u64> = answers.iter().map(|(m, _)| *m).collect();
            self.wipe(disk, &members, &kept);
            return;
        }
        let mut whole = !behind;
        for (id, known) in fates {
            if known.contains(&None) {
                continue;
            }
            let known: Vec<Known> = known.into_iter().flatten().collect();
            if fence::fate(false, &known) == Fate::Delete {
                if let Some(seg) = self.segments.get_mut(&id) {
                    seg.marks.remove(&disk);
                }
                if known.contains(&Known::Dropped)
                    && let Some(d) = self.disks.get_mut(&disk)
                {
                    d.dropped.insert(id, d.alive_ms);
                }
            }
        }
        let own = self.disks[&disk].marks.clone();
        for (m, a) in &answers {
            if !fence::reached(&own, &a.0) {
                whole = false;
                self.ask_records(disk, *m, own.clone(), None);
            }
        }
        if whole && let Some(node) = self.nodes.get_mut(&disk) {
            node.reconciled = true;
            node.joined = true;
            node.new_member = false;
            node.reconcile_ms = None;
        }
    }

    fn wipe(&mut self, old: u64, members: &[u64], kept: &[u64]) {
        let kept: Vec<(u64, u64)> = kept
            .iter()
            .map(|id| (*id, self.segments[id].marks[&old]))
            .collect();
        let erased = self.disks[&old].erased.clone();
        let (zone, format_max) = (self.zone(old), self.nodes[&old].format_max);
        let mut sets: BTreeMap<u64, HolderSet> = self
            .segments
            .keys()
            .filter_map(|id| self.set_of(old, *id).map(|set| (*id, set.clone())))
            .collect();
        for seg in self.segments.values_mut() {
            seg.marks.remove(&old);
        }
        self.down(old);
        self.disks.remove(&old);
        let new = self.create(&zone, 0, format_max);
        for m in members {
            self.ask_records(new, *m, Marks::new(), None);
        }
        for (id, mark) in kept {
            if erased.contains(&id)
                && let Some(d) = self.disks.get_mut(&new)
            {
                d.erased.insert(id);
            }
            let (Some(seg), Some(own)) = (self.segments.get_mut(&id), sets.remove(&id)) else {
                continue;
            };
            seg.marks.insert(new, mark);
            seg.copied.insert(new, self.now_ms);
            let mut holders = own.holders.clone();
            holders.remove(&old);
            holders.insert(new);
            self.wish(new, own.with(new, holders));
            self.warnings.insert(id);
        }
        self.wiped.push((old, new));
    }

    fn gossip(&mut self, disk: u64) {
        let Some(node) = self.nodes.get(&disk) else {
            return;
        };
        let mut peers: Vec<u64> = node
            .view
            .live()
            .map(|(d, _)| d)
            .filter(|d| *d != disk)
            .collect();
        peers.shuffle(&mut self.rng);
        peers.truncate(FANOUT);
        for peer in peers {
            self.exchange(disk, peer);
        }
    }

    fn exchange(&mut self, from: u64, to: u64) {
        if self.cuts.contains(&pair(from, to)) {
            self.missed_gossip += 1;
            return;
        }
        let (Some(a), Some(b)) = (self.nodes.get(&from), self.nodes.get(&to)) else {
            return;
        };
        let to_a = b.view.answer(&a.view.digest());
        let to_b = a.view.answer(&b.view.digest());
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
        let (Some(a), Some(b)) = (self.nodes.get(&from), self.nodes.get(&to)) else {
            return;
        };
        let replies = [
            (from, b.view.answer(&a.view.digest())),
            (to, a.view.answer(&b.view.digest())),
        ];
        let now = self.now_ms;
        for (target, updates) in replies {
            let run = self.run_of(target);
            if let Some(node) = self.nodes.get_mut(&target) {
                node.view.merge(&updates, now, run);
            }
        }
    }

    fn sample(&mut self, disk: u64) {
        let run = self.run_of(disk);
        let now = self.now_ms;
        let holds = self.holds(disk);
        let cap = self.capacity.get(&disk).copied();
        let Some(node) = self.nodes.get_mut(&disk) else {
            return;
        };
        let live: Vec<(u64, Body)> = node.view.live().map(|(d, e)| (d, e.body.clone())).collect();
        let bodies: Vec<&Body> = live.iter().map(|(_, b)| b).collect();
        for (peer, b) in &live {
            let meets = steer::meets(b, &bodies);
            let (hold, level) = node.heavy.entry(*peer).or_default();
            let was = hold.heavy;
            if hold.sample(meets, run) && !was {
                *level = steer::fill(b);
            }
        }
        let refs: Vec<(u64, &Body)> = live.iter().map(|(d, b)| (*d, b)).collect();
        let result = domains::infer(&refs, &node.view.co_losses());
        let hash = result.hash;
        let reports: Vec<u64> = live.iter().map(|(_, b)| b.domains).collect();
        node.domains = domains::agree(result, &reports, node.domains.take());
        let d = node.domains.as_ref().map_or(1, Domains::count);
        let before = node.ctl.count;
        let count = node.ctl.sample(&node.view, d, self.headroom, run).count;
        let debt = self.debt(disk, count);
        let mut body = self.nodes[&disk].view.own().body.clone();
        self.status(disk, &mut body);
        let Some(node) = self.nodes.get_mut(&disk) else {
            return;
        };
        if let Some(cap) = cap {
            body.disk_total = cap;
            body.disk_free = cap.saturating_sub(holds);
            body.fill_rate = holds.saturating_sub(body.holds);
        }
        body.holds = holds;
        body.debt = debt;
        body.count = count;
        body.quiet = node.view.quiet(run, QUIET_PERIOD);
        body.domains = hash;
        body.alive_ms = run;
        body.committed = node.ctl.committed;
        body.signals = node.signals;
        node.view.set_body(body, now, run);
        let acts = node.raft.is_leader();
        let (members, version) = (&node.raft.meta().members, node.raft.meta().version);
        let held = node.raft.meta().floor;
        let decided = node.ctl.decide(&node.view, members, version, acts, run);
        node.ctl.floor = held;
        let committed = node.ctl.commit(&node.view, members, version, acts, run);
        let member = node.view.durable(node.ctl.count);
        let refused = refuse(committed, node.format_max);
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
            self.propose_floor(disk, to, cause);
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
            self.write_groups(disk);
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
