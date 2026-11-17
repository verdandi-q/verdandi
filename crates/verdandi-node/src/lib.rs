use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use raft::eraftpb::{ConfChangeType, Message, MessageType};
use verdandi_control::{Command, Control, Saved};
use verdandi_core::cluster::controller::{
    self, Controller, Floor, QUIET_PERIOD, confirmed, refuse,
};
use verdandi_core::cluster::domains::{self, Domains};
use verdandi_core::cluster::placement::{self, Candidate, Groups, HolderSet};
use verdandi_core::cluster::repair::{self, Held, Want};
use verdandi_core::cluster::steer::{self, Hold};
use verdandi_core::cluster::view::{
    Body, GOSSIP_MS, HEARTBEAT_MS, Head, JOIN_MS, LIFECYCLE, SIGTERM, STOP_BUDGET_MS, State,
    TOMBSTONE_HORIZON, Update, View,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cause {
    Rise,
    Quiet,
    Merge,
    Drain,
    AcceptLoss,
}

pub const FENCE_MS: u64 = 1_500;
pub const STEP_MS: u64 = HEARTBEAT_MS;
pub const HEARD_MS: u64 = 10 * STEP_MS;
pub const LEASE_MS: u64 = 3_000;
pub(crate) const REPLICATE_MS: u64 = 5_000;
pub const ISR_MS: u64 = 2 * LEASE_MS;
const REPLACE_MS: u64 = 30_000;
const STOPS: u8 = SIGTERM | LIFECYCLE;
const SEG_ASKS_MAX: usize = 64;
const FRESH_MS: u64 = 2 * GOSSIP_MS;
const DROPS_MAX: usize = 64;
const RECORDS_MAX: usize = 64;
pub const STREAMS: u64 = 3;

pub fn pair(a: u64, b: u64) -> (u64, u64) {
    (a.min(b), a.max(b))
}

pub type Marks = BTreeMap<u64, u64>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Known {
    Copy,
    Partial,
    Dropped,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fate {
    Keep,
    Delete,
    Last,
}

pub fn reached(own: &Marks, theirs: &Marks) -> bool {
    theirs
        .iter()
        .all(|(origin, mark)| own.get(origin).copied().unwrap_or(0) >= *mark)
}

pub fn unreachable(
    list: &mut BTreeMap<u64, Option<u64>>,
    live: &[u64],
    fresh: impl Fn(u64) -> bool,
    now_ms: u64,
) {
    for peer in live {
        if !fresh(*peer) {
            list.entry(*peer).or_insert(None);
        }
    }
    for (peer, since) in list.iter_mut() {
        if !fresh(*peer) {
            *since = None;
        } else if since.is_none() {
            *since = Some(now_ms);
        }
    }
    list.retain(|peer, since| live.contains(peer) && since.is_none_or(|t| now_ms < t + JOIN_MS));
}

pub fn expired(applied_alive_ms: u64, alive_ms: u64) -> bool {
    alive_ms >= applied_alive_ms + TOMBSTONE_HORIZON
}

pub fn fate(wiping: bool, answers: &[Known]) -> Fate {
    let unknown = !answers.is_empty() && answers.iter().all(|a| *a == Known::Unknown);
    if unknown || answers.contains(&Known::Dropped) {
        Fate::Delete
    } else if !wiping {
        Fate::Keep
    } else if answers.contains(&Known::Copy) {
        Fate::Delete
    } else {
        Fate::Last
    }
}

pub fn no_holder(
    me: u64,
    holders: &BTreeSet<u64>,
    live: impl Fn(u64) -> bool,
    complete: impl Fn(u64) -> bool,
) -> bool {
    !complete(me) && !holders.iter().any(|h| *h != me && live(*h) && complete(*h))
}

mod raft_bytes {
    use protobuf::Message as _;
    use raft::eraftpb::Message;
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn serialize<S: Serializer>(m: &Message, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(&m.write_to_bytes().map_err(serde::ser::Error::custom)?)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Message, D::Error> {
        Message::parse_from_bytes(&Vec::<u8>::deserialize(d)?).map_err(D::Error::custom)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Msg {
    Gossip(Vec<Update>),
    Ping(Head),
    Pong(u64, Head),
    Raft(#[serde(with = "raft_bytes")] Message),
    Lose(u64),
    Record(u64, u64, Rec, BTreeSet<u64>),
    Ack(u64, u64, u64, u64),
    Acked(u64, bool, u64),
    Wish(HolderSet),
    IsrAsk(u64, u64, u64, BTreeSet<u64>),
    Fresh(u64, u64),
    SegAsk(u64, u64),
    Seg(u64, Option<u64>, Known),
    Beat(u64, u64),
    Copied(u64, u64, u64, bool),
    Beaten(u64, u64, u64),
    Drop(u64),
    Dropped(u64),
    Named(u64, BTreeSet<u64>),
    Confirm(u64, u64, u64, u64),
    VoteAsk(u64),
    Vote(BTreeSet<u64>, u64),
    MarksAsk,
    MarksIs(Marks, Marks, BTreeSet<(u64, u64)>),
    RecordsAsk(Marks, Option<(u64, u64)>),
    Records(Vec<((u64, u64), u64, u64)>, Marks, bool),
}

impl Msg {
    pub fn relayable(&self) -> bool {
        matches!(
            self,
            Msg::Raft(_) | Msg::IsrAsk(..) | Msg::Wish(_) | Msg::Fresh(..)
        )
    }
}

#[derive(Clone, Debug)]
pub enum Cmd {
    Signal(u8),
    Stop,
    Drain,
    AcceptLoss {
        disk: u64,
    },
    Wish {
        set: HolderSet,
    },
    Leave {
        remaining: u32,
    },
    Departed {
        disk: u64,
        restart: u64,
        at: u64,
    },
    Delete {
        id: u64,
        stream: u64,
        upto: u64,
    },
    AnswerRequest {
        id: u64,
    },
    Guard,
    Adopt {
        id: u64,
        mark: u64,
        born: u64,
        writer: Option<u64>,
        unnamed: Option<(u64, Option<u64>)>,
        named: Vec<((u64, u64), u64)>,
    },
    Erase {
        id: u64,
        sender_erased: bool,
    },
    Offer {
        id: u64,
        epoch: u64,
    },
    DropStart {
        id: u64,
    },
    Handoff {
        id: u64,
        to: u64,
        epoch: u64,
        mark: u64,
    },
    AskRecords {
        peer: u64,
    },
    Place {
        id: u64,
    },
    Seal {
        reached: usize,
    },
    Take {
        id: u64,
    },
    Read,
    Probe {
        id: u64,
    },
    Pick {
        id: u64,
        asked: Vec<u64>,
        open: Vec<u64>,
    },
    DropCheck {
        id: u64,
    },
    Settle {
        id: u64,
        sid: u64,
    },
    Answer {
        sid: u64,
    },
    Tick,
    Ping,
    SegAsks,
    Fresh,
    Beats,
    IsrAsks,
    ResendCopied,
    ResendDrops,
    Govern,
    Lead,
    Floor {
        to: Floor,
        cause: Cause,
    },
    WriteGroups,
    Stale {
        id: u64,
        epoch: u64,
    },
}

#[derive(Debug)]
pub enum Effect {
    Dropping,
    Newer {
        epoch: u64,
    },
    Copy {
        writer: Option<u64>,
        unnamed: Option<(u64, Option<u64>)>,
        named: Vec<((u64, u64), u64)>,
    },
    Picked(Vec<u64>),
    RefusedWrite {
        resealed: bool,
    },
    Accepted(bool),
    Stored {
        format: u16,
    },
    Settled {
        id: u64,
        next: u64,
        started_ms: u64,
    },
    Refused,
    Mark {
        id: u64,
        mark: u64,
    },
    Ask {
        peers: Vec<u64>,
    },
    Open {
        by: u64,
    },
    Served {
        by: u64,
        id: u64,
        mark: u64,
    },
    Unserved {
        id: u64,
        partial: bool,
    },
    DropDone {
        id: u64,
    },
    Saved(Arc<Saved>),
    Led {
        term: u64,
    },
    Decided {
        from: Floor,
        to: Floor,
        cause: Cause,
    },
    RaftRefused,
    Wish {
        by: u64,
        set: HolderSet,
    },
    Unmark {
        id: u64,
    },
    Epoch {
        id: u64,
        epoch: u64,
    },
    Replicated {
        id: u64,
        sent: u64,
    },
    Dropped {
        id: u64,
        run: u64,
    },
    DrainCommit,
    Persist(Store),
    WishDone {
        key: (u64, u64),
    },
    Stopped,
    Left {
        restart: u64,
        own: Vec<Update>,
    },
    Expired {
        alive: u64,
    },
    Announced,
    Tomb {
        id: u64,
        run: u64,
    },
    Wipe {
        members: Vec<u64>,
        kept: Vec<(u64, u64, Option<u64>, u64)>,
        erased: BTreeSet<u64>,
    },
    RequestGone {
        id: u64,
    },
    RequestDone {
        id: u64,
        key: (u64, u64),
        stream: u64,
        upto: u64,
    },
}

pub struct Job {
    pub copies: usize,
    pub segment: u64,
    pub epoch: u64,
    pub from: u64,
    pub need: usize,
    pub to: Vec<u64>,
}

pub struct Knobs<'a> {
    pub open: &'a BTreeSet<u64>,
    pub held: &'a BTreeSet<u64>,
}

pub struct Pass<'a> {
    groups: &'a Groups,
    live: BTreeSet<u64>,
    want: Want,
}

#[derive(Clone, Debug, Default)]
pub struct Store {
    pub records: BTreeMap<(u64, u64), (u64, u64, u64)>,
    pub marks: Marks,
    pub(crate) forgot: Marks,
    pub(crate) sequence: u64,
    pub erased: BTreeSet<u64>,
}

pub struct Request {
    pub(crate) rec: Rec,
    pub(crate) started_ms: u64,
    pub(crate) scope: BTreeSet<u64>,
    pub(crate) confirmed: BTreeSet<u64>,
    pub(crate) newest: u64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Rec {
    Delete((u64, u64), u64, u64),
}

pub fn led(node: &Node, disk: u64, now: u64, within: u64) -> bool {
    node.raft.meta().members.contains(&disk)
        && (node.raft.is_leader() || node.heard_ms.is_some_and(|t| now < t + within))
}

pub fn groups_of(node: &Node) -> Option<&Groups> {
    Some(&node.raft.meta().groups).filter(|g| g.epoch > 0)
}
pub(crate) type IsrKey = (u64, u64, u64, u64, Vec<u64>, Vec<u64>);

#[derive(Default)]
pub struct Lease {
    pub(crate) epoch: u64,
    pub(crate) since_ms: Option<u64>,
    pub(crate) beats: BTreeMap<u64, u64>,
    pub(crate) pending: BTreeSet<u64>,
    pub(crate) unanswered: BTreeMap<u64, u64>,
}
pub struct Settle {
    pub(crate) epoch: u64,
    pub(crate) segment: u64,
    pub(crate) next: u64,
    pub(crate) started_ms: u64,
    pub(crate) scope: BTreeSet<u64>,
    pub(crate) set: BTreeSet<u64>,
    pub(crate) acked: BTreeMap<u64, bool>,
}
pub(crate) type Vote = (BTreeSet<u64>, u64);
pub(crate) type PeerMarks = (Marks, Marks, u64, u64, BTreeSet<(u64, u64)>);
pub(crate) type SegAnswer = (Known, Marks, u64);
pub(crate) type Queued = ((u64, u64), u64);
pub struct Sense {
    pub now: u64,
    pub run: u64,
    pub cap: Option<u64>,
    pub used: u64,
    pub headroom: f64,
    pub sizes: Marks,
}

pub struct Sampled {
    pub before: u8,
    pub count: u8,
    pub decided: Option<Floor>,
    pub acts: bool,
    pub held: Floor,
    pub committed: u16,
    pub member: Vec<u8>,
    pub refused: bool,
}

pub(crate) fn node_status(node: &Node, now: u64, body: &mut Body) {
    body.reconciled = node.reconciled;
    body.marks = node.marks.keys().copied().collect();
    body.unreachable = node.unreachable.keys().copied().collect();
    body.epoch = node.raft.meta().shape;
    if body.drained {
        body.remaining = node.remaining(now);
    }
}

pub fn node_sample(node: &mut Node, sense: &Sense) -> Sampled {
    let (now, run) = (sense.now, sense.run);
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
    node.headroom = sense.headroom;
    let count = node.ctl.sample(&node.view, d, sense.headroom, run).count;
    let debt = node
        .held(now)
        .into_iter()
        .filter(|(_, copies)| *copies < usize::from(count))
        .map(|(id, _)| sense.sizes.get(&id).copied().unwrap_or(0))
        .sum();
    let mut body = node.view.own().body.clone();
    node_status(node, now, &mut body);
    if let Some(cap) = sense.cap {
        body.disk_total = cap;
        body.disk_free = cap.saturating_sub(sense.used);
        body.fill_rate = sense.used.saturating_sub(body.holds);
    }
    body.holds = sense.used;
    body.debt = debt;
    body.count = count;
    body.quiet = node.view.quiet(run, QUIET_PERIOD);
    body.domains = hash;
    body.alive_ms = run;
    body.committed = node.ctl.committed;
    body.signals = node.signals;
    node.view.set_body(body, now, run);
    let acts = node.raft.is_leader();
    let (members, version) = (&node.raft.meta().members, node.raft.meta().shape);
    let held = node.raft.meta().floor;
    let decided = node.ctl.decide(&node.view, members, version, acts, run);
    node.ctl.floor = held;
    let committed = node.ctl.commit(&node.view, members, version, acts, run);
    let member = node.view.durable(node.ctl.count);
    let refused = refuse(committed, node.format_max);
    Sampled {
        before,
        count,
        decided,
        acts,
        held,
        committed,
        member,
        refused,
    }
}

pub struct Node {
    pub(crate) me: u64,
    pub marks: Marks,
    pub epochs: Marks,
    pub dropped: Marks,
    pub view: View,
    pub ctl: Controller,
    pub domains: Option<Domains>,
    pub format_max: u16,
    pub signals: u8,
    pub phase_ms: u64,
    pub(crate) boot_run_ms: u64,
    pub(crate) proposed_groups: Option<(u64, u64)>,
    pub stop_run_ms: Option<u64>,
    pub(crate) heavy: BTreeMap<u64, (Hold, u64)>,
    pub(crate) trips: BTreeMap<u64, (u64, u64)>,
    pub reconciled: bool,
    pub joined: bool,
    pub(crate) unreachable: BTreeMap<u64, Option<u64>>,
    pub(crate) opened: Option<Floor>,
    pub(crate) proposed: Option<(u64, Floor)>,
    pub(crate) fall: Option<Cause>,
    pub raft: Control,
    pub(crate) heard_ms: Option<u64>,
    pub fresh: Vec<(u64, u64)>,
    pub adopted_ms: u64,
    pub(crate) lose: BTreeSet<u64>,
    pub(crate) booted_ms: u64,
    pub(crate) proposed_segs: BTreeMap<(u64, u64, u64, u64), u64>,
    pub(crate) lease: BTreeMap<u64, Lease>,
    pub(crate) proposed_isr: BTreeSet<IsrKey>,
    pub seen_records: BTreeMap<u64, u64>,
    pub(crate) records_asked: BTreeMap<u64, u64>,
    pub booting: BTreeSet<u64>,
    pub votes: BTreeMap<u64, Vote>,
    pub(crate) vote_ms: Option<u64>,
    pub peers: BTreeMap<u64, PeerMarks>,
    pub reconcile_ms: Option<u64>,
    pub named: BTreeMap<(u64, u64), u64>,
    pub headroom: f64,
    pub writers: Marks,
    pub(crate) seg_answers: BTreeMap<(u64, u64), SegAnswer>,
    confirm: RefCell<BTreeSet<(u64, u64)>>,
    pub(crate) seg_queue: BTreeMap<(u64, u64), u64>,
    pub(crate) seg_asked: BTreeMap<(u64, u64), u64>,
    pub(crate) drops: BTreeMap<u64, BTreeSet<u64>>,
    pub drop_waits: BTreeMap<u64, BTreeSet<u64>>,
    pub(crate) drop_seen: BTreeMap<u64, u64>,
    pub(crate) drop_epoch: BTreeMap<u64, u64>,
    pub(crate) stale: BTreeMap<u64, u64>,
    pub(crate) unnamed: BTreeMap<u64, (u64, u64, Option<u64>, u64)>,
    pub(crate) acked: BTreeSet<(u64, u64)>,
    pub settles: BTreeMap<u64, Settle>,
    pub saved: Arc<Saved>,
    pub(crate) now: u64,
    pub(crate) run: u64,
    pub(crate) isr_asks: Vec<(u64, u64, u64, BTreeSet<u64>)>,
    pub(crate) store: Store,
    pub(crate) store_dirty: bool,
    pub(crate) requests: BTreeMap<u64, Request>,
    pub(crate) pairs: BTreeSet<(u64, u64)>,
    pub born: Marks,
}

pub fn kept(node: &Node, id: u64, m: &verdandi_control::Segment) -> BTreeSet<u64> {
    let mut set = m.isr.clone();
    if let Some(l) = node.lease.get(&id).filter(|l| l.epoch == m.leader_epoch) {
        set.extend(l.pending.iter().filter(|h| m.holders.holders.contains(h)));
    }
    set
}

pub(crate) fn enough_in(set: &BTreeSet<u64>, leader: u64, stored: &BTreeSet<u64>) -> bool {
    set.len() >= usize::from(controller::KEYED_MIN_COPIES)
        && set.iter().all(|h| *h == leader || stored.contains(h))
}

pub struct Inbound {
    pub from: u64,
    pub sent: u64,
    pub version: u64,
    pub msg: Msg,
}

pub struct Input {
    pub now: u64,
    pub run: u64,
    pub cmds: Vec<Cmd>,
    pub frames: Vec<Inbound>,
    pub partial: bool,
    pub pairs: BTreeSet<(u64, u64)>,
}

impl Input {
    pub fn cmd(now: u64, run: u64, cmd: Cmd) -> Self {
        Self {
            now,
            run,
            cmds: vec![cmd],
            frames: Vec::new(),
            partial: false,
            pairs: BTreeSet::new(),
        }
    }
}

pub type Send<'a> = &'a mut dyn FnMut(u64, Msg, u64) -> bool;

#[derive(Default)]
pub struct Output<'a> {
    pub effects: Vec<Effect>,
    pub frames: Vec<(u64, Msg)>,
    send: Option<Send<'a>>,
}

impl Node {
    // allow: one constructor takes the whole boot state
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        me: u64,
        now: u64,
        run: u64,
        format_max: u16,
        phase_ms: u64,
        view: View,
        ctl: Controller,
        raft: Control,
        saved: Arc<Saved>,
        store: Store,
    ) -> Self {
        Self {
            me,
            marks: Marks::new(),
            epochs: Marks::new(),
            dropped: Marks::new(),
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
            reconciled: false,
            joined: false,
            unreachable: BTreeMap::new(),
            opened: None,
            proposed: None,
            fall: None,
            raft,
            heard_ms: None,
            fresh: Vec::new(),
            adopted_ms: now,
            lose: BTreeSet::new(),
            booted_ms: now,
            proposed_segs: BTreeMap::new(),
            lease: BTreeMap::new(),
            proposed_isr: BTreeSet::new(),
            seen_records: BTreeMap::new(),
            records_asked: BTreeMap::new(),
            booting: BTreeSet::new(),
            votes: BTreeMap::new(),
            vote_ms: None,
            peers: BTreeMap::new(),
            reconcile_ms: None,
            named: BTreeMap::new(),
            headroom: 0.0,
            writers: Marks::new(),
            seg_answers: BTreeMap::new(),
            confirm: RefCell::new(BTreeSet::new()),
            seg_queue: BTreeMap::new(),
            seg_asked: BTreeMap::new(),
            drops: BTreeMap::new(),
            drop_waits: BTreeMap::new(),
            drop_seen: BTreeMap::new(),
            drop_epoch: BTreeMap::new(),
            stale: BTreeMap::new(),
            unnamed: BTreeMap::new(),
            acked: BTreeSet::new(),
            settles: BTreeMap::new(),
            saved,
            now,
            run,
            isr_asks: Vec::new(),
            store,
            store_dirty: false,
            requests: BTreeMap::new(),
            pairs: BTreeSet::new(),
            born: Marks::new(),
        }
    }
}

impl Node {
    pub fn step(&mut self, input: Input, send: Send<'_>) -> Output<'static> {
        let Input {
            now,
            run,
            cmds,
            frames,
            partial,
            pairs,
        } = input;
        self.now = now;
        self.run = run;
        self.pairs = pairs;
        let mut out = Output {
            send: Some(send),
            ..Output::default()
        };
        for f in frames {
            self.receive(f, partial, &mut out);
        }
        for cmd in cmds {
            match cmd {
                Cmd::Take { id } => out.effects.extend(self.take(now, id)),
                Cmd::Read => out.effects.push(Effect::Ask {
                    peers: self.asks(now),
                }),
                Cmd::Probe { id } => {
                    if self.answers_copy(now, id) {
                        out.effects.push(Effect::Open { by: self.me });
                    }
                }
                Cmd::Pick { id, asked, open } => out.effects.push(self.pick(now, id, &asked, open)),
                Cmd::DropCheck { id } => out.effects.extend(self.drop_check(now, id)),
                Cmd::Settle { id, sid } => self.settle(now, id, sid, &mut out),
                Cmd::Answer { sid } => self.answer(now, sid, &mut out),
                Cmd::Delete { id, stream, upto } => self.delete(id, stream, upto, &mut out),
                Cmd::Signal(signal) => self.signal(signal),
                Cmd::Stop => {
                    self.signal(SIGTERM);
                    let deadline = self.run + STOP_BUDGET_MS;
                    self.stop_run_ms.get_or_insert(deadline);
                    out.effects.push(Effect::Stopped);
                }
                Cmd::Drain => {
                    let mut body = self.view.own().body.clone();
                    body.drained = true;
                    node_status(self, now, &mut body);
                    self.view.set_body(body, now, run);
                    self.view.drained(self.me, now, run);
                }
                Cmd::AcceptLoss { disk } => {
                    let ok = if self.view.entry(disk).is_none() {
                        !self.raft.meta().members.contains(&disk)
                    } else if self.view.absent(disk) {
                        self.lose.insert(disk);
                        true
                    } else {
                        false
                    };
                    out.effects.push(Effect::Accepted(ok));
                }
                Cmd::Wish { set } => self.open_wish(set, &mut out),
                Cmd::Leave { remaining } => {
                    self.view.leave(remaining, now, run);
                    let restart = self.view.own().head.restart;
                    let me = self.me;
                    let own = self
                        .view
                        .answer(&[])
                        .into_iter()
                        .filter(|u| u.disk == me)
                        .collect();
                    out.effects.push(Effect::Left { restart, own });
                }
                Cmd::Departed { disk, restart, at } => {
                    self.trips.insert(disk, (restart, at));
                }
                Cmd::AnswerRequest { id } => self.answer_request(id, &mut out),
                Cmd::Guard => self.guard(&mut out),
                Cmd::Place { id } => self.place(id, &mut out),
                Cmd::Seal { reached } => self.seal(reached, &mut out),
                Cmd::Adopt {
                    id,
                    mark,
                    born,
                    writer,
                    unnamed,
                    named,
                } => {
                    self.marks.insert(id, mark);
                    self.born.insert(id, born);
                    if let Some(w) = writer {
                        self.writers.insert(id, w);
                    }
                    if let Some((epoch, old)) = unnamed {
                        self.unnamed.insert(id, (epoch, mark, old, now));
                        self.named.extend(named);
                        if old.is_none() {
                            self.catch_up(now, id);
                        }
                    }
                }
                Cmd::DropStart { id } => self.drop_start(now, id, &mut out),
                Cmd::Offer { id, epoch } => {
                    if let Some(theirs) = self.set_of(id).map(|s| s.epoch).filter(|e| *e > epoch) {
                        out.effects.push(Effect::Newer { epoch: theirs });
                    }
                }
                Cmd::Handoff {
                    id,
                    to,
                    epoch,
                    mark,
                } => self.handoff(id, to, epoch, mark, &mut out),
                Cmd::Erase { id, sender_erased } => {
                    let stream = id % STREAMS;
                    let covered = self
                        .store
                        .records
                        .values()
                        .any(|(s, upto, _)| *s == stream && id <= *upto);
                    if covered || sender_erased {
                        self.store.erased.insert(id);
                        self.store_dirty = true;
                    }
                }
                Cmd::AskRecords { peer } => self.ask_records(peer, None, &mut out),
                Cmd::Tick => {
                    self.raft.tick();
                    self.flush(&mut out);
                }
                Cmd::Ping => {
                    let head = self.view.own().head;
                    out.frames
                        .extend(self.links().into_iter().map(|b| (b, Msg::Ping(head))));
                }
                Cmd::SegAsks => self.seg_asks(&mut out),
                Cmd::Fresh => self.fresh(&mut out),
                Cmd::Beats => self.beats(&mut out),
                Cmd::IsrAsks => {
                    for (id, he, le, isr) in std::mem::take(&mut self.isr_asks) {
                        self.ask_isr(id, he, le, isr, &mut out);
                    }
                }
                Cmd::ResendCopied => self.resend_copied(&mut out),
                Cmd::ResendDrops => self.resend_drops(&mut out),
                Cmd::Govern => self.govern(&mut out),
                Cmd::Lead => self.lead_segments(&mut out),
                Cmd::Floor { to, cause } => self.propose_floor(to, cause, &mut out),
                Cmd::Stale { id, epoch } => {
                    self.stale.insert(id, epoch);
                }
                Cmd::WriteGroups => {
                    if !self.raft.is_leader() {
                        continue;
                    }
                    let started = run - self.boot_run_ms;
                    if groups_of(self).is_none()
                        && !placement::ready(self.domains.is_some(), started)
                    {
                        continue;
                    }
                    let Some(made) = self.next_groups() else {
                        continue;
                    };
                    let key = (self.raft.term(), made.epoch);
                    if self.proposed_groups != Some(key) {
                        self.proposed_groups = Some(key);
                        self.propose(&Command::Groups(made), &mut out);
                        self.flush(&mut out);
                    }
                }
            }
        }
        if std::mem::take(&mut self.store_dirty) {
            out.effects.push(Effect::Persist(self.store.clone()));
        }
        Output {
            effects: out.effects,
            frames: out.frames,
            send: None,
        }
    }

    pub(crate) fn live(&self, now: u64) -> bool {
        self.reconciled && led(self, self.me, now, HEARD_MS)
    }

    pub fn live_of(&self, now: u64, p: u64) -> bool {
        if p == self.me {
            return self.live(now);
        }
        self.view
            .entry(p)
            .is_some_and(|e| e.head.state == State::Alive && e.body.reconciled)
    }

    pub(crate) fn epoch_of(&self, id: u64) -> u64 {
        self.raft
            .meta()
            .segments
            .get(&id)
            .map_or(0, |m| m.leader_epoch)
    }

    pub(crate) fn open_epoch(&self, id: u64) -> u64 {
        self.epochs
            .get(&id)
            .copied()
            .unwrap_or(0)
            .max(self.epoch_of(id))
    }

    pub fn set_of(&self, id: u64) -> Option<&HolderSet> {
        self.raft
            .meta()
            .segments
            .get(&id)
            .map(|s| &s.holders)
            .filter(|h| h.epoch > 0)
    }

    pub fn leads(&self, now: u64, id: u64) -> bool {
        let Some(m) = self.raft.meta().segments.get(&id) else {
            return false;
        };
        let n = self.me;
        m.leader == n
            && m.isr.contains(&n)
            && self.marks.contains_key(&id)
            && self.open_epoch(id) <= m.leader_epoch
            && (self.raft.is_leader()
                || self
                    .fresh
                    .iter()
                    .any(|(v, t)| *v <= self.raft.meta().version && now < t + LEASE_MS))
            && self.lease.get(&id).is_some_and(|l| {
                l.epoch == m.leader_epoch
                    && kept(self, id, m)
                        .iter()
                        .all(|h| *h == n || l.beats.get(h).is_some_and(|t| now < t + LEASE_MS))
                    && l.since_ms.is_some_and(|t| {
                        let wait = LEASE_MS + STEP_MS;
                        now >= t + wait || (m.leader_epoch == 1 && t >= self.booted_ms + wait)
                    })
            })
    }

    pub(crate) fn serving(&self) -> bool {
        self.signals == 0 && !self.view.own().body.drained
    }

    pub(crate) fn serving_of(&self, p: u64) -> bool {
        if p == self.me {
            return self.serving();
        }
        self.view.entry(p).is_some_and(|e| {
            e.body_key.0 == e.head.restart && e.body.signals == 0 && !e.body.drained
        })
    }

    fn held(&self, now: u64) -> Vec<(u64, usize)> {
        self.marks
            .keys()
            .filter_map(|id| {
                let set = self.set_of(*id).filter(|s| s.holders.contains(&self.me))?;
                let copies = set
                    .holders
                    .iter()
                    .filter(|h| self.marked(now, **h, *id))
                    .count();
                Some((*id, copies))
            })
            .collect()
    }

    pub(crate) fn remaining(&self, now: u64) -> u32 {
        let others = self
            .view
            .live()
            .filter(|(d, _)| *d != self.me && self.serving_of(*d))
            .count();
        let floor = usize::from(self.ctl.floor.value).min(others);
        self.marks
            .keys()
            .filter(|id| {
                self.view
                    .live()
                    .filter(|(d, _)| {
                        *d != self.me && self.serving_of(*d) && self.confirmed(now, *d, **id)
                    })
                    .count()
                    < floor
            })
            .count() as u32
    }

    pub fn gossip_peers(&self) -> Vec<u64> {
        self.view
            .live()
            .map(|(d, _)| d)
            .filter(|d| *d != self.me)
            .collect()
    }

    pub(crate) fn near(&self, now: u64, peer: u64) -> bool {
        self.trips
            .get(&peer)
            .is_some_and(|(_, at)| now < at + FENCE_MS)
    }

    pub fn confirming(&self) -> bool {
        !self.confirm.borrow().is_empty()
    }

    fn names_leader(&self, id: u64) -> bool {
        self.raft
            .meta()
            .segments
            .get(&id)
            .is_some_and(|m| m.leader == self.me)
    }

    fn confirmed(&self, now: u64, p: u64, id: u64) -> bool {
        self.seg_answers
            .get(&(id, p))
            .is_some_and(|a| a.0 == Known::Copy && now < a.2 + FRESH_MS)
    }

    pub(crate) fn marked(&self, now: u64, p: u64, id: u64) -> bool {
        self.live_of(now, p)
            && if p == self.me {
                self.marks.contains_key(&id)
            } else {
                self.view
                    .entry(p)
                    .is_some_and(|e| e.body_key.0 == e.head.restart && e.body.marks.contains(&id))
            }
    }

    pub(crate) fn pick_of(&self, now: u64, id: u64) -> Option<u64> {
        let set = self.set_of(id)?;
        placement::pick(
            &[id],
            set.holders
                .iter()
                .copied()
                .filter(|h| self.marked(now, *h, id)),
        )
    }

    fn take(&self, now: u64, id: u64) -> Option<Effect> {
        (self.leads(now, id) && self.live(now)).then(|| Effect::Served {
            by: self.me,
            id,
            mark: self.marks[&id],
        })
    }

    fn asks(&self, now: u64) -> Vec<u64> {
        std::iter::once(self.me)
            .chain(self.view.live().map(|(d, _)| d).filter(|d| *d != self.me))
            .filter(|p| self.live_of(now, *p) && (*p == self.me || self.near(now, *p)))
            .collect()
    }

    fn answers_copy(&self, now: u64, id: u64) -> bool {
        !self.dropped.contains_key(&id)
            && self.marks.contains_key(&id)
            && self.live(now)
            && self.set_of(id).is_some_and(|s| {
                s.holders.contains(&self.me)
                    && !s.holders.iter().any(|h| self.unreachable.contains_key(h))
            })
    }

    fn pick(&self, now: u64, id: u64, asked: &[u64], open: Vec<u64>) -> Effect {
        let chosen = self
            .pick_of(now, id)
            .filter(|w| open.contains(w))
            .or_else(|| placement::pick(&[id], open));
        match chosen {
            Some(by) => Effect::Served { by, id, mark: 0 },
            None => Effect::Unserved {
                id,
                partial: !asked.is_empty(),
            },
        }
    }

    fn drop_check(&mut self, now: u64, id: u64) -> Option<Effect> {
        let emptied = self
            .raft
            .meta()
            .segments
            .get(&id)
            .is_some_and(|m| m.holders.epoch > 0 && m.holders.holders.is_empty());
        if emptied {
            self.drop_seen.entry(id).or_insert(now);
        }
        let seen = self.drop_seen.get(&id)?;
        (self.drop_waits.get(&id)?.is_empty() && now >= seen + HEARD_MS + LEASE_MS)
            .then_some(Effect::DropDone { id })
    }

    fn settle(&mut self, now: u64, id: u64, sid: u64, out: &mut Output) {
        let leader = self.me;
        let next = self.marks[&id] + 1;
        let Some(m) = self.raft.meta().segments.get(&id) else {
            return;
        };
        let set = kept(self, id, m);
        let scope: BTreeSet<u64> = set
            .iter()
            .copied()
            .filter(|h| *h != leader && !self.view.absent(*h))
            .collect();
        if !enough_in(&set, leader, &scope) {
            out.effects.push(Effect::Refused);
            return;
        }
        self.marks.insert(id, next);
        out.effects.push(Effect::Mark { id, mark: next });
        let epoch = self.epoch_of(id);
        let observers = m
            .holders
            .holders
            .iter()
            .filter(|h| **h != leader && !set.contains(h) && !self.view.absent(**h));
        for h in scope.iter().chain(observers) {
            out.frames.push((*h, Msg::Ack(sid, id, next, epoch)));
        }
        self.settles.insert(
            sid,
            Settle {
                epoch,
                segment: id,
                next,
                started_ms: now,
                scope,
                set,
                acked: BTreeMap::new(),
            },
        );
    }

    fn answer(&mut self, now: u64, sid: u64, out: &mut Output) {
        let leader = self.me;
        let s = &self.settles[&sid];
        let gone = now >= s.started_ms + REPLICATE_MS;
        if !gone {
            let scope: BTreeSet<u64> = s
                .scope
                .iter()
                .copied()
                .filter(|h| s.acked.contains_key(h) || !self.view.absent(*h))
                .collect();
            let open: Vec<u64> = scope
                .iter()
                .copied()
                .filter(|h| !s.acked.contains_key(h))
                .collect();
            let (segment, next, epoch) = (s.segment, s.next, s.epoch);
            let stored: BTreeSet<u64> = s
                .acked
                .iter()
                .filter(|(_, c)| **c)
                .map(|(h, _)| *h)
                .collect();
            if enough_in(&s.set, leader, &scope)
                && !open.is_empty()
                && !enough_in(&s.set, leader, &stored)
            {
                if let Some(s) = self.settles.get_mut(&sid) {
                    s.scope = scope;
                }
                for h in open {
                    out.frames.push((h, Msg::Ack(sid, segment, next, epoch)));
                }
                return;
            }
        }
        let Some(s) = self.settles.remove(&sid) else {
            return;
        };
        let stored: BTreeSet<u64> = s
            .acked
            .iter()
            .filter(|(_, c)| **c)
            .map(|(h, _)| *h)
            .collect();
        if gone
            || !self.leads(now, s.segment)
            || self.epoch_of(s.segment) != s.epoch
            || !enough_in(&s.set, leader, &stored)
        {
            out.effects.push(Effect::Refused);
        } else {
            out.effects.push(Effect::Settled {
                id: s.segment,
                next: s.next,
                started_ms: s.started_ms,
            });
        }
    }
}

impl Node {
    pub(crate) fn links(&self) -> BTreeSet<u64> {
        let mut targets: BTreeSet<u64> = self.view.digest().iter().map(|l| l.disk).collect();
        targets.extend(self.raft.meta().members.iter().copied());
        targets.remove(&self.me);
        targets
    }

    pub fn view_tick(&mut self, now: u64, run: u64) {
        let list: Vec<u64> = self.raft.meta().members.iter().copied().collect();
        self.view.tick(now, run, &list);
    }

    pub fn timers(&mut self, now: u64) {
        let start = if led(self, self.me, now, HEARD_MS) {
            now
        } else {
            0
        };
        for line in self.view.digest() {
            if line.disk == self.me {
                continue;
            }
            let restart = line.head.restart;
            let trip = self.trips.entry(line.disk).or_insert((restart, start));
            if trip.0 < restart {
                *trip = (restart, trip.1.max(now));
            }
        }
    }

    pub fn prune(&mut self, now: u64) {
        let view = &self.view;
        self.trips.retain(|d, _| view.entry(*d).is_some());
        self.votes.retain(|_, v| now < v.1 + REPLICATE_MS);
        self.peers.retain(|_, p| now < p.3 + 2 * GOSSIP_MS);
        self.records_asked.retain(|_, t| now < *t + 2 * GOSSIP_MS);
        let start = self.reconcile_ms;
        self.seg_answers
            .retain(|_, a| now < a.2 + FRESH_MS || start.is_some_and(|t| a.2 >= t));
        self.seg_asked.retain(|_, t| now < *t + STEP_MS);
        self.seen_records.retain(|_, t| now < *t + REPLICATE_MS);
        self.proposed_isr.retain(|k| k.3 >= now / REPLICATE_MS);
        self.proposed_segs.retain(|_, at| now < *at + LEASE_MS);
    }

    pub(crate) fn ask_seg(&mut self, now: u64, id: u64, peer: u64) {
        if self
            .seg_asked
            .get(&(id, peer))
            .is_none_or(|t| now >= t + STEP_MS)
        {
            self.seg_queue.entry((id, peer)).or_insert(now);
        }
    }

    pub fn catch_up(&mut self, now: u64, id: u64) {
        let Some(own) = self.set_of(id) else {
            return;
        };
        let peers: Vec<u64> = own
            .holders
            .iter()
            .copied()
            .filter(|h| *h != self.me)
            .collect();
        for p in peers {
            self.ask_seg(now, id, p);
        }
    }

    fn mark(&mut self, id: u64, mark: Option<u64>, out: &mut Output) {
        match mark {
            Some(mark) => {
                self.marks.insert(id, mark);
                out.effects.push(Effect::Mark { id, mark });
            }
            None => {
                self.marks.remove(&id);
                self.unnamed.remove(&id);
                self.writers.remove(&id);
                let keys: Vec<(u64, u64)> = self
                    .named
                    .range((id, 0)..=(id, u64::MAX))
                    .map(|(k, _)| *k)
                    .collect();
                for k in keys {
                    self.named.remove(&k);
                }
                out.effects.push(Effect::Unmark { id });
            }
        }
    }

    fn hold_epoch(&mut self, id: u64, epoch: u64, out: &mut Output) -> u64 {
        let held = self.epochs.entry(id).or_insert(0);
        *held = (*held).max(epoch);
        out.effects.push(Effect::Epoch { id, epoch: *held });
        self.open_epoch(id)
    }

    fn known(&self, now: u64, id: u64, partial: bool) -> Known {
        if self.dropped.contains_key(&id) {
            Known::Dropped
        } else if self.marks.contains_key(&id) && self.live(now) {
            Known::Copy
        } else if self.marks.contains_key(&id)
            || partial
            || self.named.contains_key(&(id, self.me))
            || self
                .set_of(id)
                .is_some_and(|s| s.holders.contains(&self.me))
        {
            Known::Partial
        } else {
            Known::Unknown
        }
    }

    fn beat(&mut self, from: u64, head: Head) {
        let (now, run) = (self.now, self.run);
        self.view.merge(
            &[Update {
                disk: from,
                head,
                body: None,
            }],
            now,
            run,
        );
    }

    fn propose(&mut self, command: &Command, out: &mut Output) {
        if self.raft.propose(command).is_err() {
            out.effects.push(Effect::RaftRefused);
        }
    }

    fn receive(&mut self, f: Inbound, partial: bool, out: &mut Output) {
        let (now, run, from) = (self.now, self.run, f.from);
        match f.msg {
            Msg::Gossip(updates) => self.view.merge(&updates, now, run),
            Msg::Ping(head) => {
                self.beat(from, head);
                out.frames
                    .push((from, Msg::Pong(f.sent, self.view.own().head)));
            }
            Msg::Pong(sent, head) => {
                self.beat(from, head);
                let trip = self.trips.entry(from).or_insert((head.restart, 0));
                *trip = (trip.0.max(head.restart), trip.1.max(sent));
            }
            Msg::Raft(message) => {
                match self.raft.step(message) {
                    Ok(()) | Err(raft::Error::StepPeerNotFound | raft::Error::ProposalDropped) => {}
                    Err(e) => panic!("the control plane refused a step: {e}"),
                }
                if self.raft.leader() == Some(from) {
                    self.heard_ms = Some(now);
                }
                self.flush(out);
            }
            Msg::Lose(disk) => {
                if self.view.absent(disk) && self.raft.meta().members.contains(&disk) {
                    self.lose.insert(disk);
                }
            }
            Msg::Ack(sid, id, next, epoch) => {
                let held = self.hold_epoch(id, epoch, out);
                if held > epoch {
                    out.frames.push((from, Msg::Acked(sid, false, held)));
                    return;
                }
                let own = self.marks.get(&id).copied();
                if let Some(mark) = own {
                    self.mark(id, Some(mark.max(next)), out);
                    out.effects.push(Effect::Replicated { id, sent: f.sent });
                }
                out.frames
                    .push((from, Msg::Acked(sid, own.is_some(), epoch)));
            }
            Msg::Wish(set) => self.propose_segment(Command::Holders(set), out),
            Msg::IsrAsk(id, he, le, isr) => self.propose_isr(from, id, he, le, isr, out),
            Msg::Fresh(version, term) => {
                if term >= self.raft.term() {
                    self.fresh.retain(|(_, t)| now < t + LEASE_MS);
                    self.fresh.push((version, f.sent));
                }
            }
            Msg::SegAsk(id, epoch) => {
                self.hold_epoch(id, epoch, out);
                let mark = self.marks.get(&id).copied();
                let known = self.known(now, id, partial);
                out.frames.push((from, Msg::Seg(id, mark, known)));
            }
            Msg::Seg(id, mark, known) => {
                if let (Some(m), Some(own)) = (mark, self.marks.get(&id).copied()) {
                    self.mark(id, Some(own.max(m)), out);
                }
                let answer = Marks::from([(0, mark.unwrap_or(0))]);
                self.seg_answers.insert((id, from), (known, answer, now));
                let booting = self.reconciled && self.booting.remove(&id);
                let dropped = self
                    .set_of(id)
                    .is_some_and(|s| !s.holders.contains(&self.me));
                if booting && dropped {
                    self.mark(id, None, out);
                }
            }
            Msg::Beat(id, epoch) => {
                if self.open_epoch(id) <= epoch && self.hold_epoch(id, epoch, out) == epoch {
                    out.frames.push((from, Msg::Beaten(id, epoch, f.sent)));
                }
            }
            Msg::Copied(id, new, mark, named) => {
                let caught = self.marks.get(&id).is_some_and(|m| {
                    let open = self.settles.values().filter(|s| s.segment == id);
                    mark >= open.map(|s| s.next - 1).min().unwrap_or(*m)
                });
                if let Some(own) = self.set_of(id).cloned()
                    && caught
                    && self.leads(now, id)
                {
                    if own.holders.contains(&new) {
                        self.join_isr(id, new);
                    } else if !named {
                        self.add_holder(&own, new, out);
                    }
                }
            }
            Msg::Beaten(id, epoch, sent) => {
                if let Some(lease) = self.lease.get_mut(&id).filter(|l| l.epoch == epoch) {
                    let beat = lease.beats.entry(from).or_insert(0);
                    *beat = (*beat).max(sent);
                    if lease.unanswered.get(&from).is_some_and(|e| *e <= sent) {
                        lease.unanswered.remove(&from);
                    }
                }
            }
            Msg::Drop(id) => {
                if self.apply_drop(id, out) {
                    let targets = self
                        .set_of(id)
                        .map(|s| s.holders.clone())
                        .unwrap_or_default()
                        .into_iter()
                        .filter(|h| *h != self.me && *h != from)
                        .collect();
                    self.send_drops(id, targets, out);
                }
                out.frames.push((from, Msg::Dropped(id)));
            }
            Msg::Dropped(id) => {
                if let Some(ts) = self.drops.get_mut(&id) {
                    ts.remove(&from);
                }
                if let Some(waits) = self.drop_waits.get_mut(&id) {
                    waits.remove(&from);
                }
            }
            Msg::Acked(sid, copy, epoch) => {
                if let Some(s) = self.settles.get_mut(&sid) {
                    s.acked.insert(from, copy && epoch == s.epoch);
                }
            }
            Msg::Named(id, holders) => {
                self.writers.insert(id, from);
                self.named
                    .extend(holders.iter().map(|c| ((id, *c), f.sent)));
            }
            Msg::VoteAsk(sent) => {
                let absent = self
                    .raft
                    .meta()
                    .members
                    .iter()
                    .copied()
                    .filter(|m| self.view.absent(*m))
                    .collect();
                out.frames.push((from, Msg::Vote(absent, sent)));
            }
            Msg::Vote(absent, sent) => {
                self.votes.insert(from, (absent, sent));
            }
            Msg::MarksIs(marks, forgot, reported) => {
                self.peers
                    .insert(from, (marks, forgot, f.version, now, reported));
            }
            Msg::Record(id, origin, rec, cut) => self.take_record(from, id, origin, rec, cut, out),
            Msg::Confirm(id, origin, by, version) => {
                if self.me != origin {
                    out.frames
                        .push((origin, Msg::Confirm(id, origin, by, version)));
                } else if let Some(r) = self.requests.get_mut(&id) {
                    r.confirmed.insert(by);
                    r.newest = r.newest.max(version);
                }
            }
            Msg::MarksAsk => {
                let marks = Msg::MarksIs(
                    self.store.marks.clone(),
                    self.store.forgot.clone(),
                    self.reported(),
                );
                out.frames.push((from, marks));
            }
            Msg::RecordsAsk(marks, after) => {
                let mut records: Vec<((u64, u64), u64, u64)> = self
                    .store
                    .records
                    .iter()
                    .filter(|(key, _)| after.is_none_or(|a| **key > a))
                    .filter(|((origin, seq), _)| *seq > marks.get(origin).copied().unwrap_or(0))
                    .map(|(key, (stream, upto, _))| (*key, *stream, *upto))
                    .take(RECORDS_MAX + 1)
                    .collect();
                let more = records.len() > RECORDS_MAX;
                records.truncate(RECORDS_MAX);
                out.frames
                    .push((from, Msg::Records(records, self.store.marks.clone(), more)));
            }
            Msg::Records(records, marks, more) => {
                let last = records.last().map(|r| r.0);
                for (key, stream, upto) in records {
                    self.apply(key, stream, upto);
                }
                self.records_asked.remove(&from);
                if more {
                    self.ask_records(from, last, out);
                } else {
                    for (origin, mark) in marks {
                        let own = self.store.marks.entry(origin).or_insert(0);
                        *own = (*own).max(mark);
                    }
                    self.store_dirty = true;
                }
            }
        }
    }

    pub(crate) fn reported(&self) -> BTreeSet<(u64, u64)> {
        let mut pairs = self.pairs.clone();
        for (disk, entry) in self.view.live() {
            pairs.extend(entry.body.unreachable.iter().map(|u| pair(disk, *u)));
        }
        pairs.retain(|(a, b)| !self.view.absent(*a) && !self.view.absent(*b));
        pairs
    }

    fn apply(&mut self, key: (u64, u64), stream: u64, upto: u64) {
        let covered: Vec<u64> = self
            .marks
            .keys()
            .copied()
            .filter(|id| id % STREAMS == stream && *id <= upto)
            .collect();
        let at = self.run;
        let s = &mut self.store;
        s.records.entry(key).or_insert((stream, upto, at));
        let mark = s.marks.entry(key.0).or_insert(0);
        while s.records.contains_key(&(key.0, *mark + 1)) {
            *mark += 1;
        }
        s.erased.extend(covered);
        self.store_dirty = true;
    }

    fn ask_records(&mut self, peer: u64, after: Option<(u64, u64)>, out: &mut Output) {
        let now = self.now;
        if self
            .records_asked
            .get(&peer)
            .is_some_and(|t| now < t + 2 * GOSSIP_MS)
        {
            return;
        }
        self.records_asked.insert(peer, now);
        out.frames
            .push((peer, Msg::RecordsAsk(self.store.marks.clone(), after)));
    }

    fn take_record(
        &mut self,
        from: u64,
        id: u64,
        origin: u64,
        rec: Rec,
        cut: BTreeSet<u64>,
        out: &mut Output,
    ) {
        let (me, now) = (self.me, self.now);
        if self.seen_records.insert(id, now).is_some() {
            return;
        }
        let peers: Vec<u64> = if me == origin {
            self.requests
                .get(&id)
                .map(|r| r.scope.iter().copied().collect())
                .unwrap_or_default()
        } else if from == origin {
            cut.iter()
                .copied()
                .filter(|p| *p != me && *p != origin)
                .collect()
        } else {
            Vec::new()
        };
        let version = self.raft.meta().version;
        match &rec {
            Rec::Delete(k, stream, upto) => self.apply(*k, *stream, *upto),
        }
        if me != origin {
            out.frames
                .push((from, Msg::Confirm(id, origin, me, version)));
        }
        out.frames.extend(
            peers
                .into_iter()
                .map(|p| (p, Msg::Record(id, origin, rec.clone(), cut.clone()))),
        );
    }

    fn delete(&mut self, id: u64, stream: u64, upto: u64, out: &mut Output) {
        if !self.live(self.now) {
            return;
        }
        self.store.sequence += 1;
        self.store_dirty = true;
        let key = (self.me, self.store.sequence);
        out.effects.push(Effect::Announced);
        let me = self.me;
        let meta = self.raft.meta();
        let mut scope: BTreeSet<u64> = meta.members.clone();
        scope.remove(&me);
        let cut: BTreeSet<u64> = self
            .unreachable
            .keys()
            .copied()
            .filter(|p| scope.contains(p))
            .collect();
        let rec = Rec::Delete(key, stream, upto);
        let newest = meta.version;
        self.requests.insert(
            id,
            Request {
                rec: rec.clone(),
                started_ms: self.now,
                scope,
                confirmed: BTreeSet::new(),
                newest,
            },
        );
        self.take_record(me, id, me, rec, cut, out);
    }

    fn has(&self, d: u64, id: u64) -> bool {
        if d == self.me {
            return self.marks.contains_key(&id);
        }
        self.view
            .entry(d)
            .is_some_and(|e| e.body_key.0 == e.head.restart && e.body.marks.contains(&id))
    }

    fn holders_of(&self, id: u64) -> Vec<u64> {
        self.view
            .live()
            .map(|(d, _)| d)
            .chain([self.me])
            .collect::<BTreeSet<u64>>()
            .into_iter()
            .filter(|d| self.has(*d, id))
            .collect()
    }

    pub fn pass(&self) -> Option<Pass<'_>> {
        let groups = groups_of(self)?;
        let live = self.view.live().map(|(d, _)| d).chain([self.me]).collect();
        let members: BTreeSet<u64> = groups.groups.iter().flatten().copied().collect();
        let want = Want {
            target: usize::from(controller::target(
                groups.size,
                members.len(),
                self.headroom,
            )),
            cold: groups.size < self.ctl.floor.value.max(self.ctl.count),
        };
        Some(Pass { groups, live, want })
    }

    pub fn plan(
        &self,
        now: u64,
        id: u64,
        pass: &Pass,
        knobs: &Knobs,
    ) -> (Option<Job>, Option<(u64, HolderSet)>) {
        let me = self.me;
        let live = |d: u64| pass.live.contains(&d);
        let holders = self.holders_of(id);
        let complete = |_: u64, d: u64| holders.contains(&d);
        let Some(set) = self.set_of(id) else {
            return (None, None);
        };
        if self.names_leader(id)
            && !knobs.open.contains(&id)
            && !pass.want.cold
            && set.holders.len() > pass.want.target
        {
            let others = set.holders.iter().filter(|h| **h != me && live(**h));
            self.confirm.borrow_mut().extend(others.map(|h| (id, *h)));
        }
        if self.stale.get(&id).is_some_and(|e| set.epoch < *e) || !self.live(now) {
            return (None, None);
        }
        let ready: Vec<u64> = set
            .holders
            .iter()
            .copied()
            .filter(|d| live(*d) && complete(id, *d) && self.live_of(now, *d))
            .collect();
        let leads = self.leads(now, id);
        let sender = if leads {
            Some(me)
        } else {
            repair::sender(set, |d| ready.contains(&d))
        };
        if sender != Some(me) && !leads {
            return (None, None);
        }
        let Some(writer) = self.writers.get(&id).copied() else {
            return (None, None);
        };
        let held = Held {
            writer,
            bytes: 0,
            sealed: !knobs.open.contains(&id),
            set,
        };
        match repair::plan(&held, pass.groups, live, complete, pass.want) {
            repair::Step::Copy { need, to } if sender == Some(me) => (
                Some(Job {
                    copies: holders.iter().filter(|h| live(**h)).count(),
                    segment: id,
                    epoch: set.epoch,
                    from: me,
                    need,
                    to,
                }),
                None,
            ),
            repair::Step::Drop(keep)
                if leads
                    && keep.iter().all(|h| live(*h))
                    && keep.iter().all(|h| *h == me || self.confirmed(now, *h, id)) =>
            {
                (None, Some((me, set.with(me, keep))))
            }
            _ => (None, None),
        }
    }

    pub fn drains(&self, run: u64, knobs: &Knobs) -> Vec<Job> {
        let me = self.me;
        let Some(next) = self.next_groups().or_else(|| groups_of(self).cloned()) else {
            return Vec::new();
        };
        let serving = |d: u64| {
            d != me
                && !self.view.absent(d)
                && self.view.entry(d).is_some_and(|e| e.body.signals == 0)
        };
        let complete = |s: u64, d: u64| self.has(d, s);
        let held: Vec<Held> = self
            .marks
            .keys()
            .filter(|id| !knobs.held.contains(*id))
            .filter_map(|id| {
                let set = self.set_of(*id).filter(|set| {
                    set.holders.contains(&me) && self.stale.get(id).is_none_or(|e| set.epoch >= *e)
                })?;
                Some(Held {
                    writer: self.writers.get(id).copied()?,
                    bytes: 0,
                    sealed: !knobs.open.contains(id),
                    set,
                })
            })
            .collect();
        let deadline = self.stop_run_ms.unwrap_or(u64::MAX);
        repair::self_drain(me, &held, &next, serving, complete, run, deadline)
            .into_iter()
            .map(|(segment, to)| Job {
                copies: self
                    .holders_of(segment)
                    .iter()
                    .filter(|h| serving(**h))
                    .count(),
                segment,
                epoch: self.set_of(segment).map_or(0, |s| s.epoch),
                from: me,
                need: to.len(),
                to,
            })
            .collect()
    }

    pub fn stop_done(&self, run: u64, knobs: &Knobs) -> bool {
        self.stop_run_ms.is_some_and(|t| run >= t)
            || (self.drains(run, knobs).is_empty() && !self.raft.meta().members.contains(&self.me))
    }

    fn ring_of(&self) -> (Vec<u64>, BTreeMap<u64, u64>) {
        let members = &self.raft.meta().members;
        let domain: BTreeMap<u64, u64> = self
            .domains
            .as_ref()
            .map(|d| d.of.clone())
            .unwrap_or_default();
        let candidates: Vec<Candidate> = self
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
                heavy: self
                    .heavy
                    .get(&d)
                    .filter(|(hold, _)| hold.heavy)
                    .map(|(_, level)| *level),
            })
            .collect();
        (placement::ring(&candidates, self.ctl.floor.value), domain)
    }

    pub fn next_groups(&self) -> Option<Groups> {
        let (ring, domain) = self.ring_of();
        placement::next(
            groups_of(self),
            &ring,
            &domain,
            &self.reported(),
            self.ctl.floor.value,
        )
    }

    fn place(&mut self, id: u64, out: &mut Output) {
        let me = self.me;
        if !self.joined || !self.raft.meta().members.contains(&me) {
            out.effects.push(Effect::RefusedWrite { resealed: false });
            return;
        }
        let late: Vec<u64> = self.unreachable.keys().copied().collect();
        let Some(groups) = groups_of(self) else {
            return;
        };
        let mut holders = groups.new_holders(me, id, &late);
        holders.retain(|h| *h == me || !self.view.absent(*h));
        out.effects.push(Effect::Picked(holders));
    }

    fn seal(&mut self, reached: usize, out: &mut Output) {
        let floor = self.ctl.floor;
        let opened = self.opened.unwrap_or(floor);
        let enough = reached >= usize::from(controller::KEYED_MIN_COPIES);
        let resealed = enough && reached < usize::from(floor.value) && opened != floor;
        self.opened = if enough && !resealed {
            Some(floor)
        } else {
            None
        };
        if !enough || resealed {
            out.effects.push(Effect::RefusedWrite { resealed });
            return;
        }
        out.effects.push(Effect::Stored {
            format: self.ctl.committed,
        });
    }

    fn guard(&mut self, out: &mut Output) {
        let (me, now) = (self.me, self.now);
        self.expire(out);
        let members = &self.raft.meta().members;
        if !self.raft.is_leader() {
            self.lose.retain(|d| members.contains(d));
        }
        if !led(self, me, now, 2 * HEARD_MS) {
            self.reconciled = false;
        }
        let live: Vec<u64> = self
            .view
            .live()
            .map(|(p, _)| p)
            .filter(|p| *p != me)
            .collect();
        let young = now < self.booted_ms + FENCE_MS;
        let fresh: BTreeSet<u64> = self
            .unreachable
            .keys()
            .chain(&live)
            .copied()
            .filter(|p| young || self.near(now, *p))
            .collect();
        unreachable(&mut self.unreachable, &live, |p| fresh.contains(&p), now);
        if !self.reconciled && led(self, me, now, HEARD_MS) {
            self.reconcile(out);
        }
        let mut body = self.view.latest().clone();
        node_status(self, now, &mut body);
        if *self.view.latest() != body {
            self.view.set_body(body, now, self.run);
        }
    }

    fn reconcile(&mut self, out: &mut Output) {
        let (me, now) = (self.me, self.now);
        let start = *self.reconcile_ms.get_or_insert(now);
        let list_members = self.raft.meta().members.clone();
        let members: Vec<u64> = self
            .view
            .live()
            .map(|(d, _)| d)
            .filter(|d| *d != me && list_members.contains(d))
            .collect();
        let stale: Vec<u64> = members
            .iter()
            .copied()
            .filter(|m| {
                self.peers
                    .get(m)
                    .is_none_or(|p| p.3 < start || now >= p.3 + GOSSIP_MS)
            })
            .collect();
        out.frames
            .extend(stale.into_iter().map(|m| (m, Msg::MarksAsk)));
        let held: Vec<u64> = self
            .marks
            .keys()
            .copied()
            .filter(|id| self.born.get(id).copied().unwrap_or(0) < start)
            .collect();
        let mut fates: Vec<(u64, Vec<Option<Known>>)> = Vec::new();
        let mut asks = Vec::new();
        let mut unsure = Vec::new();
        for id in &held {
            let own = Marks::from([(0, self.marks.get(id).copied().unwrap_or(0))]);
            let co: Vec<u64> = self
                .set_of(*id)
                .iter()
                .flat_map(|s| s.holders.iter().copied())
                .filter(|h| *h != me && !self.view.absent(*h))
                .collect();
            let mut known = Vec::new();
            if self.set_of(*id).is_some_and(|s| s.holders.is_empty()) {
                known.push(Some(Known::Dropped));
            }
            for c in co {
                let a = self
                    .seg_answers
                    .get(&(*id, c))
                    .filter(|a| a.2 >= start && reached(&own, &a.1));
                if a.is_none() {
                    unsure.push(c);
                    if self.near(now, c) {
                        asks.push((*id, c));
                    }
                }
                known.push(a.map(|a| a.0));
            }
            fates.push((*id, known));
        }
        let behind = asks
            .iter()
            .any(|k| self.named.get(k).is_none_or(|t| *t <= start));
        for (id, c) in asks {
            self.ask_seg(now, id, c);
        }
        let fresh = |t: u64| t >= start && now < t + 2 * GOSSIP_MS;
        let answer = |m: u64| self.peers.get(&m).filter(|p| fresh(p.3)).cloned();
        let own_pairs = self.reported();
        let cut = |m: u64| {
            let votes = list_members
                .iter()
                .filter(|v| {
                    let pairs = if **v == me {
                        Some(&own_pairs)
                    } else {
                        self.peers.get(v).filter(|p| fresh(p.3)).map(|p| &p.4)
                    };
                    pairs.is_some_and(|p| p.contains(&pair(me, m)))
                })
                .count();
            confirmed(votes, list_members.len())
        };
        if members
            .iter()
            .any(|m| answer(*m).is_none() && (self.near(now, *m) || !cut(*m)))
        {
            return;
        }
        let answers: Vec<(u64, PeerMarks)> = members
            .iter()
            .filter_map(|m| answer(*m).map(|a| (*m, a)))
            .collect();
        let wipe = answers
            .iter()
            .any(|(_, a)| !reached(&self.store.marks, &a.1));
        let new_disk = self.view.own().head.restart == 1 && !self.joined;
        if wipe && (!new_disk || !held.is_empty()) {
            if behind {
                return;
            }
            let kept: Vec<u64> = fates
                .iter()
                .filter(|(_, known)| {
                    let all: Vec<Known> = known.iter().flatten().copied().collect();
                    known.contains(&None) || fate(true, &all) == Fate::Last
                })
                .map(|(id, _)| *id)
                .chain(
                    self.marks
                        .keys()
                        .copied()
                        .filter(|id| self.born.get(id).copied().unwrap_or(0) >= start),
                )
                .collect();
            let kept = kept
                .into_iter()
                .map(|id| {
                    (
                        id,
                        self.marks[&id],
                        self.set_of(id).map(|s| s.epoch),
                        self.born.get(&id).copied().unwrap_or(0),
                    )
                })
                .collect();
            out.effects.push(Effect::Wipe {
                members: answers.iter().map(|(m, _)| *m).collect(),
                kept,
                erased: self.store.erased.clone(),
            });
            return;
        }
        let mut whole = !behind && unsure.iter().all(|c| cut(*c));
        for (id, known) in fates {
            if known.contains(&None) {
                continue;
            }
            let known: Vec<Known> = known.into_iter().flatten().collect();
            if fate(false, &known) == Fate::Delete {
                self.mark(id, None, out);
                if known.contains(&Known::Dropped) {
                    self.dropped.insert(id, self.run);
                    out.effects.push(Effect::Tomb { id, run: self.run });
                }
            }
        }
        for (m, a) in &answers {
            if !reached(&self.store.marks, &a.0) {
                whole = false;
                self.ask_records(*m, None, out);
            }
        }
        if whole {
            self.reconciled = true;
            self.joined = true;
            self.reconcile_ms = None;
        }
    }

    fn signal(&mut self, signal: u8) {
        self.signals |= signal;
        let mut body = self.view.own().body.clone();
        body.signals = self.signals;
        self.view.set_body(body, self.now, self.run);
    }

    fn open_wish(&mut self, set: HolderSet, out: &mut Output) {
        let (me, now) = (self.me, self.now);
        let key = (set.segment, set.epoch);
        let segment = self.raft.meta().segments.get(&key.0);
        let applied = segment.is_some_and(|s| s.holders.epoch >= key.1);
        let copied = |h: &u64| {
            self.acked.contains(&(key.0, *h))
                || self
                    .view
                    .entry(*h)
                    .is_some_and(|e| e.body.marks.contains(&key.0))
                || *h == me && self.marks.contains_key(&key.0)
        };
        let before = segment.map(|s| &s.holders.holders);
        let bare =
            key.1 > 1 && before.is_some_and(|b| set.holders.difference(b).any(|h| !copied(h)));
        if applied || bare {
            out.effects.push(Effect::WishDone { key });
            return;
        }
        if self.raft.is_leader() {
            self.propose_segment(Command::Holders(set), out);
        } else if let Some(leader) = self.raft.leader() {
            let term = self.raft.term();
            let at = self
                .proposed_segs
                .entry((term, key.0, key.1, u64::MAX))
                .or_insert(0);
            if *at == 0 || now >= *at + LEASE_MS {
                *at = now.max(1);
                out.frames.push((leader, Msg::Wish(set)));
            }
        }
    }

    fn expire(&mut self, out: &mut Output) {
        let alive = self.run;
        let gone: Vec<(u64, u64)> = self
            .store
            .records
            .iter()
            .filter(|(_, (_, _, at))| expired(*at, alive))
            .map(|(key, _)| *key)
            .collect();
        for (origin, seq) in gone {
            self.store.records.remove(&(origin, seq));
            let forgot = self.store.forgot.entry(origin).or_insert(0);
            *forgot = (*forgot).max(seq);
            self.store_dirty = true;
        }
        self.dropped.retain(|_, at| !expired(*at, alive));
        out.effects.push(Effect::Expired { alive });
    }

    fn answer_request(&mut self, id: u64, out: &mut Output) {
        let (origin, now) = (self.me, self.now);
        let Some(r) = self.requests.get(&id) else {
            out.effects.push(Effect::RequestGone { id });
            return;
        };
        if now >= r.started_ms + REPLICATE_MS {
            self.requests.remove(&id);
            out.effects.push(Effect::RequestGone { id });
            return;
        }
        let meta = self.raft.meta();
        if r.newest > meta.version {
            return;
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
                        self.view.absent(*p)
                    } else {
                        self.votes
                            .get(v)
                            .is_some_and(|vote| fresh(vote) && vote.0.contains(p))
                    }
                })
                .count();
            confirmed(yes, meta.members.len())
        });
        if !settled {
            let asked = self.vote_ms.is_some_and(|t| t >= r.started_ms);
            if !asked && now >= r.started_ms + STEP_MS {
                let voters: Vec<u64> = meta
                    .members
                    .iter()
                    .copied()
                    .filter(|v| *v != origin)
                    .collect();
                self.vote_ms = Some(now);
                out.frames
                    .extend(voters.into_iter().map(|v| (v, Msg::VoteAsk(now))));
            }
            return;
        }
        let Some(r) = self.requests.remove(&id) else {
            return;
        };
        match r.rec {
            Rec::Delete(key, stream, upto) => {
                out.effects.push(Effect::RequestDone {
                    id,
                    key,
                    stream,
                    upto,
                });
            }
        }
    }

    fn join_isr(&mut self, id: u64, new: u64) {
        if let Some(l) = self.lease.get_mut(&id) {
            l.pending.insert(new);
            l.unanswered.remove(&new);
        }
    }

    fn handoff(&mut self, id: u64, to: u64, epoch: u64, mark: u64, out: &mut Output) {
        let own = self.set_of(id).cloned();
        let (unnamed, named) = match &own {
            Some(own) => {
                let named = self
                    .named
                    .iter()
                    .filter(|((s, h), _)| *s == id && *h != to && own.holders.contains(h))
                    .map(|(k, t)| (*k, *t))
                    .collect();
                (Some((epoch, None)), named)
            }
            None => (None, Vec::new()),
        };
        out.effects.push(Effect::Copy {
            writer: self.writers.get(&id).copied(),
            unnamed,
            named,
        });
        let Some(own) = own else {
            return;
        };
        if own.holders.contains(&to) {
            return;
        }
        if self.leads(self.now, id) {
            self.add_holder(&own, to, out);
        } else if let Some(leader) = self.raft.meta().segments.get(&id).map(|m| m.leader) {
            out.frames.push((leader, Msg::Copied(id, to, mark, false)));
        }
    }

    fn add_holder(&mut self, own: &HolderSet, new: u64, out: &mut Output) {
        self.acked.insert((own.segment, new));
        let mut holders = own.holders.clone();
        holders.insert(new);
        out.effects.push(Effect::Wish {
            by: self.me,
            set: own.with(self.me, holders),
        });
    }

    fn apply_drop(&mut self, id: u64, out: &mut Output) -> bool {
        if self.dropped.contains_key(&id) {
            return false;
        }
        self.mark(id, None, out);
        self.dropped.insert(id, self.run);
        out.effects.push(Effect::Dropped { id, run: self.run });
        true
    }

    fn drop_start(&mut self, now: u64, id: u64, out: &mut Output) {
        let me = self.me;
        if !self.marked(now, me, id) || self.pick_of(now, id) != Some(me) {
            return;
        }
        let Some(set) = self.set_of(id) else {
            return;
        };
        let co: BTreeSet<u64> = set.holders.iter().copied().filter(|h| *h != me).collect();
        if co
            .iter()
            .any(|h| !self.view.absent(*h) && !self.near(now, *h))
        {
            return;
        }
        let waits = co
            .iter()
            .copied()
            .filter(|h| !self.view.absent(*h))
            .collect();
        self.apply_drop(id, out);
        self.drop_waits.insert(id, waits);
        self.send_drops(id, co, out);
        out.effects.push(Effect::Dropping);
    }

    fn send_drops(&mut self, id: u64, targets: BTreeSet<u64>, out: &mut Output) {
        let now: Vec<u64> = targets
            .iter()
            .copied()
            .filter(|t| !self.view.absent(*t))
            .collect();
        self.drops.entry(id).or_default().extend(targets);
        out.frames
            .extend(now.into_iter().map(|t| (t, Msg::Drop(id))));
    }

    fn flush(&mut self, out: &mut Output) {
        let msgs = self.raft.drain().expect("the control plane drains");
        let saved = Arc::new(self.raft.save().expect("the control plane saves"));
        let term = self.raft.is_leader().then(|| self.raft.term());
        let old = std::mem::replace(&mut self.saved, saved.clone());
        out.effects.push(Effect::Saved(saved));
        self.applied(&old, out);
        if let Some(term) = term {
            out.effects.push(Effect::Led { term });
        }
        for m in msgs {
            let to = m.to;
            let snapshot = m.get_msg_type() == MessageType::MsgSnapshot;
            let version = self.raft.meta().version;
            let ok = match out.send.as_mut() {
                Some(send) => send(to, Msg::Raft(m), version),
                None => {
                    out.frames.push((to, Msg::Raft(m)));
                    true
                }
            };
            if snapshot {
                self.raft.report_snapshot(to, ok);
                self.flush(out);
            }
        }
    }

    fn applied(&mut self, old: &Saved, out: &mut Output) {
        let new = self.saved.clone();
        if new.meta.floor != old.meta.floor {
            self.ctl.floor = new.meta.floor;
            self.proposed = None;
        }
        if new.meta.version == old.meta.version {
            return;
        }
        let me = self.me;
        let released: Vec<u64> = new
            .meta
            .segments
            .values()
            .filter(|s| {
                old.meta.segments.get(&s.holders.segment).is_some_and(|b| {
                    b.holders != s.holders
                        && b.holders.holders.contains(&me)
                        && !s.holders.holders.contains(&me)
                })
            })
            .map(|s| s.holders.segment)
            .collect();
        for id in released {
            if self.reconciled {
                self.mark(id, None, out);
            } else if self.marks.contains_key(&id) {
                self.booting.insert(id);
            }
        }
        let gone: Vec<u64> = old
            .meta
            .members
            .difference(&new.meta.members)
            .copied()
            .collect();
        self.adopted_ms = self.now;
        if !self.raft.is_leader() {
            return;
        }
        let lost = gone.iter().any(|d| self.lose.contains(d));
        self.lose.retain(|d| !gone.contains(d));
        let drained = gone.iter().any(|d| {
            self.view
                .entry(*d)
                .is_some_and(|e| e.body.drained && e.body_key.0 == e.head.restart)
        });
        if lost {
            self.fall = Some(Cause::AcceptLoss);
        } else if drained {
            self.fall = Some(Cause::Drain);
            out.effects.push(Effect::DrainCommit);
        }
    }

    fn propose_floor(&mut self, to: Floor, cause: Cause, out: &mut Output) {
        let key = (self.raft.term(), to);
        if self.proposed == Some(key) {
            return;
        }
        self.proposed = Some(key);
        let from = self.raft.meta().floor;
        self.propose(&Command::Floor { from, to }, out);
        out.effects.push(Effect::Decided { from, to, cause });
        self.flush(out);
    }

    fn fall(&mut self, out: &mut Output) {
        let Some(cause) = self.fall else {
            return;
        };
        if !self.raft.is_leader() {
            self.fall = None;
            return;
        }
        match self.ctl.lower() {
            Some(to) => {
                self.ctl.floor = self.raft.meta().floor;
                self.propose_floor(to, cause, out);
            }
            None => self.fall = None,
        }
    }

    fn change(&mut self, kind: ConfChangeType, member: u64, out: &mut Output) {
        if self.raft.propose_conf_change(kind, member).is_err() {
            out.effects.push(Effect::RaftRefused);
        }
        self.flush(out);
    }

    fn propose_segment(&mut self, command: Command, out: &mut Output) {
        let (segment, epoch, leader) = match &command {
            Command::Holders(h) => (h.segment, h.epoch, 0),
            Command::Leader {
                segment,
                epoch,
                disk,
            } => (*segment, *epoch, *disk + 1),
            _ => return,
        };
        if !self.raft.is_leader() {
            return;
        }
        let now = self.now;
        let at = self
            .proposed_segs
            .entry((self.raft.term(), segment, epoch, leader))
            .or_insert(0);
        if *at != 0 && now < *at + LEASE_MS {
            return;
        }
        *at = now.max(1);
        self.propose(&command, out);
        self.flush(out);
        if let Command::Holders(h) = command
            && h.epoch == 1
        {
            self.propose_segment(
                Command::Leader {
                    segment,
                    epoch: 1,
                    disk: h.writer,
                },
                out,
            );
        }
    }

    fn propose_isr(
        &mut self,
        from: u64,
        id: u64,
        he: u64,
        le: u64,
        isr: BTreeSet<u64>,
        out: &mut Output,
    ) {
        if !self.raft.is_leader() {
            return;
        }
        let Some(m) = self.raft.meta().segments.get(&id) else {
            return;
        };
        if m.leader != from
            || m.holders.epoch != he
            || m.leader_epoch != le
            || !isr.is_subset(&m.holders.holders)
            || isr == m.isr
        {
            return;
        }
        let key = (
            self.raft.term(),
            id,
            he,
            self.now / REPLICATE_MS,
            isr.iter().copied().collect(),
            m.isr.iter().copied().collect(),
        );
        if !self.proposed_isr.insert(key) {
            return;
        }
        let command = Command::Isr {
            segment: id,
            holder_epoch: he,
            leader_epoch: le,
            isr,
        };
        self.propose(&command, out);
        self.flush(out);
    }

    fn ask_isr(&mut self, id: u64, he: u64, le: u64, isr: BTreeSet<u64>, out: &mut Output) {
        if self.raft.is_leader() {
            self.propose_isr(self.me, id, he, le, isr, out);
        } else if let Some(leader) = self.raft.leader() {
            out.frames.push((leader, Msg::IsrAsk(id, he, le, isr)));
        }
    }

    fn fresh(&self, out: &mut Output) {
        if !self.raft.is_leader() {
            return;
        }
        let (version, term) = (self.raft.meta().version, self.raft.term());
        let peers: BTreeSet<u64> = self
            .view
            .known()
            .chain(self.raft.meta().members.iter().copied())
            .filter(|p| *p != self.me)
            .collect();
        out.frames
            .extend(peers.into_iter().map(|p| (p, Msg::Fresh(version, term))));
    }

    fn beats(&mut self, out: &mut Output) {
        let (me, now) = (self.me, self.now);
        let due: Vec<(u64, u64, HolderSet, BTreeSet<u64>)> = self
            .raft
            .meta()
            .segments
            .iter()
            .filter(|(_, m)| m.leader == me && m.isr.contains(&me))
            .map(|(id, m)| (*id, m.leader_epoch, m.holders.clone(), m.isr.clone()))
            .collect();
        for (id, epoch, holders, isr) in due {
            let lease = self.lease.entry(id).or_default();
            if lease.epoch != epoch {
                *lease = Lease {
                    epoch,
                    ..Lease::default()
                };
            }
            lease.since_ms.get_or_insert(now);
            lease.pending.retain(|h| holders.holders.contains(h));
            let mut set = isr.clone();
            set.extend(&lease.pending);
            let silent: BTreeSet<u64> = set
                .iter()
                .copied()
                .filter(|h| {
                    lease
                        .unanswered
                        .get(h)
                        .is_some_and(|sent| now >= sent + ISR_MS)
                })
                .collect();
            lease.unanswered.retain(|h, _| set.contains(h));
            lease.pending.retain(|h| !silent.contains(h));
            let targets = set.clone();
            set.retain(|h| !silent.contains(h));
            if set != isr {
                self.isr_asks.push((id, holders.epoch, epoch, set));
            }
            for h in targets.into_iter().filter(|h| *h != me) {
                lease.unanswered.entry(h).or_insert(now);
                out.frames.push((h, Msg::Beat(id, epoch)));
            }
        }
    }

    fn seg_asks(&mut self, out: &mut Output) {
        let now = self.now;
        let mut wanted = std::mem::take(self.confirm.get_mut());
        if self.view.own().body.drained {
            let peers: Vec<u64> = self
                .view
                .live()
                .map(|(d, _)| d)
                .filter(|d| *d != self.me && self.serving_of(*d))
                .collect();
            for id in self.marks.keys() {
                wanted.extend(peers.iter().map(|p| (*id, *p)));
            }
        }
        for (id, p) in wanted {
            self.ask_seg(now, id, p);
        }
        let (mut due, far): (Vec<Queued>, Vec<Queued>) = self
            .seg_queue
            .iter()
            .map(|(k, t)| (*k, *t))
            .partition(|((_, p), _)| self.near(now, *p));
        due.sort_by_key(|(k, t)| (*t, *k));
        let due: Vec<(u64, u64)> = due.into_iter().take(SEG_ASKS_MAX).map(|(k, _)| k).collect();
        for (key, _) in far {
            self.seg_queue.remove(&key);
        }
        for key in due {
            self.seg_queue.remove(&key);
            self.seg_asked.insert(key, now);
            let epoch = self.epoch_of(key.0);
            out.frames.push((key.1, Msg::SegAsk(key.0, epoch)));
        }
    }

    fn resend_copied(&mut self, out: &mut Output) {
        let (me, now) = (self.me, self.now);
        let meta = self.raft.meta();
        let mut sends = Vec::new();
        let mut wishes = Vec::new();
        let mut done = Vec::new();
        let mut catch = Vec::new();
        let mut orphans = Vec::new();
        for (id, m) in &meta.segments {
            if m.holders.holders.contains(&me)
                && !m.isr.contains(&me)
                && m.leader != me
                && let Some(mark) = self.marks.get(id)
            {
                sends.push((m.leader, *id, *mark, true));
                catch.push((*id, m.leader));
            }
        }
        if self.reconciled {
            for id in &self.booting {
                for h in meta
                    .segments
                    .get(id)
                    .iter()
                    .flat_map(|m| &m.holders.holders)
                {
                    orphans.push((*id, *h));
                }
            }
        }
        for (id, (epoch, mark, old, at)) in &self.unnamed {
            let Some(m) = meta.segments.get(id) else {
                continue;
            };
            if m.holders.holders.contains(&me) {
                done.push((*id, false));
            } else if old.is_none() && m.holders.epoch > *epoch && now >= at + 10 * ISR_MS {
                done.push((*id, true));
            } else if !meta.members.contains(&me) {
                continue;
            } else if let Some(old) = old {
                let mut holders = m.holders.holders.clone();
                holders.remove(old);
                holders.insert(me);
                wishes.push(m.holders.with(me, holders));
            } else {
                sends.push((m.leader, *id, *mark, false));
            }
        }
        if meta.members.contains(&me) {
            for id in self.marks.keys() {
                if self.set_of(*id).is_some() {
                    continue;
                }
                let Some(writer) = self
                    .writers
                    .get(id)
                    .copied()
                    .filter(|w| *w != me && self.view.absent(*w))
                else {
                    continue;
                };
                let holders: BTreeSet<u64> = self
                    .named
                    .range((*id, 0)..=(*id, u64::MAX))
                    .map(|((_, h), _)| *h)
                    .collect();
                if holders.contains(&me) {
                    out.effects.push(Effect::Wish {
                        by: me,
                        set: HolderSet {
                            segment: *id,
                            epoch: 1,
                            writer,
                            holders,
                        },
                    });
                }
            }
        }
        for (id, remove) in done {
            self.unnamed.remove(&id);
            if remove {
                self.mark(id, None, out);
            }
        }
        for set in wishes {
            out.effects.push(Effect::Wish {
                by: set.writer,
                set,
            });
        }
        if now.is_multiple_of(GOSSIP_MS) {
            out.frames.extend(
                sends
                    .into_iter()
                    .map(|(leader, id, mark, named)| (leader, Msg::Copied(id, me, mark, named))),
            );
            for (id, leader) in catch.into_iter().chain(orphans) {
                self.ask_seg(now, id, leader);
            }
        }
    }

    fn resend_drops(&mut self, out: &mut Output) {
        let me = self.me;
        let dropped = &self.dropped;
        self.drop_epoch.retain(|id, _| dropped.contains_key(id));
        for id in dropped.keys() {
            let Some(set) = self.raft.meta().segments.get(id).map(|m| &m.holders) else {
                continue;
            };
            if !set.holders.is_empty() {
                out.effects.push(Effect::Wish {
                    by: me,
                    set: HolderSet {
                        segment: *id,
                        epoch: set.epoch + 1,
                        writer: set.writer,
                        holders: BTreeSet::new(),
                    },
                });
            }
            if set.epoch > self.drop_epoch.get(id).copied().unwrap_or(0) {
                self.drop_epoch.insert(*id, set.epoch);
                self.drops
                    .entry(*id)
                    .or_default()
                    .extend(set.holders.iter().copied().filter(|h| *h != me));
            }
        }
        self.drops.retain(|id, _| dropped.contains_key(id));
        let members = &self.raft.meta().members;
        for targets in self.drops.values_mut() {
            targets.retain(|t| members.contains(t));
        }
        self.drops.retain(|_, targets| !targets.is_empty());
        let view = &self.view;
        out.frames.extend(
            self.drops
                .iter()
                .flat_map(|(id, ts)| ts.iter().map(move |t| (*t, *id)))
                .filter(|(t, _)| !view.absent(*t))
                .take(DROPS_MAX)
                .map(|(t, id)| (t, Msg::Drop(id))),
        );
    }

    fn lead_segments(&mut self, out: &mut Output) {
        if !self.raft.is_leader() {
            return;
        }
        let now = self.now;
        let ready = |id: u64, h: u64| self.marked(now, h, id);
        let usable = |id: u64, h: u64| self.serving_of(h) && ready(id, h);
        let picks: Vec<Command> = self
            .raft
            .meta()
            .segments
            .iter()
            .filter(|(id, s)| {
                s.holders.epoch > 0 && !(s.isr.contains(&s.leader) && usable(**id, s.leader))
            })
            .filter_map(|(id, s)| {
                let set = s.holders.with(0, s.isr.clone());
                let none = BTreeSet::new();
                let leader = placement::leader(0, &set, |h| usable(*id, h), &none)
                    .or_else(|| placement::leader(0, &set, |h| ready(*id, h), &none))
                    .filter(|l| *l != s.leader)?;
                Some(Command::Leader {
                    segment: *id,
                    epoch: s.leader_epoch + 1,
                    disk: leader,
                })
            })
            .collect();
        for command in picks {
            self.propose_segment(command, out);
        }
    }

    fn govern(&mut self, out: &mut Output) {
        let (me, now, run) = (self.me, self.now, self.run);
        if self.raft.is_leader() {
            let members = &self.raft.meta().members;
            let removed: Vec<u64> = self
                .lose
                .iter()
                .copied()
                .filter(|d| !members.contains(d))
                .collect();
            self.lose.retain(|d| !removed.contains(d));
            if !removed.is_empty() {
                self.fall = Some(Cause::AcceptLoss);
            }
        }
        self.fall(out);
        if !self.raft.is_leader() {
            if let Some(leader) = self.raft.leader() {
                out.frames
                    .extend(self.lose.iter().map(|d| (leader, Msg::Lose(*d))));
            }
            return;
        }
        let (view, meta) = (&self.view, self.raft.meta());
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
            !stop && !drained && (self.raft.learners().contains(&d) || !meta.members.contains(&d))
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
                    .then(|| Command::Holders(s.holders.with(me, kept)))
            })
            .collect();
        let needed = |m: u64| {
            meta.segments.iter().any(|(id, s)| {
                s.isr.contains(&m)
                    && !s
                        .isr
                        .iter()
                        .any(|h| *h != m && meta.members.contains(h) && self.marked(now, *h, *id))
            })
        };
        let remove = meta.members.iter().copied().find(|m| {
            let (stop, drained, remaining) = flags(*m);
            if view.absent(*m) {
                view.entry(*m).is_some()
                    && (stop && !needed(*m)
                        || drained && !needed(*m)
                        || self.lose.contains(m)
                        || waiting && view.absent_since(*m).is_some_and(|t| run >= t + REPLACE_MS)
                        || view
                            .absent_since(*m)
                            .is_some_and(|t| run >= t + TOMBSTONE_HORIZON))
            } else {
                (stop || drained && remaining == 0) && !needed(*m)
            }
        });
        let promote = self.raft.learners().iter().copied().find(|l| {
            let (stop, drained, _) = flags(*l);
            !view.absent(*l)
                && !stop
                && !drained
                && view.entry(*l).is_some_and(|e| e.body.epoch == meta.shape)
        });
        let add = view.live().map(|(d, _)| d).find(|d| {
            let (stop, drained, _) = flags(*d);
            !meta.members.contains(d) && !self.raft.learners().contains(d) && !stop && !drained
        });
        let change = remove
            .map(|d| (ConfChangeType::RemoveNode, d))
            .or(promote.map(|d| (ConfChangeType::AddNode, d)))
            .or(add.map(|d| (ConfChangeType::AddLearnerNode, d)))
            .filter(|_| !self.raft.pending_conf());
        if let Some((kind, member)) = change {
            self.change(kind, member, out);
        }
        for command in drops {
            self.propose_segment(command, out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use verdandi_core::cluster::view::TOMBSTONE_HORIZON;

    fn marks(of: &[(u64, u64)]) -> Marks {
        of.iter().copied().collect()
    }

    #[test]
    fn marks_reach_an_answer_only_at_or_above_each_of_its_origins() {
        assert!(reached(&marks(&[(1, 5), (2, 3)]), &marks(&[(1, 5)])));
        assert!(!reached(&marks(&[(1, 4)]), &marks(&[(1, 5)])));
        assert!(!reached(&marks(&[(1, 9)]), &marks(&[(2, 1)])));
        assert!(reached(&Marks::new(), &marks(&[(2, 0)])));
    }

    #[test]
    fn an_unreachable_peer_leaves_after_join_ms_of_round_trips() {
        let mut list = BTreeMap::from([(5, None)]);
        unreachable(&mut list, &[5], |_| false, 0);
        assert_eq!(list, BTreeMap::from([(5, None)]));
        unreachable(&mut list, &[5], |_| true, 1_000);
        unreachable(&mut list, &[5], |_| true, 1_000 + JOIN_MS - 1);
        assert!(list.contains_key(&5));
        unreachable(&mut list, &[5], |_| false, 1_000 + JOIN_MS - 1);
        unreachable(&mut list, &[5], |_| true, 20_000);
        unreachable(&mut list, &[5], |_| true, 20_000 + JOIN_MS - 1);
        assert!(list.contains_key(&5));
        unreachable(&mut list, &[5], |_| true, 20_000 + JOIN_MS);
        assert!(list.is_empty());
        let mut list = BTreeMap::from([(5, None), (6, None)]);
        unreachable(&mut list, &[6], |_| true, 0);
        assert_eq!(list, BTreeMap::from([(6, Some(0))]));
        let mut list = BTreeMap::new();
        unreachable(&mut list, &[7, 8], |p| p == 8, 0);
        assert_eq!(list, BTreeMap::from([(7, None)]));
    }

    #[test]
    fn a_tombstone_expires_at_the_horizon_of_the_keeping_node_run_time() {
        assert!(!expired(100, 100 + TOMBSTONE_HORIZON - 1));
        assert!(expired(100, 100 + TOMBSTONE_HORIZON));
    }

    #[test]
    fn a_held_segment_is_deleted_on_a_drop_or_when_every_co_holder_forgot_it() {
        use Known::*;
        assert_eq!(fate(false, &[Copy, Dropped]), Fate::Delete);
        assert_eq!(fate(true, &[Partial, Dropped]), Fate::Delete);
        assert_eq!(fate(false, &[Unknown, Unknown]), Fate::Delete);
        assert_eq!(fate(true, &[Unknown]), Fate::Delete);
        assert_eq!(fate(false, &[Unknown, Partial]), Fate::Keep);
        assert_eq!(fate(false, &[]), Fate::Keep);
        assert_eq!(fate(true, &[Copy, Partial]), Fate::Delete);
        assert_eq!(fate(true, &[Partial, Unknown]), Fate::Last);
        assert_eq!(fate(true, &[]), Fate::Last);
    }

    #[test]
    fn the_no_holder_rule_fires_only_without_a_live_complete_copy() {
        let holders = BTreeSet::from([1, 2, 3]);
        assert!(!no_holder(1, &holders, |_| true, |h| h == 2));
        assert!(no_holder(1, &holders, |h| h != 2, |h| h == 2));
        assert!(!no_holder(1, &holders, |_| true, |h| h == 1));
        assert!(no_holder(4, &holders, |_| true, |_| false));
    }

    fn node() -> Node {
        let saved = Saved::default();
        Node::new(
            1,
            0,
            0,
            1,
            0,
            View::new(1, 1, Body::default(), 0, 0),
            Controller::new(true, controller::START, 1),
            Control::restore(1, saved.clone()).expect("the empty state restores"),
            Arc::new(saved),
            Store::default(),
        )
    }

    fn replay(node: &mut Node) -> String {
        let mut sent = Vec::new();
        let mut log = String::new();
        for step in 0..40 {
            let now = step * STEP_MS;
            let mut input = Input::cmd(now, now, Cmd::Signal(SIGTERM));
            if step % 2 == 0 {
                input.cmds.clear();
            }
            let out = node.step(input, &mut |to, msg, at| {
                sent.push((to, format!("{msg:?}"), at));
                true
            });
            log += &format!("{:?}{:?}", out.effects, out.frames);
        }
        format!("{log}{sent:?}")
    }

    #[test]
    fn the_same_inputs_on_the_same_state_give_the_same_output() {
        let (a, b) = (replay(&mut node()), replay(&mut node()));
        assert!(a.len() > 100);
        assert_eq!(a, b);
    }
}
