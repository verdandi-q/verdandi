use std::collections::BTreeMap;
use std::collections::BTreeSet;

use raft::eraftpb::{
    ConfChange, ConfChangeType, ConfState, Entry, EntryType, HardState, Message, Snapshot,
};
use raft::util::limit_size;
use raft::{Config, GetEntriesContext, RaftState, RawNode, StateRole, Storage, StorageError};

use verdandi_core::cluster::controller::{Floor, START};
use verdandi_core::cluster::placement::{Groups, HolderSet};
use verdandi_core::cluster::view::take;

const ELECTION_TICK: usize = 4;
const HEARTBEAT_TICK: usize = 1;
const SNAPSHOT_EVERY: u64 = 1_000;
const MAX_MESSAGE_BYTES: u64 = 1 << 20;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Groups(Groups),
    Holders(HolderSet),
    Leader { segment: u64, epoch: u64, disk: u64 },
    Floor(Floor),
}

fn put(out: &mut Vec<u8>, words: impl IntoIterator<Item = u64>) {
    for word in words {
        out.extend_from_slice(&word.to_le_bytes());
    }
}

fn put_list(out: &mut Vec<u8>, words: impl ExactSizeIterator<Item = u64>) {
    out.extend_from_slice(&(words.len() as u32).to_le_bytes());
    put(out, words);
}

fn words<const N: usize>(rest: &mut &[u8]) -> Option<[u64; N]> {
    let mut out = [0; N];
    for word in &mut out {
        *word = u64::from_le_bytes(take(rest)?);
    }
    Some(out)
}

fn list(rest: &mut &[u8]) -> Option<Vec<u64>> {
    let mut out = Vec::new();
    for _ in 0..u32::from_le_bytes(take(rest)?) {
        out.push(u64::from_le_bytes(take(rest)?));
    }
    Some(out)
}

impl Command {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            Command::Groups(g) => {
                out.push(0);
                put(&mut out, [g.epoch]);
                out.push(g.size);
                out.extend_from_slice(&(g.groups.len() as u32).to_le_bytes());
                for group in &g.groups {
                    put_list(&mut out, group.iter().copied());
                }
                out.extend_from_slice(&(g.stand_ins.len() as u32).to_le_bytes());
                for (index, disk) in &g.stand_ins {
                    put(&mut out, [*index as u64, *disk]);
                }
            }
            Command::Holders(h) => {
                out.push(1);
                put(&mut out, [h.segment, h.epoch, h.writer]);
                put_list(&mut out, h.holders.iter().copied());
            }
            Command::Leader {
                segment,
                epoch,
                disk,
            } => {
                out.push(2);
                put(&mut out, [*segment, *epoch, *disk]);
            }
            Command::Floor(f) => {
                out.push(3);
                put(&mut out, [f.epoch]);
                out.push(f.value);
            }
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Command> {
        let mut slice = bytes;
        let rest = &mut slice;
        let command = match take::<1>(rest)?[0] {
            0 => {
                let [epoch] = words(rest)?;
                let size = take::<1>(rest)?[0];
                let mut groups = Vec::new();
                for _ in 0..u32::from_le_bytes(take(rest)?) {
                    groups.push(list(rest)?);
                }
                let mut stand_ins = Vec::new();
                for _ in 0..u32::from_le_bytes(take(rest)?) {
                    let [index, disk] = words(rest)?;
                    stand_ins.push((usize::try_from(index).ok()?, disk));
                }
                Command::Groups(Groups {
                    epoch,
                    size,
                    groups,
                    stand_ins,
                })
            }
            1 => {
                let [segment, epoch, writer] = words(rest)?;
                let holders = list(rest)?;
                if !holders.is_sorted_by(|a, b| a < b) {
                    return None;
                }
                Command::Holders(HolderSet {
                    segment,
                    epoch,
                    writer,
                    holders: holders.into_iter().collect(),
                })
            }
            2 => {
                let [segment, epoch, disk] = words(rest)?;
                Command::Leader {
                    segment,
                    epoch,
                    disk,
                }
            }
            3 => {
                let [epoch] = words(rest)?;
                Command::Floor(Floor {
                    epoch,
                    value: take::<1>(rest)?[0],
                })
            }
            _ => return None,
        };
        rest.is_empty().then_some(command)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Segment {
    pub holders: HolderSet,
    pub leader_epoch: u64,
    pub leader: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Meta {
    pub members: BTreeSet<u64>,
    pub groups: Groups,
    pub segments: BTreeMap<u64, Segment>,
    pub floor: Floor,
    pub version: u64,
}

impl Default for Meta {
    fn default() -> Self {
        Meta {
            members: BTreeSet::new(),
            groups: Groups {
                epoch: 0,
                size: 0,
                groups: Vec::new(),
                stand_ins: Vec::new(),
            },
            segments: BTreeMap::new(),
            floor: START,
            version: 0,
        }
    }
}

impl Meta {
    fn apply(&mut self, command: Command) {
        match command {
            Command::Groups(g) if g.epoch == self.groups.epoch + 1 => self.groups = g,
            Command::Floor(f) if f > self.floor && f.epoch <= self.floor.epoch + 1 => {
                self.floor = f
            }
            Command::Holders(h) => {
                let current = self.segments.get(&h.segment).map_or(0, |s| s.holders.epoch);
                if h.epoch != current + 1 {
                    return;
                }
                let id = h.segment;
                self.segment(id).holders = h;
            }
            Command::Leader {
                segment,
                epoch,
                disk,
            } => {
                let current = self.segments.get(&segment).map_or(0, |s| s.leader_epoch);
                if epoch != current + 1 {
                    return;
                }
                let segment = self.segment(segment);
                segment.leader_epoch = epoch;
                segment.leader = disk;
            }
            _ => return,
        }
        self.version += 1;
    }

    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put(&mut out, [self.version]);
        put_list(&mut out, self.members.iter().copied());
        let mut commands = vec![
            Command::Groups(self.groups.clone()),
            Command::Floor(self.floor),
        ];
        for segment in self.segments.values() {
            commands.push(Command::Holders(segment.holders.clone()));
            commands.push(Command::Leader {
                segment: segment.holders.segment,
                epoch: segment.leader_epoch,
                disk: segment.leader,
            });
        }
        for command in commands {
            let bytes = command.encode();
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(&bytes);
        }
        out
    }

    fn decode(bytes: &[u8]) -> Option<Meta> {
        let mut rest = bytes;
        let [version] = words(&mut rest)?;
        let mut meta = Meta {
            members: list(&mut rest)?.into_iter().collect(),
            version,
            ..Meta::default()
        };
        while !rest.is_empty() {
            let len = u32::from_le_bytes(take(&mut rest)?) as usize;
            let (frame, tail) = rest.split_at_checked(len)?;
            rest = tail;
            match Command::decode(frame)? {
                Command::Groups(g) => meta.groups = g,
                Command::Floor(f) => meta.floor = f,
                Command::Holders(h) => {
                    let id = h.segment;
                    meta.segment(id).holders = h;
                }
                Command::Leader {
                    segment,
                    epoch,
                    disk,
                } => {
                    let segment = meta.segment(segment);
                    segment.leader_epoch = epoch;
                    segment.leader = disk;
                }
            }
        }
        Some(meta)
    }

    fn segment(&mut self, id: u64) -> &mut Segment {
        self.segments.entry(id).or_insert_with(|| Segment {
            holders: HolderSet {
                segment: id,
                epoch: 0,
                writer: 0,
                holders: BTreeSet::new(),
            },
            leader_epoch: 0,
            leader: 0,
        })
    }
}

#[derive(Clone, Default)]
pub struct Saved {
    pub hard_state: HardState,
    pub snapshot: Snapshot,
    pub entries: Vec<Entry>,
    pub conf: ConfState,
    pub applied: u64,
    pub meta: Meta,
}

struct Store {
    hard_state: HardState,
    conf: ConfState,
    snapshot: Snapshot,
    entries: Vec<Entry>,
}

impl Store {
    fn snapshot_index(&self) -> u64 {
        self.snapshot.get_metadata().index
    }

    fn last(&self) -> u64 {
        self.entries
            .last()
            .map_or(self.snapshot_index(), |e| e.index)
    }

    fn append(&mut self, entries: &[Entry]) -> raft::Result<()> {
        let Some(first) = entries.first() else {
            return Ok(());
        };
        let at = first
            .index
            .checked_sub(self.snapshot_index() + 1)
            .filter(|at| *at as usize <= self.entries.len())
            .ok_or(raft::Error::Store(StorageError::Unavailable))?;
        self.entries.truncate(at as usize);
        self.entries.extend_from_slice(entries);
        Ok(())
    }

    fn compact(&mut self, snapshot: Snapshot) {
        let index = snapshot.get_metadata().index;
        self.entries.retain(|e| e.index > index);
        self.snapshot = snapshot;
    }

    fn install(&mut self, snapshot: Snapshot) {
        let meta = snapshot.get_metadata();
        self.hard_state.commit = meta.index;
        self.hard_state.term = self.hard_state.term.max(meta.term);
        self.conf = meta.get_conf_state().clone();
        self.entries.clear();
        self.snapshot = snapshot;
    }
}

impl Storage for Store {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(RaftState::new(self.hard_state.clone(), self.conf.clone()))
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        let offset = self.snapshot_index() + 1;
        if low < offset {
            return Err(raft::Error::Store(StorageError::Compacted));
        }
        if high > self.last() + 1 {
            return Err(raft::Error::Store(StorageError::Unavailable));
        }
        let mut out = self.entries[(low - offset) as usize..(high - offset) as usize].to_vec();
        limit_size(&mut out, max_size.into());
        Ok(out)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        let offset = self.snapshot_index() + 1;
        if idx + 1 == offset {
            return Ok(self.snapshot.get_metadata().term);
        }
        if idx < offset {
            return Err(raft::Error::Store(StorageError::Compacted));
        }
        self.entries
            .get((idx - offset) as usize)
            .map(|e| e.term)
            .ok_or(raft::Error::Store(StorageError::Unavailable))
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.snapshot_index() + 1)
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.last())
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        if self.snapshot_index() == 0 || self.snapshot_index() < request_index {
            return Err(raft::Error::Store(
                StorageError::SnapshotTemporarilyUnavailable,
            ));
        }
        Ok(self.snapshot.clone())
    }
}

pub struct Control {
    node: RawNode<Store>,
    meta: Meta,
    conf: ConfState,
    applied: u64,
}

fn conf_change(kind: ConfChangeType, node: u64) -> (ConfChange, Vec<u8>) {
    let mut change = ConfChange::default();
    change.set_change_type(kind);
    change.node_id = node;
    let mut context = vec![kind as u8];
    put(&mut context, [node]);
    (change, context)
}

fn decode_conf_change(context: &[u8]) -> Option<ConfChange> {
    let mut rest = context;
    let kind = match take::<1>(&mut rest)?[0] {
        0 => ConfChangeType::AddNode,
        1 => ConfChangeType::RemoveNode,
        2 => ConfChangeType::AddLearnerNode,
        _ => return None,
    };
    let [node] = words(&mut rest)?;
    rest.is_empty().then(|| conf_change(kind, node).0)
}

impl Control {
    pub fn found(id: u64) -> raft::Result<Control> {
        let (_, context) = conf_change(ConfChangeType::AddNode, id);
        let mut entry = Entry::default();
        entry.set_entry_type(EntryType::EntryConfChange);
        entry.term = 1;
        entry.index = 1;
        entry.context = context.into();
        let hard_state = HardState {
            term: 1,
            commit: 1,
            ..HardState::default()
        };
        Control::restore(
            id,
            Saved {
                hard_state,
                snapshot: Snapshot::default(),
                entries: vec![entry],
                conf: ConfState::from((vec![id], Vec::new())),
                applied: 1,
                meta: Meta {
                    members: BTreeSet::from([id]),
                    version: 1,
                    ..Meta::default()
                },
            },
        )
    }

    pub fn restore(id: u64, saved: Saved) -> raft::Result<Control> {
        let base = saved.snapshot.get_metadata().index;
        let last = saved.entries.last().map_or(base, |e| e.index);
        let contiguous = saved
            .entries
            .iter()
            .zip(base + 1..)
            .all(|(e, i)| e.index == i);
        if !contiguous
            || saved.hard_state.commit > last
            || saved.applied > saved.hard_state.commit
            || saved.applied < base
        {
            return Err(raft::Error::ConfigInvalid("corrupt saved state".into()));
        }
        let store = Store {
            hard_state: saved.hard_state,
            conf: saved.conf.clone(),
            snapshot: saved.snapshot,
            entries: saved.entries,
        };
        let mut config = Config::new(id);
        config.election_tick = ELECTION_TICK;
        config.heartbeat_tick = HEARTBEAT_TICK;
        config.max_size_per_msg = MAX_MESSAGE_BYTES;
        config.min_election_tick = ELECTION_TICK + (id as usize % ELECTION_TICK);
        config.max_election_tick = config.min_election_tick + 1;
        config.pre_vote = true;
        config.check_quorum = true;
        config.applied = saved.applied;
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        Ok(Control {
            node: RawNode::new(&config, store, &logger)?,
            meta: saved.meta,
            conf: saved.conf,
            applied: saved.applied,
        })
    }

    pub fn save(&self) -> raft::Result<Saved> {
        let store = self.node.store();
        Ok(Saved {
            hard_state: store.hard_state.clone(),
            snapshot: store.snapshot.clone(),
            entries: store.entries.clone(),
            conf: self.conf.clone(),
            applied: self.applied,
            meta: self.meta.clone(),
        })
    }

    pub fn meta(&self) -> &Meta {
        &self.meta
    }

    pub fn term(&self) -> u64 {
        self.node.raft.term
    }

    pub fn leader(&self) -> Option<u64> {
        Some(self.node.raft.leader_id).filter(|id| *id != raft::INVALID_ID)
    }

    pub fn is_leader(&self) -> bool {
        self.node.raft.state == StateRole::Leader
    }

    pub fn learners(&self) -> &[u64] {
        &self.conf.learners
    }

    pub fn pending_conf(&self) -> bool {
        self.node.raft.has_pending_conf()
    }

    pub fn tick(&mut self) {
        self.node.tick();
    }

    pub fn step(&mut self, message: Message) -> raft::Result<()> {
        self.node.step(message)
    }

    pub fn propose(&mut self, command: &Command) -> raft::Result<()> {
        self.node.propose(Vec::new(), command.encode())
    }

    pub fn propose_conf_change(&mut self, kind: ConfChangeType, node: u64) -> raft::Result<()> {
        if kind != ConfChangeType::AddNode && self.conf.voters == [node] {
            return Err(raft::Error::ConfChangeError("removed all voters".into()));
        }
        let (change, context) = conf_change(kind, node);
        self.node.propose_conf_change(context, change)
    }

    pub fn drain(&mut self) -> raft::Result<Vec<Message>> {
        let mut out = Vec::new();
        while self.node.has_ready() {
            let mut ready = self.node.ready();
            out.extend(ready.take_messages());
            if !ready.snapshot().is_empty() {
                self.install(ready.snapshot().clone())?;
            }
            self.apply(ready.take_committed_entries());
            self.node.mut_store().append(ready.entries())?;
            if let Some(hard_state) = ready.hs() {
                self.node.mut_store().hard_state = hard_state.clone();
            }
            out.extend(ready.take_persisted_messages());
            let mut light = self.node.advance(ready);
            if let Some(commit) = light.commit_index() {
                self.node.mut_store().hard_state.commit = commit;
            }
            out.extend(light.take_messages());
            self.apply(light.take_committed_entries());
            self.node.advance_apply();
            self.compact()?;
        }
        Ok(out)
    }

    fn install(&mut self, snapshot: Snapshot) -> raft::Result<()> {
        let meta = Meta::decode(&snapshot.data)
            .ok_or_else(|| raft::Error::ConfigInvalid("corrupt snapshot".into()))?;
        self.meta = meta;
        self.conf = snapshot.get_metadata().get_conf_state().clone();
        self.applied = snapshot.get_metadata().index;
        self.node.mut_store().install(snapshot);
        Ok(())
    }

    fn compact(&mut self) -> raft::Result<()> {
        let store = self.node.store();
        if self.applied - store.snapshot_index() < SNAPSHOT_EVERY {
            return Ok(());
        }
        let mut snapshot = Snapshot::default();
        let meta = snapshot.mut_metadata();
        meta.index = self.applied;
        meta.term = store.term(self.applied)?;
        meta.set_conf_state(self.conf.clone());
        snapshot.data = self.meta.encode().into();
        self.node.mut_store().compact(snapshot);
        Ok(())
    }

    fn apply(&mut self, entries: Vec<Entry>) {
        for entry in entries {
            self.applied = entry.index;
            match entry.get_entry_type() {
                EntryType::EntryNormal => {
                    if let Some(command) = Command::decode(&entry.data) {
                        self.meta.apply(command);
                    }
                }
                EntryType::EntryConfChange => {
                    if let Some(change) = decode_conf_change(&entry.context)
                        && let Ok(conf) = self.node.apply_conf_change(&change)
                    {
                        self.conf = conf;
                        let raft = &mut self.node.raft;
                        if raft.state == StateRole::Leader && !self.conf.voters.contains(&raft.id) {
                            raft.become_follower(raft.term, raft::INVALID_ID);
                        }
                        self.meta.members = self.conf.voters.iter().copied().collect();
                        self.meta.version += 1;
                    }
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    type Log = Vec<(u64, u64, Vec<u8>, Vec<u8>)>;

    struct Slot {
        ctl: Option<Control>,
        saved: Saved,
        seen: u64,
    }

    struct Sim {
        seed: u64,
        rng: Rng,
        nodes: BTreeMap<u64, Slot>,
        flight: Vec<(u64, Message)>,
        now: u64,
        side: BTreeMap<u64, u8>,
        drop_pct: u64,
        delay: u64,
        leaders: BTreeMap<u64, u64>,
        acked: BTreeSet<u64>,
        next: u64,
    }

    fn holders(segment: u64, epoch: u64) -> Command {
        Command::Holders(HolderSet {
            segment,
            epoch,
            writer: 1,
            holders: BTreeSet::from([1, 2]),
        })
    }

    fn applied_log(ctl: &Control) -> Log {
        let saved = ctl.save().unwrap();
        saved
            .entries
            .iter()
            .filter(|e| e.index <= saved.applied)
            .map(|e| (e.index, e.term, e.data.to_vec(), e.context.to_vec()))
            .collect()
    }

    fn replay(saved: &Saved) -> Meta {
        let mut meta = if saved.snapshot.get_metadata().index == 0 {
            Meta::default()
        } else {
            Meta::decode(&saved.snapshot.data).unwrap()
        };
        for entry in saved.entries.iter().filter(|e| e.index <= saved.applied) {
            match entry.get_entry_type() {
                EntryType::EntryNormal => {
                    if let Some(command) = Command::decode(&entry.data) {
                        meta.apply(command);
                    }
                }
                EntryType::EntryConfChange => {
                    if let Some(change) = decode_conf_change(&entry.context) {
                        let node = change.node_id;
                        if change.get_change_type() == ConfChangeType::AddNode {
                            meta.members.insert(node);
                        } else if meta.members == BTreeSet::from([node]) {
                            continue;
                        } else {
                            meta.members.remove(&node);
                        }
                        meta.version += 1;
                    }
                }
                _ => {}
            }
        }
        meta
    }

    impl Sim {
        fn new(seed: u64, ids: &[u64]) -> Sim {
            let nodes = ids
                .iter()
                .map(|id| {
                    let ctl = if *id == 1 {
                        Control::found(*id)
                    } else {
                        Control::restore(*id, Saved::default())
                    }
                    .unwrap();
                    let saved = ctl.save().unwrap();
                    (
                        *id,
                        Slot {
                            ctl: Some(ctl),
                            saved,
                            seen: 0,
                        },
                    )
                })
                .collect();
            Sim {
                seed,
                rng: Rng(seed.wrapping_add(0x9E37_79B9)),
                nodes,
                flight: Vec::new(),
                now: 0,
                side: BTreeMap::new(),
                drop_pct: 0,
                delay: 3,
                leaders: BTreeMap::new(),
                acked: BTreeSet::new(),
                next: 1,
            }
        }

        fn cluster(seed: u64, n: u64) -> Sim {
            let ids: Vec<u64> = (1..=n).collect();
            let mut sim = Sim::new(seed, &ids);
            for id in 2..=n {
                sim.change(ConfChangeType::AddLearnerNode, id);
                sim.change(ConfChangeType::AddNode, id);
            }
            sim
        }

        fn ctl(&mut self, id: u64) -> Option<&mut Control> {
            self.nodes.get_mut(&id).and_then(|s| s.ctl.as_mut())
        }

        fn alive(&self) -> Vec<u64> {
            self.nodes
                .iter()
                .filter(|(_, s)| s.ctl.is_some())
                .map(|(id, _)| *id)
                .collect()
        }

        fn connected(&self, a: u64, b: u64) -> bool {
            self.side.get(&a) == self.side.get(&b)
        }

        fn leader(&self) -> Option<u64> {
            self.nodes
                .iter()
                .filter_map(|(id, s)| s.ctl.as_ref().map(|c| (*id, &c.node.raft)))
                .filter(|(_, raft)| raft.state == StateRole::Leader)
                .max_by_key(|(_, raft)| raft.term)
                .map(|(id, _)| id)
        }

        fn send(&mut self, message: Message) {
            if !self.connected(message.from, message.to) || self.rng.below(100) < self.drop_pct {
                return;
            }
            let at = self.now + 1 + self.rng.below(self.delay);
            self.flight.push((at, message));
        }

        fn flush(&mut self, id: u64) {
            let Some(slot) = self.nodes.get_mut(&id) else {
                return;
            };
            let Some(ctl) = slot.ctl.as_mut() else {
                return;
            };
            let out = ctl.drain().unwrap();
            let raft = &ctl.node.raft;
            if raft.state == StateRole::Leader {
                let holder = *self.leaders.entry(raft.term).or_insert(id);
                assert_eq!(
                    holder, id,
                    "seed {}: two leaders in term {}",
                    self.seed, raft.term
                );
            }
            if ctl.meta.version != slot.seen {
                slot.seen = ctl.meta.version;
                self.acked.extend(ctl.meta.segments.keys().copied());
            }
            for message in out {
                self.send(message);
            }
        }

        fn pump(&mut self) {
            self.now += 1;
            for id in self.alive() {
                if self.rng.below(4) != 0 {
                    self.ctl(id).unwrap().tick();
                    self.flush(id);
                }
            }
            let flight = std::mem::take(&mut self.flight);
            let (due, later): (Vec<_>, Vec<_>) =
                flight.into_iter().partition(|(at, _)| *at <= self.now);
            self.flight = later;
            for (_, message) in due {
                let to = message.to;
                if !self.connected(message.from, to) {
                    continue;
                }
                let seed = self.seed;
                let Some(ctl) = self.ctl(to) else {
                    continue;
                };
                match ctl.step(message) {
                    Ok(()) | Err(raft::Error::StepPeerNotFound | raft::Error::ProposalDropped) => {}
                    Err(e) => panic!("seed {seed}: step failed: {e}"),
                }
                self.flush(to);
            }
        }

        fn run(&mut self, steps: u64) {
            for _ in 0..steps {
                self.pump();
            }
        }

        fn until(&mut self, limit: u64, done: impl Fn(&Sim) -> bool) -> bool {
            for _ in 0..limit {
                if done(self) {
                    return true;
                }
                self.pump();
            }
            done(self)
        }

        fn propose(&mut self, id: u64, command: &Command) -> bool {
            let accepted = self.ctl(id).is_some_and(|ctl| ctl.propose(command).is_ok());
            self.flush(id);
            accepted
        }

        fn commit_one(&mut self) -> u64 {
            let segment = self.next;
            self.next += 1;
            let command = holders(segment, 1);
            for _ in 0..500 {
                if let Some(leader) = self.leader()
                    && self.propose(leader, &command)
                {
                    let done = self.until(100, |s| {
                        s.leader()
                            .and_then(|l| s.nodes[&l].ctl.as_ref())
                            .is_some_and(|c| c.meta.segments.contains_key(&segment))
                    });
                    if done {
                        return segment;
                    }
                }
                self.run(5);
            }
            panic!("seed {}: segment {segment} never committed", self.seed);
        }

        fn change(&mut self, kind: ConfChangeType, node: u64) {
            let done = |s: &Sim, kind: ConfChangeType| {
                s.leader()
                    .and_then(|l| s.nodes[&l].ctl.as_ref())
                    .is_some_and(|c| match kind {
                        ConfChangeType::AddLearnerNode => c.conf.learners.contains(&node),
                        _ => c.meta.members.contains(&node),
                    })
            };
            for _ in 0..40 {
                if done(self, kind) {
                    return;
                }
                self.propose_conf_at_leader(kind, node);
                self.until(25, |s| done(s, kind));
            }
            panic!("seed {}: conf change for {node} never applied", self.seed);
        }

        fn propose_conf_at_leader(&mut self, kind: ConfChangeType, node: u64) -> bool {
            let Some(leader) = self.leader() else {
                return false;
            };
            let accepted = self
                .ctl(leader)
                .unwrap()
                .propose_conf_change(kind, node)
                .is_ok();
            self.flush(leader);
            accepted
        }

        fn kill(&mut self, id: u64) {
            let slot = self.nodes.get_mut(&id).unwrap();
            if let Some(ctl) = slot.ctl.take() {
                slot.saved = ctl.save().unwrap();
            }
        }

        fn restart(&mut self, id: u64) {
            let slot = self.nodes.get_mut(&id).unwrap();
            if slot.ctl.is_none() {
                slot.ctl = Some(Control::restore(id, slot.saved.clone()).unwrap());
                slot.seen = 0;
            }
        }

        fn heal(&mut self) {
            self.side.clear();
            self.drop_pct = 0;
            for id in self.nodes.keys().copied().collect::<Vec<_>>() {
                self.restart(id);
            }
        }

        fn metas(&self) -> Vec<&Meta> {
            self.nodes
                .values()
                .filter_map(|s| s.ctl.as_ref().map(Control::meta))
                .collect()
        }

        fn check_prefixes(&self) {
            for ctl in self.nodes.values().filter_map(|s| s.ctl.as_ref()) {
                assert_eq!(
                    replay(&ctl.save().unwrap()),
                    *ctl.meta(),
                    "seed {}: meta differs from a replay of the applied log",
                    self.seed
                );
            }
            let logs: Vec<Log> = self
                .nodes
                .values()
                .filter_map(|s| s.ctl.as_ref().map(applied_log))
                .collect();
            let mut agreed = BTreeMap::new();
            for (index, term, data, context) in logs.into_iter().flatten() {
                assert_eq!(
                    *agreed
                        .entry(index)
                        .or_insert((term, data.clone(), context.clone())),
                    (term, data, context),
                    "seed {}: applied logs diverge",
                    self.seed
                );
            }
        }

        fn check_final(&mut self, quiet: u64) {
            self.heal();
            self.run(quiet);
            assert!(!self.acked.is_empty(), "seed {}: nothing acked", self.seed);
            assert!(
                self.until(100, |s| s.leader().is_some()),
                "seed {}: no leader at the end",
                self.seed
            );
            self.check_prefixes();
            let metas = self.metas();
            for meta in &metas {
                assert_eq!(*meta, metas[0], "seed {}: metas differ", self.seed);
            }
            for segment in &self.acked {
                assert!(
                    metas[0].segments.contains_key(segment),
                    "seed {}: acked segment {segment} lost",
                    self.seed
                );
            }
        }
    }

    #[test]
    fn three_and_five_nodes_elect_and_commit_a_hundred_commands() {
        for n in [3, 5] {
            let mut sim = Sim::cluster(n, n);
            for _ in 0..100 {
                sim.commit_one();
            }
            sim.check_final(100);
            let metas = sim.metas();
            assert_eq!(metas[0].segments.len(), 100);
            assert_eq!(metas[0].members, (1..=n).collect::<BTreeSet<_>>());
        }
    }

    #[test]
    fn a_cut_lets_only_the_majority_side_commit() {
        let mut sim = Sim::cluster(21, 5);
        for _ in 0..10 {
            sim.commit_one();
        }
        let old = sim.leader().unwrap();
        let other = (1..=5).find(|id| *id != old).unwrap();
        for id in 1..=5 {
            sim.side.insert(id, u8::from(id != old && id != other));
        }
        let lost = 1000;
        assert!(sim.propose(old, &holders(lost, 1)));
        sim.run(150);
        let majority: Vec<u64> = (1..=5).filter(|id| *id != old && *id != other).collect();
        let leader = sim.leader().unwrap();
        assert!(majority.contains(&leader));
        let kept = sim.commit_one();
        sim.run(50);
        for id in &majority {
            let meta = &sim.nodes[id].ctl.as_ref().unwrap().meta;
            assert!(meta.segments.contains_key(&kept));
            assert!(!meta.segments.contains_key(&lost));
        }
        for id in [old, other] {
            let meta = &sim.nodes[&id].ctl.as_ref().unwrap().meta;
            assert!(!meta.segments.contains_key(&kept));
            assert!(!meta.segments.contains_key(&lost));
        }
        sim.check_final(300);
        let meta = sim.metas()[0];
        assert!(meta.segments.contains_key(&kept));
        assert!(!meta.segments.contains_key(&lost));
        assert_eq!(meta.segments.len(), 11);
    }

    #[test]
    fn killing_the_leader_during_a_proposal_loses_no_committed_command() {
        let mut sim = Sim::cluster(33, 5);
        for _ in 0..4 {
            let leader = sim.leader().unwrap();
            for _ in 0..3 {
                let segment = sim.next;
                sim.next += 1;
                sim.propose(leader, &holders(segment, 1));
            }
            sim.kill(leader);
            sim.run(80);
            for _ in 0..5 {
                sim.commit_one();
            }
            sim.restart(leader);
        }
        sim.check_final(300);
        assert!(sim.metas()[0].segments.len() >= 20);
    }

    #[test]
    fn a_learner_becomes_a_voter_and_a_removal_needs_a_majority() {
        let mut sim = Sim::cluster(44, 5);
        sim.commit_one();
        sim.nodes.insert(
            6,
            Slot {
                ctl: Some(Control::restore(6, Saved::default()).unwrap()),
                saved: Saved::default(),
                seen: 0,
            },
        );
        sim.change(ConfChangeType::AddLearnerNode, 6);
        let leader = sim.leader().unwrap();
        let ctl = sim.nodes[&leader].ctl.as_ref().unwrap();
        assert_eq!(ctl.conf.learners, vec![6]);
        assert!(!ctl.meta.members.contains(&6));
        sim.change(ConfChangeType::AddNode, 6);
        sim.run(100);
        assert!(sim.metas().iter().all(|m| m.members.len() == 6));

        let leader = sim.leader().unwrap();
        let victims: Vec<u64> = (1..=5).filter(|id| *id != leader).take(3).collect();
        for id in &victims {
            sim.kill(*id);
        }
        let dead = victims[0];
        assert!(sim.propose_conf_at_leader(ConfChangeType::RemoveNode, dead));
        sim.run(300);
        assert!(sim.metas().iter().all(|m| m.members.contains(&dead)));

        sim.restart(victims[1]);
        for _ in 0..40 {
            sim.propose_conf_at_leader(ConfChangeType::RemoveNode, dead);
            sim.run(15);
        }
        sim.run(100);
        assert!(sim.metas().iter().all(|m| !m.members.contains(&dead)));
        assert!(sim.metas().iter().all(|m| m.members.len() == 5));
    }

    #[test]
    fn a_restored_node_has_the_same_meta_and_catches_up() {
        let mut sim = Sim::cluster(55, 3);
        for _ in 0..20 {
            sim.commit_one();
        }
        sim.run(50);
        let before = sim.nodes[&2].ctl.as_ref().unwrap().meta.clone();
        sim.kill(2);
        for _ in 0..10 {
            sim.commit_one();
        }
        sim.restart(2);
        assert_eq!(sim.nodes[&2].ctl.as_ref().unwrap().meta, before);
        sim.check_final(200);
        assert_eq!(sim.metas()[0].segments.len(), 30);
    }

    #[test]
    fn a_follower_cut_during_many_commands_catches_up_from_a_snapshot() {
        let mut sim = Sim::cluster(66, 3);
        let follower = (1..=3).find(|id| Some(*id) != sim.leader()).unwrap();
        sim.side.insert(follower, 1);
        for _ in 0..2500 {
            sim.commit_one();
        }
        let leader = sim.leader().unwrap();
        assert!(
            sim.nodes[&leader]
                .ctl
                .as_ref()
                .unwrap()
                .node
                .store()
                .first_index()
                .unwrap()
                > 1
        );
        sim.heal();
        assert!(sim.until(2000, |s| {
            s.nodes[&follower].ctl.as_ref().unwrap().meta.segments.len() == 2500
        }));
        sim.check_final(100);
        let leader = sim.leader().unwrap();
        let lead = &sim.nodes[&leader].ctl.as_ref().unwrap().meta;
        let caught = &sim.nodes[&follower].ctl.as_ref().unwrap().meta;
        assert_eq!(caught, lead);
        assert!(
            sim.nodes[&follower]
                .ctl
                .as_ref()
                .unwrap()
                .node
                .store()
                .snapshot_index()
                > 0
        );
    }

    #[test]
    fn a_node_saved_and_restored_after_many_commands_has_the_same_meta() {
        let mut ctl = solo();
        for segment in 1..=2500 {
            ctl.propose(&holders(segment, 1)).unwrap();
            ctl.drain().unwrap();
        }
        let saved = ctl.save().unwrap();
        assert!(saved.snapshot.get_metadata().index > 0);
        assert!(saved.entries.len() < 1000);
        let restored = Control::restore(1, saved).unwrap();
        assert_eq!(restored.meta, ctl.meta);
        assert_eq!(restored.meta.segments.len(), 2500);
    }

    #[test]
    fn the_epoch_rule_ignores_any_epoch_but_current_plus_one() {
        let mut meta = Meta::default();
        let leader = |epoch| Command::Leader {
            segment: 9,
            epoch,
            disk: 4,
        };
        let groups = |epoch| {
            Command::Groups(Groups {
                epoch,
                size: 3,
                groups: vec![vec![1, 2, 3]],
                stand_ins: vec![(0, 7)],
            })
        };
        let floor = |epoch| Command::Floor(Floor { epoch, value: 2 });
        for epoch in [0, 2, 5] {
            for command in [
                holders(9, epoch),
                leader(epoch),
                groups(epoch),
                floor(epoch),
            ] {
                let before = meta.clone();
                meta.apply(command);
                assert_eq!(meta, before, "epoch {epoch} must be a no-op");
            }
        }
        for command in [holders(9, 1), leader(1), groups(1), floor(1)] {
            let before = meta.clone();
            meta.apply(command.clone());
            assert_ne!(meta, before);
            let applied = meta.clone();
            meta.apply(command);
            assert_eq!(meta, applied);
        }
        meta.apply(holders(9, 3));
        meta.apply(leader(3));
        assert_eq!(meta.segments[&9].holders.epoch, 1);
        assert_eq!(meta.segments[&9].leader_epoch, 1);
        meta.apply(holders(9, 2));
        meta.apply(leader(2));
        assert_eq!(meta.segments[&9].holders.epoch, 2);
        assert_eq!(meta.segments[&9].leader_epoch, 2);
    }

    #[test]
    fn commands_survive_encode_and_decode() {
        let commands = [
            Command::Groups(Groups {
                epoch: 4,
                size: 3,
                groups: vec![vec![1, 2, 3], vec![], vec![9]],
                stand_ins: vec![(2, 8), (0, 5)],
            }),
            holders(7, 3),
            Command::Leader {
                segment: 1,
                epoch: 2,
                disk: 3,
            },
            Command::Floor(Floor { epoch: 6, value: 2 }),
        ];
        for command in commands {
            let bytes = command.encode();
            assert_eq!(Command::decode(&bytes), Some(command));
            assert_eq!(Command::decode(&bytes[..bytes.len() - 1]), None);
            let mut long = bytes;
            long.push(0);
            assert_eq!(Command::decode(&long), None);
        }
        assert_eq!(Command::decode(&[]), None);
        assert_eq!(Command::decode(&[9]), None);
    }

    #[test]
    fn a_leader_that_removes_itself_steps_down_and_the_rest_carry_on() {
        let mut sim = Sim::cluster(7, 3);
        let old = sim.leader().unwrap();
        assert!(sim.propose_conf_at_leader(ConfChangeType::RemoveNode, old));
        assert!(sim.until(300, |s| s.leader().is_some_and(|l| l != old)));
        sim.commit_one();
        sim.run(50);
        for id in (1..=3).filter(|id| *id != old) {
            let meta = &sim.nodes[&id].ctl.as_ref().unwrap().meta;
            assert!(!meta.members.contains(&old));
            assert_eq!(meta.segments.len(), 1);
        }
    }

    fn saved_one() -> Saved {
        Control::found(1).unwrap().save().unwrap()
    }

    #[test]
    fn restore_refuses_applied_above_commit() {
        let mut saved = saved_one();
        saved.hard_state.commit = 0;
        assert!(Control::restore(1, saved).is_err());
    }

    #[test]
    fn restore_refuses_commit_above_the_last_entry() {
        let mut saved = saved_one();
        saved.hard_state.commit = 2;
        assert!(Control::restore(1, saved).is_err());
        let mut empty = Saved::default();
        empty.hard_state.commit = 1;
        assert!(Control::restore(1, empty).is_err());
    }

    #[test]
    fn restore_refuses_entries_that_are_not_contiguous_from_one() {
        let first = saved_one().entries[0].clone();
        let at = |index| Entry {
            index,
            ..first.clone()
        };
        for indexes in [vec![1, 3], vec![2], vec![1, 1]] {
            let mut saved = saved_one();
            saved.entries = indexes.into_iter().map(at).collect();
            assert!(Control::restore(1, saved).is_err());
        }
    }

    #[test]
    fn decode_refuses_a_holder_list_that_does_not_increase() {
        let bytes = holders(7, 3).encode();
        assert!(Command::decode(&bytes).is_some());
        let n = bytes.len();
        for (a, b) in [(1u64, 1u64), (2, 1)] {
            let mut bad = bytes.clone();
            bad[n - 16..n - 8].copy_from_slice(&a.to_le_bytes());
            bad[n - 8..].copy_from_slice(&b.to_le_bytes());
            assert_eq!(Command::decode(&bad), None);
        }
    }

    fn solo() -> Control {
        let mut ctl = Control::found(1).unwrap();
        for _ in 0..30 {
            ctl.tick();
            ctl.drain().unwrap();
        }
        assert_eq!(ctl.node.raft.state, StateRole::Leader);
        ctl
    }

    #[test]
    fn a_node_reports_its_role_term_and_learners() {
        let mut ctl = Control::found(1).unwrap();
        assert!(!ctl.is_leader());
        assert_eq!(ctl.leader(), None);
        let before = ctl.term();
        for _ in 0..30 {
            ctl.tick();
            ctl.drain().unwrap();
        }
        assert!(ctl.is_leader());
        assert_eq!(ctl.leader(), Some(1));
        assert!(ctl.term() > before);
        assert!(ctl.learners().is_empty());
        ctl.propose_conf_change(ConfChangeType::AddLearnerNode, 2)
            .unwrap();
        ctl.drain().unwrap();
        assert_eq!(ctl.learners(), [2]);
    }

    #[test]
    fn a_change_that_leaves_no_voter_is_refused_at_propose() {
        let mut ctl = solo();
        assert!(
            ctl.propose_conf_change(ConfChangeType::RemoveNode, 1)
                .is_err()
        );
        assert!(
            ctl.propose_conf_change(ConfChangeType::AddLearnerNode, 1)
                .is_err()
        );
        ctl.propose(&holders(1, 1)).unwrap();
        ctl.drain().unwrap();
        assert!(ctl.meta.segments.contains_key(&1));
        assert_eq!(ctl.meta.members, BTreeSet::from([1]));
    }

    #[test]
    fn a_rejected_conf_change_entry_does_not_stop_the_commands_after_it() {
        let mut ctl = solo();
        let (change, context) = conf_change(ConfChangeType::RemoveNode, 1);
        ctl.node.propose_conf_change(context, change).unwrap();
        ctl.propose(&holders(1, 1)).unwrap();
        ctl.drain().unwrap();
        assert!(ctl.meta.segments.contains_key(&1));
        assert_eq!(ctl.meta.members, BTreeSet::from([1]));
        assert_eq!(ctl.conf.voters, vec![1]);
        assert_eq!(ctl.meta.version, 2);
        assert_eq!(ctl.applied, ctl.node.store().last_index().unwrap());
    }

    fn schedule(seed: u64) {
        let mut sim = Sim::cluster(seed, 5);
        sim.drop_pct = sim.rng.below(21);
        sim.delay = 1 + sim.rng.below(4);
        for step in 0..300 {
            match sim.rng.below(80) {
                0..=11 => {
                    let ids = sim.alive();
                    let any = ids[sim.rng.below(ids.len() as u64) as usize];
                    let id = sim.leader().unwrap_or(any);
                    let segment = sim.next;
                    sim.next += 1;
                    sim.propose(id, &holders(segment, 1));
                }
                12 => {
                    sim.side.clear();
                    for id in 1..=5 {
                        let side = sim.rng.below(2) as u8;
                        sim.side.insert(id, side);
                    }
                }
                13 => sim.side.clear(),
                14 => {
                    let ids = sim.alive();
                    if ids.len() > 1 {
                        let id = ids[sim.rng.below(ids.len() as u64) as usize];
                        sim.kill(id);
                    }
                }
                15 | 16 => {
                    let id = 1 + sim.rng.below(5);
                    sim.restart(id);
                }
                17..=19 => {
                    let ids = sim.alive();
                    let id = ids[sim.rng.below(ids.len() as u64) as usize];
                    let view = sim.ctl(id).unwrap().meta.clone();
                    let command = match sim.rng.below(3) {
                        0 => {
                            let segment = 1 + sim.rng.below(sim.next);
                            let current = view.segments.get(&segment).map_or(0, |s| s.leader_epoch);
                            Command::Leader {
                                segment,
                                epoch: current + 1,
                                disk: 1 + sim.rng.below(5),
                            }
                        }
                        1 => Command::Groups(Groups {
                            epoch: view.groups.epoch + 1,
                            size: 3,
                            groups: vec![vec![1, 2, 3], vec![3, 4, 5]],
                            stand_ins: vec![(0, 9)],
                        }),
                        _ => Command::Floor(Floor {
                            epoch: view.floor.epoch + 1,
                            value: sim.rng.below(3) as u8,
                        }),
                    };
                    sim.propose(id, &command);
                }
                20..=23 => {
                    let node = 1 + sim.rng.below(5);
                    sim.propose_conf_at_leader(ConfChangeType::AddNode, node);
                }
                24 | 25 => {
                    let node = 1 + sim.rng.below(5);
                    if sim.leader() != Some(node) {
                        sim.propose_conf_at_leader(ConfChangeType::RemoveNode, node);
                    }
                }
                _ => {}
            }
            sim.pump();
            if step % 50 == 49 {
                sim.check_prefixes();
            }
        }
        sim.heal();
        sim.run(300);
        for id in 1..=5 {
            sim.change(ConfChangeType::AddNode, id);
        }
        sim.commit_one();
        sim.check_final(600);
    }

    #[test]
    fn seeded_search_keeps_logs_leaders_and_acked_commands() {
        for seed in 1..=1000 {
            schedule(seed);
        }
        println!("CONTROL SEARCH PASS seeds=1000");
    }
}
