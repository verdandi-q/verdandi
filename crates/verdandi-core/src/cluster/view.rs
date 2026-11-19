use std::collections::{BTreeMap, BTreeSet};

use super::fnv;

pub const HEARTBEAT_MS: u64 = 500;
pub const GOSSIP_MS: u64 = 1_000;
pub const FAIL_MS: u64 = 3_000;
pub const JOIN_MS: u64 = 10_000;
pub const SAMPLE_MS: u64 = 5_000;
pub const STOP_BUDGET_MS: u64 = 110_000;
pub const TOMBSTONE_HORIZON: u64 = 7 * 24 * 3_600_000;
pub const LINE_BYTES: usize = 29;
pub const TRIP_PEERS: usize = 8;

pub const SIGTERM: u8 = 1;
pub const SPOT: u8 = 2;
pub const REBALANCE: u8 = 4;
pub const LIFECYCLE: u8 = 8;
const LOSS_SIGNALS: u8 = SPOT | REBALANCE | LIFECYCLE;
const BODY_FOLLOWS: u8 = 0x80;

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum State {
    Alive,
    Dead,
    Left,
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct Head {
    pub restart: u64,
    pub heartbeat: u64,
    pub state: State,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Body {
    pub address: String,
    pub join_ms: u64,
    pub alive_ms: u64,
    pub format_max: u16,
    pub committed: u16,
    pub zone: String,
    pub spot_pool: String,
    pub instance_type: String,
    pub launch_type: String,
    pub host: u64,
    pub prefix: String,
    pub round_trips: Vec<(u64, u32)>,
    pub disk_total: u64,
    pub disk_free: u64,
    pub fill_rate: u64,
    pub holds: u64,
    pub debt: u64,
    pub count: u8,
    pub era: u32,
    pub epoch: u64,
    pub domains: u64,
    pub quiet: bool,
    pub reconciled: bool,
    pub marks: BTreeSet<u64>,
    pub unreachable: Vec<u64>,
    pub signals: u8,
    pub remaining: u32,
    pub drained: bool,
    pub can_found: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Entry {
    pub head: Head,
    pub body_key: (u64, u32),
    pub body: Body,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Line {
    pub disk: u64,
    pub head: Head,
    pub version: u32,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Update {
    pub disk: u64,
    pub head: Head,
    pub body: Option<((u64, u32), Body)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LossKind {
    Death,
    Signal,
    Left,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Loss {
    pub run_ms: u64,
    pub last_ms: u64,
    pub prior_ms: u64,
    pub kind: LossKind,
}

impl Loss {
    fn new(run_ms: u64, kind: LossKind) -> Self {
        Self {
            run_ms,
            last_ms: run_ms,
            prior_ms: run_ms,
            kind,
        }
    }
}

#[derive(Clone, Debug)]
struct Member {
    entry: Entry,
    rise_ms: u64,
    seen_run_ms: Option<u64>,
    alive_run_ms: u64,
    absent_run_ms: Option<u64>,
    heard: bool,
}

#[derive(Clone, Debug)]
pub struct View {
    me: u64,
    start_run_ms: u64,
    beat_ms: u64,
    body_ms: u64,
    pending: Option<Body>,
    members: BTreeMap<u64, Member>,
    losses: BTreeMap<(u64, u64), Loss>,
    deaths: BTreeSet<(u64, u64)>,
    pruned: BTreeMap<u64, (u64, u64)>,
}

impl View {
    pub fn new(me: u64, restart: u64, body: Body, now_ms: u64, run_ms: u64) -> Self {
        let entry = Entry {
            head: Head {
                restart,
                heartbeat: 0,
                state: State::Alive,
            },
            body_key: (restart, 1),
            body,
        };
        let member = Member {
            entry,
            rise_ms: now_ms,
            seen_run_ms: Some(run_ms),
            alive_run_ms: run_ms,
            absent_run_ms: None,
            heard: false,
        };
        let mut view = Self {
            me,
            start_run_ms: run_ms,
            beat_ms: now_ms,
            body_ms: now_ms,
            pending: None,
            members: BTreeMap::from([(me, member)]),
            losses: BTreeMap::new(),
            deaths: BTreeSet::new(),
            pruned: BTreeMap::new(),
        };
        view.note(me, run_ms);
        view
    }

    pub fn me(&self) -> u64 {
        self.me
    }

    pub fn own(&self) -> &Entry {
        &self.members[&self.me].entry
    }

    pub fn latest(&self) -> &Body {
        self.pending.as_ref().unwrap_or(&self.own().body)
    }

    pub fn entry(&self, disk: u64) -> Option<&Entry> {
        self.members.get(&disk).map(|m| &m.entry)
    }

    pub fn absent(&self, disk: u64) -> bool {
        self.members
            .get(&disk)
            .is_none_or(|m| m.entry.head.state != State::Alive)
    }

    pub fn known(&self) -> impl Iterator<Item = u64> + '_ {
        self.members.keys().copied()
    }

    pub fn live(&self) -> impl Iterator<Item = (u64, &Entry)> {
        self.members
            .iter()
            .filter(|(_, m)| m.entry.head.state == State::Alive)
            .map(|(disk, m)| (*disk, &m.entry))
    }

    pub fn alive_for(&self, disk: u64, run_ms: u64) -> u64 {
        self.members
            .get(&disk)
            .filter(|m| m.entry.head.state == State::Alive)
            .map_or(0, |m| run_ms.saturating_sub(m.alive_run_ms))
    }

    pub fn losses(&self, from_run_ms: u64) -> usize {
        self.losses
            .values()
            .filter(|l| l.last_ms >= from_run_ms)
            .count()
    }

    pub fn quiet(&self, run_ms: u64, period: u64) -> bool {
        run_ms >= self.start_run_ms + period && self.losses(run_ms.saturating_sub(period)) == 0
    }

    pub fn co_losses(&self) -> BTreeMap<(u64, u64), u32> {
        let mut by_sample: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
        for (sample, disk) in &self.deaths {
            by_sample.entry(*sample).or_default().push(*disk);
        }
        let mut pairs = BTreeMap::new();
        for disks in by_sample.values() {
            for (i, a) in disks.iter().enumerate() {
                for b in &disks[i + 1..] {
                    *pairs.entry((*a.min(b), *a.max(b))).or_insert(0) += 1;
                }
            }
        }
        pairs
    }

    pub fn drained(&mut self, disk: u64, now_ms: u64, run_ms: u64) {
        if disk == self.me {
            let mut body = self
                .pending
                .take()
                .unwrap_or_else(|| self.own().body.clone());
            body.drained = true;
            self.pending = Some(body);
            self.publish(now_ms, run_ms);
        } else if let Some(m) = self.members.get_mut(&disk) {
            m.entry.body.drained = true;
        }
    }

    pub fn absent_since(&self, disk: u64) -> Option<u64> {
        self.members.get(&disk).and_then(|m| m.absent_run_ms)
    }

    fn is_drained(&self, disk: u64) -> bool {
        self.members
            .get(&disk)
            .is_some_and(|m| m.entry.body.drained && m.entry.body_key.0 == m.entry.head.restart)
    }

    pub fn durable(&self, count: u8) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.members.len() as u32).to_le_bytes());
        for (disk, m) in &self.members {
            let head = m.entry.head;
            for n in [
                *disk,
                head.restart,
                head.heartbeat,
                m.seen_run_ms.unwrap_or(0),
                m.absent_run_ms.unwrap_or(0),
            ] {
                out.extend_from_slice(&n.to_le_bytes());
            }
            out.push(
                head.state as u8
                    | u8::from(m.seen_run_ms.is_some()) << 2
                    | u8::from(m.absent_run_ms.is_some()) << 3
                    | u8::from(m.heard) << 4
                    | u8::from(self.is_drained(*disk)) << 5,
            );
        }
        out.extend_from_slice(&(self.pruned.len() as u32).to_le_bytes());
        for (disk, (restart, run_ms)) in &self.pruned {
            for n in [*disk, *restart, *run_ms] {
                out.extend_from_slice(&n.to_le_bytes());
            }
        }
        out.push(count);
        out.extend_from_slice(&(self.losses.len() as u32).to_le_bytes());
        for ((disk, restart), l) in &self.losses {
            for n in [*disk, *restart, l.run_ms, l.last_ms, l.prior_ms] {
                out.extend_from_slice(&n.to_le_bytes());
            }
            out.push(l.kind as u8);
        }
        out
    }

    pub fn restored(
        me: u64,
        restart: u64,
        body: Body,
        now_ms: u64,
        run_ms: u64,
        durable: &[u8],
    ) -> Option<(Self, u8)> {
        let mut view = Self::new(me, restart, body, now_ms, run_ms);
        let mut ids = BTreeSet::new();
        let mut rest = durable;
        for _ in 0..u32::from_le_bytes(take(&mut rest)?) {
            let [disk, old, heartbeat, seen, absent] =
                [(); 5].map(|_| take(&mut rest).map(u64::from_le_bytes));
            let (disk, flags) = (disk?, take::<1>(&mut rest)?[0]);
            if !ids.insert(disk) {
                return None;
            }
            let state = match (flags & 3, flags >> 6) {
                (0, 0) => State::Alive,
                (1, 0) => State::Dead,
                (2, 0) => State::Left,
                _ => return None,
            };
            let seen = (flags & 4 != 0).then_some(seen?);
            let absent = (flags & 8 != 0).then_some(absent?);
            if disk == me {
                view.members.get_mut(&me)?.seen_run_ms = seen;
                continue;
            }
            if state != State::Alive && absent.is_none() {
                return None;
            }
            let head = Head {
                restart: old?,
                heartbeat: heartbeat?,
                state,
            };
            let entry = Entry {
                head,
                body_key: (head.restart, 0),
                body: Body {
                    drained: flags & 32 != 0,
                    ..Body::default()
                },
            };
            let member = Member {
                entry,
                rise_ms: now_ms,
                seen_run_ms: seen,
                alive_run_ms: run_ms,
                absent_run_ms: absent,
                heard: flags & 16 != 0,
            };
            view.members.insert(disk, member);
        }
        for _ in 0..u32::from_le_bytes(take(&mut rest)?) {
            let [disk, old, at] = [(); 3].map(|_| take(&mut rest).map(u64::from_le_bytes));
            view.pruned.insert(disk?, (old?, at?));
        }
        let count = take::<1>(&mut rest)?[0];
        for _ in 0..u32::from_le_bytes(take(&mut rest)?) {
            let [disk, old, first, last, prior] =
                [(); 5].map(|_| take(&mut rest).map(u64::from_le_bytes));
            let kind = match take::<1>(&mut rest)?[0] {
                0 => LossKind::Death,
                1 => LossKind::Signal,
                2 => LossKind::Left,
                _ => return None,
            };
            let loss = Loss {
                run_ms: first?,
                last_ms: last?,
                prior_ms: prior?,
                kind,
            };
            view.losses.insert((disk?, old?), loss);
        }
        rest.is_empty().then_some((view, count))
    }

    pub fn set_body(&mut self, body: Body, now_ms: u64, run_ms: u64) {
        let own = &self.members[&self.me].entry.body;
        if *own == body {
            self.pending = None;
            return;
        }
        let urgent = own.signals != body.signals
            || own.reconciled != body.reconciled
            || own.marks != body.marks
            || own.unreachable != body.unreachable;
        self.pending = Some(body);
        if urgent || now_ms >= self.body_ms + SAMPLE_MS {
            self.publish(now_ms, run_ms);
        }
    }

    pub fn leave(&mut self, remaining: u32, now_ms: u64, run_ms: u64) {
        let mut body = self
            .pending
            .take()
            .unwrap_or_else(|| self.own().body.clone());
        body.remaining = remaining;
        self.pending = Some(body);
        self.publish(now_ms, run_ms);
        if let Some(me) = self.members.get_mut(&self.me) {
            me.entry.head.state = State::Left;
        }
    }

    fn publish(&mut self, now_ms: u64, run_ms: u64) {
        let Some(body) = self.pending.take() else {
            return;
        };
        let me = self.me;
        if let Some(m) = self.members.get_mut(&me) {
            m.entry.body_key = (m.entry.head.restart, m.entry.body_key.1 + 1);
            m.entry.body = body;
        }
        self.body_ms = now_ms;
        self.note(me, run_ms);
    }

    pub fn tick(&mut self, now_ms: u64, run_ms: u64, list: &[u64]) {
        if now_ms >= self.beat_ms + HEARTBEAT_MS {
            self.beat_ms = now_ms;
            if let Some(m) = self.members.get_mut(&self.me)
                && m.entry.head.state == State::Alive
            {
                m.entry.head.heartbeat += 1;
            }
        }
        if self.pending.is_some() && now_ms >= self.body_ms + SAMPLE_MS {
            self.publish(now_ms, run_ms);
        }
        let silent: Vec<u64> = self
            .members
            .iter()
            .filter(|(disk, m)| {
                **disk != self.me
                    && m.entry.head.state == State::Alive
                    && now_ms >= m.rise_ms + FAIL_MS
            })
            .map(|(disk, _)| *disk)
            .collect();
        for disk in silent {
            let old = self.members[&disk].entry.head;
            if let Some(m) = self.members.get_mut(&disk) {
                m.entry.head.state = State::Dead;
            }
            self.moved(disk, old, now_ms, run_ms);
        }
        let expired: Vec<((u64, u64), u64)> = self
            .members
            .iter()
            .filter(|(disk, m)| {
                m.entry.head.state == State::Left
                    && m.entry.body.remaining > 0
                    && m.entry.body.holds > 0
                    && !m.heard
                    && !self.is_drained(**disk)
                    && m.absent_run_ms
                        .is_some_and(|t| run_ms >= t + STOP_BUDGET_MS + JOIN_MS)
            })
            .filter_map(|(disk, m)| {
                let at = m.absent_run_ms? + STOP_BUDGET_MS + JOIN_MS;
                Some(((*disk, m.entry.head.restart), at))
            })
            .collect();
        for (key, at) in expired {
            let loss = self
                .losses
                .entry(key)
                .or_insert(Loss::new(run_ms, LossKind::Left));
            loss.last_ms = loss.last_ms.max(at);
        }
        let gone: Vec<(u64, u64)> = self
            .members
            .iter()
            .filter(|(disk, m)| {
                **disk != self.me
                    && !list.contains(disk)
                    && m.absent_run_ms
                        .is_some_and(|t| run_ms >= t + TOMBSTONE_HORIZON)
            })
            .map(|(disk, m)| (*disk, m.entry.head.restart))
            .collect();
        for (disk, restart) in gone {
            self.members.remove(&disk);
            self.pruned.insert(disk, (restart, run_ms));
        }
        self.pruned
            .retain(|_, (_, t)| run_ms < *t + TOMBSTONE_HORIZON);
        self.losses
            .retain(|_, l| run_ms < l.last_ms + TOMBSTONE_HORIZON);
        self.deaths
            .retain(|(sample, _)| run_ms < sample * SAMPLE_MS + TOMBSTONE_HORIZON);
    }

    pub fn digest(&self) -> Vec<Line> {
        self.members
            .iter()
            .map(|(disk, m)| Line {
                disk: *disk,
                head: m.entry.head,
                version: if m.entry.body_key.0 == m.entry.head.restart {
                    m.entry.body_key.1
                } else {
                    0
                },
            })
            .collect()
    }

    pub fn answer(&self, digest: &[Line]) -> Vec<Update> {
        let theirs: BTreeMap<u64, &Line> = digest.iter().map(|l| (l.disk, l)).collect();
        self.members
            .iter()
            .filter(|(disk, m)| !m.heard || theirs.contains_key(disk))
            .filter_map(|(disk, m)| {
                let line = theirs.get(disk);
                let head = line.is_none_or(|l| m.entry.head > l.head);
                let body = line.is_none_or(|l| m.entry.body_key > (l.head.restart, l.version));
                (head || body).then(|| Update {
                    disk: *disk,
                    head: m.entry.head,
                    body: body.then(|| (m.entry.body_key, m.entry.body.clone())),
                })
            })
            .collect()
    }

    pub fn merge(&mut self, updates: &[Update], now_ms: u64, run_ms: u64) {
        let me = self.me;
        for update in updates.iter().filter(|u| u.disk != me) {
            let Some(member) = self.members.get_mut(&update.disk) else {
                let Some((key, body)) = &update.body else {
                    continue;
                };
                if update.head.state != State::Alive
                    && self
                        .pruned
                        .get(&update.disk)
                        .is_some_and(|(restart, _)| update.head.restart <= *restart)
                {
                    continue;
                }
                let absent = (update.head.state != State::Alive).then_some(run_ms);
                self.members.insert(
                    update.disk,
                    Member {
                        entry: Entry {
                            head: update.head,
                            body_key: *key,
                            body: body.clone(),
                        },
                        rise_ms: now_ms,
                        seen_run_ms: absent.is_none().then_some(run_ms),
                        alive_run_ms: run_ms,
                        absent_run_ms: absent,
                        heard: absent.is_some(),
                    },
                );
                self.note(update.disk, run_ms);
                continue;
            };
            if let Some((key, body)) = &update.body
                && *key > member.entry.body_key
            {
                member.entry.body_key = *key;
                member.entry.body = body.clone();
            }
            let old = member.entry.head;
            if update.head > old {
                member.entry.head = update.head;
                if (update.head.restart, update.head.heartbeat) > (old.restart, old.heartbeat) {
                    member.rise_ms = now_ms;
                }
                self.moved(update.disk, old, now_ms, run_ms);
            }
            self.note(update.disk, run_ms);
        }
    }

    fn moved(&mut self, disk: u64, old: Head, now_ms: u64, run_ms: u64) {
        let drained = self.is_drained(disk);
        let Some(m) = self.members.get_mut(&disk) else {
            return;
        };
        let head = m.entry.head;
        if head.restart > old.restart {
            m.alive_run_ms = run_ms;
            m.rise_ms = now_ms;
        }
        match head.state {
            State::Alive => {
                if old.state == State::Dead
                    && let Some(t) = m.absent_run_ms
                    && run_ms < t + JOIN_MS
                {
                    let key = (disk, old.restart);
                    if let Some(loss) = self.losses.get_mut(&key) {
                        if loss.kind == LossKind::Death && loss.run_ms >= t {
                            self.losses.remove(&key);
                        } else if loss.last_ms >= t {
                            loss.last_ms = loss.prior_ms;
                        }
                    }
                    self.deaths.remove(&(t / SAMPLE_MS, disk));
                }
                if old.state != State::Alive {
                    m.alive_run_ms = run_ms;
                }
                m.absent_run_ms = None;
                m.heard = false;
            }
            State::Dead | State::Left => {
                let new = old.state != head.state || head.restart > old.restart;
                if new {
                    m.absent_run_ms = Some(run_ms);
                }
                if new && head.state == State::Dead && !m.heard && !drained {
                    let loss = self
                        .losses
                        .entry((disk, head.restart))
                        .or_insert(Loss::new(run_ms, LossKind::Death));
                    if loss.last_ms < run_ms {
                        loss.prior_ms = loss.last_ms;
                        loss.last_ms = run_ms;
                    }
                    self.deaths.insert((run_ms / SAMPLE_MS, disk));
                }
            }
        }
    }

    fn note(&mut self, disk: u64, run_ms: u64) {
        let Some(m) = self.members.get(&disk) else {
            return;
        };
        if m.entry.body.signals & LOSS_SIGNALS != 0
            && m.entry.body.holds > 0
            && !m.heard
            && m.entry.body_key.0 == m.entry.head.restart
            && !self.is_drained(disk)
        {
            self.losses
                .entry((disk, m.entry.head.restart))
                .or_insert(Loss::new(run_ms, LossKind::Signal));
        }
    }
}

pub fn take<const N: usize>(rest: &mut &[u8]) -> Option<[u8; N]> {
    let (bytes, tail) = rest.split_first_chunk::<N>()?;
    *rest = tail;
    Some(*bytes)
}

pub fn marks_hash(marks: &BTreeMap<u64, u64>) -> u64 {
    fnv(marks.iter().flat_map(|(origin, mark)| [*origin, *mark]))
}

pub fn trip_peers(disk: u64, trips: &[(u64, u32)]) -> Vec<(u64, u32)> {
    let mut near = trips.to_vec();
    near.sort_unstable_by_key(|(peer, micros)| (*micros, *peer));
    let mut far = near.split_off(near.len().min(TRIP_PEERS / 2));
    far.sort_unstable_by_key(|(peer, _)| fnv([disk, *peer]));
    far.truncate(TRIP_PEERS - near.len());
    near.extend(far);
    near
}

pub fn encode_digest(lines: &[Line]) -> Vec<u8> {
    let mut out = Vec::with_capacity(lines.len() * LINE_BYTES);
    for line in lines {
        put_line(&mut out, line.disk, line.head, line.version, 0);
    }
    out
}

pub fn encode_updates(updates: &[Update]) -> Vec<u8> {
    let mut out = Vec::new();
    for update in updates {
        match &update.body {
            None => put_line(&mut out, update.disk, update.head, 0, 0),
            Some(((restart, version), body)) => {
                put_line(&mut out, update.disk, update.head, *version, BODY_FOLLOWS);
                out.extend_from_slice(&restart.to_le_bytes());
                put_body(&mut out, update.disk, body);
            }
        }
    }
    out
}

pub fn encode_lost(lost: &[(u64, u64)]) -> Vec<u8> {
    let lost = &lost[..lost.len().min(usize::from(u16::MAX))];
    let mut out = (lost.len() as u16).to_le_bytes().to_vec();
    for (disk, restart) in lost {
        out.extend_from_slice(&disk.to_le_bytes());
        out.extend_from_slice(&restart.to_le_bytes());
    }
    out
}

fn put_line(out: &mut Vec<u8>, disk: u64, head: Head, version: u32, flag: u8) {
    out.extend_from_slice(&disk.to_le_bytes());
    out.extend_from_slice(&head.restart.to_le_bytes());
    out.extend_from_slice(&head.heartbeat.to_le_bytes());
    out.push(head.state as u8 | flag);
    out.extend_from_slice(&version.to_le_bytes());
}

fn put_text(out: &mut Vec<u8>, text: &str) {
    let bytes = &text.as_bytes()[..text.len().min(usize::from(u16::MAX))];
    out.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    out.extend_from_slice(bytes);
}

fn put_body(out: &mut Vec<u8>, disk: u64, b: &Body) {
    for text in [
        &b.address,
        &b.zone,
        &b.spot_pool,
        &b.instance_type,
        &b.launch_type,
        &b.prefix,
    ] {
        put_text(out, text);
    }
    for n in [
        b.join_ms,
        b.alive_ms,
        b.host,
        b.disk_total,
        b.disk_free,
        b.fill_rate,
        b.holds,
        b.debt,
        b.epoch,
        b.domains,
    ] {
        out.extend_from_slice(&n.to_le_bytes());
    }
    out.extend_from_slice(&b.format_max.to_le_bytes());
    out.extend_from_slice(&b.committed.to_le_bytes());
    out.extend_from_slice(&[
        b.count,
        u8::from(b.quiet)
            | u8::from(b.reconciled) << 1
            | u8::from(b.drained) << 2
            | u8::from(b.can_found) << 3,
        b.signals,
    ]);
    out.extend_from_slice(&b.remaining.to_le_bytes());
    out.extend_from_slice(&b.era.to_le_bytes());
    let trips = trip_peers(disk, &b.round_trips);
    out.extend_from_slice(&(trips.len() as u16).to_le_bytes());
    for (peer, micros) in trips {
        out.extend_from_slice(&peer.to_le_bytes());
        out.extend_from_slice(&micros.to_le_bytes());
    }
    let cut = &b.unreachable[..b.unreachable.len().min(usize::from(u16::MAX))];
    out.extend_from_slice(&(cut.len() as u16).to_le_bytes());
    for peer in cut {
        out.extend_from_slice(&peer.to_le_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::testkit::*;

    fn heads(view: &View, members: &[u64]) -> Vec<Option<Entry>> {
        members.iter().map(|m| view.entry(*m).cloned()).collect()
    }

    fn rich(key: (u64, u32)) -> Body {
        let n = key.0 * 10 + u64::from(key.1);
        Body {
            debt: n * 7,
            count: n as u8,
            quiet: n.is_multiple_of(2),
            reconciled: n.is_multiple_of(3),
            unreachable: (0..n % 4).collect(),
            remaining: n as u32,
            zone: format!("z{n}"),
            ..body(n)
        }
    }

    fn random_view(me: u64, members: &[u64], rng: &mut Rng, make: fn((u64, u32)) -> Body) -> View {
        let mut view = View::new(me, 1, body(me), 0, 0);
        let updates: Vec<Update> = members
            .iter()
            .map(|m| Update {
                disk: *m,
                head: Head {
                    restart: rng.below(3),
                    heartbeat: rng.below(4),
                    state: [State::Alive, State::Dead, State::Left][rng.below(3) as usize],
                },
                body: {
                    let key = (rng.below(3), rng.below(3) as u32 + 1);
                    Some((key, make(key)))
                },
            })
            .collect();
        let seen: Vec<Update> = updates
            .iter()
            .map(|u| Update {
                head: Head {
                    state: State::Alive,
                    ..u.head
                },
                ..u.clone()
            })
            .collect();
        view.merge(&seen, 0, 0);
        view.merge(&updates, 0, 0);
        view
    }

    fn joined(mut into: View, from: &View) -> View {
        into.merge(&from.answer(&[]), 0, 0);
        into
    }

    #[test]
    fn merge_is_commutative_associative_and_idempotent() {
        let members = [11, 12, 13, 14, 15];
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        for _ in 0..200 {
            let a = random_view(1, &members, &mut rng, |k| body(k.0 * 10 + u64::from(k.1)));
            let b = random_view(2, &members, &mut rng, |k| body(k.0 * 10 + u64::from(k.1)));
            let c = random_view(3, &members, &mut rng, |k| body(k.0 * 10 + u64::from(k.1)));
            let ab = joined(a.clone(), &b);
            let ba = joined(b.clone(), &a);
            assert_eq!(heads(&ab, &members), heads(&ba, &members));
            let ab_c = joined(joined(a.clone(), &b), &c);
            let a_bc = joined(a.clone(), &joined(b.clone(), &c));
            assert_eq!(heads(&ab_c, &members), heads(&a_bc, &members));
            assert_eq!(
                heads(&joined(ab.clone(), &ab), &members),
                heads(&ab, &members)
            );
            for m in members {
                let best = [&a, &b, &c]
                    .iter()
                    .filter_map(|v| v.entry(m))
                    .map(|e| e.head)
                    .max();
                assert_eq!(ab_c.entry(m).map(|e| e.head), best);
            }
        }
    }

    #[test]
    fn merge_of_whole_entries_keeps_the_body_of_the_highest_body_key() {
        let members = [11, 12, 13, 14, 15];
        let mut rng = Rng(0x2545_f491_4f6c_dd1d);
        for _ in 0..200 {
            let a = random_view(1, &members, &mut rng, rich);
            let b = random_view(2, &members, &mut rng, rich);
            let c = random_view(3, &members, &mut rng, rich);
            let ab = joined(a.clone(), &b);
            assert_eq!(
                heads(&ab, &members),
                heads(&joined(b.clone(), &a), &members)
            );
            let ab_c = joined(ab.clone(), &c);
            let a_bc = joined(a.clone(), &joined(b.clone(), &c));
            assert_eq!(heads(&ab_c, &members), heads(&a_bc, &members));
            assert_eq!(
                heads(&joined(ab_c.clone(), &ab_c), &members),
                heads(&ab_c, &members)
            );
            for m in members {
                let best = [&a, &b, &c]
                    .iter()
                    .filter_map(|v| v.entry(m))
                    .map(|e| (e.body_key, e.body.clone()))
                    .max_by_key(|(key, _)| *key);
                assert_eq!(ab_c.entry(m).map(|e| (e.body_key, e.body.clone())), best);
            }
        }
    }

    #[test]
    fn a_restart_wins_over_the_dead_and_left_entry_of_the_earlier_run() {
        let mut rng = Rng(77);
        for _ in 0..1000 {
            let restart = rng.below(1 << 40);
            let state = if rng.below(2) == 0 {
                State::Dead
            } else {
                State::Left
            };
            let mut view = View::new(1, 1, body(1), 0, 0);
            view.merge(&[update(9, restart, rng.next() >> 1, state, body(9))], 0, 0);
            view.merge(&[update(9, restart + 1, 0, State::Alive, body(9))], 1, 1);
            assert_eq!(
                view.entry(9).map(|e| (e.head.restart, e.head.state)),
                Some((restart + 1, State::Alive))
            );
            assert!(!view.absent(9));
        }
    }

    #[test]
    fn a_digest_gets_the_newer_restart_and_the_reverse_sends_nothing() {
        let mut p = View::new(1, 1, body(1), 0, 0);
        p.merge(&[update(9, 2, 0, State::Alive, body(9))], 0, 0);
        let mut q = View::new(2, 1, body(2), 0, 0);
        q.merge(&[update(9, 1, 900, State::Dead, body(9))], 0, 0);
        let to_q = p.answer(&q.digest());
        let nine = to_q
            .iter()
            .find(|u| u.disk == 9)
            .expect("p sends its entry for 9");
        assert_eq!(
            nine.head,
            Head {
                restart: 2,
                heartbeat: 0,
                state: State::Alive
            }
        );
        assert!(nine.body.is_some());
        assert!(q.answer(&p.digest()).iter().all(|u| u.disk != 9));
    }

    #[test]
    fn a_heartbeat_change_sends_only_the_digest_line() {
        let mut p = View::new(1, 1, body(1), 0, 0);
        let mut q = View::new(2, 1, body(2), 0, 0);
        q.merge(&p.answer(&q.digest()), 0, 0);
        assert!(p.answer(&q.digest()).iter().all(|u| u.disk != 1));
        p.tick(HEARTBEAT_MS, HEARTBEAT_MS, &[]);
        let updates: Vec<Update> = p
            .answer(&q.digest())
            .into_iter()
            .filter(|u| u.disk == 1)
            .collect();
        assert_eq!(updates.len(), 1);
        assert!(updates[0].body.is_none());
        assert_eq!(encode_updates(&updates).len(), LINE_BYTES);
        assert_eq!(encode_digest(&q.digest()).len(), 2 * LINE_BYTES);
    }

    #[test]
    fn the_body_version_rises_at_most_once_in_each_sample() {
        let mut view = View::new(1, 1, body(1), 0, 0);
        let mut last = (view.own().body_key.1, 0);
        for step in 1..=600u64 {
            let now = step * 100;
            view.set_body(body(1 + step), now, now);
            view.tick(now, now, &[]);
            let version = view.own().body_key.1;
            if version != last.0 {
                assert!(
                    now >= last.1 + SAMPLE_MS,
                    "version {version} at {now} after {}",
                    last.1
                );
                last = (version, now);
            }
        }
        assert!(last.0 >= 10);
        let before = view.own().body_key.1;
        let mut signalled = view.own().body.clone();
        signalled.signals = SPOT;
        view.set_body(signalled, last.1 + 1, last.1 + 1);
        assert_eq!(view.own().body_key.1, before + 1);
    }

    #[test]
    fn a_reconciled_or_unreachable_change_publishes_inside_the_sample() {
        let mut view = View::new(1, 1, body(1), 0, 0);
        let first = view.own().body_key.1;
        let mut next = view.own().body.clone();
        next.reconciled = true;
        view.set_body(next.clone(), 1, 1);
        assert_eq!(view.own().body_key.1, first + 1);
        next.unreachable = vec![9];
        view.set_body(next.clone(), 2, 2);
        assert_eq!(view.own().body_key.1, first + 2);
        next.debt = 1;
        view.set_body(next, 3, 3);
        assert_eq!(view.own().body_key.1, first + 2);
    }

    #[test]
    fn different_cluster_marks_give_different_hashes() {
        let a = BTreeMap::from([(1, 10), (2, 20)]);
        let b = BTreeMap::from([(1, 10), (2, 21)]);
        let c = BTreeMap::from([(1, 10), (3, 20)]);
        assert_ne!(marks_hash(&a), marks_hash(&b));
        assert_ne!(marks_hash(&a), marks_hash(&c));
        assert_eq!(marks_hash(&a), marks_hash(&a.clone()));
    }

    #[test]
    fn absent_is_dead_or_left_and_never_alive() {
        let mut view = View::new(1, 1, body(1), 0, 0);
        view.merge(
            &[
                update(2, 1, 1, State::Alive, body(2)),
                update(3, 1, 1, State::Dead, body(3)),
                update(4, 1, 1, State::Left, body(4)),
            ],
            0,
            0,
        );
        assert!(!view.absent(2));
        assert!(view.absent(3));
        assert!(view.absent(4));
        assert!(!view.absent(1));
    }

    #[test]
    fn a_silent_member_is_dead_after_the_fail_time() {
        let mut view = view(1, &[2, 3], 0, 0);
        for step in 1..=FAIL_MS / 500 {
            let now = step * 500;
            beat(&mut view, &[2], step + 1, now, now);
            view.tick(now, now, &[]);
        }
        assert!(!view.absent(2));
        assert!(view.absent(3));
        assert_eq!(view.losses(0), 1);
    }

    #[test]
    fn a_member_that_holds_no_data_and_dies_is_a_loss() {
        let mut view = View::new(1, 1, body(1), 0, 0);
        view.merge(&[update(2, 1, 1, State::Alive, body(0))], 0, 0);
        view.tick(FAIL_MS + 1, FAIL_MS + 1, &[]);
        assert!(view.absent(2));
        assert_eq!(view.losses(0), 1);
    }

    #[test]
    fn a_flap_inside_the_join_window_is_no_loss() {
        let mut view = view(1, &[2], 0, 0);
        view.tick(FAIL_MS, FAIL_MS, &[]);
        assert_eq!(view.losses(0), 1);
        let mut alive = view.entry(2).expect("member").head;
        alive.state = State::Alive;
        alive.heartbeat += 1;
        view.merge(
            &[Update {
                disk: 2,
                head: alive,
                body: None,
            }],
            FAIL_MS + 2,
            FAIL_MS + 2,
        );
        assert!(!view.absent(2));
        assert_eq!(view.losses(0), 0);
    }

    fn mark(view: &mut View, disk: u64, state: State, now_ms: u64, run_ms: u64) {
        let mut head = view.entry(disk).expect("member").head;
        if state == State::Alive {
            head.heartbeat += 1;
        }
        head.state = state;
        view.merge(
            &[Update {
                disk,
                head,
                body: None,
            }],
            now_ms,
            run_ms,
        );
    }

    #[test]
    fn a_pruned_absent_entry_does_not_come_back_from_a_later_peer() {
        let mut x = view(1, &[2, 9], 0, 0);
        let mut y = view(2, &[1, 9], 0, 0);
        mark(&mut x, 9, State::Dead, 0, 0);
        mark(&mut y, 9, State::Dead, 0, 1_000);
        x.tick(0, TOMBSTONE_HORIZON, &[]);
        assert!(x.entry(9).is_none());
        x.merge(&y.answer(&x.digest()), 0, TOMBSTONE_HORIZON);
        assert!(x.entry(9).is_none());
    }

    #[test]
    fn an_absent_entry_heard_second_hand_is_not_passed_on() {
        let mut y = view(2, &[9], 0, 0);
        mark(&mut y, 9, State::Dead, 0, 0);
        let mut joiner = View::new(3, 1, body(3), 0, 0);
        joiner.merge(&y.answer(&joiner.digest()), 0, 0);
        assert!(joiner.entry(9).is_some_and(|e| e.head.state == State::Dead));
        let mut next = View::new(4, 1, body(4), 0, 0);
        next.merge(&joiner.answer(&next.digest()), 0, 0);
        assert!(next.entry(9).is_none());
        next.merge(&y.answer(&next.digest()), 0, 0);
        assert!(next.entry(9).is_some());
    }

    #[test]
    fn a_flap_of_a_later_absence_keeps_the_loss_of_an_earlier_one() {
        let mut view = view(1, &[2], 0, 0);
        view.tick(FAIL_MS, FAIL_MS, &[]);
        mark(
            &mut view,
            2,
            State::Alive,
            FAIL_MS + 1,
            FAIL_MS + JOIN_MS + 1,
        );
        assert_eq!(view.losses(0), 1);
        view.tick(2 * FAIL_MS + 1, 5 * 3_600_000, &[]);
        assert!(view.absent(2));
        mark(
            &mut view,
            2,
            State::Alive,
            2 * FAIL_MS + 2,
            5 * 3_600_000 + 2_000,
        );
        assert!(!view.absent(2));
        assert_eq!(view.losses(0), 1);
    }

    #[test]
    fn a_second_absence_in_one_run_restarts_the_quiet_period() {
        let hour = 3_600_000;
        let mut view = view(1, &[2], 0, 0);
        view.tick(FAIL_MS, FAIL_MS, &[]);
        mark(
            &mut view,
            2,
            State::Alive,
            FAIL_MS + 1,
            FAIL_MS + JOIN_MS + 1,
        );
        view.tick(2 * FAIL_MS + 1, 5 * hour, &[]);
        assert!(view.absent(2));
        assert_eq!(view.losses(0), 1);
        assert!(!view.quiet(6 * hour + 4_000, 6 * hour));
        assert!(!view.quiet(11 * hour - 1, 6 * hour));
        assert!(view.quiet(11 * hour + 1, 6 * hour));
    }

    #[test]
    fn a_death_after_a_signal_is_a_co_loss() {
        let mut view = view(1, &[2, 3], 0, 0);
        for restart in 1..=2 {
            let (now, run) = (restart * 10 * FAIL_MS, restart * 3_600_000);
            let signalled: Vec<Update> = [2, 3]
                .into_iter()
                .map(|disk| Update {
                    disk,
                    head: Head {
                        restart,
                        heartbeat: 100,
                        state: State::Alive,
                    },
                    body: Some((
                        (restart, 2),
                        Body {
                            signals: SPOT,
                            ..body(disk)
                        },
                    )),
                })
                .collect();
            view.merge(&signalled, now, run);
            view.tick(now + FAIL_MS, run + FAIL_MS, &[]);
            assert!(view.absent(2) && view.absent(3));
        }
        assert_eq!(view.losses(0), 4);
        assert_eq!(view.co_losses().get(&(2, 3)), Some(&2));
    }

    #[test]
    fn a_drained_disk_that_comes_back_counts_its_next_death() {
        let hour = 3_600_000;
        let mut view = view(1, &[9], 0, 0);
        view.drained(9, 0, 0);
        view.tick(FAIL_MS, FAIL_MS, &[]);
        assert!(view.absent(9));
        assert_eq!(view.losses(0), 0);
        view.merge(&[update(9, 2, 0, State::Alive, body(9))], FAIL_MS + 1, hour);
        view.tick(2 * FAIL_MS + 1, hour + 1, &[]);
        assert!(view.absent(9));
        assert_eq!(view.losses(0), 1);
    }

    fn holder(signals: u8, remaining: u32) -> Body {
        Body {
            holds: 1 << 30,
            signals,
            remaining,
            ..Body::default()
        }
    }

    #[test]
    fn a_member_first_seen_absent_gives_no_loss() {
        let run = 5 * 3_600_000;
        let mut y = view(2, &[], 0, 0);
        y.merge(
            &[
                update(9, 1, 1, State::Alive, holder(SPOT, 0)),
                update(10, 1, 1, State::Alive, holder(0, 3)),
            ],
            0,
            0,
        );
        y.merge(&[update(10, 1, 1, State::Left, holder(0, 3))], 0, 0);
        y.tick(FAIL_MS, FAIL_MS, &[]);
        assert!(y.absent(9) && y.absent(10));
        let mut x = view(3, &[], 0, run);
        x.merge(&y.answer(&x.digest()), 0, run);
        assert!(x.absent(9) && x.absent(10));
        assert_eq!(x.losses(0), 0);
        x.tick(0, run + STOP_BUDGET_MS + JOIN_MS, &[]);
        assert_eq!(x.losses(0), 0);
        x.merge(&[update(10, 2, 1, State::Dead, holder(0, 0))], 0, run + 1);
        assert_eq!(x.losses(0), 0);
    }

    #[test]
    fn a_member_first_seen_absent_and_back_on_a_new_run_adds_no_loss() {
        let mut x = view(1, &[5], 0, 0);
        x.tick(FAIL_MS, 10_000, &[]);
        assert!(x.absent(5));
        x.merge(
            &[update(9, 1, 1, State::Dead, body(9))],
            FAIL_MS + 1,
            20_000,
        );
        x.merge(
            &[update(9, 2, 1, State::Alive, body(9))],
            FAIL_MS + 2,
            30_000,
        );
        assert!(!x.absent(9));
        assert_eq!(x.losses(0), 1);
    }

    #[test]
    fn a_restarted_node_keeps_its_pruned_keys() {
        let mut x = view(1, &[2, 9], 0, 0);
        let mut y = view(2, &[1, 9], 0, 0);
        mark(&mut x, 9, State::Dead, 0, 0);
        mark(&mut y, 9, State::Dead, 0, 1_000);
        x.tick(0, TOMBSTONE_HORIZON, &[]);
        assert!(x.entry(9).is_none());
        let run = TOMBSTONE_HORIZON + 1;
        let (mut x, _) =
            View::restored(1, 2, body(1), 0, run, &x.durable(0)).expect("the state decodes");
        x.merge(&y.answer(&x.digest()), 0, run);
        assert!(x.entry(9).is_none());
    }

    #[test]
    fn a_restarted_node_keeps_its_member_state() {
        let hour = 3_600_000;
        let mut x = view(1, &[2, 3, 4, 5], 0, 0);
        beat(&mut x, &[2], 2, FAIL_MS, 10_000);
        x.tick(FAIL_MS, 10_000, &[]);
        let state = x.durable(0);
        let (mut x, _) = View::restored(1, 2, body(1), 0, hour, &state).expect("the state decodes");
        assert!(x.absent(3) && x.absent(4) && x.absent(5));
        beat(&mut x, &[2], 3, FAIL_MS, hour + FAIL_MS);
        x.tick(FAIL_MS, hour + FAIL_MS, &[]);
        assert!(!x.absent(2) && x.absent(3));
        assert_eq!(x.losses(0), 3);
        assert!(View::restored(1, 2, body(1), 0, hour, &state[..state.len() - 1]).is_none());
        let mut bad = state.clone();
        bad[4 + 40] |= 0x40;
        assert!(View::restored(1, 2, body(1), 0, hour, &bad).is_none());
    }

    #[test]
    fn a_state_with_an_absent_member_and_no_absent_time_or_a_disk_twice_does_not_decode() {
        let mut x = view(1, &[2, 3], 0, 0);
        x.tick(FAIL_MS, FAIL_MS, &[]);
        let state = x.durable(0);
        assert!(View::restored(1, 2, body(1), 0, FAIL_MS, &state).is_some());
        let mut no_time = state.clone();
        no_time[4 + 41 + 40] &= !8;
        assert!(View::restored(1, 2, body(1), 0, FAIL_MS, &no_time).is_none());
        let mut twice = state.clone();
        twice[4 + 82..4 + 90].copy_from_slice(&2u64.to_le_bytes());
        assert!(View::restored(1, 2, body(1), 0, FAIL_MS, &twice).is_none());
    }

    #[test]
    fn a_drained_disk_is_forgotten_with_its_entry() {
        let mut view = view(1, &[9], 0, 0);
        view.drained(9, 0, 0);
        view.tick(FAIL_MS, FAIL_MS, &[]);
        view.tick(FAIL_MS, FAIL_MS + TOMBSTONE_HORIZON, &[]);
        assert!(view.entry(9).is_none());
        assert!(!view.is_drained(9));
        let stale = Body {
            drained: true,
            ..body(9)
        };
        view.merge(
            &[update(9, 1, 9, State::Left, stale)],
            0,
            FAIL_MS + TOMBSTONE_HORIZON + 1,
        );
        assert!(view.entry(9).is_none());
    }

    #[test]
    fn a_list_member_stays_in_the_view_past_the_horizon() {
        let mut x = view(1, &[2, 3], 0, 0);
        x.tick(FAIL_MS, 1_000, &[]);
        assert!(x.absent(2) && x.absent(3));
        x.tick(FAIL_MS, 1_000 + TOMBSTONE_HORIZON, &[1, 2]);
        assert!(x.entry(2).is_some());
        assert!(x.entry(3).is_none());
        x.tick(FAIL_MS, 1_000 + TOMBSTONE_HORIZON, &[1]);
        assert!(x.entry(2).is_none());
    }
}
