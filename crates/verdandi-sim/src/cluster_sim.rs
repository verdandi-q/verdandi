use std::collections::{BTreeMap, BTreeSet};

use rand::seq::{IndexedRandom, SliceRandom};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use verdandi_core::cluster::controller::{self, Controller, Floor, QUIET_PERIOD, START, refuse};
use verdandi_core::cluster::domains::{self, Domains};
use verdandi_core::cluster::placement::{self, Candidate, Groups, HolderSet};
use verdandi_core::cluster::repair::{self, DIGEST_MS, Held, Limiter, Step, Want};
use verdandi_core::cluster::view::{
    Body, FAIL_MS, GOSSIP_MS, HEARTBEAT_MS, JOIN_MS, LIFECYCLE, REBALANCE, SAMPLE_MS, SIGTERM,
    SPOT, STOP_BUDGET_MS, State, TOMBSTONE_HORIZON, Update, View,
};

pub const STEP_MS: u64 = HEARTBEAT_MS;
pub const SEGMENT_BYTES: u64 = 64 << 20;
pub const HOUR_MS: u64 = 3_600_000;
const FANOUT: usize = 3;
const HELLO_MS: u64 = 10_000;
const FLAP_MS: u64 = 13_500;
const SLACK_MS: u64 = FAIL_MS + 2 * SAMPLE_MS + GOSSIP_MS;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cause {
    Rise,
    Quiet,
    Merge,
    Drain,
    AcceptLoss,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Limit {
    FirstSeen,
    Blackout,
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
    pub limit: Option<Limit>,
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

struct Disk {
    restart: u64,
    floor: Floor,
    committed: u16,
    alive_ms: u64,
    zone: String,
    join_ms: u64,
    member: Option<Vec<u8>>,
    groups: Option<Groups>,
}

struct Node {
    view: View,
    ctl: Controller,
    domains: Option<Domains>,
    format_max: u16,
    signals: u8,
    phase_ms: u64,
    boot_run_ms: u64,
    groups: Option<Groups>,
    stop_run_ms: Option<u64>,
}

pub struct Segment {
    pub writer: u64,
    pub holders: HolderSet,
    pub bytes: u64,
    pub format: u16,
    pub born_run_ms: u64,
    pub lost_run_ms: u64,
    pub marks: BTreeMap<u64, u64>,
}

pub struct World {
    pub rng: ChaCha8Rng,
    pub now_ms: u64,
    pub run_ms: u64,
    pub drop: f64,
    pub delay_ms: u64,
    pub cuts: BTreeSet<(u64, u64)>,
    pub held_repair: BTreeSet<u64>,
    pub accepted: BTreeSet<u64>,
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
    disks: BTreeMap<u64, Disk>,
    nodes: BTreeMap<u64, Node>,
    dead: BTreeMap<u64, u64>,
    met: BTreeMap<(u64, u64), (u64, u64)>,
    dark: BTreeSet<u64>,
    parted: BTreeSet<u64>,
    lit_run_ms: Option<u64>,
    sides: BTreeMap<u64, bool>,
    queue: Vec<(u64, u64, Vec<Update>)>,
    copying: BTreeMap<(u64, u64), u64>,
    sends: BTreeMap<u64, Limiter>,
    takes: BTreeMap<u64, Limiter>,
    queued: Vec<Job>,
    dirty: bool,
    next_disk: u64,
    next_segment: u64,
    changed_run_ms: u64,
}

fn pair(a: u64, b: u64) -> (u64, u64) {
    (a.min(b), a.max(b))
}

fn set(live: usize, mut joins: Vec<u64>, mut deaths: Vec<u64>) -> usize {
    joins.sort_unstable();
    deaths.sort_unstable();
    let mut replaced = 0;
    for join in joins {
        if deaths.get(replaced).is_some_and(|death| *death < join) {
            replaced += 1;
        }
    }
    live + deaths.len() - replaced
}

impl World {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: ChaCha8Rng::seed_from_u64(seed),
            now_ms: 0,
            run_ms: 0,
            drop: 0.0,
            delay_ms: 200,
            cuts: BTreeSet::new(),
            held_repair: BTreeSet::new(),
            accepted: BTreeSet::new(),
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
            disks: BTreeMap::new(),
            nodes: BTreeMap::new(),
            dead: BTreeMap::new(),
            met: BTreeMap::new(),
            dark: BTreeSet::new(),
            parted: BTreeSet::new(),
            lit_run_ms: None,
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
                groups: None,
            },
        );
        if !self.sides.is_empty() {
            let side = self.rng.random_bool(0.5);
            self.sides.insert(disk, side);
            self.parted.insert(disk);
            self.cut(&self.crossing());
        }
        self.boot(disk, format_max);
        disk
    }

    pub fn start(&mut self, n: usize, zones: usize, alive_ms: u64) -> Vec<u64> {
        let names = ["a", "b", "c", "d"];
        (0..n)
            .map(|i| {
                let zone = if zones == 0 { "" } else { names[i % zones] };
                self.create(zone, alive_ms, 1)
            })
            .collect()
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
            floor: d.floor,
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
        let groups = d.groups.clone();
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
                groups,
                stop_run_ms: None,
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
            .filter(|p| *p != disk && !self.cuts.contains(&pair(disk, *p)))
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
        let held: Vec<u64> = self
            .segments
            .iter()
            .filter(|(_, s)| s.marks.contains_key(&disk))
            .map(|(id, _)| *id)
            .collect();
        for id in held {
            let mark = self.fresh(id, disk);
            if let Some(seg) = self.segments.get_mut(&id) {
                seg.marks.insert(disk, mark);
            }
        }
        self.dead.remove(&disk);
        self.accepted.remove(&disk);
        if self.dark.contains(&disk) && self.lit_run_ms.is_none() {
            self.lit_run_ms = Some(self.run_ms);
        }
        true
    }

    fn record(&mut self, disk: u64, kind: LossKind) {
        let held = self
            .nodes
            .get(&disk)
            .is_some_and(|n| n.view.own().body.holds > 0);
        if held {
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
        self.dead.insert(disk, self.run_ms);
        if self.nodes.is_empty() {
            self.dark = self.disks.keys().chain(self.dead.keys()).copied().collect();
            self.lit_run_ms = None;
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
        }
    }

    fn leave(&mut self, disk: u64) {
        let floor = self.nodes.get(&disk).map_or(0, |n| n.ctl.floor.value);
        let remaining = self
            .segments
            .values()
            .filter(|s| s.marks.contains_key(&disk))
            .filter(|s| {
                s.marks
                    .keys()
                    .filter(|h| **h != disk && self.serving(**h))
                    .count()
                    < usize::from(floor)
            })
            .count();
        let drained = self.drained.contains(&disk);
        if remaining > 0 && !drained {
            self.record(disk, LossKind::Left);
        }
        let (now, run) = (self.now_ms, self.run_of(disk));
        let Some(node) = self.nodes.get_mut(&disk) else {
            return;
        };
        node.view.leave(remaining as u32, now, run);
        let own = node
            .view
            .answer(&[])
            .into_iter()
            .filter(|u| u.disk == disk)
            .collect::<Vec<_>>();
        for peer in self.running() {
            if peer != disk && !self.cuts.contains(&pair(disk, peer)) {
                let run = self.run_of(peer);
                if let Some(p) = self.nodes.get_mut(&peer) {
                    p.view.merge(&own, now, run);
                }
            }
        }
        self.down(disk);
        self.dead.remove(&disk);
    }

    pub fn drain(&mut self, disk: u64) {
        self.drained.insert(disk);
        for node in self.nodes.values_mut() {
            node.view.drained(disk);
        }
        self.stop(disk);
        self.lower(Cause::Drain);
    }

    pub fn accept_loss(&mut self, disk: u64) {
        self.accepted.insert(disk);
        self.lower(Cause::AcceptLoss);
    }

    fn lower(&mut self, cause: Cause) {
        let run = self.run_ms;
        let disks: Vec<u64> = self.running();
        for disk in disks {
            let Some(node) = self.nodes.get_mut(&disk) else {
                continue;
            };
            let from = node.ctl.floor;
            if let Some(to) = node.ctl.lower() {
                if let Some(d) = self.disks.get_mut(&disk) {
                    d.floor = to;
                }
                self.floor_changes.push(FloorChange {
                    run_ms: run,
                    disk,
                    from,
                    to,
                    cause,
                    debt_ok: true,
                    majority: true,
                    counts_ok: true,
                    limit: None,
                });
            }
        }
    }

    fn serving(&self, disk: u64) -> bool {
        self.nodes.get(&disk).is_some_and(|n| n.signals == 0)
    }

    fn run_of(&self, disk: u64) -> u64 {
        self.disks.get(&disk).map_or(0, |d| d.alive_ms)
    }

    pub fn holds(&self, disk: u64) -> u64 {
        self.segments
            .values()
            .filter(|s| s.marks.contains_key(&disk))
            .map(|s| s.bytes)
            .sum()
    }

    pub fn live_copies(&self, segment: &Segment) -> usize {
        segment
            .marks
            .keys()
            .filter(|h| self.nodes.contains_key(h))
            .count()
    }

    fn debt(&self, disk: u64, count: u8) -> u64 {
        self.segments
            .values()
            .filter(|s| s.marks.contains_key(&disk) && self.live_copies(s) < usize::from(count))
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
        self.segments.insert(
            id,
            Segment {
                writer,
                holders: HolderSet {
                    segment: id,
                    epoch: 0,
                    holders: holders.iter().copied().collect(),
                },
                bytes,
                format,
                born_run_ms: self.run_ms,
                lost_run_ms: 0,
                marks: stored.iter().map(|h| (*h, 0)).collect(),
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
        let groups = self.nodes[&writer].groups.as_ref()?;
        let holders: Vec<u64> = groups
            .new_holders(writer, self.next_segment, &[])
            .into_iter()
            .filter(|h| self.reach(writer, *h))
            .collect();
        let stored: Vec<u64> = holders
            .iter()
            .copied()
            .filter(|h| self.nodes.contains_key(h))
            .collect();
        if stored.len() < 2 {
            return None;
        }
        self.count_domains(writer);
        Some(self.store(writer, &holders, &stored, SEGMENT_BYTES))
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
        from == to
            || (!self.cuts.contains(&pair(from, to))
                && self.nodes.get(&from).is_some_and(|n| !n.view.absent(to)))
    }

    pub fn zone(&self, disk: u64) -> String {
        self.disks.get(&disk).map(|d| d.zone.clone()).unwrap_or_default()
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
        self.nodes
            .values()
            .filter_map(|n| n.groups.clone())
            .reduce(Groups::merge)
    }

    fn ring_of(&self, disk: u64) -> (Vec<u64>, BTreeMap<u64, u64>) {
        let node = &self.nodes[&disk];
        let domain: BTreeMap<u64, u64> = node
            .domains
            .as_ref()
            .map(|d| d.of.clone())
            .unwrap_or_default();
        let candidates: Vec<Candidate> = node
            .view
            .live()
            .filter(|(_, e)| e.body.signals == 0)
            .map(|(d, e)| Candidate {
                disk: d,
                domain: domain.get(&d).copied().unwrap_or(u64::MAX),
                join_ms: e.body.join_ms,
                heavy: None,
            })
            .collect();
        (placement::ring(&candidates, node.ctl.floor.value), domain)
    }

    fn next_groups(&self, disk: u64) -> Option<Groups> {
        let node = self.nodes.get(&disk)?;
        let (ring, domain) = self.ring_of(disk);
        placement::next(
            node.groups.as_ref(),
            &ring,
            &domain,
            &BTreeSet::new(),
            node.ctl.floor.value,
        )
    }

    fn give(&mut self, to: u64, groups: Option<Groups>) {
        let (Some(g), Some(node)) = (groups, self.nodes.get_mut(&to)) else {
            return;
        };
        node.groups = Some(match node.groups.take() {
            Some(own) => own.merge(g),
            None => g,
        });
    }

    fn write_groups(&mut self, disk: u64) {
        let Some(node) = self.nodes.get(&disk) else {
            return;
        };
        let started = self.run_of(disk) - node.boot_run_ms;
        if node.groups.is_none() && !placement::ready(node.domains.is_some(), started) {
            return;
        }
        let epoch = node.groups.as_ref().map_or(0, |g| g.epoch);
        let (ring, _) = self.ring_of(disk);
        if placement::writer(epoch, &ring) != Some(disk) {
            return;
        }
        let Some(made) = self.next_groups(disk) else {
            return;
        };
        for peer in self.running() {
            if peer == disk || !self.cuts.contains(&pair(disk, peer)) {
                self.give(peer, Some(made.clone()));
            }
        }
    }

    fn jobs(&self) -> (Vec<Job>, Vec<(u64, HolderSet)>) {
        let mut copies = Vec::new();
        let mut drops = Vec::new();
        let known: BTreeSet<u64> = self.disks.keys().copied().collect();
        let per: BTreeMap<u64, (&Groups, BTreeSet<u64>, Want)> = self
            .nodes
            .iter()
            .filter_map(|(n, node)| {
                let groups = node.groups.as_ref()?;
                let live = known.iter().copied().filter(|d| self.reach(*n, *d)).collect();
                let members: BTreeSet<u64> = groups.groups.iter().flatten().copied().collect();
                let want = Want {
                    target: usize::from(controller::target(groups.size, members.len(), self.headroom)),
                    cold: groups.size < node.ctl.floor.value.max(node.ctl.count),
                };
                Some((*n, (groups, live, want)))
            })
            .collect();
        for (id, seg) in &self.segments {
            if self.held_repair.contains(id) {
                continue;
            }
            let complete = |_: u64, d: u64| seg.marks.contains_key(&d);
            let mut senders: BTreeMap<Vec<u64>, Option<u64>> = BTreeMap::new();
            for n in seg.marks.keys() {
                let Some((groups, live_set, want)) = per.get(n) else {
                    continue;
                };
                let live = |d: u64| live_set.contains(&d);
                let ready: Vec<u64> = seg
                    .holders
                    .holders
                    .iter()
                    .copied()
                    .filter(|d| live(*d) && complete(*id, *d))
                    .collect();
                let sender = *senders
                    .entry(ready)
                    .or_insert_with_key(|r| repair::sender(&seg.holders, |d| r.contains(&d)));
                if sender != Some(*n) {
                    continue;
                }
                let held = Held {
                    writer: seg.writer,
                    bytes: seg.bytes,
                    sealed: true,
                    set: &seg.holders,
                };
                match repair::plan(&held, groups, live, complete, *want) {
                    Step::Copy { need, to } => copies.push(Job {
                        copies: seg.marks.keys().filter(|h| live(**h)).count(),
                        segment: *id,
                        from: *n,
                        need,
                        to,
                    }),
                    Step::Drop(keep) => drops.push((*n, keep)),
                    Step::Idle => {}
                }
            }
        }
        for (disk, node) in &self.nodes {
            if node.signals != 0 {
                copies.extend(self.drains(*disk, node));
            }
        }
        copies.sort_by_key(|j| (j.copies, j.segment));
        (copies, drops)
    }

    fn drains(&self, disk: u64, node: &Node) -> Vec<Job> {
        let Some(next) = self.next_groups(disk).or_else(|| node.groups.clone()) else {
            return Vec::new();
        };
        let serving = |d: u64| {
            d != disk
                && self.reach(disk, d)
                && node.view.entry(d).is_some_and(|e| e.body.signals == 0)
        };
        let complete = |s: u64, d: u64| self.segments.get(&s).is_some_and(|x| x.marks.contains_key(&d));
        let held: Vec<Held> = self
            .segments
            .iter()
            .filter(|(id, s)| {
                !self.held_repair.contains(id)
                    && s.marks.contains_key(&disk)
                    && s.holders.holders.contains(&disk)
            })
            .map(|(_, s)| Held {
                writer: s.writer,
                bytes: s.bytes,
                sealed: true,
                set: &s.holders,
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
        let records: BTreeSet<Option<&Groups>> =
            self.nodes.values().map(|n| n.groups.as_ref()).collect();
        let (copies, drops) = self.jobs();
        records.len() == 1
            && !records.contains(&None)
            && copies.is_empty()
            && drops.is_empty()
            && self.nodes.keys().all(|d| self.next_groups(*d).is_none())
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
        let copies = self.segments[&to.segment].marks.keys().copied().collect();
        let current = self
            .groups()
            .is_some_and(|g| g.current(&to.holders));
        self.holder_changes.push(HolderChange {
            run_ms: self.run_ms,
            from,
            to,
            copies,
            current,
        });
    }

    fn repair(&mut self) {
        let now = self.now_ms;
        if self.dirty || now.is_multiple_of(DIGEST_MS) {
            let (copies, drops) = self.jobs();
            self.queued = copies;
            self.dirty = !drops.is_empty();
            self.drop_copies(drops);
        }
        let queued = std::mem::take(&mut self.queued);
        self.copy(&queued, now);
        self.queued = queued;
    }

    fn drop_copies(&mut self, drops: Vec<(u64, HolderSet)>) {
        for (by, keep) in drops {
            let Some(seg) = self.segments.get_mut(&keep.segment) else {
                continue;
            };
            if keep.epoch != seg.holders.epoch + 1 {
                continue;
            }
            let from = std::mem::replace(&mut seg.holders, keep.clone());
            for h in from.holders.difference(&keep.holders) {
                if self.nodes.contains_key(h) && !self.cuts.contains(&pair(by, *h)) {
                    seg.marks.remove(h);
                }
            }
            self.log(from, keep);
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
                .filter(|t| self.copying.contains_key(&(job.segment, *t)) && self.open(job.from, *t))
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
                let (Some(out), Some(inn)) = (self.sends.get_mut(&job.from), self.takes.get_mut(&to))
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
                let mark = self.fresh(job.segment, to);
                let Some(seg) = self.segments.get_mut(&job.segment) else {
                    continue;
                };
                seg.marks.insert(to, mark);
                if !seg.holders.holders.contains(&to) {
                    let mut holders = seg.holders.holders.clone();
                    holders.insert(to);
                    let next = seg.holders.with(holders);
                    let from = std::mem::replace(&mut seg.holders, next.clone());
                    self.log(from, next);
                }
            }
        }
    }

    fn open(&self, from: u64, to: u64) -> bool {
        self.nodes.get(&to).is_some_and(|n| n.signals == 0) && !self.cuts.contains(&pair(from, to))
    }

    fn fresh(&self, segment: u64, to: u64) -> u64 {
        self.segments[&segment]
            .marks
            .iter()
            .filter(|(h, _)| self.nodes.contains_key(h) && !self.cuts.contains(&pair(**h, to)))
            .map(|(_, m)| *m)
            .max()
            .unwrap_or(0)
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
                || self.drains(disk, node).is_empty();
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
        let running: BTreeSet<u64> = self.nodes.keys().copied().collect();
        let Some(segment) = self.segments.get_mut(&id) else {
            return;
        };
        let next = segment.marks.values().max().copied().unwrap_or(0) + 1;
        for (holder, mark) in segment.marks.iter_mut() {
            if running.contains(holder) {
                *mark = next;
            }
        }
        self.settled.insert(id, next);
    }

    pub fn lost(&self) -> usize {
        self.acked
            .iter()
            .filter(|s| self.segments.get(s).is_none_or(|seg| seg.marks.is_empty()))
            .count()
    }

    pub fn lost_settles(&self) -> usize {
        self.settled
            .iter()
            .filter(|(s, n)| {
                self.segments
                    .get(s)
                    .and_then(|seg| {
                        seg.marks
                            .iter()
                            .filter(|(h, _)| self.nodes.contains_key(h))
                            .map(|(_, m)| *m)
                            .max()
                    })
                    .is_some_and(|m| m < **n)
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
                || !s.marks.keys().any(|h| side.contains(h))
                || live >= usize::from(count)
        })
    }

    pub fn side(&self, disk: u64) -> BTreeSet<u64> {
        let mut seen = BTreeSet::from([disk]);
        let mut todo = vec![disk];
        while let Some(a) = todo.pop() {
            for b in self.nodes.keys() {
                if !self.cuts.contains(&pair(a, *b)) && seen.insert(*b) {
                    todo.push(*b);
                }
            }
        }
        seen
    }

    fn created(&self, disk: u64) -> u64 {
        self.boots
            .iter()
            .filter(|(d, _)| *d == disk)
            .map(|(_, r)| *r)
            .min()
            .unwrap_or(0)
    }

    fn deaths(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.dead
            .iter()
            .filter(|(d, at)| {
                !self.accepted.contains(d)
                    && !self.drained.contains(d)
                    && self.run_ms < **at + TOMBSTONE_HORIZON
            })
            .map(|(d, at)| (*d, *at))
    }

    pub fn members(&self) -> usize {
        let joins = self.nodes.keys().map(|d| self.created(*d)).collect();
        set(
            self.nodes.len(),
            joins,
            self.deaths().map(|(_, at)| at).collect(),
        )
    }

    fn first_seen_members(&self, by: u64, side: &BTreeSet<u64>) -> usize {
        let met = |d: &u64| self.met.get(&(by, *d)).copied();
        let born = met(&by).map_or(0, |m| m.0);
        let unseen = |d: &u64| {
            self.parted.contains(d) && side.iter().all(|s| !self.met.contains_key(&(*s, *d)))
        };
        let joins = side
            .iter()
            .map(|d| self.created(*d).max(met(d).map_or(0, |m| m.0)))
            .collect();
        let mut deaths: Vec<u64> = self
            .deaths()
            .filter(|(d, _)| !unseen(d))
            .map(|(_, at)| at.max(born))
            .collect();
        deaths.extend(
            self.nodes
                .keys()
                .filter(|d| !side.contains(d) && !unseen(d))
                .map(|d| met(d).map_or(born, |m| m.1.max(born))),
        );
        set(side.len(), joins, deaths)
    }

    fn blackout_members(&self) -> Option<usize> {
        let open =
            !self.dark.is_empty() && self.lit_run_ms.is_none_or(|t| self.run_ms <= t + SLACK_MS);
        open.then(|| {
            let joins = self.nodes.keys().map(|d| self.created(*d)).collect();
            let deaths = self
                .deaths()
                .filter(|(d, _)| !self.dark.contains(d))
                .map(|(_, at)| at)
                .collect();
            set(self.nodes.len(), joins, deaths)
        })
    }

    fn majority(&self, by: u64, side: &BTreeSet<u64>) -> (bool, Option<Limit>) {
        let holds = |set: usize| 2 * side.len() > set;
        if holds(self.members()) {
            (true, None)
        } else if holds(self.first_seen_members(by, side)) {
            (true, Some(Limit::FirstSeen))
        } else if self.blackout_members().is_some_and(holds) {
            (true, Some(Limit::Blackout))
        } else {
            (false, None)
        }
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
                    Cause::Drain | Cause::AcceptLoss => next,
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
        let run = self.run_ms;
        for a in &running {
            for b in self.side(*a) {
                self.met.entry((*a, b)).or_insert((run, run)).1 = run;
            }
        }
        let now = self.now_ms;
        let due: Vec<(u64, u64, Vec<Update>)> = {
            let (due, later) = std::mem::take(&mut self.queue)
                .into_iter()
                .partition(|m| m.0 <= now);
            self.queue = later;
            due
        };
        for (_, to, updates) in due {
            let run = self.run_of(to);
            if let Some(node) = self.nodes.get_mut(&to) {
                node.view.merge(&updates, now, run);
            }
        }
        for disk in &running {
            let run = self.run_of(*disk);
            if let Some(node) = self.nodes.get_mut(disk) {
                node.view.tick(now, run);
            }
        }
        if now.is_multiple_of(GOSSIP_MS) {
            for disk in &running {
                self.gossip(*disk);
            }
        }
        if now.is_multiple_of(HELLO_MS) && running.len() > 1 {
            for disk in &running {
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
            return;
        }
        let (Some(a), Some(b)) = (self.nodes.get(&from), self.nodes.get(&to)) else {
            return;
        };
        let to_a = b.view.answer(&a.view.digest());
        let to_b = a.view.answer(&b.view.digest());
        let (ga, gb) = (a.groups.clone(), b.groups.clone());
        self.give(from, gb);
        self.give(to, ga);
        for (target, updates) in [(from, to_a), (to, to_b)] {
            if self.rng.random_bool(self.drop) {
                continue;
            }
            let at = self.now_ms + self.rng.random_range(0..=self.delay_ms);
            self.queue.push((at, target, updates));
        }
    }

    fn hello(&mut self, from: u64, to: u64) {
        if self.cuts.contains(&pair(from, to)) {
            return;
        }
        let (Some(a), Some(b)) = (self.nodes.get(&from), self.nodes.get(&to)) else {
            return;
        };
        let replies = [
            (from, b.view.answer(&a.view.digest())),
            (to, a.view.answer(&b.view.digest())),
        ];
        let (ga, gb) = (a.groups.clone(), b.groups.clone());
        self.give(from, gb);
        self.give(to, ga);
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
        let Some(node) = self.nodes.get_mut(&disk) else {
            return;
        };
        let live: Vec<(u64, Body)> = node.view.live().map(|(d, e)| (d, e.body.clone())).collect();
        let refs: Vec<(u64, &Body)> = live.iter().map(|(d, b)| (*d, b)).collect();
        let result = domains::infer(&refs, &node.view.co_losses());
        let hash = result.hash;
        let reports: Vec<u64> = live.iter().map(|(_, b)| b.domains).collect();
        node.domains = domains::agree(result, &reports, node.domains.take());
        let d = node.domains.as_ref().map_or(1, Domains::count);
        let before = node.ctl.count;
        let count = node.ctl.sample(&node.view, d, self.headroom, run).count;
        let debt = self.debt(disk, count);
        let Some(node) = self.nodes.get_mut(&disk) else {
            return;
        };
        let mut body = node.view.own().body.clone();
        body.holds = holds;
        body.debt = debt;
        body.count = count;
        body.members = node.view.member_set(&self.accepted) as u64;
        body.floor = node.ctl.floor;
        body.quiet = node.view.quiet(run, QUIET_PERIOD);
        body.domains = hash;
        body.alive_ms = run;
        body.committed = node.ctl.committed;
        body.signals = node.signals;
        node.view.set_body(body, now, run);
        let from = node.ctl.floor;
        let merged = node
            .view
            .live()
            .map(|(_, e)| e.body.floor)
            .fold(from, Floor::max);
        node.ctl.decide(&node.view, &self.accepted, run);
        let to = node.ctl.floor;
        let committed = node.ctl.commit(&node.view, &self.accepted, run);
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
        let steps = [
            (from, merged, Cause::Merge),
            (
                merged,
                to,
                if to.epoch > merged.epoch {
                    Cause::Quiet
                } else {
                    Cause::Rise
                },
            ),
        ];
        for (from, to, cause) in steps.into_iter().filter(|(from, to, _)| from != to) {
            let settled = self.run_ms >= self.changed_run_ms + SLACK_MS;
            let side = self.side(disk);
            let (majority, limit) = if settled {
                self.majority(disk, &side)
            } else {
                (true, None)
            };
            self.floor_changes.push(FloorChange {
                run_ms: self.run_ms,
                disk,
                from,
                to,
                cause,
                debt_ok: self.debt_truth(to.value, &side),
                majority,
                counts_ok: !settled || self.counts_ok(&side, to.value),
                limit,
            });
        }
        if let Some(d) = self.disks.get_mut(&disk) {
            d.floor = to;
            d.member = Some(member);
            d.groups = self.nodes.get(&disk).and_then(|n| n.groups.clone());
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
