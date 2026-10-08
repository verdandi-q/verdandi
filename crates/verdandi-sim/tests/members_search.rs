use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use verdandi_sim::cluster_sim::{HOUR_MS, SEGMENT_BYTES, STEP_MS, World};

const GAP_MS: u64 = 8_000;
const GRACE_MS: u64 = 2_500;
const SETTLE_MS: u64 = 30_000;
const TAIL_MS: u64 = 120_000;
const MAX_JOINS: usize = 2;
const MAX_DRAINS: usize = 2;
const OLD: u64 = 25 * HOUR_MS;
const SEED: u64 = 1;
const ZONES: [&str; 3] = ["a", "b", "c"];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ev {
    Kill(u64),
    Start(u64),
    Join(u8),
    Cut(u32),
    CutOneWay(u64, u64),
    Heal,
    Accept(u64),
    Drain(u64),
    Restart(u64),
    Wait,
}

#[derive(Clone)]
struct Abs {
    up: BTreeSet<u64>,
    dead: BTreeSet<u64>,
    draining: BTreeSet<u64>,
    side: BTreeMap<u64, u8>,
    cut: bool,
    oneway: Option<(u64, u64)>,
    next: u64,
    joins: usize,
}

impl Abs {
    fn new(n: u64) -> Self {
        Abs {
            up: (1..=n).collect(),
            dead: BTreeSet::new(),
            draining: BTreeSet::new(),
            side: (1..=n).map(|d| (d, 0)).collect(),
            cut: false,
            oneway: None,
            next: n + 1,
            joins: 0,
        }
    }

    fn moves(&self) -> Vec<Ev> {
        let mut out = Vec::new();
        let active: Vec<u64> = self.up.difference(&self.draining).copied().collect();
        if active.len() >= 2 {
            out.extend(active.iter().map(|d| Ev::Kill(*d)));
        }
        out.extend(active.iter().map(|d| Ev::Restart(*d)));
        if !self.cut && active.len() > 2 && self.draining.len() < MAX_DRAINS {
            out.extend(active.iter().map(|d| Ev::Drain(*d)));
        }
        out.extend(self.dead.iter().map(|d| Ev::Start(*d)));
        out.extend(self.dead.iter().map(|d| Ev::Accept(*d)));
        if self.joins < MAX_JOINS {
            for s in 0..2u8 {
                if (s == 0 || self.cut) && active.iter().any(|d| self.side[d] == s) {
                    out.push(Ev::Join(s));
                }
            }
        }
        if self.cut || self.oneway.is_some() {
            out.push(Ev::Heal);
        } else if self.up.len() >= 2 {
            let lowest = *self.up.iter().next().expect("a node is up");
            let others: Vec<u64> = self.up.iter().copied().filter(|d| *d != lowest).collect();
            for bits in 1u32..(1 << others.len()) {
                let mask = others
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| bits & (1 << i) != 0)
                    .fold(0u32, |m, (_, d)| m | (1 << d));
                out.push(Ev::Cut(mask));
            }
            for a in &self.up {
                for b in self.up.iter().filter(|b| *b != a) {
                    out.push(Ev::CutOneWay(*a, *b));
                }
            }
        }
        out.push(Ev::Wait);
        out
    }

    fn apply(&mut self, ev: Ev) {
        match ev {
            Ev::Kill(d) => {
                self.up.remove(&d);
                self.draining.remove(&d);
                self.dead.insert(d);
            }
            Ev::Start(d) => {
                self.dead.remove(&d);
                self.up.insert(d);
            }
            Ev::Accept(d) => {
                self.dead.remove(&d);
            }
            Ev::Drain(d) => {
                self.draining.insert(d);
            }
            Ev::Restart(_) | Ev::Wait => {}
            Ev::Join(s) => {
                self.up.insert(self.next);
                self.side.insert(self.next, s);
                self.next += 1;
                self.joins += 1;
            }
            Ev::Cut(mask) => {
                for (d, s) in self.side.iter_mut() {
                    *s = u8::from(mask & (1 << d) != 0);
                }
                self.cut = true;
            }
            Ev::CutOneWay(a, b) => self.oneway = Some((a, b)),
            Ev::Heal => {
                for s in self.side.values_mut() {
                    *s = 0;
                }
                self.cut = false;
                self.oneway = None;
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct Hits {
    split: bool,
    double_delete: bool,
    double_lead: bool,
    double_groups: bool,
    two_lists: bool,
    missed_record: bool,
    stale_serve: bool,
    stuck: bool,
    lost: bool,
    tail: bool,
}

impl Hits {
    fn any(&self) -> bool {
        self.split
            || self.double_delete
            || self.double_lead
            || self.double_groups
            || self.two_lists
            || self.missed_record
            || self.stale_serve
            || self.stuck
            || self.lost
    }
}

struct Run {
    w: World,
    abs: Abs,
    topo_ms: u64,
    e0: Option<u64>,
    loss: bool,
    accepted: bool,
    hits: Hits,
    done: usize,
    deleters: BTreeSet<usize>,
    trace: Option<Vec<String>>,
}

impl Run {
    fn new(n: u64, trace: bool, s: Setup, seed: u64) -> Self {
        let mut w = World::new(seed);
        w.loss = s.loss;
        let ids = w.start(n as usize, 3, OLD);
        for i in 0..ids.len() {
            let h: Vec<u64> = (0..s.holders.min(ids.len()))
                .map(|k| ids[(i + k) % ids.len()])
                .collect();
            for _ in 0..2 {
                let id = w.place(&h, SEGMENT_BYTES);
                if !s.repair {
                    w.held_repair.insert(id);
                }
            }
        }
        let mut run = Run {
            w,
            abs: Abs::new(n),
            topo_ms: 0,
            e0: None,
            loss: false,
            accepted: false,
            hits: Hits::default(),
            done: 0,
            deleters: BTreeSet::new(),
            trace: trace.then(Vec::new),
        };
        run.w.advance(SETTLE_MS);
        run
    }

    fn recut(&mut self) {
        self.w.cuts.clear();
        self.w.oneway.clear();
        if let Some(cut) = self.abs.oneway {
            self.w.oneway.insert(cut);
        }
        if !self.abs.cut {
            return;
        }
        let disks: Vec<(u64, u8)> = self.abs.side.iter().map(|(d, s)| (*d, *s)).collect();
        for (a, sa) in &disks {
            for (b, sb) in &disks {
                if a < b && sa != sb {
                    self.w.cuts.insert((*a, *b));
                }
            }
        }
    }

    fn apply(&mut self, ev: Ev) {
        let running: BTreeSet<u64> = self.w.running().into_iter().collect();
        match ev {
            Ev::Kill(d) if running.contains(&d) => self.w.kill(d),
            Ev::Start(d) if !running.contains(&d) => {
                self.w.boot(d, 1);
            }
            Ev::Restart(d) if running.contains(&d) => {
                self.w.kill(d);
                self.w.boot(d, 1);
            }
            Ev::Drain(d) if running.contains(&d) => self.w.drain(d),
            Ev::Accept(d) => {
                self.loss = true;
                self.accepted = true;
                self.w.accept_loss(d);
            }
            _ => {}
        }
        self.loss |= matches!(ev, Ev::Drain(_));
        let predicted = self.abs.next;
        self.abs.apply(ev);
        match ev {
            Ev::Join(_) => {
                self.recut();
                self.w.create(ZONES[(predicted % 3) as usize], OLD, 1);
            }
            Ev::Cut(_) | Ev::CutOneWay(..) | Ev::Heal => self.recut(),
            _ => {}
        }
        if ev != Ev::Wait {
            self.topo_ms = self.w.now_ms;
            self.deleters.clear();
            let run = self.w.running();
            self.e0 = run.iter().filter_map(|d| self.w.groups_epoch(*d)).max();
        }
        if let Some(t) = self.trace.as_mut() {
            t.push(format!("t={}ms event {ev:?}", self.w.now_ms));
        }
    }

    fn comps(&self) -> Vec<BTreeSet<u64>> {
        let mut seen = BTreeSet::new();
        let mut out = Vec::new();
        for d in self.w.running() {
            if seen.contains(&d) {
                continue;
            }
            let s = self.w.side(d);
            seen.extend(s.iter().copied());
            out.push(s);
        }
        out
    }

    fn actor(&self, comp: &BTreeSet<u64>) -> Option<u64> {
        comp.iter()
            .copied()
            .find(|d| self.w.reconciled(*d) && self.w.acts(*d))
    }

    fn check(&mut self, tail: bool) {
        let before = (
            self.hits.two_lists,
            self.hits.missed_record,
            self.hits.stale_serve,
        );
        self.hits.two_lists |= self.w.two_lists > 0 || self.w.ballot_reuse > 0;
        self.hits.missed_record |= self.w.missed_records() > 0;
        self.hits.stale_serve |= self.w.redelivered + self.w.shown > 0;
        let after = (
            self.hits.two_lists,
            self.hits.missed_record,
            self.hits.stale_serve,
        );
        self.hits.tail |= tail && before != after;
        let comps = self.comps();
        let comp_of = |d: u64| comps.iter().position(|c| c.contains(&d));
        let quiet = self.w.now_ms >= self.topo_ms.max(self.w.commit_ms) + GRACE_MS;
        let fresh: Vec<u64> = self.w.done[self.done..].iter().map(|d| d.1).collect();
        self.done = self.w.done.len();
        let ids: Vec<u64> = self.w.segments.keys().copied().collect();
        for id in ids {
            self.hits.double_lead |= self.w.leaders(id).len() >= 2;
        }
        self.hits.double_lead |= self.w.double_leaders > 0;
        if quiet {
            self.deleters.extend(fresh.into_iter().filter_map(comp_of));
            self.hits.double_delete |= self.deleters.len() >= 2;
        }
        if comps.len() < 2 || !quiet {
            return;
        }
        let actors: Vec<u64> = comps.iter().filter_map(|c| self.actor(c)).collect();
        self.hits.split |= actors.len() >= 2;
        if let Some(e0) = self.e0 {
            let raised = comps
                .iter()
                .filter(|c| {
                    c.iter()
                        .filter_map(|d| self.w.groups_epoch(*d))
                        .filter(|e| *e > e0)
                        .any(|e| {
                            self.w.groups_writes.iter().any(|(t, side, written)| {
                                *written == e && *t >= self.topo_ms && c.contains(side)
                            })
                        })
                })
                .count();
            self.hits.double_groups |= raised >= 2;
        }
    }

    fn snapshot(&self) -> String {
        let parts: Vec<String> = self
            .w
            .running()
            .into_iter()
            .map(|d| {
                let list = self.w.list(d);
                format!(
                    "{d}[{}{} key={:?} list={:?}]",
                    if self.w.acts(d) { "A" } else { "-" },
                    if self.w.reconciled(d) { "R" } else { "-" },
                    list.map(|l| l.key()),
                    list.map(|l| l.members.clone())
                )
            })
            .collect();
        format!("t={}ms {}", self.w.now_ms, parts.join(" "))
    }

    fn advance(&mut self, ms: u64, tail: bool) {
        let steps = ms / STEP_MS;
        for i in 0..steps {
            self.w.settle();
            self.w.take();
            self.w.read();
            if i % 8 == 0 {
                for d in self.w.running() {
                    self.w.delete_from(d, 1);
                }
            }
            self.w.advance(STEP_MS);
            self.check(tail);
            if self.trace.is_some() && (i + 1 == steps || (i + 1) % 4 == 0) {
                let s = self.snapshot();
                if let Some(t) = self.trace.as_mut() {
                    t.push(s);
                }
            }
        }
    }

    fn tail(&mut self) {
        self.apply(Ev::Heal);
        let dead: Vec<u64> = self.abs.dead.iter().copied().collect();
        for d in dead {
            self.apply(Ev::Start(d));
        }
        self.advance(TAIL_MS, true);
        if !self.loss {
            self.hits.stuck |= self
                .w
                .running()
                .iter()
                .any(|d| !self.w.in_list(*d) || !self.w.acts(*d) || !self.w.reconciled(*d));
        }
        if !self.accepted {
            self.hits.lost |= self.w.lost() > 0 || self.w.lost_settles() > 0;
        }
    }
}

#[derive(Clone, Copy)]
struct Setup {
    loss: f64,
    holders: usize,
    repair: bool,
}

const BASE: Setup = Setup {
    loss: 0.0,
    holders: 3,
    repair: true,
};

fn play(n: u64, seq: &[Ev], trace: bool, s: Setup) -> (Hits, Vec<String>) {
    let mut r = Run::new(n, trace, s, SEED);
    for ev in seq {
        r.apply(*ev);
        r.advance(GAP_MS, false);
    }
    r.tail();
    (r.hits, r.trace.take().unwrap_or_default())
}

fn valid(n: u64, seq: &[Ev]) -> bool {
    let mut a = Abs::new(n);
    for ev in seq {
        if !a.moves().contains(ev) {
            return false;
        }
        a.apply(*ev);
    }
    true
}

fn minimise(n: u64, seq: &[Ev], s: Setup) -> Vec<Ev> {
    let mut cur = seq.to_vec();
    loop {
        let shrunk = (0..cur.len()).find_map(|i| {
            let mut cand = cur.clone();
            cand.remove(i);
            (valid(n, &cand) && play(n, &cand, false, s).0.any()).then_some(cand)
        });
        match shrunk {
            Some(c) => cur = c,
            None => return cur,
        }
    }
}

fn sequences(a: &Abs, depth: usize, prefix: &mut Vec<Ev>, out: &mut Vec<Vec<Ev>>) {
    if prefix.len() == depth {
        out.push(prefix.clone());
        return;
    }
    for m in a.moves() {
        let mut b = a.clone();
        b.apply(m);
        prefix.push(m);
        sequences(&b, depth, prefix, out);
        prefix.pop();
    }
}

fn search(n: u64, depth: usize, setup: Setup) {
    let started = Instant::now();
    let mut all = Vec::new();
    sequences(&Abs::new(n), depth, &mut Vec::new(), &mut all);
    let next = AtomicUsize::new(0);
    let found = Mutex::new((Vec::<(Hits, Vec<Ev>)>::new(), [0usize; 10]));
    let threads = std::thread::available_parallelism().map_or(4, |t| t.get().min(16));
    std::thread::scope(|s| {
        for _ in 0..threads {
            s.spawn(|| {
                while let Some(seq) = all.get(next.fetch_add(1, Ordering::Relaxed)) {
                    let (h, _) = play(n, seq, false, setup);
                    let mut f = found.lock().expect("results");
                    for (i, hit) in [
                        h.split,
                        h.double_delete,
                        h.double_lead,
                        h.double_groups,
                        h.two_lists,
                        h.missed_record,
                        h.stale_serve,
                        h.stuck,
                        h.lost,
                        h.tail,
                    ]
                    .into_iter()
                    .enumerate()
                    {
                        f.1[i] += usize::from(hit);
                    }
                    if h.any() && f.0.len() < 8 {
                        f.0.push((h, seq.clone()));
                    }
                }
            });
        }
    });
    let (hits, c) = found.into_inner().expect("results");
    let line = format!(
        "n={n} depth={depth} loss={} holders={} repair={} runs={} split={} double-delete={} double-lead={} double-groups={} two-lists={} missed-record={} stale-serve={} stuck={} lost={}",
        setup.loss,
        setup.holders,
        setup.repair,
        all.len(),
        c[0],
        c[1],
        c[2],
        c[3],
        c[4],
        c[5],
        c[6],
        c[7],
        c[8]
    );
    if let Some((h, seq)) = hits.iter().min_by_key(|(_, s)| s.len()) {
        let small = minimise(n, seq, setup);
        let (again, trace) = play(n, &small, true, setup);
        panic!(
            "MEMBERS SEARCH FAIL {line} tail={}\nfirst {h:?} {seq:?}\nminimised {again:?} {small:?}\n{}",
            c[9],
            trace.join("\n")
        );
    }
    println!(
        "MEMBERS SEARCH PASS {line} secs={}",
        started.elapsed().as_secs()
    );
}

fn leads_once(seq: &[Ev], frame_loss: f64, seed: u64) -> Run {
    let mut r = Run::new(
        3,
        false,
        Setup {
            loss: frame_loss,
            ..BASE
        },
        seed,
    );
    for ev in seq {
        r.apply(*ev);
        r.advance(GAP_MS, false);
    }
    r.tail();
    assert!(!r.hits.double_lead, "seed {seed}: {:?}", r.hits);
    r
}

#[test]
fn members_search_kill_restart_leads_once() {
    for seed in 1..=40 {
        leads_once(&[Ev::Kill(1), Ev::Restart(2)], 0.0, seed);
    }
}

#[test]
fn members_search_kill_join_restart_with_frame_loss_leads_once() {
    for seed in 1..=40 {
        leads_once(&[Ev::Kill(1), Ev::Join(0), Ev::Restart(2)], 0.2, seed);
    }
}

#[test]
fn members_search_kill_kill_restart_leads_once() {
    let r = leads_once(&[Ev::Kill(1), Ev::Kill(2), Ev::Restart(3)], 0.0, SEED);
    assert!(!r.hits.any(), "{:?}", r.hits);
    for (id, seg) in &r.w.segments {
        if seg.marks.keys().any(|h| r.w.running().contains(h)) {
            assert_eq!(r.w.leaders(*id).len(), 1, "segment {id}");
        }
    }
}

#[test]
fn members_search_drain_join_cut_settles_only_while_the_leader_acts() {
    let (hits, _) = play(3, &[Ev::Drain(1), Ev::Join(0), Ev::Cut(24)], false, BASE);
    assert!(!hits.any(), "{hits:?}");
}

#[test]
fn members_search_n3_depth3() {
    search(3, 3, BASE);
}

#[test]
fn members_search_n3_depth3_with_frame_loss() {
    search(3, 3, Setup { loss: 0.2, ..BASE });
}

#[test]
fn members_search_n3_depth3_two_holders() {
    search(
        3,
        3,
        Setup {
            holders: 2,
            repair: false,
            ..BASE
        },
    );
}

#[test]
fn members_search_n4_depth3_four_holders() {
    search(4, 3, Setup { holders: 4, ..BASE });
}

#[test]
fn members_search_a_reconciled_node_with_pongs_of_another_list_writes_no_groups() {
    let (hits, _) = play(4, &[Ev::Join(0), Ev::Restart(3), Ev::Cut(52)], false, BASE);
    assert!(!hits.any(), "{hits:?}");
}

#[test]
fn members_search_n3_depth4() {
    search(3, 4, BASE);
}

#[test]
fn members_search_n4_depth3() {
    search(4, 3, BASE);
}

#[test]
#[ignore]
fn members_search_n5_depth3() {
    search(5, 3, BASE);
}

#[test]
#[ignore]
fn members_search_n6_depth3() {
    search(6, 3, BASE);
}

#[test]
fn members_search_a_one_way_cut_counts_a_received_groups_record_as_no_raise() {
    let (hits, _) = play(
        3,
        &[
            Ev::Drain(1),
            Ev::Kill(3),
            Ev::Accept(3),
            Ev::CutOneWay(1, 2),
        ],
        false,
        BASE,
    );
    assert!(!hits.any(), "{hits:?}");
}
