use super::fence::confirmed;
use super::members::Members;
use super::view::{SAMPLE_MS, View};

pub const QUIET_PERIOD: u64 = 6 * 3_600_000;
pub const PRIOR_PERIOD: u64 = 24 * 3_600_000;
pub const PRIOR_LAMBDA: f64 = 0.05;
pub const LAMBDA_FLOOR: f64 = 0.0001;
pub const LOSS_TARGET: f64 = 1e-9;
pub const KEYED_MIN_COPIES: u8 = 2;
pub const MAX_COPIES: u8 = 5;
#[cfg(not(feature = "slow-repair"))]
pub const REPAIR_BYTES_PER_S: u64 = 32 << 20;
#[cfg(feature = "slow-repair")]
pub const REPAIR_BYTES_PER_S: u64 = 64 << 10;
pub const SOAK_MS: u64 = 3_600_000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Floor {
    pub epoch: u64,
    pub value: u8,
}

pub const START: Floor = Floor {
    epoch: 0,
    value: KEYED_MIN_COPIES,
};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Inputs {
    pub n: usize,
    pub domains: usize,
    pub lambda: f64,
    pub w_hours: f64,
    pub keyed: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Copies {
    pub count: u8,
    pub target: u8,
    pub why: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Gate {
    Write,
    SealReopen,
}

fn tail(mu: f64, k: u32) -> f64 {
    let mut log_term = -mu;
    let mut below = 0.0;
    for i in 1..=k {
        below += log_term.exp();
        log_term += mu.ln() - f64::from(i).ln();
    }
    1.0 - below
}

pub fn count(i: &Inputs) -> (u8, String) {
    let n = u8::try_from(i.n).unwrap_or(u8::MAX).max(1);
    let (asked, rule) = if n <= 3 {
        (n, format!("rule 1: N = {n}"))
    } else {
        let a0: u8 = if i.domains <= 1 { 1 } else { 2 };
        let mu = f64::from(n) * i.lambda * i.w_hours;
        let mut a = a0.max(3);
        while a < u8::MAX && tail(mu, u32::from(a - a0 + 1)) >= LOSS_TARGET {
            a += 1;
        }
        let a = a.min(n);
        (
            a,
            format!(
                "rule 2: D = {}, a0 = {a0}, N λ W = {mu:.6} gives {a}",
                i.domains
            ),
        )
    };
    let keyed = if i.keyed {
        asked.max(KEYED_MIN_COPIES)
    } else {
        asked
    };
    if keyed > MAX_COPIES {
        (
            MAX_COPIES,
            format!("{rule}; capped at MAX_COPIES {MAX_COPIES}"),
        )
    } else {
        (keyed, rule)
    }
}

pub fn target(count: u8, n: usize, headroom: f64) -> u8 {
    if headroom > 0.2 {
        let n = u8::try_from(n).unwrap_or(u8::MAX);
        n.min(count + 1).min(MAX_COPIES)
    } else {
        count
    }
}

pub fn gate(record_size: u8, in_force_size: u8) -> Gate {
    if record_size < in_force_size {
        Gate::SealReopen
    } else {
        Gate::Write
    }
}

pub fn refuse(committed: u16, format_max: u16) -> bool {
    committed > format_max
}

fn at_epoch(view: &View, list: &Members) -> bool {
    let voters = view
        .live()
        .filter(|(disk, e)| list.has(*disk) && (e.body.era, e.body.epoch) == list.key())
        .count();
    confirmed(voters, list.members.len())
}

#[derive(Clone, Debug)]
pub struct Controller {
    pub keyed: bool,
    pub count: u8,
    pub floor: Floor,
    pub committed: u16,
    streak: Option<(u8, u64)>,
}

impl Controller {
    pub fn new(keyed: bool, floor: Floor, committed: u16) -> Self {
        Self {
            keyed,
            count: 0,
            floor,
            committed,
            streak: None,
        }
    }

    pub fn inputs(&self, view: &View, domains: usize, run_ms: u64) -> Inputs {
        let holding: Vec<u64> = view
            .live()
            .filter(|(_, e)| e.body.signals == 0)
            .map(|(_, e)| e.body.holds)
            .collect();
        let n = holding.len().max(1);
        let losses = view.losses(run_ms.saturating_sub(QUIET_PERIOD)) as f64;
        let mut lambda =
            (losses / (n as f64 * (QUIET_PERIOD as f64 / 3_600_000.0))).max(LAMBDA_FLOOR);
        let oldest = view.live().map(|(_, e)| e.body.alive_ms).max().unwrap_or(0);
        if oldest < PRIOR_PERIOD {
            lambda = lambda.max(PRIOR_LAMBDA);
        }
        let held = holding.iter().sum::<u64>() as f64 / n as f64;
        Inputs {
            n: holding.len(),
            domains,
            lambda,
            w_hours: held / REPAIR_BYTES_PER_S as f64 / 3600.0,
            keyed: self.keyed,
        }
    }

    pub fn sample(&mut self, view: &View, domains: usize, headroom: f64, run_ms: u64) -> Copies {
        let inputs = self.inputs(view, domains, run_ms);
        let (raw, why) = count(&inputs);
        if raw > self.count || view.quiet(run_ms, QUIET_PERIOD) || self.count == 0 {
            self.count = raw;
        }
        Copies {
            count: self.count,
            target: target(self.count, inputs.n, headroom),
            why,
        }
    }

    pub fn merge(&mut self, other: Floor) -> bool {
        let changed = other > self.floor;
        self.floor = self.floor.max(other);
        changed
    }

    pub fn decide(
        &mut self,
        view: &View,
        list: &Members,
        acts: bool,
        run_ms: u64,
    ) -> Option<Floor> {
        let peers: Vec<_> = view.live().map(|(_, e)| &e.body).collect();
        for body in &peers {
            self.merge(body.floor);
        }
        let mut counts: Vec<u8> = peers.iter().map(|b| b.count).collect();
        counts.sort_unstable_by(|a, b| b.cmp(a));
        let risen = counts.get(counts.len() / 2).copied().unwrap_or(0);
        if risen > self.floor.value {
            self.floor.value = risen;
            self.streak = None;
            return Some(self.floor);
        }
        let c = self.count;
        let quiet = acts
            && peers.iter().all(|b| b.quiet && b.count == c && b.debt == 0)
            && c < self.floor.value
            && at_epoch(view, list);
        let sample = (c, run_ms / SAMPLE_MS);
        match self.streak {
            _ if !quiet => self.streak = None,
            Some((last, at)) if last == c && at + 1 == sample.1 => {
                self.streak = None;
                self.floor = Floor {
                    epoch: self.floor.epoch + 1,
                    value: c,
                };
                return Some(self.floor);
            }
            Some(last) if last == sample => {}
            _ => self.streak = Some(sample),
        }
        None
    }

    pub fn lower(&mut self) -> Option<Floor> {
        (self.count < self.floor.value).then(|| {
            self.floor = Floor {
                epoch: self.floor.epoch + 1,
                value: self.count,
            };
            self.floor
        })
    }

    pub fn commit(&mut self, view: &View, list: &Members, acts: bool, run_ms: u64) -> u16 {
        let seen = view
            .live()
            .map(|(_, e)| e.body.committed)
            .max()
            .unwrap_or(0);
        self.committed = self.committed.max(seen);
        let next = self.committed + 1;
        let live: Vec<u64> = view.live().map(|(disk, _)| disk).collect();
        let ready = live.iter().all(|disk| {
            view.entry(*disk).is_some_and(|e| e.body.format_max >= next)
                && view.alive_for(*disk, run_ms) >= SOAK_MS
        });
        if ready && acts && at_epoch(view, list) {
            self.committed = next;
        }
        self.committed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::testkit::*;
    use crate::cluster::view::{
        Body, FAIL_MS, Head, JOIN_MS, SAMPLE_MS, STOP_BUDGET_MS, State, Update,
    };

    const GB: u64 = 1_000_000_000;

    fn w(bytes: u64) -> f64 {
        bytes as f64 / REPAIR_BYTES_PER_S as f64 / 3600.0
    }

    fn rule(n: usize, domains: usize, lambda: f64, bytes: u64) -> (u8, String) {
        count(&Inputs {
            n,
            domains,
            lambda,
            w_hours: w(bytes),
            keyed: true,
        })
    }

    #[test]
    fn each_row_of_the_how_many_copies_table() {
        let open = Inputs {
            n: 1,
            domains: 1,
            lambda: LAMBDA_FLOOR,
            w_hours: 0.0,
            keyed: false,
        };
        assert_eq!(count(&open).0, 1);
        assert_eq!(target(1, 1, 1.0), 1);
        assert_eq!(rule(1, 1, LAMBDA_FLOOR, 0).0, KEYED_MIN_COPIES);
        assert_eq!(rule(2, 1, LAMBDA_FLOOR, GB).0, 2);
        assert_eq!(target(2, 2, 1.0), 2);
        assert_eq!(rule(3, 1, LAMBDA_FLOOR, GB).0, 3);
        assert_eq!(target(3, 3, 1.0), 3);
        let six = rule(6, 3, LAMBDA_FLOOR, GB).0;
        assert_eq!(six, 3);
        assert_eq!(target(six, 6, 1.0), 4);
        assert_eq!(target(six, 6, 0.1), 3);
        let mut last = 0;
        for reclaims in 1..=10 {
            let spot = rule(12, 1, f64::from(reclaims) / 72.0, 2 * GB).0;
            assert!((4..=5).contains(&spot), "{reclaims} reclaims gave {spot}");
            assert!(spot >= last);
            assert!(target(spot, 12, 1.0) <= MAX_COPIES);
            last = spot;
        }
        assert_eq!(last, 5);
    }

    #[test]
    fn the_target_never_passes_the_live_node_count() {
        assert_eq!(target(3, 2, 1.0), 2);
        assert_eq!(target(2, 1, 1.0), 1);
        assert_eq!(target(5, 3, 1.0), 3);
    }

    #[test]
    fn each_count_of_the_performance_table_and_the_cap() {
        let rows = [
            (3, PRIOR_LAMBDA, 20 * GB, 3, None),
            (10, PRIOR_LAMBDA, 20 * GB, 5, Some(7)),
            (50, PRIOR_LAMBDA, 20 * GB, 5, Some(10)),
            (3, LAMBDA_FLOOR, 20 * GB, 3, None),
            (10, LAMBDA_FLOOR, 20 * GB, 4, None),
            (50, LAMBDA_FLOOR, 20 * GB, 4, None),
            (3, LAMBDA_FLOOR, 2_000 * GB, 3, None),
            (10, LAMBDA_FLOOR, 2_000 * GB, 5, Some(6)),
            (50, LAMBDA_FLOOR, 2_000 * GB, 5, Some(7)),
        ];
        for (n, lambda, bytes, expected, asked) in rows {
            let (c, why) = rule(n, 3, lambda, bytes);
            assert_eq!(c, expected, "N={n} λ={lambda} bytes={bytes}: {why}");
            assert!(target(c, n, 1.0) <= MAX_COPIES);
            match asked {
                Some(a) => {
                    assert!(why.contains(&format!("gives {a}")), "{why}");
                    assert!(why.contains("MAX_COPIES"), "{why}");
                }
                None => assert!(!why.contains("MAX_COPIES"), "{why}"),
            }
        }
    }

    #[test]
    fn a_large_risk_gives_the_cap_and_never_a_smaller_count() {
        assert_eq!(rule(100, 3, PRIOR_LAMBDA, 8_000 * GB).0, MAX_COPIES);
        assert_eq!(rule(200, 3, PRIOR_LAMBDA, 4_000 * GB).0, MAX_COPIES);
        for mu in [1.0, 10.0, 100.0, 250.0, 300.0, 400.0, 800.0, 1_000.0, 1e6] {
            let (c, why) = count(&Inputs {
                n: 50,
                domains: 3,
                lambda: 1.0,
                w_hours: mu / 50.0,
                keyed: true,
            });
            assert_eq!(c, MAX_COPIES, "N λ W = {mu}: {why}");
        }
    }

    #[test]
    fn one_domain_covers_independent_losses_only() {
        assert_eq!(rule(10, 3, LAMBDA_FLOOR, 20 * GB).0, 4);
        assert_eq!(rule(10, 1, LAMBDA_FLOOR, 20 * GB).0, 3);
    }

    fn cluster(n: u64, holds: u64) -> View {
        let mut view = View::new(1, 1, member(1, holds), 0, 0);
        let updates: Vec<Update> = (2..=n)
            .map(|p| update(p, 1, 1, State::Alive, member(p, holds)))
            .collect();
        view.merge(&updates, 0, 0);
        view
    }

    fn member(disk: u64, holds: u64) -> Body {
        Body {
            alive_ms: PRIOR_PERIOD,
            holds,
            ..body(disk)
        }
    }

    fn set(view: &mut View, disk: u64, run_ms: u64, change: impl Fn(&mut Body)) {
        let entry = view.entry(disk).expect("member").clone();
        let mut body = entry.body.clone();
        change(&mut body);
        if disk == view.me() {
            let now = u64::from(entry.body_key.1 + 1) * SAMPLE_MS;
            view.set_body(body, now, run_ms);
            return;
        }
        let update = Update {
            disk,
            head: entry.head,
            body: Some(((entry.head.restart, entry.body_key.1 + 1), body)),
        };
        view.merge(&[update], 0, run_ms);
    }

    #[test]
    fn the_count_rises_in_the_sample_that_sees_a_loss_and_falls_after_the_quiet_period() {
        let mut view = cluster(12, 2 * GB);
        let mut ctl = Controller::new(true, START, 1);
        let start = QUIET_PERIOD;
        assert_eq!(ctl.sample(&view, 3, 1.0, start).count, 3);
        let alive: Vec<u64> = (2..=11).collect();
        beat(&mut view, &alive, 2, FAIL_MS, start);
        view.tick(FAIL_MS, start, &[]);
        assert!(view.absent(12));
        assert_eq!(ctl.sample(&view, 3, 1.0, start).count, 5);
        let mut now = FAIL_MS;
        for run in [
            start + 1,
            start + QUIET_PERIOD / 2,
            start + QUIET_PERIOD - 1,
        ] {
            now += 1;
            beat(&mut view, &alive, now, now, run);
            view.tick(now, run, &[]);
            assert_eq!(ctl.sample(&view, 3, 1.0, run).count, 5, "run {run}");
        }
        now += 1;
        beat(&mut view, &alive, now, now, start + QUIET_PERIOD + 1);
        view.tick(now, start + QUIET_PERIOD + 1, &[]);
        assert_eq!(ctl.sample(&view, 3, 1.0, start + QUIET_PERIOD + 1).count, 3);
    }

    #[test]
    fn a_count_that_rule_one_lowers_waits_for_the_quiet_period() {
        let mut view = cluster(3, GB);
        let mut ctl = Controller::new(true, START, 1);
        let start = QUIET_PERIOD;
        assert_eq!(ctl.sample(&view, 3, 1.0, start).count, 3);
        beat(&mut view, &[2], 2, FAIL_MS, start);
        view.tick(FAIL_MS, start, &[]);
        assert!(view.absent(3));
        assert_eq!(ctl.sample(&view, 3, 1.0, start + 1).count, 3);
        beat(&mut view, &[2], 3, FAIL_MS + 1, start + QUIET_PERIOD - 1);
        assert_eq!(ctl.sample(&view, 3, 1.0, start + QUIET_PERIOD - 1).count, 3);
        beat(&mut view, &[2], 4, FAIL_MS + 2, start + QUIET_PERIOD + 1);
        assert_eq!(ctl.sample(&view, 3, 1.0, start + QUIET_PERIOD + 1).count, 2);
    }

    fn leave(view: &mut View, disk: u64, remaining: u32, run_ms: u64) {
        set(view, disk, run_ms, |b| b.remaining = remaining);
        let head = view.entry(disk).expect("member").head;
        view.merge(
            &[Update {
                disk,
                head: Head {
                    state: State::Left,
                    ..head
                },
                body: None,
            }],
            0,
            run_ms,
        );
    }

    #[test]
    fn a_left_counts_as_a_loss_only_with_remaining_copies_and_no_return() {
        let window = STOP_BUDGET_MS + JOIN_MS;
        let mut view = cluster(8, GB);
        leave(&mut view, 2, 3, 0);
        leave(&mut view, 3, 1, 0);
        leave(&mut view, 4, 0, 0);
        leave(&mut view, 5, 2, 0);
        leave(&mut view, 6, 2, 0);
        view.drained(6, 0, 0);
        view.tick(0, window - 1, &[]);
        assert_eq!(view.losses(0), 0);
        view.merge(
            &[update(5, 2, 0, State::Alive, member(5, GB))],
            0,
            window - 1,
        );
        view.tick(0, window, &[]);
        assert_eq!(view.losses(0), 2);
        assert!(view.absent(2) && view.absent(4) && !view.absent(5));
    }

    #[test]
    fn a_left_after_an_earlier_death_of_the_same_run_moves_the_loss_to_the_left() {
        let hour = 3_600_000;
        let window = STOP_BUDGET_MS + JOIN_MS;
        let mut view = cluster(4, GB);
        let head = view.entry(2).expect("member").head;
        let dead = Head {
            state: State::Dead,
            ..head
        };
        view.merge(
            &[Update {
                disk: 2,
                head: dead,
                body: None,
            }],
            0,
            hour,
        );
        let back = Head {
            heartbeat: head.heartbeat + 5,
            ..head
        };
        view.merge(
            &[Update {
                disk: 2,
                head: back,
                body: None,
            }],
            0,
            hour + 2 * JOIN_MS,
        );
        assert!(!view.absent(2));
        leave(&mut view, 2, 1, 3 * hour);
        view.tick(0, 3 * hour + window, &[]);
        assert_eq!(view.losses(0), 1);
        assert!(!view.quiet(9 * hour + window - 1, QUIET_PERIOD));
        assert!(view.quiet(9 * hour + window + 1, QUIET_PERIOD));
    }

    #[test]
    fn a_young_cluster_uses_the_prior_loss_rate() {
        let ctl = Controller::new(true, START, 1);
        let mut view = cluster(4, GB);
        assert_eq!(ctl.inputs(&view, 3, 0).lambda, LAMBDA_FLOOR);
        for m in 1..=4 {
            set(&mut view, m, 0, |b| b.alive_ms = PRIOR_PERIOD - 1);
        }
        assert_eq!(ctl.inputs(&view, 3, 0).lambda, PRIOR_LAMBDA);
    }

    #[test]
    fn the_floor_merges_by_epoch_then_value() {
        let mut ctl = Controller::new(true, Floor { epoch: 0, value: 5 }, 1);
        assert!(ctl.merge(Floor { epoch: 1, value: 2 }));
        assert!(!ctl.merge(Floor { epoch: 0, value: 9 }));
        assert!(ctl.merge(Floor { epoch: 1, value: 3 }));
        assert_eq!(ctl.floor, Floor { epoch: 1, value: 3 });
    }

    fn list(members: impl IntoIterator<Item = u64>) -> Members {
        Members {
            genesis: 1,
            members: members.into_iter().collect(),
            ..Members::default()
        }
    }

    fn decide(ctl: &mut Controller, view: &View, list: &Members, at: &mut u64) -> Option<Floor> {
        *at += SAMPLE_MS;
        ctl.decide(view, list, true, *at)
    }

    #[test]
    fn a_quiet_fall_needs_two_samples_in_a_row_with_one_count() {
        let mut view = cluster(3, GB);
        report(&mut view, &[1, 2, 3], 2, 0);
        let mut ctl = Controller::new(true, Floor { epoch: 0, value: 4 }, 1);
        ctl.count = 2;
        assert_eq!(ctl.decide(&view, &list(1..=3), true, SAMPLE_MS), None);
        assert_eq!(ctl.decide(&view, &list(1..=3), true, SAMPLE_MS + 1), None);
        assert_eq!(ctl.decide(&view, &list(1..=3), true, 3 * SAMPLE_MS), None);
        ctl.count = 3;
        report(&mut view, &[1, 2, 3], 3, 0);
        assert_eq!(ctl.decide(&view, &list(1..=3), true, 4 * SAMPLE_MS), None);
        assert_eq!(
            ctl.decide(&view, &list(1..=3), true, 5 * SAMPLE_MS),
            Some(Floor { epoch: 1, value: 3 })
        );
    }

    fn report(view: &mut View, members: &[u64], count: u8, run_ms: u64) {
        for m in members {
            set(view, *m, run_ms, |b| {
                b.count = count;
                b.quiet = true;
                b.debt = 0;
            });
        }
    }

    #[test]
    fn a_restarted_floor_holds_and_a_left_never_lowers_it() {
        let mut view = cluster(4, GB);
        let all = [1, 2, 3, 4];
        report(&mut view, &all, 3, 0);
        let mut ctl = Controller::new(true, Floor { epoch: 1, value: 4 }, 1);
        let mut at = 0;
        ctl.count = 3;
        for m in all {
            set(&mut view, m, 0, |b| b.quiet = false);
        }
        assert_eq!(decide(&mut ctl, &view, &list(all), &mut at), None);
        leave(&mut view, 4, 0, 0);
        for _ in 0..3 {
            assert_eq!(decide(&mut ctl, &view, &list(all), &mut at), None);
        }
        assert_eq!(ctl.floor, Floor { epoch: 1, value: 4 });
    }

    #[test]
    fn the_floor_rises_when_more_than_half_report_the_count() {
        let mut view = cluster(4, GB);
        let mut ctl = Controller::new(true, START, 1);
        let mut at = 0;
        report(&mut view, &[1, 2], 4, 0);
        assert_eq!(decide(&mut ctl, &view, &list(1..=4), &mut at), None);
        report(&mut view, &[3], 4, 0);
        assert_eq!(
            decide(&mut ctl, &view, &list(1..=4), &mut at),
            Some(Floor { epoch: 0, value: 4 })
        );
    }

    #[test]
    fn a_quiet_fall_needs_more_than_half_of_the_list_at_its_epoch() {
        let mut view = cluster(5, GB);
        beat(&mut view, &[2], 2, FAIL_MS, 0);
        view.tick(FAIL_MS, 0, &[]);
        report(&mut view, &[1, 2], 2, 0);
        let mut ctl = Controller::new(true, Floor { epoch: 0, value: 3 }, 1);
        let mut at = 0;
        ctl.count = 2;
        for _ in 0..5 {
            assert_eq!(decide(&mut ctl, &view, &list(1..=5), &mut at), None);
        }
        assert_eq!(decide(&mut ctl, &view, &list([1, 2, 3]), &mut at), None);
        assert_eq!(
            decide(&mut ctl, &view, &list([1, 2, 3]), &mut at),
            Some(Floor { epoch: 1, value: 2 })
        );

        let mut view = cluster(12, GB);
        let early: Vec<u64> = (2..=11).collect();
        beat(&mut view, &[12], 2, FAIL_MS, 0);
        view.tick(FAIL_MS, 0, &[]);
        let late: Vec<Update> = (13..=22)
            .map(|p| update(p, 1, 1, State::Alive, member(p, GB)))
            .collect();
        view.merge(&late, FAIL_MS, 1);
        assert!(early.iter().all(|m| view.absent(*m)));
        assert_eq!(view.live().count(), 12);
        let live: Vec<u64> = view.live().map(|(d, _)| d).collect();
        report(&mut view, &live, 3, 1);
        let mut ctl = Controller::new(true, Floor { epoch: 0, value: 4 }, 1);
        ctl.count = 3;
        let mut at = 0;
        let mut other = view.clone();
        for m in [13, 14] {
            set(&mut other, m, 1, |b| b.epoch = 1);
        }
        for _ in 0..3 {
            assert_eq!(decide(&mut ctl, &other, &list(1..=22), &mut at), None);
        }
        let mut debtor = view.clone();
        let joiner = Body {
            epoch: 1,
            debt: 1,
            count: 3,
            quiet: true,
            ..member(23, GB)
        };
        debtor.merge(&[update(23, 1, 1, State::Alive, joiner)], FAIL_MS, 1);
        let mut ctl = Controller::new(true, Floor { epoch: 0, value: 4 }, 1);
        ctl.count = 3;
        for _ in 0..3 {
            assert_eq!(decide(&mut ctl, &debtor, &list(1..=22), &mut at), None);
        }
        let mut joined = view.clone();
        let joiners: Vec<Update> = (31..=34)
            .map(|p| {
                let body = Body {
                    count: 3,
                    quiet: true,
                    ..member(p, GB)
                };
                update(p, 1, 1, State::Alive, body)
            })
            .collect();
        joined.merge(&joiners, FAIL_MS, 1);
        let mut ctl = Controller::new(true, Floor { epoch: 0, value: 4 }, 1);
        ctl.count = 3;
        for _ in 0..3 {
            assert_eq!(decide(&mut ctl, &joined, &list(1..=30), &mut at), None);
        }
        let mut ctl = Controller::new(true, Floor { epoch: 0, value: 4 }, 1);
        ctl.count = 3;
        assert_eq!(decide(&mut ctl, &view, &list(1..=22), &mut at), None);
        assert_eq!(
            decide(&mut ctl, &view, &list(1..=22), &mut at),
            Some(Floor { epoch: 1, value: 3 })
        );
    }

    #[test]
    fn a_joiner_after_a_rolling_restart_counts_the_members_that_are_down() {
        let hour = 3_600_000;
        let live = [1, 2, 3, 6];
        let restarted: Vec<View> = live
            .iter()
            .map(|me| {
                let others: Vec<u64> = (1..=7).filter(|d| d != me).collect();
                let peers: Vec<u64> = live.iter().copied().filter(|d| d != me).collect();
                let mut old = view(*me, &others, 0, 0);
                beat(&mut old, &peers, 2, FAIL_MS, FAIL_MS);
                old.tick(FAIL_MS, FAIL_MS, &[]);
                let (mut back, _) = View::restored(*me, 2, body(*me), 0, hour, &old.durable(0))
                    .expect("the state decodes");
                let alive: Vec<Update> = peers
                    .iter()
                    .map(|p| update(*p, 2, 1, State::Alive, body(*p)))
                    .collect();
                back.merge(&alive, 0, hour);
                back
            })
            .collect();
        let mut h = View::new(8, 1, body(8), 0, hour);
        for peer in &restarted {
            h.merge(&peer.answer(&h.digest()), 0, hour);
        }
        assert!(
            [4, 5, 7]
                .iter()
                .all(|d| h.entry(*d).is_some_and(|e| e.head.state == State::Dead))
        );
        let (again, _) =
            View::restored(8, 2, body(8), 0, hour, &h.durable(0)).expect("the state decodes");
        let fresh = View::new(9, 1, body(9), 0, hour);
        assert!(
            again
                .answer(&fresh.digest())
                .iter()
                .all(|u| ![4, 5, 7].contains(&u.disk))
        );
        beat(&mut h, &[1, 2], 3, FAIL_MS, hour + FAIL_MS);
        h.tick(FAIL_MS, hour + FAIL_MS, &[]);
        report(&mut h, &[1, 2, 8], 3, hour + FAIL_MS);
        let mut ctl = Controller::new(true, Floor { epoch: 0, value: 4 }, 1);
        ctl.count = 3;
        let mut at = hour + FAIL_MS;
        for _ in 0..3 {
            assert_eq!(decide(&mut ctl, &h, &list(1..=7), &mut at), None);
        }
    }

    #[test]
    fn a_body_at_the_same_epoch_of_another_era_does_not_count() {
        let mut view = cluster(3, GB);
        report(&mut view, &[1, 2, 3], 2, 0);
        for m in 1..=3 {
            set(&mut view, m, 0, |b| b.epoch = 3);
        }
        let list = Members {
            era: 1,
            epoch: 3,
            ..list(1..=3)
        };
        let mut ctl = Controller::new(true, Floor { epoch: 0, value: 3 }, 1);
        ctl.count = 2;
        let mut at = 0;
        for _ in 0..3 {
            assert_eq!(decide(&mut ctl, &view, &list, &mut at), None);
        }
    }

    #[test]
    fn a_quiet_fall_and_a_commit_need_a_node_that_acts() {
        let mut view = cluster(3, GB);
        report(&mut view, &[1, 2, 3], 2, 0);
        for m in 1..=3 {
            set(&mut view, m, 0, |b| b.format_max = 2);
        }
        let mut ctl = Controller::new(true, Floor { epoch: 0, value: 3 }, 1);
        ctl.count = 2;
        for at in 1..=3 {
            assert_eq!(ctl.decide(&view, &list(1..=3), false, at * SAMPLE_MS), None);
        }
        assert_eq!(ctl.commit(&view, &list(1..=3), false, SOAK_MS), 1);
        assert_eq!(ctl.commit(&view, &list(1..=6), true, SOAK_MS), 1);
        assert_eq!(ctl.commit(&view, &list(1..=3), true, SOAK_MS), 2);
        let mut at = 3 * SAMPLE_MS;
        assert_eq!(decide(&mut ctl, &view, &list(1..=3), &mut at), None);
        assert_eq!(
            decide(&mut ctl, &view, &list(1..=3), &mut at),
            Some(Floor { epoch: 1, value: 2 })
        );
    }

    #[test]
    fn a_restarted_node_keeps_its_count_and_its_losses() {
        let mut view = cluster(12, 2 * GB);
        let mut ctl = Controller::new(true, START, 1);
        let start = QUIET_PERIOD;
        assert_eq!(ctl.sample(&view, 3, 1.0, start).count, 3);
        let alive: Vec<u64> = (2..=11).collect();
        beat(&mut view, &alive, 2, FAIL_MS, start);
        view.tick(FAIL_MS, start, &[]);
        assert_eq!(ctl.sample(&view, 3, 1.0, start).count, 5);
        let (back, count) = View::restored(
            1,
            2,
            member(1, 2 * GB),
            0,
            start + 1,
            &view.durable(ctl.count),
        )
        .expect("the state decodes");
        assert_eq!(back.losses(0), 1);
        let mut ctl = Controller::new(true, START, 1);
        ctl.count = count;
        assert_eq!(ctl.sample(&back, 3, 1.0, start + 1).count, 5);
    }

    #[test]
    fn a_quiet_fall_needs_debt_zero_and_one_count_on_every_live_node() {
        let mut view = cluster(3, GB);
        report(&mut view, &[1, 2, 3], 2, 0);
        set(&mut view, 3, 0, |b| b.debt = 1);
        let mut ctl = Controller::new(true, Floor { epoch: 0, value: 3 }, 1);
        let mut at = 0;
        ctl.count = 2;
        for _ in 0..3 {
            assert_eq!(decide(&mut ctl, &view, &list(1..=3), &mut at), None);
        }
        set(&mut view, 3, 0, |b| b.debt = 0);
        set(&mut view, 2, 0, |b| b.count = 1);
        for _ in 0..3 {
            assert_eq!(decide(&mut ctl, &view, &list(1..=3), &mut at), None);
        }
        set(&mut view, 2, 0, |b| b.count = 2);
        set(&mut view, 2, 0, |b| b.quiet = false);
        for _ in 0..3 {
            assert_eq!(decide(&mut ctl, &view, &list(1..=3), &mut at), None);
        }
    }

    #[test]
    fn drain_and_accept_loss_lower_the_floor_to_the_count() {
        let mut ctl = Controller::new(true, Floor { epoch: 2, value: 4 }, 1);
        ctl.count = 4;
        assert_eq!(ctl.lower(), None);
        ctl.count = 3;
        assert_eq!(ctl.lower(), Some(Floor { epoch: 3, value: 3 }));
    }

    #[test]
    fn a_rise_is_in_force_only_through_a_groups_record_of_its_size() {
        assert_eq!(gate(3, 4), Gate::SealReopen);
        assert_eq!(gate(4, 4), Gate::Write);
        assert_eq!(gate(5, 4), Gate::Write);
    }

    #[test]
    fn a_format_commits_only_when_every_live_member_runs_it_for_the_soak_time() {
        let mut view = cluster(4, GB);
        for m in 1..=4 {
            set(&mut view, m, 0, |b| b.format_max = 2);
        }
        let mut ctl = Controller::new(true, START, 1);
        assert_eq!(ctl.commit(&view, &list(1..=4), true, SOAK_MS - 1), 1);
        set(&mut view, 4, 0, |b| b.format_max = 1);
        assert_eq!(ctl.commit(&view, &list(1..=4), true, SOAK_MS), 1);
        set(&mut view, 4, 0, |b| b.format_max = 2);
        assert_eq!(ctl.commit(&view, &list(1..=4), true, SOAK_MS), 2);
        assert!(refuse(2, 1));
        assert!(!refuse(2, 2));
    }
}
