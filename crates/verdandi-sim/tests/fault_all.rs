use std::env::{self, VarError};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use verdandi_sim::{compaction_sim, engine_sim, log_sim};

const BUDGET: Duration = Duration::from_secs(540);
const NEEDED: u64 = 10_000;
const FIRST_SEED: u64 = 1_000_000;
const THREADS_MAX: usize = 16;

#[derive(Default)]
struct Totals {
    log: u64,
    engine: u64,
    compaction: u64,
    log_flips: usize,
    report: engine_sim::Report,
    steps: u64,
}

impl Totals {
    fn add(mut self, other: Totals) -> Totals {
        self.log += other.log;
        self.engine += other.engine;
        self.compaction += other.compaction;
        self.log_flips += other.log_flips;
        self.steps += other.steps;
        add_report(&mut self.report, &other.report);
        self
    }
}

fn add_report(sum: &mut engine_sim::Report, report: &engine_sim::Report) {
    sum.published += report.published;
    sum.acked += report.acked;
    sum.crashes += report.crashes;
    sum.flips += report.flips;
    sum.named += report.named;
    sum.dead += report.dead;
    sum.served += report.served;
    sum.repeats += report.repeats;
}

fn setting(name: &str, default: u64) -> u64 {
    match env::var(name) {
        Ok(text) => text
            .parse()
            .unwrap_or_else(|_| panic!("{name}={text} is not a whole number")),
        Err(VarError::NotPresent) => default,
        Err(error) => panic!("{name}: {error}"),
    }
}

fn worker(next: &AtomicU64, first: u64, count: u64, start: Instant) -> Totals {
    let mut totals = Totals::default();
    while start.elapsed() < BUDGET {
        let index = next.fetch_add(1, Ordering::Relaxed);
        if index >= count {
            break;
        }
        let seed = first + index;
        totals.log_flips += log_sim::run(seed).flips;
        totals.log += 1;
        add_report(&mut totals.report, &engine_sim::run(seed));
        totals.engine += 1;
        totals.steps += compaction_sim::crash_at_every_step(seed).steps;
        totals.compaction += 1;
    }
    totals
}

#[test]
#[ignore]
fn fault_all() {
    let first = setting("A3_FIRST_SEED", FIRST_SEED);
    let count = setting("A3_SEEDS", NEEDED);
    let start = Instant::now();
    let next = AtomicU64::new(0);
    let threads = thread::available_parallelism().map_or(1, |n| n.get().min(THREADS_MAX));
    let totals = thread::scope(|scope| {
        let workers: Vec<_> = (0..threads)
            .map(|_| scope.spawn(|| worker(&next, first, count, start)))
            .collect();
        workers
            .into_iter()
            .map(|w| w.join().unwrap_or_else(|e| std::panic::resume_unwind(e)))
            .fold(Totals::default(), Totals::add)
    });
    let Totals {
        log,
        engine,
        compaction,
        log_flips,
        report,
        steps,
    } = totals;
    let seeds = log.min(engine).min(compaction);
    let flips = log_flips + report.flips;
    let summary = format!(
        "seeds={seeds} log={log} engine={engine} compaction={compaction} acked={} flips={flips} named={} steps={steps} first={first} elapsed_s={}",
        report.acked,
        report.named,
        start.elapsed().as_secs()
    );
    assert!(seeds >= count, "M1 FAULT PARTIAL {summary} wanted={count}");
    let n = seeds as usize;
    assert!(
        report.published > n * 10,
        "M1 FAULT FAIL too few acked publishes {summary} {report:?}"
    );
    assert!(
        report.acked > n * 5,
        "M1 FAULT FAIL too few acked settles {summary} {report:?}"
    );
    assert!(
        report.named * 5 > n,
        "M1 FAULT FAIL too few named flips {summary} {report:?}"
    );
    assert!(
        report.named * 2 > report.flips,
        "M1 FAULT FAIL most flips are not inside a block {summary} {report:?}"
    );
    assert!(
        report.dead * 2 > n,
        "M1 FAULT FAIL too few dead copies {summary} {report:?}"
    );
    assert!(
        report.repeats > n,
        "M1 FAULT FAIL too few key repeats {summary} {report:?}"
    );
    assert!(
        steps >= seeds * 5,
        "M1 FAULT FAIL too few compaction steps {summary}"
    );
    println!("M1 FAULT PASS {summary}");
}
