// allow: each row uses part of the harness
#[allow(dead_code)]
mod c6;

use c6::{Cluster, Load, no_loss};
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn the_other_five_stay_ready_for_two_minutes_while_repair_cannot_finish() {
    if !cfg!(feature = "slow-repair") {
        panic!("run with --features slow-repair");
    }
    let run = Instant::now();
    let mut c = Cluster::new("row08", 6, false).await;
    c.start_staged().await;
    c.wait_members(6, 120).await;
    let load = Load::start(&c, 4, 0, 60_000, 10);
    while load.acked() < 1500 {
        sleep(Duration::from_millis(500)).await;
        assert!(
            run.elapsed() < Duration::from_secs(120),
            "the backlog is too slow"
        );
    }
    c.nodes[5].kill9();
    let killed = Instant::now();
    let mut polls = 0;
    let mut bad = Vec::new();
    while killed.elapsed() < Duration::from_secs(120) {
        for ip in c.running() {
            polls += 1;
            if !c.ready(&ip).await {
                bad.push((killed.elapsed().as_secs(), ip));
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
    let rec = load.stop().await;
    eprintln!(
        "C6 ROW8 acked={} polls={polls} not-ready={bad:?}",
        rec.acked.len()
    );
    assert!(
        bad.is_empty(),
        "a node was not ready: {bad:?}\n{}",
        c.logs()
    );
    no_loss(&mut c, &rec).await;
    eprintln!("C6 ROW8 run-secs={:.0}", run.elapsed().as_secs_f64());
    c.clean();
}
