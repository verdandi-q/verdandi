// allow: each row uses part of the harness
#[allow(dead_code)]
mod c6;

use c6::{Cluster, Load, below_floor, no_loss};
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn ten_nodes_cold_started_at_once_join_in_ten_seconds_and_no_write_is_below_the_floor() {
    let run = Instant::now();
    let mut c = Cluster::new("row06", 10, false).await;
    let load = Load::start(&c, 4, 2, 0, 10);
    c.start_all();
    let joined = c.try_members(10, 60).await;
    eprintln!("C6 ROW6 join={joined:?} restarts={}", c.restarts());
    sleep(Duration::from_secs(15)).await;
    let rec = load.stop().await;
    eprintln!(
        "C6 ROW6 sent={} acked={} settled={}",
        rec.sent,
        rec.acked.len(),
        rec.settled.len()
    );
    assert!(!rec.acked.is_empty());
    let low = below_floor(&c, &rec);
    assert!(
        low.is_empty(),
        "below the floor on disk: {} {:?}",
        low.len(),
        &low[..low.len().min(5)]
    );
    no_loss(&mut c, &rec).await;
    eprintln!("C6 ROW6 run-secs={:.0}", run.elapsed().as_secs_f64());
    let joined = joined.expect("every node joins");
    assert!(joined <= Duration::from_secs(10), "join took {joined:?}");
    c.clean();
}
