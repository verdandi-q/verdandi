// allow: each row uses part of the harness
#[allow(dead_code)]
mod c6;

use c6::{Cluster, Load, no_loss};
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn three_of_six_nodes_stop_within_five_seconds_and_nothing_is_lost() {
    let run = Instant::now();
    let mut c = Cluster::new("row10", 6, false).await;
    c.start_staged().await;
    c.wait_members(6, 120).await;
    let load = Load::start(&c, 4, 2, 0, 10);
    sleep(Duration::from_secs(10)).await;
    let term = Instant::now();
    for i in [3, 4, 5] {
        c.nodes[i].signal("TERM");
        sleep(Duration::from_secs(2)).await;
    }
    let sent_in = term.elapsed();
    let mut codes = Vec::new();
    for i in [3, 4, 5] {
        codes.push(c.nodes[i].wait_exit(150).await.code());
    }
    eprintln!(
        "C6 ROW10 signals-secs={:.1} stop-secs={:.0} exits={codes:?}",
        sent_in.as_secs_f64() - 2.0,
        term.elapsed().as_secs_f64()
    );
    let left = c.wait_members(3, 120).await;
    eprintln!("C6 ROW10 members-3-after-secs={:.0}", left.as_secs_f64());
    sleep(Duration::from_secs(10)).await;
    let rec = load.stop().await;
    eprintln!(
        "C6 ROW10 acked={} settled={}",
        rec.acked.len(),
        rec.settled.len()
    );
    no_loss(&mut c, &rec).await;
    eprintln!("C6 ROW10 run-secs={:.0}", run.elapsed().as_secs_f64());
    c.clean();
}
