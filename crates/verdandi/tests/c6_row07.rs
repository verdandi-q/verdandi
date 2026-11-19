// allow: each row uses part of the harness
#[allow(dead_code)]
mod c6;

use c6::{Cluster, Load, no_loss};
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn one_node_cut_for_a_minute_while_it_still_gets_writes_then_its_disk_is_deleted() {
    let run = Instant::now();
    let mut c = Cluster::new("row07", 6, false).await;
    c.start_staged().await;
    c.wait_members(6, 120).await;
    let load = Load::start(&c, 4, 2, 0, 10);
    sleep(Duration::from_secs(10)).await;
    let before = load.acked();
    c.nodes[5].pin.store(true, Relaxed);
    c.nodes[5].signal("STOP");
    sleep(Duration::from_secs(60)).await;
    let during = load.acked();
    c.nodes[5].signal("CONT");
    sleep(Duration::from_secs(2)).await;
    c.nodes[5].kill9();
    c.nodes[5].pin.store(false, Relaxed);
    std::fs::remove_dir_all(c.nodes[5].dir.join("data")).expect("delete data");
    c.nodes[5].start();
    eprintln!("C6 ROW7 acked-before={before} acked-after-cut={during}");
    let back = c.wait_members(6, 120).await;
    c.wait_ready(120).await;
    eprintln!("C6 ROW7 back-secs={:.0}", back.as_secs_f64());
    sleep(Duration::from_secs(10)).await;
    let rec = load.stop().await;
    eprintln!(
        "C6 ROW7 sent={} acked={} settled={} restarts={}",
        rec.sent,
        rec.acked.len(),
        rec.settled.len(),
        c.restarts()
    );
    assert!(during > before, "no write was acked during the cut");
    no_loss(&mut c, &rec).await;
    eprintln!("C6 ROW7 run-secs={:.0}", run.elapsed().as_secs_f64());
    c.clean();
}
