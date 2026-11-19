// allow: each row uses part of the harness
#[allow(dead_code)]
mod c6;

use c6::{Cluster, Load, no_loss};
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn half_of_one_failure_domain_reclaimed_at_once_twice_with_every_zone_unknown() {
    let run = Instant::now();
    let mut c = Cluster::new("row11", 6, false).await;
    c.start_staged().await;
    c.wait_members(6, 120).await;
    eprintln!(
        "C6 ROW11 form: real processes have no zone (fixed IMDS address), so every node is its own domain; round 1 and 2 kill 1 node (half of a one-node domain), round 3 kills 2 nodes at once; the known-zone form cannot run here"
    );
    let load = Load::start(&c, 4, 2, 0, 10);
    sleep(Duration::from_secs(15)).await;
    for (round, set) in [vec![1], vec![4], vec![2, 3]].into_iter().enumerate() {
        let at = Instant::now();
        for i in &set {
            let i = *i;
            c.nodes[i].kill9();
            std::fs::remove_dir_all(c.nodes[i].dir.join("data")).expect("delete data");
        }
        for i in &set {
            c.nodes[*i].start();
        }
        let back = c.try_members(6, 120).await;
        eprintln!(
            "C6 ROW11 round={} killed={set:?} members-back={back:?}",
            round + 1
        );
        c.wait_ready(120).await;
        sleep(Duration::from_secs(20)).await;
        let lost = load.lost_so_far(&mut c).await;
        eprintln!(
            "C6 ROW11 round={} acked={} lost={} secs={:.0}",
            round + 1,
            load.acked(),
            lost.len(),
            at.elapsed().as_secs_f64()
        );
        assert!(
            lost.is_empty(),
            "LOSS in round {}: {}\n{}",
            round + 1,
            lost.len(),
            c.logs()
        );
    }
    let rec = load.stop().await;
    eprintln!(
        "C6 ROW11 final acked={} restarts={}",
        rec.acked.len(),
        c.restarts()
    );
    no_loss(&mut c, &rec).await;
    eprintln!("C6 ROW11 run-secs={:.0}", run.elapsed().as_secs_f64());
    c.clean();
}
