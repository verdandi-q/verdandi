// allow: each row uses part of the harness
#[allow(dead_code)]
mod c6;

use c6::{Cluster, Load, no_loss, readable, tokens};
use std::collections::HashSet;
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn restart_every_node_with_disks_kept_then_with_disks_lost_and_a_backup() {
    let run = Instant::now();
    let mut c = Cluster::new("row15a", 6, false).await;
    c.start_staged().await;
    let first = c.wait_members(6, 120).await;
    eprintln!(
        "C6 ROW15a first-join-secs={:.0} restarts={}",
        first.as_secs_f64(),
        c.restarts()
    );
    let load = Load::start(&c, 4, 2, 0, 10);
    sleep(Duration::from_secs(10)).await;
    let rec = load.stop().await;
    eprintln!(
        "C6 ROW15a acked={} settled={}",
        rec.acked.len(),
        rec.settled.len()
    );
    let down = Instant::now();
    c.nodes.iter_mut().for_each(|n| n.signal("TERM"));
    let mut codes = Vec::new();
    for n in c.nodes.iter_mut() {
        codes.push(n.wait_exit(150).await.code());
    }
    eprintln!(
        "C6 ROW15a stop-secs={:.0} exits={codes:?}",
        down.elapsed().as_secs_f64()
    );
    c.start_all();
    let back = c.try_members(6, 150).await;
    eprintln!("C6 ROW15a back={back:?} restarts={}", c.restarts());
    if back.is_ok() {
        c.wait_ready(60).await;
        no_loss(&mut c, &rec).await;
        c.clean();
    } else {
        c.nodes.iter_mut().for_each(|n| n.kill9());
    }
    eprintln!("C6 ROW15a run-secs={:.0}", run.elapsed().as_secs_f64());

    let mut c = Cluster::new("row15b", 6, true).await;
    c.start_staged().await;
    c.wait_members(6, 120).await;
    let load = Load::start(&c, 4, 0, 0, 10);
    sleep(Duration::from_secs(20)).await;
    let rec = load.stop().await;
    let last = Instant::now();
    let bucket = c.root.join("bucket");
    let mut count = 0;
    let mut quiet = Instant::now();
    while (quiet.elapsed() < Duration::from_secs(10) || last.elapsed() < Duration::from_secs(40))
        && last.elapsed() < Duration::from_secs(120)
    {
        let mut found = HashSet::new();
        tokens(&bucket, &mut found);
        if found.len() != count {
            count = found.len();
            quiet = Instant::now();
        }
        sleep(Duration::from_secs(1)).await;
    }
    let mut uploaded = HashSet::new();
    tokens(&bucket, &mut uploaded);
    let acked: HashSet<&String> = rec.acked.values().collect();
    let kept = acked.iter().filter(|k| uploaded.contains(**k)).count();
    eprintln!(
        "C6 ROW15b acked={} uploaded={} not-uploaded={}",
        acked.len(),
        kept,
        acked.len() - kept
    );
    assert!(kept > 0, "nothing was uploaded");
    c.nodes.iter_mut().for_each(|n| n.kill9());
    for n in &c.nodes {
        std::fs::remove_dir_all(n.dir.join("data")).expect("delete data");
    }
    c.start_staged().await;
    c.wait_members(6, 150).await;
    c.wait_ready(120).await;
    let have = readable(&mut c).await;
    let lost = rec
        .acked
        .iter()
        .filter(|(id, k)| uploaded.contains(*k) && !have.contains(*id))
        .count();
    eprintln!("C6 ROW15b readable={} uploaded-but-lost={lost}", have.len());
    assert_eq!(lost, 0);
    c.clean();
    eprintln!("C6 ROW15 run-secs={:.0}", run.elapsed().as_secs_f64());
    back.expect("every node is back in 150 s");
}
