// allow: each row uses part of the harness
#[allow(dead_code)]
mod c6;

use c6::{Cluster, Load, no_loss};
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn a_build_that_cannot_read_the_format_refuses_and_a_kill_in_a_rolling_restart_loses_nothing()
{
    let run = Instant::now();
    let mut c = Cluster::new("row17a", 1, false).await;
    let data = c.nodes[0].dir.join("data/cluster");
    std::fs::create_dir_all(&data).expect("cluster dir");
    std::fs::write(data.join("committed"), [99u8]).expect("committed");
    c.nodes[0].start();
    let status = c.nodes[0].wait_exit(30).await;
    let log = c.nodes[0].log();
    eprintln!(
        "C6 ROW17a exit={:?} log={:?}",
        status.code(),
        log.lines().last()
    );
    assert!(!status.success(), "{status}");
    assert!(
        log.contains("the cluster format is 99; this build reads up to"),
        "{log}"
    );
    c.clean();

    let mut c = Cluster::new("row17b", 6, false).await;
    c.start_staged().await;
    c.wait_members(6, 120).await;
    let load = Load::start(&c, 4, 2, 0, 10);
    sleep(Duration::from_secs(10)).await;
    for i in 0..3 {
        c.nodes[i].signal("TERM");
        if i == 1 {
            sleep(Duration::from_secs(1)).await;
            c.nodes[5].kill9();
        }
        let code = c.nodes[i].wait_exit(150).await.code();
        eprintln!("C6 ROW17b node {} stopped exit={code:?}", i + 1);
        c.nodes[i].start();
        if i == 1 {
            c.nodes[5].start();
        }
        c.wait_ready(120).await;
        c.wait_members(6, 150).await;
    }
    sleep(Duration::from_secs(10)).await;
    let rec = load.stop().await;
    eprintln!(
        "C6 ROW17b acked={} settled={}",
        rec.acked.len(),
        rec.settled.len()
    );
    no_loss(&mut c, &rec).await;
    eprintln!("C6 ROW17 run-secs={:.0}", run.elapsed().as_secs_f64());
    c.clean();
}
