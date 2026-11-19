// allow: each row uses part of the harness
#[allow(dead_code)]
mod c6;

use c6::{Cluster, Load, no_loss, takeable};
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn a_node_back_with_its_old_disk_does_not_bring_back_a_settled_message() {
    settled_stay_settled(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn control_with_no_node_away_a_settled_message_does_not_return() {
    settled_stay_settled(false).await;
}

async fn settled_stay_settled(cut: bool) {
    let run = Instant::now();
    let mut c = Cluster::new("row16", 6, false).await;
    c.start_staged().await;
    c.wait_members(6, 120).await;
    let load = Load::start(&c, 4, 2, 0, 10);
    sleep(Duration::from_secs(15)).await;
    let mut rec = load.stop().await;
    if cut {
        c.nodes[5].kill9();
    }
    let load = Load::start(&c, 4, 2, 0, 10);
    sleep(Duration::from_secs(30)).await;
    let away = load.stop().await;
    let first = rec.settled.clone();
    rec.acked.extend(away.acked);
    rec.settled.extend(away.settled);
    eprintln!(
        "C6 ROW16 acked={} settled={}",
        rec.acked.len(),
        rec.settled.len()
    );
    if cut {
        c.nodes[5].start();
    }
    c.wait_members(6, 120).await;
    c.wait_ready(120).await;
    sleep(Duration::from_secs(30)).await;
    let back = takeable(&c, "g").await;
    let returned: Vec<&String> = back.iter().filter(|id| rec.settled.contains(*id)).collect();
    eprintln!(
        "C6 ROW16 takeable={} settled-returned={}",
        back.len(),
        returned.len()
    );
    eprintln!(
        "C6 ROW16 returned-ids={:?}",
        returned.iter().take(5).collect::<Vec<_>>()
    );
    let early = returned.iter().filter(|id| first.contains(**id)).count();
    eprintln!("C6 ROW16 returned-settled-before-the-cut={early}");
    assert!(
        returned.is_empty(),
        "settled messages returned: {}",
        returned.len()
    );
    no_loss(&mut c, &rec).await;
    eprintln!("C6 ROW16 run-secs={:.0}", run.elapsed().as_secs_f64());
    c.clean();
}
