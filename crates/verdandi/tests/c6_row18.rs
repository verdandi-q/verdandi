// allow: each row uses part of the harness
#[allow(dead_code)]
mod c6;

use c6::{Cluster, Load};
use serde_json::Value;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use verdandi_core::cluster::controller::MAX_COPIES;
use verdandi_core::cluster::view::SAMPLE_MS;

async fn copies(c: &mut Cluster) -> Option<u64> {
    let ips = c.running();
    let reply = c
        .http
        .get(format!("http://{}:8080/", ips.first()?))
        .timeout(Duration::from_secs(4))
        .send()
        .await
        .ok()?;
    let body: Value = serde_json::from_slice(&reply.bytes().await.ok()?).ok()?;
    body["copies"]["ack_count"].as_u64()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn ten_losses_in_five_minutes_raise_the_ack_count_within_one_sample() {
    if !cfg!(feature = "slow-repair") {
        panic!("run with --features slow-repair");
    }
    let run = Instant::now();
    let mut c = Cluster::new("row18", 6, false).await;
    c.start_staged().await;
    c.wait_members(6, 120).await;
    let load = Load::start(&c, 2, 0, 20_000, 10);
    while load.acked() < 40 {
        sleep(Duration::from_millis(250)).await;
    }
    let rec = load.stop().await;
    eprintln!("C6 ROW18 data-messages={}", rec.acked.len());
    sleep(Duration::from_secs(20)).await;
    let mut seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos() as u64
        | 1;
    eprintln!("C6 ROW18 seed={seed}");
    let begun = Instant::now();
    let mut last: Option<u64> = None;
    let mut base = None;
    let mut kills = Vec::new();
    let mut changes = Vec::new();
    let mut restart_at: Option<(u64, usize)> = None;
    let mut next_kill = 15;
    while begun.elapsed() < Duration::from_secs(330) {
        let t = begun.elapsed().as_secs();
        if let Some((at, victim)) = restart_at
            && t >= at
        {
            c.nodes[victim].start();
            restart_at = None;
        }
        if t >= next_kill && kills.len() < 10 && restart_at.is_none() {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let victim = 1 + (seed % 5) as usize;
            c.nodes[victim].kill9();
            std::fs::remove_dir_all(c.nodes[victim].dir.join("data")).expect("delete data");
            kills.push(t);
            restart_at = Some((t + 3, victim));
            next_kill = t + 28;
        }
        if let Some(now) = copies(&mut c).await {
            if Some(now) != last {
                eprintln!("C6 ROW18 t={t} copies={now} kills-so-far={}", kills.len());
                changes.push((t, now, kills.len()));
                last = Some(now);
            }
            if base.is_none() {
                base = Some(now);
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
    eprintln!("C6 ROW18 kills-at={kills:?} base={base:?} changes={changes:?}");
    let base = base.expect("a probe was answered before the first loss");
    assert!(
        base < u64::from(MAX_COPIES),
        "the start count {base} is already MAX_COPIES, so no rise can show"
    );
    assert!(
        changes.iter().all(|(_, n, _)| *n >= base),
        "the controller ack count fell inside the quiet period: {changes:?}"
    );
    let (t, _, count) = changes
        .iter()
        .find(|(_, n, _)| *n > base)
        .copied()
        .expect("the controller ack count in GET / never rose over 10 losses");
    let kill = kills.get(count.wrapping_sub(1)).copied().unwrap_or(0);
    assert!(
        t.saturating_sub(kill) <= SAMPLE_MS / 1000 + 2,
        "rise {t} s came {} s after loss {count}",
        t.saturating_sub(kill)
    );
    eprintln!("C6 ROW18 gap: the fall after the 6 h quiet period has no hook and is not checked");
    eprintln!("C6 ROW18 run-secs={:.0}", run.elapsed().as_secs_f64());
    c.clean();
}
