// allow: each row uses part of the harness
#[allow(dead_code)]
mod c6;

use c6::{Cluster, Load, STREAM, tokens};
use serde_json::Value;
use std::collections::{BTreeSet, HashSet};
use std::time::{Duration, Instant};
use tokio::time::sleep;

fn next(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn five_scale_and_replace_events_then_five_three_node_kill_sets_and_the_loss_fraction() {
    let run = Instant::now();
    let mut c = Cluster::new("row12", 8, false).await;
    c.nodes[0].start();
    c.wait_members(1, 90).await;
    c.nodes.iter_mut().take(6).skip(1).for_each(|n| n.start());
    c.wait_members(6, 120).await;
    let mut live: Vec<usize> = (0..6).collect();
    let load = Load::start(&c, 4, 2, 0, 10);
    sleep(Duration::from_secs(10)).await;
    let events = ["add n7", "add n8", "stop n2", "replace n3", "replace n5"];
    for event in events {
        let at = Instant::now();
        match event {
            "add n7" => {
                c.nodes[6].start();
                live.push(6);
            }
            "add n8" => {
                c.nodes[7].start();
                live.push(7);
            }
            "stop n2" => {
                c.nodes[1].signal("TERM");
                c.nodes[1].wait_exit(150).await;
                live.retain(|i| *i != 1);
            }
            _ => {
                let i = if event == "replace n3" { 2 } else { 4 };
                c.nodes[i].kill9();
                std::fs::remove_dir_all(c.nodes[i].dir.join("data")).expect("delete data");
                c.nodes[i].start();
            }
        }
        let members = c.try_members(live.len(), 20).await;
        c.wait_ready(120).await;
        sleep(Duration::from_secs(5)).await;
        eprintln!(
            "C6 ROW12 event={event} members={members:?} secs={:.0}",
            at.elapsed().as_secs_f64()
        );
    }
    let mut seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos() as u64
        | 1;
    eprintln!("C6 ROW12 seed={seed} live={live:?}");
    let probe = c
        .http
        .post(format!("http://{}:8080/s/{STREAM}", c.nodes[live[0]].ip))
        .body("c6m:9:0;probe")
        .send()
        .await
        .expect("probe");
    let body: Value = serde_json::from_slice(&probe.bytes().await.expect("body")).expect("json");
    let root = c
        .http
        .get(format!("http://{}:8080/", c.nodes[live[0]].ip))
        .send()
        .await
        .expect("root");
    let root: Value = serde_json::from_slice(&root.bytes().await.expect("body")).expect("json");
    eprintln!(
        "C6 ROW12 ack-copies={} controller={}",
        body["copies"], root["copies"]
    );
    let mut gone: BTreeSet<String> = BTreeSet::new();
    let mut hit = 0;
    let mut bound = 0;
    for set_no in 0..5 {
        let mut pool = live.clone();
        let mut set = Vec::new();
        while set.len() < 3 {
            set.push(pool.remove((next(&mut seed) % pool.len() as u64) as usize));
        }
        let at = Instant::now();
        let before = load.snapshot().acked;
        let dirs = c.dirs();
        let holds: Vec<(usize, HashSet<String>)> = live
            .iter()
            .map(|i| {
                let mut found = HashSet::new();
                tokens(&dirs[*i], &mut found);
                (*i, found)
            })
            .collect();
        let whole: BTreeSet<String> = before
            .values()
            .filter(|key| {
                let holders: Vec<usize> = holds
                    .iter()
                    .filter(|(_, found)| found.contains(*key))
                    .map(|(i, _)| *i)
                    .collect();
                !holders.is_empty() && holders.iter().all(|i| set.contains(i))
            })
            .cloned()
            .collect();
        for i in &set {
            c.nodes[*i].kill9();
            std::fs::remove_dir_all(c.nodes[*i].dir.join("data")).expect("delete data");
        }
        for i in &set {
            c.nodes[*i].start();
        }
        let back = c.try_members(live.len(), 20).await;
        c.wait_ready(120).await;
        sleep(Duration::from_secs(20)).await;
        let settled = load.snapshot().settled;
        let dirs = c.dirs();
        let mut kept = HashSet::new();
        for i in &live {
            tokens(&dirs[*i], &mut kept);
        }
        let fresh: Vec<String> = before
            .iter()
            .filter(|(id, key)| !settled.contains(*id) && !kept.contains(*key))
            .map(|(id, _)| id.clone())
            .filter(|id| !gone.contains(id))
            .collect();
        eprintln!(
            "C6 ROW12 set={set_no} killed={set:?} members-back={back:?} new-lost={} whole-on-set={} secs={:.0}",
            fresh.len(),
            whole.len(),
            at.elapsed().as_secs_f64()
        );
        let unexplained: Vec<&String> = fresh
            .iter()
            .filter(|id| before.get(*id).is_some_and(|key| !whole.contains(key)))
            .collect();
        assert!(
            unexplained.is_empty(),
            "set {set_no}: {} acked messages lost that no kill set held whole: {:?}\n{}",
            unexplained.len(),
            unexplained.iter().take(5).collect::<Vec<_>>(),
            c.logs()
        );
        hit += usize::from(!fresh.is_empty());
        bound += usize::from(!whole.is_empty());
        gone.extend(fresh);
    }
    load.stop().await;
    eprintln!(
        "C6 ROW12 kill-sets-with-loss={hit} of 5 fraction={:.2} sets-holding-a-whole-copy-group={bound} total-lost={}",
        hit as f64 / 5.0,
        gone.len()
    );
    eprintln!("C6 ROW12 run-secs={:.0}", run.elapsed().as_secs_f64());
    c.clean();
}
