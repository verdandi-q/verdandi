// allow: each row uses part of the harness
#[allow(dead_code)]
mod c6;

use c6::{Cluster, Load, Record, STREAM, no_loss, tokens};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::time::{Duration, Instant};
use tokio::time::sleep;

fn dir_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|e| match e.metadata() {
            Ok(m) if m.is_dir() => dir_bytes(&e.path()),
            Ok(m) => m.len(),
            Err(_) => 0,
        })
        .sum()
}

async fn settle_two_of_three(c: &Cluster, ids: &BTreeSet<String>) -> BTreeSet<String> {
    let mut settled = BTreeSet::new();
    for _ in 0..400 {
        if settled.len() * 3 >= ids.len() * 2 {
            break;
        }
        let Ok(reply) = c
            .http
            .get(format!("{}/s/{STREAM}?group=g&limit=100&lease=20", c.lb))
            .send()
            .await
        else {
            continue;
        };
        let Ok(bytes) = reply.bytes().await else {
            continue;
        };
        let Ok(body) = serde_json::from_slice::<Value>(&bytes) else {
            break;
        };
        let Some(receipt) = body["receipt"].as_str() else {
            break;
        };
        let got: Vec<&str> = body["messages"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|m| m["id"].as_str())
            .collect();
        let ack: Vec<&str> = got
            .iter()
            .enumerate()
            .filter(|(i, _)| i % 3 != 0)
            .map(|(_, id)| *id)
            .collect();
        let sent = c
            .http
            .post(format!("{}/r/{receipt}", c.lb))
            .body(serde_json::json!({ "ack": ack }).to_string())
            .send()
            .await;
        if sent.is_ok_and(|r| r.status().is_success()) {
            settled.extend(ack.iter().map(|id| (*id).to_owned()));
        }
    }
    settled
}

fn copies(dirs: &[std::path::PathBuf]) -> HashMap<String, usize> {
    let mut count: HashMap<String, usize> = HashMap::new();
    for dir in dirs {
        let mut found = HashSet::new();
        tokens(dir, &mut found);
        for key in found {
            *count.entry(key).or_default() += 1;
        }
    }
    count
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn compaction_under_churn_keeps_the_ack_floor_of_live_copies_at_every_sample() {
    let run = Instant::now();
    let mut c = Cluster::new("row13", 6, false).await;
    c.start_staged().await;
    c.wait_members(6, 120).await;
    let mut seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos() as u64
        | 1;
    eprintln!("C6 ROW13 seed={seed}");
    let mut acked: BTreeMap<String, String> = BTreeMap::new();
    let mut settled: BTreeSet<String> = BTreeSet::new();
    let mut samples: Vec<(usize, u64, Vec<String>, Vec<String>)> = Vec::new();
    let mut taken = 0usize;
    for step in 0..5 {
        let load = Load::start(&c, 4, 0, 500, 10);
        sleep(Duration::from_secs(4)).await;
        let rec = load.stop().await;
        let ids: BTreeSet<String> = rec.acked.keys().cloned().collect();
        acked.extend(rec.acked);
        let done = settle_two_of_three(&c, &ids).await;
        let before: u64 = c.dirs().iter().map(|d| dir_bytes(d)).sum();
        eprintln!(
            "C6 ROW13 step={step} acked={} settled={} data-bytes={before}",
            ids.len(),
            done.len()
        );
        settled.extend(done);
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let victim = (seed % 6) as usize;
        let begun = Instant::now();
        let mut killed = false;
        let mut back = false;
        while begun.elapsed() < Duration::from_secs(50) {
            let t = begun.elapsed().as_secs();
            if t >= 25 && !killed {
                c.nodes[victim].kill9();
                killed = true;
                eprintln!("C6 ROW13 step={step} kill=n{}", victim + 1);
            }
            if t >= 33 && killed && !back {
                c.nodes[victim].start();
                back = true;
            }
            let upto: Vec<String> = acked.values().cloned().collect();
            let live: Vec<_> = c
                .nodes
                .iter_mut()
                .filter_map(|n| n.alive().then(|| n.dir.join("data")))
                .collect();
            let strict = copies(&live);
            let all = copies(&c.dirs());
            let low_live: Vec<String> = upto
                .iter()
                .filter(|k| strict.get(*k).copied().unwrap_or(0) < 2)
                .cloned()
                .collect();
            let low_all: Vec<String> = upto
                .iter()
                .filter(|k| all.get(*k).copied().unwrap_or(0) < 2)
                .cloned()
                .collect();
            samples.push((step, begun.elapsed().as_secs(), low_live, low_all));
            taken += 1;
            sleep(Duration::from_secs(1)).await;
        }
        let after: u64 = c.dirs().iter().map(|d| dir_bytes(d)).sum();
        eprintln!("C6 ROW13 step={step} data-bytes-after={after}");
    }
    let by_key: HashMap<&String, &String> = acked.iter().map(|(id, k)| (k, id)).collect();
    let mut bad_live = 0;
    let mut bad_all = 0;
    let mut first = None;
    for (step, t, live, all) in &samples {
        let keep = |keys: &Vec<String>| {
            keys.iter()
                .filter(|k| by_key.get(k).is_some_and(|id| !settled.contains(*id)))
                .count()
        };
        let (l, a) = (keep(live), keep(all));
        if l > 0 && first.is_none() {
            first = Some((*step, *t, l));
        }
        bad_live += usize::from(l > 0);
        bad_all += usize::from(a > 0);
    }
    eprintln!(
        "C6 ROW13 samples={taken} below-floor-live-disks={bad_live} below-floor-all-disks={bad_all} first={first:?} restarts={}",
        c.restarts()
    );
    c.wait_ready(120).await;
    let rec = Record {
        acked,
        settled,
        sent: 0,
    };
    no_loss(&mut c, &rec).await;
    assert_eq!(bad_live, 0, "samples below the floor: {first:?}");
    eprintln!("C6 ROW13 run-secs={:.0}", run.elapsed().as_secs_f64());
    c.clean();
}
