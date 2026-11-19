// allow: each row uses part of the harness
#[allow(dead_code)]
mod c6;

use c6::{Cluster, Load, Record, STREAM, no_loss, readable};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::time::sleep;

fn starts(bytes: &[u8]) -> Vec<usize> {
    bytes
        .windows(4)
        .enumerate()
        .filter(|(_, w)| *w == b"VDLB")
        .map(|(at, _)| at)
        .collect()
}

fn fattest(data: &std::path::Path) -> (PathBuf, usize) {
    let mut best = (PathBuf::new(), 0);
    for entry in std::fs::read_dir(data).expect("data dir").flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with("seg-") {
            let n = starts(&std::fs::read(entry.path()).expect("read")).len();
            if n > best.1 {
                best = (entry.path(), n);
            }
        }
    }
    best
}

async fn hundred(c: &Cluster) -> Record {
    let mut rec = Record::default();
    for i in 0..100 {
        let key = format!("0:{i}");
        let reply = c
            .http
            .post(format!("http://{}:8080/s/{STREAM}", c.nodes[0].ip))
            .body(format!("c6m:{key};x"))
            .send()
            .await
            .expect("publish");
        assert!(
            reply.status().is_success(),
            "publish {i}: {}",
            reply.status()
        );
        let body: Value =
            serde_json::from_slice(&reply.bytes().await.expect("body")).expect("json");
        for id in body["ids"].as_array().expect("ids") {
            rec.acked
                .insert(id.as_str().expect("id").to_owned(), key.clone());
        }
        rec.sent += 1;
    }
    rec
}

async fn flipped(name: &str, all: bool) {
    let mut c = Cluster::new(name, 3, false).await;
    c.start_staged().await;
    c.wait_members(3, 120).await;
    let rec = hundred(&c).await;
    sleep(Duration::from_secs(3)).await;
    c.nodes.iter_mut().for_each(|n| n.kill9());
    let (file, blocks) = fattest(&c.nodes[0].dir.join("data"));
    let file_name = file.file_name().expect("name").to_owned();
    let bytes = std::fs::read(&file).expect("read");
    let at = starts(&bytes);
    eprintln!("C6 ROW14 {name} file={file_name:?} blocks={blocks}");
    assert!(blocks >= 100, "fewer than 100 blocks in one file: {blocks}");
    let tag = b"c6m:";
    let block_of = |key: &str| {
        let pos = bytes
            .windows(tag.len() + key.len() + 1)
            .position(|w| {
                w.starts_with(tag)
                    && &w[tag.len()..tag.len() + key.len()] == key.as_bytes()
                    && w[tag.len() + key.len()] == b';'
            })
            .expect("message in the file");
        at.iter().take_while(|s| **s <= pos).count()
    };
    let in_ten: Vec<&String> = rec.acked.values().filter(|k| block_of(k) == 10).collect();
    eprintln!("C6 ROW14 {name} keys-in-block-10={in_ten:?}");
    let targets = if all { 0..3 } else { 0..1 };
    for i in targets {
        let path = c.nodes[i].dir.join("data").join(&file_name);
        let mut b = std::fs::read(&path).expect("read copy");
        let start = starts(&b)[9];
        b[start + 54 + 4] ^= 0xff;
        std::fs::write(&path, b).expect("write copy");
    }
    c.start_all();
    c.wait_members(3, 120).await;
    c.wait_ready(120).await;
    sleep(Duration::from_secs(10)).await;
    let have = readable(&mut c).await;
    let by_key: BTreeMap<&String, &String> = rec.acked.iter().map(|(id, k)| (k, id)).collect();
    let lost: Vec<&String> = by_key
        .iter()
        .filter(|(_, id)| !have.contains(**id))
        .map(|(k, _)| *k)
        .collect();
    eprintln!("C6 ROW14 {name} readable={} lost={lost:?}", have.len());
    if all {
        assert!(
            lost.iter().all(|k| in_ten.contains(k)),
            "lost outside block 10: {lost:?}"
        );
        assert!(lost.len() <= in_ten.len());
    } else {
        assert!(lost.is_empty(), "loss with two good copies: {lost:?}");
    }
    c.clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn damaged_block_and_killed_holders_lose_nothing_that_was_acked() {
    let run = Instant::now();
    flipped("row14a", false).await;
    eprintln!("C6 ROW14 a-secs={:.0}", run.elapsed().as_secs_f64());
    flipped("row14b", true).await;
    eprintln!("C6 ROW14 b-secs={:.0}", run.elapsed().as_secs_f64());

    let mut c = Cluster::new("row14c", 3, false).await;
    c.start_staged().await;
    c.wait_members(3, 120).await;
    let load = Load::start(&c, 4, 0, 0, 10);
    sleep(Duration::from_secs(10)).await;
    let mut seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos() as u64
        | 1;
    eprintln!("C6 ROW14 seed={seed}");
    for round in 0..4 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let victim = (seed % 3) as usize;
        sleep(Duration::from_millis(seed % 1500)).await;
        c.nodes[victim].kill9();
        if round % 2 == 1 {
            std::fs::remove_dir_all(c.nodes[victim].dir.join("data")).expect("delete data");
        }
        eprintln!(
            "C6 ROW14 round={round} kill=n{} wipe={}",
            victim + 1,
            round % 2 == 1
        );
        sleep(Duration::from_secs(3)).await;
        c.nodes[victim].start();
        c.wait_ready(120).await;
        sleep(Duration::from_secs(10)).await;
        let lost = load.lost_so_far(&mut c).await;
        eprintln!(
            "C6 ROW14 round={round} acked={} lost={}",
            load.acked(),
            lost.len()
        );
        assert!(
            lost.is_empty(),
            "LOSS round {round}: {}\n{}",
            lost.len(),
            c.logs()
        );
    }
    let rec = load.stop().await;
    no_loss(&mut c, &rec).await;
    eprintln!("C6 ROW14 run-secs={:.0}", run.elapsed().as_secs_f64());
    c.clean();
}
