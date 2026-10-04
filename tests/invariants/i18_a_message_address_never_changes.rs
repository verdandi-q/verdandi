use std::path::Path;

use verdandi_core::lease::Settle;
use verdandi_core::retention::RETAIN_MS;
use verdandi_sim::fault_fs::FaultFs;
use verdandi_store::engine::{IDLE_SEAL_MS, SettleBy, Take, Taken, TestClock};

use crate::kit::{DATA, boot, publish, runtime, stop, take};

#[test]
fn i18_compaction_keeps_every_address_and_never_re_keys_an_ack() {
    let rt = runtime();
    let fs = FaultFs::new(18, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    publish(&rt, &node, "s", &[b'x'; 8192]).expect("publish");
    publish(&rt, &node, "s", b"b").expect("publish");
    publish(&rt, &node, "s", b"c").expect("publish");
    let Ok(Taken::Leased {
        receipt, messages, ..
    }) = rt.block_on(node.handle.take(Take {
        group: "g".to_owned(),
        selector: "s".to_owned(),
        limit: 10,
        lease_ms: 30_000,
        wait: false,
    }))
    else {
        panic!("no messages");
    };
    let before: Vec<_> = messages
        .iter()
        .map(|d| (d.handed.id, d.handed.address))
        .collect();
    let settle = SettleBy::Ids(Settle {
        ack: vec![before[0].0, before[1].0],
        ..Settle::default()
    });
    rt.block_on(node.handle.settle(receipt, settle))
        .expect("ack");
    clock.advance(IDLE_SEAL_MS);
    rt.block_on(node.handle.damage()).expect("a tick");
    rt.block_on(node.handle.compact()).expect("compact");
    clock.advance(RETAIN_MS);
    rt.block_on(node.handle.compact()).expect("compact");
    let live: Vec<_> = take(&rt, &node, "g", "s")
        .iter()
        .map(|d| (d.handed.id, d.handed.address))
        .collect();
    assert_eq!(live, before[2..]);
    stop(&rt, node).expect("clean stop");
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let after: Vec<_> = take(&rt, &node, "g", "s")
        .iter()
        .map(|d| (d.handed.id, d.handed.address))
        .collect();
    assert_eq!(after, before[2..]);
    stop(&rt, node).expect("clean stop");
}
