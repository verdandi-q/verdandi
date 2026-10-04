use std::ops::Bound;
use std::path::Path;

use verdandi_core::lease::Settle;
use verdandi_sim::fault_fs::FaultFs;
use verdandi_store::engine::{IDLE_SEAL_MS, SettleBy, Take, Taken, TestClock};

use crate::kit::{DATA, Node, boot, ids, publish, runtime, stop, take};

fn ack_all(rt: &tokio::runtime::Runtime, node: &Node, group: &str) {
    let Ok(Taken::Leased {
        receipt, messages, ..
    }) = rt.block_on(node.handle.take(Take {
        group: group.to_owned(),
        selector: "s".to_owned(),
        limit: 10,
        lease_ms: 30_000,
        wait: false,
    }))
    else {
        panic!("expected a lease");
    };
    let ack = Settle {
        ack: messages.iter().map(|d| d.handed.id).collect(),
        ..Settle::default()
    };
    rt.block_on(node.handle.settle(receipt, SettleBy::Ids(ack)))
        .expect("ack");
}

#[test]
fn i12_generations_merge_by_the_highest_and_acks_by_union_across_segments() {
    let rt = runtime();
    let fs = FaultFs::new(12, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let first = publish(&rt, &node, "s", b"a").expect("publish");
    ack_all(&rt, &node, "g");
    ack_all(&rt, &node, "h");
    clock.advance(IDLE_SEAL_MS);
    rt.block_on(node.handle.damage()).expect("tick");
    rt.block_on(node.handle.replay("g".to_owned(), Bound::Unbounded))
        .expect("replay");
    rt.block_on(node.handle.replay("g".to_owned(), Bound::Excluded(first)))
        .expect("replay");
    let second = publish(&rt, &node, "s", b"b").expect("publish");
    stop(&rt, node).expect("clean stop");
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    assert_eq!(ids(&take(&rt, &node, "g", "s")), [second]);
    assert_eq!(ids(&take(&rt, &node, "h", "s")), [second]);
    let tombstone = rt
        .block_on(node.handle.delete_stream("s".to_owned()))
        .expect("delete");
    assert!(tombstone > second);
    stop(&rt, node).expect("clean stop");
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    assert!(take(&rt, &node, "k", "s").is_empty());
    stop(&rt, node).expect("clean stop");
}
