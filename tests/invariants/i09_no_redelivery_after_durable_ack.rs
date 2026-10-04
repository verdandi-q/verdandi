use std::path::Path;

use verdandi_core::lease::Settle;
use verdandi_sim::fault_fs::{Crash, FaultFs};
use verdandi_store::engine::{SettleBy, Take, Taken, TestClock};

use crate::kit::{DATA, boot, ids, publish, runtime, stop, take};

#[test]
fn i09_no_redelivery_after_durable_ack() {
    let rt = runtime();
    let fs = FaultFs::new(9, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let first = publish(&rt, &node, "s", b"a").expect("publish");
    let second = publish(&rt, &node, "s", b"b").expect("publish");
    let Ok(Taken::Leased { receipt, .. }) = rt.block_on(node.handle.take(Take {
        group: "g".to_owned(),
        selector: "s".to_owned(),
        limit: 10,
        lease_ms: 30_000,
        wait: false,
    })) else {
        panic!("expected a lease");
    };
    let ack = Settle {
        ack: vec![first],
        ..Settle::default()
    };
    rt.block_on(node.handle.settle(receipt, SettleBy::Ids(ack)))
        .expect("ack");
    let fs = fs.crash(Crash::LoseAll);
    stop(&rt, node).ok();
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    assert_eq!(ids(&take(&rt, &node, "g", "s")), [second]);
    stop(&rt, node).expect("clean stop");
}
