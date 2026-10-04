use std::path::Path;

use verdandi_sim::fault_fs::{Crash, FaultFs};
use verdandi_store::engine::{Error, Take, TestClock};

use crate::kit::{DATA, boot, publish, runtime, stop, take};

#[test]
fn i17_a_failed_fsync_stops_the_node_and_the_block_never_counts() {
    let rt = runtime();
    let fs = FaultFs::new(17, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    fs.fail_next_sync();
    assert_eq!(publish(&rt, &node, "s", b"a"), Err(Error::Stopped));
    let taken = rt.block_on(node.handle.take(Take {
        group: "g".to_owned(),
        selector: "s".to_owned(),
        limit: 100,
        lease_ms: 30_000,
        wait: false,
    }));
    assert!(matches!(taken, Err(Error::Stopped)));
    assert_eq!(stop(&rt, node), Err(Error::Stopped));
    let fs = fs.crash(Crash::LoseAll);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    assert!(take(&rt, &node, "g", "s").is_empty());
    stop(&rt, node).expect("clean stop");
}
