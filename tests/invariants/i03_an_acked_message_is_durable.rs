use std::path::Path;

use verdandi_sim::fault_fs::{Crash, FaultFs};
use verdandi_store::engine::TestClock;

use crate::kit::{DATA, boot, ids, publish, runtime, stop, take};

#[test]
fn i03_a_201_publish_survives_a_power_loss() {
    let rt = runtime();
    let fs = FaultFs::new(3, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let id = publish(&rt, &node, "s", b"a").expect("publish");
    let fs = fs.crash(Crash::LoseAll);
    stop(&rt, node).ok();
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    assert_eq!(ids(&take(&rt, &node, "g", "s")), [id]);
    stop(&rt, node).expect("clean stop");
}
