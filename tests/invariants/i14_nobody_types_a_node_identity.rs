use std::path::Path;

use verdandi_sim::fault_fs::FaultFs;
use verdandi_store::engine::TestClock;

use crate::kit::{DATA, boot, publish, runtime, stop};

#[test]
fn i14_each_start_draws_its_own_identity_with_no_input() {
    let rt = runtime();
    let fs = FaultFs::new(14, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let first = publish(&rt, &node, "s", b"a").expect("publish");
    stop(&rt, node).expect("clean stop");
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let second = publish(&rt, &node, "s", b"b").expect("publish");
    stop(&rt, node).expect("clean stop");
    assert_ne!(first.incarnation, second.incarnation);
}
