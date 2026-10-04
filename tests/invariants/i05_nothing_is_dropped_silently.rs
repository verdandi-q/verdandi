use std::path::Path;

use reqwest::{Client, StatusCode};
use verdandi_sim::fault_fs::FaultFs;
use verdandi_store::engine::TestClock;
use verdandi_store::fs::Fs;

use crate::kit::{DATA, boot, publish, ready, runtime, send, serve, stop};

#[test]
fn i05_a_flipped_block_is_named_in_the_warnings_of_get_root() {
    let rt = runtime();
    let fs = FaultFs::new(5, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    publish(&rt, &node, "s", b"a").expect("publish");
    stop(&rt, node).expect("clean stop");
    let dir = Path::new(DATA);
    let segment = fs
        .list(dir)
        .expect("list")
        .into_iter()
        .find(|name| name.starts_with("seg-"))
        .expect("a segment");
    assert!(fs.flip_at(&dir.join(segment), 4_096 + 40, 0xff));
    let node = boot(&rt, &fs, dir, &clock);
    let url = serve(&rt, ready(&node));
    let (status, root) = rt.block_on(send(Client::new().get(format!("{url}/"))));
    assert_eq!(status, StatusCode::OK);
    let named = root["warnings"]
        .as_array()
        .expect("warnings")
        .iter()
        .filter_map(|w| w.as_str())
        .filter(|w| w.starts_with("damaged log range"))
        .count();
    assert!(named > 0, "{root}");
}
