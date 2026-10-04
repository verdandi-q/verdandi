use std::path::Path;

use verdandi_store::engine::TestClock;
use verdandi_store::fs::RealFs;
use verdandi_store::records;
use verdandi_store::recovery::recover;

use crate::kit::{boot, publish, runtime, stop};

#[test]
fn i02_a_publish_is_in_the_local_log_when_its_reply_comes() {
    let rt = runtime();
    std::fs::create_dir_all(env!("CARGO_TARGET_TMPDIR")).expect("target tmp dir");
    let tmp = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("temp dir");
    let node = boot(&rt, &RealFs, tmp.path(), &TestClock::new(1_000));
    let id = publish(&rt, &node, "s", b"here").expect("publish");
    let mut found = Vec::new();
    recover(&RealFs, Path::new(tmp.path()), |_, _, body| {
        let body = records::decode(body).expect("body");
        for (entry, bytes) in body.runs {
            found.extend(records::decode_run(bytes, entry.count).expect("run"));
        }
    })
    .expect("recover");
    assert!(found.iter().any(|m| m.id == id && m.value == b"here"));
    stop(&rt, node).expect("clean stop");
}
