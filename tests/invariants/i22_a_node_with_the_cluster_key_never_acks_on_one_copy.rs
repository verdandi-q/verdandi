use std::io::Read;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

#[test]
fn i22_a_node_with_the_cluster_key_refuses_to_start() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/tmp");
    std::fs::create_dir_all(&root).expect("scratch dir");
    let dir = TempDir::new_in(root).expect("temp dir");
    let mut child = Command::new(env!("CARGO_BIN_EXE_verdandi"))
        .current_dir(dir.path())
        .env("VERDANDI_CLUSTER_KEY", "k")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the binary");
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = child.try_wait().expect("try wait") {
            break Some(status);
        }
        if Instant::now() > deadline {
            child.kill().expect("kill");
            child.wait().expect("wait");
            break None;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .expect("stderr")
        .read_to_string(&mut stderr)
        .expect("read stderr");
    let status = status.expect("the binary started with a cluster key");
    assert!(!status.success());
    assert!(stderr.contains("VERDANDI_CLUSTER_KEY"), "{stderr}");
    assert!(!dir.path().join("data").exists());
}

#[test]
fn i22_a_message_in_a_stream_no_group_reads_is_never_deleted_to_free_disk() {
    let rt = crate::kit::runtime();
    let fs = verdandi_sim::fault_fs::FaultFs::new(22, 1 << 40);
    let clock = verdandi_store::engine::TestClock::new(1_000_000);
    let node = crate::kit::boot(&rt, &fs, Path::new(crate::kit::DATA), &clock);
    let id = crate::kit::publish(&rt, &node, "lone", &[b'x'; 8192]).expect("publish");
    clock.advance(verdandi_store::engine::IDLE_SEAL_MS);
    rt.block_on(node.handle.damage()).expect("a tick");
    rt.block_on(node.handle.compact()).expect("compact");
    fs.set_capacity(fs.used() + (200 << 20));
    clock.advance(1);
    rt.block_on(node.handle.compact())
        .expect("compact under pressure");
    assert_eq!(
        crate::kit::publish(&rt, &node, "other", b"refused"),
        Err(verdandi_store::engine::Error::NoSpace)
    );
    let taken = crate::kit::ids(&crate::kit::take(&rt, &node, "reader", "lone"));
    assert_eq!(taken, [id]);
    crate::kit::stop(&rt, node).expect("clean stop");
}
