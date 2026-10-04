use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::Arc;

use verdandi_store::block::{FILE_HEADER_LEN, FileHeader, PAGE, decode_block};
use verdandi_store::engine::{Engine, OsRandom, TestClock};
use verdandi_store::fs::RealFs;
use verdandi_store::log::Error;

use crate::kit::{boot, publish, runtime, stop};

#[test]
fn i21_engine_blocks_carry_version_1_and_an_unknown_version_refuses_start() {
    let rt = runtime();
    std::fs::create_dir_all(env!("CARGO_TARGET_TMPDIR")).expect("target tmp dir");
    let tmp = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("temp dir");
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &RealFs, tmp.path(), &clock);
    publish(&rt, &node, "s", b"a").expect("publish");
    stop(&rt, node).expect("clean stop");
    let segment: PathBuf = std::fs::read_dir(tmp.path())
        .expect("list")
        .map(|e| e.expect("entry").path())
        .next()
        .expect("a segment");
    let data = std::fs::read(&segment).expect("read");
    assert!(FileHeader::decode(&data).is_ok());
    assert_eq!(data[4..6], 1u16.to_le_bytes());
    assert!(decode_block(&data, PAGE).is_ok());
    assert_eq!(
        data[PAGE as usize + 4..PAGE as usize + 6],
        1u16.to_le_bytes()
    );
    let mut header = [0; FILE_HEADER_LEN];
    header.copy_from_slice(&data[..FILE_HEADER_LEN]);
    header[4] = 2;
    let crc = crc32c::crc32c(&header[..15]);
    header[15..].copy_from_slice(&crc.to_le_bytes());
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&segment)
        .expect("open");
    file.write_all_at(&header, 0).expect("patch");
    let refused = rt.block_on(Engine::open(
        RealFs,
        tmp.path(),
        Arc::new(clock),
        Box::new(OsRandom),
    ));
    assert!(matches!(
        refused.err(),
        Some(Error::UnknownVersion { path, version: 2 }) if path == segment
    ));
}
