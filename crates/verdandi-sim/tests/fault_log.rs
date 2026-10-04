use std::path::Path;

use verdandi_sim::fault_fs::FaultFs;
use verdandi_sim::log_sim::run;
use verdandi_store::block::{PAGE, SEGMENT_BYTES};
use verdandi_store::fs::{Fs, FsFile};

const SEEDS: u64 = 100;

#[test]
fn fault_log_keeps_every_synced_block_and_names_every_flip() {
    for seed in 0..SEEDS {
        run(seed);
    }
}

#[test]
fn flip_hits_only_written_pages() {
    let fs = FaultFs::new(9, 1 << 40);
    let path = Path::new("/data/seg.vdl");
    let file = fs.create(path).expect("create");
    file.allocate(SEGMENT_BYTES).expect("allocate");
    file.write_at(&[1; 10], 5 * PAGE).expect("write");
    file.sync_data().expect("sync");
    file.write_at(&[2; 10], 2 * PAGE).expect("write");
    for _ in 0..100 {
        let (flipped, at) = fs.flip().expect("a written page");
        assert_eq!(flipped, path);
        assert!((5 * PAGE..6 * PAGE).contains(&at), "{at}");
    }
}

#[test]
fn empty_write_past_the_end_does_not_grow_the_file() {
    let fs = FaultFs::new(10, 1 << 40);
    let file = fs.create(Path::new("/data/a.vdl")).expect("create");
    file.write_at(&[], 1 << 30).expect("write");
    assert_eq!(file.size().expect("size"), 0);
}

#[test]
fn a_held_sync_waits_until_its_release() {
    let fs = FaultFs::new(11, 1 << 40);
    let file = fs.create(Path::new("/data/a.vdl")).expect("create");
    file.write_at(&[1; 10], 0).expect("write");
    fs.hold_syncs();
    let syncing = std::thread::spawn(move || file.sync_data());
    while fs.syncs_held() == 0 {
        std::thread::yield_now();
    }
    assert!(!syncing.is_finished());
    fs.release_syncs();
    syncing.join().expect("join").expect("sync");
    assert_eq!(fs.syncs_held(), 0);
}
