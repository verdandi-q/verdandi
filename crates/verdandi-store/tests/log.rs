use std::fs::OpenOptions;
use std::io;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};

use tempfile::TempDir;
use verdandi_core::ids::SegmentId;
use verdandi_sim::fault_fs::{Crash, FaultFs};
use verdandi_store::block::{
    BLOCK_HEADER_LEN, Bad, BlockHeader, FILE_HEADER_LEN, FileHeader, FileKind, MAX_BLOCK, PAGE,
    SEGMENT_BYTES, align, decode_block, encode_block,
};
use verdandi_store::fs::{Fs, FsFile, RealFs};
use verdandi_store::log::{Error, Log, SealPoint, Written, fits, seal_at, segment_file_name};
use verdandi_store::recovery::{Recovered, Warning, recover};

const INCARNATION: u64 = 0x1111_2222_3333_4444;
const DATA: &str = "/data";

fn tmp() -> TempDir {
    std::fs::create_dir_all(env!("CARGO_TARGET_TMPDIR")).expect("target tmp dir");
    tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("temp dir")
}

fn start<F: Fs + Clone>(fs: &F, dir: &Path) -> Log {
    let recovered = recover(fs, dir, |_, _, _| {}).expect("recover");
    Log::start(fs.clone(), INCARNATION, recovered)
}

fn append_sized(log: &Log, body: &[u8], preallocate: u64) -> Result<Written, Error> {
    log.append(7, body.to_vec(), preallocate)
        .blocking_recv()
        .expect("writer replies")
}

fn append(log: &Log, body: &[u8]) -> Result<Written, Error> {
    append_sized(log, body, SEGMENT_BYTES)
}

fn seal(log: &Log) -> Result<(), Error> {
    log.seal().blocking_recv().expect("writer replies")
}

struct Seen {
    blocks: Vec<(PathBuf, u64, Vec<u8>)>,
    recovered: Recovered,
}

fn recover_all<F: Fs>(fs: &F, dir: &Path) -> Result<Seen, Error> {
    let mut blocks = Vec::new();
    let recovered = recover(fs, dir, |path, header, body| {
        blocks.push((path.to_path_buf(), header.offset, body.to_vec()))
    })?;
    Ok(Seen { blocks, recovered })
}

fn served(seen: &Seen) -> Vec<Vec<u8>> {
    seen.blocks
        .iter()
        .map(|(_, _, body)| body.clone())
        .collect()
}

fn path_of(dir: &Path, written: Written) -> PathBuf {
    dir.join(segment_file_name(written.segment))
}

fn end_of(written: Written, body: &[u8]) -> u64 {
    written.offset + (BLOCK_HEADER_LEN + body.len()) as u64
}

fn three_blocks(dir: &Path) -> (Vec<Written>, Vec<Vec<u8>>) {
    let log = start(&RealFs, dir);
    let bodies: Vec<Vec<u8>> = (1..=3u8).map(|i| vec![i; 5000]).collect();
    let written = bodies
        .iter()
        .map(|b| append(&log, b).expect("append"))
        .collect();
    (written, bodies)
}

fn patch(path: &Path, offset: u64, bytes: &[u8]) {
    let file = OpenOptions::new().write(true).open(path).expect("open");
    file.write_all_at(bytes, offset).expect("patch");
}

fn read(path: &Path, offset: u64, len: usize) -> Vec<u8> {
    let file = OpenOptions::new().read(true).open(path).expect("open");
    let mut buf = vec![0; len];
    file.read_exact_at(&mut buf, offset).expect("read");
    buf
}

fn flip(path: &Path, offset: u64) {
    let byte = read(path, offset, 1)[0];
    patch(path, offset, &[byte ^ 0x5a]);
}

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).expect("metadata").len()
}

#[test]
fn checksum_binds_segment_offset_and_epoch() {
    let header = BlockHeader {
        segment: SegmentId {
            incarnation: 9,
            seq: 3,
        },
        offset: PAGE,
        epoch: 4,
        compaction_version: 0,
        commit_ms: 1,
    };
    let block = encode_block(&header, b"body");
    let mut data = vec![0; 3 * PAGE as usize];
    data[PAGE as usize..PAGE as usize + block.len()].copy_from_slice(&block);
    assert_eq!(decode_block(&data, PAGE), Ok((header, &b"body"[..])));
    let mut moved = vec![0; 3 * PAGE as usize];
    moved[2 * PAGE as usize..2 * PAGE as usize + block.len()].copy_from_slice(&block);
    assert_eq!(decode_block(&moved, 2 * PAGE), Err(Bad::Damaged));
    for field in [6, 14, 18, 26] {
        let mut tampered = data.clone();
        tampered[PAGE as usize + field] ^= 1;
        assert_eq!(
            decode_block(&tampered, PAGE),
            Err(Bad::Damaged),
            "field {field}"
        );
    }

    let dir = tmp();
    let (written, _) = three_blocks(dir.path());
    let mut source = std::fs::read(path_of(dir.path(), written[0])).expect("read");
    source.truncate(end_of(written[2], &[3; 5000]) as usize);
    let other = SegmentId {
        incarnation: INCARNATION,
        seq: 9,
    };
    std::fs::write(dir.path().join(segment_file_name(other)), &source).expect("copy");
    let mut stale = source.clone();
    stale[..FILE_HEADER_LEN].copy_from_slice(
        &FileHeader {
            kind: FileKind::Compacted,
            epoch: 99,
        }
        .encode(),
    );
    std::fs::write(dir.path().join(format!("cmp-{:016x}.vdl", 99)), &stale).expect("copy");
    let seen = recover_all(&RealFs, dir.path()).expect("recover");
    assert_eq!(seen.blocks.len(), 3);
    assert!(
        seen.blocks
            .iter()
            .all(|(path, _, _)| *path == path_of(dir.path(), written[0]))
    );
    assert_eq!(seen.recovered.warnings.len(), 2);
}

#[test]
fn zero_tail_is_cut_without_warning() {
    let dir = tmp();
    let (written, bodies) = three_blocks(dir.path());
    let path = path_of(dir.path(), written[2]);
    patch(
        &path,
        written[2].offset,
        &vec![0; BLOCK_HEADER_LEN + bodies[2].len()],
    );
    let seen = recover_all(&RealFs, dir.path()).expect("recover");
    assert_eq!(served(&seen), bodies[..2].to_vec());
    assert_eq!(seen.recovered.warnings, Vec::new());
    assert_eq!(file_len(&path), end_of(written[1], &bodies[1]));
}

#[test]
fn bad_tail_with_other_bytes_warns_until_the_caller_seals_it() {
    let dir = tmp();
    let (written, bodies) = three_blocks(dir.path());
    let path = path_of(dir.path(), written[2]);
    flip(&path, written[2].offset + 100);
    let warning = Warning {
        file: path.clone(),
        range: written[2].offset..end_of(written[2], &bodies[2]),
    };
    let point = SealPoint {
        file: path.clone(),
        end: end_of(written[1], &bodies[1]),
    };
    for restart in 0..2 {
        let seen = recover_all(&RealFs, dir.path()).expect("recover");
        assert_eq!(served(&seen), bodies[..2].to_vec(), "restart {restart}");
        assert_eq!(
            seen.recovered.warnings,
            vec![warning.clone()],
            "restart {restart}"
        );
        assert_eq!(
            seen.recovered.unsealed,
            vec![point.clone()],
            "restart {restart}"
        );
    }
    assert_eq!(file_len(&path), SEGMENT_BYTES);
    seal_at(&RealFs, &point).expect("seal");
    assert_eq!(file_len(&path), point.end);
}

#[test]
fn damage_in_the_middle_is_marked_and_later_blocks_kept() {
    for (case, at) in [("body", 300), ("length field", 46)] {
        let dir = tmp();
        let (written, bodies) = three_blocks(dir.path());
        let path = path_of(dir.path(), written[1]);
        flip(&path, written[1].offset + at);
        let warning = Warning {
            file: path.clone(),
            range: written[1].offset..written[2].offset,
        };
        for restart in 0..2 {
            let seen = recover_all(&RealFs, dir.path()).expect("recover");
            assert_eq!(
                served(&seen),
                vec![bodies[0].clone(), bodies[2].clone()],
                "{case} restart {restart}"
            );
            assert_eq!(seen.recovered.warnings, vec![warning.clone()], "{case}");
        }
    }
}

#[test]
fn damaged_file_header_is_recovered_from_block_headers() {
    let dir = tmp();
    let (written, bodies) = three_blocks(dir.path());
    let path = path_of(dir.path(), written[0]);
    flip(&path, 8);
    let seen = recover_all(&RealFs, dir.path()).expect("recover");
    assert_eq!(served(&seen), bodies);
    assert_eq!(seen.recovered.next_epoch, 2);
    assert_eq!(
        seen.recovered.warnings,
        vec![Warning {
            file: path,
            range: 0..FILE_HEADER_LEN as u64,
        }]
    );
}

#[test]
fn recovered_segment_is_sealed_and_never_appended_to() {
    let dir = tmp();
    let (written, bodies) = three_blocks(dir.path());
    let path = path_of(dir.path(), written[0]);
    assert_eq!(file_len(&path), SEGMENT_BYTES);
    let seen = recover_all(&RealFs, dir.path()).expect("recover");
    let sealed = end_of(written[2], &bodies[2]);
    assert_eq!(file_len(&path), sealed);
    let log = Log::start(RealFs, INCARNATION + 1, seen.recovered);
    let next = append(&log, b"after restart").expect("append");
    assert_ne!(path_of(dir.path(), next), path);
    assert_eq!(file_len(&path), sealed);
}

#[test]
fn seal_truncates_to_the_last_block_then_syncs() {
    let fs = FaultFs::new(1, 1 << 40);
    let log = start(&fs, Path::new(DATA));
    append(&log, b"one").expect("append");
    let last = append(&log, b"two").expect("append");
    seal(&log).expect("seal");
    drop(log);
    let fs = fs.crash(Crash::LoseAll);
    let file = fs.open(&path_of(Path::new(DATA), last)).expect("open");
    assert_eq!(file.size().expect("size"), end_of(last, b"two"));
}

#[test]
fn new_segment_and_its_directory_entry_are_synced_before_the_reply() {
    let fs = FaultFs::new(2, 1 << 40);
    let log = start(&fs, Path::new(DATA));
    append(&log, b"durable").expect("append");
    drop(log);
    let fs = fs.crash(Crash::LoseAll);
    let seen = recover_all(&fs, Path::new(DATA)).expect("recover");
    assert_eq!(served(&seen), vec![b"durable".to_vec()]);
}

#[test]
fn partial_write_with_enospc_seals_and_next_block_goes_to_a_new_segment() {
    let fs = FaultFs::new(3, 1 << 40);
    let log = start(&fs, Path::new(DATA));
    let first = append(&log, b"first").expect("append");
    fs.fail_next_write(io::ErrorKind::StorageFull, 30);
    assert!(matches!(append(&log, b"refused"), Err(Error::NoSpace)));
    let sealed = fs.open(&path_of(Path::new(DATA), first)).expect("open");
    assert_eq!(sealed.size().expect("size"), end_of(first, b"first"));
    let third = append(&log, b"third").expect("append");
    assert_eq!(third.segment.seq, first.segment.seq + 1);
    drop(log);
    let fs = fs.crash(Crash::LoseAll);
    let seen = recover_all(&fs, Path::new(DATA)).expect("recover");
    assert_eq!(served(&seen), vec![b"first".to_vec(), b"third".to_vec()]);
    assert_eq!(seen.recovered.warnings, Vec::new());
}

#[test]
fn eio_on_a_write_or_a_failed_sync_stops_the_log() {
    for case in ["write", "sync"] {
        let fs = FaultFs::new(4, 1 << 40);
        let log = start(&fs, Path::new(DATA));
        append(&log, b"ok").expect("append");
        match case {
            "write" => fs.fail_next_write(io::ErrorKind::Other, 10),
            _ => fs.fail_next_sync(),
        }
        assert!(
            matches!(append(&log, b"fails"), Err(Error::Io(_))),
            "{case}"
        );
        assert!(
            matches!(append(&log, b"after"), Err(Error::Stopped)),
            "{case}"
        );
        assert!(matches!(seal(&log), Err(Error::Stopped)), "{case}");
    }
}

#[test]
fn new_segment_preallocates_the_size_the_caller_gives() {
    let fs = FaultFs::new(6, 1 << 40);
    let log = start(&fs, Path::new(DATA));
    let written = append_sized(&log, b"small", 1 << 20).expect("append");
    let file = fs.open(&path_of(Path::new(DATA), written)).expect("open");
    assert_eq!(file.size().expect("size"), 1 << 20);
}

#[test]
fn preallocation_falls_back_to_the_first_block_on_enospc() {
    let fs = FaultFs::new(7, 10 << 20);
    let log = start(&fs, Path::new(DATA));
    let ack = vec![1; 200];
    let first = append(&log, &ack).expect("append");
    let file = fs.open(&path_of(Path::new(DATA), first)).expect("open");
    assert_eq!(file.size().expect("size"), end_of(first, &ack));
    let second = append(&log, &ack).expect("append");
    assert_eq!(second.segment, first.segment);
}

#[test]
fn failed_file_header_write_removes_the_new_segment() {
    let fs = FaultFs::new(8, 1 << 40);
    let log = start(&fs, Path::new(DATA));
    fs.fail_next_write(io::ErrorKind::StorageFull, 5);
    assert!(matches!(append(&log, b"refused"), Err(Error::NoSpace)));
    assert_eq!(
        fs.list(Path::new(DATA)).expect("list"),
        Vec::<String>::new()
    );
}

#[test]
fn new_segment_is_preallocated() {
    let dir = tmp();
    let log = start(&RealFs, dir.path());
    let written = append(&log, b"x").expect("append");
    let meta = std::fs::metadata(path_of(dir.path(), written)).expect("metadata");
    assert_eq!(meta.len(), SEGMENT_BYTES);
    assert!(meta.blocks() * 512 >= SEGMENT_BYTES);
}

#[test]
fn tmpfs_is_refused_at_start() {
    let shm = Path::new("/dev/shm");
    assert!(RealFs.statfs(shm).expect("statfs").memory_backed);
    let refused = recover(&RealFs, shm, |_, _, _| {});
    assert!(matches!(refused, Err(Error::MemoryBacked(_))));
}

#[test]
fn block_over_32_mib_is_refused_and_a_full_segment_rolls() {
    let fs = FaultFs::new(5, 1 << 40);
    let log = start(&fs, Path::new(DATA));
    let max = MAX_BLOCK - BLOCK_HEADER_LEN;
    assert!(matches!(
        append(&log, &vec![1; max + 1]),
        Err(Error::TooLarge(n)) if n == MAX_BLOCK + 1
    ));
    let big = vec![2; 30 << 20];
    let written: Vec<Written> = (0..3)
        .map(|_| append(&log, &big).expect("append"))
        .collect();
    assert_eq!(written[0].segment, written[1].segment);
    assert_eq!(written[2].segment.seq, written[1].segment.seq + 1);
    let len = (BLOCK_HEADER_LEN + big.len()) as u64;
    assert!(fits(end_of(written[0], &big), len));
    assert!(!fits(end_of(written[1], &big), len));
    let first = fs
        .open(&path_of(Path::new(DATA), written[1]))
        .expect("open");
    assert_eq!(first.size().expect("size"), end_of(written[1], &big));
}

fn set_version(path: &Path, header_at: u64, crc_len: usize, body_len: usize, fix_crc: bool) {
    patch(path, header_at + 4, &2u16.to_le_bytes());
    if fix_crc {
        let fields = read(path, header_at, crc_len);
        let body = read(path, header_at + crc_len as u64 + 4, body_len);
        let crc = crc32c::crc32c_append(crc32c::crc32c(&fields), &body);
        patch(path, header_at + crc_len as u64, &crc.to_le_bytes());
    }
}

#[test]
fn version_headers_carry_version_and_crc() {
    let header = FileHeader {
        kind: FileKind::Segment,
        epoch: 3,
    }
    .encode();
    assert_eq!(header[4..6], 1u16.to_le_bytes());
    let mut changed = header;
    changed[4] = 2;
    assert_eq!(FileHeader::decode(&changed), Err(Bad::Damaged));
    let block = encode_block(
        &BlockHeader {
            segment: SegmentId {
                incarnation: 1,
                seq: 0,
            },
            offset: 0,
            epoch: 3,
            compaction_version: 0,
            commit_ms: 0,
        },
        b"v",
    );
    assert_eq!(block[4..6], 1u16.to_le_bytes());
    let mut changed = block.clone();
    changed[4] = 2;
    assert_eq!(decode_block(&changed, 0), Err(Bad::Damaged));
}

#[test]
fn version_unknown_in_a_valid_header_refuses_start() {
    for kind in ["file", "block"] {
        let dir = tmp();
        let (written, bodies) = three_blocks(dir.path());
        let path = path_of(dir.path(), written[0]);
        match kind {
            "file" => set_version(&path, 0, 15, 0, true),
            _ => set_version(&path, written[1].offset, 50, bodies[1].len(), true),
        }
        let refused = recover_all(&RealFs, dir.path()).map(|_| ());
        let Err(error @ Error::UnknownVersion { version: 2, .. }) = &refused else {
            panic!("{kind}: start not refused: {refused:?}");
        };
        let message = error.to_string();
        assert!(message.contains("version 2"), "{message}");
        assert!(
            message.contains(&segment_file_name(written[0].segment)),
            "{message}"
        );
    }
}

#[test]
fn version_change_with_a_bad_crc_is_damage_not_a_refusal() {
    let dir = tmp();
    let (written, bodies) = three_blocks(dir.path());
    let path = path_of(dir.path(), written[0]);
    set_version(&path, 0, 15, 0, false);
    set_version(&path, written[1].offset, 50, bodies[1].len(), false);
    let seen = recover_all(&RealFs, dir.path()).expect("recover");
    assert_eq!(served(&seen), vec![bodies[0].clone(), bodies[2].clone()]);
    assert_eq!(seen.recovered.warnings.len(), 2);
}

#[test]
fn false_header_with_a_wrong_offset_or_segment_is_damage_not_a_refusal() {
    for case in ["offset", "segment"] {
        let dir = tmp();
        let (written, bodies) = three_blocks(dir.path());
        let path = path_of(dir.path(), written[0]);
        let at = align(end_of(written[2], &bodies[2]));
        let mut header = BlockHeader {
            segment: written[0].segment,
            offset: at,
            epoch: 1,
            compaction_version: 0,
            commit_ms: 7,
        };
        match case {
            "offset" => header.offset += PAGE,
            _ => header.segment.seq += 1,
        }
        patch(&path, at, &encode_block(&header, b"false"));
        set_version(&path, at, 50, 5, true);
        let seen = recover_all(&RealFs, dir.path()).expect(case);
        assert_eq!(served(&seen), bodies, "{case}");
        assert_eq!(seen.recovered.warnings.len(), 1, "{case}");
    }
}

#[test]
fn false_header_with_an_unknown_version_in_a_damaged_block_is_damage_not_a_refusal() {
    let dir = tmp();
    let (written, bodies) = three_blocks(dir.path());
    let path = path_of(dir.path(), written[0]);
    let at = written[1].offset + PAGE;
    let header = BlockHeader {
        segment: written[0].segment,
        offset: at,
        epoch: 1,
        compaction_version: 0,
        commit_ms: 7,
    };
    patch(&path, at, &encode_block(&header, b"false"));
    set_version(&path, at, 50, 5, true);
    let seen = recover_all(&RealFs, dir.path()).expect("recover");
    assert_eq!(served(&seen), vec![bodies[0].clone(), bodies[2].clone()]);
    assert_eq!(seen.recovered.warnings.len(), 1);
}

#[test]
fn blocks_that_recovery_reads_after_a_process_kill_survive_a_power_loss() {
    let one = b"one".to_vec();
    let two = b"two".to_vec();
    for (freeze, first_ok, kept) in [(6, false, vec![one.clone()]), (8, true, vec![one, two])] {
        let fs = FaultFs::new(11, 1 << 40);
        let log = start(&fs, Path::new(DATA));
        fs.freeze_after(freeze);
        assert_eq!(append_sized(&log, b"one", 0).is_ok(), first_ok, "{freeze}");
        assert!(append_sized(&log, b"two", 0).is_err(), "{freeze}");
        drop(log);
        let fs = fs.crash(Crash::Kill);
        let seen = recover_all(&fs, Path::new(DATA)).expect("recover");
        assert_eq!(served(&seen), kept, "{freeze}: after the kill");
        let fs = fs.crash(Crash::LoseAll);
        let seen = recover_all(&fs, Path::new(DATA)).expect("recover");
        assert_eq!(served(&seen), kept, "{freeze}: after the power loss");
    }
}

#[test]
fn file_system_without_fallocate_writes_with_no_preallocation() {
    let fs = FaultFs::new(12, 1 << 40);
    fs.refuse_allocate();
    let log = start(&fs, Path::new(DATA));
    let first = append(&log, b"one").expect("append");
    let second = append(&log, b"two").expect("append");
    assert_eq!(second.segment, first.segment);
    drop(log);
    let fs = fs.crash(Crash::LoseAll);
    let seen = recover_all(&fs, Path::new(DATA)).expect("recover");
    assert_eq!(served(&seen), vec![b"one".to_vec(), b"two".to_vec()]);
}

#[test]
fn enospc_on_the_first_block_of_a_new_segment_removes_the_segment() {
    let fs = FaultFs::new(13, PAGE);
    fs.refuse_allocate();
    let log = start(&fs, Path::new(DATA));
    for retry in 0..3 {
        assert!(
            matches!(append(&log, b"refused"), Err(Error::NoSpace)),
            "{retry}"
        );
        assert_eq!(
            fs.list(Path::new(DATA)).expect("list"),
            Vec::<String>::new(),
            "{retry}"
        );
    }
}

#[test]
fn block_whose_sync_failed_is_cut_so_recovery_never_serves_it() {
    let fs = FaultFs::new(14, 1 << 40);
    let log = start(&fs, Path::new(DATA));
    append(&log, b"one").expect("append");
    fs.fail_next_sync();
    assert!(matches!(append(&log, b"two"), Err(Error::Io(_))));
    drop(log);
    let fs = fs.crash(Crash::Kill);
    let seen = recover_all(&fs, Path::new(DATA)).expect("recover");
    assert_eq!(served(&seen), vec![b"one".to_vec()], "after the kill");
    let fs = fs.crash(Crash::LoseAll);
    let seen = recover_all(&fs, Path::new(DATA)).expect("recover");
    assert_eq!(served(&seen), vec![b"one".to_vec()], "after the power loss");
}

#[test]
fn quota_exceeded_is_no_space_and_the_log_continues() {
    let fs = FaultFs::new(15, 1 << 40);
    let log = start(&fs, Path::new(DATA));
    append(&log, b"first").expect("append");
    fs.fail_next_write(io::ErrorKind::QuotaExceeded, 0);
    let refused = append(&log, b"refused");
    assert!(matches!(refused, Err(Error::NoSpace)), "{refused:?}");
    append(&log, b"third").expect("append");
}

#[test]
fn file_over_the_segment_size_refuses_start_and_names_the_file() {
    let fs = FaultFs::new(16, 1 << 40);
    let name = segment_file_name(SegmentId {
        incarnation: INCARNATION,
        seq: 0,
    });
    let file = fs.create(&Path::new(DATA).join(&name)).expect("create");
    file.set_len(SEGMENT_BYTES + 1).expect("set length");
    let refused = recover_all(&fs, Path::new(DATA)).map(|_| ());
    let Err(error) = &refused else {
        panic!("start not refused: {refused:?}");
    };
    assert!(error.to_string().contains(&name), "{error}");
}

#[test]
fn close_waits_for_every_queued_append() {
    let fs = FaultFs::new(17, 1 << 40);
    let log = start(&fs, Path::new(DATA));
    let reply = log.append(7, b"queued".to_vec(), SEGMENT_BYTES);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    runtime.block_on(log.close());
    assert!(matches!(reply.blocking_recv(), Ok(Ok(_))));
    let fs = fs.crash(Crash::LoseAll);
    let seen = recover_all(&fs, Path::new(DATA)).expect("recover");
    assert_eq!(served(&seen), vec![b"queued".to_vec()]);
}

#[test]
fn eio_on_a_write_cuts_the_partial_bytes_before_the_stop() {
    let fs = FaultFs::new(18, 1 << 40);
    let log = start(&fs, Path::new(DATA));
    let first = append(&log, b"ok").expect("append");
    fs.fail_next_write(io::ErrorKind::Other, 60);
    assert!(matches!(append(&log, &[9; 200]), Err(Error::Io(_))));
    drop(log);
    let fs = fs.crash(Crash::Kill);
    let seen = recover_all(&fs, Path::new(DATA)).expect("recover");
    assert_eq!(seen.recovered.warnings, Vec::new());
    assert_eq!(served(&seen), vec![b"ok".to_vec()]);
    let file = fs.open(&path_of(Path::new(DATA), first)).expect("open");
    assert_eq!(file.size().expect("size"), end_of(first, b"ok"));
}

#[test]
fn segment_removed_on_enospc_stays_removed_after_a_power_loss() {
    for seed in 0..16 {
        for case in ["preallocation", "first block"] {
            let fs = FaultFs::new(seed, PAGE);
            if case == "first block" {
                fs.refuse_allocate();
            }
            let log = start(&fs, Path::new(DATA));
            assert!(
                matches!(append(&log, b"refused"), Err(Error::NoSpace)),
                "{case}"
            );
            drop(log);
            let fs = fs.crash(Crash::Random);
            assert_eq!(
                fs.list(Path::new(DATA)).expect("list"),
                Vec::<String>::new(),
                "{case} seed {seed}"
            );
        }
    }
}

fn name_of(path: &Path) -> String {
    path.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default()
        .to_owned()
}

fn engine_on(
    rt: &tokio::runtime::Runtime,
    fs: &FaultFs,
    clock: &verdandi_store::engine::TestClock,
) -> (
    verdandi_store::engine::Handle,
    tokio::task::JoinHandle<Result<(), verdandi_store::engine::Error>>,
) {
    let (engine, handle) = rt
        .block_on(verdandi_store::engine::Engine::open(
            fs.clone(),
            Path::new(DATA),
            std::sync::Arc::new(clock.clone()),
            Box::new(verdandi_store::engine::OsRandom),
        ))
        .expect("open");
    let task = rt.spawn(engine.run());
    (handle, task)
}

fn publish_one(
    rt: &tokio::runtime::Runtime,
    handle: &verdandi_store::engine::Handle,
    value: &[u8],
) {
    let message = verdandi_store::engine::NewMessage {
        stream: "s".to_owned(),
        headers: Vec::new(),
        value: value.to_vec(),
        timing: verdandi_core::Timing::default(),
    };
    rt.block_on(handle.publish(vec![message], None))
        .expect("publish");
}

fn seal_and_compact(
    rt: &tokio::runtime::Runtime,
    handle: &verdandi_store::engine::Handle,
    clock: &verdandi_store::engine::TestClock,
) {
    clock.advance(verdandi_store::engine::IDLE_SEAL_MS);
    rt.block_on(handle.damage()).expect("a tick");
    rt.block_on(handle.compact()).expect("compact");
}

#[test]
fn after_compaction_and_a_restart_the_damage_warning_is_still_there() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let fs = FaultFs::new(41, 1 << 40);
    let clock = verdandi_store::engine::TestClock::new(1_000_000);
    let (handle, task) = engine_on(&rt, &fs, &clock);
    publish_one(&rt, &handle, &[b'a'; 300]);
    publish_one(&rt, &handle, b"b");
    let take = verdandi_store::engine::Take {
        group: "g".to_owned(),
        selector: "s".to_owned(),
        limit: 10,
        lease_ms: 30_000,
        wait: false,
    };
    let Ok(verdandi_store::engine::Taken::Leased { messages, .. }) =
        rt.block_on(handle.take(take.clone()))
    else {
        panic!("no messages");
    };
    let location = messages[0].location.clone().expect("location");
    let segment = location.start.segment;
    let at = location.run.offset + u64::from(location.run.len) - 1;
    assert!(fs.flip_at(&Path::new(DATA).join(segment_file_name(segment)), at, 0x40));
    assert_eq!(rt.block_on(handle.read(&messages)).expect("read")[0], None);
    publish_one(&rt, &handle, b"barrier");
    seal_and_compact(&rt, &handle, &clock);
    let names = fs.list(Path::new(DATA)).expect("list");
    assert!(
        !names.contains(&segment_file_name(segment)) && names.iter().any(|n| n.starts_with("cmp-")),
        "{names:?}"
    );
    let mut carried = false;
    recover(&fs.snapshot(), Path::new(DATA), |path, _, body| {
        let controls = verdandi_store::records::decode(body)
            .expect("body")
            .controls;
        carried |= name_of(path).starts_with("cmp-")
            && controls.iter().any(|c| {
                matches!(c, verdandi_store::records::Control::Damage(d)
                    if d.segment == segment && (d.start..d.end).contains(&at))
            });
    })
    .expect("recover");
    assert!(carried, "the output holds no damage record of the range");
    drop(handle);
    rt.block_on(task).expect("join").expect("clean stop");
    let (handle, task) = engine_on(&rt, &fs, &clock);
    let (damage, _) = rt.block_on(handle.damage()).expect("damage");
    assert!(
        damage
            .iter()
            .any(|d| d.segment == segment && (d.start..d.end).contains(&at)),
        "{damage:?}"
    );
    drop(handle);
    rt.block_on(task).expect("join").expect("clean stop");
}

#[test]
fn compaction_syncs_the_directory_after_the_rename_and_after_the_delete() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let fs = FaultFs::new(42, 1 << 40);
    let clock = verdandi_store::engine::TestClock::new(1_000_000);
    let (handle, task) = engine_on(&rt, &fs, &clock);
    publish_one(&rt, &handle, b"kept");
    rt.block_on(handle.delete_stream("gone".to_owned()))
        .expect("delete");
    let before = fs.list(Path::new(DATA)).expect("list");
    seal_and_compact(&rt, &handle, &clock);
    drop(handle);
    rt.block_on(task).expect("join").expect("clean stop");
    let after = fs
        .crash(Crash::LoseAll)
        .list(Path::new(DATA))
        .expect("list");
    assert!(
        before.iter().all(|n| !after.contains(n)),
        "{before:?} {after:?}"
    );
    assert!(after.iter().any(|n| n.starts_with("cmp-")), "{after:?}");
}
