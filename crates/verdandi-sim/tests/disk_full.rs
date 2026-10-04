use std::io;
use std::path::Path;

use tokio::runtime::Runtime;
use verdandi_core::Timing;
use verdandi_core::ids::MessageId;
use verdandi_core::lease::{Bury, Nack, Receipt, Settle, SettleError};
use verdandi_sim::compaction_sim::{Node, boot, publish, runtime, seal, stop, take};
use verdandi_sim::fault_fs::{Crash, FaultFs};
use verdandi_store::compaction::COMPACT_FREE;
use verdandi_store::engine::{Error, Key, NewMessage, SettleBy, Take, Taken, TestClock};
use verdandi_store::fs::{Fs, FsFile};

const MIB: u64 = 1 << 20;

fn lease(rt: &Runtime, node: &Node, group: &str, stream: &str) -> (Receipt, Vec<MessageId>) {
    let taken = rt.block_on(node.handle.take(Take {
        group: group.to_owned(),
        selector: stream.to_owned(),
        limit: 1_000,
        lease_ms: 600_000,
        wait: false,
    }));
    match taken {
        Ok(Taken::Leased {
            receipt, messages, ..
        }) => (receipt, messages.iter().map(|d| d.handed.id).collect()),
        _ => (Receipt(0), Vec::new()),
    }
}

fn settle(rt: &Runtime, node: &Node, receipt: Receipt, settle: Settle) -> Result<(), Error> {
    rt.block_on(node.handle.settle(receipt, SettleBy::Ids(settle)))
}

fn acks(ids: &[MessageId]) -> Settle {
    Settle {
        ack: ids.to_vec(),
        ..Settle::default()
    }
}

fn press(rt: &Runtime, node: &Node, fs: &FaultFs, clock: &TestClock, free: u64) {
    fs.set_capacity(fs.used() + free);
    clock.advance(1);
    rt.block_on(node.handle.compact()).expect("compact");
}

fn refused(rt: &Runtime, node: &Node) -> bool {
    let message = NewMessage {
        stream: "s".to_owned(),
        headers: Vec::new(),
        value: b"late".to_vec(),
        timing: Timing::default(),
    };
    rt.block_on(node.handle.publish(vec![message], None)) == Err(Error::NoSpace)
}

fn data_files(fs: &FaultFs) -> Vec<String> {
    fs.list(Path::new("/data")).expect("list")
}

fn file_with(fs: &FaultFs, needle: &[u8]) -> String {
    let holding: Vec<String> = data_files(fs)
        .into_iter()
        .filter(|name| {
            let file = fs.open(&Path::new("/data").join(name)).expect("open");
            let mut bytes = vec![0; file.size().expect("size") as usize];
            file.read_at(&mut bytes, 0).expect("read");
            bytes.windows(needle.len()).any(|w| w == needle)
        })
        .collect();
    assert_eq!(holding.len(), 1, "{holding:?}");
    holding[0].clone()
}

#[test]
fn disk_full_acked_messages_are_deleted_oldest_first_then_publishes_are_refused() {
    let rt = runtime();
    let fs = FaultFs::new(1, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    let mut acked = Vec::new();
    for value in [b"first", b"secnd"] {
        acked.push(publish(&rt, &node, "s", &[value])[0]);
        let (receipt, ids) = lease(&rt, &node, "g", "s");
        settle(&rt, &node, receipt, acks(&ids)).expect("ack");
        seal(&rt, &node, &clock);
    }
    let unacked = publish(&rt, &node, "s", &[b"third"]);
    seal(&rt, &node, &clock);
    let held = fs.list(Path::new("/data")).expect("list").len();
    let before = fs.snapshot();
    let (older, newer) = (file_with(&fs, b"first"), file_with(&fs, b"secnd"));
    assert_ne!(older, newer);
    press(&rt, &node, &fs, &clock, 200 * MIB);
    let left = fs.list(Path::new("/data")).expect("list").len();
    assert!(
        left < held,
        "nothing was deleted: {held} files, then {left}"
    );
    assert_eq!(take(&rt, &node, "new", "s"), unacked);
    let (_, by_g) = lease(&rt, &node, "g", "s");
    assert_eq!(by_g, unacked);
    assert!(acked.iter().all(|id| !by_g.contains(id)));
    assert!(refused(&rt, &node), "a publish passed the floor");
    stop(&rt, node).expect("clean stop");
    let mut between = false;
    for step in 1..=200 {
        let fs = before.snapshot();
        let node = boot(&rt, &fs, &clock);
        fs.set_capacity(fs.used() + 200 * MIB);
        fs.freeze_after(step);
        clock.advance(1);
        rt.block_on(node.handle.compact()).ok();
        stop(&rt, node).ok();
        let left = data_files(&fs.crash(Crash::Kill));
        let (old_left, new_left) = (left.contains(&older), left.contains(&newer));
        assert!(
            !old_left || new_left,
            "step {step}: {newer} went before {older}"
        );
        between |= !old_left && new_left;
        if !old_left && !new_left {
            break;
        }
    }
    assert!(
        between,
        "no step had the older file gone and the newer one left"
    );
}

#[test]
fn disk_full_unacked_and_no_group_data_are_never_deleted() {
    let rt = runtime();
    let fs = FaultFs::new(2, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    let lone = publish(&rt, &node, "lone", &[b"no group"]);
    let pending = publish(&rt, &node, "s", &[b"a", b"b"]);
    let (receipt, ids) = lease(&rt, &node, "g", "s");
    settle(&rt, &node, receipt, acks(&ids[..1])).expect("ack");
    seal(&rt, &node, &clock);
    press(&rt, &node, &fs, &clock, 200 * MIB);
    assert_eq!(take(&rt, &node, "x", "lone"), lone);
    clock.advance(600_001);
    let (_, by_g) = lease(&rt, &node, "g", "s");
    assert_eq!(by_g, pending[1..]);
    stop(&rt, node).expect("clean stop");
}

#[test]
fn disk_full_enospc_on_a_compaction_file_skips_that_step_and_the_node_goes_on() {
    let rt = runtime();
    let fs = FaultFs::new(3, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    let kept = publish(&rt, &node, "s", &[b"kept"]);
    publish(&rt, &node, "junk", &[&[b'j'; 4096]]);
    rt.block_on(node.handle.delete_stream("junk".to_owned()))
        .expect("delete");
    fs.fail_next_write(io::ErrorKind::StorageFull, 0);
    seal(&rt, &node, &clock);
    let names = fs.list(Path::new("/data")).expect("list");
    assert!(names.iter().all(|n| !n.ends_with(".tmp")), "{names:?}");
    publish(&rt, &node, "t", &[b"after"]);
    seal(&rt, &node, &clock);
    let names = fs.list(Path::new("/data")).expect("list");
    assert!(names.iter().any(|n| n.starts_with("cmp-")), "{names:?}");
    assert_eq!(take(&rt, &node, "g", "s"), kept);
    assert!(take(&rt, &node, "g", "junk").is_empty());
    stop(&rt, node).expect("clean stop");
}

#[test]
fn disk_full_with_the_active_segment_full_an_ack_a_nack_and_a_forget_succeed_and_a_dead_settle_gets_429()
 {
    let rt = runtime();
    let fs = FaultFs::new(4, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    publish(&rt, &node, "s", &[b"a", b"b", b"c"]);
    let (receipt, ids) = lease(&rt, &node, "g", "s");
    seal(&rt, &node, &clock);
    press(&rt, &node, &fs, &clock, MIB);
    let dead = Settle {
        dead: vec![Bury {
            id: ids[2],
            error: None,
        }],
        ..Settle::default()
    };
    assert_eq!(
        settle(&rt, &node, receipt, dead),
        Err(Error::Refused(SettleError::Pressure))
    );
    settle(&rt, &node, receipt, acks(&ids[..1])).expect("an ack under pressure");
    let nack = Settle {
        nack: vec![Nack {
            id: ids[1],
            delay_ms: None,
            error: None,
        }],
        ..Settle::default()
    };
    settle(&rt, &node, receipt, nack).expect("a nack under pressure");
    rt.block_on(node.handle.forget("g".to_owned()))
        .expect("a forget under pressure");
    assert!(refused(&rt, &node));
    stop(&rt, node).expect("clean stop");
}

#[test]
fn disk_full_a_mass_dead_settle_at_the_floor_gets_429_then_an_ack_and_a_forget_succeed() {
    let rt = runtime();
    let fs = FaultFs::new(5, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    let values: Vec<Vec<u8>> = (0..50).map(|n| vec![n as u8; 1_000]).collect();
    let refs: Vec<&[u8]> = values.iter().map(|v| &v[..]).collect();
    publish(&rt, &node, "s", &refs);
    let (receipt, ids) = lease(&rt, &node, "g", "s");
    press(&rt, &node, &fs, &clock, 64 * MIB);
    let dead = Settle {
        dead: ids
            .iter()
            .map(|&id| Bury {
                id,
                error: Some("bad".to_owned()),
            })
            .collect(),
        ..Settle::default()
    };
    assert_eq!(
        settle(&rt, &node, receipt, dead),
        Err(Error::Refused(SettleError::Pressure))
    );
    settle(&rt, &node, receipt, acks(&ids[..10])).expect("an ack at the floor");
    rt.block_on(node.handle.forget("g".to_owned()))
        .expect("a forget at the floor");
    assert!(take(&rt, &node, "x", "s.dead.g").is_empty());
    stop(&rt, node).expect("clean stop");
}

#[test]
fn disk_full_an_expired_message_under_pressure_gets_its_dead_copy_only_after_the_pressure_ends() {
    let rt = runtime();
    let fs = FaultFs::new(6, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    let message = NewMessage {
        stream: "s".to_owned(),
        headers: Vec::new(),
        value: b"short".to_vec(),
        timing: Timing {
            delay_until: None,
            expire_at: Some(1_000_500),
        },
    };
    rt.block_on(node.handle.publish(vec![message], None))
        .expect("publish");
    press(&rt, &node, &fs, &clock, 64 * MIB);
    clock.advance(1_000);
    assert!(take(&rt, &node, "g", "s").is_empty());
    rt.block_on(node.handle.compact()).expect("a tick");
    assert!(take(&rt, &node, "x", "s.dead.g").is_empty());
    fs.set_capacity(1 << 40);
    clock.advance(1);
    rt.block_on(node.handle.compact())
        .expect("the pressure ends");
    publish(&rt, &node, "barrier", &[b"b"]);
    assert_eq!(take(&rt, &node, "x", "s.dead.g").len(), 1);
    stop(&rt, node).expect("clean stop");
}

#[test]
fn disk_full_pressure_that_deletes_the_block_of_a_key_inside_24_h_keeps_the_key() {
    let rt = runtime();
    let fs = FaultFs::new(7, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    let key = Key {
        scope: "k".to_owned(),
        key: "order-1".to_owned(),
    };
    let message = |value: &[u8]| NewMessage {
        stream: "k".to_owned(),
        headers: Vec::new(),
        value: value.to_vec(),
        timing: Timing::default(),
    };
    let first = rt
        .block_on(
            node.handle
                .publish(vec![message(&[b'v'; 4096])], Some(key.clone())),
        )
        .expect("publish");
    let (receipt, ids) = lease(&rt, &node, "g", "k");
    settle(&rt, &node, receipt, acks(&ids)).expect("ack");
    seal(&rt, &node, &clock);
    press(&rt, &node, &fs, &clock, 200 * MIB);
    assert!(
        take(&rt, &node, "new", "k").is_empty(),
        "the acked message stayed"
    );
    stop(&rt, node).expect("clean stop");
    let node = boot(&rt, &fs, &clock);
    let again = rt
        .block_on(node.handle.publish(vec![message(b"w")], Some(key)))
        .expect("a repeat");
    assert!(again.repeat);
    assert_eq!((again.first, again.count), (first.first, first.count));
    stop(&rt, node).expect("clean stop");
}

#[test]
fn disk_full_under_192_mib_free_compaction_writes_no_output() {
    let rt = runtime();
    let fs = FaultFs::new(8, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    publish(&rt, &node, "s", &[b"held"]);
    lease(&rt, &node, "g", "s");
    seal(&rt, &node, &clock);
    publish(&rt, &node, "s", &[b"acked"]);
    let (receipt, ids) = lease(&rt, &node, "g", "s");
    settle(&rt, &node, receipt, acks(&ids)).expect("ack");
    seal(&rt, &node, &clock);
    publish(&rt, &node, "s", &[b"kept"]);
    let big = publish(&rt, &node, "s", &[&[b'a'; 4096]]);
    let (receipt, _) = lease(&rt, &node, "g", "s");
    settle(&rt, &node, receipt, acks(&big)).expect("ack");
    seal(&rt, &node, &clock);
    let before = fs.list(Path::new("/data")).expect("list");
    press(&rt, &node, &fs, &clock, 64 * MIB);
    let after = fs.list(Path::new("/data")).expect("list");
    assert!(
        after.iter().all(|n| before.contains(n)),
        "{before:?} {after:?}"
    );
    stop(&rt, node).expect("clean stop");
}

#[test]
fn disk_full_a_compaction_output_never_takes_free_space_under_192_mib() {
    let rt = runtime();
    let fs = FaultFs::new(9, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    let mib = vec![0u8; MIB as usize];
    let values: Vec<&[u8]> = (0..10).map(|_| &mib[..]).collect();
    publish(&rt, &node, "s", &values);
    publish(&rt, &node, "junk", &[b"junk"]);
    seal(&rt, &node, &clock);
    rt.block_on(node.handle.delete_stream("junk".to_owned()))
        .expect("delete");
    seal(&rt, &node, &clock);
    let before = fs.list(Path::new("/data")).expect("list");
    press(&rt, &node, &fs, &clock, COMPACT_FREE + 4 * MIB);
    let after = fs.list(Path::new("/data")).expect("list");
    assert!(
        after.iter().all(|n| before.contains(n)),
        "{before:?} {after:?}"
    );
    stop(&rt, node).expect("clean stop");
}
