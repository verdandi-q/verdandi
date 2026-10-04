use std::ops::Bound;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use verdandi_core::ids::{MessageId, SegmentId};
use verdandi_core::{Core, Record, nth_id};

const INC: u64 = 7;

fn first() -> MessageId {
    MessageId::new(1, INC, 0).unwrap()
}

fn huge() -> Core {
    let mut core = Core::new(INC);
    core.apply(
        Record::Messages {
            stream: "s".to_owned(),
            segment: SegmentId {
                incarnation: INC,
                seq: 1,
            },
            first_position: 0,
            first_id: first(),
            count: u32::MAX,
            timing: Vec::new(),
        },
        1,
    );
    core
}

fn history_in_time(core: Core, from: Bound<MessageId>) -> Vec<(MessageId, u32)> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let found = core.history("s", from, 10);
        tx.send(
            found
                .into_iter()
                .map(|(id, at)| (id, at.position))
                .collect(),
        )
    });
    rx.recv_timeout(Duration::from_secs(5))
        .expect("the history walked the chunk id by id")
}

#[test]
fn history_starts_at_the_from_id_inside_a_chunk_without_a_walk() {
    let last = u32::MAX - 1;
    let from = nth_id(first(), last - 1).unwrap();
    let found = history_in_time(huge(), Bound::Excluded(from));
    assert_eq!(found, [(nth_id(first(), last).unwrap(), last)]);
}

#[test]
fn history_starts_after_the_tombstone_inside_a_chunk_without_a_walk() {
    let last = u32::MAX - 1;
    let mut core = huge();
    core.apply(
        Record::Tombstone {
            stream: "s".to_owned(),
            up_to: nth_id(first(), last - 1).unwrap(),
        },
        1,
    );
    let found = history_in_time(core, Bound::Unbounded);
    assert_eq!(found, [(nth_id(first(), last).unwrap(), last)]);
}
