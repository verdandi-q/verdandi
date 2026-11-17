use std::collections::BTreeSet;

use raft::eraftpb::{Entry, Message, MessageType};
use verdandi_core::cluster::placement::HolderSet;
use verdandi_core::cluster::view::{Body, Head, State, Update};
use verdandi_net::{decode, encode};
use verdandi_node::{Known, Msg, Rec};

fn set(v: &[u64]) -> BTreeSet<u64> {
    v.iter().copied().collect()
}

fn head() -> Head {
    Head {
        restart: 7,
        heartbeat: 99,
        state: State::Dead,
    }
}

fn marks() -> std::collections::BTreeMap<u64, u64> {
    [(3, 40), (9, 1_000_000)].into_iter().collect()
}

fn all() -> Vec<Msg> {
    let mut raft = Message::default();
    raft.set_msg_type(MessageType::MsgAppend);
    raft.to = 4;
    raft.from = 5;
    raft.term = 6;
    raft.commit = 8;
    let entry = Entry {
        index: 11,
        data: vec![1, 2, 3, 255].into(),
        ..Entry::default()
    };
    raft.entries = vec![entry].into();
    let body = Body {
        address: "10.0.0.1:9".into(),
        join_ms: 5,
        alive_ms: 6,
        format_max: 3,
        round_trips: vec![(1, 2)],
        marks: set(&[4, 5]),
        zone: "z".into(),
        drained: true,
        ..Body::default()
    };
    vec![
        Msg::Gossip(vec![Update {
            disk: 2,
            head: head(),
            body: Some(((8, 9), body)),
        }]),
        Msg::Ping(head()),
        Msg::Pong(12, head()),
        Msg::Raft(raft),
        Msg::Lose(13),
        Msg::Record(1, 2, Rec::Delete((3, 4), 5, 6), set(&[7, 8])),
        Msg::Ack(1, 2, 3, 4),
        Msg::Acked(5, true, 6),
        Msg::Wish(HolderSet {
            segment: 1,
            epoch: 2,
            writer: 3,
            holders: set(&[4, 5]),
        }),
        Msg::IsrAsk(1, 2, 3, set(&[4])),
        Msg::Fresh(5, 6),
        Msg::SegAsk(7, 8),
        Msg::Seg(9, Some(10), Known::Partial),
        Msg::Beat(11, 12),
        Msg::Copied(1, 2, 3, true),
        Msg::Beaten(4, 5, 6),
        Msg::Drop(7),
        Msg::Dropped(8),
        Msg::Named(9, set(&[1, 2, 3])),
        Msg::Confirm(1, 2, 3, 4),
        Msg::VoteAsk(5),
        Msg::Vote(set(&[6, 7]), 8),
        Msg::MarksAsk,
        Msg::MarksIs(marks(), marks(), [(1, 2), (3, 4)].into_iter().collect()),
        Msg::RecordsAsk(marks(), Some((5, 6))),
        Msg::Records(vec![((1, 2), 3, 4)], marks(), true),
    ]
}

#[test]
fn every_variant_round_trips() {
    for msg in all() {
        let bytes = encode(&msg);
        let back = decode(&bytes).expect("decode");
        assert_eq!(format!("{msg:?}"), format!("{back:?}"));
    }
}

#[test]
fn a_cut_input_is_an_error() {
    for msg in all() {
        let bytes = encode(&msg);
        for len in 0..bytes.len() {
            assert!(
                decode(&bytes[..len]).map_or(true, |m| format!("{m:?}") != format!("{msg:?}")),
                "{msg:?} cut to {len}"
            );
        }
    }
    assert!(decode(&[]).is_err());
    let long = encode(&all()[3]);
    assert!(decode(&long[..long.len() - 1]).is_err());
}

#[test]
fn junk_never_panics() {
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    for _ in 0..5_000 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let n = (x % 64) as usize;
        let junk: Vec<u8> = (0..n).map(|i| (x >> (i % 56)) as u8 ^ i as u8).collect();
        let _ = decode(&junk);
    }
    assert!(decode(&[3, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255]).is_err());
    assert!(decode(&[200]).is_err());
}
