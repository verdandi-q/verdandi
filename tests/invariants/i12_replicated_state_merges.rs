use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::path::Path;

use verdandi_core::cluster::placement::{Groups, HolderSet, next};
use verdandi_core::lease::Settle;
use verdandi_sim::fault_fs::FaultFs;
use verdandi_store::engine::{IDLE_SEAL_MS, SettleBy, Take, Taken, TestClock};

use crate::kit::{DATA, Node, boot, ids, publish, runtime, stop, take};

fn ack_all(rt: &tokio::runtime::Runtime, node: &Node, group: &str) {
    let Ok(Taken::Leased {
        receipt, messages, ..
    }) = rt.block_on(node.handle.take(Take {
        group: group.to_owned(),
        selector: "s".to_owned(),
        limit: 10,
        lease_ms: 30_000,
        wait: false,
    }))
    else {
        panic!("expected a lease");
    };
    let ack = Settle {
        ack: messages.iter().map(|d| d.handed.id).collect(),
        ..Settle::default()
    };
    rt.block_on(node.handle.settle(receipt, SettleBy::Ids(ack)))
        .expect("ack");
}

#[test]
fn i12_generations_merge_by_the_highest_and_acks_by_union_across_segments() {
    let rt = runtime();
    let fs = FaultFs::new(12, 1 << 40);
    let clock = TestClock::new(1_000);
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    let first = publish(&rt, &node, "s", b"a").expect("publish");
    ack_all(&rt, &node, "g");
    ack_all(&rt, &node, "h");
    clock.advance(IDLE_SEAL_MS);
    rt.block_on(node.handle.damage()).expect("tick");
    rt.block_on(node.handle.replay("g".to_owned(), Bound::Unbounded))
        .expect("replay");
    rt.block_on(node.handle.replay("g".to_owned(), Bound::Excluded(first)))
        .expect("replay");
    let second = publish(&rt, &node, "s", b"b").expect("publish");
    stop(&rt, node).expect("clean stop");
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    assert_eq!(ids(&take(&rt, &node, "g", "s")), [second]);
    assert_eq!(ids(&take(&rt, &node, "h", "s")), [second]);
    let tombstone = rt
        .block_on(node.handle.delete_stream("s".to_owned()))
        .expect("delete");
    assert!(tombstone > second);
    stop(&rt, node).expect("clean stop");
    let node = boot(&rt, &fs, Path::new(DATA), &clock);
    assert!(take(&rt, &node, "k", "s").is_empty());
    stop(&rt, node).expect("clean stop");
}

fn shuffled<T: Clone>(items: &[T], seed: &mut u64) -> Vec<T> {
    let mut out = items.to_vec();
    for i in (1..out.len()).rev() {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        out.swap(i, (*seed % (i as u64 + 1)) as usize);
    }
    out
}

#[test]
fn i12_groups_and_holder_sets_merge_in_any_order() {
    let mut records: Vec<Groups> = Vec::new();
    for size in 2..=4u8 {
        for n in 3..=8u64 {
            let ring: Vec<u64> = (1..=n).collect();
            let first = next(None, &ring, &BTreeMap::new(), &BTreeSet::new(), size).expect("first");
            records.push(first.clone());
            if let Some(more) = next(
                Some(&first),
                &ring[1..],
                &BTreeMap::new(),
                &BTreeSet::new(),
                size,
            ) {
                records.push(more);
            }
        }
    }
    let sets: Vec<HolderSet> = (0..12u64)
        .map(|i| HolderSet {
            segment: 7,
            epoch: i % 4,
            writer: i % 2,
            holders: (1..=3 + i % 3).map(|d| d + i).collect(),
        })
        .collect();
    let mut seed = 0x9e37_79b9_7f4a_7c15;
    let groups = records.iter().cloned().reduce(Groups::merge);
    let holders = sets.iter().cloned().reduce(HolderSet::merge);
    assert_eq!(
        holders.as_ref().map(|h| h.epoch),
        sets.iter().map(|h| h.epoch).max()
    );
    for _ in 0..100 {
        assert_eq!(
            shuffled(&records, &mut seed)
                .into_iter()
                .reduce(Groups::merge),
            groups
        );
        assert_eq!(
            shuffled(&sets, &mut seed)
                .into_iter()
                .reduce(HolderSet::merge),
            holders
        );
    }
    for a in &sets {
        for b in sets.iter().filter(|b| b.epoch == a.epoch) {
            let merged = a.clone().merge(b.clone());
            assert!(
                merged == *a || merged == *b,
                "{a:?} {b:?} merged to {merged:?}"
            );
        }
    }
    for a in &records {
        assert_eq!(a.clone().merge(a.clone()), *a);
        for b in &records {
            assert_eq!(a.clone().merge(b.clone()), b.clone().merge(a.clone()));
        }
    }
}
