use std::collections::BTreeSet;

use crate::group::Hold;
use crate::ids::{Address, SegmentId};
use crate::{Core, Generation, Record, SECOND_MS, names, nth_id};

pub const RETAIN_MS: u64 = 7 * 24 * 3600 * SECOND_MS;
pub const PRESSURE_FLOOR_BYTES: u64 = 256 << 20;

pub fn pressure(free_bytes: u64, total_bytes: u64) -> bool {
    free_bytes < (total_bytes / 20).max(PRESSURE_FLOOR_BYTES)
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Holders {
    pub groups: BTreeSet<String>,
    pub streams: BTreeSet<String>,
    pub retained: bool,
    pub runs: BTreeSet<u32>,
}

impl Holders {
    pub fn removable(&self) -> bool {
        self.groups.is_empty() && self.streams.is_empty() && !self.retained
    }
}

impl Core {
    pub fn holders(&self, segment: SegmentId, now: u64, pressure: bool) -> Holders {
        let mut holders = Holders::default();
        let start = Address {
            segment,
            position: 0,
        };
        let chunks = self
            .state
            .chunks
            .range(start..)
            .take_while(|(at, _)| at.segment == segment);
        for (first, chunk) in chunks {
            let readers: Vec<_> = self
                .state
                .groups
                .iter()
                .filter(|(_, g)| g.generation.is_some() && g.acked_gen.is_some())
                .filter_map(|(name, g)| {
                    let reads = |stream: &str| g.read_set.keys().any(|s| names::matches(s, stream));
                    let direct = reads(&chunk.stream);
                    let mark = g
                        .redrive
                        .get(&chunk.stream)
                        .filter(|_| reads(names::origin(&chunk.stream)))
                        .copied();
                    (direct || mark.is_some()).then_some((name, g, direct, mark))
                })
                .collect();
            let tombstone = self.state.tombstone(&chunk.stream);
            let dead = names::is_dead(&chunk.stream);
            let mut held = false;
            for n in 0..chunk.count {
                let Some(id) = nth_id(chunk.first_id, n) else {
                    break;
                };
                let at = Address {
                    segment,
                    position: first.position + n,
                };
                if tombstone.is_some_and(|t| id <= t) {
                    continue;
                }
                let mut read = false;
                for (name, g, direct, mark) in &readers {
                    let marked = mark.is_some_and(|m| id <= m);
                    let by_name = *direct && !(dead && g.before_from(id));
                    if !by_name && !marked {
                        continue;
                    }
                    read = true;
                    if !g.is_acked(at) && ((*direct && !g.before_from(id)) || marked) {
                        holders.groups.insert((*name).clone());
                        held = true;
                    } else if !pressure
                        && g.last_ack_ms.get(&segment).copied().unwrap_or(0) + RETAIN_MS > now
                    {
                        holders.retained = true;
                        held = true;
                    }
                }
                if !read && id.ms() + RETAIN_MS > now {
                    holders.streams.insert(chunk.stream.clone());
                    held = true;
                }
            }
            if held {
                holders.runs.insert(first.position);
            }
        }
        holders
    }

    pub fn held(&self, segment: SegmentId, gone: &dyn Fn(Address) -> bool) -> bool {
        let start = Address {
            segment,
            position: 0,
        };
        self.state
            .chunks
            .range(start..)
            .take_while(|(at, _)| at.segment == segment)
            .any(|(at, _)| !gone(*at))
    }

    pub fn carries(
        &self,
        record: &Record,
        commit_ms: u64,
        now: u64,
        gone: &dyn Fn(Address) -> bool,
    ) -> bool {
        let held = |at: Address| self.state.message_at(at).is_some() && !gone(at);
        let current =
            |group: &str, generation: &Generation| self.generation(group) == Some(*generation);
        let group = |name: &str| self.state.groups.get(name);
        match record {
            Record::Ack {
                group,
                generation,
                address,
                ..
            } => {
                (current(group, generation) && held(*address))
                    || (commit_ms + RETAIN_MS > now && self.held(address.segment, gone))
                    || self.state.groups.get(group).is_some_and(|g| {
                        g.acked_gen == Some(*generation) && g.acked_ms == Some(commit_ms)
                    })
            }
            Record::Failure {
                group,
                generation,
                address,
                ..
            } => current(group, generation) && held(*address),
            Record::Generation {
                group, generation, ..
            }
            | Record::Redrive {
                group, generation, ..
            } => current(group, generation),
            Record::ReadSet {
                group: name,
                generation,
                ..
            } => group(name).is_some_and(|g| {
                g.generation.is_some() && g.forgotten.is_none_or(|f| *generation > f)
            }),
            Record::Forget {
                group: name,
                generation,
                ..
            } => group(name).is_some_and(|g| g.forgotten == Some(*generation)),
            Record::Tombstone { stream, up_to } => {
                self.state.tombstones.get(stream) == Some(up_to)
                    && self
                        .state
                        .streams
                        .iter()
                        .filter(|(s, _)| *s == stream || names::origin(s) == stream)
                        .any(|(_, starts)| starts.range(..=*up_to).any(|(_, at)| !gone(*at)))
            }
            Record::Messages { .. } => false,
        }
    }

    pub fn drop_segment(&mut self, segment: SegmentId) {
        let state = &mut self.state;
        state.chunks.retain(|at, chunk| {
            if at.segment != segment {
                return true;
            }
            if let Some(starts) = state.streams.get_mut(&chunk.stream) {
                if starts.get(&chunk.first_id) == Some(at) {
                    starts.remove(&chunk.first_id);
                }
                if starts.is_empty() {
                    state.streams.remove(&chunk.stream);
                }
            }
            false
        });
        state.timing.retain(|at, _| at.segment != segment);
        for g in state.groups.values_mut() {
            g.acked.remove(&segment);
            g.last_ack_ms.remove(&segment);
            g.failures.retain(|at, _| at.segment != segment);
        }
        for mem in self.mem.values_mut() {
            mem.holds.retain(|at, hold| {
                at.segment != segment
                    || matches!(
                        hold,
                        Hold::AckPending(_) | Hold::FailurePending(_) | Hold::DeadPending(_)
                    )
            });
            mem.delivered.retain(|at, _| at.segment != segment);
            mem.due.retain(|(_, at)| at.segment != segment);
        }
    }

    pub fn drop_run(&mut self, start: Address) {
        let state = &mut self.state;
        let Some(chunk) = state.chunks.remove(&start) else {
            return;
        };
        if let Some(starts) = state.streams.get_mut(&chunk.stream) {
            if starts.get(&chunk.first_id) == Some(&start) {
                starts.remove(&chunk.first_id);
            }
            if starts.is_empty() {
                state.streams.remove(&chunk.stream);
            }
        }
        let run = start..Address {
            position: start.position + chunk.count,
            ..start
        };
        state.timing.retain(|at, _| !run.contains(at));
        for mem in self.mem.values_mut() {
            mem.holds.retain(|at, hold| {
                !run.contains(at)
                    || matches!(
                        hold,
                        Hold::AckPending(_) | Hold::FailurePending(_) | Hold::DeadPending(_)
                    )
            });
            mem.delivered.retain(|at, _| !run.contains(at));
            mem.due.retain(|(_, at)| !run.contains(at));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Record;
    use crate::lease::Receipt;
    use crate::testkit::*;

    const DAY: u64 = 24 * 3600 * SECOND_MS;

    fn ack_everything(core: &mut Core, group: &str, receipt: u128, now: u64) {
        take(core, group, "s", 100, receipt, now);
        let actions = core.ack_all(Receipt(receipt), now).unwrap();
        sync(core, actions, now);
    }

    fn names(list: &[&str]) -> BTreeSet<String> {
        list.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn pressure_starts_under_five_percent_or_256_mib_free() {
        let gib = 1u64 << 30;
        assert!(pressure(5 * gib - 1, 100 * gib));
        assert!(!pressure(5 * gib, 100 * gib));
        assert!(pressure((256 << 20) - 1, gib));
        assert!(!pressure(256 << 20, gib));
    }

    #[test]
    fn a_segment_goes_when_every_group_acked_and_the_last_ack_is_older_than_retain() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 2);
        ack_everything(&mut core, "g", 1, DAY);
        let held = core.holders(seg(1), DAY + RETAIN_MS - 1, false);
        assert!(held.retained && !held.removable());
        assert!(core.holders(seg(1), DAY + RETAIN_MS, false).removable());
        assert!(core.holders(seg(1), DAY, true).removable());
    }

    #[test]
    fn unacked_data_is_held_and_names_its_groups_also_under_pressure() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 2);
        ack_everything(&mut core, "g", 1, 10);
        publish(&mut core, "s", addr(1, 2), 20, 1);
        take(&mut core, "h", "s", 1, 2, 30);
        let actions = core.ack_all(Receipt(2), 30).unwrap();
        sync(&mut core, actions, 30);
        let held = core.holders(seg(1), 100 * DAY, true);
        assert_eq!(held.groups, names(&["g", "h"]));
        assert!(held.streams.is_empty());
    }

    #[test]
    fn a_stream_with_no_acking_group_keeps_its_data_for_retain_from_publish() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1_000, 1);
        assert_eq!(take(&mut core, "provisional", "s", 1, 1, 2_000).len(), 1);
        let held = core.holders(seg(1), 1_000 + RETAIN_MS - 1, true);
        assert_eq!(held.streams, names(&["s"]));
        assert!(core.holders(seg(1), 1_000 + RETAIN_MS, true).removable());
    }

    #[test]
    fn a_tombstone_makes_covered_data_removable() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 2);
        core.apply(
            Record::Tombstone {
                stream: "s".to_owned(),
                up_to: id(1, 1),
            },
            5,
        );
        assert!(core.holders(seg(1), 10, false).removable());
    }

    #[test]
    fn a_dropped_run_is_never_handed_out_and_its_neighbours_stay() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 2);
        publish(&mut core, "s", addr(1, 2), 2, 2);
        let handed = take(&mut core, "g", "s", 1, 1, 3);
        assert_eq!(ids(&handed), [id(1, 0)]);
        core.drop_run(addr(1, 0));
        assert_eq!(
            ids(&take(&mut core, "g", "s", 10, 2, 4)),
            [id(2, 0), id(2, 1)]
        );
        assert_eq!(
            core.settle(
                Receipt(1),
                &crate::lease::Settle {
                    ack: vec![id(1, 0)],
                    ..Default::default()
                },
                5
            ),
            Ok(Vec::new())
        );
    }

    fn restart(log: &[(Record, u64)]) -> Core {
        let mut core = Core::new(INC + 1);
        for (record, at) in log {
            core.apply(record.clone(), *at);
        }
        core
    }

    fn writes(actions: Vec<crate::Action>, at: u64) -> Vec<(Record, u64)> {
        actions
            .into_iter()
            .filter_map(|a| match a {
                crate::Action::Write(record) => Some((record, at)),
                _ => None,
            })
            .collect()
    }

    fn messages(stream: &str, at: crate::ids::Address, ms: u64) -> (Record, u64) {
        let record = Record::Messages {
            stream: stream.to_owned(),
            segment: at.segment,
            first_position: at.position,
            first_id: id(ms, 0),
            count: 1,
            timing: Vec::new(),
        };
        (record, ms)
    }

    #[test]
    fn a_pattern_reader_holds_a_stream_with_no_ack_after_a_restart() {
        let mut core = Core::new(INC);
        let mut log = vec![
            messages("orders.eu", addr(1, 0), 1),
            messages("orders.us", addr(2, 0), 2),
        ];
        for (record, at) in &log {
            core.apply(record.clone(), *at);
        }
        let (outcome, actions) = core.take(&request("g", "orders.*", 10, 1), 3);
        log.extend(writes(actions, 3));
        let crate::group::TakeOutcome::Leased { messages, .. } = outcome else {
            panic!("expected messages");
        };
        let ack = crate::lease::Settle {
            ack: vec![messages[0].id],
            ..crate::lease::Settle::default()
        };
        log.extend(writes(core.settle(Receipt(1), &ack, 4).unwrap(), 4));
        let held = restart(&log).holders(seg(2), 100 * DAY, true);
        assert_eq!(held.groups, names(&["g"]));
    }

    #[test]
    fn a_failure_puts_its_stream_in_the_read_set_also_without_a_read_set_record() {
        let mut core = Core::new(INC);
        let mut log = vec![messages("a", addr(1, 0), 1), messages("b", addr(2, 0), 2)];
        for (record, at) in &log {
            core.apply(record.clone(), *at);
        }
        take(&mut core, "g", "a", 1, 1, 3);
        take(&mut core, "g", "b", 1, 2, 3);
        let generation = core.generation("g").unwrap();
        log.push((
            Record::Ack {
                group: "g".to_owned(),
                generation,
                address: addr(1, 0),
                stream: "a".to_owned(),
            },
            4,
        ));
        log.push((
            Record::Failure {
                group: "g".to_owned(),
                generation,
                address: addr(2, 0),
                stream: "b".to_owned(),
                count: 1,
                not_before: 5,
                error: None,
            },
            4,
        ));
        let held = restart(&log).holders(seg(2), 100 * DAY, true);
        assert_eq!(held.groups, names(&["g"]));
    }

    #[test]
    fn the_read_set_after_a_forget_does_not_depend_on_the_record_order() {
        let g1 = crate::Generation {
            counter: 1,
            incarnation: INC,
        };
        let g2 = crate::Generation {
            counter: 2,
            incarnation: INC,
        };
        let ack = |generation, address, stream: &str| Record::Ack {
            group: "b".to_owned(),
            generation,
            address,
            stream: stream.to_owned(),
        };
        let control = [
            (ack(g1, addr(1, 0), "s"), 10),
            (
                Record::Forget {
                    group: "b".to_owned(),
                    generation: g1,
                    provisional: false,
                },
                11,
            ),
            (ack(g2, addr(2, 0), "t"), 12),
        ];
        let base = [messages("s", addr(1, 0), 1), messages("t", addr(2, 0), 2)];
        let forward: Vec<_> = base.iter().chain(control.iter()).cloned().collect();
        let backward: Vec<_> = base.iter().chain(control.iter().rev()).cloned().collect();
        let (forward, backward) = (restart(&forward), restart(&backward));
        assert_eq!(forward.state(), backward.state());
        assert_eq!(
            forward.holders(seg(1), 100 * DAY, true),
            backward.holders(seg(1), 100 * DAY, true)
        );
    }

    #[test]
    fn a_dropped_segment_is_not_handed_out_and_leaves_no_state() {
        let mut core = Core::new(INC);
        let timing = crate::Timing {
            delay_until: None,
            expire_at: Some(10 * DAY),
        };
        publish_timed(&mut core, "s", addr(1, 0), 1, 2, timing);
        publish(&mut core, "s", addr(2, 0), 2, 1);
        ack_everything(&mut core, "g", 1, 3);
        core.drop_segment(seg(1));
        assert_eq!(ids(&take(&mut core, "h", "s", 10, 2, 4)), [id(2, 0)]);
        assert!(core.state.chunks.keys().all(|at| at.segment != seg(1)));
        assert!(core.state.timing.keys().all(|at| at.segment != seg(1)));
        assert!(!core.state.groups["g"].acked.contains_key(&seg(1)));
        assert!(!core.state.groups["g"].last_ack_ms.contains_key(&seg(1)));
        assert_eq!(core.state.streams["s"].len(), 1);
    }

    #[test]
    fn positions_before_a_replay_start_count_as_acked() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        publish(&mut core, "s", addr(1, 1), 5_000, 1);
        ack_everything(&mut core, "g", 1, 6_000);
        let replay = core.replay("g", std::ops::Bound::Included(id(3_000, 0)));
        sync(&mut core, vec![crate::Action::Write(replay)], 7_000);
        let held = core.holders(seg(1), 8_000, true);
        assert_eq!(held.groups, names(&["g"]));
        take(&mut core, "g", "s", 10, 2, 8_000);
        let actions = core.ack_all(Receipt(2), 8_000).unwrap();
        sync(&mut core, actions, 8_000);
        assert!(core.holders(seg(1), 8_000, true).removable());
    }

    #[test]
    fn an_ack_or_a_failure_before_its_messages_record_folds_to_the_same_state() {
        let generation = crate::Generation {
            counter: 1,
            incarnation: INC,
        };
        let delayed = crate::Timing {
            delay_until: Some(5),
            expire_at: None,
        };
        let log = [
            (
                Record::Messages {
                    stream: "t".to_owned(),
                    segment: seg(1),
                    first_position: 0,
                    first_id: id(1, 0),
                    count: 3,
                    timing: vec![(0, delayed)],
                },
                1,
            ),
            (
                Record::Ack {
                    group: "g".to_owned(),
                    generation,
                    address: addr(1, 0),
                    stream: "t".to_owned(),
                },
                10,
            ),
            (
                Record::Failure {
                    group: "g".to_owned(),
                    generation,
                    address: addr(1, 1),
                    stream: "t".to_owned(),
                    count: 1,
                    not_before: 20,
                    error: None,
                },
                11,
            ),
        ];
        let reversed: Vec<_> = log.iter().rev().cloned().collect();
        let (forward, backward) = (restart(&log), restart(&reversed));
        assert_eq!(forward.state(), backward.state());
        assert_eq!(
            backward.holders(seg(1), 100 * DAY, true).groups,
            names(&["g"])
        );
    }

    fn redriven(core: &mut Core) {
        core.apply(messages("s", addr(1, 0), 1).0, 1);
        core.apply(messages("s.dead.g", addr(10, 0), 2).0, 2);
        ack_everything(core, "g", 1, 3);
        for record in core.redrive("g") {
            core.apply(record, 4);
        }
        core.apply(messages("s.dead.g", addr(11, 0), 5).0, 5);
    }

    #[test]
    fn a_redrive_mark_holds_the_dead_stream_only_up_to_the_mark() {
        let mut core = Core::new(INC);
        redriven(&mut core);
        assert_eq!(core.holders(seg(10), 6, true).groups, names(&["g"]));
        let above = core.holders(seg(11), 6, true);
        assert!(above.groups.is_empty(), "{above:?}");
        assert_eq!(above.streams, names(&["s.dead.g"]));
    }

    #[test]
    fn a_redrive_after_a_replay_holds_the_dead_copies_made_before_its_from() {
        let mut core = Core::new(INC);
        core.apply(messages("s", addr(1, 0), 1).0, 1);
        core.apply(messages("s.dead.g", addr(10, 0), 2).0, 2);
        ack_everything(&mut core, "g", 1, 3);
        let replay = core.replay("g", std::ops::Bound::Excluded(id(4, 0)));
        core.apply(replay, 4);
        core.apply(messages("s", addr(2, 0), 5).0, 5);
        ack_everything(&mut core, "g", 2, 6);
        for record in core.redrive("g") {
            core.apply(record, 7);
        }
        assert_eq!(core.holders(seg(10), 8, true).groups, names(&["g"]));
    }

    #[test]
    fn a_dead_stream_read_by_name_and_by_a_redrive_mark_is_held_counted_and_taken_the_same() {
        let mut core = Core::new(INC);
        core.apply(messages("s", addr(1, 0), 1).0, 1);
        core.apply(messages("s.dead.g", addr(10, 0), 2).0, 2);
        ack_everything(&mut core, "g", 1, 3);
        take(&mut core, "g", "s.dead.g", 10, 2, 3);
        let replay = core.replay("g", std::ops::Bound::Excluded(id(4, 0)));
        core.apply(replay, 4);
        for record in core.redrive("g") {
            core.apply(record, 5);
        }
        assert_eq!(core.holders(seg(10), 6, true).groups, names(&["g"]));
        let stats = core.group_stats("g", 6).unwrap();
        assert_eq!((stats.pending, stats.dead), (1, 0));
        assert_eq!(ids(&take(&mut core, "g", "s", 10, 3, 6)), [id(2, 0)]);
    }

    #[test]
    fn a_dead_copy_before_a_replay_start_read_only_by_name_is_kept_for_retain() {
        let mut core = Core::new(INC);
        let now = 100 * DAY;
        core.apply(messages("s", addr(1, 0), now - 3).0, now - 3);
        core.apply(messages("s.dead.g", addr(10, 0), now - 2).0, now - 2);
        ack_everything(&mut core, "g", 1, now - 2);
        take(&mut core, "g", "s.dead.g", 10, 2, now - 2);
        let replay = core.replay("g", std::ops::Bound::Excluded(id(now - 1, 0)));
        core.apply(replay, now - 1);
        let held = core.holders(seg(10), now, false);
        assert_eq!(held.streams, names(&["s.dead.g"]), "{held:?}");
        assert!(!held.removable());
    }

    #[test]
    fn an_ack_of_a_redriven_dead_copy_does_not_read_the_whole_dead_stream() {
        let mut core = Core::new(INC);
        redriven(&mut core);
        ack_everything(&mut core, "g", 2, 6);
        assert!(core.state.groups["g"].is_acked(addr(10, 0)));
        let above = core.holders(seg(11), 7, true);
        assert!(above.groups.is_empty(), "{above:?}");
    }

    #[test]
    fn a_dropped_id_settles_as_settled_before_with_no_change() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        publish(&mut core, "s", addr(2, 0), 2, 1);
        let handed = take(&mut core, "p", "s", 2, 1, 3);
        core.drop_segment(seg(1));
        let ack = |id| crate::lease::Settle {
            ack: vec![id],
            ..crate::lease::Settle::default()
        };
        assert_eq!(
            core.settle(Receipt(1), &ack(handed[0].id), 4),
            Ok(Vec::new())
        );
        assert_eq!(
            core.settle(Receipt(1), &ack(handed[1].id), 4)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn an_abandoned_ack_of_a_dropped_message_leaves_no_hold() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        take(&mut core, "g", "s", 1, 1, 2);
        let generation = core.generation("g").unwrap();
        core.ack_all(Receipt(1), 3).unwrap();
        core.drop_segment(seg(1));
        core.abandon("g", generation, &[addr(1, 0)], false);
        assert!(core.mem["g"].holds.is_empty());
    }

    #[test]
    fn a_replay_keeps_the_retain_clock_of_the_acks_of_the_old_generation() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 2);
        ack_everything(&mut core, "g", 1, 10 * DAY);
        let later = 10 * DAY + 3600 * SECOND_MS;
        let from = crate::ids::MessageId::new(later, 0, 0).unwrap();
        let replay = core.replay("g", std::ops::Bound::Included(from));
        sync(&mut core, vec![crate::Action::Write(replay)], later);
        let held = core.holders(seg(1), later, false);
        assert!(held.retained && !held.removable(), "{held:?}");
        assert!(
            core.holders(seg(1), 10 * DAY + RETAIN_MS, false)
                .removable()
        );
    }

    #[test]
    fn a_fifth_failure_of_a_dropped_message_leaves_no_dead_copy_waiting() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        take(&mut core, "g", "s", 1, 1, 2);
        let generation = core.generation("g").unwrap();
        core.drop_segment(seg(1));
        let failure = Record::Failure {
            group: "g".to_owned(),
            generation,
            address: addr(1, 0),
            stream: "s".to_owned(),
            count: crate::MAX_FAILURES,
            not_before: 3,
            error: None,
        };
        core.apply(failure, 3);
        assert_eq!(core.tick(4), []);
        assert!(core.mem["g"].holds.is_empty());
    }

    #[test]
    fn a_dropped_segment_keeps_a_waiting_ack_so_no_provisional_forget_starts() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        take(&mut core, "g", "s", 1, 1, 10);
        let pending = core.ack_all(Receipt(1), 20).unwrap();
        core.drop_segment(seg(1));
        assert_eq!(core.tick(10 + crate::PROVISIONAL_MS), []);
        sync(&mut core, pending, 10 + crate::PROVISIONAL_MS);
        assert_eq!(core.tick(11 + crate::PROVISIONAL_MS), []);
    }
}
