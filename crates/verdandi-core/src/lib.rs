pub mod cluster;
pub mod group;
pub mod ids;
pub mod lease;
pub mod names;
pub mod retention;

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::{Bound, RangeBounds};

use group::{GroupMem, GroupState};
use ids::{Address, MessageId, SegmentId};
use lease::{Lease, Receipt};

pub const SECOND_MS: u64 = 1000;
pub const MAX_FAILURES: u32 = 5;
pub const MAX_DELAY_MS: u64 = 43_200 * SECOND_MS;
pub const PROVISIONAL_MS: u64 = 24 * 3600 * SECOND_MS;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Generation {
    pub counter: u64,
    pub incarnation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WaiterId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
    Dead,
    Expired,
    Attempts,
}

impl Reason {
    pub fn as_str(self) -> &'static str {
        match self {
            Reason::Dead => "dead",
            Reason::Expired => "expired",
            Reason::Attempts => "attempts",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Timing {
    pub delay_until: Option<u64>,
    pub expire_at: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record {
    Messages {
        stream: String,
        segment: SegmentId,
        first_position: u32,
        first_id: MessageId,
        count: u32,
        timing: Vec<(u32, Timing)>,
    },
    Ack {
        group: String,
        generation: Generation,
        address: Address,
        stream: String,
    },
    Failure {
        group: String,
        generation: Generation,
        address: Address,
        stream: String,
        count: u32,
        not_before: u64,
        error: Option<String>,
    },
    Generation {
        group: String,
        generation: Generation,
        from: Bound<MessageId>,
        issued: bool,
    },
    Forget {
        group: String,
        generation: Generation,
        provisional: bool,
    },
    ReadSet {
        group: String,
        generation: Generation,
        selector: String,
    },
    Tombstone {
        stream: String,
        up_to: MessageId,
    },
    Redrive {
        group: String,
        generation: Generation,
        stream: String,
        up_to: MessageId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeadCopy {
    pub group: String,
    pub generation: Generation,
    pub address: Address,
    pub id: MessageId,
    pub stream: String,
    pub dead_stream: String,
    pub reason: Reason,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Write(Record),
    Dead(DeadCopy),
    Wake(WaiterId, Option<String>),
    Await { group: String, address: Address },
}

pub fn nth_id(first: MessageId, n: u32) -> Option<MessageId> {
    let seq = (first.ms() << 16 | u64::from(first.counter)).checked_add(u64::from(n))?;
    MessageId::new(seq >> 16, first.incarnation, seq as u16)
}

fn ids_below(first: MessageId, id: MessageId) -> u64 {
    let seq = |ms: u64, counter: u16| ms << 16 | u64::from(counter);
    let below = match first.incarnation.cmp(&id.incarnation) {
        Ordering::Less => seq(id.ms(), u16::MAX).saturating_add(1),
        Ordering::Equal => seq(id.ms(), id.counter),
        Ordering::Greater => seq(id.ms(), 0),
    };
    below.saturating_sub(seq(first.ms(), first.counter))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Chunk {
    stream: String,
    first_id: MessageId,
    count: u32,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct State {
    chunks: BTreeMap<Address, Chunk>,
    streams: BTreeMap<String, BTreeMap<MessageId, Address>>,
    timing: BTreeMap<Address, Timing>,
    tombstones: BTreeMap<String, MessageId>,
    groups: BTreeMap<String, GroupState>,
}

impl State {
    pub fn stream_names(&self) -> impl Iterator<Item = &str> {
        self.streams.keys().map(String::as_str)
    }

    pub fn group_names(&self) -> impl Iterator<Item = (&str, bool)> {
        self.groups
            .iter()
            .filter(|(_, g)| g.generation.is_some())
            .map(|(name, g)| (name.as_str(), g.acked_gen.is_none()))
    }

    pub fn streams_in(&self, segment: SegmentId) -> BTreeSet<&str> {
        let first = Address {
            segment,
            position: 0,
        };
        let last = Address {
            segment,
            position: u32::MAX,
        };
        self.chunks
            .range(first..=last)
            .map(|(_, chunk)| chunk.stream.as_str())
            .collect()
    }

    fn message_at(&self, at: Address) -> Option<(MessageId, &str)> {
        let (start, chunk) = self.chunks.range(..=at).next_back()?;
        if start.segment != at.segment || at.position - start.position >= chunk.count {
            return None;
        }
        Some((
            nth_id(chunk.first_id, at.position - start.position)?,
            &chunk.stream,
        ))
    }

    fn live_at(&self, at: Address) -> Option<(MessageId, &str)> {
        self.message_at(at)
            .filter(|(id, stream)| self.tombstone(stream).is_none_or(|t| *id > t))
    }

    fn tombstone(&self, stream: &str) -> Option<MessageId> {
        self.tombstones
            .get(stream)
            .max(self.tombstones.get(names::origin(stream)))
            .copied()
    }
}

pub struct Core {
    incarnation: u64,
    pressure: bool,
    state: State,
    mem: BTreeMap<String, GroupMem>,
    leases: BTreeMap<Receipt, Lease>,
    waiters: BTreeMap<String, VecDeque<(WaiterId, String)>>,
    next_waiter: u64,
}

impl Core {
    pub fn new(incarnation: u64) -> Self {
        Self {
            incarnation,
            pressure: false,
            state: State::default(),
            mem: BTreeMap::new(),
            leases: BTreeMap::new(),
            waiters: BTreeMap::new(),
            next_waiter: 0,
        }
    }

    pub fn state(&self) -> &State {
        &self.state
    }

    pub fn set_pressure(&mut self, pressure: bool) {
        self.pressure = pressure;
    }

    pub fn generation(&self, group: &str) -> Option<Generation> {
        self.state.groups.get(group).and_then(|g| g.generation)
    }

    pub fn apply(&mut self, record: Record, at: u64) -> Vec<Action> {
        match record {
            Record::Messages {
                stream,
                segment,
                first_position,
                first_id,
                count,
                timing,
            } => {
                if count == 0 || nth_id(first_id, count - 1).is_none() {
                    return Vec::new();
                }
                let start = Address {
                    segment,
                    position: first_position,
                };
                self.state.chunks.insert(
                    start,
                    Chunk {
                        stream: stream.clone(),
                        first_id,
                        count,
                    },
                );
                self.state
                    .streams
                    .entry(stream.clone())
                    .or_default()
                    .insert(first_id, start);
                for (position, t) in timing {
                    self.state.timing.insert(Address { segment, position }, t);
                }
                let groups: Vec<String> = self.waiters.keys().cloned().collect();
                groups
                    .iter()
                    .filter_map(|g| self.wake_for(g, &stream))
                    .collect()
            }
            Record::Tombstone { stream, up_to } => {
                let t = self.state.tombstones.entry(stream).or_insert(up_to);
                *t = (*t).max(up_to);
                Vec::new()
            }
            Record::Ack {
                group,
                generation,
                address,
                stream,
            } => {
                self.apply_ack(&group, generation, address, stream, at);
                Vec::new()
            }
            Record::Failure {
                group,
                generation,
                address,
                stream,
                count,
                not_before,
                error,
            } => {
                let failure = group::Failure {
                    count,
                    not_before,
                    error,
                };
                self.apply_failure(&group, generation, address, stream, failure, at)
                    .into_iter()
                    .collect()
            }
            Record::Generation {
                group,
                generation,
                from,
                ..
            } => self.apply_generation(&group, generation, from, at),
            Record::Forget {
                group,
                generation,
                provisional,
            } => self.apply_forget(&group, generation, provisional),
            Record::ReadSet {
                group,
                generation,
                selector,
            } => {
                self.apply_read_set(&group, generation, selector);
                Vec::new()
            }
            Record::Redrive {
                group,
                generation,
                stream,
                up_to,
            } => self.apply_redrive(&group, generation, stream, up_to, at),
        }
    }

    pub fn history(
        &self,
        stream: &str,
        from: Bound<MessageId>,
        limit: usize,
    ) -> Vec<(MessageId, Address)> {
        let Some(starts) = self.state.streams.get(stream) else {
            return Vec::new();
        };
        let from = match (from, self.state.tombstone(stream)) {
            (Bound::Unbounded, Some(t)) => Bound::Excluded(t),
            (Bound::Included(id) | Bound::Excluded(id), Some(t)) if id <= t => Bound::Excluded(t),
            (from, _) => from,
        };
        let first = match from {
            Bound::Included(id) | Bound::Excluded(id) => {
                starts.range(..=id).next_back().map(|(first, _)| *first)
            }
            Bound::Unbounded => None,
        };
        let mut found = Vec::new();
        for start in starts
            .range((
                first.map_or(Bound::Unbounded, Bound::Included),
                Bound::Unbounded,
            ))
            .map(|(_, start)| start)
        {
            let Some(chunk) = self.state.chunks.get(start) else {
                continue;
            };
            let skip = match from {
                Bound::Included(id) | Bound::Excluded(id) => ids_below(chunk.first_id, id),
                Bound::Unbounded => 0,
            };
            let skip = u32::try_from(skip).map_or(chunk.count, |n| n.min(chunk.count));
            for n in skip..chunk.count {
                let Some(id) = nth_id(chunk.first_id, n) else {
                    break;
                };
                if !(from, Bound::Unbounded).contains(&id) {
                    continue;
                }
                if found.len() == limit.max(1) {
                    return found;
                }
                found.push((
                    id,
                    Address {
                        segment: start.segment,
                        position: start.position + n,
                    },
                ));
            }
        }
        found
    }

    pub fn tick(&mut self, now: u64) -> Vec<Action> {
        let mut actions = self.expire_leases(now);
        actions.extend(self.wake_due(now));
        if !self.pressure {
            actions.extend(self.release_dead_due());
        }
        actions.extend(self.forget_provisional(now));
        actions
    }
}

#[cfg(test)]
pub(crate) mod testkit {
    use std::sync::atomic::{AtomicU32, Ordering};

    use crate::group::{Handed, TakeOutcome, TakeRequest};
    use crate::ids::{Address, MessageId, SegmentId};
    use crate::lease::Receipt;
    use crate::{Action, Core, Record, Timing, WaiterId};

    pub const INC: u64 = 7;
    pub const LEASE: u64 = 30_000;

    static DEAD_POSITION: AtomicU32 = AtomicU32::new(0);

    pub fn seg(seq: u32) -> SegmentId {
        SegmentId {
            incarnation: INC,
            seq,
        }
    }

    pub fn id(ms: u64, counter: u16) -> MessageId {
        MessageId::new(ms, INC, counter).unwrap()
    }

    pub fn publish_timed(
        core: &mut Core,
        stream: &str,
        at: Address,
        ms: u64,
        count: u32,
        timing: Timing,
    ) -> Vec<Action> {
        core.apply(
            Record::Messages {
                stream: stream.to_owned(),
                segment: at.segment,
                first_position: at.position,
                first_id: id(ms, 0),
                count,
                timing: (0..count).map(|n| (at.position + n, timing)).collect(),
            },
            ms,
        )
    }

    pub fn publish(core: &mut Core, stream: &str, at: Address, ms: u64, count: u32) -> Vec<Action> {
        publish_timed(core, stream, at, ms, count, Timing::default())
    }

    pub fn addr(seq: u32, position: u32) -> Address {
        Address {
            segment: seg(seq),
            position,
        }
    }

    pub fn request<'a>(
        group: &'a str,
        selector: &'a str,
        limit: usize,
        receipt: u128,
    ) -> TakeRequest<'a> {
        TakeRequest {
            group,
            selector,
            limit,
            lease_ms: LEASE,
            receipt: Receipt(receipt),
            wait: false,
        }
    }

    pub fn take(
        core: &mut Core,
        group: &str,
        selector: &str,
        limit: usize,
        receipt: u128,
        now: u64,
    ) -> Vec<Handed> {
        let (outcome, actions) = core.take(&request(group, selector, limit, receipt), now);
        apply_reads(core, actions, now);
        match outcome {
            TakeOutcome::Leased { messages, .. } => messages,
            TakeOutcome::NoWork { .. } => Vec::new(),
        }
    }

    fn apply_reads(core: &mut Core, actions: Vec<Action>, now: u64) {
        for action in actions {
            if let Action::Write(record @ Record::ReadSet { .. }) = action {
                core.apply(record, now);
            }
        }
    }

    pub fn without_reads(actions: Vec<Action>) -> Vec<Action> {
        actions
            .into_iter()
            .filter(|a| !matches!(a, Action::Write(Record::ReadSet { .. })))
            .collect()
    }

    pub fn wait(core: &mut Core, group: &str, selector: &str, now: u64) -> WaiterId {
        let req = TakeRequest {
            wait: true,
            ..request(group, selector, 10, u128::MAX)
        };
        let (outcome, actions) = core.take(&req, now);
        apply_reads(core, actions, now);
        match outcome {
            TakeOutcome::NoWork {
                register: Some(waiter),
            } => waiter,
            other => panic!("expected a waiter, got {other:?}"),
        }
    }

    pub fn sync(core: &mut Core, actions: Vec<Action>, now: u64) -> Vec<Action> {
        let mut out = Vec::new();
        for action in actions {
            match action {
                Action::Write(record) => out.extend(core.apply(record, now)),
                Action::Dead(dead) => {
                    let position = DEAD_POSITION.fetch_add(1, Ordering::Relaxed);
                    out.extend(core.apply(
                        Record::Messages {
                            stream: dead.dead_stream,
                            segment: seg(999),
                            first_position: position,
                            first_id: id(now, position as u16),
                            count: 1,
                            timing: Vec::new(),
                        },
                        now,
                    ));
                    out.extend(core.apply(
                        Record::Ack {
                            group: dead.group,
                            generation: dead.generation,
                            address: dead.address,
                            stream: dead.stream,
                        },
                        now,
                    ));
                }
                Action::Wake(..) | Action::Await { .. } => out.push(action),
            }
        }
        out
    }

    pub fn ids(handed: &[Handed]) -> Vec<MessageId> {
        handed.iter().map(|h| h.id).collect()
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use crate::Core;
    use crate::ids::MessageId;
    use crate::testkit::*;

    #[test]
    fn history_from_an_id_of_another_incarnation_keeps_the_id_order() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 5, 3);
        let ids =
            |found: Vec<(MessageId, _)>| found.into_iter().map(|(id, _)| id).collect::<Vec<_>>();
        let lower = MessageId::new(5, INC - 1, 1).unwrap();
        let higher = MessageId::new(5, INC + 1, 0).unwrap();
        assert_eq!(
            ids(core.history("s", Bound::Excluded(lower), 10)),
            [id(5, 0), id(5, 1), id(5, 2)]
        );
        assert!(core.history("s", Bound::Included(higher), 10).is_empty());
        let middle = MessageId::new(5, INC, 1).unwrap();
        assert_eq!(
            ids(core.history("s", Bound::Excluded(middle), 10)),
            [id(5, 2)]
        );
    }
}
