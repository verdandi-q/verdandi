use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Bound, RangeBounds};

use crate::ids::{Address, MessageId, SegmentId};
use crate::lease::{Lease, Receipt};
use crate::{
    Action, Core, DeadCopy, Generation, MAX_FAILURES, PROVISIONAL_MS, Reason, Record, WaiterId,
    names, nth_id,
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Bitmap(Vec<u64>);

impl Bitmap {
    fn set(&mut self, position: u32) {
        let word = position as usize / 64;
        if self.0.len() <= word {
            self.0.resize(word + 1, 0);
        }
        self.0[word] |= 1 << (position % 64);
    }

    fn get(&self, position: u32) -> bool {
        self.0
            .get(position as usize / 64)
            .is_some_and(|w| (w >> (position % 64)) & 1 == 1)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Failure {
    pub(crate) count: u32,
    pub(crate) not_before: u64,
    pub(crate) error: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct GroupState {
    pub(crate) generation: Option<Generation>,
    pub(crate) forgotten: Option<Generation>,
    pub(crate) from: Option<Bound<MessageId>>,
    pub(crate) since_ms: u64,
    pub(crate) acked_gen: Option<Generation>,
    pub(crate) acked_ms: Option<u64>,
    pub(crate) acked: BTreeMap<SegmentId, Bitmap>,
    pub(crate) last_ack_ms: BTreeMap<SegmentId, u64>,
    pub(crate) failures: BTreeMap<Address, Failure>,
    pub(crate) redrive: BTreeMap<String, MessageId>,
    pub(crate) read_set: BTreeMap<String, Generation>,
}

impl GroupState {
    fn read(&mut self, selector: String, generation: Generation) {
        if self.forgotten.is_some_and(|f| generation <= f) {
            return;
        }
        let kept = self.read_set.entry(selector).or_insert(generation);
        *kept = (*kept).max(generation);
    }

    pub(crate) fn before_from(&self, id: MessageId) -> bool {
        self.from
            .is_some_and(|f| !(f, Bound::Unbounded).contains(&id))
    }

    pub(crate) fn is_acked(&self, at: Address) -> bool {
        self.acked
            .get(&at.segment)
            .is_some_and(|b| b.get(at.position))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Hold {
    Leased(Receipt),
    AckPending(Receipt),
    FailurePending(Option<Receipt>),
    DeadPending(Option<Receipt>),
    DeadDue,
}

#[derive(Debug, Default)]
pub(crate) struct Delivery {
    pub(crate) count: u32,
    pub(crate) free_expiry_used: bool,
}

#[derive(Debug, Default)]
pub(crate) struct GroupMem {
    pub(crate) holds: BTreeMap<Address, Hold>,
    pub(crate) delivered: BTreeMap<Address, Delivery>,
    pub(crate) due: BTreeSet<(u64, Address)>,
    pub(crate) reserved: u64,
    pub(crate) forgetting: bool,
    pub(crate) cursor: String,
    pub(crate) reading: BTreeSet<String>,
}

impl GroupMem {
    fn reset(&mut self) {
        *self = GroupMem {
            reserved: self.reserved,
            ..GroupMem::default()
        };
    }
}

pub struct TakeRequest<'a> {
    pub group: &'a str,
    pub selector: &'a str,
    pub limit: usize,
    pub lease_ms: u64,
    pub receipt: Receipt,
    pub wait: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Handed {
    pub address: Address,
    pub id: MessageId,
    pub stream: String,
    pub attempt: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TakeOutcome {
    Leased { until: u64, messages: Vec<Handed> },
    NoWork { register: Option<WaiterId> },
}

enum Found {
    Free,
    Wait(u64),
    Dead(Reason),
}

type Candidate = (Address, MessageId, String, Found);

const FIRST_ADDRESS: Address = Address {
    segment: SegmentId {
        incarnation: 0,
        seq: 0,
    },
    position: 0,
};

impl Core {
    pub fn take(&mut self, req: &TakeRequest<'_>, now: u64) -> (TakeOutcome, Vec<Action>) {
        let generation = self.ensure_group(req.group, now);
        let (found, left) = self.scan(req, now);
        let pressure = self.pressure;
        let mem = self.mem.entry(req.group.to_owned()).or_default();
        let group = self.state.groups.get(req.group);
        let failures = group.map(|g| &g.failures);
        let mut actions = Vec::new();
        if !group
            .and_then(|g| g.read_set.get(req.selector))
            .is_some_and(|r| *r >= generation)
            && mem.reading.insert(req.selector.to_owned())
        {
            actions.push(Action::Write(Record::ReadSet {
                group: req.group.to_owned(),
                generation,
                selector: req.selector.to_owned(),
            }));
        }
        let mut messages = Vec::new();
        for (at, id, stream, found) in found {
            match found {
                Found::Free => {
                    mem.holds.insert(at, Hold::Leased(req.receipt));
                    let failed = failures.and_then(|f| f.get(&at)).map_or(0, |f| f.count);
                    let d = mem.delivered.entry(at).or_default();
                    d.count = d.count.max(failed) + 1;
                    messages.push(Handed {
                        address: at,
                        id,
                        stream,
                        attempt: d.count,
                    });
                }
                Found::Wait(due) => {
                    mem.due.insert((due, at));
                }
                Found::Dead(_) if pressure => {
                    mem.holds.insert(at, Hold::DeadDue);
                }
                Found::Dead(reason) => {
                    mem.holds.insert(at, Hold::DeadPending(None));
                    actions.push(Action::Dead(DeadCopy {
                        group: req.group.to_owned(),
                        generation,
                        address: at,
                        id,
                        dead_stream: names::dead_stream(&stream, req.group),
                        stream,
                        reason,
                        error: failures
                            .and_then(|f| f.get(&at))
                            .and_then(|f| f.error.clone()),
                    }));
                }
            }
        }
        if let Some(last) = messages.last() {
            mem.cursor.clone_from(&last.stream);
        }
        for stream in left {
            actions.extend(self.wake_for(req.group, &stream));
        }
        if messages.is_empty() {
            let register = req.wait.then(|| self.register(req.group, req.selector));
            return (TakeOutcome::NoWork { register }, actions);
        }
        let until = now + req.lease_ms;
        let replaced = self.leases.insert(
            req.receipt,
            Lease {
                group: req.group.to_owned(),
                generation,
                until,
                ids: messages
                    .iter()
                    .map(|m| (m.id, (m.address, m.stream.clone())))
                    .collect(),
            },
        );
        assert!(replaced.is_none(), "receipt {} already in use", req.receipt);
        (TakeOutcome::Leased { until, messages }, actions)
    }

    fn scan(&self, req: &TakeRequest<'_>, now: u64) -> (Vec<Candidate>, Vec<String>) {
        let mut found = Vec::new();
        let mut left = Vec::new();
        let Some(g) = self.state.groups.get(req.group) else {
            return (found, left);
        };
        let mem = self.mem.get(req.group);
        if mem.is_some_and(|m| m.forgetting) {
            return (found, left);
        }
        let holds = mem.map(|m| &m.holds);
        let cursor = mem.map_or("", |m| m.cursor.as_str());
        let streams = self
            .state
            .streams
            .range::<str, _>((Bound::Excluded(cursor), Bound::Unbounded))
            .chain(
                self.state
                    .streams
                    .range::<str, _>((Bound::Unbounded, Bound::Included(cursor))),
            );
        let limit = req.limit.max(1);
        let mut free = 0;
        'streams: for (stream, starts) in streams {
            let mark = if names::matches(req.selector, stream) {
                None
            } else if names::matches(req.selector, names::origin(stream)) {
                match g.redrive.get(stream) {
                    Some(mark) => Some(*mark),
                    None => continue,
                }
            } else {
                continue;
            };
            let tombstone = self.state.tombstone(stream);
            for start in starts.values() {
                let Some(chunk) = self.state.chunks.get(start) else {
                    continue;
                };
                for n in 0..chunk.count {
                    let Some(id) = nth_id(chunk.first_id, n) else {
                        break;
                    };
                    let at = Address {
                        segment: start.segment,
                        position: start.position + n,
                    };
                    if tombstone.is_some_and(|t| id <= t)
                        || (mark.is_none() && g.before_from(id))
                        || mark.is_some_and(|m| id > m)
                        || g.is_acked(at)
                        || holds.is_some_and(|h| h.contains_key(&at))
                    {
                        continue;
                    }
                    let failure = g.failures.get(&at);
                    let timing = self.state.timing.get(&at).copied().unwrap_or_default();
                    let due = failure
                        .map_or(0, |f| f.not_before)
                        .max(timing.delay_until.unwrap_or(0));
                    let kind = if failure.is_some_and(|f| f.count >= MAX_FAILURES) {
                        Found::Dead(Reason::Attempts)
                    } else if timing.expire_at.is_some_and(|e| e <= now) {
                        Found::Dead(Reason::Expired)
                    } else if due > now {
                        Found::Wait(due)
                    } else if free == limit {
                        left.push(stream.clone());
                        continue 'streams;
                    } else {
                        free += 1;
                        Found::Free
                    };
                    found.push((at, id, stream.clone(), kind));
                }
            }
        }
        (found, left)
    }

    fn ensure_group(&mut self, group: &str, now: u64) -> Generation {
        if let Some(generation) = self.generation(group) {
            return generation;
        }
        let used = self.used_counter(group);
        let mem = self.mem.entry(group.to_owned()).or_default();
        mem.reserved = mem.reserved.max(used + 1);
        let generation = Generation {
            counter: mem.reserved,
            incarnation: self.incarnation,
        };
        self.adopt(group, generation, now);
        generation
    }

    fn used_counter(&self, group: &str) -> u64 {
        self.state
            .groups
            .get(group)
            .and_then(|g| g.generation.max(g.forgotten))
            .map_or(0, |generation| generation.counter)
    }

    fn next_generation(&mut self, group: &str) -> Generation {
        let used = self.used_counter(group);
        let mem = self.mem.entry(group.to_owned()).or_default();
        mem.reserved = mem.reserved.max(used) + 1;
        Generation {
            counter: mem.reserved,
            incarnation: self.incarnation,
        }
    }

    fn adopt(&mut self, group: &str, generation: Generation, at: u64) -> bool {
        let g = self.state.groups.entry(group.to_owned()).or_default();
        if g.forgotten.is_some_and(|f| generation <= f) {
            return false;
        }
        match g.generation {
            Some(current) if generation < current => false,
            Some(current) if generation == current => {
                g.since_ms = g.since_ms.min(at);
                true
            }
            _ => {
                *g = GroupState {
                    generation: Some(generation),
                    forgotten: g.forgotten,
                    since_ms: at,
                    acked_gen: g.acked_gen,
                    acked_ms: g.acked_ms,
                    last_ack_ms: std::mem::take(&mut g.last_ack_ms),
                    read_set: std::mem::take(&mut g.read_set),
                    ..GroupState::default()
                };
                self.mem.entry(group.to_owned()).or_default().reset();
                true
            }
        }
    }

    pub(crate) fn apply_ack(
        &mut self,
        group: &str,
        generation: Generation,
        at: Address,
        stream: String,
        ms: u64,
    ) {
        let g = self.state.groups.entry(group.to_owned()).or_default();
        let last = g.last_ack_ms.entry(at.segment).or_insert(ms);
        *last = (*last).max(ms);
        if g.forgotten.is_some_and(|f| generation <= f) {
            return;
        }
        if Some(generation) > g.acked_gen {
            g.acked_gen = Some(generation);
            g.acked_ms = Some(ms);
        } else if Some(generation) == g.acked_gen {
            g.acked_ms = g.acked_ms.max(Some(ms));
        }
        if !names::is_dead(&stream) {
            g.read(stream, generation);
        }
        if !self.adopt(group, generation, ms) {
            return;
        }
        let Some(g) = self.state.groups.get_mut(group) else {
            return;
        };
        g.acked.entry(at.segment).or_default().set(at.position);
        g.failures.remove(&at);
        if let Some(mem) = self.mem.get_mut(group) {
            mem.holds.remove(&at);
            mem.delivered.remove(&at);
        }
    }

    pub(crate) fn apply_failure(
        &mut self,
        group: &str,
        generation: Generation,
        at: Address,
        stream: String,
        failure: Failure,
        ms: u64,
    ) -> Option<Action> {
        let g = self.state.groups.entry(group.to_owned()).or_default();
        if !names::is_dead(&stream) {
            g.read(stream.clone(), generation);
        }
        if !self.adopt(group, generation, ms) {
            return None;
        }
        let g = self.state.groups.get_mut(group)?;
        if g.is_acked(at) {
            return None;
        }
        let kept = g.failures.entry(at).or_insert_with(|| failure.clone());
        if failure > *kept {
            *kept = failure;
        }
        let mem = self.mem.entry(group.to_owned()).or_default();
        if matches!(mem.holds.get(&at), Some(Hold::FailurePending(_))) {
            mem.holds.remove(&at);
        }
        if kept.count >= MAX_FAILURES {
            mem.holds.entry(at).or_insert(Hold::DeadDue);
            return None;
        }
        if kept.not_before > ms {
            mem.due.insert((kept.not_before, at));
            return None;
        }
        self.wake_for(group, &stream)
    }

    pub(crate) fn apply_generation(
        &mut self,
        group: &str,
        generation: Generation,
        from: Bound<MessageId>,
        ms: u64,
    ) -> Vec<Action> {
        if !self.adopt(group, generation, ms) {
            return Vec::new();
        }
        if let Some(g) = self.state.groups.get_mut(group) {
            g.from = Some(from);
        }
        self.wake_all(group)
    }

    pub(crate) fn apply_forget(
        &mut self,
        group: &str,
        generation: Generation,
        provisional: bool,
    ) -> Vec<Action> {
        if let Some(mem) = self.mem.get_mut(group) {
            mem.forgetting = false;
        }
        let g = self.state.groups.entry(group.to_owned()).or_default();
        if provisional && g.acked_gen == Some(generation) {
            return self.wake_all(group);
        }
        g.forgotten = g.forgotten.max(Some(generation));
        if g.acked_gen <= g.forgotten {
            g.acked_gen = None;
            g.acked_ms = None;
        }
        let forgotten = g.forgotten;
        g.read_set.retain(|_, r| Some(*r) > forgotten);
        if g.generation > g.forgotten {
            return self.wake_all(group);
        }
        *g = GroupState {
            forgotten: g.forgotten,
            last_ack_ms: std::mem::take(&mut g.last_ack_ms),
            read_set: std::mem::take(&mut g.read_set),
            ..GroupState::default()
        };
        if let Some(mem) = self.mem.get_mut(group) {
            mem.reset();
        }
        self.wake_all(group)
    }

    pub(crate) fn apply_redrive(
        &mut self,
        group: &str,
        generation: Generation,
        stream: String,
        up_to: MessageId,
        ms: u64,
    ) -> Vec<Action> {
        if !self.adopt(group, generation, ms) {
            return Vec::new();
        }
        if let Some(g) = self.state.groups.get_mut(group) {
            let mark = g.redrive.entry(stream).or_insert(up_to);
            *mark = (*mark).max(up_to);
        }
        self.wake_all(group)
    }

    pub(crate) fn apply_read_set(&mut self, group: &str, generation: Generation, selector: String) {
        if let Some(mem) = self.mem.get_mut(group) {
            mem.reading.remove(&selector);
        }
        self.state
            .groups
            .entry(group.to_owned())
            .or_default()
            .read(selector, generation);
    }

    pub fn replay(&mut self, group: &str, from: Bound<MessageId>) -> Record {
        Record::Generation {
            group: group.to_owned(),
            generation: self.next_generation(group),
            from,
            issued: false,
        }
    }

    pub fn redrive(&self, group: &str) -> Vec<Record> {
        let Some(generation) = self.generation(group) else {
            return Vec::new();
        };
        self.state
            .streams
            .iter()
            .filter(|(stream, _)| {
                names::is_dead(stream) && names::dead_stream(stream, group) == **stream
            })
            .filter_map(|(stream, starts)| {
                let chunk = self.state.chunks.get(starts.values().next_back()?)?;
                Some(Record::Redrive {
                    group: group.to_owned(),
                    generation,
                    stream: stream.clone(),
                    up_to: nth_id(chunk.first_id, chunk.count - 1)?,
                })
            })
            .collect()
    }

    pub fn forget(&self, group: &str) -> Option<Record> {
        Some(Record::Forget {
            group: group.to_owned(),
            generation: self.generation(group)?,
            provisional: false,
        })
    }

    pub(crate) fn reads(&self, group: &str, selector: &str, stream: &str) -> bool {
        names::matches(selector, stream)
            || (self
                .state
                .groups
                .get(group)
                .is_some_and(|g| g.redrive.contains_key(stream))
                && names::matches(selector, names::origin(stream)))
    }

    fn register(&mut self, group: &str, selector: &str) -> WaiterId {
        self.next_waiter += 1;
        let id = WaiterId(self.next_waiter);
        self.waiters
            .entry(group.to_owned())
            .or_default()
            .push_back((id, selector.to_owned()));
        id
    }

    pub fn cancel(&mut self, waiter: WaiterId) {
        for queue in self.waiters.values_mut() {
            queue.retain(|(id, _)| *id != waiter);
        }
    }

    pub fn wake_for(&mut self, group: &str, stream: &str) -> Option<Action> {
        let index = self
            .waiters
            .get(group)?
            .iter()
            .position(|(_, selector)| self.reads(group, selector, stream))?;
        let (id, _) = self.waiters.get_mut(group)?.remove(index)?;
        Some(Action::Wake(id, Some(stream.to_owned())))
    }

    pub(crate) fn wake_all(&mut self, group: &str) -> Vec<Action> {
        self.waiters
            .remove(group)
            .unwrap_or_default()
            .into_iter()
            .map(|(id, _)| Action::Wake(id, None))
            .collect()
    }

    pub(crate) fn wake_due(&mut self, now: u64) -> Vec<Action> {
        let mut ready = Vec::new();
        for (group, mem) in &mut self.mem {
            let later = mem.due.split_off(&(now + 1, FIRST_ADDRESS));
            for (_, at) in std::mem::replace(&mut mem.due, later) {
                ready.push((group.clone(), at));
            }
        }
        ready
            .into_iter()
            .filter_map(|(group, at)| {
                let stream = self.state.message_at(at)?.1.to_owned();
                self.wake_for(&group, &stream)
            })
            .collect()
    }

    pub(crate) fn release_dead_due(&mut self) -> Vec<Action> {
        let mut actions = Vec::new();
        for (group, mem) in &mut self.mem {
            let Some(g) = self.state.groups.get(group) else {
                continue;
            };
            let Some(generation) = g.generation else {
                continue;
            };
            if mem.forgetting {
                continue;
            }
            mem.holds
                .retain(|at, hold| *hold != Hold::DeadDue || self.state.live_at(*at).is_some());
            for (at, hold) in &mut mem.holds {
                if *hold != Hold::DeadDue {
                    continue;
                }
                let Some((id, stream)) = self.state.live_at(*at) else {
                    continue;
                };
                let failure = g.failures.get(at);
                let reason = if failure.is_some_and(|f| f.count >= MAX_FAILURES) {
                    Reason::Attempts
                } else {
                    Reason::Expired
                };
                *hold = Hold::DeadPending(None);
                actions.push(Action::Dead(DeadCopy {
                    group: group.clone(),
                    generation,
                    address: *at,
                    id,
                    stream: stream.to_owned(),
                    dead_stream: names::dead_stream(stream, group),
                    reason,
                    error: failure.and_then(|f| f.error.clone()),
                }));
            }
        }
        actions
    }

    pub(crate) fn forget_provisional(&mut self, now: u64) -> Vec<Action> {
        let mut actions = Vec::new();
        for (group, g) in &self.state.groups {
            let Some(generation) = g.generation else {
                continue;
            };
            let mem = self.mem.entry(group.clone()).or_default();
            let acking = mem.holds.values().any(|h| {
                matches!(
                    h,
                    Hold::AckPending(_) | Hold::FailurePending(_) | Hold::DeadPending(_)
                )
            });
            if g.acked_gen.is_none()
                && !mem.forgetting
                && !acking
                && now >= g.since_ms + PROVISIONAL_MS
            {
                mem.forgetting = true;
                actions.push(Action::Write(Record::Forget {
                    group: group.clone(),
                    generation,
                    provisional: true,
                }));
            }
        }
        actions
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupStats {
    pub pending: u64,
    pub in_flight: u64,
    pub dead: u64,
    pub oldest_ms: Option<u64>,
    pub provisional: bool,
}

impl Core {
    pub fn group_stats(&self, group: &str, now: u64) -> Option<GroupStats> {
        let g = self.state.groups.get(group)?;
        g.generation?;
        let mem = self.mem.get(group);
        let selectors: Vec<&str> = g
            .read_set
            .keys()
            .chain(mem.iter().flat_map(|m| m.reading.iter()))
            .map(String::as_str)
            .collect();
        let mut stats = GroupStats {
            pending: 0,
            in_flight: 0,
            dead: 0,
            oldest_ms: None,
            provisional: g.acked_gen.is_none(),
        };
        for (stream, starts) in &self.state.streams {
            let reads = selectors.iter().any(|s| names::matches(s, stream));
            let redriven = g.redrive.get(stream).filter(|_| {
                selectors
                    .iter()
                    .any(|s| names::matches(s, names::origin(stream)))
            });
            let own_dead = names::is_dead(stream) && names::dead_stream(stream, group) == *stream;
            if !(reads || redriven.is_some() || own_dead) {
                continue;
            }
            let tombstone = self.state.tombstone(stream);
            for start in starts.values() {
                let Some(chunk) = self.state.chunks.get(start) else {
                    continue;
                };
                for n in 0..chunk.count {
                    let Some(id) = nth_id(chunk.first_id, n) else {
                        break;
                    };
                    let at = Address {
                        segment: start.segment,
                        position: start.position + n,
                    };
                    if tombstone.is_some_and(|t| id <= t) || g.is_acked(at) {
                        continue;
                    }
                    let deliverable =
                        (reads && !g.before_from(id)) || redriven.is_some_and(|m| id <= *m);
                    if !deliverable {
                        stats.dead += u64::from(own_dead);
                        continue;
                    }
                    let hold = mem.and_then(|m| m.holds.get(&at));
                    let expired = self.state.timing.get(&at).and_then(|t| t.expire_at);
                    let leased = matches!(hold, Some(Hold::Leased(_) | Hold::AckPending(_)));
                    if matches!(hold, Some(Hold::DeadDue | Hold::DeadPending(_)))
                        || g.failures.get(&at).is_some_and(|f| f.count >= MAX_FAILURES)
                        || (!leased && expired.is_some_and(|e| e <= now))
                    {
                        stats.dead += 1;
                        continue;
                    }
                    stats.oldest_ms = Some(stats.oldest_ms.map_or(id.ms(), |o| o.min(id.ms())));
                    if leased {
                        stats.in_flight += 1;
                    } else {
                        stats.pending += 1;
                    }
                }
            }
        }
        Some(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::{Bury, Settle};
    use crate::testkit::*;
    use crate::{PROVISIONAL_MS, Timing};

    fn bury(core: &mut Core, receipt: u128, ids: &[MessageId], now: u64) {
        let settle = Settle {
            dead: ids.iter().map(|&id| Bury { id, error: None }).collect(),
            ..Settle::default()
        };
        let actions = core.settle(Receipt(receipt), &settle, now).unwrap();
        sync(core, actions, now);
    }

    fn ack_all(core: &mut Core, receipt: u128, now: u64) {
        let actions = core.ack_all(Receipt(receipt), now).unwrap();
        sync(core, actions, now);
    }

    #[test]
    fn group_stats_counts_a_message_that_goes_dead_as_dead_not_pending() {
        let expired = Timing {
            delay_until: None,
            expire_at: Some(10),
        };
        for pressure in [true, false] {
            let mut core = Core::new(INC);
            publish_timed(&mut core, "s", addr(1, 0), 1, 1, expired);
            core.set_pressure(pressure);
            assert!(take(&mut core, "g", "s", 10, 1, 20).is_empty());
            let stats = core.group_stats("g", 20).unwrap();
            assert_eq!((stats.pending, stats.dead), (0, 1), "pressure {pressure}");
        }
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        take(&mut core, "g", "s", 10, 1, 2);
        let failure = Record::Failure {
            group: "g".to_owned(),
            generation: core.generation("g").unwrap(),
            address: addr(1, 0),
            stream: "s".to_owned(),
            count: MAX_FAILURES,
            not_before: 0,
            error: None,
        };
        core.apply(failure, 3);
        core.mem.get_mut("g").unwrap().holds.clear();
        let stats = core.group_stats("g", 3).unwrap();
        assert_eq!((stats.pending, stats.dead), (0, 1), "a fifth failure");
    }

    #[test]
    fn group_stats_counts_a_message_past_its_ttl_as_dead_and_not_in_the_age() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        let expiring = Timing {
            delay_until: None,
            expire_at: Some(10),
        };
        publish_timed(&mut core, "s", addr(1, 1), 2, 1, expiring);
        take(&mut core, "g", "s", 1, 1, 3);
        ack_all(&mut core, 1, 3);
        let stats = core.group_stats("g", 20).unwrap();
        assert_eq!((stats.pending, stats.dead, stats.oldest_ms), (0, 1, None));
    }

    #[test]
    fn group_stats_counts_a_leased_or_ack_pending_message_past_its_ttl_as_in_flight() {
        let mut core = Core::new(INC);
        let expiring = Timing {
            delay_until: None,
            expire_at: Some(10),
        };
        publish_timed(&mut core, "s", addr(1, 0), 1, 1, expiring);
        take(&mut core, "g", "s", 1, 1, 3);
        let stats = core.group_stats("g", 20).unwrap();
        assert_eq!((stats.in_flight, stats.dead), (1, 0), "leased");
        let actions = core.ack_all(Receipt(1), 20).unwrap();
        let stats = core.group_stats("g", 20).unwrap();
        assert_eq!((stats.in_flight, stats.dead), (1, 0), "ack pending");
        sync(&mut core, actions, 20);
        let stats = core.group_stats("g", 20).unwrap();
        assert_eq!((stats.in_flight, stats.dead), (0, 0), "acked");
    }

    #[test]
    fn a_new_group_starts_at_the_oldest_held_message() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 2);
        publish(&mut core, "s", addr(1, 2), 2, 1);
        take(&mut core, "a", "s", 10, 1, 3);
        ack_all(&mut core, 1, 3);
        assert_eq!(
            ids(&take(&mut core, "b", "s", 10, 2, 4)),
            [id(1, 0), id(1, 1), id(2, 0)]
        );
        assert_eq!(core.generation("b").map(|g| g.incarnation), Some(INC));
    }

    #[test]
    fn a_take_registers_a_waiter_only_when_asked_to_wait() {
        let mut core = Core::new(INC);
        let (outcome, _) = core.take(&request("g", "s", 1, 1), 0);
        assert_eq!(outcome, TakeOutcome::NoWork { register: None });
        let waiter = wait(&mut core, "g", "s", 0);
        let woken = publish(&mut core, "s", addr(1, 0), 1, 1);
        assert_eq!(woken, [Action::Wake(waiter, Some("s".to_owned()))]);
    }

    #[test]
    fn a_publish_wakes_one_waiter_and_a_take_that_leaves_work_wakes_the_next() {
        let mut core = Core::new(INC);
        let first = wait(&mut core, "g", "s", 0);
        let second = wait(&mut core, "g", "s", 0);
        let other = wait(&mut core, "g", "t", 0);
        assert_eq!(
            publish(&mut core, "s", addr(1, 0), 1, 3),
            [Action::Wake(first, Some("s".to_owned()))]
        );
        let (_, actions) = core.take(&request("g", "s", 2, 1), 1);
        assert_eq!(
            without_reads(actions),
            [Action::Wake(second, Some("s".to_owned()))]
        );
        let (_, actions) = core.take(&request("g", "s", 1, 2), 1);
        assert_eq!(without_reads(actions), []);
        core.cancel(other);
        assert_eq!(publish(&mut core, "t", addr(1, 3), 2, 1), []);
    }

    #[test]
    fn a_pattern_take_reads_every_matching_stream_but_no_dead_stream() {
        let mut core = Core::new(INC);
        publish(&mut core, "orders.eu", addr(1, 0), 1, 1);
        publish(&mut core, "orders.us", addr(1, 1), 2, 1);
        publish(&mut core, "orders.dead.g", addr(1, 2), 3, 1);
        publish(&mut core, "payments", addr(1, 3), 4, 1);
        assert_eq!(
            ids(&take(&mut core, "g", "orders.*", 10, 1, 5)),
            [id(1, 0), id(2, 0)]
        );
        assert_eq!(
            ids(&take(&mut core, "g", "orders.dead.g", 10, 2, 5)),
            [id(3, 0)]
        );
    }

    #[test]
    fn a_delayed_message_waits_in_the_heap_and_its_time_wakes_a_waiter() {
        let mut core = Core::new(INC);
        let timing = Timing {
            delay_until: Some(5_000),
            expire_at: None,
        };
        publish_timed(&mut core, "s", addr(1, 0), 1, 1, timing);
        let waiter = wait(&mut core, "g", "s", 10);
        assert_eq!(core.tick(4_999), []);
        assert_eq!(
            core.tick(5_000),
            [Action::Wake(waiter, Some("s".to_owned()))]
        );
        assert_eq!(take(&mut core, "g", "s", 1, 1, 5_000).len(), 1);
    }

    #[test]
    fn an_expired_message_goes_dead_once_with_reason_expired() {
        let mut core = Core::new(INC);
        let timing = Timing {
            delay_until: None,
            expire_at: Some(100),
        };
        publish_timed(&mut core, "s", addr(1, 0), 1, 1, timing);
        let (outcome, actions) = core.take(&request("g", "s", 1, 1), 100);
        assert_eq!(outcome, TakeOutcome::NoWork { register: None });
        let actions = without_reads(actions);
        let [Action::Dead(dead)] = actions.as_slice() else {
            panic!("expected one dead copy, got {actions:?}");
        };
        assert_eq!(
            (dead.reason.as_str(), dead.dead_stream.as_str()),
            ("expired", "s.dead.g")
        );
        let (_, again) = core.take(&request("g", "s", 1, 2), 101);
        assert_eq!(without_reads(again), []);
        sync(&mut core, actions, 102);
        assert_eq!(
            without_reads(core.take(&request("g", "s", 1, 3), 103).1),
            []
        );
        assert_eq!(take(&mut core, "x", "s.dead.g", 10, 4, 103).len(), 1);
    }

    #[test]
    fn under_pressure_a_dead_copy_waits_until_the_pressure_ends() {
        let mut core = Core::new(INC);
        let timing = Timing {
            delay_until: None,
            expire_at: Some(100),
        };
        publish_timed(&mut core, "s", addr(1, 0), 1, 1, timing);
        core.set_pressure(true);
        let (outcome, actions) = core.take(&request("g", "s", 1, 1), 200);
        assert_eq!(
            (outcome, without_reads(actions)),
            (TakeOutcome::NoWork { register: None }, Vec::new())
        );
        assert_eq!(core.tick(300), []);
        core.set_pressure(false);
        let actions = core.tick(400);
        assert!(matches!(actions.as_slice(), [Action::Dead(d)] if d.reason == Reason::Expired));
        assert_eq!(core.tick(500), []);
    }

    #[test]
    fn a_dead_copy_waiting_on_pressure_is_dropped_when_its_stream_is_deleted() {
        let mut core = Core::new(INC);
        let timing = Timing {
            delay_until: None,
            expire_at: Some(5),
        };
        publish_timed(&mut core, "s", addr(1, 0), 1, 1, timing);
        core.set_pressure(true);
        assert!(take(&mut core, "g", "s", 1, 1, 10).is_empty());
        let delete = Record::Tombstone {
            stream: "s".into(),
            up_to: id(20, 0),
        };
        core.apply(delete, 20);
        core.set_pressure(false);
        assert_eq!(core.tick(30), []);
    }

    #[test]
    fn a_dead_copy_of_a_dead_copy_goes_to_the_first_dead_stream() {
        let mut core = Core::new(INC);
        publish(&mut core, "orders", addr(1, 0), 1, 1);
        let handed = take(&mut core, "g", "orders", 1, 1, 2);
        bury(&mut core, 1, &ids(&handed), 3);
        for record in core.redrive("g") {
            sync(&mut core, vec![Action::Write(record)], 4);
        }
        let again = take(&mut core, "g", "orders", 1, 2, 5);
        assert_eq!(again[0].stream, "orders.dead.g");
        let settle = Settle {
            dead: vec![Bury {
                id: again[0].id,
                error: None,
            }],
            ..Settle::default()
        };
        let actions = core.settle(Receipt(2), &settle, 6).unwrap();
        let [Action::Dead(dead)] = actions.as_slice() else {
            panic!("expected one dead copy, got {actions:?}");
        };
        assert_eq!(
            (dead.stream.as_str(), dead.dead_stream.as_str()),
            ("orders.dead.g", "orders.dead.g")
        );
    }

    #[test]
    fn redrive_delivers_the_dead_copies_up_to_the_mark_once() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 3);
        let handed = take(&mut core, "g", "s", 2, 1, 2);
        bury(&mut core, 1, &ids(&handed), 3);
        let marks = core.redrive("g");
        assert_eq!(marks.len(), 1);
        let late = take(&mut core, "g", "s", 1, 2, 4);
        bury(&mut core, 2, &ids(&late), 5);
        for record in marks {
            sync(&mut core, vec![Action::Write(record)], 6);
        }
        let redriven = take(&mut core, "g", "s", 10, 3, 7);
        assert_eq!(redriven.len(), 2);
        assert!(redriven.iter().all(|h| h.stream == "s.dead.g"));
        ack_all(&mut core, 3, 8);
        assert!(take(&mut core, "g", "s", 10, 4, 9).is_empty());
        assert!(take(&mut core, "h", "s.*", 10, 5, 9).is_empty());
    }

    #[test]
    fn an_ack_counts_only_in_the_current_generation_and_an_old_settle_is_refused() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        take(&mut core, "g", "s", 1, 1, 2);
        let old = core.generation("g");
        let old_ack = core.ack_all(Receipt(1), 3).unwrap();
        let replay = core.replay("g", Bound::Included(id(0, 0)));
        sync(&mut core, vec![Action::Write(replay)], 4);
        assert_ne!(core.generation("g"), old);
        sync(&mut core, old_ack, 5);
        assert_eq!(
            core.ack_all(Receipt(1), 6),
            Err(crate::lease::SettleError::OldGeneration)
        );
        assert_eq!(take(&mut core, "g", "s", 1, 2, 7).len(), 1);
    }

    #[test]
    fn a_replay_delivers_again_from_its_time() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1_000, 1);
        publish(&mut core, "s", addr(1, 1), 5_000, 2);
        take(&mut core, "g", "s", 10, 1, 6_000);
        ack_all(&mut core, 1, 6_000);
        let from = MessageId::new(3_000, 0, 0).unwrap();
        let replay = core.replay("g", Bound::Included(from));
        sync(&mut core, vec![Action::Write(replay)], 7_000);
        assert_eq!(
            ids(&take(&mut core, "g", "s", 10, 2, 8_000)),
            [id(5_000, 0), id(5_000, 1)]
        );
    }

    #[test]
    fn two_replays_at_once_settle_on_one_winner_in_any_order() {
        let mut core = Core::new(INC);
        take(&mut core, "g", "s", 1, 1, 0);
        let first = core.replay("g", Bound::Included(id(10, 0)));
        let second = core.replay("g", Bound::Included(id(20, 0)));
        let mut other = Core::new(INC);
        for (core, order) in [
            (&mut core, [&first, &second]),
            (&mut other, [&second, &first]),
        ] {
            for record in order {
                core.apply(record.clone(), 1);
            }
        }
        let Record::Generation { generation, .. } = second else {
            panic!("replay gives a generation record");
        };
        assert_eq!(core.generation("g"), Some(generation));
        assert_eq!(other.generation("g"), Some(generation));
        assert_eq!(
            core.state.groups["g"].from,
            Some(Bound::Included(id(20, 0)))
        );
        assert_eq!(
            other.state.groups["g"].from,
            Some(Bound::Included(id(20, 0)))
        );
    }

    #[test]
    fn a_provisional_group_is_forgotten_after_a_day_with_no_ack() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        take(&mut core, "g", "s", 1, 1, 10);
        let old = core.generation("g").unwrap();
        assert_eq!(core.tick(10 + PROVISIONAL_MS - 1), []);
        let forget = core.tick(10 + PROVISIONAL_MS);
        assert_eq!(
            forget,
            [Action::Write(Record::Forget {
                group: "g".to_owned(),
                generation: old,
                provisional: true,
            })]
        );
        assert_eq!(core.tick(11 + PROVISIONAL_MS), []);
        sync(&mut core, forget, 11 + PROVISIONAL_MS);
        assert_eq!(core.generation("g"), None);
        assert_eq!(
            take(&mut core, "g", "s", 1, 2, 12 + PROVISIONAL_MS).len(),
            1
        );
        assert!(core.generation("g").unwrap() > old);
    }

    #[test]
    fn a_group_with_an_ack_is_not_provisional_and_is_not_forgotten() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        take(&mut core, "g", "s", 1, 1, 10);
        ack_all(&mut core, 1, 11);
        assert_eq!(core.tick(10 * PROVISIONAL_MS), []);
    }

    #[test]
    fn no_provisional_forget_while_an_ack_waits_for_its_block() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 2);
        take(&mut core, "g", "s", 1, 1, 10);
        take(&mut core, "g", "s", 1, 2, 8 + PROVISIONAL_MS);
        let pending = core.ack_all(Receipt(2), 9 + PROVISIONAL_MS).unwrap();
        assert_eq!(core.tick(10 + PROVISIONAL_MS), []);
        sync(&mut core, pending, 10 + PROVISIONAL_MS);
        assert_eq!(core.tick(11 + PROVISIONAL_MS), []);
        assert_eq!(
            ids(&take(&mut core, "g", "s", 10, 3, 12 + PROVISIONAL_MS)),
            [id(1, 0)]
        );
    }

    #[test]
    fn a_provisional_forget_after_an_ack_of_its_generation_is_refused() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        take(&mut core, "g", "s", 1, 1, 10);
        let generation = core.generation("g").unwrap();
        ack_all(&mut core, 1, 11);
        let forget = Record::Forget {
            group: "g".to_owned(),
            generation,
            provisional: true,
        };
        sync(&mut core, vec![Action::Write(forget)], 12);
        assert_eq!(core.generation("g"), Some(generation));
        assert!(take(&mut core, "g", "s", 1, 2, 13).is_empty());
    }

    #[test]
    fn while_a_provisional_forget_waits_a_settle_is_refused_and_a_take_gets_no_work() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 2);
        take(&mut core, "g", "s", 1, 1, 10);
        take(&mut core, "g", "s", 1, 2, PROVISIONAL_MS);
        let forgets = core
            .tick(10 + PROVISIONAL_MS)
            .into_iter()
            .filter(|a| matches!(a, Action::Write(Record::Forget { .. })))
            .count();
        assert_eq!(forgets, 1);
        assert_eq!(
            core.ack_all(Receipt(2), 10 + PROVISIONAL_MS),
            Err(crate::lease::SettleError::OldGeneration)
        );
        assert!(take(&mut core, "g", "s", 1, 3, 11 + PROVISIONAL_MS).is_empty());
    }

    #[test]
    fn a_pattern_take_starts_after_the_stream_it_served_last() {
        let mut core = Core::new(INC);
        publish(&mut core, "a", addr(1, 0), 1, 5);
        publish(&mut core, "b", addr(1, 5), 2, 1);
        assert_eq!(
            ids(&take(&mut core, "g", "*", 2, 1, 3)),
            [id(1, 0), id(1, 1)]
        );
        assert_eq!(
            ids(&take(&mut core, "g", "*", 2, 2, 3)),
            [id(2, 0), id(1, 2)]
        );
        assert_eq!(
            ids(&take(&mut core, "g", "*", 2, 3, 3)),
            [id(1, 3), id(1, 4)]
        );
    }

    #[test]
    fn a_take_with_limit_zero_hands_out_one_message() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 2);
        let req = TakeRequest {
            wait: true,
            ..request("g", "s", 0, 1)
        };
        let (outcome, _) = core.take(&req, 2);
        let TakeOutcome::Leased { messages, .. } = outcome else {
            panic!("expected one message, got {outcome:?}");
        };
        assert_eq!(ids(&messages), [id(1, 0)]);
    }

    #[test]
    fn an_ack_keeps_every_timing_entry_until_its_segment_goes() {
        let mut core = Core::new(INC);
        let delayed = Timing {
            delay_until: Some(5),
            expire_at: None,
        };
        let ttl = Timing {
            delay_until: None,
            expire_at: Some(100),
        };
        publish_timed(&mut core, "s", addr(1, 0), 1, 1, delayed);
        publish_timed(&mut core, "s", addr(1, 1), 2, 1, ttl);
        take(&mut core, "g", "s", 10, 1, 5);
        ack_all(&mut core, 1, 6);
        assert_eq!(
            core.state.timing.keys().copied().collect::<Vec<_>>(),
            [addr(1, 0), addr(1, 1)]
        );
        let (_, actions) = core.take(&request("h", "s", 10, 2), 100);
        assert!(matches!(
            without_reads(actions).as_slice(),
            [Action::Dead(d)] if d.reason == Reason::Expired
        ));
    }

    #[test]
    fn a_replay_record_after_a_take_on_a_group_with_no_generation_still_applies() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1_000, 1);
        publish(&mut core, "s", addr(1, 1), 5_000, 1);
        let replay = core.replay("g", Bound::Included(id(3_000, 0)));
        assert_eq!(ids(&take(&mut core, "g", "s", 1, 1, 6_000)), [id(1_000, 0)]);
        sync(&mut core, vec![Action::Write(replay)], 7_000);
        assert_eq!(
            core.state.groups["g"].from,
            Some(Bound::Included(id(3_000, 0)))
        );
        assert_eq!(
            ids(&take(&mut core, "g", "s", 10, 2, 8_000)),
            [id(5_000, 0)]
        );
    }

    #[test]
    fn a_forget_ends_the_generation_and_a_later_take_starts_a_new_one_at_the_oldest() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        take(&mut core, "g", "s", 1, 1, 2);
        ack_all(&mut core, 1, 3);
        let forget = core.forget("g").unwrap();
        sync(&mut core, vec![Action::Write(forget)], 4);
        assert_eq!(core.forget("g"), None);
        assert_eq!(take(&mut core, "g", "s", 1, 2, 5).len(), 1);
    }

    #[test]
    fn a_tombstone_hides_older_messages_and_their_dead_copies() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 2);
        let handed = take(&mut core, "g", "s", 1, 1, 2);
        bury(&mut core, 1, &ids(&handed), 3);
        core.apply(
            Record::Tombstone {
                stream: "s".to_owned(),
                up_to: id(10, 0),
            },
            10,
        );
        publish(&mut core, "s", addr(1, 5), 11, 1);
        assert_eq!(ids(&take(&mut core, "h", "s", 10, 2, 12)), [id(11, 0)]);
        assert!(take(&mut core, "h", "s.dead.g", 10, 3, 12).is_empty());
    }

    #[test]
    fn the_merge_of_control_records_does_not_depend_on_their_order() {
        let g1 = Generation {
            counter: 1,
            incarnation: INC,
        };
        let g2 = Generation {
            counter: 2,
            incarnation: 3,
        };
        let records = vec![
            Record::Ack {
                group: "a".into(),
                generation: g1,
                address: addr(1, 0),
                stream: "s".into(),
            },
            Record::Failure {
                group: "a".into(),
                generation: g1,
                address: addr(1, 1),
                stream: "s".into(),
                count: 2,
                not_before: 9,
                error: Some("x".into()),
            },
            Record::Failure {
                group: "a".into(),
                generation: g1,
                address: addr(1, 1),
                stream: "s".into(),
                count: 1,
                not_before: 5,
                error: None,
            },
            Record::Failure {
                group: "a".into(),
                generation: g1,
                address: addr(1, 0),
                stream: "s".into(),
                count: 1,
                not_before: 5,
                error: None,
            },
            Record::Ack {
                group: "b".into(),
                generation: g1,
                address: addr(1, 0),
                stream: "s".into(),
            },
            Record::Forget {
                group: "b".into(),
                generation: g1,
                provisional: false,
            },
            Record::Ack {
                group: "b".into(),
                generation: g2,
                address: addr(1, 1),
                stream: "s".into(),
            },
            Record::Generation {
                group: "c".into(),
                generation: g2,
                from: Bound::Included(id(1, 1)),
                issued: false,
            },
            Record::Ack {
                group: "c".into(),
                generation: g1,
                address: addr(1, 0),
                stream: "s".into(),
            },
            Record::Tombstone {
                stream: "s".into(),
                up_to: id(1, 0),
            },
            Record::Tombstone {
                stream: "s".into(),
                up_to: id(0, 5),
            },
            Record::ReadSet {
                group: "a".into(),
                generation: g1,
                selector: "*".into(),
            },
            Record::ReadSet {
                group: "b".into(),
                generation: g1,
                selector: "t".into(),
            },
        ];
        let fold = |order: &mut dyn Iterator<Item = (usize, &Record)>| {
            let mut core = Core::new(INC);
            publish(&mut core, "s", addr(1, 0), 1, 2);
            for (n, record) in order {
                core.apply(record.clone(), n as u64 + 100);
            }
            core
        };
        let forward = fold(&mut records.iter().enumerate());
        let backward = fold(&mut records.iter().enumerate().rev());
        assert_eq!(forward.state(), backward.state());
        assert_eq!(forward.generation("b"), Some(g2));
        assert!(forward.state.groups["b"].acked_gen == Some(g2));
        assert_eq!(forward.state.groups["a"].failures.len(), 1);
        assert!(forward.state.groups["a"].read_set.contains_key("*"));
        assert!(!forward.state.groups["b"].read_set.contains_key("t"));
    }

    #[test]
    fn a_fifth_failure_that_folds_before_its_message_is_never_handed_out() {
        let mut core = Core::new(INC);
        let generation = Generation {
            counter: 1,
            incarnation: INC,
        };
        let failure = Record::Failure {
            group: "g".to_owned(),
            generation,
            address: addr(1, 0),
            stream: "s".to_owned(),
            count: MAX_FAILURES,
            not_before: 1,
            error: None,
        };
        core.apply(failure, 1);
        assert_eq!(core.tick(2), []);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        let (outcome, actions) = core.take(&request("g", "s", 1, 1), 3);
        assert_eq!(outcome, TakeOutcome::NoWork { register: None });
        assert!(matches!(
            without_reads(actions).as_slice(),
            [Action::Dead(d)] if d.reason == Reason::Attempts
        ));
    }

    #[test]
    fn a_replay_from_a_message_id_starts_after_that_id() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 3);
        take(&mut core, "g", "s", 10, 1, 2);
        ack_all(&mut core, 1, 2);
        let replay = core.replay("g", Bound::Excluded(id(1, 1)));
        sync(&mut core, vec![Action::Write(replay)], 3);
        assert_eq!(ids(&take(&mut core, "g", "s", 10, 2, 4)), [id(1, 2)]);
    }

    #[test]
    fn a_wake_names_its_stream_so_a_lost_wake_goes_to_the_next_waiter() {
        let mut core = Core::new(INC);
        let first = wait(&mut core, "g", "s", 0);
        let second = wait(&mut core, "g", "s", 0);
        let stream = Some("s".to_owned());
        assert_eq!(
            publish(&mut core, "s", addr(1, 0), 1, 1),
            [Action::Wake(first, stream.clone())]
        );
        assert_eq!(core.wake_for("g", "s"), Some(Action::Wake(second, stream)));
    }

    #[test]
    #[should_panic(expected = "already in use")]
    fn a_take_with_a_receipt_already_in_use_is_refused() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 2);
        take(&mut core, "g", "s", 1, 1, 2);
        take(&mut core, "g", "s", 1, 1, 3);
    }

    #[test]
    fn a_read_set_record_is_written_once_until_it_applies_or_its_block_is_abandoned() {
        let read_sets = |actions: &[Action]| {
            actions
                .iter()
                .filter(|a| matches!(a, Action::Write(Record::ReadSet { .. })))
                .count()
        };
        let mut core = Core::new(INC);
        let (_, first) = core.take(&request("g", "s", 1, 1), 1);
        let (_, second) = core.take(&request("g", "s", 1, 2), 2);
        assert_eq!((read_sets(&first), read_sets(&second)), (1, 0));
        let generation = core.generation("g").unwrap();
        core.abandon("g", generation, &[], false);
        let (_, third) = core.take(&request("g", "s", 1, 3), 3);
        assert_eq!(read_sets(&third), 1);
        sync(&mut core, third, 4);
        let (_, fourth) = core.take(&request("g", "s", 1, 4), 5);
        assert_eq!(read_sets(&fourth), 0);
    }

    fn is_provisional_forget(actions: &[Action]) -> bool {
        matches!(
            actions,
            [Action::Write(Record::Forget {
                provisional: true,
                ..
            })]
        )
    }

    #[test]
    fn no_provisional_forget_while_a_failure_waits_for_its_block() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        let handed = take(&mut core, "g", "s", 1, 1, 10);
        let nack = Settle {
            nack: vec![crate::lease::Nack {
                id: handed[0].id,
                delay_ms: None,
                error: None,
            }],
            ..Settle::default()
        };
        let pending = core.settle(Receipt(1), &nack, 20).unwrap();
        assert_eq!(core.tick(10 + PROVISIONAL_MS), []);
        sync(&mut core, pending, 10 + PROVISIONAL_MS);
        assert!(is_provisional_forget(&core.tick(11 + PROVISIONAL_MS)));
    }

    #[test]
    fn an_abandoned_failure_does_not_end_a_waiting_provisional_forget() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 2);
        take(&mut core, "g", "s", 1, 1, 10);
        assert_eq!(core.tick(10 + LEASE), []);
        let late = 10 + PROVISIONAL_MS - 1_000;
        take(&mut core, "g", "s", 1, 2, late);
        let kept = take(&mut core, "g", "s", 1, 3, late + 500);
        let generation = core.generation("g").unwrap();
        assert!(is_provisional_forget(&core.tick(10 + PROVISIONAL_MS)));
        let failed: Vec<Address> = core
            .tick(late + LEASE)
            .iter()
            .filter_map(|a| match a {
                Action::Write(Record::Failure { address, .. }) => Some(*address),
                _ => None,
            })
            .collect();
        assert_eq!(failed, [addr(1, 0)]);
        core.abandon("g", generation, &failed, false);
        let ack = Settle {
            ack: ids(&kept),
            ..Settle::default()
        };
        assert_eq!(
            core.settle(Receipt(3), &ack, late + LEASE),
            Err(crate::lease::SettleError::OldGeneration)
        );
        assert!(take(&mut core, "g", "s", 2, 4, late + LEASE).is_empty());
    }

    #[test]
    fn an_abandoned_provisional_forget_wakes_the_waiters_of_the_group() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        take(&mut core, "g", "s", 1, 1, 10);
        let generation = core.generation("g").unwrap();
        assert!(is_provisional_forget(&core.tick(10 + PROVISIONAL_MS)));
        let waiter = wait(&mut core, "g", "s", 10 + PROVISIONAL_MS);
        assert_eq!(
            core.abandon("g", generation, &[], true),
            [Action::Wake(waiter, None)]
        );
        assert_eq!(
            take(&mut core, "g", "s", 1, 2, 11 + PROVISIONAL_MS).len(),
            1
        );
    }

    #[test]
    fn a_refused_provisional_forget_wakes_the_waiters_of_the_group() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 2);
        take(&mut core, "g", "s", 1, 1, 10);
        let generation = core.generation("g").unwrap();
        let forget = core.tick(10 + PROVISIONAL_MS);
        let waiter = wait(&mut core, "g", "s", 10 + PROVISIONAL_MS);
        let ack = Record::Ack {
            group: "g".to_owned(),
            generation,
            address: addr(1, 0),
            stream: "s".to_owned(),
        };
        core.apply(ack, 11 + PROVISIONAL_MS);
        assert_eq!(
            sync(&mut core, forget, 12 + PROVISIONAL_MS),
            [Action::Wake(waiter, None)]
        );
        assert_eq!(core.generation("g"), Some(generation));
        assert_eq!(
            ids(&take(&mut core, "g", "s", 2, 2, 13 + PROVISIONAL_MS)),
            [id(1, 1)]
        );
    }

    #[test]
    fn a_provisional_forget_of_an_older_generation_folds_the_same_in_any_order() {
        let g1 = Generation {
            counter: 1,
            incarnation: INC,
        };
        let g2 = Generation {
            counter: 2,
            incarnation: INC,
        };
        let records = [
            Record::ReadSet {
                group: "g".into(),
                generation: g1,
                selector: "t".into(),
            },
            Record::Forget {
                group: "g".into(),
                generation: g1,
                provisional: true,
            },
            Record::Ack {
                group: "g".into(),
                generation: g2,
                address: addr(1, 0),
                stream: "s".into(),
            },
        ];
        let fold = |order: &mut dyn Iterator<Item = &Record>| {
            let mut core = Core::new(INC);
            publish(&mut core, "s", addr(1, 0), 1, 1);
            for record in order {
                core.apply(record.clone(), 5);
            }
            core
        };
        let forward = fold(&mut records.iter());
        let backward = fold(&mut records.iter().rev());
        assert_eq!(forward.state(), backward.state());
        assert!(!forward.state.groups["g"].read_set.contains_key("t"));
    }
}
