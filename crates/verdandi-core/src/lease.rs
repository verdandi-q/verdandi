use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

use crate::group::Hold;
use crate::ids::{Address, MessageId, ParseIdError, SegmentId};
use crate::{Action, Core, DeadCopy, Generation, MAX_DELAY_MS, Reason, Record, SECOND_MS, names};

pub const MAX_BACKOFF_MS: u64 = 300 * SECOND_MS;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Receipt(pub u128);

impl Receipt {
    pub fn new(issuer: u64, random: u64) -> Self {
        Self(u128::from(issuer) << 64 | u128::from(random))
    }

    pub fn issuer(self) -> u64 {
        (self.0 >> 64) as u64
    }
}

impl fmt::Display for Receipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "r_{:032x}", self.0)
    }
}

impl FromStr for Receipt {
    type Err = ParseIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let hex = s.strip_prefix("r_").ok_or(ParseIdError)?;
        if hex.len() != 32 || !hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
            return Err(ParseIdError);
        }
        u128::from_str_radix(hex, 16)
            .map(Receipt)
            .map_err(|_| ParseIdError)
    }
}

pub(crate) struct Lease {
    pub(crate) group: String,
    pub(crate) generation: Generation,
    pub(crate) until: u64,
    pub(crate) ids: BTreeMap<MessageId, (Address, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Nack {
    pub id: MessageId,
    pub delay_ms: Option<u64>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bury {
    pub id: MessageId,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Settle {
    pub ack: Vec<MessageId>,
    pub nack: Vec<Nack>,
    pub dead: Vec<Bury>,
    pub extend_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SettleError {
    UnknownReceipt,
    OldGeneration,
    UnknownId(MessageId),
    Pressure,
}

pub fn backoff_ms(failures: u32) -> u64 {
    (SECOND_MS << failures.saturating_sub(1).min(16)).min(MAX_BACKOFF_MS)
}

impl Core {
    pub fn settle(
        &mut self,
        receipt: Receipt,
        settle: &Settle,
        now: u64,
    ) -> Result<Vec<Action>, SettleError> {
        let lease = self
            .leases
            .get(&receipt)
            .filter(|l| l.until > now)
            .ok_or(SettleError::UnknownReceipt)?;
        if self.generation(&lease.group) != Some(lease.generation)
            || self.mem.get(&lease.group).is_some_and(|m| m.forgetting)
        {
            return Err(SettleError::OldGeneration);
        }
        let named = settle
            .ack
            .iter()
            .chain(settle.nack.iter().map(|n| &n.id))
            .chain(settle.dead.iter().map(|d| &d.id));
        for id in named {
            if !lease.ids.contains_key(id) {
                return Err(SettleError::UnknownId(*id));
            }
        }
        if !settle.dead.is_empty() && self.pressure {
            return Err(SettleError::Pressure);
        }
        let (group, generation) = (lease.group.clone(), lease.generation);
        let failures = self.state.groups.get(&group).map(|g| &g.failures);
        let Some(lease) = self.leases.get_mut(&receipt) else {
            return Err(SettleError::UnknownReceipt);
        };
        let mem = self.mem.entry(group.clone()).or_default();
        let mut actions = Vec::new();
        let mut waiting = Vec::new();
        let mut take = |id: &MessageId, hold: Hold| {
            let (at, stream) = lease.ids.get(id)?;
            match mem.holds.get(at) {
                Some(Hold::Leased(r)) if *r == receipt => {}
                Some(
                    Hold::AckPending(r)
                    | Hold::FailurePending(Some(r))
                    | Hold::DeadPending(Some(r)),
                ) if *r == receipt => {
                    waiting.push(*at);
                    return None;
                }
                _ => return None,
            }
            mem.holds.insert(*at, hold);
            Some((*at, stream.clone()))
        };
        for id in &settle.ack {
            if let Some((address, stream)) = take(id, Hold::AckPending(receipt)) {
                actions.push(Action::Write(Record::Ack {
                    group: group.clone(),
                    generation,
                    address,
                    stream,
                }));
            }
        }
        for nack in &settle.nack {
            if let Some((address, stream)) = take(&nack.id, Hold::FailurePending(Some(receipt))) {
                let count = failures
                    .and_then(|f| f.get(&address))
                    .map_or(0, |f| f.count)
                    + 1;
                let wait = nack
                    .delay_ms
                    .map_or(backoff_ms(count), |d| d.min(MAX_DELAY_MS));
                actions.push(Action::Write(Record::Failure {
                    group: group.clone(),
                    generation,
                    address,
                    stream,
                    count,
                    not_before: now + wait,
                    error: nack.error.clone(),
                }));
            }
        }
        for bury in &settle.dead {
            if let Some((address, stream)) = take(&bury.id, Hold::DeadPending(Some(receipt))) {
                actions.push(Action::Dead(DeadCopy {
                    group: group.clone(),
                    generation,
                    address,
                    id: bury.id,
                    dead_stream: names::dead_stream(&stream, &group),
                    stream,
                    reason: Reason::Dead,
                    error: bury.error.clone(),
                }));
            }
        }
        if let Some(extend) = settle.extend_ms {
            lease.until = now + extend;
        }
        actions.extend(waiting.into_iter().map(|address| Action::Await {
            group: group.clone(),
            address,
        }));
        Ok(actions)
    }

    pub fn ack_all(&mut self, receipt: Receipt, now: u64) -> Result<Vec<Action>, SettleError> {
        let ack = self
            .leases
            .get(&receipt)
            .map(|l| l.ids.keys().copied().collect())
            .unwrap_or_default();
        self.settle(
            receipt,
            &Settle {
                ack,
                ..Settle::default()
            },
            now,
        )
    }

    pub fn set_led(&mut self, segments: BTreeSet<SegmentId>) -> Vec<Action> {
        let old = self.led.replace(segments.clone()).unwrap_or_default();
        for lost in old.difference(&segments) {
            self.drop_leases(*lost);
        }
        if segments.is_subset(&old) {
            return Vec::new();
        }
        let groups: Vec<String> = self.waiters.keys().cloned().collect();
        groups.iter().flat_map(|g| self.wake_all(g)).collect()
    }

    pub fn drop_leases(&mut self, segment: SegmentId) {
        for lease in self.leases.values_mut() {
            lease.ids.retain(|_, (at, _)| at.segment != segment);
        }
        self.leases.retain(|_, l| !l.ids.is_empty());
        for mem in self.mem.values_mut() {
            mem.holds
                .retain(|at, hold| at.segment != segment || !matches!(hold, Hold::Leased(_)));
        }
    }

    pub fn release(&mut self, receipt: Receipt, ids: &[MessageId]) -> Vec<Action> {
        let Some(lease) = self.leases.get_mut(&receipt) else {
            return Vec::new();
        };
        let group = lease.group.clone();
        let mem = self.mem.entry(group.clone()).or_default();
        let mut streams = Vec::new();
        for id in ids {
            let Some((at, stream)) = lease.ids.remove(id) else {
                continue;
            };
            if mem.holds.get(&at) == Some(&Hold::Leased(receipt)) {
                mem.holds.remove(&at);
                if let Some(d) = mem.delivered.get_mut(&at) {
                    d.count = d.count.saturating_sub(1);
                }
                streams.push(stream);
            }
        }
        if lease.ids.is_empty() {
            self.leases.remove(&receipt);
        }
        streams
            .iter()
            .filter_map(|s| self.wake_for(&group, s))
            .collect()
    }

    pub fn dead_due(&self, copy: &DeadCopy, since: MessageId) -> bool {
        self.generation(&copy.group) == Some(copy.generation)
            && matches!(
                self.mem
                    .get(&copy.group)
                    .and_then(|m| m.holds.get(&copy.address)),
                Some(Hold::DeadPending(Some(_)))
            )
            && self
                .state
                .tombstone(&copy.stream)
                .is_none_or(|t| copy.id > t)
            && self
                .state
                .tombstone(&copy.dead_stream)
                .is_none_or(|t| t <= since)
    }

    pub fn defer_dead(&mut self, group: &str, address: Address) {
        if let Some(hold) = self
            .mem
            .get_mut(group)
            .and_then(|m| m.holds.get_mut(&address))
            && *hold == Hold::DeadPending(None)
        {
            *hold = Hold::DeadDue;
        }
    }

    pub fn abandon(
        &mut self,
        group: &str,
        generation: Generation,
        addresses: &[Address],
        provisional_forget: bool,
    ) -> Vec<Action> {
        if self.generation(group) != Some(generation) {
            return Vec::new();
        }
        let mem = self.mem.entry(group.to_owned()).or_default();
        if provisional_forget {
            mem.forgetting = false;
        }
        mem.reading.clear();
        let mut freed = Vec::new();
        for at in addresses {
            let back = match mem.holds.get(at) {
                Some(
                    Hold::AckPending(r)
                    | Hold::FailurePending(Some(r))
                    | Hold::DeadPending(Some(r)),
                ) if self.state.message_at(*at).is_some_and(|(id, _)| {
                    self.leases.get(r).is_some_and(|l| l.ids.contains_key(&id))
                }) =>
                {
                    Some(Hold::Leased(*r))
                }
                Some(Hold::DeadPending(None)) => Some(Hold::DeadDue),
                Some(Hold::AckPending(_) | Hold::FailurePending(_) | Hold::DeadPending(_)) => None,
                _ => continue,
            };
            match back {
                Some(hold) => {
                    mem.holds.insert(*at, hold);
                }
                None => {
                    mem.holds.remove(at);
                    freed.push(*at);
                }
            }
        }
        let mut actions: Vec<Action> = freed
            .into_iter()
            .filter_map(|at| {
                let stream = self.state.message_at(at)?.1.to_owned();
                self.wake_for(group, &stream)
            })
            .collect();
        if provisional_forget {
            actions.extend(self.wake_all(group));
        }
        actions
    }

    pub(crate) fn expire_leases(&mut self, now: u64) -> Vec<Action> {
        let expired: Vec<Receipt> = self
            .leases
            .iter()
            .filter(|(_, l)| l.until <= now)
            .map(|(r, _)| *r)
            .collect();
        let mut actions = Vec::new();
        for receipt in expired {
            let Some(lease) = self.leases.remove(&receipt) else {
                continue;
            };
            if self.generation(&lease.group) != Some(lease.generation) {
                continue;
            }
            let failures = self.state.groups.get(&lease.group).map(|g| &g.failures);
            let mem = self.mem.entry(lease.group.clone()).or_default();
            let mut freed = Vec::new();
            for (at, stream) in lease.ids.into_values() {
                if mem.holds.get(&at) != Some(&Hold::Leased(receipt)) {
                    continue;
                }
                let d = mem.delivered.entry(at).or_default();
                if !d.free_expiry_used {
                    d.free_expiry_used = true;
                    mem.holds.remove(&at);
                    freed.push(stream);
                    continue;
                }
                mem.holds.insert(at, Hold::FailurePending(None));
                let count = failures.and_then(|f| f.get(&at)).map_or(0, |f| f.count) + 1;
                actions.push(Action::Write(Record::Failure {
                    group: lease.group.clone(),
                    generation: lease.generation,
                    address: at,
                    stream,
                    count,
                    not_before: now + backoff_ms(count),
                    error: Some("lease expired".to_owned()),
                }));
            }
            actions.extend(freed.iter().filter_map(|s| self.wake_for(&lease.group, s)));
        }
        actions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_receipt_keeps_its_issuer_through_the_text_form() {
        let receipt = Receipt::new(u64::MAX - 2, 77);
        let text = receipt.to_string();
        assert_eq!(text.len(), 34);
        let parsed: Receipt = text.parse().unwrap();
        assert_eq!(parsed, receipt);
        assert_eq!(parsed.issuer(), u64::MAX - 2);
        assert_eq!(Receipt::new(0, 5).to_string(), format!("r_{:032x}", 5));
    }
    use crate::testkit::*;
    use crate::{MAX_FAILURES, Timing};

    fn failure_counts(actions: &[Action]) -> Vec<(Address, u32, u64)> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Write(Record::Failure {
                    address,
                    count,
                    not_before,
                    ..
                }) => Some((*address, *count, *not_before)),
                _ => None,
            })
            .collect()
    }

    fn nack(id: MessageId, delay_ms: Option<u64>) -> Settle {
        Settle {
            nack: vec![Nack {
                id,
                delay_ms,
                error: Some("boom".to_owned()),
            }],
            ..Settle::default()
        }
    }

    fn ack(ids: &[MessageId]) -> Settle {
        Settle {
            ack: ids.to_vec(),
            ..Settle::default()
        }
    }

    #[test]
    fn receipt_text_is_r_and_32_hex_and_round_trips() {
        let r = Receipt(0x0123_4567_89ab_cdef_0011_2233_4455_6677);
        assert_eq!(r.to_string(), "r_0123456789abcdef0011223344556677");
        assert_eq!(r.to_string().parse::<Receipt>(), Ok(r));
        for bad in [
            "",
            "r_",
            "0123456789abcdef0011223344556677",
            "r_0123456789ABCDEF0011223344556677",
            "r_123",
        ] {
            assert_eq!(bad.parse::<Receipt>(), Err(ParseIdError), "{bad}");
        }
    }

    #[test]
    fn nack_backoff_starts_at_one_second_and_doubles_to_five_minutes() {
        let got: Vec<u64> = [1, 2, 3, 9, 10, 40, u32::MAX].map(backoff_ms).to_vec();
        assert_eq!(
            got,
            [1_000, 2_000, 4_000, 256_000, 300_000, 300_000, 300_000]
        );
    }

    #[test]
    fn an_ack_settles_only_the_named_id_and_the_others_stay_leased() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 3);
        let handed = take(&mut core, "g", "s", 10, 1, 10);
        let actions = core.settle(Receipt(1), &ack(&[handed[0].id]), 20).unwrap();
        assert_eq!(
            actions,
            [Action::Write(Record::Ack {
                group: "g".to_owned(),
                generation: core.generation("g").unwrap(),
                address: handed[0].address,
                stream: "s".to_owned(),
            })]
        );
        sync(&mut core, actions, 20);
        assert!(take(&mut core, "g", "s", 10, 2, 30).is_empty());
        assert_eq!(
            core.settle(Receipt(1), &ack(&[handed[1].id]), 30)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn a_settle_with_an_unknown_receipt_or_id_is_refused_and_applies_nothing() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 2);
        let handed = take(&mut core, "g", "s", 1, 1, 10);
        assert_eq!(
            core.settle(Receipt(9), &ack(&[handed[0].id]), 20),
            Err(SettleError::UnknownReceipt)
        );
        let foreign = id(1, 1);
        assert_eq!(
            core.settle(Receipt(1), &ack(&[handed[0].id, foreign]), 20),
            Err(SettleError::UnknownId(foreign))
        );
        assert_eq!(
            core.settle(Receipt(1), &ack(&[handed[0].id]), 20)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn a_settled_id_settled_again_gets_no_change() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        let handed = take(&mut core, "g", "s", 1, 1, 10);
        let first = core.settle(Receipt(1), &ack(&[handed[0].id]), 20).unwrap();
        sync(&mut core, first, 20);
        assert_eq!(
            core.settle(Receipt(1), &ack(&[handed[0].id]), 21),
            Ok(Vec::new())
        );
        assert_eq!(
            core.settle(Receipt(1), &nack(handed[0].id, None), 21),
            Ok(Vec::new())
        );
    }

    #[test]
    fn an_expired_receipt_is_refused() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        let handed = take(&mut core, "g", "s", 1, 1, 10);
        assert_eq!(
            core.settle(Receipt(1), &ack(&[handed[0].id]), 10 + LEASE),
            Err(SettleError::UnknownReceipt)
        );
    }

    #[test]
    fn a_nack_waits_for_its_backoff_then_the_message_comes_back() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        let handed = take(&mut core, "g", "s", 1, 1, 10);
        let actions = core
            .settle(Receipt(1), &nack(handed[0].id, None), 100)
            .unwrap();
        assert_eq!(failure_counts(&actions), [(handed[0].address, 1, 1_100)]);
        sync(&mut core, actions, 100);
        assert!(take(&mut core, "g", "s", 1, 2, 1_099).is_empty());
        let again = take(&mut core, "g", "s", 1, 3, 1_100);
        assert_eq!(ids(&again), ids(&handed));
        assert_eq!(again[0].attempt, 2);
    }

    #[test]
    fn a_nack_delay_replaces_the_backoff_and_the_count_still_goes_up() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        let handed = take(&mut core, "g", "s", 1, 1, 10);
        let actions = core
            .settle(Receipt(1), &nack(handed[0].id, Some(30_000)), 100)
            .unwrap();
        assert_eq!(failure_counts(&actions), [(handed[0].address, 1, 30_100)]);
        sync(&mut core, actions, 100);
        assert!(take(&mut core, "g", "s", 1, 2, 30_099).is_empty());
        take(&mut core, "g", "s", 1, 3, 30_100);
        let actions = core
            .settle(Receipt(3), &nack(handed[0].id, Some(u64::MAX / 2)), 30_200)
            .unwrap();
        assert_eq!(
            failure_counts(&actions),
            [(handed[0].address, 2, 30_200 + MAX_DELAY_MS)]
        );
    }

    #[test]
    fn the_first_lease_expiry_is_free_and_later_expiries_count_as_failures() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        let at = addr(1, 0);
        take(&mut core, "g", "s", 1, 1, 0);
        assert_eq!(failure_counts(&core.tick(LEASE)), []);
        let again = take(&mut core, "g", "s", 1, 2, LEASE);
        assert_eq!(again[0].attempt, 2);
        let actions = core.tick(2 * LEASE);
        assert_eq!(failure_counts(&actions), [(at, 1, 2 * LEASE + 1_000)]);
        sync(&mut core, actions, 2 * LEASE);
        assert_eq!(
            take(&mut core, "g", "s", 1, 3, 2 * LEASE + 1_000)[0].attempt,
            3
        );
    }

    fn fail_to(core: &mut Core, failures: u32, mut now: u64) -> u64 {
        for _ in 0..failures {
            let receipt = 100 + u128::from(now);
            let handed = take(core, "g", "s", 1, receipt, now);
            let actions = core
                .settle(Receipt(receipt), &nack(handed[0].id, Some(0)), now)
                .unwrap();
            sync(core, actions, now);
            now += 1;
        }
        now
    }

    #[test]
    fn the_fifth_failure_goes_dead_by_itself() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        let now = fail_to(&mut core, MAX_FAILURES - 1, 0);
        assert!(core.tick(now).is_empty());
        let now = fail_to(&mut core, 1, now);
        let actions = core.tick(now);
        let [Action::Dead(dead)] = actions.as_slice() else {
            panic!("expected one dead copy, got {actions:?}");
        };
        assert_eq!(
            (dead.reason, dead.dead_stream.as_str()),
            (Reason::Attempts, "s.dead.g")
        );
        assert_eq!(dead.error.as_deref(), Some("boom"));
        assert!(core.tick(now).is_empty());
        sync(&mut core, actions, now);
        assert!(take(&mut core, "g", "s", 1, 1, now).is_empty());
        assert_eq!(take(&mut core, "x", "s.dead.g", 1, 2, now).len(), 1);
    }

    #[test]
    fn the_fifth_failure_of_a_deleted_message_makes_no_dead_copy() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        let now = fail_to(&mut core, MAX_FAILURES, 0);
        let delete = Record::Tombstone {
            stream: "s".into(),
            up_to: id(1, 0),
        };
        core.apply(delete, now);
        assert_eq!(core.tick(now), []);
    }

    #[test]
    fn an_unsynced_ack_and_a_lease_expiry_as_the_fifth_failure_make_no_dead_copy() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        take(&mut core, "g", "s", 1, 1, 0);
        assert!(core.tick(LEASE).is_empty());
        let now = fail_to(&mut core, MAX_FAILURES - 1, LEASE);
        let handed = take(&mut core, "g", "s", 1, 2, now);
        let pending_ack = core.settle(Receipt(2), &ack(&[handed[0].id]), now).unwrap();
        let expiry = core.tick(now + LEASE);
        assert_eq!(expiry, []);
        sync(&mut core, pending_ack, now + LEASE);
        assert!(core.tick(now + 2 * LEASE).is_empty());
        assert!(take(&mut core, "x", "s.dead.g", 1, 3, now + 2 * LEASE).is_empty());
    }

    #[test]
    fn the_same_expiry_without_the_ack_is_the_fifth_failure() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        take(&mut core, "g", "s", 1, 1, 0);
        assert!(core.tick(LEASE).is_empty());
        let now = fail_to(&mut core, MAX_FAILURES - 1, LEASE);
        take(&mut core, "g", "s", 1, 2, now);
        let expiry = core.tick(now + LEASE);
        assert_eq!(
            failure_counts(&expiry),
            [(addr(1, 0), MAX_FAILURES, now + LEASE + 16_000)]
        );
        sync(&mut core, expiry, now + LEASE);
        assert!(matches!(
            core.tick(now + LEASE).as_slice(),
            [Action::Dead(_)]
        ));
    }

    #[test]
    fn extend_moves_the_lease_of_every_id_still_held() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 2);
        let handed = take(&mut core, "g", "s", 2, 1, 0);
        let settle = Settle {
            ack: vec![handed[0].id],
            extend_ms: Some(60_000),
            ..Settle::default()
        };
        let actions = core.settle(Receipt(1), &settle, 1_000).unwrap();
        sync(&mut core, actions, 1_000);
        assert!(core.tick(LEASE).is_empty());
        assert!(take(&mut core, "g", "s", 2, 2, LEASE).is_empty());
        assert_eq!(
            core.settle(Receipt(1), &ack(&[handed[1].id]), 60_999)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn a_dead_settle_under_pressure_is_refused_and_an_ack_still_works() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        let handed = take(&mut core, "g", "s", 1, 1, 0);
        core.set_pressure(true);
        let bury = Settle {
            dead: vec![Bury {
                id: handed[0].id,
                error: None,
            }],
            ..Settle::default()
        };
        assert_eq!(
            core.settle(Receipt(1), &bury, 1),
            Err(SettleError::Pressure)
        );
        assert_eq!(
            core.settle(Receipt(1), &ack(&[handed[0].id]), 1)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn a_dead_settle_copies_to_the_dead_stream_of_the_group() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        let handed = take(&mut core, "g", "s", 1, 1, 0);
        let bury = Settle {
            dead: vec![Bury {
                id: handed[0].id,
                error: Some("bad".to_owned()),
            }],
            ..Settle::default()
        };
        let actions = core.settle(Receipt(1), &bury, 1).unwrap();
        let [Action::Dead(dead)] = actions.as_slice() else {
            panic!("expected one dead copy, got {actions:?}");
        };
        assert_eq!(
            (
                dead.reason,
                dead.stream.as_str(),
                dead.dead_stream.as_str(),
                dead.error.as_deref()
            ),
            (Reason::Dead, "s", "s.dead.g", Some("bad"))
        );
    }

    #[test]
    fn ack_all_acks_every_id_still_leased() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 3);
        let handed = take(&mut core, "g", "s", 3, 1, 0);
        let nacked = core
            .settle(Receipt(1), &nack(handed[0].id, None), 1)
            .unwrap();
        sync(&mut core, nacked, 1);
        let actions = core.ack_all(Receipt(1), 2).unwrap();
        let acked: Vec<Address> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Write(Record::Ack { address, .. }) => Some(*address),
                _ => None,
            })
            .collect();
        assert_eq!(acked, [handed[1].address, handed[2].address]);
    }

    #[test]
    fn a_release_gives_ids_back_with_no_attempt_counted() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 3);
        let handed = take(&mut core, "g", "s", 3, 1, 0);
        core.release(Receipt(1), &[handed[1].id, handed[2].id]);
        let again = take(&mut core, "g", "s", 3, 2, 1);
        assert_eq!(ids(&again), ids(&handed[1..]));
        assert!(again.iter().all(|h| h.attempt == 1));
        assert_eq!(
            core.settle(Receipt(1), &ack(&[handed[1].id]), 2),
            Err(SettleError::UnknownId(handed[1].id))
        );
    }

    #[test]
    fn a_release_of_every_id_ends_the_lease() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 2);
        let handed = take(&mut core, "g", "s", 2, 1, 0);
        core.release(Receipt(1), &ids(&handed));
        assert_eq!(
            core.settle(Receipt(1), &Settle::default(), 1),
            Err(SettleError::UnknownReceipt)
        );
    }

    fn unwritten(actions: &[Action]) -> Vec<Address> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Write(Record::Ack { address, .. } | Record::Failure { address, .. }) => {
                    Some(*address)
                }
                Action::Dead(dead) => Some(dead.address),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn an_abandoned_ack_goes_back_to_its_lease_and_a_repeat_settle_writes_it_again() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        let handed = take(&mut core, "g", "s", 1, 1, 10);
        let generation = core.generation("g").unwrap();
        let lost = core.settle(Receipt(1), &ack(&[handed[0].id]), 20).unwrap();
        core.abandon("g", generation, &unwritten(&lost), false);
        assert_eq!(
            core.settle(Receipt(1), &ack(&[handed[0].id]), 21).unwrap(),
            lost
        );
    }

    #[test]
    fn a_repeat_settle_of_an_unsynced_ack_names_the_position_to_wait_for() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        let handed = take(&mut core, "g", "s", 1, 1, 10);
        let first = core.settle(Receipt(1), &ack(&[handed[0].id]), 20).unwrap();
        let repeat = core.settle(Receipt(1), &ack(&[handed[0].id]), 21);
        assert_eq!(
            repeat,
            Ok(vec![Action::Await {
                group: "g".to_owned(),
                address: handed[0].address,
            }])
        );
        sync(&mut core, first, 22);
        assert_eq!(
            core.settle(Receipt(1), &ack(&[handed[0].id]), 23),
            Ok(Vec::new())
        );
    }

    #[test]
    fn an_abandoned_ack_of_a_released_id_frees_the_position() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        let handed = take(&mut core, "g", "s", 1, 1, 10);
        let generation = core.generation("g").unwrap();
        let lost = core.settle(Receipt(1), &ack(&[handed[0].id]), 20).unwrap();
        core.release(Receipt(1), &[handed[0].id]);
        core.abandon("g", generation, &unwritten(&lost), false);
        assert_eq!(take(&mut core, "g", "s", 1, 2, 21).len(), 1);
    }

    #[test]
    fn an_abandoned_counted_expiry_gives_the_message_back() {
        let mut core = Core::new(INC);
        publish(&mut core, "s", addr(1, 0), 1, 1);
        take(&mut core, "g", "s", 1, 1, 0);
        core.tick(LEASE);
        take(&mut core, "g", "s", 1, 2, LEASE);
        let generation = core.generation("g").unwrap();
        let lost = core.tick(2 * LEASE);
        assert_eq!(unwritten(&lost), [addr(1, 0)]);
        core.abandon("g", generation, &unwritten(&lost), false);
        assert_eq!(take(&mut core, "g", "s", 1, 3, 2 * LEASE).len(), 1);
    }

    #[test]
    fn an_abandoned_dead_copy_of_an_expiry_is_made_again() {
        let mut core = Core::new(INC);
        let timing = Timing {
            delay_until: None,
            expire_at: Some(5),
        };
        publish_timed(&mut core, "s", addr(1, 0), 1, 1, timing);
        let (_, lost) = core.take(&request("g", "s", 1, 1), 10);
        let generation = core.generation("g").unwrap();
        core.abandon("g", generation, &unwritten(&lost), false);
        assert!(matches!(core.tick(11).as_slice(), [Action::Dead(d)] if d.address == addr(1, 0)));
    }

    #[test]
    fn a_delay_and_a_ttl_are_inputs_of_the_hand_out() {
        let mut core = Core::new(INC);
        let timing = Timing {
            delay_until: Some(5_000),
            expire_at: None,
        };
        publish_timed(&mut core, "s", addr(1, 0), 1, 1, timing);
        assert!(take(&mut core, "g", "s", 1, 1, 4_999).is_empty());
        assert_eq!(take(&mut core, "g", "s", 1, 2, 5_000).len(), 1);
    }
}
