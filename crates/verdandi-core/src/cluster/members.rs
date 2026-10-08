use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};

use super::fence::{FENCE_MS, confirmed};
use super::fnv;
use super::placement::mix;
use super::view::TOMBSTONE_HORIZON;

pub const REPLICATE_MS: u64 = 5_000;
pub const FOUND_MS: u64 = 6_000;
pub const STALE_MS: u64 = 180_000;
pub const MEMBERS_MAX: usize = 150;
pub const FETCHES_MAX: usize = 8;
pub const REMOVED_MAX: usize = 150;

pub type Key = (u32, u64);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Members {
    pub genesis: u64,
    pub era: u32,
    pub epoch: u64,
    pub members: Vec<u64>,
    pub forced: bool,
    pub credits: u16,
    pub removed: Vec<(u64, u64)>,
    pub forced_from: Key,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Ballot {
    pub round: u32,
    pub disk: u64,
}

pub type Accepted = Option<(Ballot, Members)>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    Promise(Key, Ballot, Accepted),
    Accepted(Key, Ballot),
    Refused(Key, Ballot),
    Other(Key),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    Wait,
    Accept(Members),
    Commit(Members),
    CatchUp(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Change {
    SelfRemoval { seen_ms: u64, disk: u64 },
    FinishedDrain(u64),
    AnnouncedStop(u64),
    AcceptLoss { disk: u64, restart: u64 },
    Join { disk: u64, first_run: bool },
    Replacement(u64),
    Horizon(u64),
    Drop { disk: u64, restart: u64 },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Trips {
    matched: BTreeMap<u64, u64>,
    any: BTreeMap<u64, u64>,
    acts_until: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Durable {
    disk: u64,
    list: Members,
    round: u32,
    promised: Ballot,
    accepted: Accepted,
    lost: BTreeMap<u64, (u64, u64)>,
    first_seen: BTreeMap<(u64, u64), u64>,
    seen: Option<(Key, u64)>,
    conflict: Option<Key>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Node {
    disk: u64,
    state: Durable,
    trips: Trips,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Proposal {
    pub base: Members,
    pub ballot: Ballot,
    pub lost: BTreeMap<u64, u64>,
    pub seen: u32,
    own: Members,
    promises: BTreeMap<u64, Accepted>,
    value: Option<Members>,
    accepts: BTreeSet<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fresh {
    pub disk: u64,
    pub can_found: bool,
    pub live: bool,
    pub trip_ms: Option<u64>,
    pub seen_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotRead {
    pub at_ms: u64,
    pub version: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Fetches(BTreeMap<Key, u64>);

impl Members {
    pub fn key(&self) -> Key {
        (self.era, self.epoch)
    }

    pub fn has(&self, disk: u64) -> bool {
        self.members.binary_search(&disk).is_ok()
    }

    pub fn refuses(&self, disk: u64) -> bool {
        self.removed.iter().any(|(d, _)| *d == disk)
    }

    pub fn hash(&self) -> u64 {
        fnv([
            self.genesis,
            u64::from(self.era),
            self.epoch,
            u64::from(self.forced),
            u64::from(self.credits),
        ]
        .into_iter()
        .chain(self.members.iter().copied())
        .chain([u64::MAX])
        .chain(self.removed.iter().flat_map(|(d, r)| [*d, *r]))
        .chain([u64::from(self.forced_from.0), self.forced_from.1]))
    }

    fn open(&self) -> bool {
        self.era.checked_add(1).is_some() && self.epoch.checked_add(2).is_some()
    }

    fn valid(&self) -> bool {
        !self.members.is_empty()
            && self.members.len() <= MEMBERS_MAX
            && self.removed.len() <= REMOVED_MAX
            && self.members.windows(2).all(|w| w[0] < w[1])
            && self.removed.windows(2).all(|w| w[0] < w[1])
    }

    fn follows(&self, base: &Members) -> bool {
        let (next, from) = if self.forced {
            (
                base.era.checked_add(1).zip(base.epoch.checked_add(2)),
                base.key(),
            )
        } else {
            (
                Some(base.era).zip(base.epoch.checked_add(1)),
                base.forced_from,
            )
        };
        self.genesis == base.genesis
            && self.valid()
            && self.open()
            && next == Some(self.key())
            && self.forced_from == from
    }

    fn without(&self, lost: &BTreeMap<u64, u64>, base: Key) -> Option<Members> {
        let mut removed = self.removed.clone();
        removed.extend(
            self.members
                .iter()
                .filter_map(|d| lost.get(d).map(|r| (*d, *r))),
        );
        removed.sort_unstable();
        removed.dedup();
        Some(Members {
            genesis: self.genesis,
            era: base.0.checked_add(1)?,
            epoch: base.1.checked_add(2)?,
            members: self
                .members
                .iter()
                .copied()
                .filter(|d| !lost.contains_key(d))
                .collect(),
            forced: true,
            credits: self.credits,
            removed,
            forced_from: base,
        })
    }
}

impl Change {
    pub fn apply(self, list: &Members) -> Option<Members> {
        let mut next = Members {
            epoch: list.epoch.checked_add(1)?,
            forced: false,
            ..list.clone()
        };
        match self {
            Change::Join { disk, first_run } => {
                let at = next.members.binary_search(&disk).err()?;
                next.members.insert(at, disk);
                next.credits = next.credits.saturating_add(u16::from(first_run));
            }
            Change::Drop { disk, restart } => {
                let at = next.removed.binary_search(&(disk, restart)).ok()?;
                next.removed.remove(at);
            }
            Change::AcceptLoss { disk, restart } => {
                next.members.remove(list.members.binary_search(&disk).ok()?);
                let at = next.removed.binary_search(&(disk, restart)).err()?;
                next.removed.insert(at, (disk, restart));
            }
            Change::Horizon(disk) => {
                next.members.remove(list.members.binary_search(&disk).ok()?);
            }
            Change::SelfRemoval { disk, .. }
            | Change::FinishedDrain(disk)
            | Change::AnnouncedStop(disk)
            | Change::Replacement(disk) => {
                next.members.remove(list.members.binary_search(&disk).ok()?);
                next.credits = next.credits.saturating_sub(1);
            }
        }
        (!next.members.is_empty()).then_some(next)
    }
}

fn acts_until(list: &Members, me: u64, matched: &BTreeMap<u64, u64>) -> u64 {
    if !list.has(me) {
        return 0;
    }
    let mut sent: Vec<u64> = matched.values().copied().collect();
    sent.sort_unstable_by_key(|t| Reverse(*t));
    match list.members.len() / 2 {
        0 => u64::MAX,
        need => sent.get(need - 1).map_or(0, |t| t.saturating_add(FENCE_MS)),
    }
}

impl Durable {
    pub fn list(&self) -> &Members {
        &self.list
    }

    pub fn round(&self) -> u32 {
        self.round
    }

    pub fn promised(&self) -> Ballot {
        self.promised
    }

    pub fn accepted(&self) -> &Accepted {
        &self.accepted
    }

    pub fn lost(&self) -> &BTreeMap<u64, (u64, u64)> {
        &self.lost
    }

    pub fn first_seen(&self) -> &BTreeMap<(u64, u64), u64> {
        &self.first_seen
    }

    pub fn disk(&self) -> u64 {
        self.disk
    }

    pub fn seen(&self) -> Option<(Key, u64)> {
        self.seen
    }

    pub fn conflict(&self) -> Option<Key> {
        self.conflict
    }
}

impl Node {
    pub fn new(disk: u64) -> Node {
        Node::restore(
            disk,
            Durable {
                disk,
                ..Durable::default()
            },
        )
        .unwrap_or_default()
    }

    pub fn restore(disk: u64, mut state: Durable) -> Option<Node> {
        if state.disk != disk {
            return None;
        }
        if let Some((ballot, _)) = &state.accepted {
            state.promised = state.promised.max(*ballot);
        }
        state.round = state.round.max(state.promised.round);
        let mut node = Node {
            disk,
            state,
            trips: Trips::default(),
        };
        node.restarted();
        Some(node)
    }

    pub fn disk(&self) -> u64 {
        self.disk
    }

    pub fn durable(&self) -> &Durable {
        &self.state
    }

    pub fn list(&self) -> &Members {
        &self.state.list
    }

    pub fn pending(&self) -> bool {
        self.state.accepted.is_some()
    }

    pub fn restarted(&mut self) {
        self.trips = Trips::default();
        self.trips.acts_until = acts_until(&self.state.list, self.disk, &self.trips.matched);
    }

    pub fn ballot(&mut self, seen: u32) -> Option<Ballot> {
        if self.state.conflict.is_some() {
            return None;
        }
        let ballot = Ballot {
            round: seen.max(self.state.round).checked_add(1)?,
            disk: self.disk,
        };
        self.state.round = ballot.round;
        self.state.promised = self.state.promised.max(ballot);
        Some(ballot)
    }

    pub fn passes(&self, entry: &(u64, u64), run_ms: u64) -> bool {
        self.state
            .first_seen
            .get(entry)
            .is_some_and(|t| run_ms >= t.saturating_add(TOMBSTONE_HORIZON))
    }

    pub fn change(&self, change: Change, run_ms: u64) -> Option<Members> {
        if let Change::Drop { disk, restart } = change
            && !self.passes(&(disk, restart), run_ms)
        {
            return None;
        }
        change.apply(&self.state.list)
    }

    pub fn drop_due(&self, run_ms: u64) -> Option<Change> {
        self.state
            .list
            .removed
            .iter()
            .find(|e| self.passes(e, run_ms))
            .map(|(disk, restart)| Change::Drop {
                disk: *disk,
                restart: *restart,
            })
    }

    fn newer_seen(&self, run_ms: u64) -> bool {
        self.state.seen.is_some_and(|(key, at)| {
            key > self.state.list.key() && run_ms < at.saturating_add(TOMBSTONE_HORIZON)
        })
    }

    pub fn saw(&mut self, genesis: u64, key: Key, run_ms: u64) {
        let list = &self.state.list;
        if genesis == list.genesis
            && genesis != 0
            && key > list.key()
            && self.state.seen.is_none_or(|(top, _)| key >= top)
        {
            self.state.seen = Some((key, run_ms));
        }
    }

    pub fn forced(
        &self,
        reached: &BTreeSet<u64>,
        lost: &BTreeMap<u64, u64>,
        run_ms: u64,
    ) -> Option<u64> {
        if self.newer_seen(run_ms) || self.state.conflict.is_some() {
            return None;
        }
        forced(&self.state.list, reached, lost)
    }

    pub fn prepare(&mut self, base: Key, ballot: Ballot) -> Reply {
        if base != self.state.list.key() {
            return Reply::Other(self.state.list.key());
        }
        if self.state.promised > ballot || self.state.conflict.is_some() {
            return Reply::Refused(base, self.state.promised);
        }
        self.state.promised = ballot;
        self.state.round = self.state.round.max(ballot.round);
        Reply::Promise(base, ballot, self.state.accepted.clone())
    }

    pub fn accept(
        &mut self,
        base: Key,
        ballot: Ballot,
        value: &Members,
        live: impl Fn(u64) -> bool,
        run_ms: u64,
    ) -> Reply {
        let list = &self.state.list;
        if base != list.key() {
            return Reply::Other(list.key());
        }
        let drops_live = value.forced && list.members.iter().any(|d| !value.has(*d) && live(*d));
        let joins_refused = value
            .members
            .iter()
            .any(|d| !list.has(*d) && self.join_refused(*d));
        let drops_early = list
            .removed
            .iter()
            .any(|e| value.removed.binary_search(e).is_err() && !self.passes(e, run_ms));
        let stale_forced = value.forced && self.newer_seen(run_ms);
        let over_forced = !value.forced
            && self
                .state
                .accepted
                .as_ref()
                .is_some_and(|(_, held)| held.forced);
        if self.state.promised > ballot
            || self.state.conflict.is_some()
            || !value.follows(list)
            || drops_live
            || joins_refused
            || drops_early
            || over_forced
            || stale_forced
        {
            return Reply::Refused(base, self.state.promised);
        }
        self.state.promised = ballot;
        self.state.round = self.state.round.max(ballot.round);
        self.state.accepted = Some((ballot, value.clone()));
        Reply::Accepted(base, ballot)
    }

    pub fn adopt(&mut self, list: &Members, run_ms: u64) -> Option<bool> {
        let old = &self.state.list;
        let same = old.genesis == list.genesis || old.genesis == 0;
        if !same || !list.valid() || !list.open() || list.key() <= old.key() {
            return None;
        }
        if list.refuses(self.disk) {
            self.state.conflict.get_or_insert(list.key());
            return None;
        }
        if old.genesis != 0 {
            let known = [Some(old.key()), self.state.seen.map(|(key, _)| key)]
                .into_iter()
                .flatten()
                .filter(|key| key.0.checked_add(1) == Some(list.era))
                .max();
            let fork = if list.era == old.era {
                list.forced_from != old.forced_from
            } else {
                list.era > old.era + 1 || known.is_some_and(|key| list.forced_from < key)
            };
            if fork {
                self.state.conflict.get_or_insert(list.key());
                return None;
            }
        }
        let next = list.era == old.era && list.epoch == old.epoch + 1;
        let reconcile = !next || (!old.has(self.disk) && list.has(self.disk));
        let seen = std::mem::take(&mut self.state.first_seen);
        self.state.first_seen = list
            .removed
            .iter()
            .map(|e| (*e, seen.get(e).copied().filter(|_| next).unwrap_or(run_ms)))
            .collect();
        self.state.list = list.clone();
        self.state.lost.retain(|d, _| list.has(*d));
        self.state.promised = Ballot::default();
        self.state.accepted = None;
        self.trips.matched.clear();
        self.trips.any.retain(|d, _| list.has(*d));
        self.trips.acts_until = acts_until(list, self.disk, &self.trips.matched);
        Some(reconcile)
    }

    pub fn pong(
        &mut self,
        from: u64,
        genesis: u64,
        key: Key,
        hash: u64,
        sent_ms: u64,
        now_ms: u64,
    ) {
        let list = &self.state.list;
        if genesis != list.genesis || from == self.disk || !list.has(from) {
            return;
        }
        let sent_ms = sent_ms.min(now_ms);
        let any = self.trips.any.entry(from).or_insert(0);
        *any = (*any).max(sent_ms);
        if key == list.key() && hash == list.hash() {
            let matched = self.trips.matched.entry(from).or_insert(0);
            *matched = (*matched).max(sent_ms);
            self.trips.acts_until = acts_until(list, self.disk, &self.trips.matched);
        }
    }

    pub fn acts(&self, now_ms: u64) -> bool {
        self.state.conflict.is_none() && now_ms < self.trips.acts_until
    }

    pub fn keeps_reconciled(&self, now_ms: u64) -> bool {
        let list = &self.state.list;
        let fresh = self
            .trips
            .any
            .iter()
            .filter(|(d, t)| list.has(**d) && now_ms < t.saturating_add(FENCE_MS))
            .count();
        confirmed(fresh + 1, list.members.len())
    }

    pub fn join_refused(&self, disk: u64) -> bool {
        self.state.list.refuses(disk)
    }

    pub fn counts_join(&self, disk: u64, trips: &BTreeSet<u64>) -> bool {
        let list = &self.state.list;
        let named = trips.iter().filter(|d| list.has(**d)).count();
        !list.has(disk)
            && !self.join_refused(disk)
            && list.members.len() < MEMBERS_MAX
            && confirmed(named, list.members.len())
    }

    pub fn lose(&mut self, disk: u64, restart: u64, run_ms: u64) {
        if !self.state.list.has(disk) {
            return;
        }
        let entry = self.state.lost.entry(disk).or_insert((restart, run_ms));
        if restart > entry.0 {
            *entry = (restart, run_ms);
        }
    }

    pub fn lost_members(&self, restart_of: impl Fn(u64) -> Option<u64>) -> BTreeMap<u64, u64> {
        self.state
            .lost
            .iter()
            .filter(|(d, (r, _))| restart_of(**d) == Some(*r))
            .map(|(d, (r, _))| (*d, *r))
            .collect()
    }

    pub fn expire(&mut self, run_ms: u64) {
        self.state
            .lost
            .retain(|_, (_, at)| run_ms < at.saturating_add(TOMBSTONE_HORIZON));
    }
}

impl Proposal {
    pub fn new(base: &Members, ballot: Ballot, own: Members) -> Proposal {
        Proposal {
            base: base.clone(),
            ballot,
            lost: BTreeMap::new(),
            seen: 0,
            own,
            promises: BTreeMap::new(),
            value: None,
            accepts: BTreeSet::new(),
        }
    }

    pub fn forced(base: &Members, ballot: Ballot, lost: BTreeMap<u64, u64>) -> Proposal {
        Proposal {
            lost,
            ..Proposal::new(base, ballot, base.clone())
        }
    }

    pub fn acceptors(&self) -> Vec<u64> {
        self.base
            .members
            .iter()
            .copied()
            .filter(|d| !self.lost.contains_key(d))
            .collect()
    }

    pub fn value(&self) -> Option<&Members> {
        self.value.as_ref()
    }

    pub fn unanswered(&self) -> Vec<u64> {
        self.acceptors()
            .into_iter()
            .filter(|d| match self.value {
                None => !self.promises.contains_key(d),
                Some(_) => !self.accepts.contains(d),
            })
            .collect()
    }

    fn quorum(&self, answers: usize) -> bool {
        if self.lost.is_empty() {
            confirmed(answers, self.base.members.len())
        } else {
            answers == self.acceptors().len()
        }
    }

    fn choose(&self) -> Option<Members> {
        let top = self
            .promises
            .values()
            .flatten()
            .max_by_key(|(ballot, _)| *ballot)
            .map(|(_, value)| value);
        if self.lost.is_empty() {
            return Some(top.unwrap_or(&self.own).clone());
        }
        match top {
            Some(value) if value.forced => Some(value.clone()),
            _ => top
                .unwrap_or(&self.base)
                .without(&self.lost, self.base.key()),
        }
    }

    pub fn reply(&mut self, from: u64, reply: Reply) -> Step {
        let acceptor = self.base.has(from) && !self.lost.contains_key(&from);
        let slot = self.base.key();
        match reply {
            Reply::Promise(key, ballot, accepted)
                if key == slot && ballot == self.ballot && acceptor && self.value.is_none() =>
            {
                self.promises.insert(from, accepted);
                if self.quorum(self.promises.len())
                    && let Some(value) = self.choose()
                {
                    self.value = Some(value.clone());
                    return Step::Accept(value);
                }
            }
            Reply::Accepted(key, ballot) if key == slot && ballot == self.ballot && acceptor => {
                if let Some(value) = &self.value {
                    self.accepts.insert(from);
                    if self.quorum(self.accepts.len()) {
                        return Step::Commit(value.clone());
                    }
                }
            }
            Reply::Refused(key, promised) if key == slot && acceptor => {
                self.seen = self.seen.max(promised.round)
            }
            Reply::Other(key) if key > slot => return Step::CatchUp(from),
            _ => {}
        }
        Step::Wait
    }
}

pub fn pick(
    list: &Members,
    live: &BTreeSet<u64>,
    signalled: &BTreeSet<u64>,
    waited_ms: u64,
) -> Option<u64> {
    let live: Vec<u64> = list
        .members
        .iter()
        .copied()
        .filter(|d| live.contains(d))
        .collect();
    let quiet: Vec<u64> = live
        .iter()
        .copied()
        .filter(|d| !signalled.contains(d))
        .collect();
    let mut pool = if quiet.is_empty() { live } else { quiet };
    pool.sort_by_key(|d| {
        Reverse((
            mix(fnv([list.genesis, u64::from(list.era), list.epoch, *d])),
            *d,
        ))
    });
    let rank = (waited_ms / (3 * REPLICATE_MS)) as usize;
    pool.get(rank % pool.len().max(1)).copied()
}

pub fn drop_agreed(list: &Members, yes: &BTreeSet<u64>) -> bool {
    confirmed(
        yes.iter().filter(|d| list.has(**d)).count(),
        list.members.len(),
    )
}

pub fn proposes(me: u64, pick: Option<u64>, pending: bool, quiet_ms: u64) -> bool {
    pick == Some(me) || (pending && quiet_ms >= 3 * REPLICATE_MS)
}

pub fn replacement(
    list: &Members,
    absent: &BTreeMap<u64, u64>,
    votes: &BTreeMap<u64, BTreeSet<u64>>,
) -> Option<u64> {
    if list.credits == 0 {
        return None;
    }
    let target = absent
        .iter()
        .filter(|(d, _)| list.has(**d))
        .min_by_key(|(d, since)| (**since, **d))
        .map(|(d, _)| *d)?;
    let yes = votes.get(&target)?.iter().filter(|d| list.has(**d)).count();
    confirmed(yes, list.members.len()).then_some(target)
}

fn forced(list: &Members, reached: &BTreeSet<u64>, lost: &BTreeMap<u64, u64>) -> Option<u64> {
    let losses = list.members.iter().filter(|d| lost.contains_key(d)).count();
    let ready = 2 * losses >= list.members.len()
        && list
            .members
            .iter()
            .all(|d| reached.contains(d) != lost.contains_key(d));
    if !ready {
        return None;
    }
    list.members.iter().copied().find(|d| reached.contains(d))
}

pub fn found(
    me: u64,
    can_found: bool,
    holder_reached: bool,
    slot_blocked: bool,
    peers: &[Fresh],
    now_ms: u64,
) -> Option<Members> {
    let fresh: Vec<&Fresh> = peers.iter().filter(|p| p.can_found).collect();
    let lowest = fresh.iter().filter(|p| p.live).all(|p| me < p.disk);
    let settled = fresh
        .iter()
        .all(|p| now_ms >= p.seen_ms.saturating_add(FOUND_MS));
    let mut members: Vec<u64> = fresh
        .iter()
        .filter(|p| {
            p.trip_ms
                .is_some_and(|t| now_ms < t.saturating_add(FOUND_MS))
        })
        .map(|p| p.disk)
        .collect();
    if !can_found || holder_reached || slot_blocked || members.is_empty() || !lowest || !settled {
        return None;
    }
    members.push(me);
    members.sort_unstable();
    members.dedup();
    Some(Members {
        genesis: me,
        epoch: 1,
        members,
        ..Members::default()
    })
}

pub fn stale(first: SlotRead, second: SlotRead) -> bool {
    first.version == second.version && second.at_ms >= first.at_ms.saturating_add(STALE_MS)
}

pub fn founding_blocked(first: SlotRead, second: SlotRead) -> bool {
    second.version.is_some() && !stale(first, second)
}

impl Fetches {
    pub fn ask(&mut self, key: Key, now_ms: u64) -> bool {
        self.0
            .retain(|_, at| now_ms < at.saturating_add(REPLICATE_MS));
        if self.0.contains_key(&key) || self.0.len() >= FETCHES_MAX {
            return false;
        }
        self.0.insert(key, now_ms);
        true
    }

    pub fn adopted(&mut self, key: Key) {
        self.0.retain(|k, _| *k > key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::placement;
    use crate::cluster::testkit::Rng;

    fn list(members: &[u64]) -> Members {
        Members {
            genesis: 1,
            epoch: 1,
            members: members.to_vec(),
            ..Members::default()
        }
    }

    fn at(era: u32, epoch: u64, members: &[u64]) -> Members {
        Members {
            era,
            epoch,
            ..list(members)
        }
    }

    fn b(round: u32, disk: u64) -> Ballot {
        Ballot { round, disk }
    }

    fn set(disks: &[u64]) -> BTreeSet<u64> {
        disks.iter().copied().collect()
    }

    fn lost(disks: &[u64]) -> BTreeMap<u64, u64> {
        disks.iter().map(|d| (*d, 1)).collect()
    }

    fn loss(disk: u64) -> Change {
        Change::AcceptLoss { disk, restart: 1 }
    }

    fn join(disk: u64, first_run: bool) -> Change {
        Change::Join { disk, first_run }
    }

    fn held(disk: u64, list: Members) -> Node {
        let mut node = Node::new(disk);
        node.adopt(&list, 0);
        node
    }

    fn apply(change: Change, base: &Members) -> Members {
        change.apply(base).unwrap()
    }

    fn promise(base: &Members, ballot: Ballot, accepted: Accepted) -> Reply {
        Reply::Promise(base.key(), ballot, accepted)
    }

    fn commits(size: u64, answers: u64) -> bool {
        let base = list(&(1..=size).collect::<Vec<_>>());
        let own = apply(join(99, false), &base);
        let mut proposal = Proposal::new(&base, b(1, 1), own);
        let mut step = Step::Wait;
        for from in 1..=answers {
            step = proposal.reply(from, promise(&base, b(1, 1), None));
        }
        if !matches!(step, Step::Accept(_)) {
            return false;
        }
        for from in 1..=answers {
            step = proposal.reply(from, Reply::Accepted(base.key(), b(1, 1)));
        }
        matches!(step, Step::Commit(_))
    }

    #[test]
    fn a_commit_needs_more_than_half() {
        assert!(!commits(4, 2));
        assert!(commits(4, 3));
        assert!(commits(1, 1));
        assert!(!commits(2, 1));
        assert!(commits(2, 2));
    }

    #[test]
    fn a_proposer_takes_the_highest_accepted_value() {
        let base = list(&[1, 2, 3]);
        let own = apply(loss(3), &base);
        let low = apply(join(7, false), &base);
        let high = apply(join(8, false), &base);
        let mut proposal = Proposal::new(&base, b(5, 1), own.clone());
        proposal.reply(2, promise(&base, b(5, 1), Some((b(3, 2), low.clone()))));
        assert_eq!(
            proposal.reply(3, promise(&base, b(5, 1), Some((b(4, 3), high.clone())))),
            Step::Accept(high)
        );
        let mut proposal = Proposal::new(&base, b(5, 1), own.clone());
        proposal.reply(1, promise(&base, b(5, 1), None));
        assert_eq!(
            proposal.reply(2, promise(&base, b(5, 1), Some((b(3, 2), low.clone())))),
            Step::Accept(low)
        );
        let mut proposal = Proposal::new(&base, b(5, 1), own.clone());
        proposal.reply(1, promise(&base, b(5, 1), None));
        assert_eq!(
            proposal.reply(2, promise(&base, b(5, 1), None)),
            Step::Accept(own)
        );
    }

    #[test]
    fn an_acceptor_answers_only_at_its_epoch() {
        let base = list(&[1, 2, 3]);
        let key = base.key();
        let value = apply(loss(3), &base);
        let mut node = held(2, base.clone());
        assert_eq!(node.prepare((0, 2), b(2, 5)), Reply::Other((0, 1)));
        assert_eq!(node.prepare((0, 0), b(2, 5)), Reply::Other((0, 1)));
        assert_eq!(
            node.accept((0, 2), b(2, 5), &value, |_| false, 0),
            Reply::Other((0, 1))
        );
        assert_eq!(
            node.prepare(key, b(2, 5)),
            Reply::Promise(key, b(2, 5), None)
        );
        assert_eq!(node.prepare(key, b(1, 6)), Reply::Refused(key, b(2, 5)));
        assert_eq!(
            node.accept(key, b(3, 7), &value, |_| false, 0),
            Reply::Accepted(key, b(3, 7))
        );
        assert_eq!(node.state.promised, b(3, 7));
        assert_eq!(node.prepare(key, b(3, 6)), Reply::Refused(key, b(3, 7)));
        assert_eq!(
            node.accept(key, b(2, 5), &value, |_| false, 0),
            Reply::Refused(key, b(3, 7))
        );
        assert_eq!(
            node.prepare(key, b(4, 1)),
            Reply::Promise(key, b(4, 1), Some((b(3, 7), value)))
        );
    }

    #[test]
    fn a_proposer_after_a_long_cut_needs_one_retry() {
        let base = list(&[1, 2, 3]);
        let mut nodes: Vec<Node> = [1, 2, 3].map(|d| held(d, base.clone())).to_vec();
        nodes[0].state.round = 2;
        for node in &mut nodes[1..] {
            node.prepare(base.key(), b(500, 9));
        }
        let own = apply(loss(3), &base);
        let mut seen = 0;
        for attempt in 1..=2 {
            let ballot = nodes[0].ballot(seen).unwrap();
            assert_eq!(ballot.round, if attempt == 1 { 3 } else { 501 });
            let mut proposal = Proposal::new(&base, ballot, own.clone());
            proposal.reply(9, Reply::Refused(base.key(), b(900, 9)));
            proposal.reply(2, Reply::Refused((0, 7), b(800, 9)));
            assert_eq!(proposal.seen, 0);
            let mut step = Step::Wait;
            for node in &mut nodes {
                let reply = node.prepare(base.key(), ballot);
                if let Step::Accept(value) = proposal.reply(node.disk, reply) {
                    step = Step::Accept(value);
                }
            }
            if let Step::Accept(value) = step.clone() {
                for node in &mut nodes {
                    let reply = node.accept(base.key(), ballot, &value, |_| false, 0);
                    step = proposal.reply(node.disk, reply);
                }
            }
            if attempt == 1 {
                assert_eq!(step, Step::Wait);
                assert_eq!(proposal.seen, 500);
                seen = proposal.seen;
            } else {
                assert_eq!(step, Step::Commit(own.clone()));
            }
        }
    }

    #[test]
    fn a_restarted_proposer_never_reuses_a_ballot() {
        let base = list(&[1, 2, 3]);
        let key = base.key();
        let mut node = held(1, base.clone());
        let first = node.ballot(0).unwrap();
        assert_eq!(node.durable().promised(), first);
        assert_eq!(node.durable().round(), first.round);
        let mut restarted = Node::restore(1, node.durable().clone()).unwrap();
        let second = restarted.ballot(0).unwrap();
        assert!(second.round > first.round);
        assert!(matches!(restarted.prepare(key, second), Reply::Promise(..)));
        assert_eq!(restarted.prepare(key, first), Reply::Refused(key, second));
        let mut acceptor = held(2, base.clone());
        let one = apply(loss(3), &base);
        let two = apply(loss(2), &base);
        acceptor.prepare(key, first);
        acceptor.accept(key, first, &one, |_| false, 0);
        assert!(matches!(
            acceptor.prepare(key, second),
            Reply::Promise(_, _, Some((ballot, _))) if ballot == first
        ));
        assert_eq!(
            acceptor.accept(key, second, &two, |_| false, 0),
            Reply::Accepted(key, second)
        );
        assert_eq!(
            acceptor.accept(key, first, &one, |_| false, 0),
            Reply::Refused(key, second)
        );
    }

    #[test]
    fn a_late_reply_from_an_earlier_slot_never_completes_a_proposal() {
        let l1 = list(&[1, 2, 3, 4]);
        let mut n: BTreeMap<u64, Node> = (1..=4).map(|d| (d, held(d, l1.clone()))).collect();
        let b1 = n.get_mut(&1).unwrap().ballot(0).unwrap();
        let v2 = apply(loss(4), &l1);
        let mut first = Proposal::new(&l1, b1, v2.clone());
        let mut step = Step::Wait;
        for d in [1, 2, 4] {
            let reply = n.get_mut(&d).unwrap().prepare(l1.key(), b1);
            step = first.reply(d, reply);
        }
        assert_eq!(step, Step::Accept(v2.clone()));
        let late_promise = n.get_mut(&3).unwrap().prepare(l1.key(), b1);
        for d in [1, 2, 4] {
            let reply = n
                .get_mut(&d)
                .unwrap()
                .accept(l1.key(), b1, &v2, |_| true, 0);
            step = first.reply(d, reply);
        }
        assert_eq!(step, Step::Commit(v2.clone()));
        let late_accepted = n
            .get_mut(&3)
            .unwrap()
            .accept(l1.key(), b1, &v2, |_| true, 0);
        for d in [1, 2, 3] {
            n.get_mut(&d).unwrap().adopt(&v2, 0);
        }
        let b2 = n.get_mut(&1).unwrap().ballot(0).unwrap();
        assert!(b2 > b1);
        let v3 = apply(join(7, true), &v2);
        let mut second = Proposal::new(&v2, b2, v3.clone());
        let reply = n.get_mut(&1).unwrap().prepare(v2.key(), b2);
        second.reply(1, reply);
        let late = |reply: &Reply| match reply.clone() {
            Reply::Promise(_, _, accepted) => Reply::Promise(l1.key(), b2, accepted),
            Reply::Accepted(..) => Reply::Accepted(l1.key(), b2),
            other => other,
        };
        assert_eq!(second.reply(3, late(&late_promise)), Step::Wait);
        assert_eq!(second.reply(3, late_promise), Step::Wait);
        let reply = n.get_mut(&2).unwrap().prepare(v2.key(), b2);
        assert_eq!(second.reply(2, reply), Step::Accept(v3.clone()));
        let reply = n
            .get_mut(&1)
            .unwrap()
            .accept(v2.key(), b2, &v3, |_| true, 0);
        second.reply(1, reply);
        assert_eq!(second.reply(3, late(&late_accepted)), Step::Wait);
        assert_eq!(second.reply(3, late_accepted), Step::Wait);
        let mut solo = held(9, l1.clone());
        let c1 = solo.ballot(0).unwrap();
        solo.prepare(l1.key(), c1);
        solo.adopt(&v2, 0);
        assert!(solo.ballot(0).unwrap() > c1);
        let q = n.get_mut(&2).unwrap().ballot(0).unwrap();
        let w = apply(join(8, false), &v2);
        let mut third = Proposal::new(&v2, q, w.clone());
        for d in [2, 3] {
            let reply = n.get_mut(&d).unwrap().prepare(v2.key(), q);
            step = third.reply(d, reply);
        }
        assert_eq!(step, Step::Accept(w.clone()));
    }

    #[test]
    fn a_round_or_an_epoch_at_its_limit_is_refused() {
        let base = list(&[1, 2, 3]);
        let mut node = held(1, base.clone());
        let mut proposal = Proposal::new(&base, node.ballot(0).unwrap(), base.clone());
        proposal.reply(2, Reply::Refused(base.key(), b(u32::MAX, 2)));
        assert_eq!(node.ballot(proposal.seen), None);
        assert_eq!(node.ballot(u32::MAX - 1), Some(b(u32::MAX, 1)));
        let top = Members {
            epoch: u64::MAX,
            ..base.clone()
        };
        assert_eq!(node.adopt(&top, 0), None);
        let near = Members {
            epoch: u64::MAX - 1,
            ..base.clone()
        };
        assert_eq!(node.adopt(&near, 0), None);
        let last = Members {
            epoch: u64::MAX - 2,
            ..base.clone()
        };
        assert!(node.adopt(&last, 0).is_some());
        assert!(Change::Horizon(3).apply(&last).is_some());
        assert_eq!(Change::Horizon(3).apply(&top), None);
        let era = Members {
            era: u32::MAX,
            ..at(0, 9, &[1, 2, 3])
        };
        assert_eq!(held(1, base.clone()).adopt(&era, 0), None);
        let mut forced = Proposal::forced(&era, b(1, 1), lost(&[2, 3]));
        assert_eq!(
            forced.reply(1, Reply::Promise(era.key(), b(1, 1), None)),
            Step::Wait
        );
    }

    #[test]
    fn a_node_acts_only_with_more_than_half_at_its_epoch_and_hash() {
        let base = list(&[1, 2, 3, 4, 5]);
        let (key, hash) = (base.key(), base.hash());
        let mut node = held(1, base.clone());
        node.pong(2, 1, key, hash, 100, 100);
        assert!(!node.acts(100));
        node.pong(3, 7, key, hash, 200, 200);
        node.pong(3, 1, (1, 1), hash, 200, 200);
        node.pong(3, 1, (0, 2), hash, 200, 200);
        node.pong(3, 1, key, hash ^ 1, 200, 200);
        node.pong(9, 1, key, hash, 200, 200);
        assert!(!node.acts(200));
        node.pong(3, 1, key, hash, 200, 200);
        assert!(node.acts(100 + FENCE_MS - 1));
        assert!(!node.acts(100 + FENCE_MS));
        node.pong(4, 1, key, hash, 50, 200);
        assert!(!node.acts(100 + FENCE_MS));
        let mut future = held(1, base.clone());
        future.pong(2, 1, key, hash, 90_000, 300);
        future.pong(3, 1, key, hash, 90_000, 300);
        assert!(!future.acts(300 + FENCE_MS));
        let mut outsider = held(9, base.clone());
        for d in [2, 3, 4, 5] {
            outsider.pong(d, 1, key, hash, 200, 200);
        }
        assert!(!outsider.acts(200));
        let mut rng = Rng(77);
        let mut node = held(1, base.clone());
        let mut pongs: Vec<(u64, u64)> = Vec::new();
        let mut now = 0;
        for _ in 0..1_000 {
            now += rng.below(700);
            for _ in 0..rng.below(3) {
                let from = 2 + rng.below(5);
                let sent = now - rng.below(now.min(2_500) + 1);
                if rng.below(4) == 0 {
                    node.pong(from, 1, key, hash ^ 1, sent, now);
                } else {
                    node.pong(from, 1, key, hash, sent, now);
                    if base.has(from) {
                        pongs.push((from, sent));
                    }
                }
            }
            let counted: BTreeSet<u64> = pongs
                .iter()
                .filter(|(_, sent)| *sent + FENCE_MS > now)
                .map(|(d, _)| *d)
                .collect();
            assert_eq!(node.acts(now), 2 * (1 + counted.len()) > 5, "at {now}");
        }
        let next = apply(loss(5), &base);
        node.pong(2, 1, key, hash, now, now);
        node.pong(3, 1, key, hash, now, now);
        assert!(node.acts(now));
        assert_eq!(node.adopt(&next, 0), Some(false));
        assert_eq!(node.trips.acts_until, 0);
        assert!(!node.acts(now));
        let mut alone = held(1, list(&[1]));
        assert!(alone.acts(u64::MAX - 1));
        alone.restarted();
        assert!(alone.acts(u64::MAX - 1));
    }

    #[test]
    fn reconciled_clears_when_round_trips_age_out_and_stays_for_an_epoch_gap() {
        let base = list(&[1, 2, 3]);
        let mut node = held(1, base.clone());
        node.pong(2, 1, base.key(), base.hash(), 100, 100);
        assert!(node.keeps_reconciled(100 + FENCE_MS - 1));
        assert!(!node.keeps_reconciled(100 + FENCE_MS));
        let next = apply(loss(3), &base);
        node.pong(2, 1, next.key(), next.hash(), 2_000, 2_000);
        node.pong(3, 1, next.key(), next.hash(), 2_000, 2_000);
        assert!(!node.acts(2_100));
        assert!(node.keeps_reconciled(2_100));
        assert_eq!(node.adopt(&next, 0), Some(false));
        assert!(node.keeps_reconciled(2_100));
        assert_eq!(node.trips.any.keys().copied().collect::<Vec<_>>(), vec![2]);
        let added = apply(join(4, false), &base);
        let mut joiner = held(4, base.clone());
        assert_eq!(joiner.adopt(&added, 0), Some(true));
        let mut outsider = held(5, base.clone());
        assert_eq!(outsider.adopt(&added, 0), Some(false));
    }

    #[test]
    fn a_node_adopts_only_a_higher_epoch() {
        let three = at(0, 3, &[1, 2, 3]);
        let mut node = held(1, three.clone());
        assert_eq!(node.adopt(&at(0, 2, &[1, 2, 3, 4]), 0), None);
        let twin = Members {
            credits: 1,
            ..three.clone()
        };
        assert_ne!(twin.hash(), three.hash());
        assert_eq!(node.adopt(&twin, 0), None);
        assert_eq!(node.state.list, three);
        let other = Members {
            genesis: 9,
            ..at(0, 7, &[1, 2, 3])
        };
        assert_eq!(node.adopt(&other, 0), None);
        assert_eq!(node.adopt(&at(0, 4, &[1, 2, 3, 4]), 0), Some(false));
        assert_eq!(node.list().epoch, 4);
        let mut fresh = Node::new(5);
        assert!(fresh.adopt(&other, 0).is_some());
        assert_eq!(fresh.list().genesis, 9);
    }

    #[test]
    fn the_founder_is_the_lowest_can_found_disk_that_reaches_another() {
        let fresh = |disk, trip_ms| Fresh {
            disk,
            can_found: true,
            live: true,
            trip_ms,
            seen_ms: 1_000,
        };
        let t = 10_000 + FOUND_MS;
        assert_eq!(found(5, true, false, false, &[], t), None);
        assert_eq!(found(5, true, false, false, &[fresh(7, None)], t), None);
        let peers = [fresh(7, Some(t - 10)), fresh(8, Some(t - FOUND_MS))];
        assert_eq!(found(5, true, true, false, &peers, t), None);
        assert_eq!(found(5, true, false, true, &peers, t), None);
        assert_eq!(found(5, false, false, false, &peers, t), None);
        let list = found(5, true, false, false, &peers, t).unwrap();
        assert_eq!(list.members, vec![5, 7]);
        assert_eq!(
            (list.genesis, list.era, list.epoch, list.credits),
            (5, 0, 1, 0)
        );
        assert!(!list.forced && list.removed.is_empty());
        let lower = [fresh(3, Some(t - 10)), fresh(7, Some(t - 10))];
        assert_eq!(found(5, true, false, false, &lower, t), None);
        assert_eq!(
            found(3, true, false, false, &[fresh(5, Some(t - 10))], t).map(|l| l.members),
            Some(vec![3, 5])
        );
        let data = Fresh {
            can_found: false,
            seen_ms: t,
            ..fresh(2, Some(t - 10))
        };
        assert_eq!(
            found(5, true, false, false, &[data, fresh(7, Some(t - 10))], t).map(|l| l.members),
            Some(vec![5, 7])
        );
        let dead = Fresh {
            live: false,
            ..fresh(3, Some(t - FOUND_MS))
        };
        assert!(found(5, true, false, false, &[dead, fresh(7, Some(t - 10))], t).is_some());
        let new = Fresh {
            seen_ms: t - FOUND_MS + 1,
            ..fresh(8, None)
        };
        assert_eq!(
            found(5, true, false, false, &[fresh(7, Some(t - 10)), new], t),
            None
        );
        let settled = Fresh {
            seen_ms: t - FOUND_MS,
            ..new
        };
        assert!(found(5, true, false, false, &[fresh(7, Some(t - 10)), settled], t).is_some());
        let second_run = Fresh {
            seen_ms: 0,
            ..fresh(7, Some(FOUND_MS))
        };
        assert!(found(5, true, false, false, &[second_run], FOUND_MS).is_some());
    }

    #[test]
    fn lists_order_by_era_then_epoch() {
        let late = at(0, 11, &[1, 2, 3]);
        let forced = Members {
            forced: true,
            forced_from: (0, 2),
            ..at(1, 4, &[1, 2])
        };
        assert!(forced.key() > late.key());
        let mut old = held(1, at(0, 2, &[1, 2, 3]));
        assert_eq!(old.adopt(&forced, 0), Some(true));
        let mut new = held(1, forced.clone());
        assert_eq!(new.adopt(&late, 0), None);
        assert_eq!(new.state.list, forced);
        let mut skip = held(1, at(0, 2, &[1, 2, 3]));
        assert_eq!(skip.adopt(&at(0, 4, &[1, 2, 3]), 0), Some(true));
        let mut step = held(1, at(0, 2, &[1, 2, 3]));
        assert_eq!(step.adopt(&at(0, 3, &[1, 2, 3]), 0), Some(false));
        let mut era = held(1, at(0, 2, &[1, 2, 3]));
        let crossed = Members {
            forced_from: (0, 2),
            ..at(1, 4, &[1, 2, 3])
        };
        assert_eq!(era.adopt(&crossed, 0), Some(true));
    }

    #[test]
    fn a_member_with_pending_proposes_without_acting() {
        let base = list(&[1, 2, 3, 4]);
        let pick = pick(&base, &set(&[1, 2, 3, 4]), &set(&[]), 0).unwrap();
        let me = if pick == 3 { 4 } else { 3 };
        assert!(!proposes(me, Some(pick), true, 3 * REPLICATE_MS - 1));
        assert!(proposes(me, Some(pick), true, 3 * REPLICATE_MS));
        assert!(!proposes(me, Some(pick), false, 10 * REPLICATE_MS));
        assert!(proposes(pick, Some(pick), false, 0));
        let pending = apply(join(6, true), &base);
        let mut nodes: Vec<Node> = [1, 2, 3, 4].map(|d| held(d, base.clone())).to_vec();
        for i in [2, 3] {
            nodes[i].prepare(base.key(), b(2, 1));
            nodes[i].accept(base.key(), b(2, 1), &pending, |_| false, 0);
            assert!(nodes[i].pending());
        }
        let mine = me as usize - 1;
        assert!(!nodes[mine].acts(0));
        let ballot = nodes[mine].ballot(0).unwrap();
        let own = apply(loss(1), &base);
        let mut proposal = Proposal::new(&base, ballot, own);
        let mut step = Step::Wait;
        for i in [mine, 0, 5 - mine] {
            let reply = nodes[i].prepare(base.key(), ballot);
            step = proposal.reply(nodes[i].disk, reply);
        }
        assert_eq!(step, Step::Accept(pending.clone()));
        for i in [mine, 0, 5 - mine] {
            let reply = nodes[i].accept(base.key(), ballot, &pending, |_| false, 0);
            step = proposal.reply(nodes[i].disk, reply);
        }
        assert_eq!(step, Step::Commit(pending));
    }

    #[test]
    fn a_join_needs_round_trips_with_a_majority() {
        let base = list(&[1, 2, 3, 4, 5]);
        let node = held(1, base.clone());
        assert!(!node.counts_join(9, &set(&[1, 2])));
        assert!(node.counts_join(9, &set(&[1, 2, 3])));
        assert!(!node.counts_join(9, &set(&[1, 2, 8, 9])));
        assert!(!node.counts_join(3, &set(&[1, 2, 3])));
        let refusing = held(
            1,
            Members {
                removed: vec![(9, 1)],
                ..base.clone()
            },
        );
        assert!(!refusing.counts_join(9, &set(&[1, 2, 3])));
        assert!(refusing.counts_join(8, &set(&[1, 2, 3])));
        let full = held(1, list(&(1..=MEMBERS_MAX as u64).collect::<Vec<_>>()));
        let trips: BTreeSet<u64> = (1..=MEMBERS_MAX as u64).collect();
        assert!(!full.counts_join(500, &trips));
        let below = held(1, list(&(1..MEMBERS_MAX as u64).collect::<Vec<_>>()));
        assert!(below.counts_join(500, &trips));
        let live = set(&[1, 2, 3, 4, 5]);
        let none = set(&[]);
        let key = [base.genesis, u64::from(base.era), base.epoch];
        let first = pick(&base, &live, &none, 0).unwrap();
        assert_eq!(
            Some(first),
            placement::pick(&key, base.members.iter().copied())
        );
        assert_eq!(pick(&base, &live, &none, 3 * REPLICATE_MS - 1), Some(first));
        let second = pick(&base, &live, &none, 3 * REPLICATE_MS).unwrap();
        assert_ne!(second, first);
        let third = pick(&base, &live, &none, 6 * REPLICATE_MS).unwrap();
        assert!(third != first && third != second);
        let ranks: BTreeSet<u64> = (0..5)
            .filter_map(|r| pick(&base, &live, &none, r * 3 * REPLICATE_MS))
            .collect();
        assert_eq!(ranks, live);
        let others: BTreeSet<u64> = live.iter().copied().filter(|d| *d != first).collect();
        assert_ne!(pick(&base, &live, &set(&[first]), 0), Some(first));
        assert_eq!(pick(&base, &live, &live, 0), Some(first));
        assert_eq!(
            pick(&base, &others, &none, 0).map(|d| d == first),
            Some(false)
        );
        assert_eq!(pick(&base, &none, &none, 0), None);
    }

    #[test]
    fn a_founding_gate_blocks_on_a_live_slot() {
        let read = |at_ms, version| SlotRead { at_ms, version };
        assert!(!founding_blocked(read(0, None), read(STALE_MS, None)));
        assert!(!founding_blocked(read(0, None), read(0, None)));
        assert!(founding_blocked(read(0, Some(1)), read(0, Some(1))));
        assert!(founding_blocked(read(0, Some(1)), read(STALE_MS, Some(2))));
        assert!(founding_blocked(read(0, None), read(STALE_MS, Some(1))));
        assert!(founding_blocked(
            read(0, Some(1)),
            read(STALE_MS - 1, Some(1))
        ));
        assert!(!founding_blocked(read(0, Some(1)), read(STALE_MS, Some(1))));
    }

    #[test]
    fn a_used_slot_is_taken_only_when_stale() {
        let read = |at_ms, version| SlotRead { at_ms, version };
        assert!(stale(read(5, Some(3)), read(5 + STALE_MS, Some(3))));
        assert!(!stale(read(5, Some(3)), read(4 + STALE_MS, Some(3))));
        assert!(!stale(read(5, Some(3)), read(5 + STALE_MS, Some(4))));
        assert!(!stale(read(5, Some(3)), read(5 + 10 * STALE_MS, None)));
    }

    #[test]
    fn the_pick_orders_requests() {
        let mut requests = vec![
            Change::Horizon(1),
            Change::Replacement(2),
            join(8, true),
            join(6, false),
            loss(1),
            Change::AnnouncedStop(4),
            Change::FinishedDrain(2),
            Change::SelfRemoval {
                seen_ms: 7,
                disk: 1,
            },
            Change::SelfRemoval {
                seen_ms: 5,
                disk: 9,
            },
            Change::SelfRemoval {
                seen_ms: 5,
                disk: 3,
            },
        ];
        let expected: Vec<Change> = requests.iter().rev().copied().collect();
        requests.sort();
        assert_eq!(requests, expected);
    }

    #[test]
    fn credits_rise_with_first_run_joins_and_a_replacement_removal_uses_one() {
        let base = list(&[1, 2, 3, 4, 5]);
        let one = apply(join(6, true), &base);
        assert_eq!((one.credits, one.epoch, one.members.len()), (1, 2, 6));
        assert_eq!(apply(join(6, false), &base).credits, 0);
        assert_eq!(apply(Change::Replacement(5), &one).credits, 0);
        assert_eq!(
            apply(Change::Replacement(5), &one).members,
            vec![1, 2, 3, 4, 6]
        );
        for change in [
            Change::SelfRemoval {
                seen_ms: 0,
                disk: 5,
            },
            Change::FinishedDrain(5),
            Change::AnnouncedStop(5),
        ] {
            assert_eq!(apply(change, &one).credits, 0);
            assert_eq!(apply(change, &base).credits, 0);
        }
        assert_eq!(apply(loss(5), &one).credits, 1);
        assert_eq!(apply(Change::Horizon(5), &one).credits, 1);
        let absent = BTreeMap::from([(4, 300), (5, 100), (9, 50)]);
        let votes = |yes: &[u64]| BTreeMap::from([(5, set(yes)), (4, set(&[1, 2, 3, 4, 5]))]);
        assert_eq!(replacement(&base, &absent, &votes(&[1, 2, 3])), None);
        let five = Members {
            credits: 1,
            ..base.clone()
        };
        assert_eq!(replacement(&five, &absent, &votes(&[1, 2, 3])), Some(5));
        assert_eq!(replacement(&five, &absent, &votes(&[1, 2])), None);
        assert_eq!(replacement(&five, &absent, &BTreeMap::new()), None);
        let six = Members {
            credits: 1,
            ..list(&[1, 2, 3, 4, 5, 6])
        };
        assert_eq!(replacement(&six, &absent, &votes(&[1, 2, 3])), None);
        assert_eq!(replacement(&six, &absent, &votes(&[1, 2, 3, 7, 8])), None);
        assert_eq!(replacement(&six, &absent, &votes(&[1, 2, 3, 6])), Some(5));
        let tied = BTreeMap::from([(5, 100), (4, 100)]);
        let for_four = BTreeMap::from([(4, set(&[1, 2, 3, 6])), (5, set(&[]))]);
        assert_eq!(replacement(&six, &tied, &for_four), Some(4));
    }

    #[test]
    fn a_forced_change_is_a_tagged_value_in_slot_e_plus_1() {
        let base = Members {
            credits: 2,
            ..at(0, 3, &[1, 2, 3, 4, 5])
        };
        let gone = lost(&[3, 4, 5]);
        assert_eq!(forced(&base, &set(&[1, 2]), &gone), Some(1));
        assert_eq!(forced(&base, &set(&[2, 1]), &gone), Some(1));
        assert_eq!(forced(&base, &set(&[1, 2]), &lost(&[4, 5])), None);
        assert_eq!(forced(&base, &set(&[1, 2, 3]), &lost(&[4, 5])), None);
        assert_eq!(forced(&base, &set(&[1, 2, 3]), &gone), None);
        let four = at(0, 3, &[1, 2, 3, 4]);
        assert_eq!(forced(&four, &set(&[1, 2]), &lost(&[3, 4])), Some(1));
        let key = base.key();
        let mut nodes: Vec<Node> = [1, 2].map(|d| held(d, base.clone())).to_vec();
        let ballot = nodes[0].ballot(0).unwrap();
        let mut proposal = Proposal::forced(&base, ballot, gone.clone());
        assert_eq!(proposal.acceptors(), vec![1, 2]);
        let reply = nodes[0].prepare(key, ballot);
        assert_eq!(proposal.reply(1, reply), Step::Wait);
        assert_eq!(proposal.reply(3, promise(&base, ballot, None)), Step::Wait);
        let reply = nodes[1].prepare(key, ballot);
        let Step::Accept(value) = proposal.reply(2, reply) else {
            panic!("no accept");
        };
        assert!(value.forced);
        assert_eq!(value.key(), (1, 5));
        assert_eq!((value.members.clone(), value.credits), (vec![1, 2], 2));
        assert_eq!(value.removed, vec![(3, 1), (4, 1), (5, 1)]);
        let reply = nodes[0].accept(key, ballot, &value, |d| d <= 2, 0);
        assert_eq!(proposal.reply(1, reply), Step::Wait);
        assert_eq!(proposal.reply(4, Reply::Accepted(key, ballot)), Step::Wait);
        let reply = nodes[1].accept(key, ballot, &value, |d| d <= 2, 0);
        assert_eq!(proposal.reply(2, reply), Step::Commit(value.clone()));
        let mut live_lost = held(2, base.clone());
        live_lost.prepare(key, ballot);
        assert!(matches!(
            live_lost.accept(key, ballot, &value, |d| d == 4, 0),
            Reply::Refused(..)
        ));
        let v = apply(join(6, true), &base);
        let mut proposal = Proposal::forced(&base, b(9, 1), gone.clone());
        proposal.reply(1, promise(&base, b(9, 1), None));
        let Step::Accept(on_v) = proposal.reply(2, promise(&base, b(9, 1), Some((b(4, 2), v))))
        else {
            panic!("no accept");
        };
        assert_eq!(on_v.key(), (1, 5));
        assert_eq!((on_v.members.clone(), on_v.credits), (vec![1, 2, 6], 3));
        let mut node = held(1, base.clone());
        assert_eq!(node.adopt(&on_v, 0), Some(true));
        assert_eq!(node.list().key(), (1, 5));
        let own = apply(loss(5), &base);
        let mut later = Proposal::new(&base, b(12, 2), own);
        later.reply(1, promise(&base, b(12, 2), Some((b(9, 1), on_v.clone()))));
        later.reply(2, promise(&base, b(12, 2), None));
        assert_eq!(
            later.reply(3, promise(&base, b(12, 2), None)),
            Step::Accept(on_v.clone())
        );
        let mut again = Proposal::forced(&base, b(13, 1), gone);
        again.reply(1, promise(&base, b(13, 1), Some((b(9, 1), on_v.clone()))));
        assert_eq!(
            again.reply(2, promise(&base, b(13, 1), None)),
            Step::Accept(on_v.clone())
        );
        let mut wider = Proposal::forced(&base, b(14, 1), lost(&[2, 3, 4, 5]));
        assert_eq!(
            wider.reply(1, promise(&base, b(14, 1), Some((b(9, 1), on_v.clone())))),
            Step::Accept(on_v)
        );
        let six = at(0, 3, &[1, 2, 3, 4, 5, 6]);
        let mut all = Proposal::forced(&six, b(1, 1), lost(&[4, 5, 6]));
        assert_eq!(all.reply(1, promise(&six, b(1, 1), None)), Step::Wait);
        assert_eq!(all.reply(2, promise(&six, b(1, 1), None)), Step::Wait);
        assert!(matches!(
            all.reply(3, promise(&six, b(1, 1), None)),
            Step::Accept(_)
        ));
    }

    #[test]
    fn lost_counts_only_members_and_clears_at_their_removal() {
        let base = list(&[1, 2, 3, 4, 5]);
        let mut node = held(1, base.clone());
        node.lose(4, 1, 0);
        node.lose(4, 2, 10);
        node.lose(4, 1, 20);
        assert_eq!(node.state.lost[&4], (2, 10));
        node.lose(5, 1, 0);
        node.lose(9, 1, 0);
        assert_eq!(node.state.lost.len(), 2);
        let restart_of = |d: u64| Some(if d == 9 { 1 } else { 2 });
        assert_eq!(node.lost_members(restart_of), BTreeMap::from([(4, 2)]));
        node.expire(TOMBSTONE_HORIZON);
        assert_eq!(node.state.lost.keys().copied().collect::<Vec<_>>(), vec![4]);
        node.expire(10 + TOMBSTONE_HORIZON);
        assert!(node.state.lost.is_empty());
        node.lose(4, 2, 0);
        node.lose(5, 1, 0);
        let removal = apply(
            Change::AcceptLoss {
                disk: 4,
                restart: 2,
            },
            &base,
        );
        assert_eq!(removal.removed, vec![(4, 2)]);
        assert_eq!(node.adopt(&removal, 1_000), Some(false));
        assert!(!node.state.lost.contains_key(&4));
        assert!(node.state.lost.contains_key(&5));
        assert!(node.join_refused(4));
        assert!(!node.counts_join(4, &set(&[1, 2, 3])));
        let stop = node
            .change(
                Change::SelfRemoval {
                    seen_ms: 0,
                    disk: 3,
                },
                5_000,
            )
            .unwrap();
        assert_eq!(stop.removed, removal.removed);
        node.adopt(&stop, 5_000);
        assert_eq!(
            node.durable().first_seen(),
            &BTreeMap::from([((4, 2), 1_000)])
        );
        assert!(!node.join_refused(3));
        assert!(!node.state.lost.contains_key(&3));
        let entry = Change::Drop {
            disk: 4,
            restart: 2,
        };
        assert!(
            node.change(join(7, false), 1_000 + TOMBSTONE_HORIZON)
                .unwrap()
                .refuses(4)
        );
        assert_eq!(node.change(entry, 1_000 + TOMBSTONE_HORIZON - 1), None);
        assert_eq!(node.drop_due(1_000 + TOMBSTONE_HORIZON - 1), None);
        assert_eq!(node.drop_due(1_000 + TOMBSTONE_HORIZON), Some(entry));
        let dropped = node.change(entry, 1_000 + TOMBSTONE_HORIZON).unwrap();
        assert!(!dropped.refuses(4));
    }

    #[test]
    fn a_disk_removed_by_loss_is_refused_by_every_node_until_the_horizon() {
        let base = list(&[1, 2, 3, 4, 5]);
        let removal = apply(loss(5), &base);
        let mut proposer = held(1, base.clone());
        proposer.lose(5, 1, 0);
        proposer.adopt(&removal, 0);
        let mut missed = held(3, base.clone());
        missed.adopt(&removal, 0);
        let trips: BTreeSet<u64> = removal.members.iter().copied().collect();
        for node in [&proposer, &missed] {
            assert!(node.join_refused(5));
            assert!(!node.counts_join(5, &trips));
        }
        let back = apply(join(5, false), &removal);
        let ballot = b(3, 2);
        assert!(matches!(
            missed.prepare(removal.key(), ballot),
            Reply::Promise(..)
        ));
        assert_eq!(
            missed.accept(removal.key(), ballot, &back, |_| true, 0),
            Reply::Refused(removal.key(), ballot)
        );
        let other = apply(join(6, false), &removal);
        assert_eq!(
            missed.accept(removal.key(), ballot, &other, |_| true, 0),
            Reply::Accepted(removal.key(), ballot)
        );
        let dropped = proposer
            .change(
                Change::Drop {
                    disk: 5,
                    restart: 1,
                },
                TOMBSTONE_HORIZON,
            )
            .unwrap();
        assert!(!dropped.refuses(5));
        let mut early = held(3, base.clone());
        early.adopt(&removal, 0);
        assert_eq!(
            early.accept(
                removal.key(),
                b(4, 1),
                &dropped,
                |_| true,
                TOMBSTONE_HORIZON - 1
            ),
            Reply::Refused(removal.key(), Ballot::default())
        );
        assert_eq!(
            early.accept(
                removal.key(),
                b(4, 1),
                &dropped,
                |_| true,
                TOMBSTONE_HORIZON
            ),
            Reply::Accepted(removal.key(), b(4, 1))
        );
        let mut late = held(2, removal.clone());
        late.adopt(&dropped, 0);
        assert!(late.counts_join(5, &dropped.members.iter().copied().collect()));
    }

    #[test]
    fn a_removed_entry_never_expires_by_the_run_time_of_another_disk() {
        let base = list(&[1, 2, 3, 4, 5]);
        let young = 3_600_000;
        let old = 30 * 86_400_000;
        let mut writer = held(1, base.clone());
        let removal = writer.change(loss(5), young).unwrap();
        writer.adopt(&removal, young);
        let mut elder = held(2, base.clone());
        elder.adopt(&removal, old);
        assert_eq!(elder.drop_due(old + young), None);
        assert_eq!(
            elder.change(
                Change::Drop {
                    disk: 5,
                    restart: 1
                },
                old + young
            ),
            None
        );
        let next = elder.change(Change::Horizon(4), old + young).unwrap();
        assert!(next.refuses(5));
        let mut acceptor = held(3, base.clone());
        acceptor.adopt(&removal, young);
        let dropped = apply(
            Change::Drop {
                disk: 5,
                restart: 1,
            },
            &removal,
        );
        assert!(matches!(
            acceptor.accept(removal.key(), b(5, 2), &dropped, |_| true, 2 * young),
            Reply::Refused(..)
        ));
        let node = held(4, next.clone());
        assert!(!node.counts_join(5, &next.members.iter().copied().collect()));
    }

    #[test]
    fn a_removed_entry_expires_from_its_adoption_not_from_its_proposal() {
        let base = list(&[1, 2, 3, 4, 5]);
        let mut builder = held(1, base.clone());
        let removal = builder.change(loss(5), 0).unwrap();
        let three_days = 3 * 86_400_000;
        builder.adopt(&removal, three_days);
        assert_eq!(builder.drop_due(TOMBSTONE_HORIZON), None);
        let late = builder
            .change(
                Change::Drop {
                    disk: 5,
                    restart: 1,
                },
                three_days + TOMBSTONE_HORIZON,
            )
            .unwrap();
        assert!(!late.refuses(5));
    }

    #[test]
    fn a_late_normal_accept_never_replaces_a_forced_value() {
        let base = at(0, 3, &[1, 2, 3, 4, 5]);
        let k = base.key();
        let mut n: BTreeMap<u64, Node> = (1..=5).map(|d| (d, held(d, base.clone()))).collect();
        let live = |d: u64| d <= 2;
        let big = b(9, 3);
        let v = apply(join(6, false), &base);
        let mut p3 = Proposal::new(&base, big, v.clone());
        let mut step = Step::Wait;
        for d in [3, 4, 5] {
            let reply = n.get_mut(&d).unwrap().prepare(k, big);
            step = p3.reply(d, reply);
        }
        assert_eq!(step, Step::Accept(v.clone()));
        let gone = lost(&[3, 4, 5]);
        assert_eq!(forced(&base, &set(&[1, 2]), &gone), Some(1));
        let b1 = n.get_mut(&1).unwrap().ballot(0).unwrap();
        let mut p1 = Proposal::forced(&base, b1, gone.clone());
        let mut f1 = None;
        for d in [1, 2] {
            let reply = n.get_mut(&d).unwrap().prepare(k, b1);
            if let Step::Accept(value) = p1.reply(d, reply) {
                f1 = Some(value);
            }
        }
        let f1 = f1.unwrap();
        let r11 = n.get_mut(&1).unwrap().accept(k, b1, &f1, live, 0);
        let r12 = n.get_mut(&2).unwrap().accept(k, b1, &f1, live, 0);
        let late = n.get_mut(&2).unwrap().accept(k, big, &v, live, 0);
        assert!(matches!(late, Reply::Refused(..)));
        let b3 = n.get_mut(&1).unwrap().ballot(9).unwrap();
        let mut p2 = Proposal::forced(&base, b3, gone);
        let mut f2 = None;
        for d in [1, 2] {
            let reply = n.get_mut(&d).unwrap().prepare(k, b3);
            if let Step::Accept(value) = p2.reply(d, reply) {
                f2 = Some(value);
            }
        }
        let f2 = f2.unwrap();
        assert_eq!(f2, f1);
        let mut c2 = Step::Wait;
        for d in [1, 2] {
            let reply = n.get_mut(&d).unwrap().accept(k, b3, &f2, live, 0);
            c2 = p2.reply(d, reply);
        }
        p1.reply(1, r11);
        let c1 = p1.reply(2, r12);
        assert_eq!(c1, Step::Commit(f1.clone()));
        assert_eq!(c2, Step::Commit(f1.clone()));
        assert_eq!(
            n.get_mut(&2).unwrap().accept(k, b(20, 3), &f1, live, 0),
            Reply::Accepted(k, b(20, 3))
        );
    }

    #[test]
    fn a_restore_keeps_every_durable_field_and_raises_round_to_promised() {
        let base = list(&[1, 2, 3]);
        let fresh = Node::new(4);
        assert_eq!(
            fresh.durable(),
            &Durable {
                disk: 4,
                ..Durable::default()
            }
        );
        let mut node = held(1, base.clone());
        node.prepare(base.key(), b(7, 2));
        node.accept(base.key(), b(7, 2), &apply(loss(3), &base), |_| true, 0);
        node.lose(2, 1, 0);
        assert!(Node::restore(3, node.durable().clone()).is_none());
        let restored = Node::restore(1, node.durable().clone()).unwrap();
        assert_eq!(restored.durable(), node.durable());
        assert_eq!(restored.durable().round(), 7);
        assert_eq!(restored.durable().promised(), b(7, 2));
        assert!(restored.durable().accepted().is_some());
        assert_eq!(restored.durable().list(), &base);
        assert_eq!(restored.durable().lost().len(), 1);
        let mut low = node.durable().clone();
        low.round = 0;
        low.promised = b(3, 1);
        let mut raised = Node::restore(1, low).unwrap();
        assert_eq!(raised.durable().promised(), b(7, 2));
        assert_eq!(raised.durable().round(), 7);
        assert!(raised.ballot(0).unwrap().round > 7);
    }

    #[test]
    fn a_value_of_the_wrong_shape_is_refused() {
        let base = list(&[1, 2, 3]);
        let k = base.key();
        let good = apply(join(4, false), &base);
        let unsorted = Members {
            members: vec![1, 3, 2, 4],
            ..good.clone()
        };
        let twice = Members {
            members: vec![1, 2, 2, 3],
            ..good.clone()
        };
        let skipped = Members {
            epoch: 3,
            ..good.clone()
        };
        let other = Members {
            genesis: 9,
            ..good.clone()
        };
        let forced_wrong = Members {
            forced: true,
            ..good.clone()
        };
        let from_wrong = Members {
            forced_from: (0, 9),
            ..good.clone()
        };
        let huge = Members {
            members: (1..=MEMBERS_MAX as u64 + 1).collect(),
            ..good.clone()
        };
        for bad in [
            &unsorted,
            &twice,
            &skipped,
            &other,
            &forced_wrong,
            &from_wrong,
            &huge,
        ] {
            let mut node = held(1, base.clone());
            assert!(
                matches!(
                    node.accept(k, b(1, 1), bad, |_| true, 0),
                    Reply::Refused(..)
                ),
                "{bad:?}"
            );
        }
        let mut node = held(1, base.clone());
        assert_eq!(
            node.accept(k, b(1, 1), &good, |_| true, 0),
            Reply::Accepted(k, b(1, 1))
        );
        let mut node = held(1, base.clone());
        assert_eq!(node.adopt(&unsorted, 0), None);
        assert_eq!(node.adopt(&twice, 0), None);
        assert_eq!(node.adopt(&huge, 0), None);
        assert!(node.adopt(&skipped, 0).is_some());
        assert_eq!(
            Change::Join {
                disk: 2,
                first_run: true
            }
            .apply(&base),
            None
        );
    }

    #[test]
    fn a_newer_slot_in_a_reply_asks_for_a_catch_up() {
        let base = list(&[1, 2, 3]);
        let mut proposal = Proposal::new(&base, b(1, 1), apply(loss(3), &base));
        assert_eq!(proposal.reply(2, Reply::Other((0, 0))), Step::Wait);
        assert_eq!(proposal.reply(2, Reply::Other(base.key())), Step::Wait);
        assert_eq!(proposal.reply(2, Reply::Other((0, 2))), Step::CatchUp(2));
        assert_eq!(proposal.reply(3, Reply::Other((1, 0))), Step::CatchUp(3));
        let with = Members {
            removed: vec![(9, 1)],
            ..base.clone()
        };
        let restart = Members {
            removed: vec![(9, 2)],
            ..base.clone()
        };
        assert_ne!(with.hash(), base.hash());
        assert_ne!(with.hash(), restart.hash());
    }

    #[test]
    fn a_list_fetch_is_one_in_flight_per_epoch() {
        let mut fetches = Fetches::default();
        assert!(fetches.ask((0, 5), 0));
        assert!(!fetches.ask((0, 5), 100));
        assert!(fetches.ask((0, 6), 100));
        assert!(!fetches.ask((0, 5), REPLICATE_MS - 1));
        assert!(fetches.ask((0, 5), REPLICATE_MS));
        fetches.adopted((0, 5));
        assert!(!fetches.ask((0, 6), 200));
        assert!(fetches.ask((0, 5), REPLICATE_MS + 1));
        let mut full = Fetches::default();
        for epoch in 0..FETCHES_MAX as u64 {
            assert!(full.ask((0, epoch), 0));
        }
        assert!(!full.ask((0, 99), 1));
        assert!(full.ask((0, 99), REPLICATE_MS));
        assert_eq!(full.0.len(), 1);
    }

    #[test]
    fn a_forced_change_is_refused_after_a_newer_list_was_seen() {
        let base = list(&[1, 2, 3]);
        let mut node = held(1, base.clone());
        node.lose(2, 1, 0);
        node.lose(3, 1, 0);
        let lost = node.lost_members(|_| Some(1));
        let reached = set(&[1]);
        assert_eq!(node.forced(&reached, &lost, 100), Some(1));
        node.saw(9, (0, 5), 100);
        node.saw(1, (0, 1), 100);
        assert_eq!(node.forced(&reached, &lost, 100), Some(1));
        node.saw(1, (0, 5), 100);
        assert_eq!(node.durable().seen(), Some(((0, 5), 100)));
        node.saw(1, (0, 3), 200);
        assert_eq!(node.durable().seen(), Some(((0, 5), 100)));
        assert_eq!(
            node.forced(&reached, &lost, 100 + TOMBSTONE_HORIZON - 1),
            None
        );
        let ballot = node.ballot(0).unwrap();
        let mut proposal = Proposal::forced(&base, ballot, lost.clone());
        let reply = node.prepare(base.key(), ballot);
        let Step::Accept(value) = proposal.reply(1, reply) else {
            panic!("no accept");
        };
        assert!(matches!(
            node.accept(base.key(), ballot, &value, |d| d == 1, 200),
            Reply::Refused(..)
        ));
        assert_eq!(
            node.forced(&reached, &lost, 100 + TOMBSTONE_HORIZON),
            Some(1)
        );
        assert_eq!(
            node.accept(
                base.key(),
                ballot,
                &value,
                |d| d == 1,
                100 + TOMBSTONE_HORIZON
            ),
            Reply::Accepted(base.key(), ballot)
        );
    }

    fn stale_fork() -> (Members, Members, Members, Members) {
        let l1 = list(&[1, 2, 3]);
        let l2 = apply(join(4, false), &l1);
        let l3 = apply(join(5, false), &l2);
        let l4 = apply(join(6, false), &l3);
        let l5 = apply(Change::Horizon(1), &l4);
        let mut stale = held(1, l1.clone());
        stale.lose(2, 1, 10);
        stale.lose(3, 1, 10);
        let lost = stale.lost_members(|_| Some(1));
        assert_eq!(stale.forced(&set(&[1]), &lost, 20), Some(1));
        let ballot = stale.ballot(0).unwrap();
        let mut proposal = Proposal::forced(&l1, ballot, lost);
        let reply = stale.prepare(l1.key(), ballot);
        let Step::Accept(forced) = proposal.reply(1, reply) else {
            panic!("no accept");
        };
        let after = apply(join(7, false), &forced);
        (l1, l5, forced, after)
    }

    #[test]
    fn a_forced_list_from_an_older_base_is_never_adopted_silently() {
        let (l1, l5, forced, after) = stale_fork();
        assert_eq!((forced.key(), forced.forced_from), ((1, 3), l1.key()));
        let mut newer = held(4, l5.clone());
        newer.pong(5, 1, l5.key(), l5.hash(), 100, 100);
        newer.pong(6, 1, l5.key(), l5.hash(), 100, 100);
        assert!(newer.acts(200));
        assert_eq!(newer.adopt(&forced, 0), None);
        assert_eq!(newer.list(), &l5);
        assert_eq!(newer.durable().conflict(), Some(forced.key()));
        assert!(!newer.acts(200));
        let mut descendant = held(5, l5.clone());
        assert_eq!(descendant.adopt(&after, 0), None);
        assert_eq!(descendant.durable().conflict(), Some(after.key()));
        let mut heard = held(6, l1.clone());
        heard.saw(1, l5.key(), 0);
        assert_eq!(heard.adopt(&forced, 0), None);
        let mut same_era = held(7, forced.clone());
        let other = Members {
            forced_from: (0, 2),
            members: vec![1, 7],
            ..forced.clone()
        };
        let other_next = apply(join(8, false), &other);
        assert_eq!(same_era.adopt(&other_next, 0), None);
        assert_eq!(same_era.durable().conflict(), Some(other_next.key()));
        let mut two_eras = held(8, l1.clone());
        let far = Members {
            era: 2,
            epoch: 9,
            forced_from: (1, 7),
            ..forced.clone()
        };
        assert_eq!(two_eras.adopt(&far, 0), None);
        let mut base_holder = held(10, l1.clone());
        assert_eq!(base_holder.adopt(&forced, 0), Some(true));
        assert_eq!(base_holder.adopt(&after, 0), Some(false));
        assert!(base_holder.durable().conflict().is_none());
        let mut fresh = Node::new(9);
        assert!(fresh.adopt(&far, 0).is_some());
    }

    #[test]
    fn a_stale_survivor_with_a_forced_list_never_merges_into_a_newer_lineage() {
        let l1 = list(&[1, 2, 3]);
        let l2 = apply(Change::Horizon(2), &l1);
        let mut lead = held(1, l2.clone());
        lead.lose(3, 1, 0);
        let base = l2.clone();
        let ballot = lead.ballot(0).unwrap();
        let mut proposal = Proposal::forced(&base, ballot, lead.lost_members(|_| Some(1)));
        let reply = lead.prepare(base.key(), ballot);
        let Step::Accept(first) = proposal.reply(1, reply) else {
            panic!("no accept");
        };
        let mut stale = held(2, l1.clone());
        stale.lose(1, 1, 0);
        stale.lose(3, 1, 0);
        let ballot = stale.ballot(0).unwrap();
        let mut proposal = Proposal::forced(&l1, ballot, stale.lost_members(|_| Some(1)));
        let reply = stale.prepare(l1.key(), ballot);
        let Step::Accept(second) = proposal.reply(2, reply) else {
            panic!("no accept");
        };
        let second_next = apply(join(7, false), &second);
        assert_eq!(first.key(), (1, 4));
        assert_eq!(second_next.key(), (1, 4));
        assert_ne!(first.hash(), second_next.hash());
        let mut on_first = held(5, l2.clone());
        assert!(on_first.adopt(&first, 0).is_some());
        let later = apply(join(8, false), &second_next);
        assert_eq!(on_first.adopt(&later, 0), None);
        assert_eq!(on_first.durable().conflict(), Some(later.key()));
        let mut on_l2 = held(4, l2.clone());
        assert_eq!(on_l2.adopt(&second, 0), None);
        let mut cut = held(6, l1.clone());
        cut.saw(1, first.key(), 0);
        cut.saw(1, l2.key(), 0);
        cut.lose(1, 1, 0);
        cut.lose(3, 1, 0);
        let lost = cut.lost_members(|_| Some(1));
        assert_eq!(lost.len(), 2);
        let none = Node::restore(6, cut.durable().clone()).unwrap();
        assert_eq!(none.forced(&set(&[2, 6]), &lost, 0), None);
    }

    #[test]
    fn a_change_that_leaves_no_member_is_refused() {
        let one = list(&[1]);
        assert_eq!(
            Change::SelfRemoval {
                seen_ms: 0,
                disk: 1
            }
            .apply(&one),
            None
        );
        assert_eq!(Change::Horizon(1).apply(&one), None);
        assert_eq!(loss(1).apply(&one), None);
        let two = list(&[1, 2]);
        let mut node = held(1, two.clone());
        let empty = Members {
            members: Vec::new(),
            epoch: 2,
            ..two.clone()
        };
        assert!(matches!(
            node.accept(two.key(), b(1, 1), &empty, |_| true, 0),
            Reply::Refused(..)
        ));
        assert_eq!(node.adopt(&empty, 0), None);
        assert!(apply(Change::Horizon(2), &two).has(1));
    }

    #[test]
    fn a_removal_of_a_disk_that_is_not_a_member_is_refused() {
        let base = Members {
            credits: 1,
            ..list(&[1, 2, 3])
        };
        for change in [
            Change::FinishedDrain(9),
            Change::AnnouncedStop(9),
            Change::SelfRemoval {
                seen_ms: 0,
                disk: 9,
            },
            Change::Replacement(9),
            Change::Horizon(9),
            loss(9),
            Change::Drop {
                disk: 9,
                restart: 1,
            },
        ] {
            assert_eq!(change.apply(&base), None, "{change:?}");
        }
        assert!(Change::Replacement(3).apply(&base).is_some());
    }

    #[test]
    fn a_waiting_drop_never_blocks_a_join_or_a_removal() {
        let base = list(&[1, 2, 3, 9]);
        let removal = apply(loss(9), &base);
        let mut nodes: Vec<Node> = (1..=3).map(|d| held(d, removal.clone())).collect();
        let day = 86_400_000;
        let run = [
            TOMBSTONE_HORIZON,
            TOMBSTONE_HORIZON - day,
            TOMBSTONE_HORIZON - day,
        ];
        assert_eq!(
            nodes[0].drop_due(run[0]),
            Some(Change::Drop {
                disk: 9,
                restart: 1
            })
        );
        assert_eq!(nodes[1].drop_due(run[1]), None);
        assert!(!drop_agreed(&removal, &set(&[1, 9])));
        assert!(drop_agreed(&removal, &set(&[1, 2])));
        assert_eq!(
            nodes[1].change(
                Change::Drop {
                    disk: 9,
                    restart: 1
                },
                run[1]
            ),
            None
        );
        let mut requests = [
            Change::Drop {
                disk: 9,
                restart: 1,
            },
            join(4, false),
            Change::Horizon(2),
            loss(3),
        ];
        requests.sort();
        assert_eq!(requests[0], loss(3));
        assert_eq!(
            requests[3],
            Change::Drop {
                disk: 9,
                restart: 1
            }
        );
        for (round, change) in [(1, join(4, false)), (2, Change::Horizon(3))] {
            let base = nodes[0].list().clone();
            let own = nodes[0].change(change, run[0]).unwrap();
            assert!(own.refuses(9));
            let ballot = b(round, 1);
            let mut proposal = Proposal::new(&base, ballot, own.clone());
            let mut step = Step::Wait;
            for node in &mut nodes {
                let reply = node.prepare(base.key(), ballot);
                if let Step::Accept(value) = proposal.reply(node.disk(), reply) {
                    step = Step::Accept(value);
                }
            }
            assert_eq!(step, Step::Accept(own.clone()));
            for (i, node) in nodes.iter_mut().enumerate() {
                let reply = node.accept(base.key(), ballot, &own, |_| true, run[i]);
                step = proposal.reply(node.disk(), reply);
            }
            assert_eq!(step, Step::Commit(own.clone()));
            for node in &mut nodes {
                node.adopt(&own, 0);
            }
        }
        assert!(nodes[0].list().refuses(9));
        assert_eq!(nodes[0].list().members, vec![1, 2, 4]);
    }

    #[test]
    fn a_re_added_entry_starts_a_new_horizon() {
        let l0 = list(&[1, 2, 3, 4, 5]);
        let l1 = apply(loss(5), &l0);
        let l2 = apply(
            Change::Drop {
                disk: 5,
                restart: 1,
            },
            &l1,
        );
        let l3 = apply(join(5, false), &l2);
        let l4 = apply(loss(5), &l3);
        let t = TOMBSTONE_HORIZON + 10;
        let mut skipper = held(3, l1.clone());
        skipper.adopt(&l4, t);
        assert_eq!(
            skipper.durable().first_seen(),
            &BTreeMap::from([((5, 1), t)])
        );
        assert_eq!(skipper.drop_due(t), None);
        let mut stepper = held(4, l3.clone());
        stepper.adopt(&l4, t);
        assert_eq!(
            stepper.durable().first_seen(),
            &BTreeMap::from([((5, 1), t)])
        );
        let mut keeper = held(2, l1.clone());
        let l1b = apply(join(6, false), &l1);
        keeper.adopt(&l1b, t);
        assert_eq!(
            keeper.durable().first_seen(),
            &BTreeMap::from([((5, 1), 0)])
        );
    }

    #[test]
    fn a_key_with_no_successor_is_refused_as_a_value() {
        let base = Members {
            epoch: u64::MAX - 3,
            ..list(&[1, 2, 3])
        };
        let mut node = held(1, base.clone());
        let next = apply(join(4, false), &base);
        assert_eq!(next.epoch, u64::MAX - 2);
        assert_eq!(
            node.accept(base.key(), b(1, 1), &next, |_| true, 0),
            Reply::Accepted(base.key(), b(1, 1))
        );
        node.adopt(&next, 0);
        let last = apply(join(5, false), &next);
        assert!(matches!(
            node.accept(next.key(), b(2, 1), &last, |_| true, 0),
            Reply::Refused(..)
        ));
        let mut many = base.clone();
        many.removed = (10..10 + REMOVED_MAX as u64).map(|d| (d, 1)).collect();
        assert!(many.valid());
        many.removed.push((10_000, 1));
        assert!(!many.valid());
    }

    #[test]
    fn a_written_off_disk_that_returns_never_acts_on_its_old_lineage() {
        let l1 = list(&[1, 2, 3]);
        let mut survivor = held(3, l1.clone());
        survivor.lose(1, 1, 0);
        survivor.lose(2, 1, 0);
        let gone = survivor.lost_members(|_| Some(1));
        let ballot = survivor.ballot(0).unwrap();
        let mut proposal = Proposal::forced(&l1, ballot, gone);
        let reply = survivor.prepare(l1.key(), ballot);
        let Step::Accept(forced) = proposal.reply(3, reply) else {
            panic!("no accept");
        };
        let after = apply(join(6, false), &forced);
        let mut returned = held(1, l1.clone());
        let mut other = held(2, l1.clone());
        assert_eq!(returned.adopt(&after, 0), None);
        assert_eq!(returned.durable().conflict(), Some(after.key()));
        assert_eq!(returned.list(), &l1);
        assert!(!returned.acts(0));
        assert_eq!(returned.ballot(0), None);
        assert!(matches!(
            returned.prepare(l1.key(), b(9, 2)),
            Reply::Refused(..)
        ));
        let value = apply(join(6, true), &l1);
        assert!(matches!(
            returned.accept(l1.key(), b(9, 2), &value, |_| true, 0),
            Reply::Refused(..)
        ));
        assert_eq!(returned.forced(&set(&[1]), &lost(&[2, 3]), 0), None);
        let mut far = forced.clone();
        far.era = 2;
        far.epoch = 6;
        far.forced_from = (1, 4);
        assert_eq!(other.adopt(&far, 0), None);
        assert!(other.durable().conflict().is_some());
        assert!(matches!(
            other.prepare(l1.key(), b(9, 1)),
            Reply::Refused(..)
        ));
        let mut proposal = Proposal::new(&l1, b(9, 1), value.clone());
        proposal.reply(1, returned.prepare(l1.key(), b(9, 1)));
        assert_eq!(
            proposal.reply(2, other.prepare(l1.key(), b(9, 1))),
            Step::Wait
        );
    }

    #[test]
    fn a_ballot_is_promised_and_durable_before_any_prepare_is_sent() {
        let base = list(&[1, 2, 3]);
        let mut node = held(1, base.clone());
        let first = node.ballot(4).unwrap();
        assert_eq!(first, b(5, 1));
        assert_eq!(node.durable().promised(), first);
        assert_eq!(node.durable().round(), 5);
        let mut back = Node::restore(1, node.durable().clone()).unwrap();
        let second = back.ballot(0).unwrap();
        assert!(second > first);
        let value = apply(join(4, false), &base);
        let mut acceptor = held(2, base.clone());
        acceptor.prepare(base.key(), first);
        acceptor.accept(base.key(), first, &value, |_| true, 0);
        assert!(matches!(
            acceptor.prepare(base.key(), second),
            Reply::Promise(_, _, Some((ballot, _))) if ballot == first
        ));
    }

    #[derive(Clone)]
    enum Msg {
        Prepare(Key, Ballot),
        Accept(Key, Ballot, Members),
        Reply(Reply),
        Commit(Members),
        Fetch,
    }

    #[derive(Default)]
    struct Slot {
        normal: Option<(u64, BTreeSet<u64>)>,
        forced: Option<(u64, BTreeSet<u64>)>,
    }

    #[derive(Default)]
    struct Totals {
        commits: u64,
        forced: u64,
        adoptions: u64,
        losses: u64,
        restarts: u64,
        pauses: u64,
        returned: u64,
        refused_joins: u64,
        drops: u64,
        jumps: u64,
        forks: u64,
        conflicts: u64,
        settled: u64,
    }

    const DAY: u64 = 86_400_000;

    struct Schedule {
        seed: u64,
        rng: Rng,
        run: BTreeMap<u64, u64>,
        restart: BTreeMap<u64, u64>,
        nodes: BTreeMap<u64, Node>,
        proposals: BTreeMap<u64, Proposal>,
        seen: BTreeMap<u64, u32>,
        queue: Vec<(u64, u64, Msg)>,
        gone: BTreeSet<u64>,
        down: BTreeSet<u64>,
        pending_lose: Vec<(u64, u64)>,
        accepts: BTreeMap<(Key, Ballot, u64), BTreeSet<u64>>,
        chosen: BTreeMap<u64, Members>,
        parent: BTreeMap<u64, u64>,
        slots: BTreeMap<Key, Slot>,
        removed_at: BTreeMap<u64, (u64, u64)>,
        lists: BTreeMap<Key, u64>,
        values: BTreeMap<u64, Vec<Members>>,
        wall: u64,
        next_disk: u64,
        totals: Totals,
    }

    impl Schedule {
        fn new(seed: u64) -> Schedule {
            let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
            let size = 3 + rng.below(3);
            let first = list(&(1..=size).collect::<Vec<_>>());
            let all = size + 1 + rng.below(3);
            let mut run = BTreeMap::new();
            let mut nodes = BTreeMap::new();
            for d in 1..=all {
                let r = rng.below(30 * DAY);
                run.insert(d, r);
                let node = if d <= size || rng.below(2) == 0 {
                    let mut n = Node::new(d);
                    n.adopt(&first, r);
                    n
                } else {
                    Node::new(d)
                };
                nodes.insert(d, node);
            }
            Schedule {
                seed,
                rng,
                restart: run.keys().map(|d| (*d, 1)).collect(),
                run,
                nodes,
                proposals: BTreeMap::new(),
                seen: BTreeMap::new(),
                queue: Vec::new(),
                gone: BTreeSet::new(),
                down: BTreeSet::new(),
                pending_lose: Vec::new(),
                accepts: BTreeMap::new(),
                chosen: BTreeMap::from([(first.hash(), first.clone())]),
                parent: BTreeMap::new(),
                slots: BTreeMap::new(),
                removed_at: BTreeMap::new(),
                lists: BTreeMap::from([(first.key(), first.hash())]),
                values: BTreeMap::new(),
                wall: 1_000,
                next_disk: all + 1,
                totals: Totals::default(),
            }
        }

        fn fail(&self, what: String) -> ! {
            panic!("seed {}: {what}", self.seed)
        }

        fn up(&self, d: u64) -> bool {
            !self.gone.contains(&d) && !self.down.contains(&d)
        }

        fn running(&self) -> Vec<u64> {
            self.nodes.keys().copied().filter(|d| self.up(*d)).collect()
        }

        fn tick(&mut self, ms: u64) {
            self.wall += ms;
            for (d, run) in self.run.iter_mut() {
                if !self.gone.contains(d) && !self.down.contains(d) {
                    *run += ms;
                }
            }
        }

        fn descends(&self, mut x: u64, ancestor: u64) -> bool {
            for _ in 0..10_000 {
                if x == ancestor {
                    return true;
                }
                match self.parent.get(&x) {
                    Some(up) => x = *up,
                    None => return false,
                }
            }
            false
        }

        fn inside_horizon(&self, disk: u64, from: u64) -> bool {
            self.removed_at.get(&disk).is_some_and(|(at, removal)| {
                self.wall < at + TOMBSTONE_HORIZON && self.descends(from, *removal)
            })
        }

        fn note_accept(&mut self, acceptor: u64, base: &Members, ballot: Ballot, value: &Members) {
            let voters = {
                let v = self
                    .accepts
                    .entry((base.key(), ballot, value.hash()))
                    .or_default();
                v.insert(acceptor);
                v.clone()
            };
            let survivors: BTreeSet<u64> = base
                .members
                .iter()
                .copied()
                .filter(|d| !self.gone.contains(d))
                .collect();
            let by_majority = confirmed(voters.len(), base.members.len());
            let by_survivors =
                value.forced && !survivors.is_empty() && survivors.is_subset(&voters);
            if by_majority || by_survivors {
                self.choose(base, value, voters, &survivors);
            }
        }

        fn choose(
            &mut self,
            base: &Members,
            v: &Members,
            voters: BTreeSet<u64>,
            survivors: &BTreeSet<u64>,
        ) {
            let h = v.hash();
            if self.chosen.insert(h, v.clone()).is_none() {
                for d in v.members.iter().filter(|d| !base.has(**d)) {
                    if base.refuses(*d) {
                        self.fail(format!("a chosen value adds the refused disk {d}"));
                    }
                    if self.inside_horizon(*d, h) {
                        self.fail(format!("disk {d} joined inside the horizon of its loss"));
                    }
                }
                for e in base.removed.iter().filter(|e| !v.removed.contains(e)) {
                    self.totals.drops += 1;
                    if self.inside_horizon(e.0, base.hash()) {
                        self.fail(format!("the entry of disk {} left inside the horizon", e.0));
                    }
                }
                for e in v.removed.iter().filter(|e| !base.removed.contains(e)) {
                    self.removed_at.insert(e.0, (self.wall, h));
                }
                let first = *self.lists.entry(v.key()).or_insert(h);
                self.totals.forks += u64::from(first != h);
            }
            let (seed, key) = (self.seed, base.key());
            let slot = self.slots.entry(key).or_default();
            if v.forced {
                if slot.forced.as_ref().is_some_and(|(x, _)| *x != h) {
                    panic!("seed {seed}: two forced values chosen in slot {key:?}");
                }
                slot.forced.get_or_insert((h, survivors.clone()));
            } else {
                match &mut slot.normal {
                    Some((x, _)) if *x != h => {
                        panic!("seed {seed}: two normal values chosen in slot {key:?}")
                    }
                    Some((_, all)) => all.extend(voters),
                    None => slot.normal = Some((h, voters)),
                }
            }
            if let (Some((nh, nvoters)), Some((fh, at_forced))) = (&slot.normal, &slot.forced)
                && !nvoters.is_disjoint(at_forced)
            {
                let (n, f) = (&self.chosen[nh], &self.chosen[fh]);
                let kept = n
                    .members
                    .iter()
                    .filter(|d| !base.has(**d))
                    .all(|d| f.has(*d))
                    && base
                        .members
                        .iter()
                        .filter(|d| !n.has(**d))
                        .all(|d| !f.has(*d))
                    && n.removed
                        .iter()
                        .filter(|e| !base.removed.contains(e))
                        .all(|e| f.removed.contains(e))
                    && n.credits == f.credits;
                assert!(
                    kept,
                    "seed {seed}: a forced value in slot {key:?} drops a chosen normal value"
                );
            }
        }

        fn adopt(&mut self, to: u64, list: &Members) {
            let run = self.run[&to];
            let old = self.nodes[&to].list().clone();
            let conflict = self.nodes[&to].durable().conflict();
            if self.nodes.get_mut(&to).unwrap().adopt(list, run).is_none() {
                if conflict.is_none() && self.nodes[&to].durable().conflict().is_some() {
                    self.totals.conflicts += 1;
                }
                return;
            }
            self.totals.adoptions += 1;
            if !self.chosen.contains_key(&list.hash()) {
                self.fail(format!("node {to} adopted a list that was never chosen"));
            }
            if old.genesis != 0 && !self.descends(list.hash(), old.hash()) {
                self.fail(format!(
                    "node {to} adopted {:?} {:?}, which does not descend from its list {:?} {:?}",
                    list.key(),
                    list.members,
                    old.key(),
                    old.members
                ));
            }
        }

        fn start(&mut self, p: u64, lost: BTreeMap<u64, u64>, settle: bool) {
            let base = self.nodes[&p].list().clone();
            if base.members.is_empty() || self.proposals.contains_key(&p) {
                return;
            }
            let seen = self.seen.get(&p).copied().unwrap_or(0);
            let Some(ballot) = self.nodes.get_mut(&p).unwrap().ballot(seen) else {
                return;
            };
            let run = self.run[&p];
            let proposal = if lost.is_empty() {
                let inside = base.members.clone();
                let pick = |rng: &mut Rng, of: &[u64]| of[rng.below(of.len() as u64) as usize];
                let change = match if settle { 0 } else { self.rng.below(8) } {
                    0 | 1 if settle || inside.len() < 7 => {
                        let outside: Vec<u64> = self
                            .nodes
                            .keys()
                            .copied()
                            .filter(|d| !base.has(*d))
                            .collect();
                        let d = if settle || outside.is_empty() || self.rng.below(5) == 0 {
                            self.next_disk += 1;
                            self.nodes.insert(self.next_disk, Node::new(self.next_disk));
                            self.run.insert(self.next_disk, self.rng.below(30 * DAY));
                            self.restart.insert(self.next_disk, 1);
                            self.next_disk
                        } else {
                            pick(&mut self.rng, &outside)
                        };
                        let trips: BTreeSet<u64> =
                            inside.iter().copied().filter(|x| self.up(*x)).collect();
                        if !settle
                            && self.rng.below(4) != 0
                            && !self.nodes[&p].counts_join(d, &trips)
                        {
                            return;
                        }
                        if base.refuses(d) {
                            self.totals.refused_joins += 1;
                        }
                        join(d, self.rng.below(2) == 0)
                    }
                    2 => {
                        let d = pick(&mut self.rng, &inside);
                        Change::AcceptLoss {
                            disk: d,
                            restart: self.restart[&d],
                        }
                    }
                    3 => Change::SelfRemoval {
                        seen_ms: run,
                        disk: pick(&mut self.rng, &inside),
                    },
                    4 if base.credits > 0 => Change::Replacement(pick(&mut self.rng, &inside)),
                    5 => Change::FinishedDrain(pick(&mut self.rng, &inside)),
                    6 => {
                        let Some(drop @ Change::Drop { disk, restart }) =
                            self.nodes[&p].drop_due(run)
                        else {
                            return;
                        };
                        let yes: BTreeSet<u64> = inside
                            .iter()
                            .copied()
                            .filter(|d| {
                                self.up(*d) && self.nodes[d].passes(&(disk, restart), self.run[d])
                            })
                            .collect();
                        if !drop_agreed(&base, &yes) {
                            return;
                        }
                        drop
                    }
                    _ => Change::Horizon(pick(&mut self.rng, &inside)),
                };
                let Some(own) = self.nodes[&p].change(change, run) else {
                    return;
                };
                Proposal::new(&base, ballot, own)
            } else {
                Proposal::forced(&base, ballot, lost)
            };
            let acceptors = proposal.acceptors();
            self.proposals.insert(p, proposal);
            let reply = self.nodes.get_mut(&p).unwrap().prepare(base.key(), ballot);
            for a in acceptors.into_iter().filter(|a| *a != p) {
                self.queue.push((p, a, Msg::Prepare(base.key(), ballot)));
            }
            self.answer(p, p, reply);
        }

        fn source(&self, base: &Members, lost: &BTreeMap<u64, u64>, value: &Members) -> u64 {
            if !value.forced {
                return base.hash();
            }
            self.values
                .get(&base.hash())
                .into_iter()
                .flatten()
                .find(|v| !v.forced && v.without(lost, base.key()).as_ref() == Some(value))
                .map_or(base.hash(), Members::hash)
        }

        fn answer(&mut self, p: u64, from: u64, reply: Reply) {
            let Some(proposal) = self.proposals.get_mut(&p) else {
                return;
            };
            let (base, ballot, acceptors) =
                (proposal.base.clone(), proposal.ballot, proposal.acceptors());
            let lost = proposal.lost.clone();
            match proposal.reply(from, reply) {
                Step::Accept(value) => {
                    if !self.parent.contains_key(&value.hash()) {
                        let source = self.source(&base, &lost, &value);
                        self.parent.insert(value.hash(), source);
                        self.values
                            .entry(base.hash())
                            .or_default()
                            .push(value.clone());
                    }
                    for a in acceptors {
                        self.queue
                            .push((p, a, Msg::Accept(base.key(), ballot, value.clone())));
                    }
                }
                Step::Commit(value) => {
                    if !self.chosen.contains_key(&value.hash()) {
                        self.fail(format!("a commit at {:?} that is not chosen", base.key()));
                    }
                    self.totals.commits += 1;
                    self.totals.forced += u64::from(!lost.is_empty());
                    self.proposals.remove(&p);
                    self.adopt(p, &value);
                    for d in self.nodes.keys().copied().collect::<Vec<_>>() {
                        self.queue.push((p, d, Msg::Commit(value.clone())));
                    }
                }
                Step::CatchUp(peer) => self.queue.push((p, peer, Msg::Fetch)),
                Step::Wait => {}
            }
        }

        fn deliver(&mut self, from: u64, to: u64, msg: Msg) {
            if !self.up(to) {
                return;
            }
            let run = self.run[&to];
            let told = self.nodes[&from].list().key();
            let gone = &self.gone;
            let node = self.nodes.get_mut(&to).unwrap();
            node.saw(1, told, run);
            match msg {
                Msg::Prepare(key, ballot) => {
                    if key > node.list().key() {
                        self.queue.push((to, from, Msg::Fetch));
                    }
                    let reply = node.prepare(key, ballot);
                    self.queue.push((to, from, Msg::Reply(reply)));
                }
                Msg::Accept(key, ballot, value) => {
                    let base = node.list().clone();
                    let reply = node.accept(key, ballot, &value, |d| !gone.contains(&d), run);
                    if reply == Reply::Accepted(key, ballot) && base.has(to) {
                        self.note_accept(to, &base, ballot, &value);
                    }
                    self.queue.push((to, from, Msg::Reply(reply)));
                }
                Msg::Reply(reply) => self.answer(to, from, reply),
                Msg::Commit(list) => self.adopt(to, &list),
                Msg::Fetch => {
                    let list = node.list().clone();
                    if list.genesis != 0 {
                        self.queue.push((to, from, Msg::Commit(list)));
                    }
                }
            }
        }

        fn lose(&mut self) {
            let members: Vec<u64> = self
                .running()
                .into_iter()
                .filter(|d| self.nodes[d].list().has(*d))
                .collect();
            if members.is_empty() {
                return;
            }
            let m = members[self.rng.below(members.len() as u64) as usize];
            let base = self.nodes[&m].list().clone();
            let size = base.members.len() as u64;
            if size < 2 {
                return;
            }
            let mut pool: Vec<u64> = base
                .members
                .iter()
                .copied()
                .filter(|d| *d != m && !self.gone.contains(d))
                .collect();
            let half = size.div_ceil(2);
            let count = if self.rng.below(4) == 0 {
                1
            } else {
                half + self.rng.below(size - half)
            };
            let mut lost = Vec::new();
            while (lost.len() as u64) < count && !pool.is_empty() {
                lost.push(pool.swap_remove(self.rng.below(pool.len() as u64) as usize));
            }
            for l in &lost {
                self.gone.insert(*l);
                self.down.remove(l);
                self.proposals.remove(l);
            }
            for r in self
                .nodes
                .keys()
                .copied()
                .filter(|d| !self.gone.contains(d))
                .collect::<Vec<_>>()
            {
                for l in &lost {
                    if self.up(r) && self.rng.below(2) == 0 {
                        let (run, restart) = (self.run[&r], self.restart[l]);
                        self.nodes.get_mut(&r).unwrap().lose(*l, restart, run);
                    } else {
                        self.pending_lose.push((r, *l));
                    }
                }
            }
            self.totals.losses += 1;
        }

        fn try_forced(&mut self) {
            for d in self.running() {
                let node = &self.nodes[&d];
                let reached: BTreeSet<u64> = node
                    .list()
                    .members
                    .iter()
                    .copied()
                    .filter(|m| !self.gone.contains(m))
                    .collect();
                let lost = node.lost_members(|m| self.restart.get(&m).copied());
                if lost.is_empty() {
                    continue;
                }
                match node.forced(&reached, &lost, self.run[&d]) {
                    Some(who) if who == d || self.rng.below(4) == 0 => self.start(d, lost, false),
                    _ => {}
                }
            }
        }

        fn restore(&mut self, d: u64) {
            let restored = Node::restore(d, self.nodes[&d].durable().clone()).unwrap();
            self.nodes.insert(d, restored);
            self.proposals.remove(&d);
            self.seen.remove(&d);
            *self.restart.get_mut(&d).unwrap() += 1;
            let back = self.rng.below(60_000);
            let run = self.run.get_mut(&d).unwrap();
            *run = run.saturating_sub(back);
            self.totals.restarts += 1;
        }

        fn return_lost(&mut self) {
            let running = self.running();
            let back: Vec<u64> = self
                .gone
                .iter()
                .copied()
                .filter(|d| running.iter().any(|r| self.nodes[r].list().refuses(*d)))
                .collect();
            if back.is_empty() {
                return;
            }
            let d = back[self.rng.below(back.len() as u64) as usize];
            let holders: Vec<u64> = running
                .into_iter()
                .filter(|r| self.nodes[r].list().refuses(d))
                .collect();
            let s = holders[self.rng.below(holders.len() as u64) as usize];
            self.gone.remove(&d);
            self.restore(d);
            self.totals.returned += 1;
            let list = self.nodes[&s].list().clone();
            self.adopt(d, &list);
        }

        fn step(&mut self) {
            let dt = self.rng.below(60);
            self.tick(dt);
            let running = self.running();
            if running.is_empty() {
                return;
            }
            let any = running[self.rng.below(running.len() as u64) as usize];
            match self.rng.below(100) {
                0..=6 => self.start(any, BTreeMap::new(), false),
                7..=8 => {
                    if let Some(proposal) = self.proposals.remove(&any) {
                        let seen = self.seen.entry(any).or_insert(0);
                        *seen = (*seen).max(proposal.seen);
                    }
                }
                9 => self.restore(any),
                10 if self.down.len() + 1 < running.len() => {
                    self.down.insert(any);
                    self.proposals.remove(&any);
                    self.totals.pauses += 1;
                }
                11..=12 => {
                    if let Some(d) = self.down.pop_first() {
                        self.restore(d);
                    }
                }
                13..=15 => self.try_forced(),
                16 if self.totals.losses < 2 && self.rng.below(3) == 0 => self.lose(),
                17..=19 if !self.pending_lose.is_empty() => {
                    let at = self.rng.below(self.pending_lose.len() as u64) as usize;
                    let (r, l) = self.pending_lose.swap_remove(at);
                    if self.up(r) {
                        let (run, restart) = (self.run[&r], self.restart[&l]);
                        self.nodes.get_mut(&r).unwrap().lose(l, restart, run);
                    }
                }
                20 => {
                    let z = running[self.rng.below(running.len() as u64) as usize];
                    self.queue.push((any, z, Msg::Fetch));
                }
                21 if self.rng.below(2) == 0 => {
                    let jump =
                        [3_600_000, DAY, 3 * DAY, 6 * DAY, 8 * DAY][self.rng.below(5) as usize];
                    self.tick(jump);
                    self.totals.jumps += 1;
                    for d in self.running() {
                        let run = self.run[&d];
                        self.nodes.get_mut(&d).unwrap().expire(run);
                    }
                }
                22 => self.return_lost(),
                _ if !self.queue.is_empty() => {
                    let at = self.rng.below(self.queue.len() as u64) as usize;
                    let (from, to, msg) = self.queue.swap_remove(at);
                    if self.rng.below(10) == 0 {
                        return;
                    }
                    if self.rng.below(10) == 0 {
                        self.queue.push((from, to, msg.clone()));
                    }
                    self.deliver(from, to, msg);
                }
                _ => {}
            }
        }

        fn drain(&mut self) {
            for _ in 0..5_000 {
                if self.queue.is_empty() {
                    return;
                }
                let at = self.rng.below(self.queue.len() as u64) as usize;
                let (from, to, msg) = self.queue.swap_remove(at);
                self.deliver(from, to, msg);
            }
        }

        fn settle(&mut self) {
            while let Some(d) = self.down.pop_first() {
                self.restore(d);
            }
            self.pending_lose.clear();
            self.proposals.clear();
            self.drain();
            self.tick(TOMBSTONE_HORIZON + 1);
            let running = self.running();
            let Some(top) = running
                .iter()
                .copied()
                .filter(|d| self.nodes[d].durable().conflict().is_none())
                .max_by_key(|d| self.nodes[d].list().key())
            else {
                return;
            };
            let target = self.nodes[&top].list().clone();
            for d in &running {
                self.adopt(*d, &target);
            }
            let live: Vec<u64> = target
                .members
                .iter()
                .copied()
                .filter(|d| {
                    self.up(*d)
                        && self.nodes[d].list() == &target
                        && self.nodes[d].durable().conflict().is_none()
                })
                .collect();
            if live.is_empty() || !confirmed(live.len(), target.members.len()) {
                return;
            }
            for round in 0..12 {
                let p = live[round % live.len()];
                self.start(p, BTreeMap::new(), true);
                self.drain();
                if live
                    .iter()
                    .any(|d| self.nodes[d].list().key() > target.key())
                {
                    self.totals.settled += 1;
                    return;
                }
                if let Some(proposal) = self.proposals.remove(&p) {
                    let seen = self.seen.entry(p).or_insert(0);
                    *seen = (*seen).max(proposal.seen);
                }
                let dt = self.rng.below(15_000);
                self.tick(dt);
            }
            self.fail(format!(
                "a live majority of {:?} {:?} committed nothing in 12 rounds",
                target.key(),
                target.members
            ));
        }
    }

    fn schedules(seeds: u64) -> Totals {
        let mut totals = Totals::default();
        for seed in 0..seeds {
            let mut schedule = Schedule::new(seed);
            for _ in 0..400 {
                schedule.step();
            }
            schedule.settle();
            let t = schedule.totals;
            totals.commits += t.commits;
            totals.forced += t.forced;
            totals.adoptions += t.adoptions;
            totals.losses += t.losses;
            totals.restarts += t.restarts;
            totals.pauses += t.pauses;
            totals.returned += t.returned;
            totals.refused_joins += t.refused_joins;
            totals.drops += t.drops;
            totals.jumps += t.jumps;
            totals.forks += t.forks;
            totals.conflicts += t.conflicts;
            totals.settled += t.settled;
        }
        println!(
            "MEMBERS UNIT 1 schedules={seeds} commits={} forced={} adoptions={} losses={} restarts={} pauses={} returned={} refused-join-tries={} entry-drops={} time-jumps={} forks={} conflicts={} settled={}",
            totals.commits,
            totals.forced,
            totals.adoptions,
            totals.losses,
            totals.restarts,
            totals.pauses,
            totals.returned,
            totals.refused_joins,
            totals.drops,
            totals.jumps,
            totals.forks,
            totals.conflicts,
            totals.settled
        );
        totals
    }

    #[test]
    fn one_value_per_epoch_under_random_schedules() {
        let totals = schedules(100_000);
        assert!(totals.adoptions > 1_000_000);
        assert!(totals.forced > 5_000);
        assert!(totals.returned > 1_000);
        assert!(totals.drops > 100);
        assert!(totals.settled > 10_000);
        assert_eq!(totals.forks, 0);
    }

    #[test]
    #[ignore]
    fn one_value_per_epoch_under_two_million_random_schedules() {
        let totals = schedules(2_000_000);
        assert!(totals.adoptions > 20_000_000);
    }
}
