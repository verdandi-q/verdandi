use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Bound, RangeBounds};

use rand::seq::{IndexedRandom, SliceRandom};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use verdandi_core::group::{TakeOutcome, TakeRequest};
use verdandi_core::ids::{Address, MessageId, SegmentId};
use verdandi_core::lease::{Bury, Nack, Receipt, Settle, SettleError};
use verdandi_core::retention::RETAIN_MS;
use verdandi_core::{
    Action, Core, DeadCopy, Generation, MAX_FAILURES, PROVISIONAL_MS, Reason, Record, Timing,
    WaiterId, nth_id,
};

const INC: u64 = 0x5eed;
const SEEDS: u64 = 100;
const STEPS: usize = 400;
const GROUPS: [&str; 2] = ["g", "h"];
const STREAMS: [&str; 3] = ["a", "b", "c.x"];
const SELECTORS: [&str; 6] = ["a", "b", "c.x", "*", "c.*", "a.dead.g"];
const SEGMENT_POSITIONS: u32 = 16;

type Key = (String, Generation, Address);

fn origin(stream: &str) -> &str {
    stream.find(".dead.").map_or(stream, |i| &stream[..i])
}

fn glob(pattern: &[u8], text: &[u8]) -> bool {
    match pattern.split_first() {
        None => text.is_empty(),
        Some((b'*', rest)) => (0..=text.len()).any(|i| glob(rest, &text[i..])),
        Some((c, rest)) => text.first() == Some(c) && glob(rest, &text[1..]),
    }
}

fn matches(selector: &str, stream: &str) -> bool {
    selector == stream
        || (!stream.starts_with('_')
            && !stream.contains(".dead.")
            && glob(selector.as_bytes(), stream.as_bytes()))
}

struct Message {
    stream: String,
    id: MessageId,
    timing: Timing,
}

struct SimLease {
    group: String,
    generation: Generation,
    until: u64,
    open: BTreeMap<MessageId, Address>,
    settled: BTreeMap<MessageId, Address>,
}

struct Sim {
    rng: ChaCha8Rng,
    core: Core,
    now: u64,
    last_ms: u64,
    counter: u16,
    segment: u32,
    position: u32,
    pressure: bool,
    next_receipt: u128,
    messages: BTreeMap<Address, Message>,
    pending: Vec<Action>,
    log: Vec<(Record, u64)>,
    leases: BTreeMap<Receipt, SimLease>,
    waiters: BTreeMap<WaiterId, (String, String)>,
    acked: BTreeSet<Key>,
    ack_ms: BTreeMap<(String, SegmentId), u64>,
    failures: BTreeMap<Key, (u32, u64)>,
    expiries: BTreeMap<Key, u32>,
    dead: BTreeSet<Key>,
    from: BTreeMap<(String, Generation), Bound<MessageId>>,
    redrive: BTreeMap<(String, Generation, String), MessageId>,
    reads: BTreeMap<String, BTreeMap<String, Generation>>,
    read_marks: Vec<(usize, String, Generation, String)>,
    forgotten: BTreeMap<String, Generation>,
    acked_gen: BTreeMap<String, Generation>,
    generation: BTreeMap<String, Generation>,
    durable: BTreeMap<String, Generation>,
    reserved: BTreeMap<String, u64>,
    forgetting: BTreeSet<String>,
    tombstones: BTreeMap<String, MessageId>,
    restarts: u64,
    handed: u64,
    fifth: u64,
    lost: u64,
    dropped: u64,
    forgets: u64,
}

impl Sim {
    fn new(seed: u64) -> Self {
        Self {
            rng: ChaCha8Rng::seed_from_u64(seed),
            core: Core::new(INC),
            now: 1,
            last_ms: 0,
            counter: 0,
            segment: 0,
            position: 0,
            pressure: false,
            next_receipt: 0,
            messages: BTreeMap::new(),
            pending: Vec::new(),
            log: Vec::new(),
            leases: BTreeMap::new(),
            waiters: BTreeMap::new(),
            acked: BTreeSet::new(),
            ack_ms: BTreeMap::new(),
            failures: BTreeMap::new(),
            expiries: BTreeMap::new(),
            dead: BTreeSet::new(),
            from: BTreeMap::new(),
            redrive: BTreeMap::new(),
            reads: BTreeMap::new(),
            read_marks: Vec::new(),
            forgotten: BTreeMap::new(),
            acked_gen: BTreeMap::new(),
            generation: BTreeMap::new(),
            durable: BTreeMap::new(),
            reserved: BTreeMap::new(),
            forgetting: BTreeSet::new(),
            tombstones: BTreeMap::new(),
            restarts: 0,
            handed: 0,
            fifth: 0,
            lost: 0,
            dropped: 0,
            forgets: 0,
        }
    }

    fn place(&mut self, count: u32) -> (Address, MessageId) {
        if self.position + count > SEGMENT_POSITIONS {
            self.segment += 1;
            self.position = 0;
        }
        let at = Address {
            segment: SegmentId {
                incarnation: INC,
                seq: self.segment,
            },
            position: self.position,
        };
        self.position += count;
        if self.now > self.last_ms {
            self.last_ms = self.now;
            self.counter = 0;
        }
        let id = MessageId::new(self.last_ms, INC, self.counter).unwrap();
        self.counter += count as u16;
        (at, id)
    }

    fn publish(&mut self) {
        let stream = STREAMS[self.rng.random_range(0..STREAMS.len())];
        let count = self.rng.random_range(1..=3);
        let timing = Timing {
            delay_until: self
                .rng
                .random_bool(0.2)
                .then(|| self.now + self.rng.random_range(1..3_000)),
            expire_at: self
                .rng
                .random_bool(0.2)
                .then(|| self.now + self.rng.random_range(1..4_000)),
        };
        let (at, first_id) = self.place(count);
        self.pending.push(Action::Write(Record::Messages {
            stream: stream.to_owned(),
            segment: at.segment,
            first_position: at.position,
            first_id,
            count,
            timing: (0..count).map(|n| (at.position + n, timing)).collect(),
        }));
    }

    fn used(&self, group: &str) -> u64 {
        self.generation
            .get(group)
            .max(self.forgotten.get(group))
            .map_or(0, |g| g.counter)
    }

    fn adopt(&mut self, group: &str, generation: Generation) {
        if self.forgotten.get(group) >= Some(&generation) {
            return;
        }
        let durable = self.durable.entry(group.to_owned()).or_insert(generation);
        *durable = (*durable).max(generation);
        if self.generation.get(group).is_none_or(|g| *g < generation) {
            self.generation.insert(group.to_owned(), generation);
            self.forgetting.remove(group);
        }
    }

    fn eligible(&self, group: &str, generation: Generation, selector: &str, at: Address) -> bool {
        let Some(m) = self.messages.get(&at) else {
            return false;
        };
        let mark = if matches(selector, &m.stream) {
            None
        } else if matches(selector, origin(&m.stream)) {
            match self
                .redrive
                .get(&(group.to_owned(), generation, m.stream.clone()))
            {
                Some(mark) => Some(*mark),
                None => return false,
            }
        } else {
            return false;
        };
        let key = (group.to_owned(), generation, at);
        let (failures, not_before) = self.failures.get(&key).copied().unwrap_or((0, 0));
        mark.is_none_or(|mark| m.id <= mark)
            && !self.tombstoned(m)
            && (mark.is_some()
                || self
                    .from
                    .get(&(group.to_owned(), generation))
                    .is_none_or(|from| (*from, Bound::Unbounded).contains(&m.id)))
            && !self.acked.contains(&key)
            && failures < MAX_FAILURES
            && not_before <= self.now
            && m.timing.delay_until.is_none_or(|d| d <= self.now)
            && m.timing.expire_at.is_none_or(|e| e > self.now)
    }

    fn tombstoned(&self, m: &Message) -> bool {
        self.tombstones
            .get(&m.stream)
            .max(self.tombstones.get(origin(&m.stream)))
            .is_some_and(|t| m.id <= *t)
    }

    fn reads(&self, group: &str, m: &Message) -> bool {
        let Some(&generation) = self.generation.get(group) else {
            return false;
        };
        let after_from = self
            .from
            .get(&(group.to_owned(), generation))
            .is_none_or(|from| (*from, Bound::Unbounded).contains(&m.id));
        self.acked_gen.contains_key(group)
            && self.reads.get(group).is_some_and(|reads| {
                reads.keys().any(|selector| {
                    (matches(selector, &m.stream) && (after_from || origin(&m.stream) == m.stream))
                        || (matches(selector, origin(&m.stream))
                            && self
                                .redrive
                                .get(&(group.to_owned(), generation, m.stream.clone()))
                                .is_some_and(|mark| m.id <= *mark))
                })
            })
    }

    fn must_hold(&self, group: &str, at: Address, m: &Message) -> bool {
        let Some(&generation) = self.generation.get(group) else {
            return false;
        };
        let selectors = self.reads.get(group);
        let direct = selectors
            .is_some_and(|reads| reads.keys().any(|selector| matches(selector, &m.stream)));
        let marked = selectors.is_some_and(|reads| {
            reads
                .keys()
                .any(|selector| matches(selector, origin(&m.stream)))
        }) && self
            .redrive
            .get(&(group.to_owned(), generation, m.stream.clone()))
            .is_some_and(|mark| m.id <= *mark);
        self.reads(group, m)
            && !self.tombstoned(m)
            && (marked
                || (direct
                    && self
                        .from
                        .get(&(group.to_owned(), generation))
                        .is_none_or(|from| (*from, Bound::Unbounded).contains(&m.id))))
            && !self.acked.contains(&(group.to_owned(), generation, at))
    }

    fn may_drop(&self, segment: SegmentId) -> bool {
        self.messages
            .iter()
            .filter(|(at, _)| at.segment == segment)
            .all(|(at, m)| {
                let readers: Vec<&str> = GROUPS.into_iter().filter(|g| self.reads(g, m)).collect();
                self.tombstoned(m)
                    || (readers.is_empty() && m.id.ms() + RETAIN_MS <= self.now)
                    || (!readers.is_empty()
                        && readers.iter().all(|group| {
                            !self.must_hold(group, *at, m)
                                && (self.pressure
                                    || self
                                        .ack_ms
                                        .get(&((*group).to_owned(), segment))
                                        .is_none_or(|t| t + RETAIN_MS <= self.now))
                        }))
            })
    }

    fn segments(&self) -> BTreeSet<SegmentId> {
        self.messages.keys().map(|at| at.segment).collect()
    }

    fn check_holders(&self) {
        for segment in self.segments() {
            let held = self.core.holders(segment, self.now, self.pressure);
            let first = Address {
                segment,
                position: 0,
            };
            let last = Address {
                segment,
                position: u32::MAX,
            };
            for (at, m) in self.messages.range(first..=last) {
                for group in GROUPS {
                    assert!(
                        !self.must_hold(group, *at, m) || held.groups.contains(group),
                        "retention: {group} must hold {at:?}, holders {held:?}"
                    );
                }
            }
        }
    }

    fn check_fold(&self, folded: &Core) {
        for segment in self.segments() {
            assert_eq!(
                self.core.holders(segment, self.now, self.pressure),
                folded.holders(segment, self.now, self.pressure),
                "I12: the fold and the live core hold {segment:?} differently"
            );
        }
    }

    fn held(&self, group: &str, generation: Generation, at: Address) -> bool {
        let leased = self.leases.values().any(|l| {
            l.group == group && l.generation == generation && l.open.values().any(|a| *a == at)
        });
        let pending = self.pending.iter().any(|action| match action {
            Action::Write(Record::Ack {
                group: g,
                generation: n,
                address,
                ..
            })
            | Action::Write(Record::Failure {
                group: g,
                generation: n,
                address,
                ..
            }) => g == group && *n == generation && *address == at,
            Action::Dead(d) => d.group == group && d.generation == generation && d.address == at,
            _ => false,
        });
        leased || pending
    }

    fn available(&self, group: &str, selector: &str) -> Vec<Address> {
        let Some(&generation) = self.generation.get(group) else {
            return Vec::new();
        };
        if self.forgetting.contains(group) {
            return Vec::new();
        }
        self.messages
            .keys()
            .filter(|at| {
                self.eligible(group, generation, selector, **at)
                    && !self.held(group, generation, **at)
            })
            .copied()
            .collect()
    }

    fn check_dead(&mut self, dead: &DeadCopy) {
        let key = (dead.group.clone(), dead.generation, dead.address);
        assert!(
            self.dead.insert(key.clone()),
            "second dead copy for {key:?}"
        );
        assert_eq!(
            dead.dead_stream,
            format!("{}.dead.{}", origin(&dead.stream), dead.group)
        );
        match dead.reason {
            Reason::Attempts => assert!(
                self.failures.get(&key).is_some_and(|f| f.0 >= MAX_FAILURES),
                "attempts dead copy below the fifth failure for {key:?}"
            ),
            Reason::Expired => assert!(
                self.messages[&dead.address]
                    .timing
                    .expire_at
                    .is_some_and(|e| e <= self.now),
                "expired dead copy before expiry for {key:?}"
            ),
            Reason::Dead => {}
        }
        assert!(!self.pressure, "dead copy under pressure for {key:?}");
        assert!(
            !self.forgetting.contains(&dead.group),
            "dead copy while a provisional forget waits for {key:?}"
        );
    }

    fn check_forget(&mut self, group: &str, generation: Generation) {
        let waiting = self.pending.iter().any(|action| match action {
            Action::Write(
                Record::Ack {
                    group: g,
                    generation: n,
                    ..
                }
                | Record::Failure {
                    group: g,
                    generation: n,
                    ..
                },
            ) => g == group && *n == generation,
            Action::Dead(d) => d.group == group && d.generation == generation,
            _ => false,
        });
        assert!(
            !waiting,
            "provisional forget of {group} while a settle waits for its block"
        );
        assert!(
            !self.acked_gen.contains_key(group),
            "provisional forget of {group} after an ack"
        );
        assert_eq!(self.generation.get(group), Some(&generation));
        assert!(
            self.forgetting.insert(group.to_owned()),
            "second provisional forget of {group}"
        );
        self.forgets += 1;
    }

    fn handle(&mut self, actions: Vec<Action>) {
        let mut woken = Vec::new();
        for action in actions {
            match action {
                Action::Wake(waiter, _) => woken.push(waiter),
                Action::Await { .. } => {}
                Action::Dead(dead) => {
                    self.check_dead(&dead);
                    self.pending.push(Action::Dead(dead));
                }
                Action::Write(Record::Forget {
                    ref group,
                    generation,
                    provisional: true,
                }) => {
                    self.check_forget(group, generation);
                    self.pending.push(action);
                }
                write => self.pending.push(write),
            }
        }
        for waiter in woken {
            let (group, selector) = self
                .waiters
                .remove(&waiter)
                .expect("a wake names a registered waiter");
            self.take(&group, &selector, true);
        }
    }

    fn take(&mut self, group: &str, selector: &str, wait: bool) {
        self.next_receipt += 1;
        let receipt = Receipt(self.next_receipt);
        let before: Option<(Generation, BTreeSet<Address>)> = self
            .generation
            .get(group)
            .map(|g| (*g, self.available(group, selector).into_iter().collect()));
        if !self.generation.contains_key(group) {
            let used = self.used(group);
            let incarnation = INC + self.restarts;
            let reserved = self.reserved.entry(group.to_owned()).or_default();
            *reserved = (*reserved).max(used + 1);
            let created = Generation {
                counter: *reserved,
                incarnation,
            };
            self.generation.insert(group.to_owned(), created);
            self.forgetting.remove(group);
        }
        let generation = self.generation[group];
        let limit = self.rng.random_range(1..=4);
        let req = TakeRequest {
            group,
            selector,
            limit,
            lease_ms: self.rng.random_range(500..5_000),
            receipt,
            wait,
        };
        let (outcome, actions) = self.core.take(&req, self.now);
        assert_eq!(
            self.core.generation(group),
            Some(generation),
            "generation of {group} after a take"
        );
        match outcome {
            TakeOutcome::Leased { until, messages } => {
                assert!(messages.len() <= limit);
                for m in &messages {
                    let key = (group.to_owned(), generation, m.address);
                    assert!(
                        !self.acked.contains(&key),
                        "I9: redelivery after ack {key:?}"
                    );
                    assert!(
                        !self.forgetting.contains(group),
                        "handed {key:?} while a provisional forget waits"
                    );
                    assert!(
                        self.eligible(group, generation, selector, m.address),
                        "handed a position that is not due {key:?}"
                    );
                    assert!(
                        !self.held(group, generation, m.address),
                        "handed a held position {key:?}"
                    );
                    if let Some((g, avail)) = &before
                        && *g == generation
                    {
                        assert!(avail.contains(&m.address));
                    }
                    assert_eq!(self.messages[&m.address].id, m.id);
                    assert!(m.attempt >= 1);
                }
                self.handed += messages.len() as u64;
                self.leases.insert(
                    receipt,
                    SimLease {
                        group: group.to_owned(),
                        generation,
                        until,
                        open: messages.iter().map(|m| (m.id, m.address)).collect(),
                        settled: BTreeMap::new(),
                    },
                );
            }
            TakeOutcome::NoWork { register } => {
                assert_eq!(register.is_some(), wait);
                if let Some(waiter) = register {
                    self.waiters
                        .insert(waiter, (group.to_owned(), selector.to_owned()));
                }
            }
        }
        self.read_marks.push((
            self.pending.len(),
            group.to_owned(),
            generation,
            selector.to_owned(),
        ));
        self.handle(actions);
        if self.generation.get(group) == Some(&generation)
            && !self.leases.contains_key(&receipt)
            && !wait
        {
            let left = self.available(group, selector);
            assert!(left.is_empty(), "I7: empty answer with work {left:?}");
        }
    }

    fn check_forgetting(&mut self) {
        for (receipt, lease) in &self.leases {
            if lease.until > self.now {
                let refused = self
                    .core
                    .settle(*receipt, &Settle::default(), self.now)
                    .is_err();
                assert_eq!(
                    refused,
                    self.forgetting.contains(&lease.group),
                    "the core and the oracle disagree on a provisional forget of {}",
                    lease.group
                );
            }
        }
    }

    fn check_waiters(&self) {
        for (waiter, (group, selector)) in &self.waiters {
            let left = self.available(group, selector);
            assert!(
                left.is_empty(),
                "I7: waiter {waiter:?} of {group} sleeps with work {left:?}"
            );
        }
    }

    fn settle(&mut self) {
        let receipts: Vec<Receipt> = self.leases.keys().copied().collect();
        let Some(&receipt) = receipts.choose(&mut self.rng) else {
            return;
        };
        let group = self.leases[&receipt].group.clone();
        let refused = self.forgetting.contains(&group);
        let open: Vec<MessageId> = self.leases[&receipt].open.keys().copied().collect();
        let mut settle = Settle::default();
        let repeat = self.leases[&receipt]
            .settled
            .iter()
            .next()
            .filter(|_| self.rng.random_bool(0.3))
            .map(|(id, at)| (*id, *at));
        settle.ack.extend(repeat.map(|(id, _)| id));
        for id in open {
            match self.rng.random_range(0..10) {
                0..2 => settle.ack.push(id),
                2..7 => settle.nack.push(Nack {
                    id,
                    delay_ms: self
                        .rng
                        .random_bool(0.3)
                        .then(|| self.rng.random_range(0..2_000)),
                    error: Some("sim".to_owned()),
                }),
                7 => settle.dead.push(Bury { id, error: None }),
                _ => {}
            }
        }
        if self.rng.random_bool(0.2) {
            settle.extend_ms = Some(self.rng.random_range(500..5_000));
        }
        match self.core.settle(receipt, &settle, self.now) {
            Err(SettleError::OldGeneration) => assert!(
                refused,
                "settle of a live lease of {group} refused with no forget in flight"
            ),
            Err(SettleError::Pressure) => {
                assert!(!refused && self.pressure && !settle.dead.is_empty());
            }
            Err(e) => panic!("settle of a live lease refused: {e:?}"),
            Ok(actions) => {
                assert!(
                    !refused,
                    "settle accepted while a provisional forget of {group} waits"
                );
                if let Some((_, address)) = repeat {
                    let wait = Action::Await {
                        group: group.clone(),
                        address,
                    };
                    assert!(
                        actions.contains(&wait),
                        "I9: a repeat settle of {address:?} does not wait for its block"
                    );
                }
                let lease = self.leases.get_mut(&receipt).expect("lease chosen above");
                let named = settle
                    .ack
                    .iter()
                    .chain(settle.nack.iter().map(|n| &n.id))
                    .chain(settle.dead.iter().map(|d| &d.id));
                for id in named {
                    if let Some(at) = lease.open.remove(id) {
                        lease.settled.insert(*id, at);
                    }
                }
                if let Some(extend) = settle.extend_ms {
                    lease.until = self.now + extend;
                }
                self.handle(actions);
            }
        }
    }

    fn release(&mut self) {
        let receipts: Vec<Receipt> = self.leases.keys().copied().collect();
        let Some(&receipt) = receipts.choose(&mut self.rng) else {
            return;
        };
        let lease = self.leases.get_mut(&receipt).expect("lease chosen above");
        let ids: Vec<MessageId> = lease.open.keys().copied().skip(1).collect();
        for id in &ids {
            lease.open.remove(id);
        }
        let actions = self.core.release(receipt, &ids);
        self.handle(actions);
    }

    fn advance(&mut self, by: u64) {
        self.now += by;
        let actions = self.core.tick(self.now);
        let mut expected = BTreeSet::new();
        let expired: Vec<Receipt> = self
            .leases
            .iter()
            .filter(|(_, l)| l.until <= self.now)
            .map(|(r, _)| *r)
            .collect();
        for receipt in expired {
            let lease = self.leases.remove(&receipt).expect("listed above");
            if self.generation.get(&lease.group) != Some(&lease.generation) {
                continue;
            }
            for at in lease.open.values() {
                let key = (lease.group.clone(), lease.generation, *at);
                let n = self.expiries.entry(key.clone()).or_default();
                *n += 1;
                if *n >= 2 {
                    expected.insert(key);
                }
            }
        }
        let counted: BTreeSet<Key> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Write(Record::Failure {
                    group,
                    generation,
                    address,
                    ..
                }) => Some((group.clone(), *generation, *address)),
                _ => None,
            })
            .collect();
        assert_eq!(counted, expected, "first expiry free, later expiries count");
        self.handle(actions);
    }

    fn abandon(&mut self) {
        let pending = std::mem::take(&mut self.pending);
        let mut kept = Vec::new();
        let mut index = Vec::new();
        let mut woken = Vec::new();
        for action in pending {
            index.push(kept.len());
            let lossy = matches!(
                action,
                Action::Dead(_)
                    | Action::Write(
                        Record::Ack { .. } | Record::Failure { .. } | Record::Forget { .. }
                    )
            );
            if lossy && self.rng.random_bool(0.3) {
                self.lost += 1;
                woken.extend(self.lose(action));
            } else {
                kept.push(action);
            }
        }
        index.push(kept.len());
        for mark in &mut self.read_marks {
            mark.0 = index[mark.0];
        }
        self.pending = kept;
        self.handle(woken);
    }

    fn lose(&mut self, action: Action) -> Vec<Action> {
        let (group, generation, address, provisional) = match action {
            Action::Write(
                Record::Ack {
                    group,
                    generation,
                    address,
                    ..
                }
                | Record::Failure {
                    group,
                    generation,
                    address,
                    ..
                },
            ) => (group, generation, Some(address), false),
            Action::Dead(dead) => {
                self.dead
                    .remove(&(dead.group.clone(), dead.generation, dead.address));
                (dead.group, dead.generation, Some(dead.address), false)
            }
            Action::Write(Record::Forget {
                group,
                generation,
                provisional,
            }) => {
                if provisional && self.generation.get(&group) == Some(&generation) {
                    self.forgetting.remove(&group);
                }
                (group, generation, None, provisional)
            }
            _ => return Vec::new(),
        };
        if let Some(at) = address
            && let Some(lease) = self.leases.values_mut().find(|l| {
                l.group == group
                    && l.generation == generation
                    && l.settled.values().any(|a| *a == at)
            })
            && let Some(id) = lease
                .settled
                .iter()
                .find(|(_, a)| **a == at)
                .map(|(id, _)| *id)
        {
            lease.settled.remove(&id);
            lease.open.insert(id, at);
        }
        self.core
            .abandon(&group, generation, address.as_slice(), provisional)
    }

    fn drop_segment(&mut self) {
        let mut writing: BTreeSet<u32> = self
            .pending
            .iter()
            .filter_map(|a| match a {
                Action::Write(Record::Messages { segment, .. }) => Some(segment.seq),
                _ => None,
            })
            .collect();
        writing.insert(self.segment);
        let removable: Vec<SegmentId> = self
            .segments()
            .into_iter()
            .filter(|s| {
                !writing.contains(&s.seq)
                    && self.core.holders(*s, self.now, self.pressure).removable()
            })
            .collect();
        let Some(&segment) = removable.choose(&mut self.rng) else {
            return;
        };
        assert!(
            self.may_drop(segment),
            "retention: the core drops {segment:?} before the spec permits it"
        );
        self.dropped += 1;
        self.core.drop_segment(segment);
        self.ack_ms.retain(|(_, s), _| *s != segment);
        self.messages.retain(|at, _| at.segment != segment);
        self.log
            .retain(|(r, _)| !matches!(r, Record::Messages { segment: s, .. } if *s == segment));
        for lease in self.leases.values_mut() {
            lease.open.retain(|_, at| at.segment != segment);
            lease.settled.retain(|_, at| at.segment != segment);
        }
    }

    fn sync(&mut self) {
        let pending = std::mem::take(&mut self.pending);
        let mut marks = std::mem::take(&mut self.read_marks).into_iter().peekable();
        for (n, action) in pending.into_iter().enumerate() {
            while let Some((_, group, generation, selector)) = marks.next_if(|m| m.0 <= n) {
                self.read(group, generation, selector);
            }
            match action {
                Action::Write(record) => self.apply(record),
                Action::Dead(dead) => {
                    let (at, id) = self.place(1);
                    self.apply(Record::Messages {
                        stream: dead.dead_stream,
                        segment: at.segment,
                        first_position: at.position,
                        first_id: id,
                        count: 1,
                        timing: Vec::new(),
                    });
                    self.apply(Record::Ack {
                        group: dead.group,
                        generation: dead.generation,
                        address: dead.address,
                        stream: dead.stream,
                    });
                }
                Action::Wake(..) | Action::Await { .. } => {
                    unreachable!("wakes and waits are handled at once")
                }
            }
        }
        for (_, group, generation, selector) in marks {
            self.read(group, generation, selector);
        }
    }

    fn read(&mut self, group: String, generation: Generation, selector: String) {
        if self.forgotten.get(&group) < Some(&generation) {
            let reads = self.reads.entry(group).or_default();
            let kept = reads.entry(selector).or_insert(generation);
            *kept = (*kept).max(generation);
        }
    }

    fn reads_stream(&mut self, group: &str, generation: Generation, stream: &str) {
        if !stream.contains(".dead.") {
            self.read(group.to_owned(), generation, stream.to_owned());
        }
    }

    fn apply(&mut self, record: Record) {
        match &record {
            Record::Messages {
                stream,
                segment,
                first_position,
                first_id,
                count,
                timing,
            } => {
                for n in 0..*count {
                    let at = Address {
                        segment: *segment,
                        position: first_position + n,
                    };
                    let timing = timing
                        .iter()
                        .find(|(p, _)| *p == at.position)
                        .map(|(_, t)| *t)
                        .unwrap_or_default();
                    let id = nth_id(*first_id, n).unwrap();
                    let stream = stream.clone();
                    self.messages.insert(at, Message { stream, id, timing });
                }
            }
            Record::Ack {
                group,
                generation,
                address,
                stream,
            } => {
                self.settled(group, *address);
                self.reads_stream(group, *generation, stream);
                let last = self
                    .ack_ms
                    .entry((group.clone(), address.segment))
                    .or_insert(self.now);
                *last = (*last).max(self.now);
                self.acked.insert((group.clone(), *generation, *address));
                if self.forgotten.get(group) < Some(generation) {
                    let kept = self.acked_gen.entry(group.clone()).or_insert(*generation);
                    *kept = (*kept).max(*generation);
                    self.adopt(group, *generation);
                }
            }
            Record::Forget {
                group,
                generation,
                provisional,
            } => {
                self.forgetting.remove(group);
                if !(*provisional && self.acked_gen.get(group) == Some(generation)) {
                    let forgotten = self.forgotten.entry(group.clone()).or_insert(*generation);
                    *forgotten = (*forgotten).max(*generation);
                    let forgotten = *forgotten;
                    if self.acked_gen.get(group).is_some_and(|g| *g <= forgotten) {
                        self.acked_gen.remove(group);
                    }
                    if let Some(reads) = self.reads.get_mut(group) {
                        reads.retain(|_, g| *g > forgotten);
                    }
                    for current in [&mut self.generation, &mut self.durable] {
                        if current.get(group).is_some_and(|g| *g <= forgotten) {
                            current.remove(group);
                        }
                    }
                }
            }
            Record::Tombstone { stream, up_to } => {
                let kept = self.tombstones.entry(stream.clone()).or_insert(*up_to);
                *kept = (*kept).max(*up_to);
            }
            Record::Failure {
                group,
                generation,
                address,
                stream,
                count,
                not_before,
                ..
            } => {
                self.settled(group, *address);
                self.reads_stream(group, *generation, stream);
                let f = self
                    .failures
                    .entry((group.clone(), *generation, *address))
                    .or_default();
                *f = (*f).max((*count, *not_before));
                assert!(f.0 <= MAX_FAILURES, "a failure past the fifth");
                if f.0 == MAX_FAILURES {
                    self.fifth += 1;
                }
                self.adopt(group, *generation);
            }
            Record::Generation {
                group,
                generation,
                from,
                ..
            } => {
                self.from.insert((group.clone(), *generation), *from);
                self.adopt(group, *generation);
            }
            Record::Redrive {
                group,
                generation,
                stream,
                up_to,
            } => {
                let mark = self
                    .redrive
                    .entry((group.clone(), *generation, stream.clone()))
                    .or_insert(*up_to);
                *mark = (*mark).max(*up_to);
                self.adopt(group, *generation);
            }
            Record::ReadSet { .. } => {}
        }
        self.log.push((record.clone(), self.now));
        let actions = self.core.apply(record, self.now);
        let current = &self.generation;
        self.leases
            .retain(|_, l| current.get(&l.group) == Some(&l.generation));
        self.handle(actions);
    }

    fn settled(&mut self, group: &str, at: Address) {
        for lease in self.leases.values_mut().filter(|l| l.group == group) {
            lease.settled.retain(|_, a| *a != at);
        }
    }

    fn replay(&mut self, group: &str) {
        let used = self.used(group);
        let reserved = self.reserved.entry(group.to_owned()).or_default();
        *reserved = (*reserved).max(used) + 1;
        let expected = Generation {
            counter: *reserved,
            incarnation: INC + self.restarts,
        };
        let ids: Vec<MessageId> = self.messages.values().map(|m| m.id).collect();
        let from = match ids.choose(&mut self.rng) {
            Some(&id) if self.rng.random_bool(0.5) => Bound::Excluded(id),
            _ => {
                Bound::Included(MessageId::new(self.rng.random_range(0..=self.now), 0, 0).unwrap())
            }
        };
        let record = self.core.replay(group, from);
        assert!(
            matches!(&record, Record::Generation { generation, .. } if *generation == expected),
            "replay of {group}: expected {expected:?}, got {record:?}"
        );
        self.pending.push(Action::Write(record));
    }

    fn step(&mut self) {
        let group = GROUPS[self.rng.random_range(0..GROUPS.len())];
        let selector = SELECTORS[self.rng.random_range(0..SELECTORS.len())];
        match self.rng.random_range(0..100) {
            0..20 => self.publish(),
            20..40 => {
                let wait = self.rng.random_bool(0.3);
                self.take(group, selector, wait);
            }
            40..57 => self.settle(),
            57..68 => {
                self.sync();
                self.check_holders();
            }
            68..80 => {
                let by = self.rng.random_range(1..3_000);
                self.advance(by);
            }
            80..83 => self.release(),
            83..84 => self.replay(group),
            84..87 => {
                let records = self.core.redrive(group);
                self.pending.extend(records.into_iter().map(Action::Write));
            }
            87..88 => {
                if let Some(record) = self.core.forget(group) {
                    self.pending.push(Action::Write(record));
                }
            }
            88..89 => self.tombstone(),
            89..90 if self.rng.random_bool(0.25) => {
                self.restart();
                self.check_holders();
            }
            89..90 => {}
            90..92 => {
                let waiters: Vec<WaiterId> = self.waiters.keys().copied().collect();
                if let Some(&waiter) = waiters.choose(&mut self.rng) {
                    let (group, selector) = self.waiters.remove(&waiter).expect("chosen above");
                    self.core.cancel(waiter);
                    self.take(&group, &selector, false);
                }
            }
            92..95 => self.abandon(),
            95..96 => self.drop_segment(),
            96..98 => {
                let by = PROVISIONAL_MS - self.rng.random_range(0..60_000);
                self.advance(by);
            }
            _ => {
                self.pressure = !self.pressure;
                self.core.set_pressure(self.pressure);
            }
        }
        for group in GROUPS {
            assert_eq!(
                self.generation.get(group).copied(),
                self.core.generation(group),
                "generation of {group}"
            );
        }
        self.check_forgetting();
        self.check_waiters();
    }

    fn tombstone(&mut self) {
        let mut oldest: BTreeMap<&String, MessageId> = BTreeMap::new();
        for m in self.messages.values().filter(|m| !self.tombstoned(m)) {
            oldest.entry(&m.stream).or_insert(m.id);
        }
        let oldest: Vec<(&String, MessageId)> = oldest.into_iter().collect();
        if let Some(&(stream, up_to)) = oldest.choose(&mut self.rng) {
            let stream = stream.clone();
            self.pending
                .push(Action::Write(Record::Tombstone { stream, up_to }));
        }
    }

    fn restart(&mut self) {
        if self.rng.random_bool(0.5) {
            self.sync();
        }
        for action in std::mem::take(&mut self.pending) {
            if let Action::Dead(dead) = action {
                self.dead
                    .remove(&(dead.group, dead.generation, dead.address));
            }
        }
        self.restarts += 1;
        let seed = self.rng.random();
        let folded = self.fold(seed, INC + self.restarts);
        self.check_fold(&folded);
        self.core = folded;
        self.core.set_pressure(self.pressure);
        self.generation = self.durable.clone();
        self.reserved.clear();
        self.forgetting.clear();
        self.read_marks.clear();
        self.leases.clear();
        self.waiters.clear();
        self.expiries.clear();
    }

    fn drain(&mut self) {
        self.pressure = false;
        self.core.set_pressure(false);
        for _ in 0..20 {
            self.sync();
            self.now += 60_000;
            let actions = self.core.tick(self.now);
            self.leases.clear();
            self.handle(actions);
        }
        self.sync();
        for ((group, generation, at), (count, _)) in &self.failures {
            if *count >= MAX_FAILURES
                && self.generation.get(group) == Some(generation)
                && self.messages.contains_key(at)
            {
                assert!(
                    self.acked.contains(&(group.clone(), *generation, *at)),
                    "fifth failure of {at:?} in {group} has no dead copy"
                );
            }
        }
    }

    fn fold(&self, seed: u64, incarnation: u64) -> Core {
        let mut log = self.log.clone();
        log.shuffle(&mut ChaCha8Rng::seed_from_u64(seed));
        let mut core = Core::new(incarnation);
        for (record, at) in log {
            core.apply(record, at);
        }
        core
    }
}

fn run(seed: u64) -> Sim {
    let mut sim = Sim::new(seed);
    for _ in 0..STEPS {
        sim.step();
    }
    sim.drain();
    let first = sim.fold(seed, INC);
    let second = sim.fold(seed ^ 0xffff, INC);
    assert!(
        first.state() == second.state(),
        "I12: the fold depends on the record order"
    );
    sim.check_fold(&first);
    sim
}

#[test]
fn core_sim_hand_out_and_settle_keep_the_rules_over_100_seeds() {
    let (mut handed, mut dead, mut fifth, mut restarts) = (0, 0, 0, 0);
    let (mut lost, mut dropped, mut forgets) = (0, 0, 0);
    for seed in 0..SEEDS {
        let sim = run(seed);
        handed += sim.handed;
        dead += sim.dead.len();
        fifth += sim.fifth;
        restarts += sim.restarts;
        lost += sim.lost;
        dropped += sim.dropped;
        forgets += sim.forgets;
    }
    println!(
        "core_sim seeds={SEEDS} handed={handed} dead={dead} fifth={fifth} restarts={restarts} lost={lost} dropped={dropped} forgets={forgets}"
    );
    assert!(handed > 1_000, "the sim hands out too little: {handed}");
    assert!(dead > 50, "the sim makes too few dead copies: {dead}");
    assert!(
        fifth > 20,
        "the sim reaches the fifth failure too rarely: {fifth}"
    );
    assert!(restarts > 50, "the sim restarts too rarely: {restarts}");
    assert!(lost > 100, "the sim abandons too few records: {lost}");
    assert!(dropped > 10, "the sim drops too few segments: {dropped}");
    assert!(
        forgets > 10,
        "the sim makes too few provisional forgets: {forgets}"
    );
}
