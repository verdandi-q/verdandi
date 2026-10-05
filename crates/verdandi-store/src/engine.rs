use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ring::rand::{SecureRandom, SystemRandom};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::MissedTickBehavior;
use verdandi_core::group::{GroupStats, Handed, TakeOutcome, TakeRequest};
use verdandi_core::ids::{Address, MessageId, SegmentId};
use verdandi_core::lease::{Receipt, Settle, SettleError};
use verdandi_core::{
    Action, Core, DeadCopy, Generation, Reason, Record, Timing, WaiterId, names, nth_id, retention,
};

use crate::block::{BLOCK_HEADER_LEN, BlockHeader, FILE_HEADER_LEN, MAX_BLOCK, SEGMENT_BYTES};
use crate::compaction::{Compactor, Job, Keep, Output, Report, Scanned};
use crate::fs::{Fs, FsFile, FsStat};
use crate::index::{self, Index, Location, ReadError, Run};
use crate::log::{self, Log, SealPoint, Written, fits, seal_at, segment_file_name};
use crate::records::{self, Control, Damage, Message, Origin, RunEntry};
use crate::recovery::{Warning, recover};
use crate::retention::{FileInfo, Holding, RESCAN_MS, SWEEP_MS, carries, select};

pub const IDLE_SEAL_MS: u64 = 30_000;
pub const KEY_MS: u64 = 24 * 3600 * 1000;
pub const CHANNEL: usize = 1024;
pub const BULK: &str = "/bulk";
pub const DEAD_PER_PASS: usize = 16;
pub const PRESSURE_HOLD_MS: u64 = 10_000;

const TICK: Duration = Duration::from_millis(100);
const PRUNE_MS: u64 = 60_000;
const RATE_MS: u64 = 60_000;
const BLOCK_CAP: usize = MAX_BLOCK - BLOCK_HEADER_LEN;
const EMPTY_BODY: usize = 4;
const QUEUE_CAP: usize = 64 << 20;

pub trait Clock: Send + Sync + 'static {
    fn now_ms(&self) -> u64;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct WallClock;

impl Clock for WallClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64)
    }
}

pub trait Random: Send + 'static {
    fn fill(&mut self, bytes: &mut [u8]) -> Result<(), Error>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct OsRandom;

impl Random for OsRandom {
    fn fill(&mut self, bytes: &mut [u8]) -> Result<(), Error> {
        SystemRandom::new().fill(bytes).map_err(|_| Error::Random)
    }
}

#[derive(Clone, Debug, Default)]
pub struct TestClock(Arc<AtomicU64>);

impl TestClock {
    pub fn new(ms: u64) -> Self {
        Self(Arc::new(AtomicU64::new(ms)))
    }

    pub fn set(&self, ms: u64) {
        self.0.store(ms, Ordering::SeqCst);
    }

    pub fn advance(&self, ms: u64) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("no space left on the device")]
    NoSpace,
    #[error("the node stopped after an I/O error")]
    Stopped,
    #[error("the stored records of one request are over the 32 MiB block cap")]
    TooLarge,
    #[error("a publish holds no message")]
    Empty,
    #[error("the stream name is not valid")]
    BadName,
    #[error(
        "a header or an idempotency key is over 65,535 bytes, or a message has over 65,535 headers"
    )]
    TooLong,
    #[error("the settle was refused: {0:?}")]
    Refused(SettleError),
    #[error("the system random source failed")]
    Random,
    #[error("a run of the log could not be read: {0}")]
    Unreadable(String),
    #[error("no group with this name exists")]
    NoGroup,
    #[error("a replay of this group waits for its block")]
    Replaying,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewMessage {
    pub stream: String,
    pub headers: Vec<(String, String)>,
    pub value: Vec<u8>,
    pub timing: Timing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Key {
    pub scope: String,
    pub key: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Published {
    pub first: MessageId,
    pub count: u32,
    pub repeat: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Take {
    pub group: String,
    pub selector: String,
    pub limit: usize,
    pub lease_ms: u64,
    pub wait: bool,
}

#[derive(Clone)]
pub struct Delivery {
    pub handed: Handed,
    pub location: Option<Location>,
}

pub enum Taken {
    Leased {
        receipt: Receipt,
        until: u64,
        messages: Vec<Delivery>,
    },
    Waiting {
        waiter: WaiterId,
        woken: oneshot::Receiver<()>,
    },
    Empty,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SettleBy {
    Ids(Settle),
    All,
}

type Reply<T> = oneshot::Sender<Result<T, Error>>;
type Done = Box<dyn FnOnce(Result<(), Error>) + Send>;
type Cached = Option<(Address, Result<Vec<Message>, String>)>;

enum Command {
    Publish {
        messages: Vec<NewMessage>,
        key: Option<Key>,
        reply: Reply<Published>,
    },
    Take {
        take: Take,
        reply: Reply<Taken>,
    },
    Settle {
        receipt: Receipt,
        by: SettleBy,
        reply: Reply<()>,
    },
    Release {
        receipt: Receipt,
        ids: Vec<MessageId>,
        reply: Reply<()>,
    },
    Cancel(WaiterId),
    Replay {
        group: String,
        from: Option<Bound<MessageId>>,
        reply: Reply<()>,
    },
    Redrive {
        group: String,
        reply: Reply<()>,
    },
    Forget {
        group: String,
        reply: Reply<()>,
    },
    DeleteStream {
        stream: String,
        reply: Reply<MessageId>,
    },
    Damaged(Address, u64),
    Damage(Reply<(Vec<Damage>, Vec<Warning>)>),
    Dropped(Reply<Vec<DeadCopy>>),
    Holding(Reply<Holding>),
    Status(Reply<Status>),
    Compact(Reply<()>),
    Stats {
        group: String,
        reply: Reply<Option<GroupStats>>,
    },
    Scan {
        stream: String,
        from: Bound<MessageId>,
        limit: usize,
        reply: Reply<Vec<Delivery>>,
    },
    Issued(Reply<MessageId>),
    Close,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Status {
    pub disk: Option<FsStat>,
    pub write_bytes_per_s: u64,
    pub streams: Vec<String>,
    pub groups: Vec<(String, bool)>,
    pub damage: Vec<Damage>,
    pub damaged: BTreeMap<SegmentId, Vec<String>>,
    pub unmapped: Vec<Warning>,
    pub dropped: Vec<DeadCopy>,
}

#[derive(Clone)]
pub struct Handle {
    tx: mpsc::Sender<Command>,
    clock: Arc<dyn Clock>,
    applied: watch::Receiver<(u64, Vec<String>)>,
}

impl Handle {
    pub fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }

    async fn call<T>(&self, command: impl FnOnce(Reply<T>) -> Command) -> Result<T, Error> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(command(reply))
            .await
            .map_err(|_| Error::Stopped)?;
        rx.await.unwrap_or(Err(Error::Stopped))
    }

    pub async fn publish(
        &self,
        messages: Vec<NewMessage>,
        key: Option<Key>,
    ) -> Result<Published, Error> {
        self.call(|reply| Command::Publish {
            messages,
            key,
            reply,
        })
        .await
    }

    pub async fn take(&self, take: Take) -> Result<Taken, Error> {
        (names::valid_group(&take.group)
            && (names::valid_pattern(&take.selector) || names::valid_name(&take.selector)))
        .then_some(())
        .ok_or(Error::BadName)?;
        self.call(|reply| Command::Take { take, reply }).await
    }

    pub async fn settle(&self, receipt: Receipt, by: SettleBy) -> Result<(), Error> {
        self.call(|reply| Command::Settle { receipt, by, reply })
            .await
    }

    pub async fn release(&self, receipt: Receipt, ids: Vec<MessageId>) -> Result<(), Error> {
        self.call(|reply| Command::Release {
            receipt,
            ids,
            reply,
        })
        .await
    }

    pub async fn cancel(&self, waiter: WaiterId) -> Result<(), Error> {
        self.tx
            .send(Command::Cancel(waiter))
            .await
            .map_err(|_| Error::Stopped)
    }

    pub async fn replay(&self, group: String, from: Bound<MessageId>) -> Result<(), Error> {
        group_name(&group)?;
        self.call(|reply| Command::Replay {
            group,
            from: Some(from),
            reply,
        })
        .await
    }

    pub async fn replay_now(&self, group: String) -> Result<(), Error> {
        group_name(&group)?;
        self.call(|reply| Command::Replay {
            group,
            from: None,
            reply,
        })
        .await
    }

    pub async fn redrive(&self, group: String) -> Result<(), Error> {
        group_name(&group)?;
        self.call(|reply| Command::Redrive { group, reply }).await
    }

    pub async fn forget(&self, group: String) -> Result<(), Error> {
        group_name(&group)?;
        self.call(|reply| Command::Forget { group, reply }).await
    }

    pub async fn delete_stream(&self, stream: String) -> Result<MessageId, Error> {
        (names::valid_name(&stream) && !stream.starts_with('_'))
            .then_some(())
            .ok_or(Error::BadName)?;
        self.call(|reply| Command::DeleteStream { stream, reply })
            .await
    }

    pub async fn damage(&self) -> Result<(Vec<Damage>, Vec<Warning>), Error> {
        self.call(Command::Damage).await
    }

    pub async fn dropped(&self) -> Result<Vec<DeadCopy>, Error> {
        self.call(Command::Dropped).await
    }

    pub async fn holding(&self) -> Result<Holding, Error> {
        self.call(Command::Holding).await
    }

    pub async fn status(&self) -> Result<Status, Error> {
        self.call(Command::Status).await
    }

    pub async fn group_stats(&self, group: String) -> Result<Option<GroupStats>, Error> {
        group_name(&group)?;
        self.call(|reply| Command::Stats { group, reply }).await
    }

    pub async fn compact(&self) -> Result<(), Error> {
        self.call(Command::Compact).await
    }

    pub async fn scan(
        &self,
        stream: String,
        from: Bound<MessageId>,
        limit: usize,
    ) -> Result<Vec<Delivery>, Error> {
        names::valid_name(&stream)
            .then_some(())
            .ok_or(Error::BadName)?;
        self.call(|reply| Command::Scan {
            stream,
            from,
            limit,
            reply,
        })
        .await
    }

    pub async fn issued(&self) -> Result<MessageId, Error> {
        self.call(Command::Issued).await
    }

    pub fn applied(&self) -> watch::Receiver<(u64, Vec<String>)> {
        self.applied.clone()
    }

    pub async fn close(&self) -> Result<(), Error> {
        self.tx
            .send(Command::Close)
            .await
            .map_err(|_| Error::Stopped)
    }

    pub async fn read(&self, deliveries: &[Delivery]) -> Result<Vec<Option<Message>>, Error> {
        let mut run: Option<(Address, Vec<Message>)> = None;
        let mut out = Vec::with_capacity(deliveries.len());
        for delivery in deliveries {
            let Some(location) = &delivery.location else {
                out.push(None);
                continue;
            };
            if run
                .as_ref()
                .is_none_or(|(start, _)| *start != location.start)
            {
                run.take();
                let owned = location.clone();
                let read = tokio::task::spawn_blocking(move || index::read(&owned))
                    .await
                    .map_err(|e| Error::Unreadable(e.to_string()))?;
                let messages = match read {
                    Ok(messages) => messages,
                    Err(ReadError::Damaged) => {
                        self.tx
                            .send(Command::Damaged(location.start, location.run.offset))
                            .await
                            .map_err(|_| Error::Stopped)?;
                        Vec::new()
                    }
                    Err(ReadError::Io(e)) => return Err(Error::Unreadable(e.to_string())),
                };
                run = Some((location.start, messages));
            }
            let position = delivery.handed.address.position;
            let offset = (position - location.start.position) as usize;
            out.push(
                run.as_ref()
                    .and_then(|(_, messages)| messages.get(offset))
                    .filter(|m| m.position == position)
                    .cloned(),
            );
        }
        Ok(out)
    }
}

fn group_name(group: &str) -> Result<(), Error> {
    names::valid_group(group)
        .then_some(())
        .ok_or(Error::BadName)
}

struct Kept {
    first: MessageId,
    count: u32,
    at: u64,
}

struct Cursor {
    end: u64,
    next_position: u32,
}

struct Pending {
    messages: Vec<(String, Message)>,
    controls: Vec<Control>,
    tickets: Vec<u64>,
    bytes: usize,
}

impl Pending {
    fn new() -> Self {
        Self {
            messages: Vec::new(),
            controls: Vec::new(),
            tickets: Vec::new(),
            bytes: EMPTY_BODY,
        }
    }
}

struct Landing {
    rx: oneshot::Receiver<Result<Written, log::Error>>,
    runs: Vec<(String, Vec<Message>)>,
    entries: Vec<RunEntry>,
    controls: Vec<Control>,
    tickets: Vec<u64>,
    end: u32,
    total: u64,
    commit_ms: u64,
}

enum Flight {
    Block(Box<Landing>),
    Seal(oneshot::Receiver<Result<(), log::Error>>),
}

struct Ticket {
    remaining: usize,
    error: Option<Error>,
    done: Done,
}

pub struct Engine<F: Fs> {
    fs: F,
    dir: PathBuf,
    clock: Arc<dyn Clock>,
    random: Box<dyn Random>,
    incarnation: u64,
    core: Core,
    log: Option<Log>,
    rx: mpsc::Receiver<Command>,
    index: Index,
    last_id: (u64, u16),
    cursor: Option<Cursor>,
    queue: VecDeque<Pending>,
    flight: Option<Flight>,
    tickets: BTreeMap<u64, Ticket>,
    next_ticket: u64,
    awaits: BTreeMap<(String, Address), Vec<u64>>,
    waiters: BTreeMap<WaiterId, (String, oneshot::Sender<()>)>,
    replaying: BTreeMap<String, usize>,
    applied: watch::Sender<(u64, Vec<String>)>,
    keys: BTreeMap<(String, String), Kept>,
    keys_in_flight: BTreeMap<(String, String), Vec<Reply<Published>>>,
    damage: BTreeSet<Damage>,
    unwritten: BTreeSet<Damage>,
    unmapped: Vec<Warning>,
    tail: Option<(Vec<Damage>, Vec<SealPoint>)>,
    full: bool,
    floor: bool,
    enospc: Option<u64>,
    dropped: Vec<DeadCopy>,
    burying: VecDeque<(u64, MessageId, VecDeque<DeadCopy>)>,
    burying_bytes: usize,
    buried: Cached,
    deleting: usize,
    compactor: Option<Compactor>,
    compacting: bool,
    pass_due: bool,
    changes: u64,
    passed: u64,
    waiting: Vec<Reply<()>>,
    running: Vec<Reply<()>>,
    files: BTreeMap<PathBuf, FileInfo>,
    owner: BTreeMap<SegmentId, PathBuf>,
    last_rescan: u64,
    last_sweep: u64,
    active: Option<SegmentId>,
    last_write: u64,
    last_tick: Option<u64>,
    last_prune: u64,
    written: u64,
    marks: [(u64, u64); 2],
    stopped: bool,
}

#[derive(Default)]
struct Fold {
    runs: Vec<(PathBuf, Address, Run, Record, u64)>,
    first: BTreeMap<PathBuf, (u64, u64)>,
    controls: BTreeSet<PathBuf>,
    max_id: Option<MessageId>,
    keys: BTreeMap<(String, String), Kept>,
    damage: BTreeSet<Damage>,
    malformed: Vec<Warning>,
    blocks: BTreeMap<(PathBuf, u64), SegmentId>,
}

impl Fold {
    fn seen(&mut self, id: MessageId) {
        self.max_id = self.max_id.max(Some(id));
    }

    fn visit(&mut self, core: &mut Core, path: &Path, header: &BlockHeader, body: &[u8]) {
        self.blocks
            .insert((path.to_path_buf(), header.offset), header.segment);
        let (first, bodies) = self
            .first
            .entry(path.to_path_buf())
            .or_insert((header.commit_ms, 0));
        *first = (*first).min(header.commit_ms);
        *bodies += body.len() as u64;
        let malformed = Warning {
            file: path.to_path_buf(),
            range: header.offset..header.offset + (BLOCK_HEADER_LEN + body.len()) as u64,
        };
        let Ok(body) = records::decode(body) else {
            self.malformed.push(malformed);
            return;
        };
        if !body.controls.is_empty() {
            self.controls.insert(path.to_path_buf());
        }
        let mut runs = Vec::with_capacity(body.runs.len());
        for (entry, bytes) in &body.runs {
            let Ok(messages) = records::decode_run(bytes, entry.count) else {
                self.malformed.push(malformed);
                return;
            };
            runs.push((entry, messages));
        }
        for (entry, messages) in runs {
            for m in &messages {
                self.seen(m.id);
                if let Some(origin) = &m.origin {
                    self.seen(origin.id);
                }
            }
            let start = Address {
                segment: header.segment,
                position: messages[0].position,
            };
            let run = run_of(entry, &messages, header.offset);
            let record = messages_record(&entry.stream, start, &messages);
            self.runs
                .push((path.to_path_buf(), start, run, record, header.commit_ms));
        }
        for control in body.controls {
            match control {
                Control::Record(record) => {
                    match &record {
                        Record::Tombstone { up_to, .. } | Record::Redrive { up_to, .. } => {
                            self.seen(*up_to);
                        }
                        Record::Generation {
                            from: Bound::Excluded(up_to),
                            issued,
                            ..
                        } if *issued || up_to.ms() <= header.commit_ms => self.seen(*up_to),
                        _ => {}
                    }
                    core.apply(record, header.commit_ms);
                }
                Control::Key {
                    scope,
                    key,
                    first,
                    count,
                } => {
                    if let Some(last) = nth_id(first, count.saturating_sub(1)) {
                        self.seen(last);
                    }
                    let kept = Kept {
                        first,
                        count,
                        at: header.commit_ms,
                    };
                    match self.keys.get(&(scope.clone(), key.clone())) {
                        Some(old) if old.at >= kept.at => {}
                        _ => {
                            self.keys.insert((scope, key), kept);
                        }
                    }
                }
                Control::Damage(d) => {
                    self.damage.insert(d);
                }
            }
        }
    }
}

fn run_of(entry: &RunEntry, messages: &[Message], block_offset: u64) -> Run {
    Run {
        stream: entry.stream.clone(),
        offset: block_offset + (BLOCK_HEADER_LEN as u64) + u64::from(entry.offset),
        len: entry.len,
        count: entry.count,
        crc: entry.crc,
        first_id: messages[0].id,
        last_id: messages[messages.len() - 1].id,
    }
}

fn messages_record(stream: &str, start: Address, messages: &[Message]) -> Record {
    Record::Messages {
        stream: stream.to_owned(),
        segment: start.segment,
        first_position: start.position,
        first_id: messages[0].id,
        count: messages.len() as u32,
        timing: messages
            .iter()
            .filter(|m| m.timing != Timing::default())
            .map(|m| (m.position, m.timing))
            .collect(),
    }
}

fn segment_of(file: &Path) -> Option<SegmentId> {
    file.file_name()?
        .to_str()?
        .strip_prefix("seg-")?
        .strip_suffix(".vdl")?
        .parse()
        .ok()
}

fn segments_at(
    blocks: &BTreeMap<(PathBuf, u64), SegmentId>,
    warning: &Warning,
) -> BTreeSet<SegmentId> {
    if let Some(segment) = segment_of(&warning.file) {
        return BTreeSet::from([segment]);
    }
    let first = (warning.file.clone(), 0);
    let last = (warning.file.clone(), u64::MAX);
    let at = (warning.file.clone(), warning.range.start);
    let before = blocks.range(first.clone()..at.clone()).next_back();
    match (before, blocks.range(at.clone()..=last.clone()).next()) {
        (_, Some((key, segment))) if *key == at => BTreeSet::from([*segment]),
        (Some((_, a)), Some((_, b))) if a == b => BTreeSet::from([*a]),
        _ => blocks.range(first..=last).map(|(_, s)| *s).collect(),
    }
}

fn random<const N: usize>(source: &mut dyn Random) -> Result<[u8; N], Error> {
    let mut bytes = [0; N];
    source.fill(&mut bytes)?;
    Ok(bytes)
}

fn engine_error(error: &log::Error) -> Error {
    match error {
        log::Error::NoSpace => Error::NoSpace,
        _ => Error::Stopped,
    }
}

async fn reported(compactor: &mut Option<Compactor>) -> Option<Report> {
    match compactor {
        Some(c) => c.reports.recv().await,
        None => std::future::pending().await,
    }
}

async fn land(slot: &mut Option<Flight>) -> (Flight, Result<Option<Written>, log::Error>) {
    let result = match slot {
        Some(Flight::Block(landing)) => (&mut landing.rx).await.map(|r| r.map(Some)),
        Some(Flight::Seal(rx)) => rx.await.map(|r| r.map(|()| None)),
        None => std::future::pending().await,
    };
    match slot.take() {
        Some(flight) => (flight, result.unwrap_or(Err(log::Error::Stopped))),
        None => std::future::pending().await,
    }
}

impl<F: Fs + Clone> Engine<F> {
    pub async fn open(
        fs: F,
        dir: &Path,
        clock: Arc<dyn Clock>,
        mut source: Box<dyn Random>,
    ) -> Result<(Self, Handle), log::Error> {
        let incarnation = u64::from_le_bytes(
            random(source.as_mut()).map_err(|e| log::Error::Io(std::io::Error::other(e)))?,
        );
        let mut core = Core::new(incarnation);
        let mut fold = Fold::default();
        let mut recovered = recover(&fs, dir, |path, header, body| {
            fold.visit(&mut core, path, header, body)
        })?;
        let mut warnings = std::mem::take(&mut recovered.warnings);
        let unsealed = std::mem::take(&mut recovered.unsealed);
        let winners = std::mem::take(&mut recovered.winners);
        fold.malformed
            .retain(|w| winners.values().any(|path| *path == w.file));
        warnings.append(&mut fold.malformed);
        let log = Log::start(fs.clone(), incarnation, recovered);
        let compactor = Compactor::start(fs.clone(), dir.to_path_buf(), log.epochs())?;
        let mut index = Index::default();
        let mut opened: BTreeMap<PathBuf, Arc<dyn FsFile>> = BTreeMap::new();
        let mut run_bytes: BTreeMap<PathBuf, u64> = BTreeMap::new();
        for (path, start, run, record, commit_ms) in fold.runs {
            if winners.get(&start.segment) != Some(&path) {
                continue;
            }
            let file = match opened.get(&path) {
                Some(file) => Arc::clone(file),
                None => {
                    let file: Arc<dyn FsFile> = Arc::new(fs.open(&path)?);
                    opened.insert(path.clone(), Arc::clone(&file));
                    file
                }
            };
            *run_bytes.entry(path).or_default() += u64::from(run.len);
            core.apply(record, commit_ms);
            index.add(start.segment, file);
            index.insert(start, run);
        }
        let mut held: BTreeMap<&Path, BTreeSet<SegmentId>> = BTreeMap::new();
        for ((path, _), segment) in &fold.blocks {
            held.entry(path).or_default().insert(*segment);
        }
        let mut files = BTreeMap::new();
        for path in winners.values() {
            if files.contains_key(path) {
                continue;
            }
            let len = fs.open(path)?.size()?;
            let compacted = path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("cmp-"));
            let runs = run_bytes.get(path).copied().unwrap_or(0);
            let (first_ms, bodies) = fold.first.get(path).copied().unwrap_or_default();
            let info = FileInfo {
                len,
                bodies,
                carried: if compacted {
                    bodies.saturating_sub(runs)
                } else {
                    0
                },
                first_ms,
                controls: fold.controls.contains(path),
                sealed: !unsealed.iter().any(|p| p.file == *path),
                segments: held.remove(path.as_path()).unwrap_or_default(),
            };
            files.insert(path.clone(), info);
        }
        let (tx, rx) = mpsc::channel(CHANNEL);
        let (applied, watched) = watch::channel((0, Vec::new()));
        let floor = fold.max_id.map_or((0, u16::MAX), |id| (id.ms(), u16::MAX));
        let mut engine = Self {
            fs: fs.clone(),
            dir: dir.to_path_buf(),
            clock: Arc::clone(&clock),
            random: source,
            incarnation,
            core,
            log: Some(log),
            rx,
            index,
            last_id: floor,
            cursor: None,
            queue: VecDeque::new(),
            flight: None,
            tickets: BTreeMap::new(),
            next_ticket: 0,
            awaits: BTreeMap::new(),
            waiters: BTreeMap::new(),
            replaying: BTreeMap::new(),
            applied,
            keys: fold.keys,
            keys_in_flight: BTreeMap::new(),
            damage: fold.damage,
            unwritten: BTreeSet::new(),
            unmapped: Vec::new(),
            tail: None,
            full: false,
            floor: false,
            enospc: None,
            dropped: Vec::new(),
            burying: VecDeque::new(),
            burying_bytes: 0,
            buried: None,
            deleting: 0,
            compactor: Some(compactor),
            compacting: false,
            pass_due: false,
            changes: 0,
            passed: 0,
            waiting: Vec::new(),
            running: Vec::new(),
            files,
            owner: winners,
            last_rescan: clock.now_ms(),
            last_sweep: clock.now_ms(),
            active: None,
            last_write: 0,
            last_tick: None,
            last_prune: 0,
            written: 0,
            marks: [(clock.now_ms(), 0); 2],
            stopped: false,
        };
        let fresh = engine.fresh_damage(&warnings, &fold.blocks);
        let mut seal_now = unsealed;
        match engine.record_damage(&fresh).await {
            Ok(()) => {}
            Err(log::Error::NoSpace) => {
                let (later, now): (Vec<SealPoint>, Vec<SealPoint>) = seal_now
                    .into_iter()
                    .partition(|p| warnings.iter().any(|w| w.file == p.file));
                seal_now = now;
                for point in &later {
                    let end = warnings
                        .iter()
                        .filter(|w| w.file == point.file)
                        .fold(point.end, |end, w| end.max(w.range.end));
                    seal_at(
                        &fs,
                        &SealPoint {
                            file: point.file.clone(),
                            end,
                        },
                    )?;
                }
                engine.set_pressure(true);
                engine.unwritten.extend(fresh.iter().copied());
                engine.tail = Some((fresh, later));
            }
            Err(e) => return Err(e),
        }
        for point in &seal_now {
            seal_at(&fs, point)?;
            if let Some(info) = engine.files.get_mut(&point.file) {
                info.len = point.end;
                info.sealed = true;
            }
        }
        Ok((
            engine,
            Handle {
                tx,
                clock,
                applied: watched,
            },
        ))
    }

    fn fresh_damage(
        &mut self,
        warnings: &[Warning],
        blocks: &BTreeMap<(PathBuf, u64), SegmentId>,
    ) -> Vec<Damage> {
        let mut fresh = BTreeSet::new();
        for warning in warnings {
            let segments = segments_at(blocks, warning);
            if segments.is_empty() {
                self.unmapped.push(warning.clone());
            }
            fresh.extend(segments.into_iter().map(|segment| Damage {
                segment,
                start: warning.range.start,
                end: warning.range.end,
            }));
        }
        fresh.retain(|d| !self.damage.contains(d));
        fresh.into_iter().collect()
    }

    async fn record_damage(&mut self, fresh: &[Damage]) -> Result<(), log::Error> {
        let Some(log) = self.log.as_ref().filter(|_| !fresh.is_empty()) else {
            return Ok(());
        };
        let controls: Vec<Control> = fresh.iter().copied().map(Control::Damage).collect();
        let (body, _) = records::encode(&[], &controls);
        let total = (BLOCK_HEADER_LEN + body.len()) as u64;
        let now = self.clock.now_ms();
        let mut last = None;
        for _ in 0..2 {
            let written = log
                .append(now, body.clone(), SEGMENT_BYTES)
                .await
                .unwrap_or(Err(log::Error::Stopped))?;
            last = Some(written);
        }
        if let Some(written) = last {
            self.written += total;
            self.cursor = Some(Cursor {
                end: written.offset + total,
                next_position: 0,
            });
            self.wrote(
                written.segment,
                written.offset + total,
                body.len() as u64,
                now,
                true,
            );
        }
        self.last_write = now;
        self.damage.extend(fresh.iter().copied());
        Ok(())
    }

    pub async fn run(mut self) -> Result<(), Error> {
        let mut ticker = tokio::time::interval(TICK);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut open = true;
        while !self.stopped
            && (open || self.flight.is_some() || !self.queue.is_empty() || !self.burying.is_empty())
        {
            self.bury();
            self.flush();
            self.pass();
            tokio::select! {
                command = self.rx.recv(), if open && self.queued() < QUEUE_CAP && self.burying_bytes < QUEUE_CAP => match command {
                    Some(command) => {
                        self.tick();
                        self.command(command);
                    }
                    None => {
                        open = false;
                        self.abandon_burying(Error::Stopped);
                    }
                },
                (flight, result) = land(&mut self.flight) => self.landed(flight, result),
                report = reported(&mut self.compactor) => self.report(report),
                _ = ticker.tick(), if open => self.tick(),
            }
        }
        if let Some(compactor) = self.compactor.take() {
            compactor.close().await;
        }
        if let Some(log) = self.log.take() {
            log.close().await;
        }
        if self.stopped {
            Err(Error::Stopped)
        } else {
            Ok(())
        }
    }

    fn command(&mut self, command: Command) {
        match command {
            Command::Publish {
                messages,
                key,
                reply,
            } => self.publish(messages, key, reply),
            Command::Take { take, reply } => {
                if let Err(Ok(Taken::Leased {
                    receipt, messages, ..
                })) = reply.send(self.take(take))
                {
                    let ids: Vec<MessageId> = messages.iter().map(|d| d.handed.id).collect();
                    let actions = self.core.release(receipt, &ids);
                    self.act(actions, None);
                }
            }
            Command::Settle { receipt, by, reply } => self.settle(receipt, by, reply),
            Command::Release {
                receipt,
                ids,
                reply,
            } => {
                let actions = self.core.release(receipt, &ids);
                self.act(actions, None);
                reply.send(Ok(())).ok();
            }
            Command::Cancel(waiter) => {
                self.core.cancel(waiter);
                self.waiters.remove(&waiter);
            }
            Command::Replay { group, from, reply } => {
                let issued = from.is_none();
                let from = from.unwrap_or_else(|| {
                    let now = self.clock.now_ms();
                    Bound::Excluded(self.next_id(now))
                });
                let mut record = self.core.replay(&group, from);
                if let Record::Generation { issued: flag, .. } = &mut record {
                    *flag = issued;
                }
                *self.replaying.entry(group).or_default() += 1;
                self.write(vec![record], reply);
            }
            Command::Redrive { group, reply } if self.core.generation(&group).is_none() => {
                reply.send(Err(Error::NoGroup)).ok();
            }
            Command::Redrive { group, reply } if self.replaying.contains_key(&group) => {
                reply.send(Err(Error::Replaying)).ok();
            }
            Command::Redrive { group, reply } => {
                let records = self.core.redrive(&group);
                self.write(records, reply);
            }
            Command::Forget { group, reply } => match self.core.forget(&group) {
                Some(record) => self.write(vec![record], reply),
                None => {
                    reply.send(Err(Error::NoGroup)).ok();
                }
            },
            Command::DeleteStream { stream, reply } => {
                let now = self.clock.now_ms();
                let up_to = self.next_id(now);
                let ticket = self.ticket(Box::new(move |result| {
                    reply.send(result.map(|()| up_to)).ok();
                }));
                let record = Record::Tombstone { stream, up_to };
                self.act(vec![Action::Write(record)], Some(ticket));
                self.release_ticket(ticket, Ok(()));
            }
            Command::Damaged(start, offset) => self.damaged(start, offset),
            Command::Damage(reply) => {
                let damage = self.damage.iter().copied().collect();
                reply.send(Ok((damage, self.unmapped.clone()))).ok();
            }
            Command::Dropped(reply) => {
                reply.send(Ok(self.dropped.clone())).ok();
            }
            Command::Holding(reply) => {
                reply.send(Ok(self.holding())).ok();
            }
            Command::Status(reply) => {
                reply.send(Ok(self.status())).ok();
            }
            Command::Stats { group, reply } => {
                reply
                    .send(Ok(self.core.group_stats(&group, self.clock.now_ms())))
                    .ok();
            }
            Command::Compact(reply) => {
                self.pass_due = true;
                self.waiting.push(reply);
            }
            Command::Scan {
                stream,
                from,
                limit,
                reply,
            } => {
                let found = self
                    .core
                    .history(&stream, from, limit)
                    .into_iter()
                    .map(|(id, address)| Delivery {
                        location: self.index.locate(address),
                        handed: Handed {
                            address,
                            id,
                            stream: stream.clone(),
                            attempt: 0,
                        },
                    })
                    .collect();
                reply.send(Ok(found)).ok();
            }
            Command::Issued(reply) => {
                let (ms, counter) = self.last_id;
                let incarnation = if counter == u16::MAX {
                    u64::MAX
                } else {
                    self.incarnation
                };
                let id =
                    MessageId::new(ms, incarnation, counter).expect("the clock is inside 48 bits");
                reply.send(Ok(id)).ok();
            }
            Command::Close => self.rx.close(),
        }
    }

    fn write(&mut self, records: Vec<Record>, reply: Reply<()>) {
        let ticket = self.ticket(Box::new(move |result| {
            reply.send(result).ok();
        }));
        self.act(
            records.into_iter().map(Action::Write).collect(),
            Some(ticket),
        );
        self.release_ticket(ticket, Ok(()));
    }

    fn next_id(&mut self, now: u64) -> MessageId {
        let (ms, counter) = self.last_id;
        self.last_id = if now > ms {
            (now, 0)
        } else if counter == u16::MAX {
            (ms + 1, 0)
        } else {
            (ms, counter + 1)
        };
        self.issued()
    }

    fn issued(&self) -> MessageId {
        let (ms, counter) = self.last_id;
        MessageId::new(ms, self.incarnation, counter).expect("the clock is inside 48 bits")
    }

    fn publish(&mut self, messages: Vec<NewMessage>, key: Option<Key>, reply: Reply<Published>) {
        if messages.is_empty() {
            reply.send(Err(Error::Empty)).ok();
            return;
        }
        if messages.iter().any(|m| !names::publishable(&m.stream))
            || key
                .as_ref()
                .is_some_and(|k| k.scope != BULK && messages.iter().any(|m| m.stream != k.scope))
        {
            reply.send(Err(Error::BadName)).ok();
            return;
        }
        let short = |s: &String| s.len() <= usize::from(u16::MAX);
        if key.as_ref().is_some_and(|k| !short(&k.key))
            || messages.iter().any(|m| {
                m.headers.len() > usize::from(u16::MAX)
                    || m.headers.iter().any(|(k, v)| !short(k) || !short(v))
            })
        {
            reply.send(Err(Error::TooLong)).ok();
            return;
        }
        let now = self.clock.now_ms();
        let key = key.map(|k| (k.scope, k.key));
        if let Some(key) = &key {
            if let Some(kept) = self.keys.get(key).filter(|k| k.at + KEY_MS > now) {
                let published = Published {
                    first: kept.first,
                    count: kept.count,
                    repeat: true,
                };
                reply.send(Ok(published)).ok();
                return;
            }
            if let Some(waiting) = self.keys_in_flight.get_mut(key) {
                waiting.push(reply);
                return;
            }
        }
        if self.floor {
            reply.send(Err(Error::NoSpace)).ok();
            return;
        }
        let count = messages.len() as u32;
        let stored: Vec<(String, Message)> = messages
            .into_iter()
            .map(|m| {
                let message = Message {
                    position: 0,
                    id: self.next_id(now),
                    timing: m.timing,
                    origin: None,
                    headers: m.headers,
                    value: m.value,
                };
                (m.stream, message)
            })
            .collect();
        let first = stored[0].1.id;
        let controls: Vec<Control> = key
            .iter()
            .map(|(scope, key)| Control::Key {
                scope: scope.clone(),
                key: key.clone(),
                first,
                count,
            })
            .collect();
        let longest = "g".repeat(names::MAX_NAME_BYTES);
        if request_len(&stored, &controls) > BLOCK_CAP
            || stored
                .iter()
                .any(|(stream, m)| !dead_copy_fits(stream, m, &longest))
        {
            reply.send(Err(Error::TooLarge)).ok();
            return;
        }
        if let Some(key) = key {
            self.keys_in_flight.insert(key, Vec::new());
        }
        let ticket = self.ticket(Box::new(move |result| {
            let published = Published {
                first,
                count,
                repeat: false,
            };
            reply.send(result.map(|()| published)).ok();
        }));
        self.push(stored, controls, Some(ticket));
        self.release_ticket(ticket, Ok(()));
    }

    fn take(&mut self, take: Take) -> Result<Taken, Error> {
        let receipt = Receipt(u128::from_le_bytes(random(self.random.as_mut())?));
        let request = TakeRequest {
            group: &take.group,
            selector: &take.selector,
            limit: take.limit,
            lease_ms: take.lease_ms,
            receipt,
            wait: take.wait,
        };
        let (outcome, actions) = self.core.take(&request, self.clock.now_ms());
        self.act(actions, None);
        Ok(match outcome {
            TakeOutcome::Leased { until, messages } => Taken::Leased {
                receipt,
                until,
                messages: messages
                    .into_iter()
                    .map(|handed| Delivery {
                        location: self.index.locate(handed.address),
                        handed,
                    })
                    .collect(),
            },
            TakeOutcome::NoWork {
                register: Some(waiter),
            } => {
                let (tx, woken) = oneshot::channel();
                self.waiters.insert(waiter, (take.group, tx));
                Taken::Waiting { waiter, woken }
            }
            TakeOutcome::NoWork { register: None } => Taken::Empty,
        })
    }

    fn settle(&mut self, receipt: Receipt, by: SettleBy, reply: Reply<()>) {
        let now = self.clock.now_ms();
        let settled = match &by {
            SettleBy::Ids(settle) => self.core.settle(receipt, settle, now),
            SettleBy::All => self.core.ack_all(receipt, now),
        };
        match settled {
            Err(refused) => {
                reply.send(Err(Error::Refused(refused))).ok();
            }
            Ok(actions) => {
                let ticket = self.ticket(Box::new(move |result| {
                    reply.send(result).ok();
                }));
                self.act(actions, Some(ticket));
                self.release_ticket(ticket, Ok(()));
            }
        }
    }

    fn act(&mut self, actions: Vec<Action>, ticket: Option<u64>) {
        let mut budget = if self.queued() < QUEUE_CAP {
            DEAD_PER_PASS
        } else {
            0
        };
        let mut dead = Vec::new();
        for action in actions {
            match action {
                Action::Write(record) => {
                    self.push(Vec::new(), vec![Control::Record(record)], ticket)
                }
                Action::Dead(copy) => dead.push(copy),
                Action::Wake(waiter, stream) => self.wake(waiter, stream),
                Action::Await { group, address } => {
                    if let Some(ticket) = ticket {
                        self.hold_ticket(ticket);
                        self.awaits
                            .entry((group, address))
                            .or_default()
                            .push(ticket);
                    }
                }
            }
        }
        dead.sort_by_key(|d| d.address);
        if let Some(ticket) = ticket {
            if !dead.is_empty() {
                self.hold_ticket(ticket);
                self.burying_bytes += dead.iter().map(waiting_len).sum::<usize>();
                self.burying.push_back((ticket, self.issued(), dead.into()));
            }
            return;
        }
        let mut run = None;
        for copy in dead {
            if budget == 0 || self.deleting > 0 {
                self.core.defer_dead(&copy.group, copy.address);
            } else {
                budget -= 1;
                self.dead_copy(copy, None, &mut run);
            }
        }
    }

    fn bury(&mut self) {
        let refill =
            !self.burying.is_empty() && self.deleting == 0 && self.queued() < QUEUE_CAP / 2;
        let low = refill
            && self.fs.statfs(&self.dir).is_ok_and(|stat| {
                retention::pressure(stat.free.saturating_sub(self.queued() as u64), stat.total)
            });
        if self.full || low {
            self.abandon_burying(Error::Refused(SettleError::Pressure));
        }
        while self.deleting == 0
            && self.queued() < QUEUE_CAP / 2
            && let Some((ticket, since, copies)) = self.burying.front_mut()
            && let Some(copy) = copies.pop_front()
        {
            let (ticket, since) = (*ticket, *since);
            self.burying_bytes -= waiting_len(&copy);
            if self.core.dead_due(&copy, since) {
                let mut run = self.buried.take();
                self.dead_copy(copy, Some(ticket), &mut run);
                self.buried = run;
            } else {
                self.abandon_dead(&copy.group, copy.generation, copy.address, None, None);
            }
            let next = self
                .burying
                .front()
                .and_then(|(_, _, copies)| copies.front())
                .map(|copy| self.index.locate(copy.address).map(|l| l.start));
            let same_run = next.flatten().is_some_and(|start| {
                self.buried
                    .as_ref()
                    .is_some_and(|(cached, _)| *cached == start)
            });
            if next.is_none() {
                self.burying.pop_front();
                self.release_ticket(ticket, Ok(()));
            } else if !same_run {
                self.burying.rotate_left(1);
            }
            if !same_run {
                self.buried = None;
            }
        }
        self.buried = self.buried.take().filter(|(_, read)| read.is_ok());
    }

    fn abandon_burying(&mut self, error: Error) {
        for (ticket, _, copies) in std::mem::take(&mut self.burying) {
            for copy in copies {
                let failed = Some(error.clone());
                self.abandon_dead(
                    &copy.group,
                    copy.generation,
                    copy.address,
                    Some(ticket),
                    failed,
                );
            }
            self.release_ticket(ticket, Ok(()));
        }
        self.burying_bytes = 0;
        self.buried = None;
    }

    fn dead_copy(&mut self, mut dead: DeadCopy, ticket: Option<u64>, run: &mut Cached) {
        let original = match self.index.locate(dead.address) {
            Some(location) => {
                if run
                    .as_ref()
                    .is_none_or(|(start, _)| *start != location.start)
                {
                    *run = None;
                    let read = match index::read(&location) {
                        Ok(messages) => Ok(messages),
                        Err(ReadError::Damaged) => {
                            self.damaged(location.start, location.run.offset);
                            Ok(Vec::new())
                        }
                        Err(ReadError::Io(_)) if ticket.is_none() => return self.stop(),
                        Err(ReadError::Io(e)) => Err(e.to_string()),
                    };
                    *run = Some((location.start, read));
                }
                let offset = (dead.address.position - location.start.position) as usize;
                match run.as_ref().map(|(_, read)| read) {
                    Some(Ok(messages)) => Ok(messages
                        .get(offset)
                        .filter(|m| m.position == dead.address.position)
                        .cloned()),
                    Some(Err(e)) => Err(Error::Unreadable(e.clone())),
                    None => Ok(None),
                }
            }
            None => Ok(None),
        };
        let original = match original {
            Ok(Some(original)) => original,
            refused => {
                let (group, generation, address) = (dead.group, dead.generation, dead.address);
                return self.abandon_dead(&group, generation, address, ticket, refused.err());
            }
        };
        let (id, stream) = match original.origin {
            Some(first) => (first.id, first.stream),
            None => (dead.id, dead.stream.clone()),
        };
        let mut copy = Message {
            position: 0,
            id: self.next_id(self.clock.now_ms()),
            timing: Timing::default(),
            origin: Some(Origin {
                id,
                stream,
                reason: dead.reason,
                error: dead.error.take(),
            }),
            headers: original.headers,
            value: original.value,
        };
        let ack = Control::Record(Record::Ack {
            group: dead.group.clone(),
            generation: dead.generation,
            address: dead.address,
            stream: dead.stream.clone(),
        });
        let over = dead_len(&dead.dead_stream, &copy, &ack).saturating_sub(BLOCK_CAP);
        if over > 0
            && let Some(origin) = copy.origin.as_mut()
        {
            origin.error = origin
                .error
                .take()
                .filter(|e| e.len() >= over)
                .map(|mut e| {
                    e.truncate(e.floor_char_boundary(e.len() - over));
                    e
                });
        }
        if ticket.is_none() && dead_len(&dead.dead_stream, &copy, &ack) > BLOCK_CAP {
            self.push(Vec::new(), vec![ack], None);
            self.dropped.push(dead);
            return;
        }
        self.push(vec![(dead.dead_stream, copy)], vec![ack], ticket);
    }

    fn abandon_dead(
        &mut self,
        group: &str,
        generation: Generation,
        address: Address,
        ticket: Option<u64>,
        error: Option<Error>,
    ) {
        let actions = self.core.abandon(group, generation, &[address], false);
        self.act(actions, None);
        let result = error.map_or(Ok(()), Err);
        self.resolve_await((group.to_owned(), address), &result);
        if let (Some(ticket), Err(error)) = (ticket, result) {
            self.hold_ticket(ticket);
            self.release_ticket(ticket, Err(error));
        }
    }

    fn wake(&mut self, waiter: WaiterId, stream: Option<String>) {
        let Some((group, tx)) = self.waiters.remove(&waiter) else {
            return;
        };
        if tx.send(()).is_ok() {
            return;
        }
        if let Some(next) = stream.and_then(|s| self.core.wake_for(&group, &s)) {
            self.act(vec![next], None);
        }
    }

    fn damaged(&mut self, start: Address, offset: u64) {
        if self
            .index
            .locate(start)
            .is_none_or(|location| location.run.offset != offset)
        {
            return;
        }
        let Some(run) = self.index.remove(start) else {
            return;
        };
        self.core.drop_run(start);
        let damage = Damage {
            segment: start.segment,
            start: run.offset,
            end: run.offset + u64::from(run.len),
        };
        self.push_damage(vec![damage], None);
    }

    fn push_damage(&mut self, damage: Vec<Damage>, ticket: Option<u64>) {
        for _ in 0..2 {
            self.queue.push_back(Pending::new());
            self.push(
                Vec::new(),
                damage.iter().copied().map(Control::Damage).collect(),
                ticket,
            );
        }
    }

    fn push(
        &mut self,
        messages: Vec<(String, Message)>,
        controls: Vec<Control>,
        ticket: Option<u64>,
    ) {
        self.deleting += controls.iter().filter(|c| deletes(c)).count();
        if request_len(&messages, &controls) > BLOCK_CAP {
            if let Some(ticket) = ticket {
                self.hold_ticket(ticket);
            }
            return self.unwritten_block(&controls, ticket.into_iter().collect(), &Error::TooLarge);
        }
        let bytes = body_len(&messages, &controls);
        if !self
            .queue
            .back()
            .is_some_and(|b| b.bytes + bytes <= BLOCK_CAP)
        {
            self.queue.push_back(Pending::new());
        }
        let Some(block) = self.queue.back_mut() else {
            return;
        };
        block.messages.extend(messages);
        block.controls.extend(controls);
        block.bytes += bytes;
        let fresh = ticket.filter(|t| block.tickets.last() != Some(t));
        if let Some(ticket) = fresh {
            block.tickets.push(ticket);
            self.hold_ticket(ticket);
        }
    }

    fn ticket(&mut self, done: Done) -> u64 {
        self.next_ticket += 1;
        self.tickets.insert(
            self.next_ticket,
            Ticket {
                remaining: 1,
                error: None,
                done,
            },
        );
        self.next_ticket
    }

    fn hold_ticket(&mut self, ticket: u64) {
        if let Some(t) = self.tickets.get_mut(&ticket) {
            t.remaining += 1;
        }
    }

    fn release_ticket(&mut self, ticket: u64, result: Result<(), Error>) {
        let Some(t) = self.tickets.get_mut(&ticket) else {
            return;
        };
        t.remaining -= 1;
        if let Err(e) = result {
            t.error.get_or_insert(e);
        }
        if t.remaining > 0 {
            return;
        }
        if let Some(t) = self.tickets.remove(&ticket) {
            (t.done)(t.error.map_or(Ok(()), Err));
        }
    }

    fn resolve_await(&mut self, key: (String, Address), result: &Result<(), Error>) {
        for ticket in self.awaits.remove(&key).unwrap_or_default() {
            self.release_ticket(ticket, result.clone());
        }
    }

    fn flush(&mut self) {
        if self.flight.is_some() || self.stopped {
            return;
        }
        let Some(log) = &self.log else {
            return;
        };
        let Some(block) = self.queue.pop_front() else {
            return;
        };
        let mut runs: Vec<(String, Vec<Message>)> = Vec::new();
        for (stream, message) in block.messages {
            match runs.last_mut() {
                Some((s, run))
                    if *s == stream
                        && run.last().and_then(|m| nth_id(m.id, 1)) == Some(message.id) =>
                {
                    run.push(message)
                }
                _ => runs.push((stream, vec![message])),
            }
        }
        let count: u32 = runs.iter().map(|(_, r)| r.len() as u32).sum();
        let mut base = self.cursor.as_ref().map_or(0, |c| c.next_position);
        let mut encoded = encode_at(&mut runs, base, &block.controls);
        let total = (BLOCK_HEADER_LEN + encoded.0.len()) as u64;
        if base != 0 && self.cursor.as_ref().is_some_and(|c| !fits(c.end, total)) {
            base = 0;
            encoded = encode_at(&mut runs, base, &block.controls);
        }
        let (body, entries) = encoded;
        let commit_ms = self.clock.now_ms();
        let preallocate = if self.full { 0 } else { SEGMENT_BYTES };
        let rx = log.append(commit_ms, body, preallocate);
        self.flight = Some(Flight::Block(Box::new(Landing {
            rx,
            runs,
            entries,
            controls: block.controls,
            tickets: block.tickets,
            end: base + count,
            total,
            commit_ms,
        })));
    }

    fn landed(&mut self, flight: Flight, result: Result<Option<Written>, log::Error>) {
        match (flight, result) {
            (Flight::Block(landing), Ok(Some(written))) => self.apply(*landing, written),
            (Flight::Block(landing), Err(error)) => self.abandon(*landing, &error),
            (Flight::Seal(_), Err(_)) => self.stop(),
            (Flight::Seal(_), Ok(_)) => {
                if let Some(segment) = self.active.take() {
                    self.sealed(segment);
                }
            }
            _ => {}
        }
    }

    fn apply(&mut self, landing: Landing, written: Written) {
        self.cursor = Some(Cursor {
            end: written.offset + landing.total,
            next_position: landing.end,
        });
        self.last_write = landing.commit_ms;
        self.changes += 1;
        self.wrote(
            written.segment,
            written.offset + landing.total,
            landing.total - BLOCK_HEADER_LEN as u64,
            landing.commit_ms,
            !landing.controls.is_empty(),
        );
        if !landing.runs.is_empty() && !self.index.has(written.segment) {
            match self
                .fs
                .open(&self.dir.join(segment_file_name(written.segment)))
            {
                Ok(file) => self.index.add(written.segment, Arc::new(file)),
                Err(_) => return self.stop(),
            }
        }
        let mut actions = Vec::new();
        for ((stream, messages), entry) in landing.runs.iter().zip(&landing.entries) {
            let start = Address {
                segment: written.segment,
                position: messages[0].position,
            };
            self.index
                .insert(start, run_of(entry, messages, written.offset));
            actions.extend(
                self.core
                    .apply(messages_record(stream, start, messages), landing.commit_ms),
            );
        }
        if !landing.runs.is_empty() {
            self.applied.send_modify(|(n, streams)| {
                *n += 1;
                streams.clear();
                streams.extend(landing.runs.iter().map(|(stream, _)| stream.clone()));
            });
        }
        for control in landing.controls {
            if deletes(&control) {
                self.deleting -= 1;
            }
            match control {
                Control::Record(record) => {
                    let settled = settled_key(&record);
                    if let Record::Generation { group, .. } = &record {
                        self.replayed(group);
                    }
                    actions.extend(self.core.apply(record, landing.commit_ms));
                    if let Some(key) = settled {
                        self.resolve_await(key, &Ok(()));
                    }
                }
                Control::Key {
                    scope,
                    key,
                    first,
                    count,
                } => {
                    let id = (scope, key);
                    for waiting in self.keys_in_flight.remove(&id).unwrap_or_default() {
                        let published = Published {
                            first,
                            count,
                            repeat: true,
                        };
                        waiting.send(Ok(published)).ok();
                    }
                    let kept = Kept {
                        first,
                        count,
                        at: landing.commit_ms,
                    };
                    self.keys.insert(id, kept);
                }
                Control::Damage(damage) => {
                    self.damage.insert(damage);
                }
            }
        }
        let sealable = self
            .tail
            .as_ref()
            .is_some_and(|(ranges, _)| ranges.iter().all(|d| self.damage.contains(d)));
        if sealable && let Some((_, points)) = self.tail.take() {
            if points.iter().any(|p| seal_at(&self.fs, p).is_err()) {
                return self.stop();
            }
            for point in points {
                if let Some(info) = self.files.get_mut(&point.file) {
                    info.sealed = true;
                    self.pass_due = true;
                }
            }
        }
        for ticket in landing.tickets {
            self.release_ticket(ticket, Ok(()));
        }
        self.act(actions, None);
    }

    fn abandon(&mut self, landing: Landing, error: &log::Error) {
        self.cursor = None;
        self.unwritten_block(&landing.controls, landing.tickets, &engine_error(error));
        if matches!(error, log::Error::NoSpace) {
            if let Some(segment) = self.active {
                self.sealed(segment);
            }
            self.set_pressure(true);
        } else {
            self.stop();
        }
    }

    fn replayed(&mut self, group: &str) {
        if let Some(count) = self.replaying.get_mut(group) {
            *count -= 1;
            if *count == 0 {
                self.replaying.remove(group);
            }
        }
    }

    fn unwritten_block(&mut self, controls: &[Control], tickets: Vec<u64>, error: &Error) {
        let failed = Err(error.clone());
        let mut actions = Vec::new();
        for control in controls {
            if deletes(control) {
                self.deleting -= 1;
            }
            match control {
                Control::Record(
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
                ) => {
                    actions.extend(self.core.abandon(group, *generation, &[*address], false));
                    self.resolve_await((group.clone(), *address), &failed);
                }
                Control::Record(Record::Forget {
                    group,
                    generation,
                    provisional: true,
                }) => actions.extend(self.core.abandon(group, *generation, &[], true)),
                Control::Record(Record::ReadSet {
                    group, generation, ..
                }) => actions.extend(self.core.abandon(group, *generation, &[], false)),
                Control::Record(Record::Generation { group, .. }) => self.replayed(group),
                Control::Key { scope, key, .. } => {
                    let id = (scope.clone(), key.clone());
                    for waiting in self.keys_in_flight.remove(&id).unwrap_or_default() {
                        waiting.send(Err(error.clone())).ok();
                    }
                }
                Control::Damage(damage) => {
                    self.unwritten.insert(*damage);
                }
                _ => {}
            }
        }
        for ticket in tickets {
            self.release_ticket(ticket, failed.clone());
        }
        self.act(actions, None);
    }

    fn set_pressure(&mut self, pressure: bool) {
        if pressure {
            self.enospc = Some(self.clock.now_ms());
        }
        self.full = pressure;
        self.core.set_pressure(pressure);
    }

    fn queued(&self) -> usize {
        self.queue.iter().map(|b| b.bytes).sum()
    }

    fn stop(&mut self) {
        self.stopped = true;
        self.queue.clear();
        self.burying.clear();
        self.burying_bytes = 0;
        self.buried = None;
        self.awaits.clear();
        self.waiters.clear();
        for (_, waiting) in std::mem::take(&mut self.keys_in_flight) {
            for reply in waiting {
                reply.send(Err(Error::Stopped)).ok();
            }
        }
        for (_, ticket) in std::mem::take(&mut self.tickets) {
            (ticket.done)(Err(Error::Stopped));
        }
    }

    fn wrote(&mut self, segment: SegmentId, end: u64, body: u64, commit_ms: u64, controls: bool) {
        if let Some(old) = self.active.filter(|a| *a != segment) {
            self.sealed(old);
        }
        self.active = Some(segment);
        let path = self.dir.join(segment_file_name(segment));
        self.owner.entry(segment).or_insert_with(|| path.clone());
        if !self.files.contains_key(&path) {
            self.written += FILE_HEADER_LEN as u64;
        }
        let info = self.files.entry(path).or_insert(FileInfo {
            len: 0,
            bodies: 0,
            carried: 0,
            first_ms: commit_ms,
            controls: false,
            sealed: false,
            segments: BTreeSet::from([segment]),
        });
        info.controls |= controls;
        info.len = end;
        info.bodies += body;
        self.written += BLOCK_HEADER_LEN as u64 + body;
    }

    fn sealed(&mut self, segment: SegmentId) {
        if let Some(info) = self
            .owner
            .get(&segment)
            .and_then(|path| self.files.get_mut(path))
        {
            info.sealed = true;
        }
        self.pass_due = true;
    }

    fn kept(&self, segment: SegmentId, now: u64) -> u64 {
        let held = self.core.holders(segment, now, self.full).runs;
        self.index
            .runs(segment)
            .filter(|(start, _)| held.contains(start))
            .map(|(_, run)| u64::from(run.len))
            .sum()
    }

    fn run_bytes(&self, segment: SegmentId) -> u64 {
        self.index
            .runs(segment)
            .map(|(_, r)| u64::from(r.len))
            .sum()
    }

    fn active_removable(&self) -> bool {
        let now = self.clock.now_ms();
        self.active
            .and_then(|segment| {
                let info = self.files.get(self.owner.get(&segment)?)?;
                let removable = self
                    .run_bytes(segment)
                    .saturating_sub(self.kept(segment, now));
                Some(removable > 0 && removable * 2 >= info.bodies)
            })
            .unwrap_or(false)
    }

    fn pass(&mut self) {
        if !self.pass_due
            || self.compacting
            || self.stopped
            || matches!(self.flight, Some(Flight::Seal(_)))
        {
            return;
        }
        self.pass_due = false;
        self.passed = self.changes;
        let waiting = std::mem::take(&mut self.waiting);
        let now = self.clock.now_ms();
        let rescan = now >= self.last_rescan + RESCAN_MS;
        if rescan {
            self.last_rescan = now;
        }
        let sweep = now >= self.last_sweep + SWEEP_MS;
        if sweep {
            self.last_sweep = now;
        }
        let aged = sweep.then(|| now.saturating_sub(SWEEP_MS));
        let inputs = select(
            &self.files,
            &self.owner,
            self.full,
            rescan,
            aged,
            |segment| (self.run_bytes(segment), self.kept(segment, now)),
        );
        let result = if inputs.is_empty() {
            Ok(())
        } else if self
            .compactor
            .as_ref()
            .is_some_and(|c| c.send(Job { inputs }))
        {
            self.compacting = true;
            self.running = waiting;
            return;
        } else {
            self.stop();
            Err(Error::Stopped)
        };
        for reply in waiting {
            reply.send(result.clone()).ok();
        }
    }

    fn report(&mut self, report: Option<Report>) {
        match report {
            Some(Report::Ask(scanned, reply)) => {
                reply.send(self.keep(scanned)).ok();
            }
            Some(Report::Damaged(ranges, reply)) => self.compaction_damage(ranges, reply),
            Some(Report::Output(output)) => self.installed(output),
            Some(Report::Deleted(path, lost)) => self.deleted(&path, &lost),
            Some(Report::Failed) => self.stop(),
            Some(Report::Done) => self.passed_all(),
            None => {
                self.compactor = None;
                self.stop();
            }
        }
    }

    fn passed_all(&mut self) {
        self.compacting = false;
        for reply in std::mem::take(&mut self.running) {
            reply.send(Ok(())).ok();
        }
    }

    fn keep(&self, scanned: Vec<Scanned>) -> Vec<Keep> {
        let now = self.clock.now_ms();
        let mut held: BTreeMap<SegmentId, BTreeSet<u32>> = BTreeMap::new();
        let mut gone: BTreeMap<Address, u32> = BTreeMap::new();
        let runs: Vec<Vec<bool>> = scanned
            .iter()
            .map(|block| {
                let segment = block.segment;
                let held = held
                    .entry(segment)
                    .or_insert_with(|| self.core.holders(segment, now, self.full).runs);
                block
                    .runs
                    .iter()
                    .map(|position| {
                        let keep = held.contains(position);
                        let start = Address {
                            segment,
                            position: *position,
                        };
                        if let Some(location) = self.index.locate(start).filter(|_| !keep) {
                            gone.insert(location.start, location.run.count);
                        }
                        keep
                    })
                    .collect()
            })
            .collect();
        let is_gone = |at: Address| {
            gone.range(..=at)
                .next_back()
                .is_some_and(|(s, n)| s.segment == at.segment && at.position - s.position < *n)
        };
        scanned
            .into_iter()
            .zip(runs)
            .map(|(block, runs)| Keep {
                runs,
                controls: block
                    .controls
                    .into_iter()
                    .filter(|c| carries(&self.core, c, block.commit_ms, now, &is_gone))
                    .collect(),
            })
            .collect()
    }

    fn installed(&mut self, output: Output) {
        let held = output.segments.iter().copied().collect();
        let mut offsets: BTreeMap<SegmentId, BTreeMap<u32, u64>> = BTreeMap::new();
        for (start, offset) in output.runs {
            offsets
                .entry(start.segment)
                .or_default()
                .insert(start.position, offset);
        }
        for segment in output.segments {
            let moved = offsets.remove(&segment).unwrap_or_default();
            let old: Vec<(u32, Run)> = self
                .index
                .runs(segment)
                .map(|(start, run)| (start, run.clone()))
                .collect();
            let mut runs = BTreeMap::new();
            for (start, run) in old {
                match moved.get(&start) {
                    Some(offset) => {
                        runs.insert(
                            start,
                            Run {
                                offset: *offset,
                                ..run
                            },
                        );
                    }
                    None => self.core.drop_run(Address {
                        segment,
                        position: start,
                    }),
                }
            }
            if runs.is_empty() {
                self.drop_segment(segment);
            }
            self.index.replace(segment, Arc::clone(&output.file), runs);
            self.owner.insert(segment, output.path.clone());
        }
        self.written += output.written;
        let info = FileInfo {
            len: output.len,
            bodies: output.bodies,
            carried: output.carried,
            first_ms: output.first_ms,
            controls: false,
            sealed: true,
            segments: held,
        };
        self.files.insert(output.path, info);
        self.changes += 1;
    }

    fn deleted(&mut self, path: &Path, lost: &[SegmentId]) {
        self.files.remove(path);
        for segment in lost {
            if self.owner.get(segment).is_some_and(|p| p == path) {
                self.owner.remove(segment);
                self.index.drop_segment(*segment);
                self.drop_segment(*segment);
            }
        }
        self.owner.retain(|_, p| p != path);
        self.changes += 1;
    }

    fn drop_segment(&mut self, segment: SegmentId) {
        self.core.drop_segment(segment);
        self.damage.retain(|d| d.segment != segment);
    }

    fn compaction_damage(
        &mut self,
        ranges: Vec<(Vec<SegmentId>, std::ops::Range<u64>)>,
        reply: oneshot::Sender<bool>,
    ) {
        let ticket = self.ticket(Box::new(move |result| {
            reply.send(result.is_ok()).ok();
        }));
        for (segments, range) in ranges {
            let hits: Vec<Address> = segments
                .iter()
                .flat_map(|segment| {
                    self.index
                        .runs(*segment)
                        .filter(|(_, run)| range.contains(&run.offset))
                        .map(|(position, _)| Address {
                            segment: *segment,
                            position,
                        })
                })
                .collect();
            for start in hits {
                self.index.remove(start);
                self.core.drop_run(start);
            }
            for segment in segments {
                let damage = Damage {
                    segment,
                    start: range.start,
                    end: range.end,
                };
                if !self.damage.contains(&damage) {
                    self.push_damage(vec![damage], Some(ticket));
                }
            }
        }
        self.release_ticket(ticket, Ok(()));
    }

    fn status(&self) -> Status {
        let state = self.core.state();
        let (since, base) = self.marks[0];
        let now = self.clock.now_ms();
        Status {
            disk: self.fs.statfs(&self.dir).ok(),
            write_bytes_per_s: if now < since {
                0
            } else {
                (self.written - base) * 1000 / (now - since).max(RATE_MS)
            },
            streams: state.stream_names().map(str::to_owned).collect(),
            groups: state
                .group_names()
                .map(|(name, provisional)| (name.to_owned(), provisional))
                .collect(),
            damage: self.damage.iter().copied().collect(),
            damaged: self
                .damage
                .iter()
                .map(|d| {
                    let streams = state.streams_in(d.segment);
                    (d.segment, streams.into_iter().map(str::to_owned).collect())
                })
                .collect(),
            unmapped: self.unmapped.clone(),
            dropped: self.dropped.clone(),
        }
    }

    fn holding(&self) -> Holding {
        let now = self.clock.now_ms();
        let mut order: Vec<(u64, SegmentId)> = self
            .owner
            .iter()
            .map(|(segment, path)| {
                (
                    self.files.get(path).map_or(u64::MAX, |f| f.first_ms),
                    *segment,
                )
            })
            .collect();
        order.sort();
        for (_, segment) in order {
            let held = self.core.holders(segment, now, true);
            if !held.groups.is_empty() || !held.streams.is_empty() {
                return Holding {
                    groups: held.groups.into_iter().collect(),
                    streams: held.streams.into_iter().collect(),
                };
            }
        }
        Holding::default()
    }

    fn tick(&mut self) {
        let now = self.clock.now_ms();
        if self.last_tick != Some(now) {
            self.last_tick = Some(now);
            let gone: Vec<WaiterId> = self
                .waiters
                .iter()
                .filter(|(_, (_, tx))| tx.is_closed())
                .map(|(waiter, _)| *waiter)
                .collect();
            for waiter in gone {
                self.core.cancel(waiter);
                self.waiters.remove(&waiter);
            }
            let held = self.enospc.is_some_and(|at| now < at + PRESSURE_HOLD_MS);
            if let Ok(stat) = self.fs.statfs(&self.dir) {
                self.floor = retention::pressure(stat.free, stat.total);
                let full = held || self.floor;
                if full != self.full {
                    self.full = full;
                    self.core.set_pressure(full);
                }
            }
            let actions = self.core.tick(now);
            self.act(actions, None);
            if now < self.marks[1].0 {
                self.marks = [(now, self.written); 2];
            } else if now >= self.marks[1].0 + RATE_MS {
                self.marks = [self.marks[1], (now, self.written)];
            }
            if now >= self.last_prune + PRUNE_MS {
                self.keys.retain(|_, k| k.at + KEY_MS > now);
                self.last_prune = now;
            }
        }
        let mut damage = std::mem::take(&mut self.unwritten);
        damage.retain(|d| !self.damage.contains(d));
        if !damage.is_empty() {
            self.push_damage(damage.into_iter().collect(), None);
        }
        let changed = self.full && self.changes != self.passed;
        if changed || now >= self.last_rescan + RESCAN_MS || now >= self.last_sweep + SWEEP_MS {
            self.pass_due = true;
        }
        if self.flight.is_none()
            && self.queue.is_empty()
            && self.cursor.is_some()
            && (now >= self.last_write + IDLE_SEAL_MS || (changed && self.active_removable()))
            && let Some(log) = &self.log
        {
            self.flight = Some(Flight::Seal(log.seal()));
            self.cursor = None;
        }
    }
}

fn settled_key(record: &Record) -> Option<(String, Address)> {
    match record {
        Record::Ack { group, address, .. } | Record::Failure { group, address, .. } => {
            Some((group.clone(), *address))
        }
        _ => None,
    }
}

fn deletes(control: &Control) -> bool {
    matches!(
        control,
        Control::Record(Record::Tombstone { .. } | Record::Forget { .. })
    )
}

fn waiting_len(copy: &DeadCopy) -> usize {
    size_of::<DeadCopy>()
        + copy.group.len()
        + copy.stream.len()
        + copy.dead_stream.len()
        + copy.error.as_ref().map_or(0, String::len)
}

fn dead_len(dead_stream: &str, copy: &Message, ack: &Control) -> usize {
    EMPTY_BODY + records::message_len(dead_stream, copy) + records::control_len(ack)
}

fn dead_copy_fits(stream: &str, message: &Message, group: &str) -> bool {
    let copy = Message {
        position: 0,
        id: message.id,
        timing: Timing::default(),
        origin: Some(Origin {
            id: message.id,
            stream: stream.to_owned(),
            reason: Reason::Expired,
            error: None,
        }),
        headers: message.headers.clone(),
        value: Vec::new(),
    };
    let dead_stream = names::dead_stream(stream, group);
    let ack = Control::Record(Record::Ack {
        group: group.to_owned(),
        generation: Generation {
            counter: 0,
            incarnation: 0,
        },
        address: Address {
            segment: SegmentId {
                incarnation: 0,
                seq: 0,
            },
            position: 0,
        },
        stream: dead_stream.clone(),
    });
    dead_len(&dead_stream, &copy, &ack) + message.value.len() <= BLOCK_CAP
}

fn body_len(messages: &[(String, Message)], controls: &[Control]) -> usize {
    messages
        .iter()
        .map(|(s, m)| records::message_len(s, m))
        .sum::<usize>()
        + controls.iter().map(records::control_len).sum::<usize>()
}

fn request_len(messages: &[(String, Message)], controls: &[Control]) -> usize {
    let shared: usize = messages
        .windows(2)
        .filter(|pair| pair[0].0 == pair[1].0)
        .map(|pair| records::run_entry_len(&pair[1].0))
        .sum();
    EMPTY_BODY + body_len(messages, controls) - shared
}

fn decodes(body: &[u8]) -> bool {
    records::decode(body).is_ok_and(|b| {
        b.runs
            .iter()
            .all(|(entry, run)| records::decode_run(run, entry.count).is_ok())
    })
}

fn encode_at(
    runs: &mut [(String, Vec<Message>)],
    base: u32,
    controls: &[Control],
) -> (Vec<u8>, Vec<RunEntry>) {
    let mut position = base;
    for (_, run) in runs.iter_mut() {
        for message in run {
            message.position = position;
            position += 1;
        }
    }
    let refs: Vec<(&str, &[Message])> = runs.iter().map(|(s, r)| (s.as_str(), &r[..])).collect();
    let encoded = records::encode(&refs, controls);
    debug_assert!(decodes(&encoded.0), "an encoded block does not decode");
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::RealFs;

    #[test]
    fn a_waiter_whose_request_has_gone_is_removed_at_the_next_tick() {
        let scratch = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/tmp");
        std::fs::create_dir_all(&scratch).expect("scratch dir");
        let tmp = tempfile::TempDir::new_in(&scratch).expect("temp dir");
        let clock = TestClock::new(1_000);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let (mut engine, _handle) = runtime
            .block_on(Engine::open(
                RealFs,
                tmp.path(),
                Arc::new(clock.clone()),
                Box::new(OsRandom),
            ))
            .expect("open");
        let wait = Take {
            group: "g".to_owned(),
            selector: "s".to_owned(),
            limit: 1,
            lease_ms: 30_000,
            wait: true,
        };
        let Ok(Taken::Waiting { woken, .. }) = engine.take(wait) else {
            panic!("expected a waiter");
        };
        drop(woken);
        clock.advance(1);
        engine.tick();
        assert!(engine.waiters.is_empty(), "a gone waiter is still held");
    }

    #[test]
    fn a_compact_whose_job_cannot_reach_the_compaction_thread_gets_stopped() {
        let scratch = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/tmp");
        std::fs::create_dir_all(&scratch).expect("scratch dir");
        let tmp = tempfile::TempDir::new_in(&scratch).expect("temp dir");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let (mut engine, _handle) = runtime
            .block_on(Engine::open(
                RealFs,
                tmp.path(),
                Arc::new(TestClock::new(1_000)),
                Box::new(OsRandom),
            ))
            .expect("open");
        engine.compactor = None;
        let info = FileInfo {
            len: 0,
            bodies: 0,
            carried: 0,
            first_ms: 0,
            controls: false,
            sealed: true,
            segments: BTreeSet::new(),
        };
        engine.files.insert(tmp.path().join("seg-x.vdl"), info);
        let (reply, mut answer) = oneshot::channel();
        engine.command(Command::Compact(reply));
        engine.pass();
        assert_eq!(answer.try_recv(), Ok(Err(Error::Stopped)));
        assert!(engine.stopped);
    }
}
