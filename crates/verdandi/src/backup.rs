use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as Key;
use object_store::{ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{info, warn};
use verdandi_core::ids::SegmentId;
use verdandi_store::block::{BLOCK_HEADER_LEN, MAX_BLOCK, PAGE, VERSION, align, decode_bytes};
use verdandi_store::engine::{Handle, IDLE_SEAL_MS};
use verdandi_store::log::segment_file_name;
use verdandi_store::records;

use crate::runner::node_id;

const TICK: Duration = Duration::from_secs(1);
const CTL_EVERY: Duration = Duration::from_secs(10);
const RETRY_MIN: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(60);
const LIST_EVERY: Duration = Duration::from_secs(5);
const LIST_MAX: Duration = Duration::from_secs(300);
const RESTORE_TRIES: u32 = 5;
const FETCH_WAIT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterRecord {
    pub members: Vec<u64>,
    pub format: u16,
}

#[derive(Clone)]
pub struct Backup {
    pub store: Arc<dyn ObjectStore>,
    base: Key,
    uploaded: Arc<Mutex<BTreeSet<SegmentId>>>,
}

impl Backup {
    pub fn open(url: &str, key: &[u8]) -> Result<Backup> {
        let url = reqwest::Url::parse(url).context("it is not a URL")?;
        let (store, root): (Arc<dyn ObjectStore>, Key) = match url.scheme() {
            "file" => {
                let (store, root) =
                    object_store::parse_url(&url).context("it is not a file URL")?;
                (Arc::from(store), root)
            }
            "s3" => {
                let store = AmazonS3Builder::from_env()
                    .with_url(url.as_str())
                    .build()
                    .context("it is not an S3 URL")?;
                (
                    Arc::new(store),
                    Key::parse(url.path().trim_matches('/')).context("its path is not valid")?,
                )
            }
            other => bail!("the scheme {other}:// is not supported. Use file:// or s3://."),
        };
        Ok(Backup::with_store(store, root, key))
    }

    pub fn with_store(store: Arc<dyn ObjectStore>, root: Key, key: &[u8]) -> Backup {
        let mut hash = Sha256::new();
        hash.update(b"verdandi backup v1");
        hash.update(key);
        let digest = hash.finalize();
        let prefix: String = digest[..8].iter().map(|b| format!("{b:02x}")).collect();
        Backup {
            store,
            base: root.join(prefix),
            uploaded: Arc::default(),
        }
    }

    pub fn segment_key(&self, segment: SegmentId, len: u64) -> Key {
        self.base
            .clone()
            .join("seg")
            .join(format!("{segment}-{len:016x}"))
    }

    pub fn control_key(&self, node: u64) -> Key {
        self.base.clone().join("ctl").join(format!("{node:016x}"))
    }

    pub fn cluster_key(&self) -> Key {
        self.base.clone().join("cluster")
    }

    pub fn uploaded(&self) -> BTreeSet<SegmentId> {
        self.uploaded.lock().unwrap().clone()
    }
}

pub fn pick(segment: SegmentId, holders: &[u64]) -> Option<u64> {
    holders.iter().copied().max_by_key(|node| {
        let mut hash = Sha256::new();
        hash.update(segment.incarnation.to_be_bytes());
        hash.update(segment.seq.to_be_bytes());
        hash.update(node.to_be_bytes());
        let mut low = [0u8; 8];
        low.copy_from_slice(&hash.finalize()[..8]);
        (u64::from_be_bytes(low), *node)
    })
}

pub struct Source {
    pub handle: Handle,
    pub dir: PathBuf,
    pub me: u64,
    pub mine: Box<dyn Fn(SegmentId) -> bool + Send + Sync>,
    pub record: Box<dyn Fn() -> Option<ClusterRecord> + Send + Sync>,
    pub wanted: Box<dyn Fn() -> BTreeSet<u64> + Send + Sync>,
    pub fetched: Box<dyn Fn(SegmentId) + Send + Sync>,
}

impl Source {
    pub fn alone(handle: Handle, dir: PathBuf) -> Source {
        Source {
            handle,
            dir,
            me: 0,
            mine: Box::new(|_| true),
            record: Box::new(|| {
                Some(ClusterRecord {
                    members: Vec::new(),
                    format: VERSION,
                })
            }),
            wanted: Box::new(BTreeSet::new),
            fetched: Box::new(|_| {}),
        }
    }
}

struct Warned(BTreeSet<&'static str>);

impl Warned {
    fn failed(&mut self, kind: &'static str, error: &dyn std::fmt::Display) {
        if self.0.insert(kind) {
            warn!("backup: {kind} failed: {error}. It retries.");
        }
    }

    fn worked(&mut self, kind: &'static str) {
        self.0.remove(kind);
    }
}

async fn idle(source: &Source, segment: SegmentId) -> bool {
    let path = source.dir.join(segment_file_name(segment));
    let age = tokio::fs::metadata(path)
        .await
        .and_then(|m| m.modified())
        .ok()
        .and_then(|at| at.elapsed().ok());
    age.is_some_and(|age| age >= Duration::from_millis(2 * IDLE_SEAL_MS))
}

async fn send(
    backup: &Backup,
    source: &Source,
    segment: SegmentId,
) -> Result<bool, (&'static str, String)> {
    let bytes = tokio::fs::read(source.dir.join(segment_file_name(segment)))
        .await
        .map_err(|e| ("read the segment", e.to_string()))?;
    let key = backup.segment_key(segment, bytes.len() as u64);
    match backup.store.head(&key).await {
        Ok(_) => return Ok(false),
        Err(object_store::Error::NotFound { .. }) => {}
        Err(e) => return Err(("check the object", e.to_string())),
    }
    backup
        .store
        .put(&key, bytes.into())
        .await
        .map_err(|e| ("upload a segment", e.to_string()))?;
    Ok(true)
}

pub async fn run(backup: Backup, source: Source) {
    let mut warned = Warned(BTreeSet::new());
    let mut done: BTreeSet<SegmentId> = BTreeSet::new();
    let mut retry: BTreeMap<SegmentId, (u32, Instant)> = BTreeMap::new();
    let mut ctl_at: Option<Instant> = None;
    let mut ctl_last: Option<Vec<u8>> = None;
    let mut record_last: Option<ClusterRecord> = None;
    let mut seen_stored: BTreeSet<SegmentId> = BTreeSet::new();
    let mut listed_at: Option<Instant> = None;
    let mut list_wait = LIST_EVERY;
    let mut told: BTreeSet<SegmentId> = BTreeSet::new();
    let mut wanted_since: BTreeMap<u64, Instant> = BTreeMap::new();
    let mut fetch_at: Option<Instant> = None;
    let mut fetch_wait = LIST_EVERY;
    if source.handle.stored(told.clone()).await.is_err() {
        return;
    }
    loop {
        tokio::time::sleep(TICK).await;
        let (Ok(sealed), Ok(all)) = (source.handle.sealed().await, source.handle.segments().await)
        else {
            return;
        };
        for &segment in &all {
            let waiting = retry
                .get(&segment)
                .is_some_and(|(_, at)| *at > Instant::now());
            if done.contains(&segment) || waiting || !(source.mine)(segment) {
                continue;
            }
            if !sealed.contains(&segment) && !idle(&source, segment).await {
                continue;
            }
            match send(&backup, &source, segment).await {
                Ok(put) => {
                    if put {
                        backup.uploaded.lock().unwrap().insert(segment);
                    }
                    done.insert(segment);
                    retry.remove(&segment);
                    warned.worked("upload a segment");
                }
                Err((kind, error)) => {
                    warned.failed(kind, &error);
                    let tries = retry.get(&segment).map_or(0, |(n, _)| *n) + 1;
                    let wait = (RETRY_MIN * 2u32.saturating_pow(tries - 1)).min(RETRY_MAX);
                    retry.insert(segment, (tries, Instant::now() + wait));
                }
            }
        }
        done.retain(|s| all.contains(s));
        seen_stored.retain(|s| all.contains(s));
        let missing = all
            .iter()
            .any(|s| !done.contains(s) && !seen_stored.contains(s));
        if !missing {
            list_wait = LIST_EVERY;
        } else if listed_at.is_none_or(|at| at.elapsed() >= list_wait) {
            listed_at = Some(Instant::now());
            list_wait = (list_wait * 2).min(LIST_MAX);
            match longest(&backup).await {
                Ok(found) => {
                    for (segment, (len, _)) in found {
                        if local_len(&source, segment).await == Some(len) {
                            seen_stored.insert(segment);
                        }
                    }
                    warned.worked("list the segments");
                }
                Err(e) => warned.failed("list the segments", &format!("{e:#}")),
            }
        }
        let stored: BTreeSet<SegmentId> = done.union(&seen_stored).copied().collect();
        if told != stored {
            if source.handle.stored(stored.clone()).await.is_err() {
                return;
            }
            told = stored;
        }
        let wanted = (source.wanted)();
        wanted_since.retain(|id, _| wanted.contains(id));
        for id in &wanted {
            wanted_since.entry(*id).or_insert_with(Instant::now);
        }
        let due: BTreeSet<u64> = wanted_since
            .iter()
            .filter(|(_, at)| at.elapsed() >= FETCH_WAIT)
            .map(|(id, _)| *id)
            .collect();
        if due.is_empty() {
            fetch_wait = LIST_EVERY;
        } else if fetch_at.is_none_or(|at| at.elapsed() >= fetch_wait) {
            fetch_at = Some(Instant::now());
            fetch_wait = (fetch_wait * 2).min(LIST_MAX);
            match fetch_wanted(&backup, &source, &due).await {
                Ok(()) => warned.worked("fetch a segment"),
                Err(e) => warned.failed("fetch a segment", &format!("{e:#}")),
            }
        }
        if ctl_at.is_some_and(|at| at.elapsed() < CTL_EVERY) {
            continue;
        }
        ctl_at = Some(Instant::now());
        match source.handle.snapshot().await {
            Ok(bytes) => {
                let body = bytes[BLOCK_HEADER_LEN.min(bytes.len())..].to_vec();
                if ctl_last.as_ref() != Some(&body) {
                    let key = backup.control_key(source.me);
                    match backup.store.put(&key, bytes.into()).await {
                        Ok(_) => {
                            ctl_last = Some(body);
                            warned.worked("upload the control snapshot");
                        }
                        Err(e) => warned.failed("upload the control snapshot", &e),
                    }
                }
            }
            Err(_) => return,
        }
        if let Some(record) = (source.record)().filter(|r| record_last.as_ref() != Some(r)) {
            let sent = match postcard::to_allocvec(&record) {
                Ok(bytes) => backup
                    .store
                    .put(&backup.cluster_key(), bytes.into())
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string()),
                Err(e) => Err(e.to_string()),
            };
            match sent {
                Ok(()) => {
                    record_last = Some(record);
                    warned.worked("upload the cluster record");
                }
                Err(e) => warned.failed("upload the cluster record", &e),
            }
        }
    }
}

async fn local_len(source: &Source, segment: SegmentId) -> Option<u64> {
    let path = source.dir.join(segment_file_name(segment));
    tokio::fs::metadata(path).await.ok().map(|m| m.len())
}

async fn longest(backup: &Backup) -> Result<BTreeMap<SegmentId, (u64, Key)>> {
    let listing = backup
        .store
        .list_with_delimiter(Some(&backup.base.clone().join("seg")))
        .await
        .context("list the segments")?;
    let mut longest: BTreeMap<SegmentId, (u64, Key)> = BTreeMap::new();
    for object in listing.objects {
        let parsed = object.location.filename().and_then(parse_name);
        if let Some((segment, len)) = parsed
            && longest.get(&segment).is_none_or(|(have, _)| *have < len)
        {
            longest.insert(segment, (len, object.location));
        }
    }
    Ok(longest)
}

async fn fetch_wanted(backup: &Backup, source: &Source, due: &BTreeSet<u64>) -> Result<()> {
    for (segment, (_, key)) in longest(backup).await? {
        if !due.contains(&node_id(segment)) {
            continue;
        }
        let file = fetch(backup, &key).await.context("fetch a segment")?;
        replay(&source.handle, segment, &file).await?;
        (source.fetched)(segment);
        info!("backup: fetched segment {segment}");
    }
    Ok(())
}

fn parse_name(name: &str) -> Option<(SegmentId, u64)> {
    let (segment, len) = name.rsplit_once('-')?;
    Some((
        SegmentId::from_str(segment).ok()?,
        u64::from_str_radix(len, 16).ok()?,
    ))
}

async fn fetch(backup: &Backup, key: &Key) -> Result<Vec<u8>> {
    Ok(backup.store.get(key).await?.bytes().await?.to_vec())
}

async fn replay(handle: &Handle, segment: SegmentId, file: &[u8]) -> Result<()> {
    let mut offset = PAGE;
    while offset + BLOCK_HEADER_LEN as u64 <= file.len() as u64 {
        let at = offset as usize;
        let mut size = [0u8; 4];
        size.copy_from_slice(&file[at + 46..at + 50]);
        let len = BLOCK_HEADER_LEN + u32::from_le_bytes(size) as usize;
        if len > MAX_BLOCK || at + len > file.len() {
            break;
        }
        let block = &file[at..at + len];
        if decode_bytes(block, offset).is_err() {
            break;
        }
        handle.replicate(segment, offset, block.to_vec()).await?;
        offset = align(offset + len as u64);
    }
    let rest = file.get(offset as usize..).unwrap_or_default();
    if rest.iter().any(|b| *b != 0) {
        bail!("the backup object of segment {segment} is cut or has a bad block");
    }
    Ok(())
}

async fn is_empty(handle: &Handle) -> Result<bool> {
    if !handle.segments().await?.is_empty() {
        return Ok(false);
    }
    let snapshot = handle.snapshot().await?;
    let (_, body) = decode_bytes(&snapshot, 0).map_err(|_| anyhow::anyhow!("bad snapshot"))?;
    let body = records::decode(body).map_err(|_| anyhow::anyhow!("bad snapshot"))?;
    Ok(body.controls.is_empty())
}

pub async fn rehydrate(backup: &Backup, handle: &Handle) -> Result<bool> {
    if !is_empty(handle).await? {
        return Ok(false);
    }
    match backup.store.head(&backup.cluster_key()).await {
        Ok(_) => {}
        Err(object_store::Error::NotFound { .. }) => {
            info!("backup: no backup under this key. The node starts empty.");
            return Ok(false);
        }
        Err(e) => return Err(e).context("check for a backup"),
    }
    let base = backup.base.clone();
    let longest = longest(backup).await?;
    for (segment, (_, key)) in &longest {
        let file = fetch(backup, key).await.context("fetch a segment")?;
        replay(handle, *segment, &file).await?;
    }
    let listing = backup
        .store
        .list_with_delimiter(Some(&base.join("ctl")))
        .await
        .context("list the control objects")?;
    for object in listing.objects {
        let bytes = fetch(backup, &object.location)
            .await
            .context("fetch a control object")?;
        handle.controls(0, bytes).await?;
    }
    info!("backup: rehydrated {} segments", longest.len());
    Ok(true)
}

pub fn fresh_disk(dir: &Path, cluster: bool) -> bool {
    if cluster {
        return !dir.join("cluster").join("control").exists();
    }
    std::fs::read_dir(dir).is_ok_and(|mut files| {
        files.all(|f| f.is_ok_and(|f| !f.file_name().to_string_lossy().ends_with(".vdl")))
    })
}

pub async fn with_retry<T, F, Fut>(tries: u32, wait: Duration, mut work: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut tried = 0;
    loop {
        tried += 1;
        match work().await {
            Err(e) if tried < tries => {
                warn!("backup: restore failed: {e:#}. It retries.");
                tokio::time::sleep(wait * 2u32.pow(tried - 1)).await;
            }
            done => return done,
        }
    }
}

pub async fn restore(backup: &Backup, handle: &Handle, dir: &Path, cluster: bool) -> Result<bool> {
    if !fresh_disk(dir, cluster) {
        return Ok(false);
    }
    with_retry(RESTORE_TRIES, RETRY_MIN, || rehydrate(backup, handle))
        .await
        .context("the node cannot restore from the backup. Fix the backup or remove VERDANDI_BACKUP_URL, then restart")
}
