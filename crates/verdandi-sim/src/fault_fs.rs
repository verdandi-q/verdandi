use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use verdandi_store::fs::{Fs, FsFile, FsStat};

const PAGE: u64 = 4096;

type Page = Box<[u8; PAGE as usize]>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Crash {
    Random,
    LoseAll,
    Kill,
}

#[derive(Clone)]
struct Dirty {
    before: Option<Page>,
    written: bool,
    failed: bool,
}

#[derive(Clone, Default)]
struct Inode {
    pages: BTreeMap<u64, Page>,
    len: u64,
    synced_len: u64,
    dirty: BTreeMap<u64, Dirty>,
}

#[derive(Clone)]
enum Change {
    Link(String, u64),
    Unlink(String),
    Rename(String, String),
}

#[derive(Clone, Default)]
struct Dir {
    synced: BTreeMap<String, u64>,
    current: BTreeMap<String, u64>,
    pending: Vec<Change>,
}

#[derive(Clone)]
struct State {
    rng: ChaCha8Rng,
    dead: bool,
    ops: u64,
    freeze_at: Option<u64>,
    capacity: u64,
    write_error_per_mille: u32,
    sync_error_per_mille: u32,
    read_error_per_mille: u32,
    next_write_error: Option<(io::ErrorKind, usize)>,
    next_sync_error: bool,
    no_fallocate: bool,
    inodes: BTreeMap<u64, Inode>,
    next_ino: u64,
    dirs: BTreeMap<PathBuf, Dir>,
}

#[derive(Default)]
struct Gate {
    held: Mutex<(bool, usize)>,
    changed: Condvar,
}

#[derive(Clone)]
pub struct FaultFs {
    state: Arc<Mutex<State>>,
    gate: Arc<Gate>,
}

pub struct FaultFile {
    state: Arc<Mutex<State>>,
    gate: Arc<Gate>,
    ino: u64,
}

fn eio() -> io::Error {
    io::Error::other("injected EIO")
}

fn lock<T>(state: &Mutex<T>) -> MutexGuard<'_, T> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

fn split(path: &Path) -> io::Result<(PathBuf, String)> {
    let parent = path.parent().ok_or(io::ErrorKind::InvalidInput)?;
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or(io::ErrorKind::InvalidInput)?;
    Ok((parent.to_path_buf(), name.to_owned()))
}

impl Inode {
    fn mark(&mut self, page: u64, written: bool) {
        if let Some(dirty) = self.dirty.get_mut(&page) {
            dirty.written |= written;
            dirty.failed &= !written;
        } else {
            let before = self.pages.get(&page).cloned();
            self.dirty.insert(
                page,
                Dirty {
                    before,
                    written,
                    failed: false,
                },
            );
        }
    }

    fn write(&mut self, buf: &[u8], offset: u64) {
        let mut at = offset;
        let mut rest = buf;
        while !rest.is_empty() {
            let page = at / PAGE;
            let start = (at % PAGE) as usize;
            let n = rest.len().min(PAGE as usize - start);
            self.mark(page, true);
            let bytes = self
                .pages
                .entry(page)
                .or_insert_with(|| Box::new([0; PAGE as usize]));
            bytes[start..start + n].copy_from_slice(&rest[..n]);
            rest = &rest[n..];
            at += n as u64;
        }
        if !buf.is_empty() {
            self.len = self.len.max(offset + buf.len() as u64);
        }
    }

    fn shrink(&mut self, len: u64) {
        let gone: Vec<u64> = self
            .pages
            .range(len.div_ceil(PAGE)..)
            .map(|(p, _)| *p)
            .collect();
        for page in gone {
            self.mark(page, false);
            self.pages.remove(&page);
        }
        let cut = (len % PAGE) as usize;
        if cut != 0 && self.pages.contains_key(&(len / PAGE)) {
            self.mark(len / PAGE, false);
            if let Some(bytes) = self.pages.get_mut(&(len / PAGE)) {
                bytes[cut..].fill(0);
            }
        }
        self.len = len;
    }
}

impl State {
    fn tick(&mut self) -> io::Result<()> {
        if self.dead {
            return Err(eio());
        }
        self.ops += 1;
        if self.freeze_at.is_some_and(|n| self.ops >= n) {
            self.dead = true;
            return Err(eio());
        }
        Ok(())
    }

    fn live(&self) -> io::Result<()> {
        if self.dead { Err(eio()) } else { Ok(()) }
    }

    fn linked(&self) -> BTreeSet<u64> {
        self.dirs
            .values()
            .flat_map(|d| d.current.values().copied())
            .collect()
    }

    fn used(&self) -> u64 {
        self.linked()
            .iter()
            .filter_map(|i| self.inodes.get(i))
            .map(|i| i.len)
            .sum()
    }

    fn inode(&mut self, ino: u64) -> io::Result<&mut Inode> {
        self.inodes.get_mut(&ino).ok_or_else(eio)
    }

    fn grow_room(&self, from: u64, to: u64) -> u64 {
        let free = self.capacity.saturating_sub(self.used());
        to.saturating_sub(from).min(free)
    }

    fn lose_power(
        &mut self,
        rng: &mut ChaCha8Rng,
        mode: Crash,
    ) -> (BTreeMap<PathBuf, Dir>, BTreeMap<u64, Inode>) {
        let keep = |rng: &mut ChaCha8Rng| mode == Crash::Random && rng.random_bool(0.5);
        let mut dirs = BTreeMap::new();
        for (path, dir) in &self.dirs {
            let mut entries = dir.synced.clone();
            for change in &dir.pending {
                if !keep(rng) {
                    continue;
                }
                match change {
                    Change::Link(name, ino) => {
                        entries.insert(name.clone(), *ino);
                    }
                    Change::Unlink(name) => {
                        entries.remove(name);
                    }
                    Change::Rename(from, to) => {
                        if let Some(ino) = entries.remove(from) {
                            entries.insert(to.clone(), ino);
                        }
                    }
                }
            }
            let dir = Dir {
                synced: entries.clone(),
                current: entries,
                pending: Vec::new(),
            };
            dirs.insert(path.clone(), dir);
        }
        let linked: BTreeSet<u64> = dirs
            .values()
            .flat_map(|d: &Dir| d.current.values().copied())
            .collect();
        let mut inodes = BTreeMap::new();
        for (ino, mut inode) in std::mem::take(&mut self.inodes) {
            if !linked.contains(&ino) {
                continue;
            }
            let len = if keep(rng) {
                inode.len
            } else {
                inode.synced_len
            };
            for (page, dirty) in std::mem::take(&mut inode.dirty) {
                if !keep(rng) {
                    match dirty.before {
                        Some(bytes) => inode.pages.insert(page, bytes),
                        None => inode.pages.remove(&page),
                    };
                } else if dirty.written && rng.random_ratio(1, 4) {
                    let mut torn: Page = match rng.random_range(0..3) {
                        0 => dirty.before.unwrap_or_else(|| Box::new([0; PAGE as usize])),
                        1 => Box::new([0; PAGE as usize]),
                        _ => {
                            let mut bytes = Box::new([0; PAGE as usize]);
                            rng.fill(&mut bytes[..]);
                            bytes
                        }
                    };
                    if let Some(new) = inode.pages.get(&page) {
                        let cut = rng.random_range(0..PAGE as usize);
                        torn[..cut].copy_from_slice(&new[..cut]);
                    }
                    inode.pages.insert(page, torn);
                }
            }
            inode.len = inode.len.max(len);
            inode.shrink(len);
            inode.dirty.clear();
            inode.synced_len = len;
            inodes.insert(ino, inode);
        }
        (dirs, inodes)
    }
}

impl FaultFs {
    pub fn new(seed: u64, capacity: u64) -> Self {
        Self::from_state(State {
            rng: ChaCha8Rng::seed_from_u64(seed),
            dead: false,
            ops: 0,
            freeze_at: None,
            capacity,
            write_error_per_mille: 0,
            sync_error_per_mille: 0,
            read_error_per_mille: 0,
            next_write_error: None,
            next_sync_error: false,
            no_fallocate: false,
            inodes: BTreeMap::new(),
            next_ino: 1,
            dirs: BTreeMap::new(),
        })
    }

    fn from_state(state: State) -> Self {
        Self {
            state: Arc::new(Mutex::new(state)),
            gate: Arc::default(),
        }
    }

    pub fn hold_syncs(&self) {
        lock(&self.gate.held).0 = true;
    }

    pub fn release_syncs(&self) {
        lock(&self.gate.held).0 = false;
        self.gate.changed.notify_all();
    }

    pub fn syncs_held(&self) -> usize {
        lock(&self.gate.held).1
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        lock(&self.state)
    }

    pub fn set_fault_rates(&self, write_per_mille: u32, sync_per_mille: u32) {
        let mut s = self.lock();
        s.write_error_per_mille = write_per_mille;
        s.sync_error_per_mille = sync_per_mille;
    }

    pub fn set_read_fault_rate(&self, per_mille: u32) {
        self.lock().read_error_per_mille = per_mille;
    }

    pub fn fail_next_write(&self, kind: io::ErrorKind, written: usize) {
        self.lock().next_write_error = Some((kind, written));
    }

    pub fn fail_next_sync(&self) {
        self.lock().next_sync_error = true;
    }

    pub fn set_capacity(&self, capacity: u64) {
        self.lock().capacity = capacity;
    }

    pub fn ops(&self) -> u64 {
        self.lock().ops
    }

    pub fn used(&self) -> u64 {
        self.lock().used()
    }

    pub fn refuse_allocate(&self) {
        self.lock().no_fallocate = true;
    }

    pub fn freeze_after(&self, ops: u64) {
        let mut s = self.lock();
        s.freeze_at = Some(s.ops + ops);
    }

    pub fn crash(&self, mode: Crash) -> FaultFs {
        let mut old = self.lock();
        old.dead = true;
        let mut rng = old.rng.clone();
        let (dirs, inodes) = match mode {
            Crash::Kill => (
                std::mem::take(&mut old.dirs),
                std::mem::take(&mut old.inodes),
            ),
            _ => old.lose_power(&mut rng, mode),
        };
        FaultFs::from_state(State {
            rng,
            dead: false,
            ops: 0,
            freeze_at: None,
            capacity: old.capacity,
            write_error_per_mille: old.write_error_per_mille,
            sync_error_per_mille: old.sync_error_per_mille,
            read_error_per_mille: 0,
            next_write_error: None,
            next_sync_error: false,
            no_fallocate: old.no_fallocate,
            inodes,
            next_ino: old.next_ino,
            dirs,
        })
    }

    pub fn snapshot(&self) -> FaultFs {
        Self::from_state(self.lock().clone())
    }

    pub fn flip(&self) -> Option<(PathBuf, u64)> {
        let mut s = self.lock();
        let pages: Vec<(PathBuf, u64, u64, u64)> = s
            .dirs
            .iter()
            .flat_map(|(dir, d)| d.current.iter().map(move |(name, ino)| (dir, name, *ino)))
            .filter(|(_, name, _)| name.ends_with(".vdl"))
            .filter_map(|(dir, name, ino)| Some((dir.join(name), ino, s.inodes.get(&ino)?)))
            .flat_map(|(path, ino, inode)| {
                let synced = inode.synced_len;
                inode
                    .pages
                    .range(..synced.div_ceil(PAGE))
                    .filter(|(page, _)| !inode.dirty.contains_key(page))
                    .map(move |(page, _)| {
                        let start = page * PAGE;
                        (path.clone(), ino, start, (synced - start).min(PAGE))
                    })
            })
            .collect();
        if pages.is_empty() {
            return None;
        }
        let State { rng, inodes, .. } = &mut *s;
        let (path, ino, start, span) = pages[rng.random_range(0..pages.len())].clone();
        let offset = start + rng.random_range(0..span);
        let mask: u8 = rng.random_range(1..=255);
        let inode = inodes.get_mut(&ino)?;
        let (page, at) = (offset / PAGE, (offset % PAGE) as usize);
        inode
            .pages
            .entry(page)
            .or_insert_with(|| Box::new([0; PAGE as usize]))[at] ^= mask;
        Some((path, offset))
    }
}

impl FaultFs {
    pub fn flip_at(&self, path: &Path, offset: u64, mask: u8) -> bool {
        let mut s = self.lock();
        let Some(ino) = path
            .parent()
            .and_then(|dir| s.dirs.get(dir))
            .and_then(|d| d.current.get(path.file_name()?.to_str()?))
            .copied()
        else {
            return false;
        };
        let Some(inode) = s.inodes.get_mut(&ino) else {
            return false;
        };
        let page = offset / PAGE;
        if offset >= inode.synced_len || inode.dirty.contains_key(&page) {
            return false;
        }
        inode
            .pages
            .entry(page)
            .or_insert_with(|| Box::new([0; PAGE as usize]))[(offset % PAGE) as usize] ^= mask;
        true
    }
}

impl FsFile for FaultFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let mut s = lock(&self.state);
        s.live()?;
        let rate = s.read_error_per_mille;
        if rate > 0 && s.rng.random_ratio(rate, 1000) {
            return Err(eio());
        }
        let inode = s.inode(self.ino)?;
        if offset + buf.len() as u64 > inode.len {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let mut at = offset;
        let mut rest = buf;
        while !rest.is_empty() {
            let start = (at % PAGE) as usize;
            let n = rest.len().min(PAGE as usize - start);
            let (head, tail) = rest.split_at_mut(n);
            match inode.pages.get(&(at / PAGE)) {
                Some(page) => head.copy_from_slice(&page[start..start + n]),
                None => head.fill(0),
            }
            rest = tail;
            at += n as u64;
        }
        Ok(())
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        let mut s = lock(&self.state);
        s.tick()?;
        let mut error = s.next_write_error.take();
        if error.is_none() && s.write_error_per_mille > 0 {
            let rate = s.write_error_per_mille;
            if s.rng.random_ratio(rate, 1000) {
                let kind = if s.rng.random_bool(0.5) {
                    io::ErrorKind::StorageFull
                } else {
                    io::ErrorKind::Other
                };
                error = Some((kind, s.rng.random_range(0..=buf.len())));
            }
        }
        let mut n = error.map_or(buf.len(), |(_, written)| written.min(buf.len()));
        let len = s.inode(self.ino)?.len;
        let end = offset + n as u64;
        if end > len && s.grow_room(len, end) < end - len {
            n = (len + s.grow_room(len, end)).saturating_sub(offset) as usize;
            error = error.or(Some((io::ErrorKind::StorageFull, n)));
        }
        s.inode(self.ino)?.write(&buf[..n], offset);
        match error {
            Some((kind, _)) => Err(kind.into()),
            None => Ok(()),
        }
    }

    fn allocate(&self, len: u64) -> io::Result<()> {
        let mut s = lock(&self.state);
        s.tick()?;
        if s.no_fallocate {
            return Err(io::ErrorKind::Unsupported.into());
        }
        let current = s.inode(self.ino)?.len;
        if len > current {
            let room = s.grow_room(current, len);
            s.inode(self.ino)?.len = current + room;
            if room < len - current {
                return Err(io::ErrorKind::StorageFull.into());
            }
        }
        Ok(())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        let mut s = lock(&self.state);
        s.tick()?;
        let current = s.inode(self.ino)?.len;
        if len > current && s.grow_room(current, len) < len - current {
            return Err(io::ErrorKind::StorageFull.into());
        }
        let inode = s.inode(self.ino)?;
        if len < current {
            inode.shrink(len);
        } else {
            inode.len = len;
        }
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        let mut held = lock(&self.gate.held);
        if held.0 {
            held.1 += 1;
            while held.0 {
                held = self
                    .gate
                    .changed
                    .wait(held)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            held.1 -= 1;
        }
        drop(held);
        let mut s = lock(&self.state);
        s.tick()?;
        let rate = s.sync_error_per_mille;
        let fail =
            std::mem::take(&mut s.next_sync_error) || (rate > 0 && s.rng.random_ratio(rate, 1000));
        let inode = s.inode(self.ino)?;
        if fail {
            inode
                .dirty
                .values_mut()
                .for_each(|dirty| dirty.failed = true);
            return Err(eio());
        }
        inode.dirty.retain(|_, dirty| dirty.failed);
        inode.synced_len = inode.len;
        Ok(())
    }

    fn size(&self) -> io::Result<u64> {
        let mut s = lock(&self.state);
        s.live()?;
        Ok(s.inode(self.ino)?.len)
    }
}

impl Fs for FaultFs {
    type File = FaultFile;

    fn create(&self, path: &Path) -> io::Result<FaultFile> {
        let (parent, name) = split(path)?;
        let mut s = self.lock();
        s.tick()?;
        let ino = s.next_ino;
        let dir = s.dirs.entry(parent).or_default();
        if dir.current.contains_key(&name) {
            return Err(io::ErrorKind::AlreadyExists.into());
        }
        dir.current.insert(name.clone(), ino);
        dir.pending.push(Change::Link(name, ino));
        s.next_ino += 1;
        s.inodes.insert(ino, Inode::default());
        Ok(FaultFile {
            state: Arc::clone(&self.state),
            gate: Arc::clone(&self.gate),
            ino,
        })
    }

    fn open(&self, path: &Path) -> io::Result<FaultFile> {
        let (parent, name) = split(path)?;
        let s = self.lock();
        s.live()?;
        let ino = s
            .dirs
            .get(&parent)
            .and_then(|d| d.current.get(&name))
            .copied()
            .ok_or(io::ErrorKind::NotFound)?;
        Ok(FaultFile {
            state: Arc::clone(&self.state),
            gate: Arc::clone(&self.gate),
            ino,
        })
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        let mut s = self.lock();
        s.tick()?;
        if let Some(d) = s.dirs.get_mut(dir) {
            d.synced = d.current.clone();
            d.pending.clear();
        }
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let (parent, from) = split(from)?;
        let (to_parent, to) = split(to)?;
        if parent != to_parent {
            return Err(io::ErrorKind::Unsupported.into());
        }
        let mut s = self.lock();
        s.tick()?;
        let dir = s.dirs.get_mut(&parent).ok_or(io::ErrorKind::NotFound)?;
        let ino = dir.current.remove(&from).ok_or(io::ErrorKind::NotFound)?;
        dir.current.insert(to.clone(), ino);
        dir.pending.push(Change::Rename(from, to));
        Ok(())
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        let (parent, name) = split(path)?;
        let mut s = self.lock();
        s.tick()?;
        let dir = s.dirs.get_mut(&parent).ok_or(io::ErrorKind::NotFound)?;
        dir.current.remove(&name).ok_or(io::ErrorKind::NotFound)?;
        dir.pending.push(Change::Unlink(name));
        Ok(())
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
        let s = self.lock();
        s.live()?;
        Ok(s.dirs
            .get(dir)
            .map(|d| d.current.keys().cloned().collect())
            .unwrap_or_default())
    }

    fn statfs(&self, _path: &Path) -> io::Result<FsStat> {
        let s = self.lock();
        s.live()?;
        Ok(FsStat {
            memory_backed: false,
            total: s.capacity,
            free: s.capacity.saturating_sub(s.used()),
        })
    }
}
