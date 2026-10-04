use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};

use tokio::sync::oneshot;
use verdandi_core::ids::SegmentId;

use crate::block::{
    BLOCK_HEADER_LEN, BlockHeader, FILE_HEADER_LEN, FileHeader, FileKind, MAX_BLOCK, PAGE,
    SEGMENT_BYTES, align, encode_block,
};
use crate::fs::{Fs, FsFile};
use crate::recovery::Recovered;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("no space left on the device")]
    NoSpace,
    #[error("a block of {0} bytes is over the 32 MiB cap")]
    TooLarge(usize),
    #[error("the log stopped after an earlier I/O error")]
    Stopped,
    #[error("the data directory {} is on a memory-backed file system (tmpfs or ramfs)", .0.display())]
    MemoryBacked(PathBuf),
    #[error("{}: format version {version} is unknown, this build reads version 1", path.display())]
    UnknownVersion { path: PathBuf, version: u16 },
    #[error("{}: {len} bytes is more than the 64 MiB file limit", path.display())]
    Oversize { path: PathBuf, len: u64 },
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Written {
    pub segment: SegmentId,
    pub offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealPoint {
    pub file: PathBuf,
    pub end: u64,
}

pub fn segment_file_name(segment: SegmentId) -> String {
    format!("seg-{segment}.vdl")
}

pub fn seal_at<F: Fs>(fs: &F, point: &SealPoint) -> Result<(), Error> {
    seal_file(&fs.open(&point.file)?, point.end)?;
    Ok(())
}

enum Command {
    Append {
        commit_ms: u64,
        body: Vec<u8>,
        preallocate: u64,
        reply: oneshot::Sender<Result<Written, Error>>,
    },
    Seal {
        reply: oneshot::Sender<Result<(), Error>>,
    },
}

pub struct Log {
    epochs: Arc<AtomicU64>,
    tx: Option<mpsc::Sender<Command>>,
    writer: Option<JoinHandle<()>>,
    done: Option<oneshot::Receiver<()>>,
}

impl Log {
    pub fn start<F: Fs>(fs: F, incarnation: u64, recovered: Recovered) -> Self {
        let (tx, rx) = mpsc::channel();
        let (done_tx, done) = oneshot::channel();
        let epochs = Arc::new(AtomicU64::new(recovered.next_epoch));
        let mut writer = Writer {
            fs,
            dir: recovered.dir,
            incarnation,
            next_seq: 0,
            epochs: Arc::clone(&epochs),
            active: None,
            stopped: false,
        };
        let handle = thread::spawn(move || {
            for command in rx {
                match command {
                    Command::Append {
                        commit_ms,
                        body,
                        preallocate,
                        reply,
                    } => reply
                        .send(writer.append(commit_ms, &body, preallocate))
                        .ok(),
                    Command::Seal { reply } => reply.send(writer.seal_command()).ok(),
                };
            }
            drop(writer);
            done_tx.send(()).ok();
        });
        Self {
            epochs,
            tx: Some(tx),
            writer: Some(handle),
            done: Some(done),
        }
    }

    pub fn epochs(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.epochs)
    }

    pub async fn close(mut self) {
        self.tx = None;
        if let Some(done) = self.done.take() {
            done.await.ok();
        }
    }

    pub fn append(
        &self,
        commit_ms: u64,
        body: Vec<u8>,
        preallocate: u64,
    ) -> oneshot::Receiver<Result<Written, Error>> {
        let (reply, rx) = oneshot::channel();
        self.send(Command::Append {
            commit_ms,
            body,
            preallocate,
            reply,
        });
        rx
    }

    pub fn seal(&self) -> oneshot::Receiver<Result<(), Error>> {
        let (reply, rx) = oneshot::channel();
        self.send(Command::Seal { reply });
        rx
    }

    fn send(&self, command: Command) {
        if let Some(tx) = &self.tx {
            tx.send(command).ok();
        }
    }
}

impl Drop for Log {
    fn drop(&mut self) {
        self.tx = None;
        if let Some(handle) = self.writer.take()
            && let Err(panic) = handle.join()
            && !thread::panicking()
        {
            std::panic::resume_unwind(panic);
        }
    }
}

struct Active<F: FsFile> {
    file: F,
    segment: SegmentId,
    epoch: u64,
    end: u64,
    dir_synced: bool,
}

struct Writer<F: Fs> {
    fs: F,
    dir: PathBuf,
    incarnation: u64,
    next_seq: u32,
    epochs: Arc<AtomicU64>,
    active: Option<Active<F::File>>,
    stopped: bool,
}

fn full(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::StorageFull | io::ErrorKind::QuotaExceeded
    )
}

fn space(e: io::Error) -> Error {
    if full(&e) {
        Error::NoSpace
    } else {
        Error::Io(e)
    }
}

impl<F: Fs> Writer<F> {
    fn guarded<T>(&mut self, op: impl FnOnce(&mut Self) -> Result<T, Error>) -> Result<T, Error> {
        if self.stopped {
            return Err(Error::Stopped);
        }
        let result = op(self);
        if let Err(Error::Io(_)) = result {
            self.stopped = true;
        }
        result
    }

    fn append(&mut self, commit_ms: u64, body: &[u8], preallocate: u64) -> Result<Written, Error> {
        let total = BLOCK_HEADER_LEN + body.len();
        if total > MAX_BLOCK {
            return Err(Error::TooLarge(total));
        }
        self.guarded(|w| w.write_block(commit_ms, body, total as u64, preallocate))
    }

    fn seal_command(&mut self) -> Result<(), Error> {
        self.guarded(Self::seal)
    }

    fn write_block(
        &mut self,
        commit_ms: u64,
        body: &[u8],
        total: u64,
        preallocate: u64,
    ) -> Result<Written, Error> {
        if self.active.as_ref().is_some_and(|a| !fits(a.end, total)) {
            self.seal()?;
        }
        let active = match self.active.take() {
            Some(active) => active,
            None => self.open_segment(PAGE + total, preallocate)?,
        };
        let active = self.active.insert(active);
        let offset = next_offset(active.end);
        let bytes = encode_block(
            &BlockHeader {
                segment: active.segment,
                offset,
                epoch: active.epoch,
                compaction_version: 0,
                commit_ms,
            },
            body,
        );
        if let Err(e) = active.file.write_at(&bytes, offset) {
            let error = space(e);
            if matches!(error, Error::NoSpace) && active.end == FILE_HEADER_LEN as u64 {
                let path = self.dir.join(segment_file_name(active.segment));
                self.active = None;
                self.remove_segment(&path)?;
            } else {
                self.seal()?;
            }
            return Err(error);
        }
        if let Err(e) = active.file.sync_data() {
            active.file.set_len(active.end)?;
            return Err(e.into());
        }
        if !active.dir_synced {
            self.fs.sync_dir(&self.dir)?;
            active.dir_synced = true;
        }
        active.end = offset + total;
        Ok(Written {
            segment: active.segment,
            offset,
        })
    }

    fn open_segment(&mut self, first: u64, preallocate: u64) -> Result<Active<F::File>, Error> {
        let segment = SegmentId {
            incarnation: self.incarnation,
            seq: self.next_seq,
        };
        self.next_seq += 1;
        let epoch = self.epochs.fetch_add(1, Ordering::SeqCst);
        let path = self.dir.join(segment_file_name(segment));
        let file = self.fs.create(&path).map_err(space)?;
        let header = FileHeader {
            kind: FileKind::Segment,
            epoch,
        };
        let written = match file.allocate(preallocate.max(first)) {
            Err(e) if full(&e) => file.set_len(0).and_then(|()| file.allocate(first)),
            Err(e) if e.kind() == io::ErrorKind::Unsupported => Ok(()),
            allocated => allocated,
        }
        .and_then(|()| file.write_at(&header.encode(), 0));
        if let Err(e) = written {
            self.remove_segment(&path)?;
            return Err(space(e));
        }
        Ok(Active {
            file,
            segment,
            epoch,
            end: FILE_HEADER_LEN as u64,
            dir_synced: false,
        })
    }

    fn remove_segment(&self, path: &Path) -> io::Result<()> {
        self.fs.remove(path)?;
        self.fs.sync_dir(&self.dir)
    }

    fn seal(&mut self) -> Result<(), Error> {
        if let Some(active) = self.active.take() {
            seal_file(&active.file, active.end)?;
        }
        Ok(())
    }
}

pub(crate) fn next_offset(end: u64) -> u64 {
    align(end).max(PAGE)
}

pub fn fits(end: u64, block_len: u64) -> bool {
    next_offset(end) + block_len <= SEGMENT_BYTES
}

pub(crate) fn seal_file(file: &impl FsFile, end: u64) -> io::Result<()> {
    file.set_len(end)?;
    file.sync_data()
}
