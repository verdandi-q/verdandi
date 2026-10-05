use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};

use tokio::sync::{mpsc as channel, oneshot};
use verdandi_core::ids::{Address, SegmentId};

use crate::block::{
    BLOCK_HEADER_LEN, BlockHeader, FILE_HEADER_LEN, FileHeader, FileKind, encode_block,
};
use crate::fs::{Fs, FsFile};
use crate::log::{fits, next_offset};
use crate::records::{self, Control, RunEntry};
use crate::recovery::{Expect, expect_for, temp_name, walk};

pub const COMPACT_FREE: u64 = 192 << 20;
const NICE: i32 = 10;

pub(crate) struct Input {
    pub(crate) path: PathBuf,
    pub(crate) segments: BTreeSet<SegmentId>,
    pub(crate) pinned: BTreeSet<SegmentId>,
    pub(crate) recheck: bool,
}

pub(crate) struct Job {
    pub(crate) inputs: Vec<Input>,
}

pub(crate) struct Scanned {
    pub(crate) segment: SegmentId,
    pub(crate) commit_ms: u64,
    pub(crate) runs: Vec<u32>,
    pub(crate) controls: Vec<Control>,
}

pub(crate) struct Keep {
    pub(crate) runs: Vec<bool>,
    pub(crate) controls: Vec<Control>,
}

pub(crate) struct Output {
    pub(crate) path: PathBuf,
    pub(crate) file: Arc<dyn FsFile>,
    pub(crate) len: u64,
    pub(crate) bodies: u64,
    pub(crate) carried: u64,
    pub(crate) written: u64,
    pub(crate) first_ms: u64,
    pub(crate) segments: Vec<SegmentId>,
    pub(crate) runs: Vec<(Address, u64)>,
}

pub(crate) enum Report {
    Ask(Vec<Scanned>, oneshot::Sender<Vec<Keep>>),
    Damaged(Vec<(Vec<SegmentId>, Range<u64>)>, oneshot::Sender<bool>),
    Output(Output),
    Deleted(PathBuf, Vec<SegmentId>),
    Failed,
    Done,
}

pub(crate) struct Compactor {
    jobs: Option<mpsc::Sender<Job>>,
    pub(crate) reports: channel::UnboundedReceiver<Report>,
    thread: Option<JoinHandle<()>>,
}

impl Compactor {
    pub(crate) fn start<F: Fs>(fs: F, dir: PathBuf, epochs: Arc<AtomicU64>) -> io::Result<Self> {
        let (jobs, rx) = mpsc::channel::<Job>();
        let (tx, reports) = channel::unbounded_channel();
        let (niced_tx, niced) = mpsc::channel();
        let thread = thread::spawn(move || {
            let result = rustix::process::setpriority_process(Some(rustix::thread::gettid()), NICE);
            let ok = result.is_ok();
            niced_tx.send(result).ok();
            if !ok {
                return;
            }
            let pass = Pass {
                fs,
                dir,
                epochs,
                tx,
            };
            for job in rx {
                pass.run(&job);
            }
        });
        niced
            .recv()
            .map_err(|_| io::Error::other("the compaction thread stopped at start"))??;
        Ok(Self {
            jobs: Some(jobs),
            reports,
            thread: Some(thread),
        })
    }

    pub(crate) fn send(&self, job: Job) -> bool {
        self.jobs.as_ref().is_some_and(|j| j.send(job).is_ok())
    }

    pub(crate) async fn close(mut self) {
        self.jobs = None;
        while self.reports.recv().await.is_some() {}
        if let Some(thread) = self.thread.take()
            && let Err(panic) = thread.join()
        {
            std::panic::resume_unwind(panic);
        }
    }
}

type Moves = Vec<(Address, u64)>;
type Copies = Vec<(SegmentId, Vec<Moved>)>;

struct Moved {
    header: BlockHeader,
    body: Vec<u8>,
    entries: Vec<RunEntry>,
    positions: Vec<u32>,
}

enum Stop {
    Skip,
    Failed,
    Gone,
}

#[derive(Default)]
struct Out {
    end: u64,
    segments: Copies,
}

fn end_after(mut end: u64, segments: &[(SegmentId, Vec<Moved>)]) -> Option<u64> {
    for copy in segments.iter().flat_map(|(_, copies)| copies) {
        let total = (BLOCK_HEADER_LEN + copy.body.len()) as u64;
        if !fits(end, total) {
            return None;
        }
        end = next_offset(end) + total;
    }
    Some(end)
}

fn full(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::StorageFull | io::ErrorKind::QuotaExceeded
    )
}

struct Pass<F: Fs> {
    fs: F,
    dir: PathBuf,
    epochs: Arc<AtomicU64>,
    tx: channel::UnboundedSender<Report>,
}

impl<F: Fs> Pass<F> {
    fn report(&self, report: Report) -> Result<(), Stop> {
        self.tx.send(report).map_err(|_| Stop::Gone)
    }

    fn run(&self, job: &Job) {
        let report = match self.compact(job) {
            Ok(()) | Err(Stop::Skip) => Report::Done,
            Err(Stop::Failed) => Report::Failed,
            Err(Stop::Gone) => return,
        };
        self.tx.send(report).ok();
    }

    fn compact(&self, job: &Job) -> Result<(), Stop> {
        let mut out = Out {
            end: FILE_HEADER_LEN as u64,
            segments: Vec::new(),
        };
        let mut waiting: Vec<(&Input, Vec<SegmentId>)> = Vec::new();
        let mut done: Vec<(&Input, Vec<SegmentId>)> = Vec::new();
        let mut plain: Vec<(&Input, Vec<SegmentId>)> = Vec::new();
        let mut room = true;
        for input in &job.inputs {
            let scanned = match self.scan(input) {
                Ok(Some(scanned)) => scanned,
                Ok(None) | Err(Stop::Skip) => continue,
                Err(stop) => return Err(stop),
            };
            let (empty, copies): (Copies, Copies) =
                scanned.into_iter().partition(|(_, c)| c.is_empty());
            let gone: Vec<SegmentId> = empty.into_iter().map(|(s, _)| s).collect();
            if gone.iter().any(|s| input.pinned.contains(s)) {
                continue;
            }
            if copies.is_empty() {
                plain.push((input, gone));
                continue;
            }
            if !room {
                continue;
            }
            if end_after(out.end, &copies).is_none() {
                self.delete(std::mem::take(&mut plain))?;
                if self.flush(&mut out)? {
                    done.append(&mut waiting);
                } else {
                    waiting.clear();
                    room = false;
                    continue;
                }
            }
            if let Some(end) = end_after(out.end, &copies) {
                out.end = end;
                out.segments.extend(copies);
                waiting.push((input, gone));
            }
        }
        self.delete(plain)?;
        if room && self.flush(&mut out)? {
            done.append(&mut waiting);
        }
        self.delete(done)
    }

    fn delete(&self, inputs: Vec<(&Input, Vec<SegmentId>)>) -> Result<(), Stop> {
        for (input, _) in &inputs {
            self.fs.remove(&input.path).map_err(|_| Stop::Failed)?;
        }
        if !inputs.is_empty() {
            self.fs.sync_dir(&self.dir).map_err(|_| Stop::Failed)?;
        }
        for (input, lost) in inputs {
            self.report(Report::Deleted(input.path.clone(), lost))?;
        }
        Ok(())
    }

    fn scan(&self, input: &Input) -> Result<Option<Copies>, Stop> {
        let expect = input
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(expect_for)
            .ok_or(Stop::Failed)?;
        let file = self.fs.open(&input.path).map_err(|_| Stop::Skip)?;
        let len = file.size().map_err(|_| Stop::Skip)?;
        let mut data = vec![0; len as usize];
        file.read_at(&mut data, 0).map_err(|_| Stop::Skip)?;
        let walked = walk(&data, &expect).map_err(|_| Stop::Failed)?;
        let mut damaged: Vec<(Vec<SegmentId>, Range<u64>)> = walked
            .damaged
            .iter()
            .map(|range| {
                let named: Vec<SegmentId> = match &expect {
                    Expect::Segment(segment) => vec![*segment],
                    Expect::Any => input.segments.iter().copied().collect(),
                };
                (
                    named
                        .into_iter()
                        .filter(|s| input.segments.contains(s))
                        .collect(),
                    range.clone(),
                )
            })
            .collect();
        let mut blocks = Vec::new();
        for (header, body) in &walked.blocks {
            if !input.segments.contains(&header.segment) {
                continue;
            }
            let decoded = records::decode(body).and_then(|b| {
                let positions = b
                    .runs
                    .iter()
                    .map(|(_, run)| records::first_position(run))
                    .collect::<Result<Vec<u32>, _>>()?;
                Ok((b, positions))
            });
            match decoded {
                Ok((body, positions)) => blocks.push((*header, body, positions)),
                Err(_) => damaged.push((
                    vec![header.segment],
                    header.offset..header.offset + (BLOCK_HEADER_LEN + body.len()) as u64,
                )),
            }
        }
        let same = input.recheck && damaged.is_empty() && blocks.len() == walked.blocks.len();
        if !damaged.is_empty() {
            let (reply, recorded) = oneshot::channel();
            self.report(Report::Damaged(damaged, reply))?;
            if !recorded.blocking_recv().map_err(|_| Stop::Gone)? {
                return Err(Stop::Skip);
            }
        }
        let asked = blocks
            .iter()
            .map(|(header, body, positions)| Scanned {
                segment: header.segment,
                commit_ms: header.commit_ms,
                runs: positions.clone(),
                controls: body.controls.clone(),
            })
            .collect();
        let (reply, answer) = oneshot::channel();
        self.report(Report::Ask(asked, reply))?;
        let keeps = answer.blocking_recv().map_err(|_| Stop::Gone)?;
        if same
            && blocks.iter().zip(&keeps).all(|((_, body, _), keep)| {
                keep.runs.iter().all(|k| *k) && keep.controls.len() == body.controls.len()
            })
        {
            return Ok(None);
        }
        let mut out: BTreeMap<SegmentId, Vec<Moved>> =
            input.segments.iter().map(|s| (*s, Vec::new())).collect();
        for ((header, body, positions), keep) in blocks.into_iter().zip(keeps) {
            let mut runs = Vec::new();
            let mut kept = Vec::new();
            for (((entry, bytes), position), keep) in body.runs.iter().zip(positions).zip(keep.runs)
            {
                if keep {
                    runs.push((entry.stream.as_str(), entry.count, *bytes));
                    kept.push(position);
                }
            }
            if runs.is_empty() && keep.controls.is_empty() {
                continue;
            }
            let (body, entries) = records::assemble(&runs, &keep.controls);
            out.entry(header.segment).or_default().push(Moved {
                header,
                body,
                entries,
                positions: kept,
            });
        }
        Ok(Some(out.into_iter().collect()))
    }

    fn flush(&self, out: &mut Out) -> Result<bool, Stop> {
        let segments = std::mem::take(&mut out.segments);
        let planned = std::mem::replace(&mut out.end, FILE_HEADER_LEN as u64);
        if segments.is_empty() {
            return Ok(true);
        }
        let free = self.fs.statfs(&self.dir).map_or(0, |s| s.free);
        if free < COMPACT_FREE + planned {
            return Ok(false);
        }
        let epoch = self.epochs.fetch_add(1, Ordering::SeqCst);
        let temp = self.dir.join(temp_name(epoch));
        let path = self.dir.join(format!("cmp-{epoch:016x}.vdl"));
        let written = match self.write(&temp, epoch, &segments) {
            Ok(written) => written,
            Err(e) => {
                if let Err(gone) = self.fs.remove(&temp)
                    && gone.kind() != io::ErrorKind::NotFound
                {
                    return Err(Stop::Failed);
                }
                return if full(&e) {
                    Ok(false)
                } else {
                    Err(Stop::Failed)
                };
            }
        };
        self.fs.rename(&temp, &path).map_err(|_| Stop::Failed)?;
        self.fs.sync_dir(&self.dir).map_err(|_| Stop::Failed)?;
        let (file, len, runs) = written;
        let copies = segments.iter().flat_map(|(_, copies)| copies);
        let bodies: u64 = copies.clone().map(|c| c.body.len() as u64).sum();
        let blocks = copies.clone().count() as u64;
        let run_bytes: u64 = copies
            .flat_map(|c| &c.entries)
            .map(|e| u64::from(e.len))
            .sum();
        let ids: Vec<SegmentId> = segments.iter().map(|(s, _)| *s).collect();
        self.report(Report::Output(Output {
            path,
            file: Arc::new(file),
            len,
            bodies,
            carried: bodies - run_bytes,
            written: FILE_HEADER_LEN as u64 + blocks * BLOCK_HEADER_LEN as u64 + bodies,
            first_ms: segments
                .iter()
                .flat_map(|(_, copies)| copies.iter().map(|c| c.header.commit_ms))
                .min()
                .unwrap_or(0),
            segments: ids,
            runs,
        }))?;
        Ok(true)
    }

    fn write(
        &self,
        temp: &Path,
        epoch: u64,
        segments: &[(SegmentId, Vec<Moved>)],
    ) -> io::Result<(F::File, u64, Moves)> {
        let file = self.fs.create(temp)?;
        let header = FileHeader {
            kind: FileKind::Compacted,
            epoch,
        };
        file.write_at(&header.encode(), 0)?;
        let mut end = FILE_HEADER_LEN as u64;
        let mut runs = Vec::new();
        for (segment, copies) in segments {
            for copy in copies {
                let offset = next_offset(end);
                let bytes = encode_block(
                    &BlockHeader {
                        segment: *segment,
                        offset,
                        epoch,
                        compaction_version: copy.header.compaction_version + 1,
                        commit_ms: copy.header.commit_ms,
                    },
                    &copy.body,
                );
                file.write_at(&bytes, offset)?;
                for (position, entry) in copy.positions.iter().zip(&copy.entries) {
                    let start = Address {
                        segment: *segment,
                        position: *position,
                    };
                    runs.push((
                        start,
                        offset + BLOCK_HEADER_LEN as u64 + u64::from(entry.offset),
                    ));
                }
                end = offset + bytes.len() as u64;
            }
        }
        file.sync_data()?;
        Ok((file, end, runs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::RealFs;
    use crate::log::Log;
    use crate::records::Message;
    use crate::recovery::recover;
    use verdandi_core::Timing;
    use verdandi_core::ids::MessageId;

    #[test]
    fn a_recheck_input_whose_runs_are_all_kept_at_ask_time_writes_no_file() {
        let scratch = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/tmp");
        std::fs::create_dir_all(&scratch).expect("scratch dir");
        let tmp = tempfile::TempDir::new_in(&scratch).expect("temp dir");
        let dir = tmp.path();
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let message = Message {
            position: 0,
            id: MessageId::new(1_000, 1, 0).expect("id"),
            timing: Timing::default(),
            origin: None,
            headers: Vec::new(),
            value: b"acked".to_vec(),
        };
        let (body, _) = records::encode(&[("s", std::slice::from_ref(&message))], &[]);
        let log = Log::start(
            RealFs,
            1,
            recover(&RealFs, dir, |_, _, _| {}).expect("recover"),
        );
        rt.block_on(log.append(1_000, body, 0))
            .expect("reply")
            .expect("append");
        rt.block_on(log.close());
        let mut segments = BTreeSet::new();
        recover(&RealFs, dir, |_, header, _| {
            segments.insert(header.segment);
        })
        .expect("recover");
        let files = || {
            let mut names: Vec<String> = std::fs::read_dir(dir)
                .expect("list")
                .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
                .collect();
            names.sort();
            names
        };
        let before = files();
        let [name] = before.as_slice() else {
            panic!("one segment file: {before:?}");
        };
        let mut compactor =
            Compactor::start(RealFs, dir.to_path_buf(), Arc::default()).expect("compactor");
        assert!(compactor.send(Job {
            inputs: vec![Input {
                path: dir.join(name),
                segments,
                pinned: BTreeSet::new(),
                recheck: true,
            }],
        }));
        loop {
            match rt.block_on(compactor.reports.recv()) {
                Some(Report::Ask(scanned, reply)) => {
                    let keeps = scanned
                        .into_iter()
                        .map(|s| Keep {
                            runs: vec![true; s.runs.len()],
                            controls: s.controls,
                        })
                        .collect();
                    reply.send(keeps).ok();
                }
                Some(Report::Done) => break,
                Some(Report::Output(output)) => panic!("wrote {}", output.path.display()),
                _ => panic!("an unexpected report"),
            }
        }
        rt.block_on(compactor.close());
        assert_eq!(files(), before);
    }
}
