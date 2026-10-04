use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use verdandi_store::block::{BLOCK_HEADER_LEN, SEGMENT_BYTES};
use verdandi_store::log::{Error, Log, seal_at, segment_file_name};
use verdandi_store::recovery::{Recovered, Warning, recover};

use crate::fault_fs::{Crash, FaultFs};

const ROUNDS: usize = 6;

#[derive(Debug, Default)]
pub struct Report {
    pub blocks: usize,
    pub flips: usize,
}

struct Durable {
    file: PathBuf,
    offset: u64,
    end: u64,
}

fn body(rng: &mut ChaCha8Rng, tag: u64) -> Vec<u8> {
    let mut body = tag.to_le_bytes().to_vec();
    let mut rest = vec![0; rng.random_range(0..12_000)];
    rng.fill(&mut rest[..]);
    body.extend_from_slice(&rest);
    body
}

type Seen = Vec<(PathBuf, u64, Vec<u8>)>;

fn recover_round(
    fs: &FaultFs,
    dir: &Path,
    warnings: &mut Vec<Warning>,
) -> Result<(Seen, Recovered), Error> {
    let mut seen = Vec::new();
    let mut recovered = recover(fs, dir, |path, header, body| {
        seen.push((path.to_path_buf(), header.offset, body.to_vec()))
    })?;
    warnings.append(&mut recovered.warnings);
    for point in &recovered.unsealed {
        seal_at(fs, point)?;
    }
    Ok((seen, recovered))
}

pub fn run(seed: u64) -> Report {
    let mut report = Report::default();
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let dir = Path::new("/data");
    let capacity = if rng.random_bool(0.3) {
        200 << 20
    } else {
        1 << 40
    };
    let mut fs = FaultFs::new(seed, capacity);
    let mut submitted: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    let mut durable: BTreeMap<u64, Durable> = BTreeMap::new();
    let mut flipped: BTreeSet<u64> = BTreeSet::new();
    let (_, mut recovered) = recover_round(&fs, dir, &mut Vec::new()).expect("recover");
    let mut tag = 0;
    for round in 0..ROUNDS {
        fs.set_fault_rates(rng.random_range(0..40), rng.random_range(0..15));
        if rng.random_bool(0.7) {
            fs.freeze_after(rng.random_range(1..80));
        }
        let log = Log::start(fs.clone(), rng.random(), recovered);
        let mut replies = Vec::new();
        for _ in 0..rng.random_range(1..25) {
            if rng.random_bool(0.1) {
                drop(log.seal());
            }
            tag += 1;
            let body = body(&mut rng, tag);
            replies.push((tag, log.append(round as u64, body.clone(), SEGMENT_BYTES)));
            submitted.insert(tag, body);
        }
        for (tag, reply) in replies {
            if let Ok(Ok(written)) = reply.blocking_recv() {
                let len = (BLOCK_HEADER_LEN + submitted[&tag].len()) as u64;
                durable.insert(
                    tag,
                    Durable {
                        file: dir.join(segment_file_name(written.segment)),
                        offset: written.offset,
                        end: written.offset + len,
                    },
                );
            }
        }
        drop(log);
        let mode = match rng.random_range(0..10) {
            0..6 => Crash::Random,
            6..8 => Crash::LoseAll,
            _ => Crash::Kill,
        };
        fs = fs.crash(mode);
        fs.set_fault_rates(0, 0);
        let mut hits = Vec::new();
        for _ in 0..rng.random_range(0..3) {
            let Some((file, at)) = fs.flip() else {
                continue;
            };
            for (tag, d) in &durable {
                if d.file == file && (d.offset..d.end).contains(&at) {
                    flipped.insert(*tag);
                    hits.push((file.clone(), d.offset));
                }
            }
        }
        let mut warnings = Vec::new();
        if rng.random_bool(0.3) {
            fs.freeze_after(rng.random_range(1..6));
        }
        let (seen, next) = match recover_round(&fs, dir, &mut warnings) {
            Ok(done) => done,
            Err(error) => {
                assert!(matches!(error, Error::Io(_)), "seed {seed}: {error}");
                fs = fs.crash(Crash::Random);
                recover_round(&fs, dir, &mut warnings).expect("recovery succeeds")
            }
        };
        recovered = next;
        let mut seen_tags = BTreeSet::new();
        for (file, offset, body) in &seen {
            let mut id = [0; 8];
            id.copy_from_slice(&body[..8]);
            let tag = u64::from_le_bytes(id);
            assert_eq!(
                submitted.get(&tag),
                Some(body),
                "seed {seed}: unknown block"
            );
            assert!(seen_tags.insert(tag), "seed {seed}: block {tag} seen twice");
            assert!(
                !flipped.contains(&tag),
                "seed {seed}: flipped block {tag} served"
            );
            let d = durable.entry(tag).or_insert(Durable {
                file: file.clone(),
                offset: *offset,
                end: offset + (BLOCK_HEADER_LEN + body.len()) as u64,
            });
            assert_eq!(
                (&d.file, d.offset),
                (file, *offset),
                "seed {seed}: block {tag} moved"
            );
        }
        for tag in durable.keys().filter(|t| !flipped.contains(t)) {
            assert!(
                seen_tags.contains(tag),
                "seed {seed} round {round}: synced block {tag} lost"
            );
        }
        for (file, offset) in hits {
            assert!(
                warnings
                    .iter()
                    .any(|w| w.file == file && w.range.contains(&offset)),
                "seed {seed}: flip of the block at {offset} not named"
            );
        }
    }
    report.blocks = durable.len();
    report.flips = flipped.len();
    report
}
