use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use verdandi_sim::compaction_sim::{self, boot, publish, runtime, seal, stop, take};
use verdandi_sim::fault_fs::FaultFs;
use verdandi_store::engine::TestClock;
use verdandi_store::recovery::recover;

const SEEDS: u64 = 100;

#[test]
fn fault_compaction_a_crash_at_every_step_loses_and_duplicates_nothing() {
    let mut steps = 0;
    let mut crashes = 0;
    for seed in 0..SEEDS {
        let report = compaction_sim::crash_at_every_step(seed);
        steps += report.steps;
        crashes += report.crashes;
    }
    println!("fault_compaction seeds={SEEDS} steps={steps} crashes={crashes}");
    assert!(steps >= SEEDS * 5, "too few compaction steps: {steps}");
}

#[test]
fn fault_compaction_an_input_whose_output_passes_64_mib_splits_by_source_segment() {
    let rt = runtime();
    let fs = FaultFs::new(1, 1 << 40);
    let clock = TestClock::new(1_000_000);
    let node = boot(&rt, &fs, &clock);
    let big = vec![7u8; 1 << 20];
    let values: Vec<&[u8]> = (0..24).map(|_| &big[..]).collect();
    let mut kept = Vec::new();
    for _ in 0..3 {
        kept.extend(publish(&rt, &node, "keep", &values));
        publish(&rt, &node, "junk", &values);
        publish(&rt, &node, "junk", &values[..1]);
        seal(&rt, &node, &clock);
    }
    rt.block_on(node.handle.delete_stream("junk".to_owned()))
        .expect("delete");
    seal(&rt, &node, &clock);
    let mut owner = BTreeMap::new();
    let mut outputs = BTreeSet::new();
    recover(&fs.snapshot(), Path::new("/data"), |path, header, _| {
        let first = owner
            .entry(header.segment)
            .or_insert_with(|| path.to_path_buf());
        assert_eq!(first, path, "segment {} is in two files", header.segment);
        if path.to_string_lossy().contains("cmp-") {
            outputs.insert(path.to_path_buf());
        }
    })
    .expect("recover");
    assert_eq!(outputs.len(), 2, "{outputs:?}");
    let mut handed = take(&rt, &node, "g", "keep");
    handed.sort();
    kept.sort();
    assert_eq!(handed, kept);
    assert!(take(&rt, &node, "h", "junk").is_empty());
    stop(&rt, node).expect("clean stop");
}

#[test]
fn fault_compaction_under_pressure_a_partial_placement_never_brings_deleted_data_back() {
    compaction_sim::pressure_with_a_partial_placement_then_more_passes_and_a_crash();
}

#[test]
fn fault_compaction_a_deleted_group_stays_deleted_after_compaction_and_a_crash_at_every_step() {
    let report = compaction_sim::forget_then_compaction_with_a_crash_at_every_step(5);
    assert!(report.steps > 0, "the pass did nothing");
}

#[test]
fn fault_compaction_a_compacted_input_with_two_segments_past_64_mib_goes_whole_at_every_crash() {
    let report = compaction_sim::two_segment_input_past_64_mib_with_a_crash_at_every_step();
    println!(
        "fault_compaction two segments steps={} crashes={}",
        report.steps, report.crashes
    );
    assert!(report.steps > 0, "the pass did nothing");
}
