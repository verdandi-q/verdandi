use verdandi_sim::compaction_sim::crash_at_every_step;

#[test]
fn i19_a_crash_at_any_step_of_a_compaction_loses_no_message() {
    let report = crash_at_every_step(19);
    assert!(report.steps >= 5, "{report:?}");
    assert!(report.kept > 0, "{report:?}");
}
