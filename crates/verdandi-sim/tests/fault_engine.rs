use verdandi_sim::engine_sim;

const SEEDS: u64 = 100;

#[test]
fn fault_engine_loses_no_acked_message_over_100_seeds() {
    let mut total = engine_sim::Report::default();
    for seed in 0..SEEDS {
        let report = engine_sim::run(seed);
        total.published += report.published;
        total.acked += report.acked;
        total.crashes += report.crashes;
        total.flips += report.flips;
        total.named += report.named;
        total.dead += report.dead;
        total.served += report.served;
        total.repeats += report.repeats;
    }
    println!("fault_engine seeds={SEEDS} {total:?}");
    assert!(
        total.published > 1_000,
        "too few acked publishes: {total:?}"
    );
    assert!(total.acked > 500, "too few acked settles: {total:?}");
    assert!(total.named > 20, "too few named flips: {total:?}");
    assert!(
        total.named * 2 > total.flips,
        "most flips are not inside a block: {total:?}"
    );
    assert!(total.dead > 50, "too few dead copies: {total:?}");
    assert!(total.repeats > 100, "too few key repeats: {total:?}");
}

#[test]
fn batch_atomic() {
    let checked: usize = (0..SEEDS).map(engine_sim::batch_atomic).sum();
    println!("batch_atomic seeds={SEEDS} messages={checked}");
    assert!(checked > 1_000, "too few messages checked: {checked}");
}
