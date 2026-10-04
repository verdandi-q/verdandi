mod soak;

use std::time::Duration;

#[test]
#[ignore]
fn consumer_churn_soak() {
    soak::run(&soak::Config {
        duration: Duration::from_secs(3600),
        kill_every: Duration::from_secs(60),
        require_phases: true,
    });
}

#[test]
#[ignore]
fn smoke_consumer_churn() {
    soak::run(&soak::Config {
        duration: Duration::from_secs(30),
        kill_every: Duration::from_secs(5),
        require_phases: false,
    });
}
