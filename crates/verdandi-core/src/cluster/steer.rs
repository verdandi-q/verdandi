use super::view::Body;

pub const HEAVY_FILL: u64 = 10;
pub const HEAVY_HOLD_MS: u64 = 600_000;
const DAY_S: u64 = 86_400;

pub fn fill(body: &Body) -> u64 {
    body.disk_total
        .checked_sub(body.disk_free)
        .and_then(|used| (used * 100).checked_div(body.disk_total))
        .unwrap_or(0)
}

fn median(mut values: Vec<u64>) -> u64 {
    values.sort_unstable();
    values.get(values.len() / 2).copied().unwrap_or(0)
}

pub fn meets(me: &Body, live: &[&Body]) -> bool {
    let fill_median = median(live.iter().map(|b| fill(b)).collect());
    let rate_median = median(live.iter().map(|b| b.fill_rate).collect());
    fill(me) >= fill_median + HEAVY_FILL
        || (me.fill_rate > rate_median && 2 * me.fill_rate >= 3 * rate_median)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Hold {
    pub heavy: bool,
    since: Option<u64>,
}

impl Hold {
    pub fn sample(&mut self, meets: bool, run_ms: u64) -> bool {
        if meets == self.heavy {
            self.since = None;
        } else if run_ms - *self.since.get_or_insert(run_ms) >= HEAVY_HOLD_MS {
            self.heavy = meets;
            self.since = None;
        }
        self.heavy
    }
}

pub fn scale_hint(heavy: Option<u64>, until_full_s: Option<u64>, live: usize, floor: u8) -> Option<String> {
    if let Some(disk) = heavy {
        Some(format!("node {disk:016x} fills faster than its peers"))
    } else if until_full_s.is_some_and(|s| s < DAY_S) {
        Some("the cluster is full in under 24 h".to_owned())
    } else if live < usize::from(floor) {
        Some("fewer live nodes than the ack floor".to_owned())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::view::SAMPLE_MS;

    fn body(total: u64, free: u64, rate: u64) -> Body {
        Body {
            disk_total: total,
            disk_free: free,
            fill_rate: rate,
            ..Body::default()
        }
    }

    #[test]
    fn the_heavy_rule_is_ten_points_above_the_median_fill_or_one_and_a_half_times_its_rate() {
        let peers = [body(100, 60, 10), body(100, 60, 10), body(100, 60, 10)];
        let refs: Vec<&Body> = peers.iter().collect();
        assert!(meets(&body(100, 50, 10), &refs));
        assert!(!meets(&body(100, 51, 10), &refs));
        assert!(meets(&body(100, 60, 15), &refs));
        assert!(!meets(&body(100, 60, 14), &refs));
        let idle = [body(100, 60, 0), body(100, 60, 0)];
        let refs: Vec<&Body> = idle.iter().collect();
        assert!(!meets(&body(100, 60, 0), &refs));
        assert_eq!(fill(&body(0, 0, 0)), 0);
    }

    #[test]
    fn a_node_leaves_the_ring_only_after_the_hold_and_returns_only_after_the_hold() {
        let mut hold = Hold::default();
        let mut t = 0;
        while t < HEAVY_HOLD_MS {
            assert!(!hold.sample(true, t), "heavy at {t}");
            t += SAMPLE_MS;
        }
        assert!(hold.sample(true, t));
        let left = t + SAMPLE_MS;
        t = left;
        while t < left + HEAVY_HOLD_MS {
            assert!(hold.sample(false, t), "back at {t}");
            t += SAMPLE_MS;
        }
        assert!(!hold.sample(false, t));
    }

    #[test]
    fn a_node_that_meets_and_fails_in_turn_for_an_hour_changes_the_ring_at_most_once() {
        let mut hold = Hold::default();
        let mut last = false;
        let mut changes = 0;
        for i in 0..3_600_000 / SAMPLE_MS {
            let now = hold.sample(i % 2 == 0, i * SAMPLE_MS);
            if now != last {
                changes += 1;
                last = now;
            }
        }
        assert!(changes <= 1, "{changes}");
    }

    #[test]
    fn the_scale_hint_names_a_heavy_node_and_is_none_under_every_line() {
        assert_eq!(
            scale_hint(Some(0xab), None, 5, 3).as_deref(),
            Some("node 00000000000000ab fills faster than its peers")
        );
        assert!(scale_hint(None, Some(DAY_S - 1), 5, 3).is_some());
        assert!(scale_hint(None, None, 2, 3).is_some());
        assert_eq!(scale_hint(None, Some(DAY_S), 3, 3), None);
    }
}
