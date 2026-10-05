use std::collections::BTreeMap;

use super::fnv;
use super::view::Body;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Domains {
    pub of: BTreeMap<u64, u64>,
    pub hash: u64,
}

impl Domains {
    pub fn count(&self) -> usize {
        let mut labels: Vec<u64> = self.of.values().copied().collect();
        labels.sort_unstable();
        labels.dedup();
        labels.len()
    }
}

fn root(parent: &mut [usize], mut i: usize) -> usize {
    while parent[i] != i {
        parent[i] = parent[parent[i]];
        i = parent[i];
    }
    i
}

fn join(parent: &mut [usize], a: usize, b: usize) {
    let (ra, rb) = (root(parent, a), root(parent, b));
    parent[ra.max(rb)] = ra.min(rb);
}

pub fn infer(live: &[(u64, &Body)], co_losses: &BTreeMap<(u64, u64), u32>) -> Domains {
    let mut nodes: Vec<(u64, &Body)> = live.to_vec();
    nodes.sort_unstable_by_key(|(disk, _)| *disk);
    let index: BTreeMap<u64, usize> = nodes
        .iter()
        .enumerate()
        .map(|(i, (d, _))| (*d, i))
        .collect();
    let mut parent: Vec<usize> = (0..nodes.len()).collect();
    if nodes.iter().any(|(_, b)| !b.zone.is_empty()) {
        let mut zones: BTreeMap<&str, usize> = BTreeMap::new();
        for (i, (_, body)) in nodes.iter().enumerate() {
            let first = *zones.entry(body.zone.as_str()).or_insert(i);
            join(&mut parent, first, i);
        }
    } else {
        for (i, (_, a)) in nodes.iter().enumerate() {
            for (j, (_, b)) in nodes.iter().enumerate().skip(i + 1) {
                if a.host == b.host || (!a.prefix.is_empty() && a.prefix == b.prefix) {
                    join(&mut parent, i, j);
                }
            }
        }
        let trips: Vec<(usize, usize, u32)> = nodes
            .iter()
            .enumerate()
            .flat_map(|(i, (_, body))| {
                body.round_trips
                    .iter()
                    .filter_map(|(peer, micros)| index.get(peer).map(|j| (i, *j, (*micros).max(1))))
                    .collect::<Vec<_>>()
            })
            .collect();
        let mut times: Vec<u32> = trips.iter().map(|t| t.2).collect();
        times.sort_unstable();
        let gap = times
            .windows(2)
            .map(|w| (f64::from(w[1]) / f64::from(w[0]), w[0]))
            .max_by(|a, b| a.0.total_cmp(&b.0));
        if let Some((ratio, below)) = gap
            && ratio >= 2.0
        {
            for (i, j, micros) in &trips {
                if *micros <= below {
                    join(&mut parent, *i, *j);
                }
            }
        }
        for ((a, b), count) in co_losses {
            if let (Some(i), Some(j)) = (index.get(a), index.get(b))
                && *count >= 2
            {
                join(&mut parent, *i, *j);
            }
        }
    }
    let of: BTreeMap<u64, u64> = (0..nodes.len())
        .map(|i| (nodes[i].0, nodes[root(&mut parent, i)].0))
        .collect();
    let hash = fnv(of.iter().flat_map(|(disk, label)| [*disk, *label]));
    Domains { of, hash }
}

pub fn agree(result: Domains, reports: &[u64], last: Option<Domains>) -> Option<Domains> {
    let same = reports.iter().filter(|h| **h == result.hash).count();
    if 2 * same > reports.len() {
        Some(result)
    } else {
        last
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::view::{TRIP_PEERS, trip_peers};

    fn node(host: u64) -> Body {
        Body {
            host,
            prefix: format!("10.{host}.0.0/24"),
            ..Body::default()
        }
    }

    fn domains(bodies: &[Body], co_losses: &BTreeMap<(u64, u64), u32>) -> Domains {
        let live: Vec<(u64, &Body)> = bodies
            .iter()
            .enumerate()
            .map(|(i, b)| (i as u64 + 1, b))
            .collect();
        infer(&live, co_losses)
    }

    fn same(d: &Domains, a: u64, b: u64) -> bool {
        d.of[&a] == d.of[&b]
    }

    #[test]
    fn independent_nodes_are_separate_domains() {
        let d = domains(&[node(1), node(2), node(3)], &BTreeMap::new());
        assert_eq!(d.count(), 3);
    }

    #[test]
    fn a_shared_host_joins() {
        let mut b = node(1);
        b.prefix = node(2).prefix;
        let d = domains(&[node(1), b, node(3)], &BTreeMap::new());
        assert!(same(&d, 1, 2));
        assert_eq!(d.count(), 2);
    }

    #[test]
    fn a_shared_prefix_joins() {
        let mut b = node(2);
        b.prefix = node(1).prefix;
        let d = domains(&[node(1), b, node(3)], &BTreeMap::new());
        assert!(same(&d, 1, 2));
        assert_eq!(d.count(), 2);
    }

    fn trips(fast: u32, slow: u32) -> Vec<Body> {
        let mut bodies = vec![node(1), node(2), node(3), node(4)];
        let near = |a: u64, b: u64| (a <= 2) == (b <= 2);
        for (i, body) in bodies.iter_mut().enumerate() {
            let me = i as u64 + 1;
            body.round_trips = (1..=4)
                .filter(|p| *p != me)
                .map(|p| (p, if near(me, p) { fast } else { slow }))
                .collect();
        }
        bodies
    }

    #[test]
    fn a_round_trip_gap_of_ratio_two_joins_below_the_gap() {
        let d = domains(&trips(500, 1_000), &BTreeMap::new());
        assert!(same(&d, 1, 2));
        assert!(same(&d, 3, 4));
        assert!(!same(&d, 1, 3));
        assert_eq!(d.count(), 2);
    }

    #[test]
    fn a_round_trip_ratio_under_two_joins_nothing() {
        let d = domains(&trips(500, 999), &BTreeMap::new());
        assert_eq!(d.count(), 4);
    }

    #[test]
    fn two_co_losses_join_and_one_does_not() {
        let bodies = [node(1), node(2), node(3)];
        let d = domains(&bodies, &BTreeMap::from([((1, 3), 2)]));
        assert!(same(&d, 1, 3));
        assert_eq!(d.count(), 2);
        let d = domains(&bodies, &BTreeMap::from([((1, 3), 1)]));
        assert_eq!(d.count(), 3);
    }

    #[test]
    fn round_trips_capped_to_the_trip_peers_still_find_each_zone() {
        let n = 150;
        let near = |a: u64, b: u64| a % 3 == b % 3;
        let bodies: Vec<Body> = (1..=n)
            .map(|me| {
                let all: Vec<(u64, u32)> = (1..=n)
                    .filter(|p| *p != me)
                    .map(|p| (p, if near(me, p) { 300 } else { 900 }))
                    .collect();
                Body {
                    round_trips: trip_peers(me, &all),
                    ..node(me)
                }
            })
            .collect();
        assert!(bodies.iter().all(|b| b.round_trips.len() == TRIP_PEERS));
        let d = domains(&bodies, &BTreeMap::new());
        assert_eq!(d.count(), 3);
        assert!(same(&d, 1, 4) && !same(&d, 1, 2));
    }

    #[test]
    fn a_known_zone_wins_over_inference() {
        let mut bodies = vec![node(1), node(1), node(3), node(4)];
        for (body, zone) in bodies.iter_mut().zip(["a", "b", "a", "c"]) {
            body.zone = zone.to_owned();
        }
        let d = domains(&bodies, &BTreeMap::new());
        assert!(!same(&d, 1, 2));
        assert!(same(&d, 1, 3));
        assert_eq!(d.count(), 3);
    }

    #[test]
    fn a_result_counts_only_with_more_than_half_of_the_live_nodes() {
        let result = domains(&[node(1), node(2), node(3), node(4)], &BTreeMap::new());
        let other = domains(&[node(1), node(1), node(3), node(4)], &BTreeMap::new());
        let h = result.hash;
        assert_eq!(agree(result.clone(), &[h, h, 7, 8], None), None);
        assert_eq!(
            agree(result.clone(), &[h, h, 7, 8], Some(other.clone())),
            Some(other)
        );
        assert_eq!(agree(result.clone(), &[h, h, h, 8], None), Some(result));
    }
}
