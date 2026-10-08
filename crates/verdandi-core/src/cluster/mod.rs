pub mod controller;
pub mod domains;
pub mod fence;
pub mod members;
pub mod placement;
pub mod repair;
pub mod steer;
pub mod view;

pub fn fnv(words: impl IntoIterator<Item = u64>) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for word in words {
        for byte in word.to_le_bytes() {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3);
        }
    }
    hash
}

#[cfg(test)]
pub(crate) mod testkit {
    use super::view::{Body, Head, State, Update, View};

    pub struct Rng(pub u64);

    impl Rng {
        pub fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        pub fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    pub fn body(holds: u64) -> Body {
        Body {
            holds,
            host: holds,
            ..Body::default()
        }
    }

    pub fn update(disk: u64, restart: u64, heartbeat: u64, state: State, body: Body) -> Update {
        Update {
            disk,
            head: Head {
                restart,
                heartbeat,
                state,
            },
            body: Some(((restart, 1), body)),
        }
    }

    pub fn view(me: u64, peers: &[u64], now_ms: u64, run_ms: u64) -> View {
        let mut view = View::new(me, 1, body(me), now_ms, run_ms);
        let updates: Vec<Update> = peers
            .iter()
            .map(|p| update(*p, 1, 1, State::Alive, body(*p)))
            .collect();
        view.merge(&updates, now_ms, run_ms);
        view
    }

    pub fn beat(view: &mut View, peers: &[u64], heartbeat: u64, now_ms: u64, run_ms: u64) {
        let updates: Vec<Update> = peers
            .iter()
            .filter_map(|p| view.entry(*p).map(|e| (*p, e.clone())))
            .map(|(p, e)| Update {
                disk: p,
                head: Head {
                    heartbeat,
                    ..e.head
                },
                body: None,
            })
            .collect();
        view.merge(&updates, now_ms, run_ms);
    }
}
