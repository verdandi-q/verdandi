use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::Extension;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use verdandi_net::{Hello, open};

use crate::error::Fail;

pub const SKEW_MS: u64 = 30_000;
const SEEN_FOR: Duration = Duration::from_secs(60);
const SEEN_MAX: usize = 4096;

pub const HOP_ADDR: &str = "hop";

type PressureFn = Box<dyn Fn() -> (Option<u64>, Option<u64>) + Send + Sync>;

type CopiesFn = Box<dyn Fn() -> (u8, u8, u8) + Send + Sync>;
type PeersFn = Box<dyn Fn() -> crate::route::Peers + Send + Sync>;

pub struct Cluster {
    pub(crate) key: Vec<u8>,
    pub(crate) peers: Option<PeersFn>,
    hello: Box<dyn Fn(Hello) -> Vec<u8> + Send + Sync>,
    status: Box<dyn Fn() -> String + Send + Sync>,
    seen: Mutex<HashMap<u64, Instant>>,
    pub(crate) health: Option<Health>,
    pub(crate) ask: Option<crate::nodes::Ask>,
    pub(crate) scale: Option<Box<dyn Fn() -> (usize, u8) + Send + Sync>>,
    pub(crate) copies: Option<CopiesFn>,
    pub(crate) pressure: Option<PressureFn>,
}

pub struct Health {
    pub held: Box<dyn Fn() -> bool + Send + Sync>,
    pub stopping: Box<dyn Fn() -> bool + Send + Sync>,
}

impl Cluster {
    pub fn new(
        key: Vec<u8>,
        hello: Box<dyn Fn(Hello) -> Vec<u8> + Send + Sync>,
        status: Box<dyn Fn() -> String + Send + Sync>,
    ) -> Cluster {
        Cluster {
            key,
            peers: None,
            hello,
            status,
            seen: Mutex::default(),
            health: None,
            ask: None,
            scale: None,
            copies: None,
            pressure: None,
        }
    }

    pub fn with_health(mut self, health: Health) -> Cluster {
        self.health = Some(health);
        self
    }

    pub fn with_ask(mut self, ask: crate::nodes::Ask) -> Cluster {
        self.ask = Some(ask);
        self
    }

    pub fn with_scale(mut self, scale: Box<dyn Fn() -> (usize, u8) + Send + Sync>) -> Cluster {
        self.scale = Some(scale);
        self
    }

    pub fn with_copies(mut self, copies: CopiesFn) -> Cluster {
        self.copies = Some(copies);
        self
    }

    pub fn with_pressure(mut self, pressure: PressureFn) -> Cluster {
        self.pressure = Some(pressure);
        self
    }

    pub fn with_peers(mut self, peers: PeersFn) -> Cluster {
        self.peers = Some(peers);
        self
    }

    pub(crate) fn accept(&self, sealed: &[u8]) -> Result<Hello, Fail> {
        let bad = || refuse(StatusCode::BAD_REQUEST, "the hello is not valid");
        let hello = open(&self.key, sealed).map_err(|_| bad())?;
        let now = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_millis()),
        )
        .unwrap_or(0);
        if hello.sent_ms.abs_diff(now) > SKEW_MS {
            return Err(refuse(StatusCode::BAD_REQUEST, "the hello clock is off"));
        }
        Ok(hello)
    }

    fn fresh(&self, sealed: &[u8]) -> bool {
        let mut hasher = DefaultHasher::new();
        sealed.hash(&mut hasher);
        let id = hasher.finish();
        let mut seen = self.seen.lock().unwrap();
        seen.retain(|_, at| at.elapsed() < SEEN_FOR);
        if seen.len() >= SEEN_MAX || seen.contains_key(&id) {
            return false;
        }
        seen.insert(id, Instant::now());
        true
    }
}

fn refuse(status: StatusCode, why: &'static str) -> Fail {
    Fail::new(status, why, "send a fresh sealed hello")
}

pub async fn hello(
    Extension(cluster): Extension<Arc<Cluster>>,
    body: Bytes,
) -> Result<impl IntoResponse, Fail> {
    let hello = cluster.accept(&body)?;
    if !cluster.fresh(&body) {
        return Err(refuse(
            StatusCode::CONFLICT,
            "the hello is a repeat, or too many hellos came",
        ));
    }
    if hello.addr == HOP_ADDR {
        return Err(refuse(StatusCode::BAD_REQUEST, "the hello is not valid"));
    }
    let reply = (cluster.hello)(hello);
    if reply.is_empty() {
        return Err(refuse(StatusCode::INTERNAL_SERVER_ERROR, "no reply"));
    }
    Ok(reply)
}

pub async fn status(Extension(cluster): Extension<Arc<Cluster>>) -> String {
    (cluster.status)()
}

#[cfg(test)]
mod tests {
    use super::*;
    use verdandi_net::seal;

    fn token(node: u64, addr: &str) -> Vec<u8> {
        let sent_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap();
        let hello = Hello {
            node,
            addr: addr.to_owned(),
            incarnation: 0,
            sent_ms,
            cluster: None,
        };
        seal(b"0123456789abcdef0123456789abcdef", &hello).unwrap()
    }

    #[test]
    fn many_hop_tokens_pass_and_a_hello_still_works_after_them() {
        let cluster = Cluster::new(
            b"0123456789abcdef0123456789abcdef".to_vec(),
            Box::new(|_| Vec::new()),
            Box::new(String::new),
        );
        for node in 0..(SEEN_MAX as u64 + 100) {
            assert!(cluster.accept(&token(node, HOP_ADDR)).is_ok(), "hop {node}");
        }
        assert!(cluster.fresh(&token(1, "10.0.0.1:7946")));
    }
}
