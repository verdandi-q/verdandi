use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::Extension;
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use futures_util::future::join_all;
use serde::Deserialize;
use serde_json::value::RawValue;
use verdandi_core::lease::Receipt;
use verdandi_net::{Hello, seal};

use crate::error::Fail;
use crate::hello::{Cluster, HOP_ADDR};

pub const HOP: &str = "x-vd-hop";
pub const PARTIAL: &str = "x-vd-partial";
const HTTP_PORT: u16 = 8080;
const CALL: Duration = Duration::from_secs(5);

pub type Item = (String, Box<RawValue>);

pub struct Peers {
    pub me: u64,
    pub addrs: BTreeMap<u64, String>,
    pub leaders: BTreeSet<u64>,
}

pub(crate) struct Route {
    cluster: Arc<Cluster>,
    peers: Peers,
    pub(crate) hop: bool,
}

#[derive(Deserialize)]
struct Reply {
    messages: Vec<Box<RawValue>>,
}

#[derive(Deserialize)]
struct Ident {
    id: String,
}

fn client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(CALL)
            .build()
            .unwrap_or_default()
    })
}

fn json(body: Bytes) -> Response {
    ([(header::CONTENT_TYPE, "application/json")], body).into_response()
}

impl Route {
    pub(crate) fn of(
        cluster: Option<Extension<Arc<Cluster>>>,
        headers: &HeaderMap,
    ) -> Result<Option<Route>, Fail> {
        let Some(Extension(cluster)) = cluster else {
            return Ok(None);
        };
        let Some(peers) = cluster.peers.as_ref().map(|peers| peers()) else {
            return Ok(None);
        };
        let hop = match headers.get(HOP) {
            None => false,
            Some(value) => {
                let sealed = value
                    .to_str()
                    .ok()
                    .and_then(|text| STANDARD.decode(text).ok())
                    .unwrap_or_default();
                if cluster.accept(&sealed)?.addr != HOP_ADDR {
                    return Err(Fail::new(
                        StatusCode::FORBIDDEN,
                        "the hop header is not valid",
                        "send the request without the hop header",
                    ));
                }
                true
            }
        };
        Ok(Some(Route {
            cluster,
            peers,
            hop,
        }))
    }

    pub(crate) fn foreign(&self, receipt: Receipt) -> bool {
        !self.hop && receipt.issuer() != self.peers.me
    }

    fn token(&self) -> Option<String> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
        let hello = Hello {
            node: self.peers.me,
            addr: HOP_ADDR.to_owned(),
            incarnation: 0,
            sent_ms: u64::try_from(now.as_millis()).ok()?,
            cluster: None,
        };
        seal(&self.cluster.key, &hello)
            .ok()
            .map(|sealed| STANDARD.encode(sealed))
    }

    pub(crate) fn me(&self) -> u64 {
        self.peers.me
    }

    pub(crate) async fn call(
        &self,
        id: u64,
        method: Method,
        path: &str,
        body: Bytes,
    ) -> Option<(StatusCode, Bytes)> {
        let mut addr: SocketAddr = self.peers.addrs.get(&id)?.parse().ok()?;
        addr.set_port(HTTP_PORT);
        let sent = client()
            .request(method, format!("http://{addr}{path}"))
            .header(HOP, self.token()?)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .ok()?;
        let status = sent.status();
        Some((status, sent.bytes().await.ok()?))
    }

    pub(crate) async fn take(&self, path: &str) -> Option<Response> {
        for id in self.peers.leaders.iter().filter(|id| **id != self.peers.me) {
            if let Some((StatusCode::OK, body)) =
                self.call(*id, Method::GET, path, Bytes::new()).await
            {
                return Some(json(body));
            }
        }
        None
    }

    pub(crate) async fn settle(
        &self,
        receipt: Receipt,
        method: Method,
        body: Bytes,
    ) -> Result<Response, Fail> {
        let path = format!("/r/{receipt}");
        match self.call(receipt.issuer(), method, &path, body).await {
            Some((status, body)) => Ok((status, json(body)).into_response()),
            None => Err(Fail::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "the node that issued the receipt does not answer",
                "retry later; the messages come again when the lease ends",
            )),
        }
    }

    pub(crate) async fn merge(
        &self,
        local: Vec<Item>,
        path: &str,
        limit: usize,
    ) -> (Vec<Item>, usize) {
        let asked = self.peers.addrs.keys().filter(|id| **id != self.peers.me);
        let answers =
            join_all(asked.map(|id| self.call(*id, Method::GET, path, Bytes::new()))).await;
        let mut all: BTreeMap<String, Box<RawValue>> = local.into_iter().collect();
        let mut missing = 0;
        for answer in answers {
            let Some((StatusCode::OK, body)) = answer else {
                missing += 1;
                continue;
            };
            let Ok(reply) = serde_json::from_slice::<Reply>(&body) else {
                missing += 1;
                continue;
            };
            for raw in reply.messages {
                if let Ok(Ident { id }) = serde_json::from_str(raw.get()) {
                    all.insert(id, raw);
                }
            }
        }
        (all.into_iter().take(limit).collect(), missing)
    }
}
