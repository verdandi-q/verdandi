use std::collections::HashMap;
use std::sync::Arc;

use axum::Json;
use axum::extract::rejection::PathRejection;
use axum::extract::{Extension, Path, Query};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use serde_json::json;
use tokio::sync::oneshot;

use crate::error::Fail;
use crate::hello::Cluster;
use crate::route::Route;

#[derive(Clone, Debug, Serialize)]
pub struct NodeInfo {
    pub id: u64,
    pub address: String,
    pub member: bool,
    pub live: bool,
    pub draining: bool,
    pub stopping: bool,
    pub segments: usize,
}

pub enum Op {
    Info(u64),
    Drain,
    Accept(u64),
}

pub enum Reply {
    Info(Option<NodeInfo>),
    Done,
    Accept { ok: bool, quorum: bool },
}

pub type Ask = Box<dyn Fn(Op) -> oneshot::Receiver<Reply> + Send + Sync>;

fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |sum, (x, y)| sum | (x ^ y)) == 0
}

fn unauthorised() -> Fail {
    Fail::new(
        StatusCode::UNAUTHORIZED,
        "the cluster key is missing or wrong",
        "send Authorization: Bearer <cluster key>",
    )
}

fn node_id(path: Result<Path<String>, PathRejection>) -> Result<u64, Fail> {
    path.ok()
        .and_then(|Path(text)| text.parse().ok())
        .ok_or_else(|| {
            Fail::bad(
                "the node id is not a number",
                "use the id from GET /n/{node}",
            )
        })
}

async fn ask(cluster: &Cluster, op: Op) -> Result<Reply, Fail> {
    let down = || {
        Fail::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "the node does not answer",
            "retry later",
        )
    };
    let ask = cluster.ask.as_ref().ok_or_else(down)?;
    ask(op).await.map_err(|_| down())
}

fn gate(cluster: &Arc<Cluster>, headers: &HeaderMap, hop_ok: bool) -> Result<Option<Route>, Fail> {
    let route = Route::of(Some(Extension(cluster.clone())), headers)?;
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .is_some_and(|token| same(token.as_bytes(), &cluster.key));
    let hop = hop_ok && route.as_ref().is_some_and(|r| r.hop);
    if bearer || hop {
        Ok(route)
    } else {
        Err(unauthorised())
    }
}

pub async fn get(
    Extension(cluster): Extension<Arc<Cluster>>,
    headers: HeaderMap,
    path: Result<Path<String>, PathRejection>,
) -> Result<Response, Fail> {
    gate(&cluster, &headers, false)?;
    let node = node_id(path)?;
    match ask(&cluster, Op::Info(node)).await? {
        Reply::Info(Some(info)) => Ok(Json(info).into_response()),
        _ => Err(Fail::new(
            StatusCode::NOT_FOUND,
            "this node does not know that node",
            "use an id from GET /",
        )),
    }
}

pub async fn drain(
    Extension(cluster): Extension<Arc<Cluster>>,
    headers: HeaderMap,
    path: Result<Path<String>, PathRejection>,
) -> Result<Response, Fail> {
    let route = gate(&cluster, &headers, true)?;
    let node = node_id(path)?;
    let me = route.as_ref().map_or(0, |r| r.me());
    if node == me {
        ask(&cluster, Op::Drain).await?;
        return Ok(Json(json!({ "drain": "started" })).into_response());
    }
    let hop = route.as_ref().is_some_and(|r| r.hop);
    let sent = match route {
        Some(route) if !hop => {
            route
                .call(
                    node,
                    Method::POST,
                    &format!("/n/{node}/drain"),
                    Default::default(),
                )
                .await
        }
        _ => None,
    };
    match sent {
        Some((status, body)) => {
            Ok((status, [(header::CONTENT_TYPE, "application/json")], body).into_response())
        }
        None => Err(Fail::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "the node cannot be reached",
            "send the request to the node itself, or retry later",
        )),
    }
}

pub async fn accept_loss(
    Extension(cluster): Extension<Arc<Cluster>>,
    headers: HeaderMap,
    path: Result<Path<String>, PathRejection>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, Fail> {
    gate(&cluster, &headers, false)?;
    let node = node_id(path)?;
    if query.get("confirm").map(String::as_str) != Some(node.to_string().as_str()) {
        return Err(Fail::bad(
            "confirm does not equal the node id",
            "send ?confirm={node} with the same id as the path",
        ));
    }
    match ask(&cluster, Op::Accept(node)).await? {
        Reply::Accept { ok: false, .. } => Err(Fail::new(
            StatusCode::CONFLICT,
            "the node is live in this node's view",
            "accept a loss only for a node that is gone for good",
        )),
        Reply::Accept { quorum: false, .. } => Ok(Json(json!({ "list": "waits" })).into_response()),
        _ => Ok(Json(json!({ "accepted": node })).into_response()),
    }
}
