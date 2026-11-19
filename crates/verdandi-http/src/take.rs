use std::collections::BTreeMap;
use std::fmt::Display;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use axum::Json;
use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use serde_json::value::{RawValue, to_raw_value};
use tokio::sync::watch;
use tokio::time::Instant;
use utoipa::ToSchema;
use verdandi_core::ids::MessageId;
use verdandi_core::lease::Receipt;
use verdandi_core::names;
use verdandi_store::engine::{Delivery, Handle, Take, Taken};
use verdandi_store::records::Message;

use crate::error::{ErrorBody, Fail, NAMES, STOPPED};
use crate::publish::{essence, json_type};
use crate::reads;
use crate::route::Route;
use crate::{Cluster, Node, ready};

pub(crate) const LEASE_MAX_S: u64 = 43_200;
pub(crate) const LIMIT: usize = 100;
pub(crate) const LIMIT_MAX: usize = 10_000;
const LEASE_S: u64 = 30;
const WAIT_MAX_S: u64 = 60;
pub(crate) const REPLY_MAX: usize = 16 << 20;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Options {
    group: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) max_bytes: Option<usize>,
    pub(crate) lease: Option<u64>,
    pub(crate) wait: Option<u64>,
    pub(crate) from: Option<String>,
    pub(crate) follow: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct Leased {
    receipt: String,
    lease_until: String,
    #[schema(value_type = Vec<View>)]
    messages: Vec<Box<RawValue>>,
    next: Next,
}

// allow: the type exists only to give OpenAPI one schema for the two JSON replies
#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(as = TakeOrRead)]
#[serde(untagged)]
enum Reply {
    Leased(Leased),
    Page(reads::Page),
}

#[derive(Serialize, ToSchema)]
#[schema(as = TakeNext)]
struct Next {
    ack: String,
    settle: String,
}

#[derive(Serialize, ToSchema)]
#[schema(as = Message)]
pub(crate) struct View<'a> {
    id: String,
    stream: &'a str,
    at: String,
    #[serde(skip_serializing_if = "is_zero")]
    attempt: u32,
    #[schema(value_type = BTreeMap<String, String>)]
    headers: BTreeMap<&'a str, &'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<serde_json::Value>)]
    value: Option<Value<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    value_base64: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    original: Option<Original<'a>>,
}

#[derive(Serialize, ToSchema)]
struct Original<'a> {
    id: String,
    stream: &'a str,
    reason: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a str>,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

#[derive(Serialize)]
#[serde(untagged)]
enum Value<'a> {
    Json(&'a RawValue),
    Text(&'a str),
}

#[utoipa::path(
    get,
    path = "/s/{stream}",
    summary = "Take work as a group (with group), or read history (without group)",
    description = "A read with follow gives NDJSON. Send the header Accept: text/event-stream to get server-sent events.",
    params(("stream" = String, Path, description = "The stream name. A take also accepts a pattern."), ("group" = Option<String>, Query, description = "The group that takes the work. Without a group, the call reads history."), ("limit" = Option<usize>, Query, minimum = 1, maximum = 10000, description = "Most messages in the reply. Default 100."), ("max_bytes" = Option<usize>, Query, minimum = 0, maximum = 16777216, description = "A take only: most bytes of message JSON in the reply. Default 16777216. The reply holds at least one message."), ("lease" = Option<u64>, Query, minimum = 1, maximum = 43200, description = "A take only: seconds that you hold the messages. Default 30."), ("wait" = Option<u64>, Query, minimum = 0, maximum = 60, description = "A take only: seconds to wait for work. Default 0."), ("from" = Option<String>, Query, description = "A read only: an RFC 3339 time, -7d, now, or a message id (after that id)."), ("follow" = Option<String>, Query, description = "A read only: send each new message when it comes. It takes no value."), ("Last-Event-ID" = Option<String>, Header, description = "A read with follow resumes after this message id")),
    responses(
        (status = 200, description = "A take (group given) gives a lease. A read (no group) gives {\"messages\": [...], \"next\": \"...\"}. With follow, a read gives NDJSON or server-sent events.", content((Reply = "application/json"), (String = "application/x-ndjson"), (String = "text/event-stream"))),
        (status = 204, description = "No work. The Link header gives the next take."),
        (status = 400, description = "The request is not valid", body = ErrorBody),
        (status = 403, description = "A cross-origin or non-local request", body = ErrorBody),
        (status = 503, description = "The node is starting or stopping", body = ErrorBody),
    )
)]
pub async fn take(
    State(node): State<Node>,
    Extension(mut stopping): Extension<watch::Receiver<bool>>,
    cluster: Option<Extension<Arc<Cluster>>>,
    method: Method,
    headers: HeaderMap,
    path: Result<Path<String>, PathRejection>,
    query: Result<Query<Options>, QueryRejection>,
) -> Result<Response, Fail> {
    let handle = ready(&node)?;
    let route = Route::of(cluster, &headers)?;
    let Path(stream) = path.map_err(|e| Fail::bad(e.body_text(), "check the stream name"))?;
    let Query(mut options) =
        query.map_err(|e| Fail::bad(e.body_text(), "check the query parameters"))?;
    let Some(group) = options.group.take() else {
        return reads::read(handle, stopping, &headers, stream, options, route.as_ref()).await;
    };
    if options.from.is_some() || options.follow.is_some() {
        return Err(Fail::bad(
            "from and follow belong to a read, and a take has a group",
            format!(
                "read with GET /s/{stream}?from=now&follow, or take with GET /s/{stream}?group={group}"
            ),
        ));
    }
    if !names::valid_group(&group) {
        return Err(Fail::bad("the group name is not valid", NAMES));
    }
    if method == Method::HEAD {
        return Err(Fail::bad(
            "a HEAD cannot take, because its reply has no messages",
            format!("take with GET /s/{stream}?group={group}"),
        ));
    }
    let limit = limit(options.limit)?;
    let lease = seconds(options.lease.unwrap_or(LEASE_S), "lease")?;
    let wait = capped(options.wait.unwrap_or(0), WAIT_MAX_S, "wait")?;
    let max_bytes = capped(
        options.max_bytes.unwrap_or(REPLY_MAX),
        REPLY_MAX,
        "max_bytes",
    )?;
    let deadline = Instant::now() + Duration::from_secs(wait);
    let mut take = Take {
        group,
        selector: stream,
        limit,
        lease_ms: lease * 1000,
        wait: wait > 0,
    };
    let mut asked = false;
    loop {
        match handle.take(take.clone()).await? {
            Taken::Leased {
                receipt,
                until,
                messages,
            } => {
                let mut unreplied = Unreplied {
                    handle: handle.clone(),
                    receipt,
                    ids: messages.iter().map(|d| d.handed.id).collect(),
                };
                let reply = match leased(handle, receipt, until, &messages, max_bytes).await {
                    Ok(reply) => reply,
                    Err(error) => {
                        handle.release(receipt, unreplied.ids.clone()).await?;
                        unreplied.ids.clear();
                        return Err(error);
                    }
                };
                unreplied.ids.clear();
                return Ok(reply.unwrap_or_else(|| empty(&take, max_bytes)));
            }
            Taken::Empty => {
                let elsewhere = elsewhere(route.as_ref(), &mut asked, &take, max_bytes).await;
                return Ok(elsewhere.unwrap_or_else(|| empty(&take, max_bytes)));
            }
            Taken::Waiting { waiter, woken } => {
                if let Some(reply) = elsewhere(route.as_ref(), &mut asked, &take, max_bytes).await {
                    handle.cancel(waiter).await?;
                    return Ok(reply);
                }
                tokio::select! {
                    woken = tokio::time::timeout_at(deadline, woken) => match woken {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) => return Err(stopped()),
                        Err(_) => {
                            handle.cancel(waiter).await?;
                            take.wait = false;
                        }
                    },
                    _ = async { stopping.wait_for(|stop| *stop).await.is_ok() } => {
                        handle.cancel(waiter).await?;
                        return Err(stopped());
                    }
                }
            }
        }
    }
}

async fn elsewhere(
    route: Option<&Route>,
    asked: &mut bool,
    take: &Take,
    max_bytes: usize,
) -> Option<Response> {
    let route = route.filter(|route| !route.hop)?;
    if std::mem::replace(asked, true) {
        return None;
    }
    let path = format!(
        "/s/{}?group={}&limit={}&lease={}&max_bytes={max_bytes}",
        take.selector,
        take.group,
        take.limit,
        take.lease_ms / 1000
    );
    route.take(&path).await
}

struct Unreplied {
    handle: Handle,
    receipt: Receipt,
    ids: Vec<MessageId>,
}

impl Drop for Unreplied {
    fn drop(&mut self) {
        let ids = std::mem::take(&mut self.ids);
        let (handle, receipt) = (self.handle.clone(), self.receipt);
        if let (false, Ok(runtime)) = (ids.is_empty(), tokio::runtime::Handle::try_current()) {
            runtime.spawn(async move { handle.release(receipt, ids).await.ok() });
        }
    }
}

pub(crate) fn seconds(value: u64, name: &str) -> Result<u64, Fail> {
    if value == 0 {
        return Err(Fail::bad(
            format!("{name} 0 is under the minimum"),
            format!("give {name} at least 1"),
        ));
    }
    capped(value, LEASE_MAX_S, name)
}

pub(crate) fn limit(value: Option<usize>) -> Result<usize, Fail> {
    match value.unwrap_or(LIMIT) {
        0 => Err(Fail::bad(
            "limit 0 is under the minimum",
            "give limit at least 1",
        )),
        limit => capped(limit, LIMIT_MAX, "limit"),
    }
}

pub(crate) fn capped<T: PartialOrd + Display>(value: T, max: T, name: &str) -> Result<T, Fail> {
    if value > max {
        return Err(Fail::bad(
            format!("{name} {value} is over the cap"),
            format!("give {name} at most {max}"),
        ));
    }
    Ok(value)
}

fn stopped() -> Fail {
    Fail::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "the node stopped while the take waited",
        STOPPED,
    )
}

fn empty(take: &Take, max_bytes: usize) -> Response {
    let link = format!(
        "</s/{}?group={}&limit={}&lease={}&max_bytes={max_bytes}&wait={WAIT_MAX_S}>; rel=\"next\"",
        take.selector,
        take.group,
        take.limit,
        take.lease_ms / 1000
    );
    (StatusCode::NO_CONTENT, [(header::LINK, link)]).into_response()
}

async fn leased(
    handle: &Handle,
    receipt: Receipt,
    until: u64,
    deliveries: &[Delivery],
    max_bytes: usize,
) -> Result<Option<Response>, Fail> {
    let (read, release) = gather(handle, deliveries, max_bytes).await?;
    if !release.is_empty() {
        handle.release(receipt, release).await?;
    }
    if read.is_empty() {
        return Ok(None);
    }
    let text = receipt.to_string();
    let reply = Leased {
        lease_until: rfc3339(until),
        messages: read.into_iter().map(|(_, raw)| raw).collect(),
        next: Next {
            ack: format!("DELETE /r/{text}"),
            settle: format!("POST /r/{text}"),
        },
        receipt: text,
    };
    Ok(Some(Json(reply).into_response()))
}

pub(crate) async fn gather<'a>(
    handle: &Handle,
    deliveries: &'a [Delivery],
    max_bytes: usize,
) -> Result<(Vec<(&'a Delivery, Box<RawValue>)>, Vec<MessageId>), Fail> {
    let mut read = Vec::with_capacity(deliveries.len());
    let mut release = Vec::new();
    let mut bytes = 0usize;
    for run in deliveries.chunk_by(|a, b| {
        a.location.as_ref().map(|l| l.start) == b.location.as_ref().map(|l| l.start)
    }) {
        if bytes > max_bytes {
            release.extend(run.iter().map(|d| d.handed.id));
            continue;
        }
        for (delivery, message) in run.iter().zip(handle.read(run).await?) {
            let Some(message) = message else {
                release.push(delivery.handed.id);
                continue;
            };
            let raw = to_raw_value(&view(delivery, &message)).map_err(|e| {
                Fail::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    e.to_string(),
                    "retry the request",
                )
            })?;
            bytes = bytes.saturating_add(raw.get().len());
            if !read.is_empty() && bytes > max_bytes {
                release.push(delivery.handed.id);
                continue;
            }
            read.push((delivery, raw));
        }
    }
    Ok((read, release))
}

fn view<'a>(delivery: &'a Delivery, message: &'a Message) -> View<'a> {
    let headers: BTreeMap<&str, &str> = message
        .headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let json = headers
        .get(header::CONTENT_TYPE.as_str())
        .is_none_or(|t| json_type(&essence(t)));
    let (value, value_base64) = match std::str::from_utf8(&message.value) {
        Ok(text) => match serde_json::from_str::<&RawValue>(text) {
            Ok(raw) if json => (Some(Value::Json(raw)), None),
            _ => (Some(Value::Text(text)), None),
        },
        Err(_) => (None, Some(STANDARD.encode(&message.value))),
    };
    View {
        id: delivery.handed.id.to_string(),
        stream: &delivery.handed.stream,
        at: rfc3339(delivery.handed.id.ms()),
        attempt: delivery.handed.attempt,
        headers,
        value,
        value_base64,
        original: message.origin.as_ref().map(|o| Original {
            id: o.id.to_string(),
            stream: &o.stream,
            reason: o.reason.as_str(),
            error: o.error.as_deref(),
        }),
    }
}

fn rfc3339(ms: u64) -> String {
    humantime::format_rfc3339_millis(UNIX_EPOCH + Duration::from_millis(ms)).to_string()
}
