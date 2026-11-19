use std::cell::Cell;
use std::collections::BTreeSet;
use std::fmt;
use std::marker::PhantomData;
use std::time::{Duration, UNIX_EPOCH};

use axum::Json;
use axum::body::{self, Body, Bytes};
use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use http_body_util::LengthLimitError;
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;
use utoipa::{IntoParams, ToSchema};
use verdandi_core::{Timing, nth_id};
use verdandi_store::block::{BLOCK_HEADER_LEN, MAX_BLOCK};
use verdandi_store::engine::{BULK, Error, Handle, Key, NewMessage};
use verdandi_store::records;

use crate::error::{ErrorBody, Fail, full};
use crate::take::LIMIT_MAX;
use crate::{Node, ready};

const BODY_MAX: usize = 16 << 20;
const MESSAGE_MAX: usize = 1 << 20;
const HEADERS_MAX: usize = 8 << 10;
const BODY_TIMEOUT: Duration = Duration::from_secs(60);
const FORM: &str = "application/x-www-form-urlencoded";
const ROOM: usize = MAX_BLOCK - BLOCK_HEADER_LEN;

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(deny_unknown_fields)]
pub struct Options {
    delay: Option<String>,
    ttl: Option<String>,
}

#[derive(Deserialize, ToSchema)]
#[schema(as = BulkItem)]
#[serde(deny_unknown_fields)]
struct Item<'a> {
    stream: String,
    #[serde(borrow)]
    #[schema(value_type = Value)]
    value: &'a RawValue,
    #[serde(default, deserialize_with = "pairs")]
    #[schema(value_type = BTreeMap<String, String>)]
    headers: Vec<(String, String)>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct Published {
    ids: Vec<String>,
    copies: u32,
    next: Next,
}

#[derive(Serialize, ToSchema)]
#[schema(as = PublishNext)]
struct Next {
    take: String,
}

#[utoipa::path(
    post,
    path = "/s/{stream}",
    summary = "Publish one message or a batch. The call creates the stream.",
    description = "Each header whose name starts with ce- or X-Msg- travels with the message, as traceparent and tracestate do.",
    params(("stream" = String, Path, description = "The stream name"), Options, ("Idempotency-Key" = Option<String>, Header, description = "A repeat with the same key in 24 hours gives 200 and the first reply"), ("traceparent" = Option<String>, Header, description = "Travels with the message"), ("tracestate" = Option<String>, Header, description = "Travels with the message")),
    request_body(description = "One message, or a JSON array for a batch", content((Value = "application/json"), (String = "text/plain"))),
    responses(
        (status = 201, description = "Accepted", body = Published),
        (status = 200, description = "The first reply to a repeated Idempotency-Key", body = Published),
        (status = 400, description = "The request is not valid", body = ErrorBody),
        (status = 403, description = "A cross-origin or non-local request", body = ErrorBody),
        (status = 408, description = "The body did not arrive in 60 s", body = ErrorBody),
        (status = 413, description = "A message is over 1 MiB, the body is over 16 MiB, or a batch is over the 32 MiB block cap", body = ErrorBody),
        (status = 429, description = "The disk is full of unacked work", body = ErrorBody),
        (status = 503, description = "The node is starting or stopping", body = ErrorBody),
    )
)]
pub async fn publish(
    State(node): State<Node>,
    path: Result<Path<String>, PathRejection>,
    query: Result<Query<Options>, QueryRejection>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, Fail> {
    let handle = ready(&node)?;
    let Path(stream) = path.map_err(|e| Fail::bad(e.body_text(), "check the stream name"))?;
    let Query(options) = query.map_err(query_error)?;
    let body = read(body).await?;
    let timing = timing(&options, handle.now_ms())?;
    let content_type = text(&headers, header::CONTENT_TYPE.as_str())?;
    let kind = content_type.map(essence);
    let json = kind.as_deref().is_some_and(json_type);
    let parsed = kind
        .as_deref()
        .is_none_or(|e| json || e == FORM)
        .then(|| serde_json::from_slice::<&RawValue>(&body).ok())
        .flatten();
    let mut kept = kept(&headers)?;
    let typed = match parsed {
        Some(_) => content_type.filter(|_| json),
        None => content_type,
    };
    if let Some(typed) = typed {
        kept.push((header::CONTENT_TYPE.as_str().to_owned(), typed.to_owned()));
    }
    let overhead = records::overhead_len(timing, &kept);
    let values: Vec<&[u8]> = match parsed {
        Some(raw) if raw.get().starts_with('[') => array(
            raw.get().as_bytes(),
            |v: &&RawValue| overhead.saturating_add(v.get().len()),
            "send a JSON array of values",
        )?
        .into_iter()
        .map(|v| v.get().as_bytes())
        .collect(),
        Some(raw) => vec![raw.get().as_bytes()],
        None => vec![&body[..]],
    };
    fits(
        values.len(),
        &kept,
        timing,
        values.iter().map(|v| v.len()).sum(),
    )?;
    let messages = values
        .into_iter()
        .map(|value| message(&stream, kept.clone(), value, timing))
        .collect::<Result<Vec<_>, _>>()?;
    let key = idempotency(&headers, &stream)?;
    send(handle, messages, key, &stream).await
}

#[utoipa::path(
    post,
    path = "/bulk",
    summary = "Publish to many streams in one atomic request",
    description = "Each header whose name starts with ce- or X-Msg- travels with the message, as traceparent and tracestate do.",
    params(Options, ("Idempotency-Key" = Option<String>, Header, description = "A repeat with the same key in 24 hours gives 200 and the first reply"), ("traceparent" = Option<String>, Header, description = "Travels with the message"), ("tracestate" = Option<String>, Header, description = "Travels with the message")),
    request_body = Vec<Item>,
    responses(
        (status = 201, description = "Accepted", body = Published),
        (status = 200, description = "The first reply to a repeated Idempotency-Key", body = Published),
        (status = 400, description = "The request is not valid", body = ErrorBody),
        (status = 403, description = "A cross-origin or non-local request", body = ErrorBody),
        (status = 408, description = "The body did not arrive in 60 s", body = ErrorBody),
        (status = 413, description = "A message is over 1 MiB, the body is over 16 MiB, or a batch is over the 32 MiB block cap", body = ErrorBody),
        (status = 429, description = "The disk is full of unacked work", body = ErrorBody),
        (status = 503, description = "The node is starting or stopping", body = ErrorBody),
    )
)]
pub async fn bulk(
    State(node): State<Node>,
    query: Result<Query<Options>, QueryRejection>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, Fail> {
    let handle = ready(&node)?;
    let Query(options) = query.map_err(query_error)?;
    let body = read(body).await?;
    let timing = timing(&options, handle.now_ms())?;
    let kept = kept(&headers)?;
    let overhead = records::overhead_len(timing, &kept);
    let items: Vec<Item> = array(
        &body,
        |i: &Item| overhead.saturating_add(i.value.get().len()),
        r#"send a JSON array of {"stream": …, "value": …, "headers": {…}}"#,
    )?;
    let messages = items
        .into_iter()
        .map(|item| {
            let mut headers = kept.clone();
            for (name, value) in item.headers {
                let name = name.to_ascii_lowercase();
                if !token(&name) {
                    return Err(bad_name(&name));
                }
                if !visible(&value) {
                    return Err(not_ascii(&name));
                }
                if travels(&name) {
                    headers.push((name, value));
                }
            }
            unique(&headers)?;
            message(&item.stream, headers, item.value.get().as_bytes(), timing)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let key = idempotency(&headers, BULK)?;
    send(handle, messages, key, "{stream}").await
}

async fn send(
    handle: &Handle,
    messages: Vec<NewMessage>,
    key: Option<Key>,
    stream: &str,
) -> Result<Response, Fail> {
    let published = match handle.publish(messages, key).await {
        Ok(published) => published,
        Err(e @ Error::NoSpace) => return Err(full(handle, e).await),
        Err(e) => return Err(e.into()),
    };
    let ids = (0..published.count)
        .filter_map(|n| nth_id(published.first, n))
        .map(|id| id.to_string())
        .collect();
    let status = if published.repeat {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    let reply = Published {
        ids,
        copies: published.copies,
        next: Next {
            take: format!("/s/{stream}?group={{group}}"),
        },
    };
    Ok((status, Json(reply)).into_response())
}

fn message(
    stream: &str,
    headers: Vec<(String, String)>,
    value: &[u8],
    timing: Timing,
) -> Result<NewMessage, Fail> {
    if value.len() > MESSAGE_MAX {
        return Err(Fail::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "a message is over 1 MiB",
            "use the claim-check pattern: store the payload elsewhere, for example in an object store, and publish a message that names it",
        ));
    }
    if headers
        .iter()
        .map(|(k, v)| k.len() + v.len())
        .sum::<usize>()
        > HEADERS_MAX
    {
        return Err(Fail::bad(
            "the headers of a message are over 8 KB",
            "send at most 8 KB of Content-Type, traceparent, tracestate, ce-* and X-Msg-* headers",
        ));
    }
    Ok(NewMessage {
        stream: stream.to_owned(),
        headers,
        value: value.to_vec(),
        timing,
    })
}

struct Capped<'a, T, F> {
    cap: usize,
    cost: F,
    over: &'a Cell<bool>,
    item: PhantomData<T>,
}

impl<'de, T: Deserialize<'de>, F: Fn(&T) -> usize> Visitor<'de> for Capped<'_, T, F> {
    type Value = Vec<T>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON array")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<T>, A::Error> {
        let mut items = Vec::new();
        let mut total = 0usize;
        while let Some(item) = seq.next_element::<T>()? {
            total = total.saturating_add((self.cost)(&item));
            if total > self.cap {
                self.over.set(true);
                return Err(de::Error::custom(format_args!(
                    "the array is over its cap of {}",
                    self.cap
                )));
            }
            items.push(item);
        }
        Ok(items)
    }
}

struct Pairs;

impl<'de> Visitor<'de> for Pairs {
    type Value = Vec<(String, String)>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON object of header names and values")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut pairs = Vec::new();
        while let Some(pair) = map.next_entry()? {
            pairs.push(pair);
        }
        Ok(pairs)
    }
}

fn pairs<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<(String, String)>, D::Error> {
    deserializer.deserialize_map(Pairs)
}

pub(crate) fn list<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> Result<Vec<T>, D::Error> {
    deserializer.deserialize_seq(Capped {
        cap: LIMIT_MAX,
        cost: |_: &T| 1,
        over: &Cell::new(false),
        item: PhantomData,
    })
}

fn array<'de, T: Deserialize<'de>>(
    text: &'de [u8],
    cost: impl Fn(&T) -> usize,
    next: &str,
) -> Result<Vec<T>, Fail> {
    let over = Cell::new(false);
    let mut json = serde_json::Deserializer::from_slice(text);
    let items = json
        .deserialize_seq(Capped {
            cap: ROOM,
            cost,
            over: &over,
            item: PhantomData,
        })
        .and_then(|items| json.end().map(|()| items));
    items.map_err(|e| {
        if over.get() {
            Error::TooLarge.into()
        } else {
            Fail::bad(e.to_string(), next)
        }
    })
}

fn fits(
    count: usize,
    headers: &[(String, String)],
    timing: Timing,
    values: usize,
) -> Result<(), Fail> {
    let least = count
        .saturating_mul(records::overhead_len(timing, headers))
        .saturating_add(values);
    if least > ROOM {
        return Err(Error::TooLarge.into());
    }
    Ok(())
}

pub(crate) async fn read(body: Body) -> Result<Bytes, Fail> {
    let read = tokio::time::timeout(BODY_TIMEOUT, body::to_bytes(body, BODY_MAX))
        .await
        .map_err(|_| {
            Fail::new(
                StatusCode::REQUEST_TIMEOUT,
                "the request body did not arrive in 60 s",
                "send the whole body without a pause",
            )
        })?;
    read.map_err(|e| {
        let error = e.into_inner();
        if error.is::<LengthLimitError>() {
            Fail::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "the request body is over 16 MiB",
                "split the batch into smaller requests",
            )
        } else {
            Fail::bad(
                format!("the request body could not be read: {error}"),
                "send the request again with a complete body",
            )
        }
    })
}

fn query_error(rejection: QueryRejection) -> Fail {
    Fail::bad(rejection.body_text(), "check the query parameters")
}

pub(crate) fn essence(content_type: &str) -> String {
    content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

pub(crate) fn json_type(essence: &str) -> bool {
    essence == "application/json" || essence.ends_with("+json")
}

fn travels(name: &str) -> bool {
    matches!(name, "traceparent" | "tracestate")
        || name.starts_with("ce-")
        || name.starts_with("x-msg-")
}

fn kept(headers: &HeaderMap) -> Result<Vec<(String, String)>, Fail> {
    let kept = headers
        .iter()
        .filter(|(name, _)| travels(name.as_str()))
        .map(|(name, value)| {
            if !token(name.as_str()) {
                return Err(bad_name(name.as_str()));
            }
            value
                .to_str()
                .map(|v| (name.as_str().to_owned(), v.to_owned()))
                .map_err(|_| not_ascii(name.as_str()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    unique(&kept)?;
    Ok(kept)
}

fn bad_name(name: &str) -> Fail {
    Fail::bad(
        format!("the header name {name:?} is not valid"),
        "use a header name of letters, digits and !#$%&'*+-.^_`|~, with text after ce- or x-msg-",
    )
}

fn unique(headers: &[(String, String)]) -> Result<(), Fail> {
    let mut names = BTreeSet::new();
    match headers
        .iter()
        .find(|(name, _)| !names.insert(name.as_str()))
    {
        Some((name, _)) => Err(Fail::bad(
            format!("the {name} header is in the message more than once"),
            "send each travelling header once",
        )),
        None => Ok(()),
    }
}

fn visible(text: &str) -> bool {
    text.bytes()
        .all(|b| b == b'\t' || (b' '..=b'~').contains(&b))
}

fn token(name: &str) -> bool {
    !name.is_empty()
        && !matches!(name, "ce-" | "x-msg-")
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
}

fn text<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, Fail> {
    if headers.get_all(name).iter().nth(1).is_some() {
        return Err(Fail::bad(
            format!("the {name} header is in the request more than once"),
            format!("send one {name} header"),
        ));
    }
    headers
        .get(name)
        .map(|v| v.to_str().map_err(|_| not_ascii(name)))
        .transpose()
}

fn not_ascii(name: &str) -> Fail {
    Fail::bad(
        format!("the {name} header is not visible ASCII"),
        "send header values as visible ASCII",
    )
}

fn idempotency(headers: &HeaderMap, scope: &str) -> Result<Option<Key>, Fail> {
    Ok(text(headers, "idempotency-key")?.map(|key| Key {
        scope: scope.to_owned(),
        key: key.to_owned(),
    }))
}

fn timing(options: &Options, now: u64) -> Result<Timing, Fail> {
    let delay_until = options
        .delay
        .as_deref()
        .map(|d| {
            at(d, now, true).ok_or_else(|| {
                Fail::bad(
                    "delay is not valid",
                    "give delay as seconds, a duration such as 30s, or an RFC 3339 time",
                )
            })
        })
        .transpose()?;
    let expire_at = options
        .ttl
        .as_deref()
        .map(|t| {
            at(t, now, false).ok_or_else(|| {
                Fail::bad(
                    "ttl is not valid",
                    "give ttl as seconds or a duration such as 30s",
                )
            })
        })
        .transpose()?;
    Ok(Timing {
        delay_until,
        expire_at,
    })
}

fn at(text: &str, now: u64, time: bool) -> Option<u64> {
    let after = |ms: u128| u64::try_from(ms).ok().and_then(|ms| now.checked_add(ms));
    if let Ok(seconds) = text.parse::<u64>() {
        return after(u128::from(seconds) * 1000);
    }
    if let Ok(duration) = humantime::parse_duration(text) {
        return after(duration.as_millis());
    }
    if time && let Some(ms) = time_ms(text) {
        return Some(ms);
    }
    None
}

pub(crate) fn time_ms(text: &str) -> Option<u64> {
    let (local, offset) = match text.as_bytes().last()? {
        b'Z' | b'z' => (&text[..text.len() - 1], 0),
        _ => {
            let (local, zone) = text.split_at_checked(text.len().checked_sub(6)?)?;
            let &[sign, h1, h2, b':', m1, m2] = zone.as_bytes() else {
                return None;
            };
            let digit = |b: u8| b.is_ascii_digit().then(|| i64::from(b - b'0'));
            let (hours, minutes) = (digit(h1)? * 10 + digit(h2)?, digit(m1)? * 10 + digit(m2)?);
            let sign = match sign {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            if hours > 23 || minutes > 59 {
                return None;
            }
            (local, sign * (hours * 60 + minutes) * 60_000)
        }
    };
    let utc = humantime::parse_rfc3339(&format!("{}Z", local.to_ascii_uppercase())).ok()?;
    let ms = i64::try_from(utc.duration_since(UNIX_EPOCH).ok()?.as_millis()).ok()?;
    u64::try_from(ms.checked_sub(offset)?).ok()
}
