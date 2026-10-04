use std::collections::VecDeque;
use std::io;
use std::ops::Bound;

use axum::Json;
use axum::body::Body;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures_util::{Stream, StreamExt, stream};
use serde::Serialize;
use serde_json::value::RawValue;
use tokio::sync::watch;
use utoipa::ToSchema;
use verdandi_core::ids::MessageId;
use verdandi_core::names;
use verdandi_store::engine::{Delivery, Handle};

use crate::error::{Fail, NAMES};
use crate::groups::bound;
use crate::take::{Options, REPLY_MAX, View, gather, limit};

type Item = (String, Box<RawValue>);

#[derive(Serialize, ToSchema)]
pub(crate) struct Page {
    #[schema(value_type = Vec<View>)]
    messages: Vec<Box<RawValue>>,
    next: String,
}

struct Follow {
    handle: Handle,
    applied: watch::Receiver<(u64, Vec<String>)>,
    stopping: watch::Receiver<bool>,
    stream: String,
    from: Bound<MessageId>,
    limit: usize,
    queue: VecDeque<Item>,
}

pub(crate) async fn read(
    handle: &Handle,
    stopping: watch::Receiver<bool>,
    headers: &HeaderMap,
    stream: String,
    options: Options,
) -> Result<Response, Fail> {
    if options.lease.is_some() || options.wait.is_some() || options.max_bytes.is_some() {
        return Err(Fail::bad(
            "lease, wait and max_bytes belong to a take, and this read has no group",
            format!("take with GET /s/{stream}?group={{group}}, or remove them"),
        ));
    }
    if !names::valid_name(&stream) {
        return Err(if names::valid_pattern(&stream) {
            Fail::bad(
                "a read takes one stream, not a pattern",
                "read each stream, or take as a group: GET /s/{stream}?group={group}",
            )
        } else {
            Fail::bad("the stream name is not valid", NAMES)
        });
    }
    let limit = limit(options.limit)?;
    let follow = match options.follow.as_deref() {
        None => false,
        Some("" | "true") => true,
        Some(_) => return Err(Fail::bad("follow takes no value", "send ?follow")),
    };
    let last_event = headers
        .get("last-event-id")
        .map(|v| {
            v.to_str().map_err(|_| {
                Fail::bad(
                    "Last-Event-ID is not valid",
                    "send Last-Event-ID as a message id, or remove it",
                )
            })
        })
        .transpose()?;
    let from = match last_event.or(options.from.as_deref()) {
        None => Bound::Unbounded,
        Some("now") => Bound::Excluded(handle.issued().await?),
        Some(text) => bound(text, handle.now_ms()).ok_or_else(|| {
            Fail::bad(
                "from is not valid",
                "give from as an RFC 3339 time, -7d, now or a message id",
            )
        })?,
    };
    let after = match from {
        Bound::Excluded(id) => Some(id),
        Bound::Included(id) => id
            .ms()
            .checked_sub(1)
            .and_then(|ms| MessageId::new(ms, u64::MAX, u16::MAX)),
        Bound::Unbounded => None,
    };
    if !follow {
        let (items, resume) = fetch(handle, &stream, from, limit).await?;
        let next = match resume.or(after) {
            Some(from) => format!("/s/{stream}?from={from}&limit={limit}"),
            None => format!("/s/{stream}?limit={limit}"),
        };
        let messages = items.into_iter().map(|(_, raw)| raw).collect();
        return Ok(Json(Page { messages, next }).into_response());
    }
    let events = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|accept| accept.contains("text/event-stream"));
    let items = follow_from(Follow {
        applied: handle.applied(),
        handle: handle.clone(),
        stopping,
        stream,
        from,
        limit,
        queue: VecDeque::new(),
    });
    if events {
        let start = after.map(|id| Ok(Event::default().id(id.to_string())));
        let events = stream::iter(start)
            .chain(items.map(|item| item.map(|(id, raw)| Event::default().id(id).data(raw.get()))));
        return Ok(Sse::new(events)
            .keep_alive(KeepAlive::default())
            .into_response());
    }
    let lines = items.map(|item| item.map(|(_, raw)| format!("{}\n", raw.get())));
    Ok((
        [(header::CONTENT_TYPE, "application/x-ndjson")],
        Body::from_stream(lines),
    )
        .into_response())
}

async fn fetch(
    handle: &Handle,
    stream: &str,
    from: Bound<MessageId>,
    limit: usize,
) -> Result<(Vec<Item>, Option<MessageId>), Fail> {
    let found = handle.scan(stream.to_owned(), from, limit).await?;
    let (read, _) = gather(handle, &found, REPLY_MAX).await?;
    let items = read
        .iter()
        .map(|(delivery, raw)| item(delivery, raw))
        .collect::<Result<_, _>>()
        .map_err(|e: serde_json::Error| {
            Fail::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                e.to_string(),
                "retry the request",
            )
        })?;
    let resume = read
        .last()
        .map(|(delivery, _)| delivery.handed.id)
        .or_else(|| found.last().map(|delivery| delivery.handed.id));
    Ok((items, resume))
}

fn item(delivery: &Delivery, raw: &RawValue) -> Result<Item, serde_json::Error> {
    Ok((
        delivery.handed.id.to_string(),
        RawValue::from_string(compact(raw.get()))?,
    ))
}

fn compact(json: &str) -> String {
    let mut out = String::with_capacity(json.len());
    let (mut quoted, mut escaped) = (false, false);
    for c in json.chars() {
        if quoted {
            quoted = escaped || c != '"';
            escaped = !escaped && c == '\\';
        } else if matches!(c, ' ' | '\t' | '\n' | '\r') {
            continue;
        } else {
            quoted = c == '"';
        }
        out.push(c);
    }
    out
}

fn follow_from(follow: Follow) -> impl Stream<Item = Result<Item, io::Error>> {
    stream::unfold(follow, |mut f| async move {
        loop {
            if let Some(item) = f.queue.pop_front() {
                return Some((Ok(item), f));
            }
            if *f.stopping.borrow() {
                return None;
            }
            let mut seen = f.applied.borrow_and_update().0;
            let (items, resume) = match fetch(&f.handle, &f.stream, f.from, f.limit).await {
                Ok(fetched) => fetched,
                Err(e) => return Some((Err(io::Error::other(format!("{e:?}"))), f)),
            };
            f.queue.extend(items);
            if let Some(id) = resume {
                f.from = Bound::Excluded(id);
                continue;
            }
            loop {
                tokio::select! {
                    changed = f.applied.changed() => if changed.is_err() {
                        return None;
                    },
                    _ = f.stopping.wait_for(|stop| *stop) => return None,
                }
                let applied = f.applied.borrow_and_update();
                if rescan(&applied, seen, &f.stream) {
                    break;
                }
                seen = applied.0;
            }
        }
    })
}

fn rescan(applied: &(u64, Vec<String>), seen: u64, stream: &str) -> bool {
    applied.0 != seen + 1 || applied.1.iter().any(|s| s == stream)
}

#[cfg(test)]
mod tests {
    use super::rescan;

    fn block(count: u64, streams: &[&str]) -> (u64, Vec<String>) {
        (count, streams.iter().map(|s| (*s).to_owned()).collect())
    }

    #[test]
    fn a_follower_rescans_for_a_block_of_its_stream_or_after_a_block_it_did_not_see() {
        assert!(rescan(&block(4, &["t", "s"]), 3, "s"));
        assert!(!rescan(&block(4, &["t"]), 3, "s"));
        assert!(rescan(&block(5, &["t"]), 3, "s"));
    }
}
