use std::ops::Bound;

use axum::Json;
use axum::body::Body;
use axum::extract::rejection::PathRejection;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::json;
use utoipa::ToSchema;
use verdandi_core::ids::MessageId;
use verdandi_core::names;

use crate::error::{ErrorBody, Fail, NAMES};
use crate::publish::{read, time_ms};
use crate::{Node, ready};

const PUT_BODY: &str = r#"send {"from": time, "-7d", "now" or a message id} to replay, or {"redrive": "dead"} to deliver dead letters again"#;

#[derive(Deserialize, ToSchema)]
#[schema(as = GroupRequest)]
#[serde(deny_unknown_fields)]
struct Request {
    from: Option<String>,
    redrive: Option<String>,
}

fn name(path: Result<Path<String>, PathRejection>) -> Result<String, Fail> {
    let Path(group) = path.map_err(|e| Fail::bad(e.body_text(), "check the group name"))?;
    if !names::valid_group(&group) {
        return Err(Fail::bad("the group name is not valid", NAMES));
    }
    Ok(group)
}

#[utoipa::path(
    get,
    path = "/g/{group}",
    summary = "Show the count of work of a group",
    params(("group" = String, Path, description = "The group name")),
    responses(
        (status = 200, description = "The counts", body = Object, example = json!({"pending": 0, "in_flight": 0, "dead": 0, "oldest_age_s": 0.0, "provisional": false})),
        (status = 400, description = "The request is not valid", body = ErrorBody),
        (status = 403, description = "The Host is not an IP address or localhost", body = ErrorBody),
        (status = 404, description = "The group does not exist", body = ErrorBody),
        (status = 503, description = "The node is starting or stopping", body = ErrorBody),
    )
)]
pub async fn stats(
    State(node): State<Node>,
    path: Result<Path<String>, PathRejection>,
) -> Result<Response, Fail> {
    let handle = ready(&node)?;
    let group = name(path)?;
    let stats = handle.group_stats(group.clone()).await?.ok_or_else(|| {
        Fail::new(
            StatusCode::NOT_FOUND,
            format!("no group {group} exists"),
            format!("take as the group to create it: GET /s/{{stream}}?group={group}"),
        )
    })?;
    let oldest_age_s = stats
        .oldest_ms
        .map_or(0.0, |ms| handle.now_ms().saturating_sub(ms) as f64 / 1000.0);
    Ok(Json(json!({
        "pending": stats.pending,
        "in_flight": stats.in_flight,
        "dead": stats.dead,
        "oldest_age_s": oldest_age_s,
        "provisional": stats.provisional,
    }))
    .into_response())
}

#[utoipa::path(
    put,
    path = "/g/{group}",
    summary = "Replay from a time, or redrive the dead letters",
    params(("group" = String, Path, description = "The group name")),
    request_body = Request,
    responses(
        (status = 200, description = "Done", body = Object, example = json!({})),
        (status = 400, description = "The request is not valid", body = ErrorBody),
        (status = 403, description = "A cross-origin or non-local request", body = ErrorBody),
        (status = 408, description = "The body did not arrive in 60 s", body = ErrorBody),
        (status = 404, description = "A redrive of a group that does not exist", body = ErrorBody),
        (status = 409, description = "A replay of the group is not yet durable", body = ErrorBody),
        (status = 503, description = "The node is starting or stopping", body = ErrorBody),
    )
)]
pub async fn put(
    State(node): State<Node>,
    path: Result<Path<String>, PathRejection>,
    body: Body,
) -> Result<Response, Fail> {
    let handle = ready(&node)?;
    let group = name(path)?;
    let body = read(body).await?;
    let request: Request =
        serde_json::from_slice(&body).map_err(|e| Fail::bad(e.to_string(), PUT_BODY))?;
    match (request.from, request.redrive) {
        (Some(from), None) if from == "now" => handle.replay_now(group).await?,
        (Some(from), None) => {
            let bound = bound(&from, handle.now_ms()).ok_or_else(|| {
                Fail::bad(
                    "from is not valid",
                    "give from as an RFC 3339 time, -7d, now or a message id",
                )
            })?;
            handle.replay(group, bound).await?;
        }
        (None, Some(target)) if target == "dead" => handle.redrive(group).await?,
        (None, Some(_)) => {
            return Err(Fail::bad(
                "redrive accepts only \"dead\"",
                r#"send {"redrive": "dead"}"#,
            ));
        }
        _ => return Err(Fail::bad("send exactly one of from and redrive", PUT_BODY)),
    }
    Ok(Json(json!({})).into_response())
}

#[utoipa::path(
    delete,
    path = "/g/{group}",
    summary = "Forget the group",
    params(("group" = String, Path, description = "The group name")),
    responses(
        (status = 200, description = "Forgotten", body = Object, example = json!({"deleted": "billing"})),
        (status = 400, description = "The request is not valid", body = ErrorBody),
        (status = 403, description = "A cross-origin or non-local request", body = ErrorBody),
        (status = 404, description = "The group does not exist", body = ErrorBody),
        (status = 503, description = "The node is starting or stopping", body = ErrorBody),
    )
)]
pub async fn forget(
    State(node): State<Node>,
    path: Result<Path<String>, PathRejection>,
) -> Result<Response, Fail> {
    let handle = ready(&node)?;
    let group = name(path)?;
    handle.forget(group.clone()).await?;
    Ok(Json(json!({ "deleted": group })).into_response())
}

pub(crate) fn bound(text: &str, now: u64) -> Option<Bound<MessageId>> {
    if let Ok(id) = text.parse::<MessageId>() {
        return Some(Bound::Excluded(id));
    }
    let ms = if let Some(age) = text.strip_prefix('-') {
        let age = u64::try_from(humantime::parse_duration(age).ok()?.as_millis()).ok()?;
        now.saturating_sub(age)
    } else {
        time_ms(text)?
    };
    MessageId::new(ms, 0, 0).map(Bound::Included)
}
