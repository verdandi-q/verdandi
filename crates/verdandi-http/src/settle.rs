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
use verdandi_core::lease::{Bury, Nack, Receipt, Settle, SettleError};
use verdandi_store::engine::{Error, SettleBy};

use crate::error::{ErrorBody, Fail, full};
use crate::publish::{list, read};
use crate::take::{LEASE_MAX_S, capped, seconds};
use crate::{Node, ready};

const LOGGED_IDS: usize = 10;

#[derive(Deserialize, ToSchema)]
#[schema(as = SettleRequest)]
#[serde(deny_unknown_fields)]
struct Request {
    #[serde(default, deserialize_with = "list")]
    ack: Vec<String>,
    #[serde(default, deserialize_with = "list")]
    nack: Vec<NackRequest>,
    #[serde(default, deserialize_with = "list")]
    dead: Vec<DeadRequest>,
    #[schema(minimum = 1, maximum = 43200)]
    extend: Option<u64>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
struct NackRequest {
    id: String,
    #[schema(minimum = 0, maximum = 43200)]
    delay: Option<u64>,
    error: Option<String>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
struct DeadRequest {
    id: String,
    error: Option<String>,
}

#[utoipa::path(
    post,
    path = "/r/{receipt}",
    summary = "Settle by id: ack, nack, dead, extend. An id not named stays leased.",
    params(("receipt" = String, Path, description = "The receipt of the take reply")),
    request_body = Request,
    responses(
        (status = 200, description = "Settled", body = Object, example = json!({})),
        (status = 400, description = "The request is not valid", body = ErrorBody),
        (status = 403, description = "A cross-origin or non-local request", body = ErrorBody),
        (status = 408, description = "The body did not arrive in 60 s", body = ErrorBody),
        (status = 409, description = "The receipt is unknown or stale, or an id is not in it", body = ErrorBody),
        (status = 413, description = "The dead originals are over 64 MiB", body = ErrorBody),
        (status = 429, description = "The disk is full of unacked work", body = ErrorBody),
        (status = 503, description = "The node is starting or stopping", body = ErrorBody),
    )
)]
pub async fn settle(
    State(node): State<Node>,
    path: Result<Path<String>, PathRejection>,
    body: Body,
) -> Result<Response, Fail> {
    let handle = ready(&node)?;
    let receipt = receipt(path)?;
    let body = read(body).await?;
    let request: Request = serde_json::from_slice(&body).map_err(|e| {
        Fail::bad(
            e.to_string(),
            r#"send {"ack": [ids], "nack": [{"id", "delay", "error"}], "dead": [{"id", "error"}], "extend": seconds}"#,
        )
    })?;
    let extend = request.extend.map(|s| seconds(s, "extend")).transpose()?;
    let settle = Settle {
        ack: request
            .ack
            .iter()
            .map(|id| id_of(id))
            .collect::<Result<_, _>>()?,
        nack: request
            .nack
            .into_iter()
            .map(|n| {
                Ok(Nack {
                    id: id_of(&n.id)?,
                    delay_ms: n
                        .delay
                        .map(|s| capped(s, LEASE_MAX_S, "delay"))
                        .transpose()?
                        .map(|s| s * 1000),
                    error: n.error,
                })
            })
            .collect::<Result<_, Fail>>()?,
        dead: request
            .dead
            .into_iter()
            .map(|d| {
                Ok(Bury {
                    id: id_of(&d.id)?,
                    error: d.error,
                })
            })
            .collect::<Result<_, Fail>>()?,
        extend_ms: extend.map(|s| s * 1000),
    };
    let (kinds, count, first) = describe(&settle);
    match handle.settle(receipt, SettleBy::Ids(settle)).await {
        Ok(()) => {
            let ids = first
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",");
            tracing::info!(%receipt, %kinds, count, %ids, "settle");
            Ok(Json(json!({})).into_response())
        }
        Err(e @ Error::Refused(SettleError::Pressure)) => Err(full(handle, e).await),
        Err(e) => Err(settle_error(e)),
    }
}

#[utoipa::path(
    delete,
    path = "/r/{receipt}",
    summary = "Ack every message of the take",
    params(("receipt" = String, Path, description = "The receipt of the take reply")),
    responses(
        (status = 200, description = "Acked", body = Object, example = json!({})),
        (status = 400, description = "The request is not valid", body = ErrorBody),
        (status = 403, description = "A cross-origin or non-local request", body = ErrorBody),
        (status = 409, description = "The receipt is unknown or stale", body = ErrorBody),
        (status = 503, description = "The node is starting or stopping", body = ErrorBody),
    )
)]
pub async fn ack_all(
    State(node): State<Node>,
    path: Result<Path<String>, PathRejection>,
) -> Result<Response, Fail> {
    let handle = ready(&node)?;
    let receipt = receipt(path)?;
    handle
        .settle(receipt, SettleBy::All)
        .await
        .map_err(settle_error)?;
    tracing::info!(%receipt, kinds = %"ack_all", ids = %"", "settle");
    Ok(Json(json!({})).into_response())
}

fn describe(settle: &Settle) -> (String, usize, Vec<MessageId>) {
    let kinds = [
        ("ack", !settle.ack.is_empty()),
        ("nack", !settle.nack.is_empty()),
        ("dead", !settle.dead.is_empty()),
        ("extend", settle.extend_ms.is_some()),
    ]
    .into_iter()
    .filter_map(|(kind, used)| used.then_some(kind))
    .collect::<Vec<_>>()
    .join(",");
    let count = settle.ack.len() + settle.nack.len() + settle.dead.len();
    let first = settle
        .ack
        .iter()
        .chain(settle.nack.iter().map(|n| &n.id))
        .chain(settle.dead.iter().map(|d| &d.id))
        .take(LOGGED_IDS)
        .copied()
        .collect();
    (kinds, count, first)
}

fn receipt(path: Result<Path<String>, PathRejection>) -> Result<Receipt, Fail> {
    let Path(text) = path.map_err(|e| Fail::bad(e.body_text(), "check the receipt"))?;
    text.parse().map_err(|_| {
        Fail::bad(
            format!("{text} is not a receipt"),
            "use the receipt that the take reply gives",
        )
    })
}

fn id_of(text: &str) -> Result<MessageId, Fail> {
    text.parse().map_err(|_| {
        Fail::bad(
            format!("{text} is not a message id"),
            "use the ids that the take reply gives",
        )
    })
}

fn settle_error(error: Error) -> Fail {
    match error {
        Error::TooLarge => Fail::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "the originals of the dead ids are over 64 MiB",
            "settle fewer ids at a time",
        ),
        error => error.into(),
    }
}
