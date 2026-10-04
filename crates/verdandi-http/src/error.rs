use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use utoipa::ToSchema;
use verdandi_core::lease::SettleError;
use verdandi_store::engine::{Error, Handle};

pub const NAMES: &str = "use dot-separated parts of A-Z, a-z, 0-9, _ and -, at most 200 bytes. A client cannot publish to a name that starts with _, holds .dead. or has dead as its last part.";
pub const PRESSURE: &str = "the disk is full of unacked work: ack or settle messages, delete a group with DELETE /g/{group}, or delete a stream that no group reads with DELETE /s/{stream}?confirm={stream}";
pub const STOPPED: &str = "the node stopped: retry on another node";

#[derive(Serialize, ToSchema)]
pub struct ErrorBody {
    error: String,
    next: String,
}

#[derive(Debug)]
pub struct Fail {
    status: StatusCode,
    error: String,
    next: String,
}

impl Fail {
    pub fn new(status: StatusCode, error: impl Into<String>, next: impl Into<String>) -> Self {
        Self {
            status,
            error: error.into(),
            next: next.into(),
        }
    }

    pub fn bad(error: impl Into<String>, next: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, error, next)
    }
}

impl IntoResponse for Fail {
    fn into_response(self) -> Response {
        let body = ErrorBody {
            error: self.error,
            next: self.next,
        };
        (self.status, Json(body)).into_response()
    }
}

impl From<Error> for Fail {
    fn from(error: Error) -> Self {
        let text = error.to_string();
        match error {
            Error::BadName => Self::bad(text, NAMES),
            Error::TooLong => Self::bad(text, "send shorter headers and a shorter Idempotency-Key"),
            Error::Empty => Self::bad(text, "send at least one message"),
            Error::TooLarge => Self::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                text,
                "split the batch into smaller requests",
            ),
            Error::Refused(SettleError::Pressure) => {
                Self::new(StatusCode::TOO_MANY_REQUESTS, text, PRESSURE)
            }
            Error::Refused(SettleError::UnknownReceipt) => Self::new(
                StatusCode::CONFLICT,
                "the receipt is unknown or its lease expired",
                "take again: GET /s/{stream}?group={group}",
            ),
            Error::Refused(SettleError::OldGeneration) => Self::new(
                StatusCode::CONFLICT,
                "the group was replayed or forgotten after this take",
                "take again: GET /s/{stream}?group={group}",
            ),
            Error::Refused(SettleError::UnknownId(id)) => Self::new(
                StatusCode::CONFLICT,
                format!("the id {id} is not in this receipt, so nothing was settled"),
                "settle only the ids that this receipt handed out",
            ),
            Error::NoSpace => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                text,
                "the disk is full: retry later",
            ),
            Error::Replaying => Self::new(StatusCode::CONFLICT, text, "send the redrive again"),
            Error::NoGroup => Self::new(
                StatusCode::NOT_FOUND,
                text,
                "take as the group to create it: GET /s/{stream}?group={group}",
            ),
            Error::Unreadable(_) => {
                Self::new(StatusCode::SERVICE_UNAVAILABLE, text, "retry the request")
            }
            Error::Stopped | Error::Random => {
                Self::new(StatusCode::SERVICE_UNAVAILABLE, text, STOPPED)
            }
        }
    }
}

pub async fn full(handle: &Handle, error: Error) -> Fail {
    let holding = match handle.holding().await {
        Ok(holding) => holding,
        Err(e) => return e.into(),
    };
    if holding.groups.is_empty() && holding.streams.is_empty() {
        return Fail::new(StatusCode::TOO_MANY_REQUESTS, error.to_string(), PRESSURE);
    }
    let mut named = Vec::new();
    let mut next = Vec::new();
    if !holding.groups.is_empty() {
        named.push(format!(
            "the groups {} hold the oldest unacked data",
            holding.groups.join(", ")
        ));
        next.extend(holding.groups.iter().map(|g| format!("DELETE /g/{g}")));
    }
    if !holding.streams.is_empty() {
        named.push(format!(
            "no group holds the data of the streams {}",
            holding.streams.join(", ")
        ));
        next.extend(
            holding
                .streams
                .iter()
                .map(|s| format!("DELETE /s/{s}?confirm={s}")),
        );
    }
    Fail::new(
        StatusCode::TOO_MANY_REQUESTS,
        format!("{error}: {}", named.join("; ")),
        format!("ack or settle messages, or free space: {}", next.join(", ")),
    )
}
