use axum::Json;
use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::json;
use utoipa::IntoParams;

use crate::error::{ErrorBody, Fail};
use crate::{Node, ready};

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(deny_unknown_fields)]
pub struct Confirm {
    #[param(required = true)]
    confirm: Option<String>,
}

#[utoipa::path(
    delete,
    path = "/s/{stream}",
    summary = "Delete the stream and its dead streams. Name the stream twice.",
    params(("stream" = String, Path, description = "The stream name"), Confirm),
    responses(
        (status = 200, description = "Deleted. up_to is the last deleted id.", body = Object, example = json!({"deleted": "orders", "up_to": "0193a2f1c4e8-7f3a-0001"})),
        (status = 400, description = "The request is not valid", body = ErrorBody),
        (status = 403, description = "A cross-origin or non-local request", body = ErrorBody),
        (status = 503, description = "The node is starting or stopping", body = ErrorBody),
    )
)]
pub async fn delete(
    State(node): State<Node>,
    path: Result<Path<String>, PathRejection>,
    query: Result<Query<Confirm>, QueryRejection>,
) -> Result<Response, Fail> {
    let handle = ready(&node)?;
    let Path(stream) = path.map_err(|e| Fail::bad(e.body_text(), "check the stream name"))?;
    let Query(confirm) =
        query.map_err(|e| Fail::bad(e.body_text(), "send only ?confirm={stream}"))?;
    if confirm.confirm.as_deref() != Some(stream.as_str()) {
        return Err(Fail::bad(
            "a stream delete needs the stream name twice",
            format!("DELETE /s/{stream}?confirm={stream}"),
        ));
    }
    let up_to = handle.delete_stream(stream.clone()).await?;
    Ok(Json(json!({ "deleted": stream, "up_to": up_to.to_string() })).into_response())
}
