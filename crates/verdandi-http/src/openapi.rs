use axum::Json;
use axum::extract::State;
use utoipa::OpenApi;

use crate::error::{ErrorBody, Fail};
use crate::{Node, groups, publish, ready, root, settle, streams, take};

#[derive(OpenApi)]
#[openapi(paths(
    root::root,
    openapi,
    crate::livez,
    crate::readyz,
    publish::publish,
    publish::bulk,
    take::take,
    streams::delete,
    settle::settle,
    settle::ack_all,
    groups::stats,
    groups::put,
    groups::forget,
))]
struct Api;

#[utoipa::path(
    get,
    path = "/openapi.json",
    summary = "The whole API as OpenAPI",
    responses(
        (status = 200, description = "The OpenAPI document", body = Object),
        (status = 403, description = "The Host is not an IP address or localhost", body = ErrorBody),
        (status = 503, description = "The node is recovering its data", body = ErrorBody),
    )
)]
pub async fn openapi(State(node): State<Node>) -> Result<Json<utoipa::openapi::OpenApi>, Fail> {
    ready(&node)?;
    Ok(Json(Api::openapi()))
}
