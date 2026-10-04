mod error;
mod groups;
mod openapi;
mod publish;
mod reads;
mod root;
mod settle;
mod streams;
mod take;

use std::future::Future;
use std::io;
use std::net::IpAddr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::Router;
use axum::extract::{Extension, Query, Request, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tower_http::cors::{Any, CorsLayer};
use verdandi_store::engine::Handle;

use error::ErrorBody;
pub use error::Fail;

pub const OPEN_MODE: &str = "open mode: no cluster key and no auth, so any client can publish, take and delete, and any web page can read the streams across origins. Use it only on a trusted network.";
pub const NEXT_STEP: &str = "add disk or nodes, or delete streams or groups";

const ROUTES: &str = "read the guide: GET / with Accept: text/plain. The routes are GET /, GET /openapi.json, GET /livez, GET /readyz, POST /s/{stream}, GET /s/{stream}?group={group}, GET /s/{stream}?from={from}, DELETE /s/{stream}?confirm={stream}, POST /bulk, POST /r/{receipt}, DELETE /r/{receipt}, GET /g/{group}, PUT /g/{group} and DELETE /g/{group}";

pub const STOP_GRACE: Duration = Duration::from_secs(5);

pub type Node = Arc<OnceLock<Handle>>;

pub fn router(node: Node, stopping: watch::Receiver<bool>) -> Router {
    Router::new()
        .route("/openapi.json", get(openapi::openapi))
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route(
            "/s/{stream}",
            get(take::take)
                .post(publish::publish)
                .delete(streams::delete),
        )
        .route(
            "/g/{group}",
            get(groups::stats).put(groups::put).delete(groups::forget),
        )
        .route("/bulk", post(publish::bulk))
        .route("/r/{receipt}", post(settle::settle).delete(settle::ack_all))
        .fallback(|State(node): State<Node>| async move {
            ready(&node)?;
            Err::<(), _>(Fail::new(
                StatusCode::NOT_FOUND,
                "no route has this path",
                ROUTES,
            ))
        })
        .method_not_allowed_fallback(wrong_method)
        .layer(Extension(stopping))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([Method::GET, Method::HEAD]),
        )
        .route("/", get(root::root).fallback(wrong_method))
        .layer(middleware::from_fn(guard))
        .layer(middleware::from_fn(log))
        .with_state(node)
}

pub async fn serve(
    listener: TcpListener,
    node: Node,
    stop: impl Future<Output = ()> + Send + 'static,
) -> io::Result<()> {
    let (stop_all, stopping) = watch::channel(false);
    let mut stopped = stopping.clone();
    let server = axum::serve(listener, router(node, stopping)).with_graceful_shutdown(async move {
        stop.await;
        stop_all.send_replace(true);
    });
    tokio::select! {
        served = server => served,
        _ = async {
            stopped.wait_for(|stop| *stop).await.ok();
            tokio::time::sleep(STOP_GRACE).await;
        } => Ok(()),
    }
}

async fn wrong_method(State(node): State<Node>) -> Result<(), Fail> {
    ready(&node)?;
    Err(Fail::new(
        StatusCode::METHOD_NOT_ALLOWED,
        "this path does not accept this method",
        ROUTES,
    ))
}

fn ready(node: &Node) -> Result<&Handle, Fail> {
    node.get().ok_or_else(|| {
        Fail::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "the node is recovering its data",
            "retry when GET /readyz gives 200",
        )
    })
}

#[utoipa::path(
    get,
    path = "/livez",
    summary = "The process is alive",
    responses(
        (status = 200, description = "Alive"),
        (status = 403, description = "The Host is not an IP address or localhost", body = ErrorBody),
    )
)]
async fn livez() -> StatusCode {
    StatusCode::OK
}

#[utoipa::path(
    get,
    path = "/readyz",
    summary = "The node holds its data and serves requests",
    responses(
        (status = 200, description = "Ready"),
        (status = 403, description = "The Host is not an IP address or localhost", body = ErrorBody),
        (status = 503, description = "The node is recovering its data", body = ErrorBody),
    )
)]
async fn readyz(State(node): State<Node>) -> Result<StatusCode, Fail> {
    ready(&node).map(|_| StatusCode::OK)
}

pub(crate) fn host<'a>(headers: &'a HeaderMap, uri: &'a Uri) -> Option<&'a str> {
    headers
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .or_else(|| uri.authority().map(|a| a.as_str()))
}

async fn log(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let response = next.run(request).await;
    tracing::info!(%method, %uri, status = response.status().as_u16(), "request");
    response
}

async fn guard(request: Request, next: Next) -> Response {
    let headers = request.headers();
    let Some(host) = host(headers, request.uri()).filter(|h| local(h)) else {
        return Fail::new(
            StatusCode::FORBIDDEN,
            "open mode accepts only an IP address or localhost as the Host",
            "connect by IP address, for example http://127.0.0.1:8080",
        )
        .into_response();
    };
    let take = Query::<Vec<(String, String)>>::try_from_uri(request.uri())
        .is_ok_and(|q| q.iter().any(|(k, _)| k == "group"));
    let changes = take
        || !matches!(
            *request.method(),
            Method::GET | Method::HEAD | Method::OPTIONS
        );
    let other_site = headers
        .get("sec-fetch-site")
        .is_some_and(|v| !matches!(v.as_bytes(), b"same-origin" | b"none"));
    let foreign = headers.get(header::ORIGIN).is_some_and(|origin| {
        origin
            .to_str()
            .ok()
            .and_then(|o| o.split_once("://"))
            .is_none_or(|(_, authority)| !authority.eq_ignore_ascii_case(host))
    });
    if changes && (other_site || foreign) {
        return Fail::new(
            StatusCode::FORBIDDEN,
            "a cross-origin request cannot publish, take, settle or delete",
            "send the request from the same origin, or from a client that is not a browser",
        )
        .into_response();
    }
    next.run(request).await
}

fn local(host: &str) -> bool {
    let (name, port) = match host.strip_prefix('[') {
        Some(rest) => match rest.split_once(']') {
            Some(split) => split,
            None => return false,
        },
        None => host
            .rfind(':')
            .map_or((host, ""), |at| (&host[..at], &host[at..])),
    };
    let port = port.is_empty()
        || port
            .strip_prefix(':')
            .is_some_and(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()));
    port && (name.eq_ignore_ascii_case("localhost") || name.parse::<IpAddr>().is_ok())
}
