use std::fmt::Write;
use std::sync::Arc;

use axum::extract::{Extension, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};

use verdandi_core::group::GroupStats;

use crate::error::{ErrorBody, Fail};
use crate::groups::drain_rate;
use crate::{Cluster, Node, ready, scale};

type Series = (&'static str, &'static str, fn(&GroupStats, u64) -> f64);

fn family(out: &mut String, name: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} gauge");
}

#[utoipa::path(
    get,
    path = "/metrics",
    summary = "Metrics in Prometheus text format",
    responses(
        (status = 200, description = "The metrics", content_type = "text/plain; version=0.0.4", body = String),
        (status = 403, description = "The Host is not an IP address or localhost", body = ErrorBody),
        (status = 503, description = "The node is starting or stopping", body = ErrorBody),
    )
)]
pub async fn metrics(
    State(node): State<Node>,
    cluster: Option<Extension<Arc<Cluster>>>,
) -> Result<Response, Fail> {
    let handle = ready(&node)?;
    let status = handle.status().await?;
    let mut rows = Vec::new();
    for (name, _) in &status.groups {
        if let Some(stats) = handle.group_stats(name.clone()).await? {
            rows.push((name, stats));
        }
    }
    let mut out = String::new();
    let per_group: [Series; 5] = [
        ("verdandi_group_pending", "Messages waiting", |s, _| {
            s.pending as f64
        }),
        (
            "verdandi_group_oldest_age_seconds",
            "Age of the oldest waiting message",
            |s, now| {
                s.oldest_ms
                    .map_or(0.0, |ms| now.saturating_sub(ms) as f64 / 1000.0)
            },
        ),
        ("verdandi_group_in_flight", "Messages leased", |s, _| {
            s.in_flight as f64
        }),
        ("verdandi_group_dead", "Dead letters", |s, _| s.dead as f64),
        (
            "verdandi_group_drain_rate",
            "Settles per second over the last 60 s",
            |s, _| drain_rate(s),
        ),
    ];
    let now = handle.now_ms();
    for (name, help, value) in per_group {
        family(&mut out, name, help);
        for (group, stats) in &rows {
            let _ = writeln!(out, "{name}{{group=\"{group}\"}} {}", value(stats, now));
        }
    }
    family(
        &mut out,
        "verdandi_disk_used_bytes",
        "Bytes used on the data disk",
    );
    if let Some(d) = status.disk {
        let _ = writeln!(
            out,
            "verdandi_disk_used_bytes {}",
            d.total.saturating_sub(d.free)
        );
    }
    family(
        &mut out,
        "verdandi_live_members",
        "Live members in this node's view",
    );
    let _ = writeln!(
        out,
        "verdandi_live_members {}",
        scale(cluster.as_deref().map(|c| &**c)).0
    );
    Ok(([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], out).into_response())
}
