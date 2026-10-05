use std::collections::BTreeMap;

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, Uri, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use utoipa::ToSchema;

use crate::error::{ErrorBody, Fail};
use crate::{NEXT_STEP, Node, OPEN_MODE, host, ready};

const GUIDE: &str = include_str!("guide.txt");
const DAY_S: u64 = 24 * 3600;

#[derive(Serialize, ToSchema)]
pub(crate) struct Status {
    node: Disk,
    copies: Copies,
    streams: Vec<String>,
    groups: Vec<GroupInfo>,
    warnings: Vec<String>,
    links: BTreeMap<&'static str, &'static str>,
}

#[derive(Serialize, ToSchema)]
struct Disk {
    #[schema(required = true)]
    disk_total_bytes: Option<u64>,
    #[schema(required = true)]
    disk_free_bytes: Option<u64>,
    write_bytes_per_s: u64,
    #[schema(required = true)]
    time_until_full_s: Option<u64>,
}

#[derive(Serialize, ToSchema)]
struct Copies {
    ack_count: u32,
    target: u32,
    why: &'static str,
}

#[derive(Serialize, ToSchema)]
struct GroupInfo {
    name: String,
    provisional: bool,
}

const LINKS: [(&str, &str); 10] = [
    ("guide", "GET / with the header Accept: text/plain"),
    ("openapi", "GET /openapi.json"),
    ("publish", "POST /s/{stream}"),
    ("take", "GET /s/{stream}?group={group}"),
    ("ack", "DELETE /r/{receipt}"),
    ("settle", "POST /r/{receipt}"),
    ("read", "GET /s/{stream}"),
    ("group", "GET /g/{group}"),
    ("bulk", "POST /bulk"),
    ("ready", "GET /readyz"),
];

fn wants_guide(headers: &HeaderMap) -> bool {
    let accept = headers
        .get_all(header::ACCEPT)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .collect::<Vec<_>>()
        .join(",")
        .to_ascii_lowercase();
    !accept.contains("application/json")
        && (accept.contains("text/plain") || accept.contains("text/html"))
}

#[utoipa::path(
    get,
    path = "/",
    summary = "Status as JSON. With Accept: text/plain, the guide.",
    responses(
        (status = 200, description = "The status, or the guide", content((Status = "application/json"), (String = "text/plain"))),
        (status = 403, description = "The Host is not an IP address or localhost", body = ErrorBody),
        (status = 503, description = "The node is recovering its data", body = ErrorBody),
    )
)]
pub async fn root(
    State(node): State<Node>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, Fail> {
    if wants_guide(&headers) {
        let text = GUIDE.replace("{host}", host(&headers, &uri).unwrap_or_default());
        return Ok((
            [
                (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
                (header::VARY, "Accept"),
            ],
            text,
        )
            .into_response());
    }
    let status = ready(&node)?.status().await?;
    let mut warnings = vec![OPEN_MODE.to_owned()];
    warnings.extend(status.damage.iter().map(|d| {
        let held = status.damaged.get(&d.segment).filter(|s| !s.is_empty());
        let named = held.map_or_else(String::new, |s| {
            format!("; streams held in this segment: {}", s.join(", "))
        });
        format!(
            "damaged log range: segment {} bytes {}..{}{named}",
            d.segment, d.start, d.end
        )
    }));
    warnings.extend(status.unmapped.iter().map(|w| {
        format!(
            "damaged log range: file {} bytes {}..{}",
            w.file.file_name().unwrap_or_default().display(),
            w.range.start,
            w.range.end
        )
    }));
    warnings.extend(status.dropped.iter().map(|d| {
        format!(
            "dead copy dropped: message {} of stream {} for group {}",
            d.id, d.stream, d.group
        )
    }));
    let time_until_full_s = status
        .disk
        .and_then(|d| d.free.checked_div(status.write_bytes_per_s));
    if let Some(d) = status.disk
        && d.free * 5 < d.total
    {
        warnings.push(format!(
            "disk space low: {} bytes free, {} percent of the disk; {NEXT_STEP}",
            d.free,
            d.free * 100 / d.total
        ));
    }
    if let (Some(d), Some(s)) = (status.disk, time_until_full_s)
        && s < DAY_S
    {
        let when = match s {
            _ if d.free == 0 => String::new(),
            0..=60 => " in about 1 minute at the current write rate".to_owned(),
            61..3600 => format!(
                " in about {} minutes at the current write rate",
                s.div_ceil(60)
            ),
            _ => format!(
                " in about {:.1} hours at the current write rate",
                s as f64 / 3600.0
            ),
        };
        warnings.push(format!(
            "disk full{when}, {} bytes free; {NEXT_STEP}",
            d.free
        ));
    }
    let json = Json(Status {
        node: Disk {
            disk_total_bytes: status.disk.map(|d| d.total),
            disk_free_bytes: status.disk.map(|d| d.free),
            write_bytes_per_s: status.write_bytes_per_s,
            time_until_full_s,
        },
        copies: Copies {
            ack_count: 1,
            target: 1,
            why: "one node",
        },
        streams: status.streams,
        groups: status
            .groups
            .into_iter()
            .map(|(name, provisional)| GroupInfo { name, provisional })
            .collect(),
        warnings,
        links: BTreeMap::from(LINKS),
    });
    Ok(([(header::VARY, "Accept")], json).into_response())
}
