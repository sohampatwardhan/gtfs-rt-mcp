use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;

use crate::error::{err, AppError};

pub const MAX_STATIC_BYTES: usize = 250 * 1024 * 1024;
pub const MAX_RT_BYTES: usize = 80 * 1024 * 1024;

#[derive(Debug, Clone)]
pub enum Loc {
    Http(String),
    File(PathBuf),
}

#[derive(Debug, Clone)]
pub enum FeedSpec {
    FeedSet { url: Loc },
    Direct {
        static_gtfs: Loc,
        trip_updates: Option<Loc>,
        vehicle_positions: Option<Loc>,
        alerts: Option<Loc>,
    },
}

#[derive(Debug, Clone)]
pub struct Manifest {
    pub generation_id: Option<String>,
    pub generated_at_unix: Option<u64>,
    pub static_version: Option<String>,
    pub source: Option<String>,
    pub entity_counts: Option<EntityCounts>,
    pub static_zip: Loc,
    pub trip_updates: Option<Loc>,
    pub vehicle_positions: Option<Loc>,
    pub alerts: Option<Loc>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EntityCounts {
    #[serde(default)]
    pub trip_updates: Option<u64>,
    #[serde(default)]
    pub vehicle_positions: Option<u64>,
    #[serde(default)]
    pub alerts: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct FeedSetDoc {
    #[serde(default)]
    generation_id: Option<String>,
    #[serde(default)]
    generated_at_unix: Option<u64>,
    #[serde(default)]
    static_version: Option<String>,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    entity_counts: Option<EntityCounts>,
    urls: FeedSetUrls,
}

#[derive(Debug, Deserialize)]
struct FeedSetUrls {
    #[serde(alias = "static")]
    static_zip: String,
    #[serde(default)]
    trip_updates: Option<String>,
    #[serde(default)]
    vehicle_positions: Option<String>,
    #[serde(default)]
    alerts: Option<String>,
}

#[derive(Debug, Clone)]
enum Base {
    Http(String),
    File(PathBuf),
}

pub fn parse_feed_set(bytes: &[u8], base: &Loc) -> Result<Manifest, AppError> {
    let doc: FeedSetDoc = serde_json::from_slice(bytes)
        .map_err(|e| err(format!("feed-set JSON: {e}")).hint("expected a /v1/feed-set.json document"))?;
    let base = match base {
        Loc::Http(url) => Base::Http(url.clone()),
        Loc::File(path) => Base::File(path.clone()),
    };
    Ok(Manifest {
        generation_id: doc.generation_id,
        generated_at_unix: doc.generated_at_unix,
        static_version: doc.static_version,
        source: doc.source,
        entity_counts: doc.entity_counts,
        static_zip: resolve_ref(&base, &doc.urls.static_zip)?,
        trip_updates: optional_ref(&base, doc.urls.trip_updates)?,
        vehicle_positions: optional_ref(&base, doc.urls.vehicle_positions)?,
        alerts: optional_ref(&base, doc.urls.alerts)?,
    })
}

fn optional_ref(base: &Base, value: Option<String>) -> Result<Option<Loc>, AppError> {
    match value {
        Some(value) if !value.trim().is_empty() => Ok(Some(resolve_ref(base, &value)?)),
        _ => Ok(None),
    }
}

fn resolve_ref(base: &Base, reference: &str) -> Result<Loc, AppError> {
    let reference = reference.trim();
    if reference.is_empty() {
        return Err(err("feed-set URL is empty"));
    }
    if reference
        .chars()
        .any(|ch| ch.is_control() || ch == ' ')
    {
        return Err(err(format!("refusing feed URL with whitespace or controls: {reference}")));
    }
    if let Some(rest) = reference.strip_prefix("file://") {
        if matches!(base, Base::Http(_)) {
            return Err(err(
                "a remote feed-set cannot point at file:// URLs",
            )
            .hint("fetch artifacts over http(s), or register a local feed-set path"));
        }
        let path = urlencoding_path(rest);
        return Ok(Loc::File(PathBuf::from(path)));
    }
    if reference.starts_with("http://") || reference.starts_with("https://") {
        return Ok(Loc::Http(reference.to_string()));
    }
    if reference.contains("://") {
        return Err(err(format!("unsupported URL scheme in '{reference}'"))
            .hint("use http(s) or a filesystem path"));
    }
    match base {
        Base::Http(manifest_url) => {
            if reference.starts_with('/') {
                Ok(Loc::Http(format!("{}{reference}", origin_of(manifest_url))))
            } else {
                Ok(Loc::Http(join_url_dir(manifest_url, reference)))
            }
        }
        Base::File(path) => {
            let dir = path.parent().unwrap_or_else(|| Path::new("."));
            if reference.starts_with('/') {
                Ok(Loc::File(PathBuf::from(reference)))
            } else {
                Ok(Loc::File(dir.join(reference)))
            }
        }
    }
}

fn origin_of(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let rest = &url[scheme_end + 3..];
    match rest.find('/') {
        Some(index) => url[..scheme_end + 3 + index].to_string(),
        None => url.trim_end_matches('/').to_string(),
    }
}

fn join_url_dir(manifest_url: &str, reference: &str) -> String {
    let trimmed = manifest_url.trim_end_matches('/');
    match trimmed.rfind('/') {
        Some(index) if index > trimmed.find("://").unwrap_or(0) + 2 => {
            format!("{}/{reference}", &trimmed[..index])
        }
        _ => format!("{trimmed}/{reference}"),
    }
}

fn urlencoding_path(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let Ok(byte) = u8::from_str_radix(
                std::str::from_utf8(&bytes[index + 1..index + 3]).unwrap_or(""),
                16,
            ) {
                out.push(byte);
                index += 3;
                continue;
            }
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub fn validate_headers(headers: &BTreeMap<String, String>) -> Result<(), AppError> {
    for (name, value) in headers {
        if name.is_empty()
            || name.bytes().any(|b| b == b'\r' || b == b'\n' || b == b':')
            || value.bytes().any(|b| b == b'\r' || b == b'\n')
        {
            return Err(err(format!("refusing header '{name}'"))
                .hint("header names and values cannot contain CR, LF, or ':'"));
        }
    }
    Ok(())
}

pub async fn fetch_bytes(
    http: &reqwest::Client,
    loc: &Loc,
    headers: &BTreeMap<String, String>,
    max_bytes: usize,
) -> Result<Vec<u8>, AppError> {
    match loc {
        Loc::File(path) => {
            let bytes = tokio::fs::read(path).await.map_err(|e| {
                err(format!("reading {}: {e}", path.display()))
            })?;
            if bytes.len() > max_bytes {
                return Err(err(format!(
                    "{} is {} bytes, over the {} byte limit",
                    path.display(),
                    bytes.len(),
                    max_bytes
                )));
            }
            Ok(bytes)
        }
        Loc::Http(url) => {
            let mut request = http.get(url);
            for (name, value) in headers {
                request = request.header(name, value);
            }
            let response = request.send().await?;
            let status = response.status();
            if !status.is_success() {
                let snippet = response
                    .text()
                    .await
                    .unwrap_or_default()
                    .chars()
                    .take(180)
                    .collect::<String>();
                return Err(err(format!("GET {url} returned {status}: {snippet}"))
                    .extra("status", Value::from(status.as_u16())));
            }
            read_capped(response, max_bytes, url).await
        }
    }
}

async fn read_capped(
    mut response: reqwest::Response,
    max_bytes: usize,
    url: &str,
) -> Result<Vec<u8>, AppError> {
    if let Some(len) = response.content_length() {
        if len as usize > max_bytes {
            return Err(err(format!(
                "{url} declared {len} bytes, over the {max_bytes} byte limit"
            )));
        }
    }
    let mut out = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if out.len().saturating_add(chunk.len()) > max_bytes {
            return Err(err(format!(
                "{url} exceeded the {max_bytes} byte download limit"
            )));
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

/// Whether a fetch failed with HTTP 404. For a feed-set artifact that means its generation was
/// pruned, and the manifest names a newer one.
pub fn is_not_found(error: &AppError) -> bool {
    error.extra.get("status").and_then(Value::as_u64) == Some(404)
}

pub fn loc_label(loc: &Loc) -> String {
    match loc {
        Loc::Http(url) => url.clone(),
        Loc::File(path) => path.display().to_string(),
    }
}

pub fn http_client() -> Result<reqwest::Client, AppError> {
    reqwest::Client::builder()
        .user_agent(concat!("gtfs-rt-mcp/", env!("CARGO_PKG_VERSION")))
        .timeout(Duration::from_secs(45))
        .connect_timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()
        .map_err(|e| err(format!("http client: {e}")))
}

pub fn manifest_json(manifest: &Manifest) -> Value {
    serde_json::json!({
        "generation_id": manifest.generation_id,
        "generated_at_unix": manifest.generated_at_unix,
        "static_version": manifest.static_version,
        "source": manifest.source,
        "entity_counts": manifest.entity_counts.as_ref().map(|counts| serde_json::json!({
            "trip_updates": counts.trip_updates,
            "vehicle_positions": counts.vehicle_positions,
            "alerts": counts.alerts,
        })),
        "static_zip": loc_label(&manifest.static_zip),
        "trip_updates": manifest.trip_updates.as_ref().map(loc_label),
        "vehicle_positions": manifest.vehicle_positions.as_ref().map(loc_label),
        "alerts": manifest.alerts.as_ref().map(loc_label),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_feed_set_rejects_file_urls() {
        let base = Base::Http("https://feeds.example/v1/feed-set.json".into());
        let error = resolve_ref(&base, "file:///etc/passwd").unwrap_err();
        assert!(error.error.contains("file://"));
    }

    #[test]
    fn path_absolute_urls_join_the_origin() {
        let base = Base::Http("https://feeds.example/v1/feed-set.json".into());
        let loc = resolve_ref(&base, "/v1/generations/gen-1/trip-updates.pb").unwrap();
        match loc {
            Loc::Http(url) => {
                assert_eq!(
                    url,
                    "https://feeds.example/v1/generations/gen-1/trip-updates.pb"
                );
            }
            Loc::File(_) => panic!("expected http"),
        }
    }

    #[test]
    fn relative_file_refs_stay_beside_the_manifest() {
        let base = Base::File(PathBuf::from("/tmp/harbor/feed-set.json"));
        let loc = resolve_ref(&base, "static.zip").unwrap();
        match loc {
            Loc::File(path) => assert_eq!(path, PathBuf::from("/tmp/harbor/static.zip")),
            Loc::Http(_) => panic!("expected file"),
        }
    }
}
