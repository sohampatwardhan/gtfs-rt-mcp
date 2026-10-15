use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine;
use serde_json::{json, Value};

use crate::error::{err, AppError};
#[cfg(test)]
use crate::fixtures;
use crate::gtfs::StaticDb;
use crate::gtfs_rt::FeedMessage;
use crate::query::{
    self, AlertQuery, BoardQuery, RtView, ScheduleQuery, TripStatusQuery, TripUpdateQuery,
    VehicleQuery,
};
use crate::realtime::{self, incrementality};
use crate::source::{
    self, fetch_bytes, loc_label, manifest_json, parse_feed_set, validate_headers, FeedSpec, Loc,
    Manifest, MAX_RT_BYTES, MAX_STATIC_BYTES,
};

const MAX_FEEDS: usize = 16;

#[derive(Debug, Clone, Default)]
pub struct RegisterRequest {
    pub feed_id: Option<String>,
    pub feed_set: Option<String>,
    pub static_url: Option<String>,
    pub static_path: Option<String>,
    pub trip_updates_url: Option<String>,
    pub trip_updates_path: Option<String>,
    pub vehicle_positions_url: Option<String>,
    pub vehicle_positions_path: Option<String>,
    pub alerts_url: Option<String>,
    pub alerts_path: Option<String>,
    pub rt_ttl_secs: Option<u64>,
    pub static_ttl_secs: Option<u64>,
    pub headers: BTreeMap<String, String>,
}

struct CachedFeed {
    message: FeedMessage,
    bytes: usize,
    source: String,
}

struct FeedData {
    id: String,
    spec: FeedSpec,
    headers: BTreeMap<String, String>,
    rt_ttl: Duration,
    static_ttl: Duration,
    db: StaticDb,
    static_key: String,
    static_fetched: Instant,
    static_fetched_unix: u64,
    trip_updates: Option<CachedFeed>,
    vehicles: Option<CachedFeed>,
    alerts: Option<CachedFeed>,
    manifest: Option<Manifest>,
    rt_fetched: Option<Instant>,
    rt_fetched_unix: u64,
    last_error: Option<String>,
}

struct Slot {
    refresh: tokio::sync::Mutex<()>,
    data: Mutex<FeedData>,
}

struct FailedFeed {
    request: RegisterRequest,
    error: String,
    hint: Option<String>,
}

pub struct Catalog {
    http: reqwest::Client,
    feeds: Mutex<HashMap<String, std::sync::Arc<Slot>>>,
    failed: Mutex<HashMap<String, FailedFeed>>,
    config_errors: Mutex<Vec<Value>>,
}

impl Catalog {
    pub fn new() -> Result<Self, AppError> {
        Ok(Self {
            http: source::http_client()?,
            feeds: Mutex::new(HashMap::new()),
            failed: Mutex::new(HashMap::new()),
            config_errors: Mutex::new(Vec::new()),
        })
    }

    pub fn add_config_errors(&self, errors: Vec<Value>) {
        if errors.is_empty() {
            return;
        }
        lock(&self.config_errors).extend(errors);
    }

    pub fn note_load_failure(&self, mut request: RegisterRequest, error: &AppError) {
        let Some(id) = request.feed_id.clone().filter(|id| !id.is_empty()) else {
            self.add_config_errors(vec![json!({
                "error": error.error,
                "hint": error.hint,
            })]);
            return;
        };
        if lock(&self.feeds).contains_key(&id) {
            return;
        }
        let mut message = error.error.clone();
        for value in request.headers.values() {
            if value.len() >= 4 {
                message = message.replace(value, "[redacted]");
            }
        }
        request.feed_id = Some(id.clone());
        lock(&self.failed).insert(
            id,
            FailedFeed {
                request,
                error: message,
                hint: error.hint.clone(),
            },
        );
    }

    pub async fn register(&self, request: RegisterRequest) -> Result<Value, AppError> {
        validate_headers(&request.headers)?;
        let id = normalize_id(request.feed_id.as_deref())?;
        let spec = spec_from_request(&request)?;
        let rt_ttl = Duration::from_secs(request.rt_ttl_secs.unwrap_or(15).clamp(1, 3600));
        let static_ttl = Duration::from_secs(
            request
                .static_ttl_secs
                .unwrap_or(6 * 3600)
                .min(7 * 24 * 3600),
        );
        let data = load_feed(
            &self.http,
            id.clone(),
            spec,
            request.headers.clone(),
            rt_ttl,
            static_ttl,
        )
        .await?;
        let summary = health_json(&data, now_unix());
        {
            let replacing_failure = lock(&self.failed).contains_key(&id);
            let failed_count = lock(&self.failed).len();
            let mut feeds = lock(&self.feeds);
            let occupied = feeds.len() + failed_count - usize::from(replacing_failure);
            if !feeds.contains_key(&id) && occupied >= MAX_FEEDS {
                return Err(err(format!("at most {MAX_FEEDS} feeds can be registered")));
            }
            feeds.insert(
                id.clone(),
                std::sync::Arc::new(Slot {
                    refresh: tokio::sync::Mutex::new(()),
                    data: Mutex::new(data),
                }),
            );
        }
        lock(&self.failed).remove(&id);
        Ok(self.decorate(summary))
    }

    pub fn list(&self) -> Value {
        let feeds = lock(&self.feeds);
        let mut rows: Vec<Value> = feeds
            .values()
            .map(|slot| health_json(&lock(&slot.data), now_unix()))
            .collect();
        drop(feeds);
        for (id, failure) in lock(&self.failed).iter() {
            if rows
                .iter()
                .any(|row| row.get("feed_id").and_then(|v| v.as_str()) == Some(id))
            {
                continue;
            }
            rows.push(failure_json(failure));
        }
        rows.sort_by(|a, b| {
            a.get("feed_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .cmp(b.get("feed_id").and_then(|v| v.as_str()).unwrap_or(""))
        });
        self.decorate(json!({ "feeds": rows }))
    }

    pub fn unregister(&self, feed_id: Option<String>) -> Result<Value, AppError> {
        let id = self.resolve_id(feed_id.as_deref())?;
        let removed_live = lock(&self.feeds).remove(&id).is_some();
        let removed_failed = lock(&self.failed).remove(&id).is_some();
        if !removed_live && !removed_failed {
            return Err(err(format!("no feed named '{id}'")));
        }
        Ok(json!({ "removed": id }))
    }

    pub async fn refresh(&self, feed_id: Option<String>) -> Result<Value, AppError> {
        let id = self.resolve_id(feed_id.as_deref())?;
        let failure = lock(&self.failed).remove(&id);
        if let Some(failure) = failure {
            return match self.register(failure.request.clone()).await {
                Ok(value) => Ok(value),
                Err(error) => {
                    self.note_load_failure(failure.request, &error);
                    Err(error)
                }
            };
        }
        self.ensure(&id, true).await?;
        self.health_cached(&id)
    }

    pub async fn health(&self, feed_id: Option<String>) -> Result<Value, AppError> {
        let id = match self.resolve_id(feed_id.as_deref()) {
            Ok(id) => id,
            Err(error)
                if feed_id.as_deref().unwrap_or("").trim().is_empty()
                    && error.error == "feed configuration is invalid" =>
            {
                return Ok(error.to_json());
            }
            Err(error) => return Err(error),
        };
        if let Some(value) = self.failure_document(&id) {
            return Ok(value);
        }
        self.ensure(&id, false).await?;
        self.health_cached(&id)
    }

    pub async fn search_stops(
        &self,
        feed_id: Option<String>,
        query: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Value, AppError> {
        let id = self.require_loaded(feed_id.as_deref())?;
        self.ensure(&id, false).await?;
        let slot = self.slot(&id)?;
        let data = lock(&slot.data);
        let (rows, total) = data.db.search_stops(query, limit, offset)?;
        let stops: Vec<Value> = rows
            .iter()
            .map(|stop| {
                realtime::obj([
                    ("stop_id", json!(stop.stop_id)),
                    ("stop_code", realtime::json_opt(stop.stop_code.clone())),
                    ("stop_name", realtime::json_opt(stop.stop_name.clone())),
                    ("lat", realtime::json_f64(stop.lat)),
                    ("lon", realtime::json_f64(stop.lon)),
                    ("location_type", json!(stop.location_type)),
                    (
                        "parent_station",
                        realtime::json_opt(stop.parent_station.clone()),
                    ),
                    (
                        "platform_code",
                        realtime::json_opt(stop.platform_code.clone()),
                    ),
                ])
            })
            .collect();
        Ok(page_list(&id, "stops", stops, total as usize, offset))
    }

    pub async fn search_routes(
        &self,
        feed_id: Option<String>,
        query: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Value, AppError> {
        let id = self.require_loaded(feed_id.as_deref())?;
        self.ensure(&id, false).await?;
        let slot = self.slot(&id)?;
        let data = lock(&slot.data);
        let (rows, total) = data.db.search_routes(query, limit, offset)?;
        let routes: Vec<Value> = rows
            .iter()
            .map(|route| {
                realtime::obj([
                    ("route_id", json!(route.route_id)),
                    (
                        "route_short_name",
                        realtime::json_opt(route.short_name.clone()),
                    ),
                    (
                        "route_long_name",
                        realtime::json_opt(route.long_name.clone()),
                    ),
                    ("route_type", json!(route.route_type)),
                    (
                        "route_type_name",
                        json!(crate::gtfs::route_type_name(route.route_type)),
                    ),
                    ("route_color", realtime::json_opt(route.color.clone())),
                ])
            })
            .collect();
        Ok(page_list(&id, "routes", routes, total as usize, offset))
    }

    pub async fn trip_status(
        &self,
        feed_id: Option<String>,
        trip: &str,
        stop: Option<String>,
        at_unix: Option<i64>,
        upcoming_limit: usize,
    ) -> Result<Value, AppError> {
        self.query(feed_id, |db, rt| {
            query::trip_status(
                db,
                rt,
                &TripStatusQuery {
                    trip: trip.to_string(),
                    stop,
                    at_unix: at_unix.unwrap_or_else(now_unix),
                    upcoming_limit,
                },
            )
        })
        .await
    }

    pub async fn trip_schedule(
        &self,
        feed_id: Option<String>,
        trip: &str,
        at_unix: Option<i64>,
        limit: usize,
        offset: usize,
    ) -> Result<Value, AppError> {
        self.query(feed_id, |db, rt| {
            query::trip_schedule(
                db,
                rt,
                &ScheduleQuery {
                    trip: trip.to_string(),
                    at_unix: at_unix.unwrap_or_else(now_unix),
                    limit,
                    offset,
                },
            )
        })
        .await
    }

    pub async fn departure_board(
        &self,
        feed_id: Option<String>,
        stop: &str,
        route: Option<String>,
        at_unix: Option<i64>,
        window_minutes: u64,
        past_minutes: u64,
        limit: usize,
        offset: usize,
        include_skipped: bool,
    ) -> Result<Value, AppError> {
        self.query(feed_id, |db, rt| {
            query::departure_board(
                db,
                rt,
                &BoardQuery {
                    stop: stop.to_string(),
                    route,
                    at_unix: at_unix.unwrap_or_else(now_unix),
                    window_minutes,
                    past_minutes,
                    limit,
                    offset,
                    include_skipped,
                },
            )
        })
        .await
    }

    pub async fn vehicles(
        &self,
        feed_id: Option<String>,
        query: VehicleQuery,
    ) -> Result<Value, AppError> {
        self.query(feed_id, |db, rt| query::vehicle_positions(db, rt, &query))
            .await
    }

    pub async fn trip_updates(
        &self,
        feed_id: Option<String>,
        query: TripUpdateQuery,
    ) -> Result<Value, AppError> {
        self.query(feed_id, |db, rt| query::trip_updates(db, rt, &query))
            .await
    }

    pub async fn alerts(
        &self,
        feed_id: Option<String>,
        query: AlertQuery,
    ) -> Result<Value, AppError> {
        self.query(feed_id, |db, rt| query::alerts(db, rt, &query))
            .await
    }

    pub async fn decode(
        &self,
        path: Option<String>,
        url: Option<String>,
        base64_body: Option<String>,
        offset: usize,
        limit: usize,
        max_chars: usize,
        headers: BTreeMap<String, String>,
    ) -> Result<Value, AppError> {
        validate_headers(&headers)?;
        let sources = [
            path.as_ref()
                .filter(|s| !s.trim().is_empty())
                .map(|_| "path"),
            url.as_ref().filter(|s| !s.trim().is_empty()).map(|_| "url"),
            base64_body
                .as_ref()
                .filter(|s| !s.trim().is_empty())
                .map(|_| "base64"),
        ]
        .into_iter()
        .flatten()
        .count();
        if sources != 1 {
            return Err(err("pass exactly one of path, url, or base64"));
        }
        let bytes = if let Some(path) = path.filter(|s| !s.trim().is_empty()) {
            fetch_bytes(
                &self.http,
                &Loc::File(PathBuf::from(path)),
                &headers,
                MAX_RT_BYTES,
            )
            .await?
        } else if let Some(url) = url.filter(|s| !s.trim().is_empty()) {
            let loc = if url.starts_with("http://") || url.starts_with("https://") {
                Loc::Http(url)
            } else {
                return Err(err("url must be http or https"));
            };
            fetch_bytes(&self.http, &loc, &headers, MAX_RT_BYTES).await?
        } else {
            decode_base64(base64_body.as_deref().unwrap_or(""))?
        };
        let bytes_len = bytes.len();
        let message = tokio::task::spawn_blocking(move || realtime::decode_feed(&bytes))
            .await
            .map_err(|e| err(format!("decode task: {e}")))??;
        let mut value = query::decode_message(&message, offset, limit, max_chars);
        if let Some(object) = value.as_object_mut() {
            object.insert("bytes".into(), json!(bytes_len));
        }
        Ok(value)
    }

    async fn query<F>(&self, feed_id: Option<String>, f: F) -> Result<Value, AppError>
    where
        F: FnOnce(&StaticDb, RtView<'_>) -> Result<Value, AppError>,
    {
        let id = self.require_loaded(feed_id.as_deref())?;
        self.ensure(&id, false).await?;
        let slot = self.slot(&id)?;
        let data = lock(&slot.data);
        let view = RtView {
            trip_updates: data.trip_updates.as_ref().map(|c| &c.message),
            vehicles: data.vehicles.as_ref().map(|c| &c.message),
            alerts: data.alerts.as_ref().map(|c| &c.message),
        };
        let mut value = f(&data.db, view)?;
        if let Some(object) = value.as_object_mut() {
            object.insert("feed_id".into(), json!(id));
        }
        Ok(value)
    }

    fn health_cached(&self, id: &str) -> Result<Value, AppError> {
        let value = {
            let slot = self.slot(id)?;
            let data = lock(&slot.data);
            health_json(&data, now_unix())
        };
        Ok(self.decorate(value))
    }

    fn resolve_id(&self, requested: Option<&str>) -> Result<String, AppError> {
        let feeds = lock(&self.feeds);
        let failed = lock(&self.failed);
        let mut known: Vec<String> = feeds
            .keys()
            .cloned()
            .chain(failed.keys().cloned())
            .collect();
        known.sort();
        known.dedup();
        if let Some(id) = requested.map(str::trim).filter(|id| !id.is_empty()) {
            if known.iter().any(|known| known == id) {
                return Ok(id.to_string());
            }
            return Err(self.with_config_errors(
                err(format!("no feed named '{id}'")).extra("feeds", json!(known)),
            ));
        }
        match known.len() {
            0 => {
                let errors = lock(&self.config_errors).clone();
                drop(failed);
                drop(feeds);
                if errors.is_empty() {
                    Err(err("no feed registered").hint(
                        "set GTFS_RT_MCP_FEED_SET or GTFS_RT_MCP_STATIC in the MCP client env, or call register_feed",
                    ))
                } else {
                    Err(err("feed configuration is invalid")
                        .hint("see config_errors")
                        .extra("config_errors", json!(errors)))
                }
            }
            1 => Ok(known.pop().unwrap()),
            _ => Err(self.with_config_errors(
                err("multiple feeds are registered; pass feed_id").extra("feeds", json!(known)),
            )),
        }
    }

    fn require_loaded(&self, requested: Option<&str>) -> Result<String, AppError> {
        let id = self.resolve_id(requested)?;
        if let Some(message) = self.failure_message(&id) {
            return Err(err(message).hint(
                "this feed did not load from its startup configuration; fix it and call refresh_feed",
            ));
        }
        Ok(id)
    }

    fn with_config_errors(&self, error: AppError) -> AppError {
        let errors = lock(&self.config_errors).clone();
        if errors.is_empty() {
            error
        } else {
            error.extra("config_errors", json!(errors))
        }
    }

    fn failure_message(&self, id: &str) -> Option<String> {
        lock(&self.failed)
            .get(id)
            .map(|failure| failure.error.clone())
    }

    fn failure_document(&self, id: &str) -> Option<Value> {
        lock(&self.failed)
            .get(id)
            .map(|failure| self.decorate(failure_json(failure)))
    }

    fn decorate(&self, mut value: Value) -> Value {
        let errors = lock(&self.config_errors).clone();
        if errors.is_empty() {
            return value;
        }
        if let Some(object) = value.as_object_mut() {
            object.insert("config_errors".into(), json!(errors));
        }
        value
    }

    fn slot(&self, id: &str) -> Result<std::sync::Arc<Slot>, AppError> {
        lock(&self.feeds)
            .get(id)
            .cloned()
            .ok_or_else(|| err(format!("no feed named '{id}'")))
    }

    async fn ensure(&self, id: &str, force: bool) -> Result<(), AppError> {
        let slot = self.slot(id)?;
        let _guard = slot.refresh.lock().await;
        let plan = {
            let data = lock(&slot.data);
            let rt_age = data
                .rt_fetched
                .map(|t| t.elapsed())
                .unwrap_or(Duration::MAX);
            let static_age = data.static_fetched.elapsed();
            let need_rt = force || rt_age >= data.rt_ttl;
            let recheck_static = data.static_ttl > Duration::ZERO && static_age >= data.static_ttl;
            if !need_rt && !recheck_static {
                return Ok(());
            }
            RefreshPlan {
                id: data.id.clone(),
                spec: data.spec.clone(),
                headers: data.headers.clone(),
                rt_ttl: data.rt_ttl,
                static_ttl: data.static_ttl,
                static_key: data.static_key.clone(),
                generation: data.manifest.as_ref().and_then(|m| m.generation_id.clone()),
                need_rt,
                // Direct feeds republish one mutable zip. Feed-set static files are immutable,
                // so a static timer only re-reads the manifest.
                reload_direct_static: force || recheck_static,
            }
        };
        let update = match refresh_loaded(&self.http, &plan).await {
            Ok(update) => update,
            Err(error) => {
                let mut data = lock(&slot.data);
                data.last_error = Some(error.error.clone());
                if data.db.counts.stops > 0 {
                    return Ok(());
                }
                return Err(error);
            }
        };
        let mut data = lock(&slot.data);
        apply_update(&mut data, update);
        Ok(())
    }
}

struct RefreshPlan {
    id: String,
    spec: FeedSpec,
    headers: BTreeMap<String, String>,
    rt_ttl: Duration,
    static_ttl: Duration,
    static_key: String,
    generation: Option<String>,
    need_rt: bool,
    reload_direct_static: bool,
}

enum FeedUpdate {
    Full(FeedData),
    Realtime {
        manifest: Option<Manifest>,
        trip_updates: Option<CachedFeed>,
        vehicles: Option<CachedFeed>,
        alerts: Option<CachedFeed>,
    },
    ManifestOnly(Manifest),
}

fn apply_update(data: &mut FeedData, update: FeedUpdate) {
    match update {
        FeedUpdate::Full(fresh) => *data = fresh,
        FeedUpdate::Realtime {
            manifest,
            trip_updates,
            vehicles,
            alerts,
        } => {
            if let Some(manifest) = manifest {
                data.manifest = Some(manifest);
            }
            data.trip_updates = trip_updates;
            data.vehicles = vehicles;
            data.alerts = alerts;
            data.rt_fetched = Some(Instant::now());
            data.rt_fetched_unix = now_unix() as u64;
            data.last_error = None;
        }
        FeedUpdate::ManifestOnly(manifest) => {
            data.manifest = Some(manifest);
            data.last_error = None;
        }
    }
}

fn static_key_for(manifest: &Manifest) -> String {
    format!(
        "{}|{}",
        loc_label(&manifest.static_zip),
        manifest.static_version.clone().unwrap_or_default()
    )
}

fn same_generation(plan: &RefreshPlan, manifest: &Manifest) -> bool {
    match (&plan.generation, &manifest.generation_id) {
        (Some(previous), Some(next)) => {
            previous == next && plan.static_key == static_key_for(manifest)
        }
        _ => false,
    }
}

async fn refresh_loaded(
    http: &reqwest::Client,
    plan: &RefreshPlan,
) -> Result<FeedUpdate, AppError> {
    match &plan.spec {
        FeedSpec::FeedSet { url } => {
            let bytes = fetch_bytes(http, url, &plan.headers, 2_000_000).await?;
            let manifest = parse_feed_set(&bytes, url)?;
            if same_generation(plan, &manifest) {
                if !plan.need_rt {
                    return Ok(FeedUpdate::ManifestOnly(manifest));
                }
                let (trip_updates, vehicles, alerts) = download_triplet(
                    http,
                    &plan.headers,
                    manifest.trip_updates.as_ref(),
                    manifest.vehicle_positions.as_ref(),
                    manifest.alerts.as_ref(),
                )
                .await?;
                return Ok(FeedUpdate::Realtime {
                    manifest: Some(manifest),
                    trip_updates,
                    vehicles,
                    alerts,
                });
            }
            Ok(FeedUpdate::Full(
                load_feed_set(http, plan, url, manifest).await?,
            ))
        }
        FeedSpec::Direct {
            static_gtfs,
            trip_updates,
            vehicle_positions,
            alerts,
        } => {
            if !plan.reload_direct_static {
                let (trip_updates, vehicles, alerts) = download_triplet(
                    http,
                    &plan.headers,
                    trip_updates.as_ref(),
                    vehicle_positions.as_ref(),
                    alerts.as_ref(),
                )
                .await?;
                return Ok(FeedUpdate::Realtime {
                    manifest: None,
                    trip_updates,
                    vehicles,
                    alerts,
                });
            }
            Ok(FeedUpdate::Full(
                load_direct(
                    http,
                    plan,
                    static_gtfs,
                    trip_updates.as_ref(),
                    vehicle_positions.as_ref(),
                    alerts.as_ref(),
                )
                .await?,
            ))
        }
    }
}

async fn load_feed(
    http: &reqwest::Client,
    id: String,
    spec: FeedSpec,
    headers: BTreeMap<String, String>,
    rt_ttl: Duration,
    static_ttl: Duration,
) -> Result<FeedData, AppError> {
    let plan = RefreshPlan {
        id,
        spec: spec.clone(),
        headers,
        rt_ttl,
        static_ttl,
        static_key: String::new(),
        generation: None,
        need_rt: true,
        reload_direct_static: true,
    };
    match spec {
        FeedSpec::FeedSet { url } => {
            let bytes = fetch_bytes(http, &url, &plan.headers, 2_000_000).await?;
            let manifest = parse_feed_set(&bytes, &url)?;
            load_feed_set(http, &plan, &url, manifest).await
        }
        FeedSpec::Direct {
            static_gtfs,
            trip_updates,
            vehicle_positions,
            alerts,
        } => {
            load_direct(
                http,
                &plan,
                &static_gtfs,
                trip_updates.as_ref(),
                vehicle_positions.as_ref(),
                alerts.as_ref(),
            )
            .await
        }
    }
}

async fn load_feed_set(
    http: &reqwest::Client,
    plan: &RefreshPlan,
    url: &Loc,
    manifest: Manifest,
) -> Result<FeedData, AppError> {
    let static_key = static_key_for(&manifest);
    let zip = fetch_bytes(http, &manifest.static_zip, &plan.headers, MAX_STATIC_BYTES).await?;
    let db = tokio::task::spawn_blocking(move || StaticDb::load_zip(&zip))
        .await
        .map_err(|e| err(format!("static load: {e}")))??;
    let (trip_updates, vehicles, alerts) = download_triplet(
        http,
        &plan.headers,
        manifest.trip_updates.as_ref(),
        manifest.vehicle_positions.as_ref(),
        manifest.alerts.as_ref(),
    )
    .await?;
    Ok(FeedData {
        id: plan.id.clone(),
        spec: FeedSpec::FeedSet { url: url.clone() },
        headers: plan.headers.clone(),
        rt_ttl: plan.rt_ttl,
        static_ttl: plan.static_ttl,
        db,
        static_key,
        static_fetched: Instant::now(),
        static_fetched_unix: now_unix() as u64,
        trip_updates,
        vehicles,
        alerts,
        manifest: Some(manifest),
        rt_fetched: Some(Instant::now()),
        rt_fetched_unix: now_unix() as u64,
        last_error: None,
    })
}

async fn load_direct(
    http: &reqwest::Client,
    plan: &RefreshPlan,
    static_gtfs: &Loc,
    trip_updates: Option<&Loc>,
    vehicle_positions: Option<&Loc>,
    alerts: Option<&Loc>,
) -> Result<FeedData, AppError> {
    let static_key = loc_label(static_gtfs);
    let zip = fetch_bytes(http, static_gtfs, &plan.headers, MAX_STATIC_BYTES).await?;
    let db = tokio::task::spawn_blocking(move || StaticDb::load_zip(&zip))
        .await
        .map_err(|e| err(format!("static load: {e}")))??;
    let (trip_updates, vehicles, alerts) =
        download_triplet(http, &plan.headers, trip_updates, vehicle_positions, alerts).await?;
    Ok(FeedData {
        id: plan.id.clone(),
        spec: plan.spec.clone(),
        headers: plan.headers.clone(),
        rt_ttl: plan.rt_ttl,
        static_ttl: plan.static_ttl,
        db,
        static_key,
        static_fetched: Instant::now(),
        static_fetched_unix: now_unix() as u64,
        trip_updates,
        vehicles,
        alerts,
        manifest: None,
        rt_fetched: Some(Instant::now()),
        rt_fetched_unix: now_unix() as u64,
        last_error: None,
    })
}

async fn download_triplet(
    http: &reqwest::Client,
    headers: &BTreeMap<String, String>,
    trip_updates: Option<&Loc>,
    vehicles: Option<&Loc>,
    alerts: Option<&Loc>,
) -> Result<(Option<CachedFeed>, Option<CachedFeed>, Option<CachedFeed>), AppError> {
    let trip_updates = download_feed(http, headers, trip_updates).await?;
    let vehicles = download_feed(http, headers, vehicles).await?;
    let alerts = download_feed(http, headers, alerts).await?;
    Ok((trip_updates, vehicles, alerts))
}

async fn download_feed(
    http: &reqwest::Client,
    headers: &BTreeMap<String, String>,
    loc: Option<&Loc>,
) -> Result<Option<CachedFeed>, AppError> {
    let Some(loc) = loc else {
        return Ok(None);
    };
    let source = loc_label(loc);
    let bytes = fetch_bytes(http, loc, headers, MAX_RT_BYTES).await?;
    let len = bytes.len();
    let message = tokio::task::spawn_blocking(move || realtime::decode_feed(&bytes))
        .await
        .map_err(|e| err(format!("decode task: {e}")))??;
    Ok(Some(CachedFeed {
        message,
        bytes: len,
        source,
    }))
}

fn health_json(data: &FeedData, now: i64) -> Value {
    let source = match &data.spec {
        FeedSpec::FeedSet { url } => json!({
            "kind": "feed_set",
            "url": loc_label(url),
        }),
        FeedSpec::Direct {
            static_gtfs,
            trip_updates,
            vehicle_positions,
            alerts,
        } => json!({
            "kind": "direct",
            "static": loc_label(static_gtfs),
            "trip_updates": trip_updates.as_ref().map(loc_label),
            "vehicle_positions": vehicle_positions.as_ref().map(loc_label),
            "alerts": alerts.as_ref().map(loc_label),
        }),
    };
    let rt_age = data
        .rt_fetched
        .map(|t| t.elapsed().as_secs())
        .unwrap_or(u64::MAX);
    json!({
        "feed_id": data.id,
        "ready": true,
        "source": source,
        "headers": data.headers.keys().cloned().collect::<Vec<_>>(),
        "static": {
            "agency": data.db.agency_name,
            "timezone": data.db.tz_name,
            "feed_version": data.db.feed_version,
            "publisher": data.db.publisher,
            "agencies": data.db.counts.agencies,
            "stops": data.db.counts.stops,
            "routes": data.db.counts.routes,
            "trips": data.db.counts.trips,
            "stop_times": data.db.counts.stop_times,
            "calendar": data.db.counts.calendar,
            "calendar_dates": data.db.counts.calendar_dates,
            "fetched_unix": data.static_fetched_unix,
            "fetched_age_sec": data.static_fetched.elapsed().as_secs(),
        },
        "realtime": {
            "ttl_sec": data.rt_ttl.as_secs(),
            "fetched_age_sec": rt_age.min(i64::MAX as u64),
            "stale": data.rt_fetched.map(|t| t.elapsed() > data.rt_ttl).unwrap_or(true),
            "last_error": data.last_error,
            "trip_updates": feed_health(data.trip_updates.as_ref(), now),
            "vehicle_positions": feed_health(data.vehicles.as_ref(), now),
            "alerts": feed_health(data.alerts.as_ref(), now),
        },
        "feed_set": data.manifest.as_ref().map(manifest_json),
    })
}

fn failure_json(failure: &FailedFeed) -> Value {
    let request = &failure.request;
    let source = if let Some(feed_set) = &request.feed_set {
        json!({ "kind": "feed_set", "url": feed_set })
    } else {
        json!({
            "kind": "direct",
            "static": request.static_url.clone().or(request.static_path.clone()),
            "trip_updates": request.trip_updates_url.clone().or(request.trip_updates_path.clone()),
            "vehicle_positions": request
                .vehicle_positions_url
                .clone()
                .or(request.vehicle_positions_path.clone()),
            "alerts": request.alerts_url.clone().or(request.alerts_path.clone()),
        })
    };
    realtime::obj([
        ("feed_id", json!(failure.request.feed_id)),
        ("ready", json!(false)),
        ("error", json!(failure.error)),
        ("hint", realtime::json_opt(failure.hint.clone())),
        (
            "headers",
            json!(request.headers.keys().cloned().collect::<Vec<_>>()),
        ),
        ("source", source),
    ])
}

fn feed_health(feed: Option<&CachedFeed>, now: i64) -> Value {
    let Some(feed) = feed else {
        return Value::Null;
    };
    let timestamp = feed.message.header.timestamp;
    let header_age = timestamp.and_then(|ts| now.checked_sub(ts as i64));
    realtime::obj([
        ("entities", json!(feed.message.entity.len())),
        ("bytes", json!(feed.bytes)),
        ("source", json!(feed.source)),
        (
            "header_timestamp",
            realtime::json_i64(timestamp.map(|v| v as i64)),
        ),
        ("header_age_sec", realtime::json_i64(header_age)),
        (
            "incrementality",
            realtime::json_opt(incrementality(feed.message.header.incrementality)),
        ),
        (
            "feed_version",
            realtime::json_opt(feed.message.header.feed_version.clone()),
        ),
    ])
}

fn page_list(feed_id: &str, key: &str, rows: Vec<Value>, total: usize, offset: usize) -> Value {
    let next = offset + rows.len();
    let mut value = json!({
        "feed_id": feed_id,
        "total": total,
        "offset": offset,
        "returned": rows.len(),
        "next_offset": if next < total { serde_json::Value::from(next) } else { Value::Null },
    });
    if let Some(object) = value.as_object_mut() {
        object.insert(key.to_string(), Value::Array(rows));
    }
    value
}

fn spec_from_request(request: &RegisterRequest) -> Result<FeedSpec, AppError> {
    let feed_set = nonempty(request.feed_set.clone());
    let static_loc = one_loc(
        request.static_url.clone(),
        request.static_path.clone(),
        "static GTFS",
    )?;
    if feed_set.is_some() && static_loc.is_some() {
        return Err(err("pass either feed_set or a static zip, not both"));
    }
    if let Some(feed_set) = feed_set {
        return Ok(FeedSpec::FeedSet {
            url: parse_user_loc(&feed_set)?,
        });
    }
    let Some(static_gtfs) = static_loc else {
        return Err(err("pass feed_set, static_url, or static_path"));
    };
    let trip_updates = one_loc(
        request.trip_updates_url.clone(),
        request.trip_updates_path.clone(),
        "trip updates",
    )?;
    let vehicle_positions = one_loc(
        request.vehicle_positions_url.clone(),
        request.vehicle_positions_path.clone(),
        "vehicle positions",
    )?;
    let alerts = one_loc(
        request.alerts_url.clone(),
        request.alerts_path.clone(),
        "alerts",
    )?;
    Ok(FeedSpec::Direct {
        static_gtfs,
        trip_updates,
        vehicle_positions,
        alerts,
    })
}

fn one_loc(
    url: Option<String>,
    path: Option<String>,
    label: &str,
) -> Result<Option<Loc>, AppError> {
    match (nonempty(url), nonempty(path)) {
        (Some(_), Some(_)) => Err(err(format!("{label}: pass a url or a path, not both"))),
        (Some(url), None) => Ok(Some(parse_user_loc(&url)?)),
        (None, Some(path)) => Ok(Some(Loc::File(PathBuf::from(path)))),
        (None, None) => Ok(None),
    }
}

fn parse_user_loc(value: &str) -> Result<Loc, AppError> {
    if value.starts_with("http://") || value.starts_with("https://") {
        Ok(Loc::Http(value.to_string()))
    } else if let Some(rest) = value.strip_prefix("file://") {
        Ok(Loc::File(PathBuf::from(percent_decode(rest))))
    } else if value.contains("://") {
        Err(err(format!("unsupported scheme in '{value}'"))
            .hint("use http(s) or a filesystem path"))
    } else {
        Ok(Loc::File(PathBuf::from(value)))
    }
}

fn percent_decode(value: &str) -> String {
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

fn nonempty(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

pub(crate) fn validate_startup_request(request: &RegisterRequest) -> Result<String, AppError> {
    validate_headers(&request.headers)?;
    let id = normalize_id(request.feed_id.as_deref())?;
    spec_from_request(request)?;
    if let Some(ttl) = request.rt_ttl_secs {
        if !(1..=3600).contains(&ttl) {
            return Err(err(format!(
                "realtime TTL must be 1-3600 seconds, got {ttl}"
            )));
        }
    }
    if let Some(ttl) = request.static_ttl_secs {
        if ttl > 7 * 24 * 3600 {
            return Err(err(format!(
                "static TTL must be 0-604800 seconds, got {ttl}"
            )));
        }
    }
    Ok(id)
}

fn normalize_id(id: Option<&str>) -> Result<String, AppError> {
    let id = id
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .unwrap_or("default");
    if id.len() > 64
        || !id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | ':' | '-'))
    {
        return Err(err(
            "feed_id must be 1-64 characters of letters, numbers, '_', '.', ':', or '-'",
        ));
    }
    Ok(id.to_string())
}

fn decode_base64(value: &str) -> Result<Vec<u8>, AppError> {
    let cleaned: String = value.chars().filter(|ch| !ch.is_whitespace()).collect();
    base64::engine::general_purpose::STANDARD
        .decode(&cleaned)
        .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(&cleaned))
        .map_err(|e| err(format!("base64: {e}")))
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn temp_dir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "gtfs-rt-mcp-{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[tokio::test]
    async fn harbor_feed_set_answers_status_board_and_alerts() {
        let dir = temp_dir("harbor");
        let paths = fixtures::materialize(&dir).unwrap();
        let catalog = Catalog::new().unwrap();
        catalog
            .register(RegisterRequest {
                feed_id: Some("harbor".into()),
                feed_set: Some(paths.feed_set.display().to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        let status = catalog
            .trip_status(None, "177", Some("NHV".into()), Some(paths.at_unix), 6)
            .await
            .unwrap();
        let trip = &status["trips"][0];
        assert_eq!(trip["trip_short_name"], "177");
        assert_eq!(trip["route_short_name"], "NEC");
        assert_eq!(trip["vehicle"]["status"], "IN_TRANSIT_TO");
        assert_eq!(trip["vehicle"]["stop_id"], "NHV");
        assert_eq!(trip["at_stop"]["track"], "12");
        assert_eq!(trip["at_stop"]["assigned_stop_id"], "NHV:track:12");
        assert_eq!(trip["at_stop"]["arrival_delay_sec"], 420);
        assert_eq!(trip["at_stop"]["departure_delay_sec"], 480);
        assert_eq!(trip["at_stop"]["scheduled_departure_local"], "12:04");
        assert_eq!(trip["at_stop"]["estimated_departure_local"], "12:12");
        assert!(trip["delay_label"].as_str().unwrap().contains("late"));

        let board = catalog
            .departure_board(None, "NHV", None, Some(paths.at_unix), 120, 5, 20, 0, false)
            .await
            .unwrap();
        let short_names: Vec<_> = board["departures"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["trip_short_name"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(short_names, vec!["177".to_string(), "B1".to_string()]);
        assert_eq!(board["departures"][0]["track"], "12");

        let from_platform = catalog
            .departure_board(
                None,
                "NHV:track:12",
                None,
                Some(paths.at_unix),
                120,
                5,
                20,
                0,
                false,
            )
            .await
            .unwrap();
        assert_eq!(from_platform["departures"][0]["trip_short_name"], "177");

        let alerts = catalog
            .alerts(
                None,
                AlertQuery {
                    route: Some("NEC".into()),
                    stop: None,
                    text: None,
                    active_only: true,
                    at_unix: paths.at_unix,
                    limit: 10,
                    offset: 0,
                    max_chars: 200,
                },
            )
            .await
            .unwrap();
        assert_eq!(alerts["returned"], 1);
        assert_eq!(
            alerts["alerts"][0]["header"],
            "Track work between NYP and NHV"
        );
        assert_eq!(alerts["alerts"][0]["effect"], "SIGNIFICANT_DELAYS");
        assert_eq!(
            alerts["alerts"][0]["informed_entity"][0]["route_short_name"],
            "NEC"
        );

        let decoded = catalog
            .decode(
                Some(paths.trip_updates.display().to_string()),
                None,
                None,
                0,
                10,
                200,
                BTreeMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            decoded["entities"][0]["trip_update"]["stop_updates"][1]["assigned_stop_id"],
            "NHV:track:12"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn remote_feed_set_cannot_read_local_files() {
        let app = axum::Router::new().route(
            "/v1/feed-set.json",
            axum::routing::get(|| async {
                axum::Json(json!({
                    "generation_id": "bad",
                    "urls": {
                        "static_zip": "file:///etc/passwd",
                        "trip_updates": "file:///etc/passwd",
                        "vehicle_positions": "file:///etc/passwd",
                        "alerts": "file:///etc/passwd"
                    }
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let catalog = Catalog::new().unwrap();
        let error = catalog
            .register(RegisterRequest {
                feed_set: Some(format!("http://{addr}/v1/feed-set.json")),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(error.error.contains("file://"), "{}", error.error);
    }

    #[tokio::test]
    async fn http_feed_set_joins_origin_and_serves_bytes() {
        let dir = temp_dir("http-feed");
        let paths = fixtures::materialize(&dir).unwrap();
        let root = Arc::new(dir.clone());
        let at = paths.at_unix;
        let app = axum::Router::new()
            .route(
                "/v1/feed-set.json",
                axum::routing::get(move || async move {
                    axum::Json(json!({
                        "generation_id": "gen-1",
                        "generated_at_unix": at,
                        "static_version": "harbor-1",
                        "source": "fixture",
                        "urls": {
                            "static_zip": "/v1/generations/gen-1/static.zip",
                            "trip_updates": "/v1/generations/gen-1/trip-updates.pb",
                            "vehicle_positions": "/v1/generations/gen-1/vehicle-positions.pb",
                            "alerts": "/v1/generations/gen-1/alerts.pb"
                        }
                    }))
                }),
            )
            .route(
                "/v1/generations/{id}/{name}",
                axum::routing::get({
                    let root = Arc::clone(&root);
                    move |axum::extract::Path((_, name)): axum::extract::Path<(String, String)>| {
                        let root = Arc::clone(&root);
                        async move {
                            let path = root.join(&name);
                            match tokio::fs::read(path).await {
                                Ok(bytes) => (axum::http::StatusCode::OK, bytes),
                                Err(_) => (axum::http::StatusCode::NOT_FOUND, Vec::new()),
                            }
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let catalog = Catalog::new().unwrap();
        catalog
            .register(RegisterRequest {
                feed_set: Some(format!("http://{addr}/v1/feed-set.json")),
                ..Default::default()
            })
            .await
            .unwrap();
        let health = catalog.health(None).await.unwrap();
        assert_eq!(health["feed_set"]["generation_id"], "gen-1");
        assert_eq!(health["realtime"]["vehicle_positions"]["entities"], 1);
        assert_eq!(health["static"]["stops"], 6);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn unchanged_generation_does_not_redownload_static() {
        let dir = temp_dir("gen-skip");
        let paths = fixtures::materialize(&dir).unwrap();
        let root = Arc::new(dir.clone());
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let generation = Arc::new(std::sync::Mutex::new("gen-1".to_string()));
        let at = paths.at_unix;
        let app = axum::Router::new()
            .route(
                "/v1/feed-set.json",
                axum::routing::get({
                    let generation = Arc::clone(&generation);
                    move || {
                        let generation = Arc::clone(&generation);
                        async move {
                            let id = generation.lock().unwrap().clone();
                            axum::Json(json!({
                                "generation_id": id,
                                "generated_at_unix": at,
                                "static_version": "harbor-1",
                                "urls": {
                                    "static_zip": "/v1/generations/gen/static.zip",
                                    "trip_updates": "/v1/generations/gen/trip-updates.pb",
                                    "vehicle_positions": "/v1/generations/gen/vehicle-positions.pb",
                                    "alerts": "/v1/generations/gen/alerts.pb"
                                }
                            }))
                        }
                    }
                }),
            )
            .route(
                "/v1/generations/{id}/{name}",
                axum::routing::get({
                    let root = Arc::clone(&root);
                    let hits = Arc::clone(&hits);
                    move |axum::extract::Path((_, name)): axum::extract::Path<(String, String)>| {
                        let root = Arc::clone(&root);
                        let hits = Arc::clone(&hits);
                        async move {
                            if name == "static.zip" {
                                hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            }
                            match tokio::fs::read(root.join(&name)).await {
                                Ok(bytes) => (axum::http::StatusCode::OK, bytes),
                                Err(_) => (axum::http::StatusCode::NOT_FOUND, Vec::new()),
                            }
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let catalog = Catalog::new().unwrap();
        catalog
            .register(RegisterRequest {
                feed_set: Some(format!("http://{addr}/v1/feed-set.json")),
                rt_ttl_secs: Some(1),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let health = catalog.health(None).await.unwrap();
        assert_eq!(health["feed_set"]["generation_id"], "gen-1");
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
        *generation.lock().unwrap() = "gen-2".into();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let health = catalog.health(None).await.unwrap();
        assert_eq!(health["feed_set"]["generation_id"], "gen-2");
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 2);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn failed_startup_feed_is_visible_and_register_clears_it() {
        let secret = "header-secret-value";
        let catalog = Catalog::new().unwrap();
        catalog.add_config_errors(vec![json!({
            "error": "numbered feeds skip 2",
            "var": "GTFS_RT_MCP_FEED_2_STATIC"
        })]);
        let mut headers = BTreeMap::new();
        headers.insert("x-api-key".into(), secret.into());
        catalog.note_load_failure(
            RegisterRequest {
                feed_id: Some("amtrak".into()),
                static_path: Some("/no/such/gtfs-rt-mcp-static.zip".into()),
                headers,
                ..Default::default()
            },
            &err(format!("connect failed token {secret}")),
        );

        let listed = catalog.list().to_string();
        assert!(listed.contains("amtrak"), "{listed}");
        assert!(listed.contains("\"ready\":false"), "{listed}");
        assert!(listed.contains("config_errors"), "{listed}");
        assert!(listed.contains("[redacted]"), "{listed}");
        assert!(!listed.contains(secret), "{listed}");

        let health = catalog.health(Some("amtrak".into())).await.unwrap();
        let health_text = health.to_string();
        assert_eq!(health["ready"], false);
        assert_eq!(health["headers"][0], "x-api-key");
        assert!(health_text.contains("config_errors"), "{health_text}");
        assert!(!health_text.contains(secret), "{health_text}");

        let query_error = catalog
            .trip_status(Some("amtrak".into()), "177", None, None, 6)
            .await
            .unwrap_err();
        assert!(
            query_error.error.contains("[redacted]"),
            "{}",
            query_error.error
        );
        assert!(!query_error.error.contains(secret), "{}", query_error.error);

        let refreshed = catalog.refresh(Some("amtrak".into())).await.unwrap_err();
        assert!(!refreshed.error.contains(secret), "{}", refreshed.error);
        assert!(catalog.list().to_string().contains("\"ready\":false"));

        let dir = temp_dir("clear-failure");
        let paths = fixtures::materialize(&dir).unwrap();
        catalog
            .register(RegisterRequest {
                feed_id: Some("amtrak".into()),
                feed_set: Some(paths.feed_set.display().to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        let health = catalog.health(Some("amtrak".into())).await.unwrap();
        assert_eq!(health["ready"], true);
        assert!(health.get("error").is_none(), "{health}");
        let _ = std::fs::remove_dir_all(dir);
    }
}
