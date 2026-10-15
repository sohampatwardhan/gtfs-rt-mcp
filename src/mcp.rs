//! MCP tool surface. Responses are compact JSON text so agents spend tokens on facts.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{tool, tool_handler, tool_router, ServerHandler, ServiceExt};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::Value;

use crate::catalog::{Catalog, RegisterRequest};
use crate::error::{err, AppError};
use crate::query::{AlertQuery, TripUpdateQuery, VehicleQuery};

#[derive(Clone)]
pub struct GtfsServer {
    catalog: Arc<Catalog>,
    tool_router: ToolRouter<Self>,
}

impl GtfsServer {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self {
            catalog,
            tool_router: Self::tool_router(),
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
struct RegisterArgs {
    /// Stable id, 1-64 characters [A-Za-z0-9_.:-]. Defaults to "default".
    #[serde(default)]
    feed_id: Option<String>,
    /// URL or path of a feed-set JSON document. Resolved artifact URLs come only from that document.
    #[serde(default)]
    feed_set: Option<String>,
    /// HTTP(S) URL of a GTFS static zip. Mutually exclusive with feed_set and static_path.
    #[serde(default)]
    static_url: Option<String>,
    /// Filesystem path of a GTFS static zip.
    #[serde(default)]
    static_path: Option<String>,
    /// HTTP(S) URL of a GTFS-Realtime TripUpdates protobuf.
    #[serde(default)]
    trip_updates_url: Option<String>,
    /// Filesystem path of a TripUpdates protobuf.
    #[serde(default)]
    trip_updates_path: Option<String>,
    /// HTTP(S) URL of a VehiclePositions protobuf.
    #[serde(default)]
    vehicle_positions_url: Option<String>,
    /// Filesystem path of a VehiclePositions protobuf.
    #[serde(default)]
    vehicle_positions_path: Option<String>,
    /// HTTP(S) URL of an Alerts protobuf.
    #[serde(default)]
    alerts_url: Option<String>,
    /// Filesystem path of an Alerts protobuf.
    #[serde(default)]
    alerts_path: Option<String>,
    /// Realtime cache TTL in seconds. Default 15. Range 1-3600.
    #[serde(default)]
    rt_ttl_secs: Option<u64>,
    /// Static zip refresh interval for direct URLs. 0 disables the timer. Feed-set static files reload only when generation_id changes. Default 21600.
    #[serde(default)]
    static_ttl_secs: Option<u64>,
    /// Extra request headers. Names are reported by feed_health; values are never returned.
    #[serde(default)]
    headers: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct FeedIdArgs {
    /// Feed id. Omit when only one feed is registered.
    #[serde(default)]
    feed_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SearchArgs {
    /// Feed id. Omit when only one feed is registered.
    #[serde(default)]
    feed_id: Option<String>,
    /// stop_id, stop_code, or name substring. Empty lists the first page.
    #[serde(default)]
    query: Option<String>,
    /// Page size. Default 20, max 100.
    #[serde(default)]
    limit: Option<u32>,
    /// Rows to skip. Use next_offset from the previous page.
    #[serde(default)]
    offset: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct TripStatusArgs {
    /// Feed id. Omit when only one feed is registered.
    #[serde(default)]
    feed_id: Option<String>,
    /// trip_id or trip_short_name (train number), for example "177".
    trip: String,
    /// Optional stop_id, stop_code, or stop name. Fills at_stop with delay and track.
    #[serde(default)]
    stop: Option<String>,
    /// Unix seconds. Omit to use the current time.
    #[serde(default)]
    at_unix: Option<i64>,
    /// Upcoming stops to include. Default 4, max 20.
    #[serde(default)]
    upcoming_limit: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ScheduleArgs {
    /// Feed id. Omit when only one feed is registered.
    #[serde(default)]
    feed_id: Option<String>,
    /// trip_id or trip_short_name.
    trip: String,
    /// Unix seconds used to pick the service date and overlay realtime. Omit for now.
    #[serde(default)]
    at_unix: Option<i64>,
    /// Page size. Default 20, max 100.
    #[serde(default)]
    limit: Option<u32>,
    /// Rows to skip.
    #[serde(default)]
    offset: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct BoardArgs {
    /// Feed id. Omit when only one feed is registered.
    #[serde(default)]
    feed_id: Option<String>,
    /// stop_id, stop_code, stop name, or a platform stop such as NHV:track:12.
    stop: String,
    /// Optional route_id or route_short_name filter.
    #[serde(default)]
    route: Option<String>,
    /// Unix seconds. Omit for now.
    #[serde(default)]
    at_unix: Option<i64>,
    /// How far ahead to look, in minutes. Default 120, max 1440.
    #[serde(default)]
    window_minutes: Option<u32>,
    /// How far back to include recent departures, in minutes. Default 5, max 180.
    #[serde(default)]
    past_minutes: Option<u32>,
    /// Page size. Default 20, max 100.
    #[serde(default)]
    limit: Option<u32>,
    /// Rows to skip.
    #[serde(default)]
    offset: Option<u32>,
    /// Include SKIPPED stops. Default false.
    #[serde(default)]
    include_skipped: Option<bool>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct VehicleArgs {
    /// Feed id. Omit when only one feed is registered.
    #[serde(default)]
    feed_id: Option<String>,
    /// route_id or route_short_name.
    #[serde(default)]
    route: Option<String>,
    /// trip_id or trip_short_name.
    #[serde(default)]
    trip: Option<String>,
    /// Vehicle id or label substring.
    #[serde(default)]
    vehicle: Option<String>,
    /// stop_id the vehicle currently reports.
    #[serde(default)]
    stop: Option<String>,
    /// Page size. Default 20, max 100.
    #[serde(default)]
    limit: Option<u32>,
    /// Rows to skip.
    #[serde(default)]
    offset: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct TripUpdateArgs {
    /// Feed id. Omit when only one feed is registered.
    #[serde(default)]
    feed_id: Option<String>,
    /// route_id or route_short_name.
    #[serde(default)]
    route: Option<String>,
    /// trip_id or trip_short_name.
    #[serde(default)]
    trip: Option<String>,
    /// Keep updates that mention this stop, by stop_id or stop_sequence.
    #[serde(default)]
    stop: Option<String>,
    /// Page size of trips. Default 20, max 100.
    #[serde(default)]
    limit: Option<u32>,
    /// Rows to skip.
    #[serde(default)]
    offset: Option<u32>,
    /// StopTimeUpdates to keep per trip. Default 8, max 40.
    #[serde(default)]
    stop_limit: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct AlertArgs {
    /// Feed id. Omit when only one feed is registered.
    #[serde(default)]
    feed_id: Option<String>,
    /// route_id or route_short_name. Matches informed_entity.
    #[serde(default)]
    route: Option<String>,
    /// stop_id, stop_code, or stop name. Matches informed_entity, including parent and platform stops.
    #[serde(default)]
    stop: Option<String>,
    /// Case-insensitive substring of the header or description.
    #[serde(default)]
    text: Option<String>,
    /// Keep alerts whose active or impact period contains at_unix. Default true.
    #[serde(default)]
    active_only: Option<bool>,
    /// Unix seconds. Omit for now.
    #[serde(default)]
    at_unix: Option<i64>,
    /// Page size. Default 20, max 100.
    #[serde(default)]
    limit: Option<u32>,
    /// Rows to skip.
    #[serde(default)]
    offset: Option<u32>,
    /// Max characters of header and description. Default 400, max 2000.
    #[serde(default)]
    max_chars: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct DecodeArgs {
    /// Filesystem path of a GTFS-Realtime protobuf. Pass exactly one of path, url, or base64.
    #[serde(default)]
    path: Option<String>,
    /// HTTP(S) URL of a GTFS-Realtime protobuf.
    #[serde(default)]
    url: Option<String>,
    /// Standard base64 of the protobuf bytes.
    #[serde(default)]
    base64: Option<String>,
    /// Entity offset. Default 0.
    #[serde(default)]
    offset: Option<u32>,
    /// Entities to return. Default 20, max 100.
    #[serde(default)]
    limit: Option<u32>,
    /// Max characters of alert text. Default 800, max 4000.
    #[serde(default)]
    max_chars: Option<u32>,
    /// Extra request headers when url is set. Values are not returned.
    #[serde(default)]
    headers: Option<BTreeMap<String, String>>,
}

#[tool_router]
impl GtfsServer {
    #[tool(
        description = "Register a GTFS source. Pass feed_set, or static_url/static_path plus optional realtime URLs. Replaces an existing feed with the same feed_id. Returns feed health. Header values are stored and not echoed."
    )]
    async fn register_feed(&self, Parameters(args): Parameters<RegisterArgs>) -> Result<String, String> {
        let request = RegisterRequest {
            feed_id: args.feed_id,
            feed_set: args.feed_set,
            static_url: args.static_url,
            static_path: args.static_path,
            trip_updates_url: args.trip_updates_url,
            trip_updates_path: args.trip_updates_path,
            vehicle_positions_url: args.vehicle_positions_url,
            vehicle_positions_path: args.vehicle_positions_path,
            alerts_url: args.alerts_url,
            alerts_path: args.alerts_path,
            rt_ttl_secs: args.rt_ttl_secs,
            static_ttl_secs: args.static_ttl_secs,
            headers: args.headers.unwrap_or_default(),
        };
        finish(self.catalog.register(request).await)
    }

    #[tool(description = "List registered feeds and their freshness. Header values are omitted.")]
    async fn list_feeds(&self) -> Result<String, String> {
        Ok(compact(&self.catalog.list()))
    }

    #[tool(description = "Drop a registered feed and its caches.")]
    async fn unregister_feed(
        &self,
        Parameters(args): Parameters<FeedIdArgs>,
    ) -> Result<String, String> {
        finish(self.catalog.unregister(args.feed_id))
    }

    #[tool(
        description = "Fetch the feed again. A feed-set reloads static.zip only when generation_id or static_version changes. Direct realtime URLs are always refetched."
    )]
    async fn refresh_feed(&self, Parameters(args): Parameters<FeedIdArgs>) -> Result<String, String> {
        finish(self.catalog.refresh(args.feed_id).await)
    }

    #[tool(
        description = "Feed health: entity counts, header timestamps, age, TTL, last error, and the current feed-set generation. Refreshes first when the realtime TTL has elapsed."
    )]
    async fn feed_health(&self, Parameters(args): Parameters<FeedIdArgs>) -> Result<String, String> {
        finish(self.catalog.health(args.feed_id).await)
    }

    #[tool(
        description = "Search stops by stop_id, stop_code, or name. Ranked exact ids and codes first. Returns stop_id, name, coordinates, location_type, parent_station, and platform_code."
    )]
    async fn search_stops(&self, Parameters(args): Parameters<SearchArgs>) -> Result<String, String> {
        finish(
            self.catalog
                .search_stops(
                    args.feed_id,
                    args.query.as_deref().unwrap_or(""),
                    limit_of(args.limit, 20, 100),
                    offset_of(args.offset),
                )
                .await,
        )
    }

    #[tool(
        description = "Search routes by route_id, short name, or long name. Returns route_type and route_type_name."
    )]
    async fn search_routes(&self, Parameters(args): Parameters<SearchArgs>) -> Result<String, String> {
        finish(
            self.catalog
                .search_routes(
                    args.feed_id,
                    args.query.as_deref().unwrap_or(""),
                    limit_of(args.limit, 20, 100),
                    offset_of(args.offset),
                )
                .await,
        )
    }

    #[tool(
        description = "Where a trip is now. Matches trip_id or trip_short_name and prefers the trip that has a live vehicle. Returns delay_label, vehicle position and status, the next stops, and at_stop (scheduled and estimated local HH:MM, delay seconds, assigned_stop_id, platform_code, track) when stop is set."
    )]
    async fn trip_status(
        &self,
        Parameters(args): Parameters<TripStatusArgs>,
    ) -> Result<String, String> {
        if args.trip.trim().is_empty() {
            return finish(Err(err("trip is empty")));
        }
        finish(
            self.catalog
                .trip_status(
                    args.feed_id,
                    &args.trip,
                    args.stop,
                    args.at_unix,
                    limit_of(args.upcoming_limit, 4, 20),
                )
                .await,
        )
    }

    #[tool(
        description = "Stop sequence for one trip, with scheduled and estimated times and any realtime platform assignment. Page with offset."
    )]
    async fn trip_schedule(
        &self,
        Parameters(args): Parameters<ScheduleArgs>,
    ) -> Result<String, String> {
        if args.trip.trim().is_empty() {
            return finish(Err(err("trip is empty")));
        }
        finish(
            self.catalog
                .trip_schedule(
                    args.feed_id,
                    &args.trip,
                    args.at_unix,
                    limit_of(args.limit, 20, 100),
                    offset_of(args.offset),
                )
                .await,
        )
    }

    #[tool(
        description = "Departure board for a station. Combines today's and yesterday's static schedule with realtime delays, cancellations, and assigned tracks. Platform stops expand to the parent station and sibling platforms. Sorted by estimated departure."
    )]
    async fn departure_board(
        &self,
        Parameters(args): Parameters<BoardArgs>,
    ) -> Result<String, String> {
        if args.stop.trim().is_empty() {
            return finish(Err(err("stop is empty")));
        }
        finish(
            self.catalog
                .departure_board(
                    args.feed_id,
                    &args.stop,
                    args.route,
                    args.at_unix,
                    minutes_of(args.window_minutes, 120, 1440),
                    minutes_of(args.past_minutes, 5, 180),
                    limit_of(args.limit, 20, 100),
                    offset_of(args.offset),
                    args.include_skipped.unwrap_or(false),
                )
                .await,
        )
    }

    #[tool(
        description = "Live vehicle positions. Filter by route, trip, vehicle id/label, or current stop. Coordinates are rounded to 5 decimals."
    )]
    async fn vehicle_positions(
        &self,
        Parameters(args): Parameters<VehicleArgs>,
    ) -> Result<String, String> {
        let feed_id = args.feed_id;
        finish(
            self.catalog
                .vehicles(
                    feed_id,
                    VehicleQuery {
                        route: args.route,
                        trip: args.trip,
                        vehicle: args.vehicle,
                        stop: args.stop,
                        limit: limit_of(args.limit, 20, 100),
                        offset: offset_of(args.offset),
                    },
                )
                .await,
        )
    }

    #[tool(
        description = "Trip updates with delay seconds and assigned_stop_id / platform_code. A stop filter matches stop_id or the static stop at that stop_sequence, so updates that omit stop_id still match."
    )]
    async fn trip_updates(
        &self,
        Parameters(args): Parameters<TripUpdateArgs>,
    ) -> Result<String, String> {
        let feed_id = args.feed_id;
        finish(
            self.catalog
                .trip_updates(
                    feed_id,
                    TripUpdateQuery {
                        route: args.route,
                        trip: args.trip,
                        stop: args.stop,
                        limit: limit_of(args.limit, 20, 100),
                        offset: offset_of(args.offset),
                        stop_limit: limit_of(args.stop_limit, 8, 40),
                    },
                )
                .await,
        )
    }

    #[tool(
        description = "Service alerts with cause, effect, severity, active period, informed entities, and clipped header/description text. active_only keeps alerts in effect at at_unix. Route and stop selectors are OR within an alert and AND across the two filters."
    )]
    async fn alerts(&self, Parameters(args): Parameters<AlertArgs>) -> Result<String, String> {
        let feed_id = args.feed_id;
        let at_unix = args.at_unix.unwrap_or_else(now_unix);
        finish(
            self.catalog
                .alerts(
                    feed_id,
                    AlertQuery {
                        route: args.route,
                        stop: args.stop,
                        text: args.text,
                        active_only: args.active_only.unwrap_or(true),
                        at_unix,
                        limit: limit_of(args.limit, 20, 100),
                        offset: offset_of(args.offset),
                        max_chars: limit_of(args.max_chars, 400, 2000),
                    },
                )
                .await,
        )
    }

    #[tool(
        description = "Decode one GTFS-Realtime FeedMessage from a path, http(s) URL, or base64 string into compact JSON. Does not register a feed. Use this when a protobuf looks wrong."
    )]
    async fn decode_protobuf(
        &self,
        Parameters(args): Parameters<DecodeArgs>,
    ) -> Result<String, String> {
        finish(
            self.catalog
                .decode(
                    args.path,
                    args.url,
                    args.base64,
                    offset_of(args.offset),
                    limit_of(args.limit, 20, 100),
                    limit_of(args.max_chars, 800, 4000),
                    args.headers.unwrap_or_default(),
                )
                .await,
        )
    }
}

#[tool_handler(
    router = self.tool_router,
    name = "gtfs-rt-mcp",
    instructions = "Explore GTFS static schedules and GTFS-Realtime for any agency. register_feed accepts a feed-set URL (a /v1/feed-set.json document whose urls point at an immutable generation: static.zip, trip-updates.pb, vehicle-positions.pb, alerts.pb) or a static GTFS zip plus realtime protobuf URLs. Then answer with trip_status (trip id or trip_short_name / train number), departure_board, trip_schedule, vehicle_positions, trip_updates, and alerts. Delays are seconds and positive means late. Track or platform assignments are assigned_stop_id and platform_code. Pass at_unix to pin the clock. Responses are compact JSON. Page with offset until next_offset is null. feed_health reports freshness. decode_protobuf turns a GTFS-Realtime message into JSON without registering a feed. HTTP header values given at registration are kept for fetches and are never echoed back."
)]
impl ServerHandler for GtfsServer {}

pub async fn serve_stdio(server: GtfsServer) -> Result<(), AppError> {
    let running = server
        .serve(rmcp::transport::stdio())
        .await
        .map_err(|e| err(format!("stdio: {e}")))?;
    running
        .waiting()
        .await
        .map_err(|e| err(format!("stdio: {e}")))?;
    Ok(())
}

pub async fn serve_http(
    catalog: Arc<Catalog>,
    bind: SocketAddr,
    allowed_hosts: Vec<String>,
) -> Result<(), AppError> {
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|e| err(format!("bind {bind}: {e}")))?;
    let addr = listener
        .local_addr()
        .map_err(|e| err(format!("local address: {e}")))?;
    tracing::info!("gtfs-rt-mcp listening on http://{addr}/mcp");
    let router = http_router(catalog, allowed_hosts);
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|e| err(format!("http: {e}")))?;
    Ok(())
}

/// Bind an HTTP server on a background task. Used by tests and embedders.
pub async fn spawn_http(
    catalog: Arc<Catalog>,
    bind: SocketAddr,
    allowed_hosts: Vec<String>,
) -> Result<(SocketAddr, tokio::task::JoinHandle<()>), AppError> {
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|e| err(format!("bind {bind}: {e}")))?;
    let addr = listener
        .local_addr()
        .map_err(|e| err(format!("local address: {e}")))?;
    let router = http_router(catalog, allowed_hosts);
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    Ok((addr, handle))
}

fn http_router(catalog: Arc<Catalog>, allowed_hosts: Vec<String>) -> axum::Router {
    let mut config = StreamableHttpServerConfig::default()
        .with_legacy_session_mode(false)
        .with_json_response(true);
    if !allowed_hosts.is_empty() {
        config = config.with_allowed_hosts(allowed_hosts);
    }
    let service = StreamableHttpService::new(
        {
            let catalog = Arc::clone(&catalog);
            move || Ok(GtfsServer::new(Arc::clone(&catalog)))
        },
        Arc::new(LocalSessionManager::default()),
        config,
    );
    axum::Router::new()
        .route(
            "/healthz",
            axum::routing::get(|| async { "ok" }),
        )
        .nest_service("/mcp", service)
}

fn finish(result: Result<Value, AppError>) -> Result<String, String> {
    match result {
        Ok(value) => Ok(compact(&value)),
        Err(error) => Err(compact(&error.to_json())),
    }
}

fn compact(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "{\"error\":\"failed to serialize\"}".into())
}

fn limit_of(value: Option<u32>, default: u32, max: u32) -> usize {
    value.unwrap_or(default).clamp(1, max) as usize
}

fn minutes_of(value: Option<u32>, default: u32, max: u32) -> u64 {
    u64::from(value.unwrap_or(default).min(max))
}

fn offset_of(value: Option<u32>) -> usize {
    value.unwrap_or(0) as usize
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
