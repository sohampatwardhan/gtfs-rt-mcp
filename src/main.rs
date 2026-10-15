use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use gtfs_rt_mcp::{serve_http, serve_stdio, Catalog, GtfsServer, RegisterRequest};

#[derive(Parser)]
#[command(
    name = "gtfs-rt-mcp",
    version,
    about = "MCP server for GTFS static schedules and GTFS-Realtime"
)]
struct Cli {
    /// Feed-set JSON URL or path. Artifact URLs are taken from the document.
    #[arg(long, global = true, env = "GTFS_RT_MCP_FEED_SET")]
    feed_set: Option<String>,
    /// Id for the preloaded feed. Default "default".
    #[arg(long, global = true, env = "GTFS_RT_MCP_FEED_ID")]
    feed_id: Option<String>,
    /// GTFS static zip URL or filesystem path.
    #[arg(long, global = true, env = "GTFS_RT_MCP_STATIC")]
    r#static: Option<String>,
    /// TripUpdates protobuf URL or path.
    #[arg(long, global = true, env = "GTFS_RT_MCP_TRIP_UPDATES")]
    trip_updates: Option<String>,
    /// VehiclePositions protobuf URL or path.
    #[arg(long, global = true, env = "GTFS_RT_MCP_VEHICLE_POSITIONS")]
    vehicle_positions: Option<String>,
    /// Alerts protobuf URL or path.
    #[arg(long, global = true, env = "GTFS_RT_MCP_ALERTS")]
    alerts: Option<String>,
    /// Realtime TTL in seconds. Default 15.
    #[arg(long, global = true, env = "GTFS_RT_MCP_RT_TTL")]
    rt_ttl: Option<u64>,
    /// JSON object of HTTP headers. Values are never logged.
    #[arg(long, global = true, env = "GTFS_RT_MCP_HTTP_HEADERS")]
    http_headers: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Speak MCP over stdin and stdout. Logs go to stderr.
    Stdio,
    /// Speak streamable HTTP MCP.
    Http {
        /// Bind address. Default 127.0.0.1:8091.
        #[arg(long, env = "GTFS_RT_MCP_BIND", default_value = "127.0.0.1:8091")]
        bind: SocketAddr,
        /// Host header to allow, in addition to localhost, 127.0.0.1, and ::1.
        #[arg(long = "allowed-host", env = "GTFS_RT_MCP_ALLOWED_HOST")]
        allowed_hosts: Vec<String>,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    init_tracing(matches!(cli.command, Command::Http { .. }));
    let catalog = match Catalog::new() {
        Ok(catalog) => Arc::new(catalog),
        Err(error) => {
            eprintln!("gtfs-rt-mcp: {error}");
            std::process::exit(1);
        }
    };
    if let Err(error) = preload(&catalog, &cli).await {
        eprintln!("gtfs-rt-mcp: {error}");
        std::process::exit(1);
    }
    let result = match cli.command {
        Command::Stdio => serve_stdio(GtfsServer::new(catalog)).await,
        Command::Http {
            bind,
            allowed_hosts,
        } => serve_http(catalog, bind, allowed_hosts).await,
    };
    if let Err(error) = result {
        eprintln!("gtfs-rt-mcp: {error}");
        std::process::exit(1);
    }
}

fn init_tracing(http: bool) {
    let fallback = if http { "info" } else { "warn" };
    let filter = std::env::var("GTFS_RT_MCP_LOG")
        .or_else(|_| std::env::var("RUST_LOG"))
        .unwrap_or_else(|_| fallback.to_string());
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

async fn preload(catalog: &Catalog, cli: &Cli) -> Result<(), gtfs_rt_mcp::AppError> {
    let headers = parse_headers(cli.http_headers.as_deref())?;
    let header_count = headers.len();
    let has_source = cli.feed_set.is_some()
        || cli.r#static.is_some()
        || cli.trip_updates.is_some()
        || cli.vehicle_positions.is_some()
        || cli.alerts.is_some();
    if !has_source {
        if header_count > 0 {
            tracing::warn!("GTFS_RT_MCP_HTTP_HEADERS is set but no feed was preloaded");
        }
        return Ok(());
    }
    let (static_url, static_path) = split_loc(cli.r#static.clone());
    let (trip_updates_url, trip_updates_path) = split_loc(cli.trip_updates.clone());
    let (vehicle_positions_url, vehicle_positions_path) = split_loc(cli.vehicle_positions.clone());
    let (alerts_url, alerts_path) = split_loc(cli.alerts.clone());
    tracing::info!(
        feed_id = cli.feed_id.as_deref().unwrap_or("default"),
        header_count,
        "preloading feed"
    );
    catalog
        .register(RegisterRequest {
            feed_id: cli.feed_id.clone(),
            feed_set: cli.feed_set.clone(),
            static_url,
            static_path,
            trip_updates_url,
            trip_updates_path,
            vehicle_positions_url,
            vehicle_positions_path,
            alerts_url,
            alerts_path,
            rt_ttl_secs: cli.rt_ttl,
            static_ttl_secs: None,
            headers,
        })
        .await?;
    Ok(())
}

fn parse_headers(raw: Option<&str>) -> Result<BTreeMap<String, String>, gtfs_rt_mcp::AppError> {
    let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(BTreeMap::new());
    };
    serde_json::from_str(raw).map_err(|e| {
        gtfs_rt_mcp::AppError::new(format!("GTFS_RT_MCP_HTTP_HEADERS is not a JSON object: {e}"))
    })
}

fn split_loc(value: Option<String>) -> (Option<String>, Option<String>) {
    match value {
        Some(value) if value.starts_with("http://") || value.starts_with("https://") => {
            (Some(value), None)
        }
        Some(value) => (None, Some(value)),
        None => (None, None),
    }
}
