//! GTFS static and GTFS-Realtime explorer for MCP clients.

mod catalog;
mod error;
pub mod fixtures;
mod gtfs;
mod mcp;
mod query;
mod realtime;
mod source;

pub mod gtfs_rt {
    include!(concat!(env!("OUT_DIR"), "/transit_realtime.rs"));
}

pub use catalog::{Catalog, RegisterRequest};
pub use error::AppError;
pub use query::{AlertQuery, TripUpdateQuery, VehicleQuery};
pub use mcp::{serve_http, serve_stdio, spawn_http, GtfsServer};
