//! Tiny Harbor Line feed used by tests and local demos. No network.

use std::fs;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};

use prost::Message;
use zip::write::SimpleFileOptions;
use zip::CompressionMethod;
use zip::ZipWriter;

use crate::gtfs_rt::alert::{Cause, Effect, SeverityLevel};
use crate::gtfs_rt::feed_header::Incrementality;
use crate::gtfs_rt::trip_update::stop_time_update::ScheduleRelationship;
use crate::gtfs_rt::vehicle_position::VehicleStopStatus;
use crate::gtfs_rt::{
    EntitySelector, FeedEntity, FeedHeader, FeedMessage, Position, TimeRange, TranslatedString,
    TripDescriptor, TripUpdate, VehicleDescriptor, VehiclePosition,
};
use crate::gtfs_rt::translated_string::Translation;
use crate::gtfs_rt::trip_update::{StopTimeEvent, StopTimeUpdate};

/// 2026-01-15 16:30:00 UTC, which is 11:30 in America/New_York.
pub fn reference_unix() -> i64 {
    chrono::TimeZone::with_ymd_and_hms(&chrono::Utc, 2026, 1, 15, 16, 30, 0)
        .unwrap()
        .timestamp()
}

#[derive(Debug)]
pub struct HarborPaths {
    pub dir: PathBuf,
    pub feed_set: PathBuf,
    pub static_zip: PathBuf,
    pub trip_updates: PathBuf,
    pub vehicle_positions: PathBuf,
    pub alerts: PathBuf,
    pub at_unix: i64,
}

pub fn materialize(dir: &Path) -> std::io::Result<HarborPaths> {
    fs::create_dir_all(dir)?;
    let static_zip = dir.join("static.zip");
    write_static_zip(&static_zip)?;
    let at = reference_unix();
    let trip_updates = dir.join("trip-updates.pb");
    let vehicle_positions = dir.join("vehicle-positions.pb");
    let alerts = dir.join("alerts.pb");
    fs::write(&trip_updates, trip_updates_bytes(at))?;
    fs::write(&vehicle_positions, vehicle_bytes(at))?;
    fs::write(&alerts, alerts_bytes(at))?;
    let feed_set = dir.join("feed-set.json");
    let manifest = serde_json::json!({
        "generation_id": "gen-1",
        "generated_at_unix": at,
        "static_version": "harbor-1",
        "source": "fixture",
        "entity_counts": {
            "trip_updates": 1,
            "vehicle_positions": 1,
            "alerts": 2
        },
        "urls": {
            "static_zip": "static.zip",
            "trip_updates": "trip-updates.pb",
            "vehicle_positions": "vehicle-positions.pb",
            "alerts": "alerts.pb"
        }
    });
    fs::write(&feed_set, serde_json::to_vec_pretty(&manifest).unwrap())?;
    Ok(HarborPaths {
        dir: dir.to_path_buf(),
        feed_set,
        static_zip,
        trip_updates,
        vehicle_positions,
        alerts,
        at_unix: at,
    })
}

fn write_static_zip(path: &Path) -> std::io::Result<()> {
    let file = fs::File::create(path)?;
    let mut zip = ZipWriter::new(file);
    let opts = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
    for (name, body) in [
        ("agency.txt", include_str!("../tests/fixtures/harbor/agency.txt")),
        ("stops.txt", include_str!("../tests/fixtures/harbor/stops.txt")),
        ("routes.txt", include_str!("../tests/fixtures/harbor/routes.txt")),
        ("trips.txt", include_str!("../tests/fixtures/harbor/trips.txt")),
        ("stop_times.txt", include_str!("../tests/fixtures/harbor/stop_times.txt")),
        ("calendar.txt", include_str!("../tests/fixtures/harbor/calendar.txt")),
        ("calendar_dates.txt", include_str!("../tests/fixtures/harbor/calendar_dates.txt")),
        ("feed_info.txt", include_str!("../tests/fixtures/harbor/feed_info.txt")),
    ] {
        zip.start_file(name, opts)?;
        zip.write_all(body.as_bytes())?;
    }
    zip.finish()?.flush()?;
    Ok(())
}

fn header(at: i64) -> FeedHeader {
    FeedHeader {
        gtfs_realtime_version: "2.0".into(),
        incrementality: Some(Incrementality::FullDataset as i32),
        timestamp: Some(at as u64),
        feed_version: Some("harbor-1".into()),
    }
}

fn trip_updates_bytes(at: i64) -> Vec<u8> {
    let update = TripUpdate {
        trip: TripDescriptor {
            trip_id: Some("177-BOS".into()),
            route_id: Some("NEC".into()),
            ..Default::default()
        },
        vehicle: Some(VehicleDescriptor {
            id: Some("AMT177".into()),
            label: Some("177".into()),
            ..Default::default()
        }),
        stop_time_update: vec![
            StopTimeUpdate {
                stop_sequence: Some(2),
                arrival: Some(StopTimeEvent {
                    delay: Some(300),
                    ..Default::default()
                }),
                departure: Some(StopTimeEvent {
                    delay: Some(300),
                    ..Default::default()
                }),
                schedule_relationship: Some(ScheduleRelationship::Scheduled as i32),
                ..Default::default()
            },
            StopTimeUpdate {
                stop_sequence: Some(3),
                arrival: Some(StopTimeEvent {
                    delay: Some(420),
                    ..Default::default()
                }),
                departure: Some(StopTimeEvent {
                    delay: Some(480),
                    ..Default::default()
                }),
                schedule_relationship: Some(ScheduleRelationship::Scheduled as i32),
                stop_time_properties: Some(crate::gtfs_rt::trip_update::stop_time_update::StopTimeProperties {
                    assigned_stop_id: Some("NHV:track:12".into()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ],
        timestamp: Some(at as u64),
        ..Default::default()
    };
    FeedMessage {
        header: header(at),
        entity: vec![FeedEntity {
            id: "177-BOS".into(),
            trip_update: Some(update),
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

fn vehicle_bytes(at: i64) -> Vec<u8> {
    let vehicle = VehiclePosition {
        trip: Some(TripDescriptor {
            trip_id: Some("177-BOS".into()),
            route_id: Some("NEC".into()),
            ..Default::default()
        }),
        vehicle: Some(VehicleDescriptor {
            id: Some("AMT177".into()),
            label: Some("177".into()),
            ..Default::default()
        }),
        position: Some(Position {
            latitude: 41.25,
            longitude: -72.95,
            bearing: Some(40.0),
            speed: Some(22.5),
            ..Default::default()
        }),
        current_stop_sequence: Some(3),
        stop_id: Some("NHV".into()),
        current_status: Some(VehicleStopStatus::InTransitTo as i32),
        timestamp: Some(at as u64),
        ..Default::default()
    };
    FeedMessage {
        header: header(at),
        entity: vec![FeedEntity {
            id: "AMT177".into(),
            vehicle: Some(vehicle),
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

#[allow(deprecated)]
fn alerts_bytes(at: i64) -> Vec<u8> {
    let window = TimeRange {
        start: Some((at - 3600) as u64),
        end: Some((at + 86_400) as u64),
    };
    let nec = FeedEntity {
        id: "alert-nec".into(),
        alert: Some(crate::gtfs_rt::Alert {
            active_period: vec![window.clone()],
            informed_entity: vec![
                EntitySelector {
                    route_id: Some("NEC".into()),
                    ..Default::default()
                },
                EntitySelector {
                    stop_id: Some("NYP".into()),
                    ..Default::default()
                },
            ],
            cause: Some(Cause::Construction as i32),
            effect: Some(Effect::SignificantDelays as i32),
            header_text: Some(text("Track work between NYP and NHV")),
            description_text: Some(text(
                "Northeast Corridor trains may be delayed or reassigned to a different track at New Haven.",
            )),
            url: Some(text("https://example.test/nec")),
            severity_level: Some(SeverityLevel::Warning as i32),
            ..Default::default()
        }),
        ..Default::default()
    };
    let local = FeedEntity {
        id: "alert-local".into(),
        alert: Some(crate::gtfs_rt::Alert {
            active_period: vec![window],
            informed_entity: vec![EntitySelector {
                route_id: Some("LOCAL".into()),
                ..Default::default()
            }],
            cause: Some(Cause::OtherCause as i32),
            effect: Some(Effect::Detour as i32),
            header_text: Some(text("Local bus detour")),
            description_text: Some(text("Harbor Local is detouring off Chapel Street.")),
            severity_level: Some(SeverityLevel::Info as i32),
            ..Default::default()
        }),
        ..Default::default()
    };
    FeedMessage {
        header: header(at),
        entity: vec![nec, local],
    }
    .encode_to_vec()
}

fn text(value: &str) -> TranslatedString {
    TranslatedString {
        translation: vec![Translation {
            text: value.into(),
            language: Some("en".into()),
        }],
    }
}

/// Silence unused import if Cursor is unused. Kept so callers can wrap bytes.
#[allow(dead_code)]
pub fn static_zip_bytes() -> Vec<u8> {
    let mut cursor = Cursor::new(Vec::new());
    {
        let mut zip = ZipWriter::new(&mut cursor);
        let opts = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
        for (name, body) in [
            ("agency.txt", include_str!("../tests/fixtures/harbor/agency.txt")),
            ("stops.txt", include_str!("../tests/fixtures/harbor/stops.txt")),
            ("routes.txt", include_str!("../tests/fixtures/harbor/routes.txt")),
            ("trips.txt", include_str!("../tests/fixtures/harbor/trips.txt")),
            ("stop_times.txt", include_str!("../tests/fixtures/harbor/stop_times.txt")),
            ("calendar.txt", include_str!("../tests/fixtures/harbor/calendar.txt")),
            ("feed_info.txt", include_str!("../tests/fixtures/harbor/feed_info.txt")),
        ] {
            zip.start_file(name, opts).unwrap();
            zip.write_all(body.as_bytes()).unwrap();
        }
        zip.finish().unwrap();
    }
    cursor.into_inner()
}
