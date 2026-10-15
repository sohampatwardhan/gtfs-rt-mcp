//! Opt-in check against the public MBTA feed. `cargo test --test live_mbta -- --ignored --nocapture`

use std::sync::Arc;

use serde_json::Value;

#[tokio::test]
#[ignore]
async fn mbta_public_feed() {
    let catalog = Arc::new(gtfs_rt_mcp::Catalog::new().unwrap());
    let health = catalog
        .register(gtfs_rt_mcp::RegisterRequest {
            feed_id: Some("mbta".into()),
            static_url: Some("https://cdn.mbta.com/MBTA_GTFS.zip".into()),
            trip_updates_url: Some("https://cdn.mbta.com/realtime/TripUpdates.pb".into()),
            vehicle_positions_url: Some(
                "https://cdn.mbta.com/realtime/VehiclePositions.pb".into(),
            ),
            alerts_url: Some("https://cdn.mbta.com/realtime/Alerts.pb".into()),
            rt_ttl_secs: Some(30),
            ..Default::default()
        })
        .await
        .expect("register MBTA");
    assert!(health["static"]["stops"].as_i64().unwrap_or(0) > 100);
    assert!(health["realtime"]["vehicle_positions"]["entities"].as_i64().unwrap_or(0) > 0);
    let agency = health["static"]["agency"].as_str().unwrap_or("");
    assert!(
        agency.contains("MBTA") || agency.contains("Massachusetts Bay"),
        "dominant agency was {agency}"
    );

    let stops = catalog
        .search_stops(Some("mbta".into()), "South Station", 5, 0)
        .await
        .unwrap();
    let stop_id = stops["stops"][0]["stop_id"].as_str().unwrap().to_string();
    let board = catalog
        .departure_board(
            Some("mbta".into()),
            &stop_id,
            None,
            None,
            90,
            5,
            5,
            0,
            false,
        )
        .await
        .unwrap();
    let vehicles = catalog
        .vehicles(
            Some("mbta".into()),
            gtfs_rt_mcp::VehicleQuery {
                route: None,
                trip: None,
                vehicle: None,
                stop: None,
                limit: 3,
                offset: 0,
            },
        )
        .await
        .unwrap();
    let alerts = catalog
        .alerts(
            Some("mbta".into()),
            gtfs_rt_mcp::AlertQuery {
                route: None,
                stop: None,
                text: None,
                active_only: true,
                at_unix: chrono_now(),
                limit: 3,
                offset: 0,
                max_chars: 180,
            },
        )
        .await
        .unwrap();

    println!("HEALTH {}", compact(&health));
    println!("STOPS {}", compact(&stops));
    println!("BOARD {}", compact(&board));
    println!("VEHICLES {}", compact(&vehicles));
    println!("ALERTS {}", compact(&alerts));
    assert!(board["departures"].as_array().is_some());
    assert!(board["expanded_stops"].as_u64().unwrap_or(999) < 80);
    assert!(alerts["alerts"].as_array().is_some());
    assert!(vehicles["vehicles"].as_array().unwrap().len() > 0);
}

fn compact(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

fn chrono_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
