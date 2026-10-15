use std::collections::{HashMap, HashSet};

use chrono::{DateTime, NaiveDate, TimeZone, Timelike};
use chrono_tz::Tz;
use serde_json::{json, Value};

use crate::error::{err, AppError};
use crate::gtfs::{route_type_name, StaticDb, StopRow, StopTimeRow, TripRow};
use crate::gtfs_rt::trip_update::stop_time_update::ScheduleRelationship as StopRel;
use crate::gtfs_rt::{FeedEntity, FeedMessage, TripUpdate, VehiclePosition};
use crate::realtime::{
    alert_periods, cause_name, delay_label, effect_name, find_stop_update, incrementality,
    is_canceled, json_i32, json_i64, json_opt, obj, occupancy_name, period_contains, predict,
    propagated_delay, round_coord, severity_name, stop_relationship, translated, trip_relationship,
    vehicle_json, vehicle_status,
};

#[derive(Clone)]
pub struct RtView<'a> {
    pub trip_updates: Option<&'a FeedMessage>,
    pub vehicles: Option<&'a FeedMessage>,
    pub alerts: Option<&'a FeedMessage>,
}

pub struct TripStatusQuery {
    pub trip: String,
    pub stop: Option<String>,
    pub at_unix: i64,
    pub upcoming_limit: usize,
}

pub struct BoardQuery {
    pub stop: String,
    pub route: Option<String>,
    pub at_unix: i64,
    pub window_minutes: u64,
    pub past_minutes: u64,
    pub limit: usize,
    pub offset: usize,
    pub include_skipped: bool,
}

pub struct ScheduleQuery {
    pub trip: String,
    pub at_unix: i64,
    pub limit: usize,
    pub offset: usize,
}

pub struct VehicleQuery {
    pub route: Option<String>,
    pub trip: Option<String>,
    pub vehicle: Option<String>,
    pub stop: Option<String>,
    pub limit: usize,
    pub offset: usize,
}

pub struct TripUpdateQuery {
    pub route: Option<String>,
    pub trip: Option<String>,
    pub stop: Option<String>,
    pub limit: usize,
    pub offset: usize,
    pub stop_limit: usize,
}

pub struct AlertQuery {
    pub route: Option<String>,
    pub stop: Option<String>,
    pub text: Option<String>,
    pub active_only: bool,
    pub at_unix: i64,
    pub limit: usize,
    pub offset: usize,
    pub max_chars: usize,
}

pub fn trip_status(
    db: &StaticDb,
    rt: RtView<'_>,
    query: &TripStatusQuery,
) -> Result<Value, AppError> {
    let trips = resolve_trips(db, &query.trip)?;
    if trips.is_empty() {
        return Err(err(format!("no trip matches '{}'", query.trip))
            .hint("pass a trip_id or trip_short_name (train number)"));
    }
    let vehicles = index_vehicles(rt.vehicles);
    let updates = index_updates(rt.trip_updates);
    let mut ranked = Vec::new();
    for trip in &trips {
        let dates = service_dates(db, &trip.service_id, query.at_unix);
        let dates = if dates.is_empty() {
            vec![local_date(query.at_unix, db.tz)]
        } else {
            dates
        };
        for date in dates {
            let stops = db.trip_stop_times(&trip.trip_id, 400, 0)?;
            let vehicle = vehicles.get(trip.trip_id.as_str()).copied();
            let update = updates.get(trip.trip_id.as_str()).copied();
            let score = score_candidate(query.at_unix, db.tz, date, &stops, vehicle, update);
            ranked.push((score, date, trip, stops));
        }
    }
    ranked.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.2.trip_id.cmp(&b.2.trip_id)));
    let best_score = ranked.first().map(|row| row.0).unwrap_or(0);
    let mut chosen: Vec<_> = ranked
        .iter()
        .filter(|row| row.0 + 25 >= best_score && vehicles.contains_key(row.2.trip_id.as_str()))
        .take(2)
        .collect();
    if chosen.is_empty() {
        chosen.extend(ranked.iter().take(1));
    }
    let at_stop = match &query.stop {
        Some(stop) => Some(db.resolve_stop(stop)?),
        None => None,
    };
    let mut full = Vec::new();
    let mut seen = HashSet::new();
    for (_, date, trip, stops) in &chosen {
        seen.insert(trip.trip_id.clone());
        full.push(render_trip(
            db,
            query,
            trip,
            *date,
            stops,
            vehicles.get(trip.trip_id.as_str()).copied(),
            updates.get(trip.trip_id.as_str()).copied(),
            at_stop.as_ref(),
        )?);
    }
    let alternatives: Vec<Value> = ranked
        .iter()
        .filter(|(_, _, trip, _)| !seen.contains(&trip.trip_id))
        .take(5)
        .map(|(_, date, trip, _)| {
            obj([
                ("trip_id", json!(trip.trip_id)),
                ("trip_short_name", json_opt(trip.short_name.clone())),
                ("headsign", json_opt(trip.headsign.clone())),
                ("route_short_name", json_opt(trip.route_short_name.clone())),
                ("service_date", json!(date.format("%Y%m%d").to_string())),
                (
                    "live",
                    json!(vehicles.contains_key(trip.trip_id.as_str())),
                ),
            ])
        })
        .collect();
    Ok(obj([
        (
            "matched",
            json!(ranked.len()),
        ),
        ("trips", Value::Array(full)),
        (
            "alternatives",
            if alternatives.is_empty() {
                Value::Null
            } else {
                Value::Array(alternatives)
            },
        ),
    ]))
}

pub fn trip_schedule(
    db: &StaticDb,
    rt: RtView<'_>,
    query: &ScheduleQuery,
) -> Result<Value, AppError> {
    let trips = resolve_trips(db, &query.trip)?;
    let Some(trip) = trips.first() else {
        return Err(err(format!("no trip matches '{}'", query.trip)));
    };
    if trips.len() > 1 && db.trip_by_id(&query.trip)?.is_none() {
        let ids: Vec<_> = trips.iter().take(8).map(|t| json!(t.trip_id)).collect();
        return Err(err(format!(
            "'{}' matches {} trips; pass a trip_id",
            query.trip,
            trips.len()
        ))
        .extra("candidates", Value::Array(ids)));
    }
    let total = db.trip_stop_count(&trip.trip_id)? as usize;
    let rows = db.trip_stop_times(&trip.trip_id, query.limit, query.offset)?;
    let date = service_dates(db, &trip.service_id, query.at_unix)
        .into_iter()
        .next_back()
        .unwrap_or_else(|| local_date(query.at_unix, db.tz));
    let update = index_updates(rt.trip_updates)
        .get(trip.trip_id.as_str())
        .copied()
        .cloned();
    let stops: Vec<Value> = rows
        .iter()
        .map(|row| stop_json(db, &update, trip_rel_of(update.as_ref()), row, date, db.tz))
        .collect();
    let next = query.offset + stops.len();
    Ok(obj([
        ("trip_id", json!(trip.trip_id)),
        ("trip_short_name", json_opt(trip.short_name.clone())),
        ("headsign", json_opt(trip.headsign.clone())),
        ("route_id", json!(trip.route_id)),
        ("route_short_name", json_opt(trip.route_short_name.clone())),
        ("route_long_name", json_opt(trip.route_long_name.clone())),
        ("service_date", json!(date.format("%Y%m%d").to_string())),
        (
            "service_active",
            json!(db.service_active(&trip.service_id, date)),
        ),
        ("timezone", json!(db.tz_name)),
        ("stop_count", json!(total)),
        ("offset", json!(query.offset)),
        ("returned", json!(stops.len())),
        (
            "next_offset",
            if next < total {
                json!(next)
            } else {
                Value::Null
            },
        ),
        ("stops", Value::Array(stops)),
    ]))
}

pub fn departure_board(
    db: &StaticDb,
    rt: RtView<'_>,
    query: &BoardQuery,
) -> Result<Value, AppError> {
    let stop = db.resolve_stop(&query.stop)?;
    let stop_ids = db.expand_stop_ids(&stop.stop_id)?;
    let route_ids = match &query.route {
        Some(route) => {
            let ids = db.resolve_route_ids(route)?;
            if ids.is_empty() {
                vec![route.clone()]
            } else {
                ids
            }
        }
        None => Vec::new(),
    };
    let updates = index_updates(rt.trip_updates);
    let today = local_date(query.at_unix, db.tz);
    let mut dates = Vec::new();
    if let Some(yesterday) = today.pred_opt() {
        dates.push(yesterday);
    }
    dates.push(today);
    let rows = db.stop_times_for_stops(&stop_ids)?;
    let start = query.at_unix - (query.past_minutes as i64) * 60;
    let end = query.at_unix + (query.window_minutes as i64) * 60;
    let mut departures = Vec::new();
    for date in dates {
        for row in &rows {
            if !db.service_active(&row.service_id, date) {
                continue;
            }
            if !route_ids.is_empty()
                && !route_ids.iter().any(|id| id == &row.route_id)
                && !route_ids.iter().any(|id| {
                    row.route_short_name
                        .as_deref()
                        .is_some_and(|name| name.eq_ignore_ascii_case(id))
                })
            {
                continue;
            }
            let event_sec = row.departure_sec.or(row.arrival_sec);
            let Some(event_sec) = event_sec else {
                continue;
            };
            let scheduled = gtfs_unix(date, event_sec, db.tz);
            if scheduled < start || scheduled > end {
                continue;
            }
            let update = updates.get(row.trip_id.as_str()).copied();
            let rendered = stop_json(
                db,
                &update.cloned(),
                trip_rel_of(update),
                row,
                date,
                db.tz,
            );
            if !query.include_skipped && rendered.get("skipped").and_then(|v| v.as_bool()) == Some(true)
            {
                continue;
            }
            departures.push((scheduled, rendered));
        }
    }
    departures.sort_by(|a, b| {
        let ae = a.1.get("estimated_departure").and_then(|v| v.as_i64()).unwrap_or(a.0);
        let be = b.1.get("estimated_departure").and_then(|v| v.as_i64()).unwrap_or(b.0);
        ae.cmp(&be).then(a.0.cmp(&b.0))
    });
    let total = departures.len();
    let offset = query.offset.min(total);
    let end_index = (offset + query.limit).min(total);
    let page: Vec<_> = departures[offset..end_index]
        .iter()
        .map(|(_, row)| row.clone())
        .collect();
    Ok(obj([
        ("stop_id", json!(stop.stop_id)),
        ("stop_name", json_opt(stop.stop_name.clone())),
        ("timezone", json!(db.tz_name)),
        ("at_unix", json!(query.at_unix)),
        ("at_local", json!(hhmm(query.at_unix, db.tz))),
        ("window_minutes", json!(query.window_minutes)),
        ("past_minutes", json!(query.past_minutes)),
        ("expanded_stops", json!(stop_ids.len())),
        ("total", json!(total)),
        ("offset", json!(offset)),
        ("returned", json!(page.len())),
        (
            "next_offset",
            if end_index < total {
                json!(end_index)
            } else {
                Value::Null
            },
        ),
        ("departures", Value::Array(page)),
    ]))
}

pub fn vehicle_positions(
    db: &StaticDb,
    rt: RtView<'_>,
    query: &VehicleQuery,
) -> Result<Value, AppError> {
    let Some(feed) = rt.vehicles else {
        return Err(err("this feed has no vehicle positions"));
    };
    let trip_filter = trip_filter(db, query.trip.as_deref())?;
    let route_ids = route_filter(db, query.route.as_deref())?;
    let stop_ids: Option<HashSet<String>> = match &query.stop {
        Some(stop) => Some(HashSet::from_iter(db.expand_stop_ids(&db.resolve_stop(stop)?.stop_id)?)),
        None => None,
    };
    let mut matched: Vec<&FeedEntity> = feed
        .entity
        .iter()
        .filter(|entity| entity.vehicle.is_some())
        .filter(|entity| {
            let vehicle = entity.vehicle.as_ref().unwrap();
            if let Some(wanted) = &query.vehicle {
                let id = vehicle.vehicle.as_ref().and_then(|v| v.id.as_deref());
                let label = vehicle.vehicle.as_ref().and_then(|v| v.label.as_deref());
                if id != Some(wanted.as_str())
                    && label.map(|l| l.eq_ignore_ascii_case(wanted)) != Some(true)
                {
                    return false;
                }
            }
            let trip_id = vehicle.trip.as_ref().and_then(|t| t.trip_id.as_deref());
            if let Some(filter) = &trip_filter {
                let label = vehicle.vehicle.as_ref().and_then(|v| v.label.as_deref());
                let id_ok = trip_id.is_some_and(|id| filter.contains(id));
                let label_ok = label.is_some_and(|label| {
                    filter.iter().any(|id| id.eq_ignore_ascii_case(label))
                        || query
                            .trip
                            .as_ref()
                            .is_some_and(|q| label.eq_ignore_ascii_case(q))
                });
                if !id_ok && !label_ok {
                    return false;
                }
            }
            if let Some(routes) = &route_ids {
                let route = vehicle.trip.as_ref().and_then(|t| t.route_id.as_deref());
                let static_route = trip_id.and_then(|id| db.trip_route_id(id).ok().flatten());
                if !route.is_some_and(|id| routes.iter().any(|r| r == id))
                    && !static_route.is_some_and(|id| routes.iter().any(|r| r == &id))
                {
                    return false;
                }
            }
            if let Some(stops) = &stop_ids {
                if !vehicle
                    .stop_id
                    .as_ref()
                    .is_some_and(|id| stops.contains(id))
                {
                    return false;
                }
            }
            true
        })
        .collect();
    matched.sort_by(|a, b| {
        let ta = a.vehicle.as_ref().and_then(|v| v.timestamp).unwrap_or(0);
        let tb = b.vehicle.as_ref().and_then(|v| v.timestamp).unwrap_or(0);
        tb.cmp(&ta).then(a.id.cmp(&b.id))
    });
    let total = matched.len();
    let offset = query.offset.min(total);
    let end = (offset + query.limit).min(total);
    let page = &matched[offset..end];
    let mut stop_ids_page = Vec::new();
    for entity in page {
        if let Some(id) = entity.vehicle.as_ref().and_then(|v| v.stop_id.clone()) {
            stop_ids_page.push(id);
        }
    }
    let stops = db.stops_for_ids(&stop_ids_page)?;
    let rows: Vec<Value> = page
        .iter()
        .map(|entity| vehicle_row(db, entity, &stops))
        .collect();
    Ok(page_object(
        feed,
        "vehicles",
        rows,
        total,
        offset,
    ))
}

pub fn trip_updates(
    db: &StaticDb,
    rt: RtView<'_>,
    query: &TripUpdateQuery,
) -> Result<Value, AppError> {
    let Some(feed) = rt.trip_updates else {
        return Err(err("this feed has no trip updates"));
    };
    let trip_filter = trip_filter(db, query.trip.as_deref())?;
    let route_ids = route_filter(db, query.route.as_deref())?;
    let stop_scope = match &query.stop {
        Some(stop) => {
            let resolved = db.resolve_stop(stop)?;
            let ids = db.expand_stop_ids(&resolved.stop_id)?;
            let visits = db.stop_times_for_stops(&ids)?;
            let mut by_trip: HashMap<String, HashSet<i64>> = HashMap::new();
            for row in visits {
                by_trip
                    .entry(row.trip_id)
                    .or_default()
                    .insert(row.stop_sequence);
            }
            Some((HashSet::from_iter(ids), by_trip))
        }
        None => None,
    };
    let mut matched: Vec<&FeedEntity> = feed
        .entity
        .iter()
        .filter(|entity| entity.trip_update.is_some())
        .filter(|entity| {
            let update = entity.trip_update.as_ref().unwrap();
            let trip_id = update.trip.trip_id.as_deref();
            if let Some(filter) = &trip_filter {
                if !trip_id.is_some_and(|id| filter.contains(id)) {
                    return false;
                }
            }
            if let Some(routes) = &route_ids {
                let route = update.trip.route_id.as_deref();
                let static_route = trip_id.and_then(|id| db.trip_route_id(id).ok().flatten());
                if !route.is_some_and(|id| routes.iter().any(|r| r == id))
                    && !static_route.is_some_and(|id| routes.iter().any(|r| r == &id))
                {
                    return false;
                }
            }
            if let Some((stop_ids, by_trip)) = &stop_scope {
                if !update_hits_stop(update, stop_ids, by_trip) {
                    return false;
                }
            }
            true
        })
        .collect();
    matched.sort_by(|a, b| a.id.cmp(&b.id));
    let total = matched.len();
    let offset = query.offset.min(total);
    let end = (offset + query.limit).min(total);
    let rows: Vec<Value> = matched[offset..end]
        .iter()
        .map(|entity| trip_update_row(db, entity, query.stop_limit))
        .collect();
    Ok(page_object(feed, "trip_updates", rows, total, offset))
}

pub fn alerts(db: &StaticDb, rt: RtView<'_>, query: &AlertQuery) -> Result<Value, AppError> {
    let Some(feed) = rt.alerts else {
        return Err(err("this feed has no alerts"));
    };
    let route_ids = route_filter(db, query.route.as_deref())?;
    let stop_ids: Option<HashSet<String>> = match &query.stop {
        Some(stop) => Some(HashSet::from_iter(
            db.expand_stop_ids(&db.resolve_stop(stop)?.stop_id)?,
        )),
        None => None,
    };
    let needle = query.text.as_ref().map(|t| t.to_lowercase());
    let mut matched: Vec<&FeedEntity> = feed
        .entity
        .iter()
        .filter(|entity| entity.alert.is_some())
        .filter(|entity| {
            let alert = entity.alert.as_ref().unwrap();
            if query.active_only {
                let periods = alert_periods(alert);
                if !periods.is_empty()
                    && !periods.iter().any(|range| period_contains(range, query.at_unix))
                {
                    return false;
                }
            }
            if let Some(routes) = &route_ids {
                if !alert_matches_route(db, alert, routes) {
                    return false;
                }
            }
            if let Some(stops) = &stop_ids {
                if !alert.informed_entity.iter().any(|selector| {
                    selector
                        .stop_id
                        .as_ref()
                        .is_some_and(|id| stops.contains(id))
                }) {
                    return false;
                }
            }
            if let Some(needle) = &needle {
                let header = translated(alert.header_text.as_ref(), 2000)
                    .map(|t| t.0)
                    .unwrap_or_default()
                    .to_lowercase();
                let body = translated(alert.description_text.as_ref(), 4000)
                    .map(|t| t.0)
                    .unwrap_or_default()
                    .to_lowercase();
                if !header.contains(needle) && !body.contains(needle) {
                    return false;
                }
            }
            true
        })
        .collect();
    matched.sort_by(|a, b| a.id.cmp(&b.id));
    let total = matched.len();
    let offset = query.offset.min(total);
    let end = (offset + query.limit).min(total);
    let rows: Vec<Value> = matched[offset..end]
        .iter()
        .map(|entity| alert_row(db, entity, query.max_chars))
        .collect();
    Ok(page_object(feed, "alerts", rows, total, offset))
}

pub fn decode_message(message: &FeedMessage, offset: usize, limit: usize, max_chars: usize) -> Value {
    let total = message.entity.len();
    let offset = offset.min(total);
    let end = (offset + limit).min(total);
    let entities: Vec<Value> = message.entity[offset..end]
        .iter()
        .map(|entity| decode_entity(entity, max_chars))
        .collect();
    obj([
        (
            "header",
            obj([
                (
                    "gtfs_realtime_version",
                    json!(message.header.gtfs_realtime_version),
                ),
                (
                    "incrementality",
                    json_opt(incrementality(message.header.incrementality)),
                ),
                ("timestamp", json_i64(message.header.timestamp.map(|v| v as i64))),
                ("feed_version", json_opt(message.header.feed_version.clone())),
            ]),
        ),
        ("entity_count", json!(total)),
        ("offset", json!(offset)),
        ("returned", json!(entities.len())),
        (
            "next_offset",
            if end < total {
                json!(end)
            } else {
                Value::Null
            },
        ),
        ("entities", Value::Array(entities)),
    ])
}

fn render_trip(
    db: &StaticDb,
    query: &TripStatusQuery,
    trip: &TripRow,
    date: NaiveDate,
    stops: &[StopTimeRow],
    vehicle: Option<&VehiclePosition>,
    update: Option<&TripUpdate>,
    at_stop: Option<&StopRow>,
) -> Result<Value, AppError> {
    let canceled = update
        .map(|update| is_canceled(update.trip.schedule_relationship))
        .unwrap_or(false);
    let relationship = update.and_then(|update| trip_relationship(update.trip.schedule_relationship));
    let owned = update.cloned();
    let rendered: Vec<Value> = stops
        .iter()
        .map(|row| stop_json(db, &owned, relationship.clone(), row, date, db.tz))
        .collect();
    let cursor = vehicle
        .and_then(|vehicle| vehicle.current_stop_sequence)
        .map(|seq| seq as i64);
    let upcoming_start = if let Some(seq) = cursor {
        rendered.iter().position(|row| row.get("stop_sequence").and_then(|v| v.as_i64()) == Some(seq))
    } else {
        rendered.iter().position(|row| {
            let when = row
                .get("estimated_departure")
                .and_then(|v| v.as_i64())
                .or_else(|| row.get("scheduled_departure").and_then(|v| v.as_i64()))
                .or_else(|| row.get("estimated_arrival").and_then(|v| v.as_i64()));
            when.is_some_and(|when| when >= query.at_unix - 60)
        })
    };
    let upcoming = if let Some(start) = upcoming_start {
        rendered.iter().skip(start).take(query.upcoming_limit).cloned().collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let next_stop = upcoming.first().cloned();
    let delay_sec = next_stop.as_ref().and_then(|row| {
        row.get("delay_sec")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32)
    });
    let at_stop_json = if let Some(wanted) = at_stop {
        let ids = db.expand_stop_ids(&wanted.stop_id)?;
        let found = rendered.into_iter().find(|row| {
            row.get("stop_id")
                .and_then(|v| v.as_str())
                .is_some_and(|id| ids.iter().any(|candidate| candidate == id))
        });
        match found {
            Some(row) => row,
            None => obj([
                ("stop_id", json!(wanted.stop_id)),
                (
                    "note",
                    json!(format!("trip {} does not stop here", trip.trip_id)),
                ),
            ]),
        }
    } else {
        Value::Null
    };
    let mut vehicle_out = Value::Null;
    if let Some(vehicle) = vehicle {
        let ids = vehicle.stop_id.clone().into_iter().collect::<Vec<_>>();
        let stops = db.stops_for_ids(&ids)?;
        vehicle_out = vehicle_position_json(db, vehicle, &stops);
    }
    Ok(obj([
        ("trip_id", json!(trip.trip_id)),
        ("trip_short_name", json_opt(trip.short_name.clone())),
        ("headsign", json_opt(trip.headsign.clone())),
        ("route_id", json!(trip.route_id)),
        ("route_short_name", json_opt(trip.route_short_name.clone())),
        ("route_long_name", json_opt(trip.route_long_name.clone())),
        ("route_type", json!(route_type_name(trip.route_type))),
        ("direction_id", json_i64(trip.direction_id)),
        ("service_date", json!(date.format("%Y%m%d").to_string())),
        ("timezone", json!(db.tz_name)),
        ("schedule_relationship", json_opt(relationship)),
        ("canceled", if canceled { json!(true) } else { Value::Null }),
        ("delay_sec", json_i32(delay_sec)),
        (
            "delay_label",
            delay_sec.map(|sec| json!(delay_label(sec))).unwrap_or(Value::Null),
        ),
        ("vehicle", vehicle_out),
        ("next_stop", next_stop.unwrap_or(Value::Null)),
        ("upcoming_stops", Value::Array(upcoming)),
        ("at_stop", at_stop_json),
    ]))
}

fn stop_json(
    db: &StaticDb,
    update: &Option<TripUpdate>,
    trip_relationship_name: Option<String>,
    row: &StopTimeRow,
    date: NaiveDate,
    tz: Tz,
) -> Value {
    let sched_arr = row.arrival_sec.map(|sec| gtfs_unix(date, sec, tz));
    let sched_dep = row.departure_sec.map(|sec| gtfs_unix(date, sec, tz));
    let canceled = trip_relationship_name.as_deref() == Some("CANCELED")
        || trip_relationship_name.as_deref() == Some("DELETED");
    let exact = update
        .as_ref()
        .and_then(|update| find_stop_update(update, row.stop_sequence as u32, &row.stop_id));
    let mut arrival = predict(sched_arr, exact.and_then(|row| row.arrival.as_ref()));
    let mut departure = predict(sched_dep, exact.and_then(|row| row.departure.as_ref()));
    if exact.is_none() && !canceled {
        if let Some(delay) = update
            .as_ref()
            .and_then(|update| propagated_delay(update, row.stop_sequence as u32))
        {
            if arrival.delay_sec.is_none() {
                arrival.delay_sec = Some(delay);
                arrival.estimated = sched_arr.map(|sched| sched + i64::from(delay));
            }
            if departure.delay_sec.is_none() {
                departure.delay_sec = Some(delay);
                departure.estimated = sched_dep.map(|sched| sched + i64::from(delay));
            }
        }
    }
    let relationship = exact.and_then(|row| stop_relationship(row.schedule_relationship));
    let skipped = exact
        .and_then(|row| row.schedule_relationship)
        == Some(StopRel::Skipped as i32);
    let assigned = exact.and_then(|row| {
        row.stop_time_properties
            .as_ref()
            .and_then(|props| props.assigned_stop_id.clone())
    });
    let assigned_stop = assigned
        .as_deref()
        .and_then(|id| db.stop_by_id(id).ok().flatten());
    let platform = assigned_stop
        .as_ref()
        .and_then(|stop| stop.platform_code.clone())
        .or_else(|| row.platform_code.clone());
    let delay = departure.delay_sec.or(arrival.delay_sec);
    obj([
        ("stop_sequence", json!(row.stop_sequence)),
        ("stop_id", json!(row.stop_id)),
        ("stop_name", json_opt(row.stop_name.clone())),
        ("trip_id", json!(row.trip_id)),
        ("trip_short_name", json_opt(row.trip_short_name.clone())),
        ("headsign", json_opt(row.headsign.clone().or(row.trip_headsign.clone()))),
        ("route_id", json!(row.route_id)),
        ("route_short_name", json_opt(row.route_short_name.clone())),
        ("route_long_name", json_opt(row.route_long_name.clone())),
        ("route_type", json!(route_type_name(row.route_type))),
        ("scheduled_arrival", json_i64(sched_arr)),
        (
            "scheduled_arrival_local",
            sched_arr.map(|unix| json!(hhmm(unix, tz))).unwrap_or(Value::Null),
        ),
        ("estimated_arrival", json_i64(arrival.estimated)),
        (
            "estimated_arrival_local",
            arrival
                .estimated
                .map(|unix| json!(hhmm(unix, tz)))
                .unwrap_or(Value::Null),
        ),
        ("arrival_delay_sec", json_i32(arrival.delay_sec)),
        ("scheduled_departure", json_i64(sched_dep)),
        (
            "scheduled_departure_local",
            sched_dep.map(|unix| json!(hhmm(unix, tz))).unwrap_or(Value::Null),
        ),
        ("estimated_departure", json_i64(departure.estimated)),
        (
            "estimated_departure_local",
            departure
                .estimated
                .map(|unix| json!(hhmm(unix, tz)))
                .unwrap_or(Value::Null),
        ),
        ("departure_delay_sec", json_i32(departure.delay_sec)),
        ("delay_sec", json_i32(delay)),
        (
            "delay_label",
            delay.map(|sec| json!(delay_label(sec))).unwrap_or(Value::Null),
        ),
        ("schedule_relationship", json_opt(relationship.or(trip_relationship_name))),
        ("skipped", if skipped { json!(true) } else { Value::Null }),
        ("canceled", if canceled { json!(true) } else { Value::Null }),
        ("assigned_stop_id", json_opt(assigned)),
        ("platform_code", json_opt(platform.clone())),
        ("track", json_opt(platform)),
    ])
}

fn vehicle_row(db: &StaticDb, entity: &FeedEntity, stops: &HashMap<String, StopRow>) -> Value {
    let Some(vehicle) = entity.vehicle.as_ref() else {
        return obj([("id", json!(entity.id))]);
    };
    let mut row = vehicle_position_json(db, vehicle, stops);
    if let Some(object) = row.as_object_mut() {
        object.insert("entity_id".into(), json!(entity.id));
    }
    row
}

fn vehicle_position_json(
    db: &StaticDb,
    vehicle: &VehiclePosition,
    stops: &HashMap<String, StopRow>,
) -> Value {
    let stop = vehicle
        .stop_id
        .as_ref()
        .and_then(|id| stops.get(id).cloned().or_else(|| db.stop_by_id(id).ok().flatten()));
    let trip_id = vehicle.trip.as_ref().and_then(|t| t.trip_id.clone());
    let static_trip = trip_id
        .as_deref()
        .and_then(|id| db.trip_by_id(id).ok().flatten());
    obj([
        ("trip_id", json_opt(trip_id)),
        (
            "trip_short_name",
            json_opt(static_trip.as_ref().and_then(|t| t.short_name.clone())),
        ),
        (
            "headsign",
            json_opt(static_trip.as_ref().and_then(|t| t.headsign.clone())),
        ),
        (
            "route_id",
            json_opt(
                vehicle
                    .trip
                    .as_ref()
                    .and_then(|t| t.route_id.clone())
                    .or_else(|| static_trip.as_ref().map(|t| t.route_id.clone())),
            ),
        ),
        (
            "route_short_name",
            json_opt(static_trip.as_ref().and_then(|t| t.route_short_name.clone())),
        ),
        ("vehicle", vehicle_json(vehicle.vehicle.as_ref())),
        (
            "lat",
            vehicle
                .position
                .as_ref()
                .map(|p| json!(round_coord(p.latitude)))
                .unwrap_or(Value::Null),
        ),
        (
            "lon",
            vehicle
                .position
                .as_ref()
                .map(|p| json!(round_coord(p.longitude)))
                .unwrap_or(Value::Null),
        ),
        (
            "bearing",
            vehicle
                .position
                .as_ref()
                .and_then(|p| p.bearing)
                .map(|v| json!((f64::from(v) * 10.0).round() / 10.0))
                .unwrap_or(Value::Null),
        ),
        (
            "speed_mps",
            vehicle
                .position
                .as_ref()
                .and_then(|p| p.speed)
                .map(|v| json!((f64::from(v) * 10.0).round() / 10.0))
                .unwrap_or(Value::Null),
        ),
        (
            "status",
            json_opt(vehicle_status(
                vehicle.current_status,
                vehicle.current_stop_sequence.is_some(),
            )),
        ),
        ("stop_sequence", json_i64(vehicle.current_stop_sequence.map(|v| v as i64))),
        ("stop_id", json_opt(vehicle.stop_id.clone())),
        ("stop_name", json_opt(stop.as_ref().and_then(|s| s.stop_name.clone()))),
        (
            "platform_code",
            json_opt(stop.as_ref().and_then(|s| s.platform_code.clone())),
        ),
        ("timestamp", json_i64(vehicle.timestamp.map(|v| v as i64))),
        ("occupancy", json_opt(occupancy_name(vehicle.occupancy_status))),
        (
            "occupancy_percentage",
            json_i64(vehicle.occupancy_percentage.map(|v| v as i64)),
        ),
    ])
}

fn trip_update_row(db: &StaticDb, entity: &FeedEntity, stop_limit: usize) -> Value {
    let Some(update) = entity.trip_update.as_ref() else {
        return obj([("id", json!(entity.id))]);
    };
    let trip_id = update.trip.trip_id.clone();
    let static_trip = trip_id
        .as_deref()
        .and_then(|id| db.trip_by_id(id).ok().flatten());
    let total = update.stop_time_update.len();
    let shown = update.stop_time_update.iter().take(stop_limit);
    let stops: Vec<Value> = shown
        .map(|row| {
            let assigned = row
                .stop_time_properties
                .as_ref()
                .and_then(|props| props.assigned_stop_id.clone());
            let platform = assigned
                .as_deref()
                .and_then(|id| db.stop_by_id(id).ok().flatten())
                .and_then(|stop| stop.platform_code);
            let stop_name = row
                .stop_id
                .as_deref()
                .and_then(|id| db.stop_by_id(id).ok().flatten())
                .and_then(|stop| stop.stop_name);
            obj([
                ("stop_sequence", json_i64(row.stop_sequence.map(|v| v as i64))),
                ("stop_id", json_opt(row.stop_id.clone())),
                ("stop_name", json_opt(stop_name)),
                ("arrival_delay_sec", json_i32(row.arrival.as_ref().and_then(|e| e.delay))),
                ("arrival_time", json_i64(row.arrival.as_ref().and_then(|e| e.time))),
                (
                    "departure_delay_sec",
                    json_i32(row.departure.as_ref().and_then(|e| e.delay)),
                ),
                ("departure_time", json_i64(row.departure.as_ref().and_then(|e| e.time))),
                (
                    "schedule_relationship",
                    json_opt(stop_relationship(row.schedule_relationship)),
                ),
                ("assigned_stop_id", json_opt(assigned)),
                ("platform_code", json_opt(platform.clone())),
                ("track", json_opt(platform)),
            ])
        })
        .collect();
    obj([
        ("entity_id", json!(entity.id)),
        ("trip_id", json_opt(trip_id)),
        (
            "trip_short_name",
            json_opt(static_trip.as_ref().and_then(|t| t.short_name.clone())),
        ),
        ("headsign", json_opt(static_trip.as_ref().and_then(|t| t.headsign.clone()))),
        (
            "route_id",
            json_opt(
                update
                    .trip
                    .route_id
                    .clone()
                    .or_else(|| static_trip.as_ref().map(|t| t.route_id.clone())),
            ),
        ),
        (
            "route_short_name",
            json_opt(static_trip.as_ref().and_then(|t| t.route_short_name.clone())),
        ),
        (
            "schedule_relationship",
            json_opt(trip_relationship(update.trip.schedule_relationship)),
        ),
        ("start_date", json_opt(update.trip.start_date.clone())),
        ("delay_sec", json_i32(update.delay)),
        ("vehicle", vehicle_json(update.vehicle.as_ref())),
        ("stop_update_count", json!(total)),
        (
            "stop_updates_omitted",
            if total > stops.len() {
                json!(total - stops.len())
            } else {
                Value::Null
            },
        ),
        ("stop_updates", Value::Array(stops)),
    ])
}

#[allow(deprecated)]
fn alert_row(db: &StaticDb, entity: &FeedEntity, max_chars: usize) -> Value {
    let Some(alert) = entity.alert.as_ref() else {
        return obj([("id", json!(entity.id))]);
    };
    let (header, header_truncated) =
        translated(alert.header_text.as_ref(), max_chars).unwrap_or_default();
    let (description, description_truncated) =
        translated(alert.description_text.as_ref(), max_chars).unwrap_or_default();
    let (url, _) = translated(alert.url.as_ref(), 500).unwrap_or_default();
    let informed: Vec<Value> = alert
        .informed_entity
        .iter()
        .map(|selector| {
            let route = selector
                .route_id
                .as_deref()
                .and_then(|id| db.route(id).ok().flatten());
            obj([
                ("agency_id", json_opt(selector.agency_id.clone())),
                ("route_id", json_opt(selector.route_id.clone())),
                (
                    "route_short_name",
                    json_opt(route.as_ref().and_then(|r| r.short_name.clone())),
                ),
                (
                    "route_long_name",
                    json_opt(route.as_ref().and_then(|r| r.long_name.clone())),
                ),
                ("route_type", json_i64(selector.route_type.map(|v| v as i64))),
                ("stop_id", json_opt(selector.stop_id.clone())),
                (
                    "trip_id",
                    json_opt(selector.trip.as_ref().and_then(|t| t.trip_id.clone())),
                ),
                ("direction_id", json_i64(selector.direction_id.map(|v| v as i64))),
            ])
        })
        .collect();
    let periods: Vec<Value> = alert_periods(alert)
        .into_iter()
        .map(|range| {
            obj([
                ("start", json_i64(range.start.map(|v| v as i64))),
                ("end", json_i64(range.end.map(|v| v as i64))),
            ])
        })
        .collect();
    obj([
        ("id", json!(entity.id)),
        ("header", if header.is_empty() { Value::Null } else { json!(header) }),
        (
            "header_truncated",
            if header_truncated { json!(true) } else { Value::Null },
        ),
        (
            "description",
            if description.is_empty() {
                Value::Null
            } else {
                json!(description)
            },
        ),
        (
            "description_truncated",
            if description_truncated {
                json!(true)
            } else {
                Value::Null
            },
        ),
        ("url", if url.is_empty() { Value::Null } else { json!(url) }),
        ("cause", json_opt(cause_name(alert.cause))),
        ("effect", json_opt(effect_name(alert.effect))),
        ("severity", json_opt(severity_name(alert.severity_level))),
        ("informed_entity", Value::Array(informed)),
        (
            "active_period",
            if periods.is_empty() {
                Value::Null
            } else {
                Value::Array(periods)
            },
        ),
    ])
}

fn decode_entity(entity: &FeedEntity, max_chars: usize) -> Value {
    let kind = if entity.trip_update.is_some() {
        "trip_update"
    } else if entity.vehicle.is_some() {
        "vehicle"
    } else if entity.alert.is_some() {
        "alert"
    } else if entity.shape.is_some() {
        "shape"
    } else if entity.stop.is_some() {
        "stop"
    } else if entity.trip_modifications.is_some() {
        "trip_modifications"
    } else {
        "unknown"
    };
    let mut value = obj([
        ("id", json!(entity.id)),
        ("kind", json!(kind)),
        (
            "is_deleted",
            if entity.is_deleted == Some(true) {
                json!(true)
            } else {
                Value::Null
            },
        ),
    ]);
    if let Some(object) = value.as_object_mut() {
        if let Some(update) = entity.trip_update.as_ref() {
            let stops: Vec<Value> = update
                .stop_time_update
                .iter()
                .take(40)
                .map(|row| {
                    obj([
                        ("stop_sequence", json_i64(row.stop_sequence.map(|v| v as i64))),
                        ("stop_id", json_opt(row.stop_id.clone())),
                        ("arrival_delay_sec", json_i32(row.arrival.as_ref().and_then(|e| e.delay))),
                        ("departure_delay_sec", json_i32(row.departure.as_ref().and_then(|e| e.delay))),
                        (
                            "schedule_relationship",
                            json_opt(stop_relationship(row.schedule_relationship)),
                        ),
                        (
                            "assigned_stop_id",
                            json_opt(
                                row.stop_time_properties
                                    .as_ref()
                                    .and_then(|props| props.assigned_stop_id.clone()),
                            ),
                        ),
                    ])
                })
                .collect();
            object.insert(
                "trip_update".into(),
                obj([
                    ("trip_id", json_opt(update.trip.trip_id.clone())),
                    ("route_id", json_opt(update.trip.route_id.clone())),
                    (
                        "schedule_relationship",
                        json_opt(trip_relationship(update.trip.schedule_relationship)),
                    ),
                    ("delay_sec", json_i32(update.delay)),
                    ("stop_updates", Value::Array(stops)),
                ]),
            );
        }
        if let Some(vehicle) = entity.vehicle.as_ref() {
            object.insert(
                "vehicle".into(),
                obj([
                    ("trip_id", json_opt(vehicle.trip.as_ref().and_then(|t| t.trip_id.clone()))),
                    ("route_id", json_opt(vehicle.trip.as_ref().and_then(|t| t.route_id.clone()))),
                    ("stop_id", json_opt(vehicle.stop_id.clone())),
                    (
                        "status",
                        json_opt(vehicle_status(
                            vehicle.current_status,
                            vehicle.current_stop_sequence.is_some(),
                        )),
                    ),
                    ("lat", vehicle.position.as_ref().map(|p| json!(round_coord(p.latitude))).unwrap_or(Value::Null)),
                    ("lon", vehicle.position.as_ref().map(|p| json!(round_coord(p.longitude))).unwrap_or(Value::Null)),
                    ("timestamp", json_i64(vehicle.timestamp.map(|v| v as i64))),
                    ("vehicle", vehicle_json(vehicle.vehicle.as_ref())),
                ]),
            );
        }
        if entity.alert.is_some() {
            let (header, truncated) = entity
                .alert
                .as_ref()
                .and_then(|alert| translated(alert.header_text.as_ref(), max_chars))
                .unwrap_or_default();
            let (description, description_truncated) = entity
                .alert
                .as_ref()
                .and_then(|alert| translated(alert.description_text.as_ref(), max_chars))
                .unwrap_or_default();
            object.insert(
                "alert".into(),
                obj([
                    ("header", if header.is_empty() { Value::Null } else { json!(header) }),
                    ("header_truncated", if truncated { json!(true) } else { Value::Null }),
                    (
                        "description",
                        if description.is_empty() {
                            Value::Null
                        } else {
                            json!(description)
                        },
                    ),
                    (
                        "description_truncated",
                        if description_truncated { json!(true) } else { Value::Null },
                    ),
                    (
                        "cause",
                        json_opt(entity.alert.as_ref().and_then(|a| cause_name(a.cause))),
                    ),
                    (
                        "effect",
                        json_opt(entity.alert.as_ref().and_then(|a| effect_name(a.effect))),
                    ),
                ]),
            );
        }
    }
    value
}

fn page_object(
    feed: &FeedMessage,
    key: &'static str,
    rows: Vec<Value>,
    total: usize,
    offset: usize,
) -> Value {
    let next = offset + rows.len();
    obj([
        (
            "header_timestamp",
            json_i64(feed.header.timestamp.map(|v| v as i64)),
        ),
        (
            "feed_version",
            json_opt(feed.header.feed_version.clone()),
        ),
        ("total", json!(total)),
        ("offset", json!(offset)),
        ("returned", json!(rows.len())),
        (
            "next_offset",
            if next < total { json!(next) } else { Value::Null },
        ),
        (key, Value::Array(rows)),
    ])
}

fn resolve_trips(db: &StaticDb, query: &str) -> Result<Vec<TripRow>, AppError> {
    let query = query.trim();
    if query.is_empty() {
        return Err(err("trip is empty").hint("pass a trip_id or train number"));
    }
    if let Some(trip) = db.trip_by_id(query)? {
        return Ok(vec![trip]);
    }
    db.trips_by_short_name(query)
}

fn trip_filter(db: &StaticDb, query: Option<&str>) -> Result<Option<HashSet<String>>, AppError> {
    let Some(query) = query.map(str::trim).filter(|q| !q.is_empty()) else {
        return Ok(None);
    };
    let trips = resolve_trips(db, query)?;
    if trips.is_empty() {
        return Ok(Some(HashSet::from([query.to_string()])));
    }
    Ok(Some(trips.into_iter().map(|t| t.trip_id).collect()))
}

fn route_filter(db: &StaticDb, query: Option<&str>) -> Result<Option<Vec<String>>, AppError> {
    let Some(query) = query.map(str::trim).filter(|q| !q.is_empty()) else {
        return Ok(None);
    };
    let ids = db.resolve_route_ids(query)?;
    if ids.is_empty() {
        Ok(Some(vec![query.to_string()]))
    } else {
        Ok(Some(ids))
    }
}

fn alert_matches_route(
    db: &StaticDb,
    alert: &crate::gtfs_rt::Alert,
    routes: &[String],
) -> bool {
    alert.informed_entity.iter().any(|selector| {
        if selector
            .route_id
            .as_ref()
            .is_some_and(|id| routes.iter().any(|route| route == id))
        {
            return true;
        }
        selector
            .trip
            .as_ref()
            .and_then(|trip| trip.route_id.as_ref())
            .is_some_and(|id| routes.iter().any(|route| route == id))
            || selector
                .trip
                .as_ref()
                .and_then(|trip| trip.trip_id.as_deref())
                .and_then(|id| db.trip_route_id(id).ok().flatten())
                .is_some_and(|id| routes.iter().any(|route| route == &id))
    })
}

fn update_hits_stop(
    update: &TripUpdate,
    stop_ids: &HashSet<String>,
    by_trip: &HashMap<String, HashSet<i64>>,
) -> bool {
    if update.stop_time_update.iter().any(|row| {
        row.stop_id
            .as_ref()
            .is_some_and(|id| stop_ids.contains(id))
            || row
                .stop_time_properties
                .as_ref()
                .and_then(|props| props.assigned_stop_id.as_ref())
                .is_some_and(|id| stop_ids.contains(id))
    }) {
        return true;
    }
    let Some(trip_id) = update.trip.trip_id.as_deref() else {
        return false;
    };
    let Some(sequences) = by_trip.get(trip_id) else {
        return false;
    };
    update.stop_time_update.iter().any(|row| {
        row.stop_sequence
            .is_some_and(|seq| sequences.contains(&(seq as i64)))
    })
}

fn index_vehicles(feed: Option<&FeedMessage>) -> HashMap<&str, &VehiclePosition> {
    let mut map = HashMap::new();
    if let Some(feed) = feed {
        for entity in &feed.entity {
            if let Some(vehicle) = entity.vehicle.as_ref() {
                if let Some(trip_id) = vehicle.trip.as_ref().and_then(|t| t.trip_id.as_deref()) {
                    map.insert(trip_id, vehicle);
                }
            }
        }
    }
    map
}

fn index_updates(feed: Option<&FeedMessage>) -> HashMap<&str, &TripUpdate> {
    let mut map = HashMap::new();
    if let Some(feed) = feed {
        for entity in &feed.entity {
            if let Some(update) = entity.trip_update.as_ref() {
                if let Some(trip_id) = update.trip.trip_id.as_deref() {
                    map.insert(trip_id, update);
                }
            }
        }
    }
    map
}

fn trip_rel_of(update: Option<&TripUpdate>) -> Option<String> {
    update.and_then(|update| trip_relationship(update.trip.schedule_relationship))
}

fn score_candidate(
    at: i64,
    tz: Tz,
    date: NaiveDate,
    stops: &[StopTimeRow],
    vehicle: Option<&VehiclePosition>,
    update: Option<&TripUpdate>,
) -> i64 {
    let mut score = 0i64;
    if vehicle.is_some() {
        score += 1000;
    }
    if update.is_some() {
        score += 200;
    }
    if update.is_some_and(|update| is_canceled(update.trip.schedule_relationship)) {
        score -= 50;
    }
    if let (Some(first), Some(last)) = (stops.first(), stops.last()) {
        let start = first
            .arrival_sec
            .or(first.departure_sec)
            .map(|sec| gtfs_unix(date, sec, tz));
        let end = last
            .departure_sec
            .or(last.arrival_sec)
            .map(|sec| gtfs_unix(date, sec, tz));
        if let (Some(start), Some(end)) = (start, end) {
            if at >= start - 600 && at <= end + 600 {
                score += 300;
            }
            let distance = if at < start { start - at } else if at > end { at - end } else { 0 };
            score -= distance / 60;
        }
    }
    score
}

fn service_dates(db: &StaticDb, service_id: &str, at: i64) -> Vec<NaiveDate> {
    let today = local_date(at, db.tz);
    let mut dates = Vec::new();
    if let Some(yesterday) = today.pred_opt() {
        if db.service_active(service_id, yesterday) {
            dates.push(yesterday);
        }
    }
    if db.service_active(service_id, today) {
        dates.push(today);
    }
    dates
}

fn local_date(at: i64, tz: Tz) -> NaiveDate {
    at_datetime(at, tz).date_naive()
}

fn at_datetime(at: i64, tz: Tz) -> DateTime<Tz> {
    DateTime::from_timestamp(at, 0)
        .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap())
        .with_timezone(&tz)
}

fn midnight_unix(date: NaiveDate, tz: Tz) -> i64 {
    let naive = date.and_hms_opt(0, 0, 0).unwrap();
    tz.from_local_datetime(&naive)
        .single()
        .or_else(|| tz.from_local_datetime(&naive).earliest())
        .map(|dt| dt.timestamp())
        .unwrap_or(0)
}

fn gtfs_unix(date: NaiveDate, sec: i64, tz: Tz) -> i64 {
    midnight_unix(date, tz) + sec
}

fn hhmm(unix: i64, tz: Tz) -> String {
    let dt = at_datetime(unix, tz);
    format!("{:02}:{:02}", dt.hour(), dt.minute())
}

