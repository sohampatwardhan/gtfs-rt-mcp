use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Read};

use chrono::{Datelike, NaiveDate, Weekday};
use chrono_tz::Tz;
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use zip::ZipArchive;

use crate::error::{err, AppError};

#[derive(Debug, Clone)]
pub struct Counts {
    pub agencies: i64,
    pub stops: i64,
    pub routes: i64,
    pub trips: i64,
    pub stop_times: i64,
    pub calendar: i64,
    pub calendar_dates: i64,
}

#[derive(Debug, Clone)]
pub struct StopRow {
    pub stop_id: String,
    pub stop_code: Option<String>,
    pub stop_name: Option<String>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub location_type: i64,
    pub parent_station: Option<String>,
    pub platform_code: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RouteRow {
    pub route_id: String,
    pub short_name: Option<String>,
    pub long_name: Option<String>,
    pub route_type: i64,
    pub color: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TripRow {
    pub trip_id: String,
    pub route_id: String,
    pub service_id: String,
    pub headsign: Option<String>,
    pub short_name: Option<String>,
    pub direction_id: Option<i64>,
    pub route_short_name: Option<String>,
    pub route_long_name: Option<String>,
    pub route_type: i64,
}

#[derive(Debug, Clone)]
pub struct StopTimeRow {
    pub trip_id: String,
    pub stop_sequence: i64,
    pub stop_id: String,
    pub stop_name: Option<String>,
    pub platform_code: Option<String>,
    pub arrival_sec: Option<i64>,
    pub departure_sec: Option<i64>,
    #[allow(dead_code)]
    pub pickup: Option<i64>,
    #[allow(dead_code)]
    pub drop_off: Option<i64>,
    pub headsign: Option<String>,
    pub service_id: String,
    pub trip_headsign: Option<String>,
    pub trip_short_name: Option<String>,
    #[allow(dead_code)]
    pub direction_id: Option<i64>,
    pub route_id: String,
    pub route_short_name: Option<String>,
    pub route_long_name: Option<String>,
    pub route_type: i64,
}

#[derive(Debug, Clone)]
struct CalendarRow {
    monday: bool,
    tuesday: bool,
    wednesday: bool,
    thursday: bool,
    friday: bool,
    saturday: bool,
    sunday: bool,
    start_date: String,
    end_date: String,
}

#[derive(Debug)]
pub struct StaticDb {
    conn: Connection,
    pub tz_name: String,
    pub tz: Tz,
    pub agency_name: Option<String>,
    pub feed_version: Option<String>,
    pub publisher: Option<String>,
    pub counts: Counts,
    calendar: HashMap<String, CalendarRow>,
    added: HashMap<String, HashSet<String>>,
    removed: HashMap<String, HashSet<String>>,
}

impl StaticDb {
    pub fn load_zip(bytes: &[u8]) -> Result<Self, AppError> {
        let files = read_zip(bytes)?;
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "
            PRAGMA foreign_keys=OFF;
            CREATE TABLE stops (
                stop_id TEXT PRIMARY KEY,
                stop_code TEXT,
                stop_name TEXT,
                name_fold TEXT,
                code_fold TEXT,
                lat REAL,
                lon REAL,
                location_type INTEGER NOT NULL DEFAULT 0,
                parent_station TEXT,
                platform_code TEXT
            );
            CREATE TABLE routes (
                route_id TEXT PRIMARY KEY,
                agency_id TEXT,
                short_name TEXT,
                long_name TEXT,
                name_fold TEXT,
                short_fold TEXT,
                route_type INTEGER NOT NULL DEFAULT 0,
                color TEXT
            );
            CREATE TABLE trips (
                trip_id TEXT PRIMARY KEY,
                route_id TEXT,
                service_id TEXT,
                headsign TEXT,
                short_name TEXT,
                short_fold TEXT,
                direction_id INTEGER
            );
            CREATE TABLE stop_times (
                trip_id TEXT NOT NULL,
                stop_sequence INTEGER NOT NULL,
                stop_id TEXT NOT NULL,
                arrival_sec INTEGER,
                departure_sec INTEGER,
                pickup INTEGER,
                drop_off INTEGER,
                headsign TEXT,
                PRIMARY KEY (trip_id, stop_sequence)
            );
            CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
            ",
        )?;

        let agencies = load_agencies(&files);
        let feed = load_feed_info(&files);
        load_stops(&conn, files.get("stops.txt"))?;
        load_routes(&conn, files.get("routes.txt"))?;
        load_trips(&conn, files.get("trips.txt"))?;
        load_stop_times(&conn, files.get("stop_times.txt"))?;
        let (calendar, added, removed) = load_services(&files)?;
        conn.execute_batch(
            "
            CREATE INDEX idx_stops_parent ON stops(parent_station);
            CREATE INDEX idx_stops_name ON stops(name_fold);
            CREATE INDEX idx_stops_code ON stops(code_fold);
            CREATE INDEX idx_trips_short ON trips(short_fold);
            CREATE INDEX idx_trips_route ON trips(route_id);
            CREATE INDEX idx_st_stop ON stop_times(stop_id);
            CREATE INDEX idx_st_trip ON stop_times(trip_id);
            ",
        )?;

        if conn.query_row::<i64, _, _>("SELECT COUNT(*) FROM stops", [], |r| r.get(0))? == 0 {
            return Err(err("GTFS archive has no stops").hint("expected stops.txt inside the zip"));
        }
        if conn.query_row::<i64, _, _>("SELECT COUNT(*) FROM stop_times", [], |r| r.get(0))? == 0
        {
            return Err(
                err("GTFS archive has no stop_times").hint("departure boards need stop_times.txt")
            );
        }

        let agency = choose_agency(&conn, &agencies)?;
        let tz_name = agency
            .timezone
            .clone()
            .unwrap_or_else(|| "UTC".to_string());
        let tz = tz_name.parse::<Tz>().unwrap_or(chrono_tz::UTC);
        let counts = Counts {
            agencies: agencies.len() as i64,
            stops: count(&conn, "stops")?,
            routes: count(&conn, "routes")?,
            trips: count(&conn, "trips")?,
            stop_times: count(&conn, "stop_times")?,
            calendar: calendar.len() as i64,
            calendar_dates: (added.values().map(|s| s.len()).sum::<usize>()
                + removed.values().map(|s| s.len()).sum::<usize>()) as i64,
        };
        let _ = conn.execute(
            "INSERT INTO meta (key, value) VALUES ('timezone', ?1)",
            params![tz_name],
        );

        Ok(Self {
            conn,
            tz,
            tz_name,
            agency_name: agency.name,
            feed_version: feed.version,
            publisher: feed.publisher,
            counts,
            calendar,
            added,
            removed,
        })
    }

    pub fn service_active(&self, service_id: &str, date: NaiveDate) -> bool {
        let key = date.format("%Y%m%d").to_string();
        if self
            .removed
            .get(service_id)
            .is_some_and(|days| days.contains(&key))
        {
            return false;
        }
        if self
            .added
            .get(service_id)
            .is_some_and(|days| days.contains(&key))
        {
            return true;
        }
        let Some(row) = self.calendar.get(service_id) else {
            return false;
        };
        if key < row.start_date || key > row.end_date {
            return false;
        }
        match date.weekday() {
            Weekday::Mon => row.monday,
            Weekday::Tue => row.tuesday,
            Weekday::Wed => row.wednesday,
            Weekday::Thu => row.thursday,
            Weekday::Fri => row.friday,
            Weekday::Sat => row.saturday,
            Weekday::Sun => row.sunday,
        }
    }

    pub fn stop_by_id(&self, stop_id: &str) -> Result<Option<StopRow>, AppError> {
        Ok(self
            .conn
            .query_row(
                "SELECT stop_id, stop_code, stop_name, lat, lon, location_type, parent_station, platform_code
                 FROM stops WHERE stop_id = ?1",
                params![stop_id],
                map_stop,
            )
            .optional()?)
    }

    pub fn resolve_stop(&self, query: &str) -> Result<StopRow, AppError> {
        let query = query.trim();
        if query.is_empty() {
            return Err(err("stop is empty").hint("pass a stop_id, stop_code, or stop name"));
        }
        if let Some(stop) = self.stop_by_id(query)? {
            return Ok(stop);
        }
        let fold = query.to_lowercase();
        let mut stmt = self.conn.prepare(
            "SELECT stop_id, stop_code, stop_name, lat, lon, location_type, parent_station, platform_code
             FROM stops
             WHERE code_fold = ?1 OR name_fold = ?1 OR name_fold LIKE ?2 ESCAPE '\\'",
        )?;
        let like = like_contains(&fold);
        let rows = stmt
            .query_map(params![fold, like], map_stop)?
            .collect::<Result<Vec<_>, _>>()?;
        choose_stop(self, rows, query)
    }

    pub fn search_stops(
        &self,
        query: &str,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<StopRow>, i64), AppError> {
        let fold = query.trim().to_lowercase();
        let (where_sql, args): (&str, Vec<String>) = if fold.is_empty() {
            ("1", vec![])
        } else {
            (
                "stop_id = ?1 OR code_fold = ?2 OR name_fold LIKE ?3 ESCAPE '\\' OR code_fold LIKE ?3 ESCAPE '\\'",
                vec![query.trim().to_string(), fold.clone(), like_contains(&fold)],
            )
        };
        let total: i64 = self.conn.query_row(
            &format!("SELECT COUNT(*) FROM stops WHERE {where_sql}"),
            params_from_iter(args.iter()),
            |r| r.get(0),
        )?;
        let mut stmt = self.conn.prepare(&format!(
            "SELECT stop_id, stop_code, stop_name, lat, lon, location_type, parent_station, platform_code
             FROM stops WHERE {where_sql}
             ORDER BY
               CASE WHEN stop_id = ?{id} THEN 0
                    WHEN code_fold = ?{fold} THEN 1
                    WHEN name_fold = ?{fold} THEN 2
                    ELSE 3 END,
               name_fold
             LIMIT ?{limit} OFFSET ?{offset}",
            id = args.len() + 1,
            fold = args.len() + 2,
            limit = args.len() + 3,
            offset = args.len() + 4,
        ))?;
        let mut bound = args.clone();
        bound.push(query.trim().to_string());
        bound.push(fold);
        let rows = stmt
            .query_map(
                params_from_iter(
                    bound
                        .iter()
                        .map(|s| s as &dyn rusqlite::ToSql)
                        .chain([
                            &(limit as i64) as &dyn rusqlite::ToSql,
                            &(offset as i64) as &dyn rusqlite::ToSql,
                        ]),
                ),
                map_stop,
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok((rows, total))
    }

    pub fn search_routes(
        &self,
        query: &str,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<RouteRow>, i64), AppError> {
        let fold = query.trim().to_lowercase();
        let (where_sql, args): (&str, Vec<String>) = if fold.is_empty() {
            ("1", vec![])
        } else {
            (
                "route_id = ?1 OR short_fold = ?2 OR name_fold LIKE ?3 ESCAPE '\\' OR short_fold LIKE ?3 ESCAPE '\\'",
                vec![query.trim().to_string(), fold.clone(), like_contains(&fold)],
            )
        };
        let total: i64 = self.conn.query_row(
            &format!("SELECT COUNT(*) FROM routes WHERE {where_sql}"),
            params_from_iter(args.iter()),
            |r| r.get(0),
        )?;
        let mut stmt = self.conn.prepare(&format!(
            "SELECT route_id, short_name, long_name, route_type, color
             FROM routes WHERE {where_sql}
             ORDER BY CASE WHEN route_id = ?{id} THEN 0 WHEN short_fold = ?{fold} THEN 1 ELSE 2 END, short_fold
             LIMIT ?{limit} OFFSET ?{offset}",
            id = args.len() + 1,
            fold = args.len() + 2,
            limit = args.len() + 3,
            offset = args.len() + 4,
        ))?;
        let mut bound = args.clone();
        bound.push(query.trim().to_string());
        bound.push(fold);
        let rows = stmt
            .query_map(
                params_from_iter(
                    bound
                        .iter()
                        .map(|s| s as &dyn rusqlite::ToSql)
                        .chain([
                            &(limit as i64) as &dyn rusqlite::ToSql,
                            &(offset as i64) as &dyn rusqlite::ToSql,
                        ]),
                ),
                |r| {
                    Ok(RouteRow {
                        route_id: r.get(0)?,
                        short_name: r.get(1)?,
                        long_name: r.get(2)?,
                        route_type: r.get(3)?,
                        color: r.get(4)?,
                    })
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok((rows, total))
    }

    /// Route ids matching an exact id, short name, or long name.
    pub fn resolve_route_ids(&self, query: &str) -> Result<Vec<String>, AppError> {
        let query = query.trim();
        let fold = query.to_lowercase();
        if let Some(id) = self
            .conn
            .query_row(
                "SELECT route_id FROM routes WHERE route_id = ?1",
                params![query],
                |r| r.get::<_, String>(0),
            )
            .optional()?
        {
            return Ok(vec![id]);
        }
        let mut stmt = self
            .conn
            .prepare("SELECT route_id FROM routes WHERE short_fold = ?1 OR name_fold = ?1")?;
        let exact = stmt
            .query_map(params![fold], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        if !exact.is_empty() {
            return Ok(exact);
        }
        let mut stmt = self.conn.prepare(
            "SELECT route_id FROM routes WHERE short_fold LIKE ?1 ESCAPE '\\' OR name_fold LIKE ?1 ESCAPE '\\' LIMIT 8",
        )?;
        let fuzzy = stmt
            .query_map(params![like_contains(&fold)], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(fuzzy)
    }

    pub fn route(&self, route_id: &str) -> Result<Option<RouteRow>, AppError> {
        Ok(self
            .conn
            .query_row(
                "SELECT route_id, short_name, long_name, route_type, color FROM routes WHERE route_id = ?1",
                params![route_id],
                |r| {
                    Ok(RouteRow {
                        route_id: r.get(0)?,
                        short_name: r.get(1)?,
                        long_name: r.get(2)?,
                        route_type: r.get(3)?,
                        color: r.get(4)?,
                    })
                },
            )
            .optional()?)
    }

    pub fn trip_by_id(&self, trip_id: &str) -> Result<Option<TripRow>, AppError> {
        Ok(self
            .conn
            .query_row(
                "SELECT t.trip_id, t.route_id, t.service_id, t.headsign, t.short_name, t.direction_id,
                        r.short_name, r.long_name, r.route_type
                 FROM trips t LEFT JOIN routes r ON r.route_id = t.route_id
                 WHERE t.trip_id = ?1",
                params![trip_id],
                map_trip,
            )
            .optional()?)
    }

    pub fn trips_by_short_name(&self, short_name: &str) -> Result<Vec<TripRow>, AppError> {
        let mut stmt = self.conn.prepare(
            "SELECT t.trip_id, t.route_id, t.service_id, t.headsign, t.short_name, t.direction_id,
                    r.short_name, r.long_name, r.route_type
             FROM trips t LEFT JOIN routes r ON r.route_id = t.route_id
             WHERE t.short_fold = ?1
             ORDER BY t.trip_id",
        )?;
        let rows = stmt
            .query_map(params![short_name.to_lowercase()], map_trip)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn trip_route_id(&self, trip_id: &str) -> Result<Option<String>, AppError> {
        Ok(self
            .conn
            .query_row(
                "SELECT route_id FROM trips WHERE trip_id = ?1",
                params![trip_id],
                |r| r.get(0),
            )
            .optional()?)
    }

    pub fn expand_stop_ids(&self, stop_id: &str) -> Result<Vec<String>, AppError> {
        let mut ids = HashSet::new();
        ids.insert(stop_id.to_string());
        if let Some(stop) = self.stop_by_id(stop_id)? {
            if let Some(parent) = stop.parent_station {
                ids.insert(parent);
            }
        }
        let seeds: Vec<String> = ids.iter().cloned().collect();
        let placeholders = vec!["?"; seeds.len()].join(",");
        // Stations in a full GTFS graph also parent entrances and pathway nodes.
        // Boards and alerts only need the station, its platforms, and boarding areas.
        let mut stmt = self.conn.prepare(&format!(
            "SELECT stop_id FROM stops
             WHERE location_type IN (0, 1, 4)
               AND (stop_id IN ({placeholders}) OR parent_station IN ({placeholders}))"
        ))?;
        let mut bound: Vec<&dyn rusqlite::ToSql> = Vec::new();
        for id in seeds.iter().chain(seeds.iter()) {
            bound.push(id);
        }
        let extras = stmt
            .query_map(params_from_iter(bound.iter().copied()), |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        for id in extras {
            ids.insert(id);
        }
        let mut out: Vec<_> = ids.into_iter().collect();
        out.sort();
        Ok(out)
    }

    pub fn stops_for_ids(&self, ids: &[String]) -> Result<HashMap<String, StopRow>, AppError> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let placeholders = vec!["?"; ids.len()].join(",");
        let mut stmt = self.conn.prepare(&format!(
            "SELECT stop_id, stop_code, stop_name, lat, lon, location_type, parent_station, platform_code
             FROM stops WHERE stop_id IN ({placeholders})"
        ))?;
        let rows = stmt
            .query_map(params_from_iter(ids.iter()), map_stop)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows.into_iter().map(|s| (s.stop_id.clone(), s)).collect())
    }

    pub fn stop_times_for_stops(&self, stop_ids: &[String]) -> Result<Vec<StopTimeRow>, AppError> {
        if stop_ids.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders = vec!["?"; stop_ids.len()].join(",");
        let mut stmt = self.conn.prepare(&format!(
            "SELECT st.trip_id, st.stop_sequence, st.stop_id, s.stop_name, s.platform_code,
                    st.arrival_sec, st.departure_sec, st.pickup, st.drop_off, st.headsign,
                    t.service_id, t.headsign, t.short_name, t.direction_id, t.route_id,
                    r.short_name, r.long_name, r.route_type
             FROM stop_times st
             JOIN trips t ON t.trip_id = st.trip_id
             LEFT JOIN stops s ON s.stop_id = st.stop_id
             LEFT JOIN routes r ON r.route_id = t.route_id
             WHERE st.stop_id IN ({placeholders})"
        ))?;
        let rows = stmt
            .query_map(params_from_iter(stop_ids.iter()), map_stop_time)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn trip_stop_count(&self, trip_id: &str) -> Result<i64, AppError> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*) FROM stop_times WHERE trip_id = ?1",
            params![trip_id],
            |r| r.get(0),
        )?)
    }

    pub fn trip_stop_times(
        &self,
        trip_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<StopTimeRow>, AppError> {
        let mut stmt = self.conn.prepare(
            "SELECT st.trip_id, st.stop_sequence, st.stop_id, s.stop_name, s.platform_code,
                    st.arrival_sec, st.departure_sec, st.pickup, st.drop_off, st.headsign,
                    t.service_id, t.headsign, t.short_name, t.direction_id, t.route_id,
                    r.short_name, r.long_name, r.route_type
             FROM stop_times st
             JOIN trips t ON t.trip_id = st.trip_id
             LEFT JOIN stops s ON s.stop_id = st.stop_id
             LEFT JOIN routes r ON r.route_id = t.route_id
             WHERE st.trip_id = ?1
             ORDER BY st.stop_sequence
             LIMIT ?2 OFFSET ?3",
        )?;
        let rows = stmt
            .query_map(params![trip_id, limit as i64, offset as i64], map_stop_time)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn stop_has_times(&self, stop_id: &str) -> Result<bool, AppError> {
        Ok(self
            .conn
            .query_row(
                "SELECT 1 FROM stop_times WHERE stop_id = ?1 LIMIT 1",
                params![stop_id],
                |_| Ok(1i64),
            )
            .optional()?
            .is_some())
    }
}

fn choose_stop(db: &StaticDb, rows: Vec<StopRow>, query: &str) -> Result<StopRow, AppError> {
    if rows.is_empty() {
        return Err(err(format!("no stop matches '{query}'"))
            .hint("call search_stops to see identifiers"));
    }
    if rows.len() == 1 {
        return Ok(rows.into_iter().next().unwrap());
    }
    let fold = query.to_lowercase();
    let code_exact: Vec<_> = rows
        .iter()
        .filter(|s| s.stop_code.as_deref().is_some_and(|c| c.eq_ignore_ascii_case(query)))
        .cloned()
        .collect();
    let pool = if code_exact.is_empty() {
        let name_exact: Vec<_> = rows
            .iter()
            .filter(|s| {
                s.stop_name
                    .as_deref()
                    .is_some_and(|n| n.to_lowercase() == fold)
            })
            .cloned()
            .collect();
        if name_exact.is_empty() {
            rows
        } else {
            name_exact
        }
    } else {
        code_exact
    };
    if pool.len() == 1 {
        return Ok(pool.into_iter().next().unwrap());
    }
    let with_times: Vec<_> = pool
        .iter()
        .filter(|s| db.stop_has_times(&s.stop_id).unwrap_or(false))
        .cloned()
        .collect();
    if with_times.len() == 1 {
        return Ok(with_times.into_iter().next().unwrap());
    }
    let narrowed = if with_times.is_empty() {
        pool
    } else {
        with_times
    };
    let plain: Vec<_> = narrowed
        .iter()
        .filter(|s| s.location_type == 0 && s.platform_code.is_none())
        .cloned()
        .collect();
    if plain.len() == 1 {
        return Ok(plain.into_iter().next().unwrap());
    }
    let shown: Vec<_> = narrowed
        .iter()
        .take(8)
        .map(|s| {
            serde_json::json!({
                "stop_id": s.stop_id,
                "stop_name": s.stop_name,
                "location_type": s.location_type,
                "platform_code": s.platform_code,
            })
        })
        .collect();
    Err(err(format!("stop '{query}' matches {} places", narrowed.len()))
        .hint("pass an exact stop_id")
        .extra("candidates", serde_json::Value::Array(shown)))
}

fn map_stop(r: &rusqlite::Row<'_>) -> rusqlite::Result<StopRow> {
    Ok(StopRow {
        stop_id: r.get(0)?,
        stop_code: r.get(1)?,
        stop_name: r.get(2)?,
        lat: r.get(3)?,
        lon: r.get(4)?,
        location_type: r.get(5)?,
        parent_station: r.get(6)?,
        platform_code: r.get(7)?,
    })
}

fn map_trip(r: &rusqlite::Row<'_>) -> rusqlite::Result<TripRow> {
    Ok(TripRow {
        trip_id: r.get(0)?,
        route_id: r.get(1)?,
        service_id: r.get(2)?,
        headsign: r.get(3)?,
        short_name: r.get(4)?,
        direction_id: r.get(5)?,
        route_short_name: r.get(6)?,
        route_long_name: r.get(7)?,
        route_type: r.get::<_, Option<i64>>(8)?.unwrap_or(0),
    })
}

fn map_stop_time(r: &rusqlite::Row<'_>) -> rusqlite::Result<StopTimeRow> {
    Ok(StopTimeRow {
        trip_id: r.get(0)?,
        stop_sequence: r.get(1)?,
        stop_id: r.get(2)?,
        stop_name: r.get(3)?,
        platform_code: r.get(4)?,
        arrival_sec: r.get(5)?,
        departure_sec: r.get(6)?,
        pickup: r.get(7)?,
        drop_off: r.get(8)?,
        headsign: r.get(9)?,
        service_id: r.get(10)?,
        trip_headsign: r.get(11)?,
        trip_short_name: r.get(12)?,
        direction_id: r.get(13)?,
        route_id: r.get(14)?,
        route_short_name: r.get(15)?,
        route_long_name: r.get(16)?,
        route_type: r.get::<_, Option<i64>>(17)?.unwrap_or(0),
    })
}

fn count(conn: &Connection, table: &str) -> Result<i64, AppError> {
    Ok(conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))?)
}

fn like_contains(fold: &str) -> String {
    let escaped = fold
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    format!("%{escaped}%")
}

#[derive(Clone)]
struct Agency {
    id: Option<String>,
    name: Option<String>,
    timezone: Option<String>,
}

impl Default for Agency {
    fn default() -> Self {
        Self {
            id: None,
            name: None,
            timezone: None,
        }
    }
}

struct FeedInfo {
    version: Option<String>,
    publisher: Option<String>,
}

fn load_agencies(files: &HashMap<String, Vec<u8>>) -> Vec<Agency> {
    let Some(bytes) = files.get("agency.txt") else {
        return Vec::new();
    };
    let mut reader = csv_reader(bytes);
    let Ok(headers) = reader.headers() else {
        return Vec::new();
    };
    let headers = headers.clone();
    let idx = header_index(&headers);
    reader
        .records()
        .flatten()
        .map(|record| Agency {
            id: nonempty(col(&record, &idx, "agency_id")),
            name: nonempty(col(&record, &idx, "agency_name")),
            timezone: nonempty(col(&record, &idx, "agency_timezone")),
        })
        .filter(|agency| agency.name.is_some() || agency.timezone.is_some())
        .collect()
}

fn choose_agency(conn: &Connection, agencies: &[Agency]) -> Result<Agency, AppError> {
    if agencies.is_empty() {
        return Ok(Agency::default());
    }
    let dominant: Option<String> = conn
        .query_row(
            "SELECT agency_id FROM routes
             WHERE agency_id IS NOT NULL AND agency_id != ''
             GROUP BY agency_id
             ORDER BY COUNT(*) DESC, agency_id
             LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(id) = dominant {
        if let Some(agency) = agencies.iter().find(|agency| agency.id.as_deref() == Some(id.as_str())) {
            return Ok(agency.clone());
        }
    }
    Ok(agencies[0].clone())
}

fn load_feed_info(files: &HashMap<String, Vec<u8>>) -> FeedInfo {
    let Some(bytes) = files.get("feed_info.txt") else {
        return FeedInfo {
            version: None,
            publisher: None,
        };
    };
    let mut reader = csv_reader(bytes);
    let Ok(headers) = reader.headers() else {
        return FeedInfo {
            version: None,
            publisher: None,
        };
    };
    let idx = header_index(headers);
    let mut info = FeedInfo {
        version: None,
        publisher: None,
    };
    if let Some(Ok(record)) = reader.records().next() {
        info.version = nonempty(col(&record, &idx, "feed_version"));
        info.publisher = nonempty(col(&record, &idx, "feed_publisher_name"));
    }
    info
}

fn load_stops(conn: &Connection, bytes: Option<&Vec<u8>>) -> Result<(), AppError> {
    let Some(bytes) = bytes else {
        return Ok(());
    };
    let mut reader = csv_reader(bytes);
    let headers = reader.headers()?.clone();
    let idx = header_index(&headers);
    let tx = conn.unchecked_transaction()?;
    {
        let mut stmt = tx.prepare(
            "INSERT OR REPLACE INTO stops
             (stop_id, stop_code, stop_name, name_fold, code_fold, lat, lon, location_type, parent_station, platform_code)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        )?;
        for record in reader.records() {
            let record = record?;
            let stop_id = col(&record, &idx, "stop_id");
            if stop_id.is_empty() {
                continue;
            }
            let name = nonempty(col(&record, &idx, "stop_name"));
            let code = nonempty(col(&record, &idx, "stop_code"));
            stmt.execute(params![
                stop_id,
                code.clone(),
                name.clone(),
                name.as_deref().map(|s| s.to_lowercase()),
                code.as_deref().map(|s| s.to_lowercase()),
                parse_f64(col(&record, &idx, "stop_lat")),
                parse_f64(col(&record, &idx, "stop_lon")),
                parse_i64(col(&record, &idx, "location_type")).unwrap_or(0),
                nonempty(col(&record, &idx, "parent_station")),
                nonempty(col(&record, &idx, "platform_code")),
            ])?;
        }
    }
    tx.commit()?;
    Ok(())
}

fn load_routes(conn: &Connection, bytes: Option<&Vec<u8>>) -> Result<(), AppError> {
    let Some(bytes) = bytes else {
        return Ok(());
    };
    let mut reader = csv_reader(bytes);
    let headers = reader.headers()?.clone();
    let idx = header_index(&headers);
    let tx = conn.unchecked_transaction()?;
    {
        let mut stmt = tx.prepare(
            "INSERT OR REPLACE INTO routes
             (route_id, agency_id, short_name, long_name, name_fold, short_fold, route_type, color)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )?;
        for record in reader.records() {
            let record = record?;
            let route_id = col(&record, &idx, "route_id");
            if route_id.is_empty() {
                continue;
            }
            let short = nonempty(col(&record, &idx, "route_short_name"));
            let long = nonempty(col(&record, &idx, "route_long_name"));
            let fold = match (&short, &long) {
                (Some(s), Some(l)) => Some(format!("{s} {l}").to_lowercase()),
                (Some(s), None) => Some(s.to_lowercase()),
                (None, Some(l)) => Some(l.to_lowercase()),
                _ => None,
            };
            stmt.execute(params![
                route_id,
                nonempty(col(&record, &idx, "agency_id")),
                short.clone(),
                long,
                fold,
                short.as_deref().map(|s| s.to_lowercase()),
                parse_i64(col(&record, &idx, "route_type")).unwrap_or(0),
                nonempty(col(&record, &idx, "route_color")),
            ])?;
        }
    }
    tx.commit()?;
    Ok(())
}

fn load_trips(conn: &Connection, bytes: Option<&Vec<u8>>) -> Result<(), AppError> {
    let Some(bytes) = bytes else {
        return Ok(());
    };
    let mut reader = csv_reader(bytes);
    let headers = reader.headers()?.clone();
    let idx = header_index(&headers);
    let tx = conn.unchecked_transaction()?;
    {
        let mut stmt = tx.prepare(
            "INSERT OR REPLACE INTO trips
             (trip_id, route_id, service_id, headsign, short_name, short_fold, direction_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )?;
        for record in reader.records() {
            let record = record?;
            let trip_id = col(&record, &idx, "trip_id");
            if trip_id.is_empty() {
                continue;
            }
            let short = nonempty(col(&record, &idx, "trip_short_name"));
            stmt.execute(params![
                trip_id,
                col(&record, &idx, "route_id"),
                col(&record, &idx, "service_id"),
                nonempty(col(&record, &idx, "trip_headsign")),
                short.clone(),
                short.as_deref().map(|s| s.to_lowercase()),
                parse_i64(col(&record, &idx, "direction_id")),
            ])?;
        }
    }
    tx.commit()?;
    Ok(())
}

fn load_stop_times(conn: &Connection, bytes: Option<&Vec<u8>>) -> Result<(), AppError> {
    let Some(bytes) = bytes else {
        return Ok(());
    };
    let mut reader = csv_reader(bytes);
    let headers = reader.headers()?.clone();
    let idx = header_index(&headers);
    let tx = conn.unchecked_transaction()?;
    {
        let mut stmt = tx.prepare(
            "INSERT OR REPLACE INTO stop_times
             (trip_id, stop_sequence, stop_id, arrival_sec, departure_sec, pickup, drop_off, headsign)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )?;
        for record in reader.records() {
            let record = record?;
            let trip_id = col(&record, &idx, "trip_id");
            let stop_id = col(&record, &idx, "stop_id");
            let Some(seq) = parse_i64(col(&record, &idx, "stop_sequence")) else {
                continue;
            };
            if trip_id.is_empty() || stop_id.is_empty() {
                continue;
            }
            stmt.execute(params![
                trip_id,
                seq,
                stop_id,
                parse_gtfs_time(col(&record, &idx, "arrival_time")),
                parse_gtfs_time(col(&record, &idx, "departure_time")),
                parse_i64(col(&record, &idx, "pickup_type")),
                parse_i64(col(&record, &idx, "drop_off_type")),
                nonempty(col(&record, &idx, "stop_headsign")),
            ])?;
        }
    }
    tx.commit()?;
    Ok(())
}

fn load_services(
    files: &HashMap<String, Vec<u8>>,
) -> Result<
    (
        HashMap<String, CalendarRow>,
        HashMap<String, HashSet<String>>,
        HashMap<String, HashSet<String>>,
    ),
    AppError,
> {
    let mut calendar = HashMap::new();
    if let Some(bytes) = files.get("calendar.txt") {
        let mut reader = csv_reader(bytes);
        let headers = reader.headers()?.clone();
        let idx = header_index(&headers);
        for record in reader.records() {
            let record = record?;
            let service_id = col(&record, &idx, "service_id");
            if service_id.is_empty() {
                continue;
            }
            calendar.insert(
                service_id.to_string(),
                CalendarRow {
                    monday: flag(col(&record, &idx, "monday")),
                    tuesday: flag(col(&record, &idx, "tuesday")),
                    wednesday: flag(col(&record, &idx, "wednesday")),
                    thursday: flag(col(&record, &idx, "thursday")),
                    friday: flag(col(&record, &idx, "friday")),
                    saturday: flag(col(&record, &idx, "saturday")),
                    sunday: flag(col(&record, &idx, "sunday")),
                    start_date: col(&record, &idx, "start_date").to_string(),
                    end_date: col(&record, &idx, "end_date").to_string(),
                },
            );
        }
    }
    let mut added: HashMap<String, HashSet<String>> = HashMap::new();
    let mut removed: HashMap<String, HashSet<String>> = HashMap::new();
    if let Some(bytes) = files.get("calendar_dates.txt") {
        let mut reader = csv_reader(bytes);
        let headers = reader.headers()?.clone();
        let idx = header_index(&headers);
        for record in reader.records() {
            let record = record?;
            let service_id = col(&record, &idx, "service_id");
            let date = col(&record, &idx, "date");
            if service_id.is_empty() || date.is_empty() {
                continue;
            }
            match col(&record, &idx, "exception_type") {
                "1" => {
                    added
                        .entry(service_id.to_string())
                        .or_default()
                        .insert(date.to_string());
                }
                "2" => {
                    removed
                        .entry(service_id.to_string())
                        .or_default()
                        .insert(date.to_string());
                }
                _ => {}
            }
        }
    }
    Ok((calendar, added, removed))
}

fn read_zip(bytes: &[u8]) -> Result<HashMap<String, Vec<u8>>, AppError> {
    if bytes.len() >= 2 && bytes[0] == 0x1f && bytes[1] == 0x8b {
        let mut decoder = flate2::read::GzDecoder::new(bytes);
        let mut inflated = Vec::new();
        decoder.read_to_end(&mut inflated)?;
        return read_zip(&inflated);
    }
    let mut archive = ZipArchive::new(Cursor::new(bytes))?;
    let mut best: HashMap<String, (usize, Vec<u8>)> = HashMap::new();
    for index in 0..archive.len() {
        let mut file = archive.by_index(index)?;
        if !file.is_file() {
            continue;
        }
        let name = file.name().to_string();
        if name.contains("..") {
            continue;
        }
        let Some(key) = canonical_gtfs_name(&name) else {
            continue;
        };
        let depth = name.matches(['/', '\\']).count();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        match best.get(key) {
            Some((existing, _)) if *existing <= depth => {}
            _ => {
                best.insert(key.to_string(), (depth, buf));
            }
        }
    }
    Ok(best.into_iter().map(|(k, (_, v))| (k, v)).collect())
}

fn canonical_gtfs_name(path: &str) -> Option<&'static str> {
    let base = path.rsplit(['/', '\\']).next().unwrap_or(path);
    match base.to_ascii_lowercase().as_str() {
        "agency.txt" => Some("agency.txt"),
        "stops.txt" => Some("stops.txt"),
        "routes.txt" => Some("routes.txt"),
        "trips.txt" => Some("trips.txt"),
        "stop_times.txt" => Some("stop_times.txt"),
        "calendar.txt" => Some("calendar.txt"),
        "calendar_dates.txt" => Some("calendar_dates.txt"),
        "feed_info.txt" => Some("feed_info.txt"),
        _ => None,
    }
}

fn csv_reader(bytes: &[u8]) -> csv::Reader<&[u8]> {
    let bytes = bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(bytes);
    csv::ReaderBuilder::new()
        .flexible(true)
        .trim(csv::Trim::None)
        .from_reader(bytes)
}

fn header_index(headers: &csv::StringRecord) -> HashMap<String, usize> {
    headers
        .iter()
        .enumerate()
        .map(|(i, h)| (h.trim().trim_start_matches('\u{feff}').to_ascii_lowercase(), i))
        .collect()
}

fn col<'a>(record: &'a csv::StringRecord, idx: &HashMap<String, usize>, name: &str) -> &'a str {
    idx.get(name)
        .and_then(|i| record.get(*i))
        .map(str::trim)
        .unwrap_or("")
}

fn nonempty(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn parse_f64(value: &str) -> Option<f64> {
    if value.is_empty() {
        None
    } else {
        value.parse().ok()
    }
}

fn parse_i64(value: &str) -> Option<i64> {
    if value.is_empty() {
        None
    } else {
        value.parse().ok()
    }
}

fn flag(value: &str) -> bool {
    value == "1"
}

pub fn parse_gtfs_time(value: &str) -> Option<i64> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    let mut parts = value.split(':');
    let hours: i64 = parts.next()?.parse().ok()?;
    let minutes: i64 = parts.next()?.parse().ok()?;
    let seconds: i64 = parts.next()?.parse().ok()?;
    if parts.next().is_some() || !(0..=59).contains(&minutes) || !(0..=59).contains(&seconds) {
        return None;
    }
    Some(hours * 3600 + minutes * 60 + seconds)
}

pub fn route_type_name(route_type: i64) -> &'static str {
    match route_type {
        0 => "tram",
        1 => "subway",
        2 => "rail",
        3 => "bus",
        4 => "ferry",
        5 => "cable_tram",
        6 => "aerial",
        7 => "funicular",
        11 => "trolleybus",
        12 => "monorail",
        _ => "other",
    }
}
