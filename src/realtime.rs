use std::io::Read;

use prost::Message;
use serde_json::{json, Map, Value};

use crate::error::{err, AppError};
use crate::gtfs_rt::feed_header::Incrementality;
use crate::gtfs_rt::trip_descriptor::ScheduleRelationship as TripRel;
use crate::gtfs_rt::trip_update::stop_time_update::ScheduleRelationship as StopRel;
use crate::gtfs_rt::vehicle_position::VehicleStopStatus;
use crate::gtfs_rt::{
    FeedMessage, TranslatedString, TripUpdate, VehicleDescriptor, alert, vehicle_position,
};

pub fn maybe_gunzip(bytes: &[u8]) -> Result<Vec<u8>, AppError> {
    if bytes.len() >= 2 && bytes[0] == 0x1f && bytes[1] == 0x8b {
        let mut decoder = flate2::read::GzDecoder::new(bytes);
        let mut out = Vec::new();
        decoder.read_to_end(&mut out)?;
        Ok(out)
    } else {
        Ok(bytes.to_vec())
    }
}

pub fn decode_feed(bytes: &[u8]) -> Result<FeedMessage, AppError> {
    let raw = maybe_gunzip(bytes)?;
    if raw.is_empty() {
        return Err(err("protobuf payload is empty"));
    }
    Ok(FeedMessage::decode(raw.as_slice())?)
}

pub fn delay_label(sec: i32) -> String {
    if sec.abs() < 30 {
        return "on time".into();
    }
    let mins = (sec.abs() as f64 / 60.0).round() as i32;
    if mins == 0 {
        return if sec > 0 {
            format!("{sec} sec late")
        } else {
            format!("{} sec early", -sec)
        };
    }
    if sec > 0 {
        format!("{mins} min late")
    } else {
        format!("{mins} min early")
    }
}

#[derive(Debug, Clone)]
pub struct Prediction {
    pub estimated: Option<i64>,
    pub delay_sec: Option<i32>,
}

pub fn predict(scheduled: Option<i64>, event: Option<&crate::gtfs_rt::trip_update::StopTimeEvent>) -> Prediction {
    let Some(event) = event else {
        return Prediction {
            estimated: scheduled,
            delay_sec: None,
        };
    };
    if let Some(time) = event.time {
        let delay = scheduled
            .map(|sched| saturating_i32(time - sched))
            .or(event.delay);
        return Prediction {
            estimated: Some(time),
            delay_sec: delay,
        };
    }
    if let Some(delay) = event.delay {
        return Prediction {
            estimated: scheduled.map(|sched| sched + i64::from(delay)),
            delay_sec: Some(delay),
        };
    }
    Prediction {
        estimated: scheduled,
        delay_sec: None,
    }
}

fn saturating_i32(value: i64) -> i32 {
    i32::try_from(value).unwrap_or(if value > 0 { i32::MAX } else { i32::MIN })
}

pub fn trip_relationship(value: Option<i32>) -> Option<String> {
    enum_label::<TripRel>(value)
}

pub fn stop_relationship(value: Option<i32>) -> Option<String> {
    enum_label::<StopRel>(value)
}

pub fn vehicle_status(value: Option<i32>, has_sequence: bool) -> Option<String> {
    let value = value.or(if has_sequence {
        Some(VehicleStopStatus::InTransitTo as i32)
    } else {
        None
    });
    enum_label::<VehicleStopStatus>(value)
}

pub fn incrementality(value: Option<i32>) -> Option<String> {
    enum_label::<Incrementality>(value.or(Some(Incrementality::FullDataset as i32)))
}

pub fn cause_name(value: Option<i32>) -> Option<String> {
    enum_label::<alert::Cause>(value)
}

pub fn effect_name(value: Option<i32>) -> Option<String> {
    enum_label::<alert::Effect>(value)
}

pub fn severity_name(value: Option<i32>) -> Option<String> {
    enum_label::<alert::SeverityLevel>(value)
}

pub fn occupancy_name(value: Option<i32>) -> Option<String> {
    enum_label::<vehicle_position::OccupancyStatus>(value)
}

fn enum_label<E>(value: Option<i32>) -> Option<String>
where
    E: TryFrom<i32> + Copy,
    E: Label,
{
    value
        .and_then(|v| E::try_from(v).ok())
        .map(|v| v.label().to_string())
}

trait Label {
    fn label(self) -> &'static str;
}

macro_rules! label_impl {
    ($ty:path) => {
        impl Label for $ty {
            fn label(self) -> &'static str {
                self.as_str_name()
            }
        }
    };
}

label_impl!(TripRel);
label_impl!(StopRel);
label_impl!(VehicleStopStatus);
label_impl!(Incrementality);
label_impl!(alert::Cause);
label_impl!(alert::Effect);
label_impl!(alert::SeverityLevel);
label_impl!(vehicle_position::OccupancyStatus);

pub fn translated(text: Option<&TranslatedString>, max_chars: usize) -> Option<(String, bool)> {
    let text = text?;
    let chosen = text
        .translation
        .iter()
        .find(|t| {
            t.language
                .as_deref()
                .is_some_and(|lang| lang.eq_ignore_ascii_case("en") || lang.to_ascii_lowercase().starts_with("en-"))
        })
        .or_else(|| text.translation.iter().find(|t| t.language.is_none()))
        .or(text.translation.first())?;
    Some(clip(&chosen.text, max_chars))
}

pub fn clip(text: &str, max_chars: usize) -> (String, bool) {
    let count = text.chars().count();
    if count <= max_chars {
        (text.to_string(), false)
    } else {
        let clipped: String = text.chars().take(max_chars).collect();
        (clipped, true)
    }
}

pub fn vehicle_json(vehicle: Option<&VehicleDescriptor>) -> Value {
    let Some(vehicle) = vehicle else {
        return Value::Null;
    };
    obj([
        ("id", json_opt(vehicle.id.clone())),
        ("label", json_opt(vehicle.label.clone())),
        ("license_plate", json_opt(vehicle.license_plate.clone())),
    ])
}

pub fn obj(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    let mut map = Map::new();
    for (key, value) in pairs {
        if !value.is_null() {
            map.insert(key.to_string(), value);
        }
    }
    Value::Object(map)
}

pub fn json_opt(value: Option<impl Into<String>>) -> Value {
    value.map(|v| Value::String(v.into())).unwrap_or(Value::Null)
}

pub fn json_i64(value: Option<i64>) -> Value {
    value.map(|v| json!(v)).unwrap_or(Value::Null)
}

pub fn json_i32(value: Option<i32>) -> Value {
    value.map(|v| json!(v)).unwrap_or(Value::Null)
}

pub fn json_f64(value: Option<f64>) -> Value {
    value.map(|v| json!(v)).unwrap_or(Value::Null)
}

pub fn round_coord(value: f32) -> f64 {
    (f64::from(value) * 100_000.0).round() / 100_000.0
}

pub fn is_canceled(relationship: Option<i32>) -> bool {
    matches!(
        relationship,
        Some(x) if x == TripRel::Canceled as i32 || x == TripRel::Deleted as i32
    )
}

pub fn find_stop_update<'a>(
    update: &'a TripUpdate,
    stop_sequence: u32,
    stop_id: &str,
) -> Option<&'a crate::gtfs_rt::trip_update::StopTimeUpdate> {
    update
        .stop_time_update
        .iter()
        .find(|row| row.stop_sequence == Some(stop_sequence))
        .or_else(|| {
            update
                .stop_time_update
                .iter()
                .find(|row| row.stop_id.as_deref() == Some(stop_id))
        })
}

pub fn previous_stop_update<'a>(
    update: &'a TripUpdate,
    stop_sequence: u32,
) -> Option<&'a crate::gtfs_rt::trip_update::StopTimeUpdate> {
    update
        .stop_time_update
        .iter()
        .filter(|row| row.stop_sequence.is_some_and(|seq| seq < stop_sequence))
        .max_by_key(|row| row.stop_sequence.unwrap_or(0))
}

pub fn propagated_delay(update: &TripUpdate, stop_sequence: u32) -> Option<i32> {
    if let Some(previous) = previous_stop_update(update, stop_sequence) {
        if previous.schedule_relationship == Some(StopRel::NoData as i32) {
            return None;
        }
        if let Some(delay) = previous
            .departure
            .as_ref()
            .and_then(|event| event.delay)
            .or_else(|| previous.arrival.as_ref().and_then(|event| event.delay))
        {
            return Some(delay);
        }
    }
    update.delay
}

#[allow(deprecated)]
pub fn alert_periods(alert: &crate::gtfs_rt::Alert) -> Vec<&crate::gtfs_rt::TimeRange> {
    if !alert.impact_period.is_empty() {
        alert.impact_period.iter().collect()
    } else {
        alert.active_period.iter().collect()
    }
}

pub fn period_contains(range: &crate::gtfs_rt::TimeRange, at: i64) -> bool {
    let at = u64::try_from(at).unwrap_or(0);
    let starts = range.start.is_none_or(|start| at >= start);
    let ends = range.end.is_none_or(|end| at < end);
    starts && ends
}

