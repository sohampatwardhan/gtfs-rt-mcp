//! Startup feed configuration from the process environment and CLI flags.
//!
//! Header values are parsed into [`RegisterRequest`] and are never copied into error text.

use std::collections::{BTreeMap, HashMap};

use serde_json::{json, Value};

use crate::catalog::{validate_startup_request, RegisterRequest};

const MAX_INDEX: u32 = 32;

#[derive(Debug, Clone, Default)]
pub struct StartupOverrides {
    pub feed_id: Option<String>,
    pub feed_set: Option<String>,
    pub static_gtfs: Option<String>,
    pub trip_updates: Option<String>,
    pub vehicle_positions: Option<String>,
    pub alerts: Option<String>,
    pub rt_ttl_secs: Option<u64>,
    pub static_ttl_secs: Option<u64>,
    pub http_headers: Option<String>,
}

#[derive(Debug, Default)]
pub struct StartupPlan {
    pub requests: Vec<RegisterRequest>,
    pub failures: Vec<StartupFailure>,
    pub errors: Vec<Value>,
}

#[derive(Debug)]
pub struct StartupFailure {
    pub request: RegisterRequest,
    pub error: String,
    pub hint: Option<String>,
}

pub fn startup_plan(
    vars: impl IntoIterator<Item = (String, String)>,
    overrides: &StartupOverrides,
) -> StartupPlan {
    let vars: HashMap<String, String> = vars
        .into_iter()
        .map(|(key, value)| (key, value.trim().to_string()))
        .filter(|(_, value)| !value.is_empty())
        .collect();

    let mut plan = StartupPlan::default();
    let numbered = numbered_drafts(&vars, &mut plan.errors);
    let json_feeds = json_drafts(vars.get("GTFS_RT_MCP_FEEDS"), &mut plan.errors);
    if !numbered.is_empty() && json_feeds.is_some() {
        plan.errors.push(error_value(
            None,
            Some("GTFS_RT_MCP_FEEDS".to_string()),
            "set either GTFS_RT_MCP_FEEDS or GTFS_RT_MCP_FEED_<n>_* variables, not both",
            Some("remove one of the two multi-feed styles".to_string()),
        ));
    }

    let mut drafts = Vec::new();
    if let Some(draft) = default_draft(&vars, overrides) {
        drafts.push(draft);
    }
    if json_feeds.is_none() {
        drafts.extend(numbered);
    } else if numbered.is_empty() {
        if let Some(feeds) = json_feeds {
            drafts.extend(feeds);
        }
    }

    let mut seen: BTreeMap<String, String> = BTreeMap::new();
    for draft in drafts {
        match finish_draft(draft) {
            Ok(request) => {
                let id = request.feed_id.clone().unwrap_or_else(|| "default".into());
                if let Some(previous) = seen.insert(id.clone(), id.clone()) {
                    plan.requests
                        .retain(|existing| existing.feed_id.as_deref() != Some(previous.as_str()));
                    plan.failures.retain(|failure| {
                        failure.request.feed_id.as_deref() != Some(previous.as_str())
                    });
                    plan.errors.push(error_value(
                        Some(previous.clone()),
                        None,
                        format!("duplicate feed id '{previous}'"),
                        Some(
                            "give each feed its own GTFS_RT_MCP_FEED_ID or GTFS_RT_MCP_FEED_<n>_ID"
                                .to_string(),
                        ),
                    ));
                } else {
                    plan.requests.push(request);
                }
            }
            Err(failure) => {
                let duplicate = failure
                    .request
                    .feed_id
                    .as_ref()
                    .is_some_and(|id| seen.insert(id.clone(), id.clone()).is_some());
                if let Some(id) = failure.request.feed_id.clone().filter(|_| duplicate) {
                    plan.requests
                        .retain(|existing| existing.feed_id.as_deref() != Some(id.as_str()));
                    plan.failures.retain(|existing| {
                        existing.request.feed_id.as_deref() != Some(id.as_str())
                    });
                    plan.errors.push(error_value(
                        Some(id.clone()),
                        None,
                        format!("duplicate feed id '{id}'"),
                        Some("give each feed its own id".to_string()),
                    ));
                } else {
                    plan.failures.push(failure);
                }
            }
        }
    }
    plan
}

struct Draft {
    label: String,
    id: Option<String>,
    feed_set: Option<String>,
    static_gtfs: Option<String>,
    trip_updates: Option<String>,
    vehicle_positions: Option<String>,
    alerts: Option<String>,
    rt_ttl: Option<Result<u64, String>>,
    static_ttl: Option<Result<u64, String>>,
    headers: Result<BTreeMap<String, String>, String>,
}

fn default_draft(vars: &HashMap<String, String>, overrides: &StartupOverrides) -> Option<Draft> {
    let feed_set = overrides
        .feed_set
        .clone()
        .or_else(|| vars.get("GTFS_RT_MCP_FEED_SET").cloned());
    let static_gtfs = overrides
        .static_gtfs
        .clone()
        .or_else(|| vars.get("GTFS_RT_MCP_STATIC").cloned());
    let trip_updates = overrides
        .trip_updates
        .clone()
        .or_else(|| vars.get("GTFS_RT_MCP_TRIP_UPDATES").cloned());
    let vehicle_positions = overrides
        .vehicle_positions
        .clone()
        .or_else(|| vars.get("GTFS_RT_MCP_VEHICLE_POSITIONS").cloned());
    let alerts = overrides
        .alerts
        .clone()
        .or_else(|| vars.get("GTFS_RT_MCP_ALERTS").cloned());
    let id = overrides
        .feed_id
        .clone()
        .or_else(|| vars.get("GTFS_RT_MCP_FEED_ID").cloned());
    let headers_raw = overrides
        .http_headers
        .clone()
        .or_else(|| vars.get("GTFS_RT_MCP_HTTP_HEADERS").cloned());
    let rt_ttl = overrides
        .rt_ttl_secs
        .map(Ok)
        .or_else(|| vars.get("GTFS_RT_MCP_RT_TTL").map(|value| parse_ttl(value)));
    let static_ttl = overrides.static_ttl_secs.map(Ok).or_else(|| {
        vars.get("GTFS_RT_MCP_STATIC_TTL")
            .map(|value| parse_ttl(value))
    });
    let any = feed_set.is_some()
        || static_gtfs.is_some()
        || trip_updates.is_some()
        || vehicle_positions.is_some()
        || alerts.is_some()
        || headers_raw.is_some()
        || rt_ttl.is_some()
        || static_ttl.is_some()
        || id.is_some();
    if !any {
        return None;
    }
    Some(Draft {
        label: "default feed".into(),
        id,
        feed_set,
        static_gtfs,
        trip_updates,
        vehicle_positions,
        alerts,
        rt_ttl,
        static_ttl,
        headers: headers_raw
            .map(|raw| parse_headers(&raw, "GTFS_RT_MCP_HTTP_HEADERS"))
            .unwrap_or(Ok(BTreeMap::new())),
    })
}

fn numbered_drafts(vars: &HashMap<String, String>, errors: &mut Vec<Value>) -> Vec<Draft> {
    const FIELDS: &[&str] = &[
        "ID",
        "NAME",
        "FEED_SET",
        "STATIC",
        "TRIP_UPDATES",
        "VEHICLE_POSITIONS",
        "ALERTS",
        "RT_TTL",
        "STATIC_TTL",
        "HTTP_HEADERS",
    ];
    let mut grouped: BTreeMap<u32, BTreeMap<String, String>> = BTreeMap::new();
    for (key, value) in vars {
        let Some(rest) = key.strip_prefix("GTFS_RT_MCP_FEED_") else {
            continue;
        };
        let Some((index_text, field)) = rest.split_once('_') else {
            if rest.chars().all(|ch| ch.is_ascii_digit()) {
                errors.push(error_value(
                    None,
                    Some(key.clone()),
                    format!("{key} needs a field suffix such as _STATIC or _FEED_SET"),
                    None,
                ));
            }
            continue;
        };
        let Ok(index) = index_text.parse::<u32>() else {
            continue;
        };
        if !FIELDS.contains(&field) {
            errors.push(error_value(
                None,
                Some(key.clone()),
                format!("unknown feed setting {key}"),
                Some(format!(
                    "expected one of {}",
                    FIELDS
                        .iter()
                        .map(|field| format!("_{field}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )),
            ));
            continue;
        }
        if index == 0 || index > MAX_INDEX {
            errors.push(error_value(
                None,
                Some(key.clone()),
                format!("{key} uses index {index}"),
                Some("numbered feeds start at GTFS_RT_MCP_FEED_1 and stop at 32".to_string()),
            ));
            continue;
        }
        grouped
            .entry(index)
            .or_default()
            .insert(field.to_string(), value.clone());
    }
    if grouped.is_empty() {
        return Vec::new();
    }
    let max = *grouped.keys().next_back().unwrap_or(&0);
    for index in 1..=max {
        if !grouped.contains_key(&index) {
            errors.push(error_value(
                None,
                Some(format!("GTFS_RT_MCP_FEED_{index}_STATIC")),
                format!("numbered feeds skip {index}"),
                Some("use 1, 2, 3 with no gaps".to_string()),
            ));
        }
    }
    grouped
        .into_iter()
        .map(|(index, fields)| {
            let id = fields
                .get("ID")
                .cloned()
                .or_else(|| fields.get("NAME").cloned());
            if fields
                .get("ID")
                .zip(fields.get("NAME"))
                .is_some_and(|(id, name)| id != name)
            {
                errors.push(error_value(
                    id.clone(),
                    Some(format!("GTFS_RT_MCP_FEED_{index}_ID")),
                    format!("GTFS_RT_MCP_FEED_{index}_ID and _NAME disagree"),
                    Some("set only one of ID or NAME".to_string()),
                ));
            }
            Draft {
                label: format!("feed {index}"),
                id: id.or_else(|| Some(format!("feed{index}"))),
                feed_set: fields.get("FEED_SET").cloned(),
                static_gtfs: fields.get("STATIC").cloned(),
                trip_updates: fields.get("TRIP_UPDATES").cloned(),
                vehicle_positions: fields.get("VEHICLE_POSITIONS").cloned(),
                alerts: fields.get("ALERTS").cloned(),
                rt_ttl: fields.get("RT_TTL").map(|value| parse_ttl(value)),
                static_ttl: fields.get("STATIC_TTL").map(|value| parse_ttl(value)),
                headers: fields
                    .get("HTTP_HEADERS")
                    .map(|raw| {
                        parse_headers(raw, &format!("GTFS_RT_MCP_FEED_{index}_HTTP_HEADERS"))
                    })
                    .unwrap_or(Ok(BTreeMap::new())),
            }
        })
        .collect()
}

fn json_drafts(raw: Option<&String>, errors: &mut Vec<Value>) -> Option<Vec<Draft>> {
    let raw = raw?;
    let value: Value = match serde_json::from_str(raw) {
        Ok(value) => value,
        Err(error) => {
            errors.push(error_value(
                None,
                Some("GTFS_RT_MCP_FEEDS".into()),
                format!("GTFS_RT_MCP_FEEDS is not valid JSON ({error})"),
                Some("use a JSON array of feed objects".to_string()),
            ));
            return Some(Vec::new());
        }
    };
    let Some(items) = value.as_array() else {
        errors.push(error_value(
            None,
            Some("GTFS_RT_MCP_FEEDS".into()),
            "GTFS_RT_MCP_FEEDS must be a JSON array",
            Some(
                r#"example: [{"id":"mbta","static":"https://cdn.mbta.com/MBTA_GTFS.zip"}]"#
                    .to_string(),
            ),
        ));
        return Some(Vec::new());
    };
    if items.is_empty() {
        errors.push(error_value(
            None,
            Some("GTFS_RT_MCP_FEEDS".into()),
            "GTFS_RT_MCP_FEEDS is an empty array",
            None,
        ));
        return Some(Vec::new());
    }
    let mut drafts = Vec::new();
    for (offset, item) in items.iter().enumerate() {
        let index = offset + 1;
        let Some(object) = item.as_object() else {
            errors.push(error_value(
                None,
                Some("GTFS_RT_MCP_FEEDS".into()),
                format!("GTFS_RT_MCP_FEEDS[{offset}] is not an object"),
                None,
            ));
            continue;
        };
        let id = json_string(object, &["id", "name", "feed_id"]);
        drafts.push(Draft {
            label: format!("GTFS_RT_MCP_FEEDS[{offset}]"),
            id: id.or_else(|| Some(format!("feed{index}"))),
            feed_set: json_string(object, &["feed_set", "feedSet"]),
            static_gtfs: json_string(object, &["static", "static_url", "static_path"]),
            trip_updates: json_string(object, &["trip_updates", "tripUpdates"]),
            vehicle_positions: json_string(object, &["vehicle_positions", "vehiclePositions"]),
            alerts: json_string(object, &["alerts"]),
            rt_ttl: json_ttl(object, &["rt_ttl_secs", "rt_ttl"]),
            static_ttl: json_ttl(object, &["static_ttl_secs", "static_ttl"]),
            headers: json_headers(object),
        });
    }
    Some(drafts)
}

fn finish_draft(draft: Draft) -> Result<RegisterRequest, StartupFailure> {
    let mut problems = Vec::new();
    let headers = match draft.headers {
        Ok(headers) => headers,
        Err(error) => {
            problems.push(error);
            BTreeMap::new()
        }
    };
    let rt_ttl_secs = match draft.rt_ttl {
        Some(Ok(value)) => Some(value),
        Some(Err(error)) => {
            problems.push(format!("realtime TTL: {error}"));
            None
        }
        None => None,
    };
    let static_ttl_secs = match draft.static_ttl {
        Some(Ok(value)) => Some(value),
        Some(Err(error)) => {
            problems.push(format!("static TTL: {error}"));
            None
        }
        None => None,
    };
    let (static_url, static_path) = split_loc(draft.static_gtfs);
    let (trip_updates_url, trip_updates_path) = split_loc(draft.trip_updates);
    let (vehicle_positions_url, vehicle_positions_path) = split_loc(draft.vehicle_positions);
    let (alerts_url, alerts_path) = split_loc(draft.alerts);
    let request = RegisterRequest {
        feed_id: draft.id.clone(),
        feed_set: draft.feed_set,
        static_url,
        static_path,
        trip_updates_url,
        trip_updates_path,
        vehicle_positions_url,
        vehicle_positions_path,
        alerts_url,
        alerts_path,
        rt_ttl_secs,
        static_ttl_secs,
        headers,
    };
    if !problems.is_empty() {
        return Err(failure(request, draft.label, problems));
    }
    match validate_startup_request(&request) {
        Ok(id) => Ok(RegisterRequest {
            feed_id: Some(id),
            ..request
        }),
        Err(error) => Err(StartupFailure {
            request,
            error: format!("{}: {}", draft.label, error.error),
            hint: error.hint,
        }),
    }
}

fn failure(request: RegisterRequest, label: String, problems: Vec<String>) -> StartupFailure {
    StartupFailure {
        request,
        error: format!("{label}: {}", problems.join("; ")),
        hint: Some("fix the environment variables for this feed".into()),
    }
}

fn parse_ttl(value: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|_| format!("{value:?} is not an integer number of seconds"))
}

fn parse_headers(raw: &str, var: &str) -> Result<BTreeMap<String, String>, String> {
    let value: Value =
        serde_json::from_str(raw).map_err(|_| format!("{var} is not a JSON object of strings"))?;
    let Some(object) = value.as_object() else {
        return Err(format!("{var} is not a JSON object of strings"));
    };
    let mut headers = BTreeMap::new();
    for (name, item) in object {
        let Some(text) = item.as_str() else {
            return Err(format!("{var} value for '{name}' must be a string"));
        };
        headers.insert(name.clone(), text.to_string());
    }
    Ok(headers)
}

fn json_string(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(value) = object.get(*key).and_then(Value::as_str) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn json_ttl(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<Result<u64, String>> {
    for key in keys {
        let Some(value) = object.get(*key) else {
            continue;
        };
        if value.is_null() {
            continue;
        }
        if let Some(number) = value.as_u64() {
            return Some(Ok(number));
        }
        if let Some(text) = value.as_str() {
            return Some(parse_ttl(text.trim()));
        }
        return Some(Err(format!("{key} must be an integer number of seconds")));
    }
    None
}

fn json_headers(
    object: &serde_json::Map<String, Value>,
) -> Result<BTreeMap<String, String>, String> {
    let Some(value) = object.get("headers").or_else(|| object.get("http_headers")) else {
        return Ok(BTreeMap::new());
    };
    if value.is_null() {
        return Ok(BTreeMap::new());
    }
    let Some(headers) = value.as_object() else {
        return Err("headers must be a JSON object of strings".into());
    };
    let mut out = BTreeMap::new();
    for (name, item) in headers {
        let Some(text) = item.as_str() else {
            return Err(format!("headers value for '{name}' must be a string"));
        };
        out.insert(name.clone(), text.to_string());
    }
    Ok(out)
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

fn error_value(
    feed_id: Option<String>,
    var: Option<String>,
    error: impl Into<String>,
    hint: Option<String>,
) -> Value {
    let mut value = json!({ "error": error.into() });
    if let Some(object) = value.as_object_mut() {
        if let Some(feed_id) = feed_id {
            object.insert("feed_id".into(), json!(feed_id));
        }
        if let Some(var) = var {
            object.insert("var".into(), json!(var));
        }
        if let Some(hint) = hint {
            object.insert("hint".into(), json!(hint));
        }
    }
    value
}

impl StartupFailure {
    pub fn redact_error(&self) -> String {
        let mut text = self.error.clone();
        for value in self.request.headers.values() {
            if value.len() >= 4 {
                text = text.replace(value, "[redacted]");
            }
        }
        text
    }
}

#[cfg(test)]
fn assert_no_header_values(text: &str, headers: &BTreeMap<String, String>) -> bool {
    headers
        .values()
        .all(|value| value.len() < 4 || !text.contains(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(pairs: &[(&str, &str)]) -> StartupPlan {
        startup_plan(
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string())),
            &StartupOverrides::default(),
        )
    }

    #[test]
    fn default_feed_set_and_headers_do_not_leak_into_errors() {
        let secret = "super-secret-amtrak-token";
        let plan = plan(&[
            ("GTFS_RT_MCP_FEED_ID", "amtrak"),
            (
                "GTFS_RT_MCP_FEED_SET",
                "http://127.0.0.1:8080/v1/feed-set.json",
            ),
            ("GTFS_RT_MCP_RT_TTL", "20"),
            ("GTFS_RT_MCP_STATIC_TTL", "0"),
            (
                "GTFS_RT_MCP_HTTP_HEADERS",
                &format!(r#"{{"Authorization":"Bearer {secret}"}}"#),
            ),
        ]);
        assert!(plan.errors.is_empty(), "{:?}", plan.errors);
        assert!(
            plan.failures.is_empty(),
            "{:?}",
            plan.failures.iter().map(|f| &f.error).collect::<Vec<_>>()
        );
        assert_eq!(plan.requests.len(), 1);
        let request = &plan.requests[0];
        assert_eq!(request.feed_id.as_deref(), Some("amtrak"));
        assert_eq!(request.rt_ttl_secs, Some(20));
        assert_eq!(request.static_ttl_secs, Some(0));
        assert_eq!(
            request.headers.get("Authorization").map(String::as_str),
            Some(format!("Bearer {secret}").as_str())
        );
        let rendered = format!("{:?}", plan.errors);
        assert!(!rendered.contains(secret));
    }

    #[test]
    fn numbered_feeds_and_json_are_separate_styles() {
        let numbered = plan(&[
            ("GTFS_RT_MCP_FEED_1_ID", "mbta"),
            (
                "GTFS_RT_MCP_FEED_1_STATIC",
                "https://cdn.mbta.com/MBTA_GTFS.zip",
            ),
            (
                "GTFS_RT_MCP_FEED_1_TRIP_UPDATES",
                "https://cdn.mbta.com/realtime/TripUpdates.pb",
            ),
            ("GTFS_RT_MCP_FEED_2_NAME", "amtrak"),
            ("GTFS_RT_MCP_FEED_2_FEED_SET", "/tmp/feed-set.json"),
            (
                "GTFS_RT_MCP_FEED_2_HTTP_HEADERS",
                r#"{"x-api-key":"not-in-errors"}"#,
            ),
        ]);
        assert!(numbered.errors.is_empty(), "{:?}", numbered.errors);
        assert_eq!(numbered.requests.len(), 2);
        assert_eq!(numbered.requests[0].feed_id.as_deref(), Some("mbta"));
        assert_eq!(
            numbered.requests[0].static_url.as_deref(),
            Some("https://cdn.mbta.com/MBTA_GTFS.zip")
        );
        assert_eq!(numbered.requests[1].feed_id.as_deref(), Some("amtrak"));
        assert_eq!(
            numbered.requests[1].feed_set.as_deref(),
            Some("/tmp/feed-set.json")
        );
        assert_eq!(
            numbered.requests[1]
                .headers
                .get("x-api-key")
                .map(String::as_str),
            Some("not-in-errors")
        );
        let public = serde_json::to_string(&numbered.errors).unwrap();
        assert!(!public.contains("not-in-errors"));

        let json = plan(&[(
            "GTFS_RT_MCP_FEEDS",
            r#"[{"id":"mbta","static":"https://cdn.mbta.com/MBTA_GTFS.zip","rt_ttl_secs":30},{"name":"amtrak","feed_set":"http://127.0.0.1:8080/v1/feed-set.json"}]"#,
        )]);
        assert!(json.errors.is_empty(), "{:?}", json.errors);
        assert_eq!(json.requests.len(), 2);
        assert_eq!(
            json.requests[1].feed_set.as_deref(),
            Some("http://127.0.0.1:8080/v1/feed-set.json")
        );
    }

    #[test]
    fn bad_config_is_reported_without_the_secret() {
        let secret = "header-secret-value";
        let plan = plan(&[
            (
                "GTFS_RT_MCP_FEED_1_STATIC",
                "https://cdn.mbta.com/MBTA_GTFS.zip",
            ),
            (
                "GTFS_RT_MCP_FEED_3_FEED_SET",
                "http://127.0.0.1:8080/v1/feed-set.json",
            ),
            ("GTFS_RT_MCP_FEED_1_RT_TTL", "soon"),
            (
                "GTFS_RT_MCP_FEED_1_HTTP_HEADERS",
                &format!(r#"{{"x-api-key":"{secret}"}}"#),
            ),
            ("GTFS_RT_MCP_FEEDS", "{not json"),
            ("GTFS_RT_MCP_HTTP_HEADERS", "[]"),
        ]);
        let blob = serde_json::to_string(&plan.errors).unwrap()
            + &plan
                .failures
                .iter()
                .map(|failure| failure.redact_error())
                .collect::<String>();
        assert!(blob.contains("skip 2") || blob.contains("FEED_2"), "{blob}");
        assert!(
            blob.contains("not both") || blob.contains("GTFS_RT_MCP_FEEDS"),
            "{blob}"
        );
        assert!(assert_no_header_values(&blob, &{
            let mut headers = BTreeMap::new();
            headers.insert("x-api-key".into(), secret.into());
            headers
        }));
        assert!(plan.requests.is_empty());
    }

    #[test]
    fn a_gap_is_reported_and_the_other_indexes_remain() {
        let plan = plan(&[
            (
                "GTFS_RT_MCP_FEED_1_STATIC",
                "https://cdn.mbta.com/MBTA_GTFS.zip",
            ),
            (
                "GTFS_RT_MCP_FEED_3_FEED_SET",
                "http://127.0.0.1:8080/v1/feed-set.json",
            ),
        ]);
        let errors = serde_json::to_string(&plan.errors).unwrap();
        assert!(errors.contains("skip 2"), "{errors}");
        assert_eq!(plan.requests.len(), 2);
        assert_eq!(plan.requests[0].feed_id.as_deref(), Some("feed1"));
        assert_eq!(plan.requests[1].feed_id.as_deref(), Some("feed3"));
    }

    #[test]
    fn disagreeing_id_and_name_are_reported() {
        let plan = plan(&[
            ("GTFS_RT_MCP_FEED_1_ID", "mbta"),
            ("GTFS_RT_MCP_FEED_1_NAME", "amtrak"),
            (
                "GTFS_RT_MCP_FEED_1_STATIC",
                "https://cdn.mbta.com/MBTA_GTFS.zip",
            ),
        ]);
        let errors = serde_json::to_string(&plan.errors).unwrap();
        assert!(errors.contains("disagree"), "{errors}");
        assert_eq!(plan.requests.len(), 1);
        assert_eq!(plan.requests[0].feed_id.as_deref(), Some("mbta"));
    }

    #[test]
    fn flags_override_the_matching_default_variable() {
        let plan = startup_plan(
            [(
                "GTFS_RT_MCP_STATIC".into(),
                "https://example.test/old.zip".into(),
            )],
            &StartupOverrides {
                static_gtfs: Some("https://cdn.mbta.com/MBTA_GTFS.zip".into()),
                feed_id: Some("mbta".into()),
                ..StartupOverrides::default()
            },
        );
        assert_eq!(plan.requests.len(), 1);
        assert_eq!(
            plan.requests[0].static_url.as_deref(),
            Some("https://cdn.mbta.com/MBTA_GTFS.zip")
        );
        assert_eq!(plan.requests[0].feed_id.as_deref(), Some("mbta"));
    }
}
