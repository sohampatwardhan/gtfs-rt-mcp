use serde_json::{Map, Value};

/// Failure an agent should see, with optional structured fields.
#[derive(Debug)]
pub struct AppError {
    pub error: String,
    pub hint: Option<String>,
    pub extra: Map<String, Value>,
}

impl AppError {
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            hint: None,
            extra: Map::new(),
        }
    }

    pub fn hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    pub fn extra(mut self, key: impl Into<String>, value: Value) -> Self {
        self.extra.insert(key.into(), value);
        self
    }

    pub fn to_json(&self) -> Value {
        let mut map = self.extra.clone();
        map.insert("error".into(), Value::String(self.error.clone()));
        if let Some(hint) = &self.hint {
            map.insert("hint".into(), Value::String(hint.clone()));
        }
        Value::Object(map)
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl std::error::Error for AppError {}

impl From<rusqlite::Error> for AppError {
    fn from(value: rusqlite::Error) -> Self {
        Self::new(format!("sqlite: {value}"))
    }
}

impl From<zip::result::ZipError> for AppError {
    fn from(value: zip::result::ZipError) -> Self {
        Self::new(format!("zip: {value}"))
    }
}

impl From<csv::Error> for AppError {
    fn from(value: csv::Error) -> Self {
        Self::new(format!("csv: {value}"))
    }
}

impl From<reqwest::Error> for AppError {
    fn from(value: reqwest::Error) -> Self {
        Self::new(format!("http: {value}"))
    }
}

impl From<std::io::Error> for AppError {
    fn from(value: std::io::Error) -> Self {
        Self::new(format!("io: {value}"))
    }
}

impl From<prost::DecodeError> for AppError {
    fn from(value: prost::DecodeError) -> Self {
        Self::new(format!("protobuf: {value}"))
            .hint("the bytes are not a GTFS-Realtime FeedMessage")
    }
}

pub fn err(message: impl Into<String>) -> AppError {
    AppError::new(message)
}
