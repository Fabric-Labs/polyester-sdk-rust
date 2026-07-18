//! Shared escape-hatch models (Go `models/common.go` parity).

use serde_json::Value;

/// Escape hatch for responses not yet fully modeled.
#[derive(Debug, Clone, PartialEq)]
pub struct ApiData {
    pub raw: Value,
}

impl ApiData {
    pub fn empty() -> Self {
        Self {
            raw: Value::Object(serde_json::Map::new()),
        }
    }
}
