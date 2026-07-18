//! Shared proto decoders.

use crate::models::ApiData;
use serde::Serialize;

pub fn api_data_from_proto<T: Serialize>(msg: &T) -> ApiData {
    match serde_json::to_value(msg) {
        Ok(raw) => ApiData { raw },
        Err(_) => ApiData::empty(),
    }
}
