//! REST problem metadata shared by realtime and gateway decoders.

/// A REST refusal. Code is the discriminator; type is its canonical URI form.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HttpProblem {
    pub code: String,
    pub status: u16,
    #[serde(default)]
    pub detail: String,
    #[serde(rename = "type", default)]
    pub problem_type: String,
}

/// Derive a problem URI with the same rule for every endpoint.
pub fn problem_type(code: &str) -> String {
    format!(
        "https://polyester.dev/problems/{}",
        code.to_lowercase().replace('_', "-")
    )
}

/// Read problem metadata using the actual HTTP status, preserving an empty detail.
pub fn decode_http_problem(body: &[u8], status: u16) -> Option<HttpProblem> {
    let mut value: serde_json::Value = serde_json::from_slice(body).ok()?;
    let code = value.get("code")?.as_str()?;
    if code.is_empty() {
        return None;
    }
    let canonical = problem_type(code);
    if value.get("detail").is_none() {
        value["detail"] = value.get("message").cloned().unwrap_or_else(|| "".into());
    }
    value["status"] = status.into();
    value["type"] = canonical.into();
    serde_json::from_value(value).ok()
}
