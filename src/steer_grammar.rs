//! The hold wish's grammar - claims and terms at the door.
//!
//! Python's controller/steer_grammar.py, ported verbatim: the STEER
//! vocabulary (bands, claims, terms, the declaration options) while the
//! wish registry stays grammar-agnostic (wish_service). The walk wish's
//! grammar lives beside it in seek_grammar. Error strings are the
//! observable surface - they port verbatim.

use serde_json::Map;
use serde_json::Value;

pub fn checked_steer_band(band: &Value, where_: &str) -> Result<Value, String> {
    let is_band = band
        .as_object()
        .map(|o| o.contains_key("lo") && o.contains_key("hi"))
        .unwrap_or(false);
    if !is_band {
        return Err(format!(
            "{} requires lo and hi (in the outcome measurement units)",
            where_
        ));
    }
    let lo = band.get("lo").and_then(|v| v.as_f64());
    let hi = band.get("hi").and_then(|v| v.as_f64());
    let (Some(lo), Some(hi)) = (lo, hi) else {
        return Err(format!("{} lo and hi must be numbers", where_));
    };
    if !(lo < hi) {
        return Err(format!("{} lo must be below hi", where_));
    }
    if !(lo.is_finite() && hi.is_finite()) {
        return Err(format!("{} lo and hi must be finite", where_));
    }
    if let Some(target) = band.get("target") {
        if !target.is_null() {
            let Some(t) = target.as_f64() else {
                return Err(format!("{} target must be a number", where_));
            };
            if !t.is_finite() {
                return Err(format!("{} target must be finite", where_));
            }
        }
    }
    Ok(band.clone())
}

pub fn checked_steer_claim(claim: &Value, where_: &str) -> Result<Value, String> {
    if !claim.is_object() {
        return Err(format!("{} must be an object (outcome, band)", where_));
    }
    if claim
        .get("direction")
        .map(|d| !d.is_null())
        .unwrap_or(false)
    {
        return Err(format!(
            "{}.direction is refused: the direction rung is not built - declare a band",
            where_
        ));
    }
    let name = claim.get("outcome");
    let name_ok = name
        .and_then(|n| n.as_str())
        .map(|n| !n.trim().is_empty())
        .unwrap_or(false);
    if !name_ok {
        return Err(format!(
            "{}.outcome is required: the plain name of one measured value",
            where_
        ));
    }
    let band = checked_steer_claim_band(claim, where_)?;
    let mut m = Map::new();
    m.insert(
        "outcome".to_string(),
        Value::String(
            name.and_then(|n| n.as_str())
                .unwrap_or("")
                .trim()
                .to_string(),
        ),
    );
    m.insert("band".to_string(), band);
    Ok(Value::Object(m))
}

fn checked_steer_claim_band(claim: &Value, where_: &str) -> Result<Value, String> {
    match claim.get("band") {
        Some(b) => checked_steer_band(b, &format!("{}.band", where_)),
        None => Err(format!(
            "{}.band requires lo and hi (in the outcome measurement units)",
            where_
        )),
    }
}

/// The two caller-stated declaration options (design §XIX) - family
/// grammar, both wish types: answer timing and standard, intent only
/// the wisher holds. Both optional with sealed defaults: answer
/// 'later' (the consult's answers surface at serving time and in
/// status), standard 'all' for holds (a half-kept state is a lying
/// state) and 'best' for walks (a seek's candidates by nature).
/// Returns (answer, standard).
pub fn checked_declaration_options(
    body: &Value,
    default_standard: &str,
) -> Result<(String, String), String> {
    let answer = match body.get("answer") {
        None | Some(Value::Null) => "later".to_string(),
        Some(a) => a
            .as_str()
            .map(|s| s.to_string())
            .unwrap_or_else(|| a.to_string()),
    };
    if answer != "now" && answer != "later" {
        return Err(format!(
            "answer must be 'now' or 'later' (got: {}) - when the reachability consult answers: in the same breath, or at serving time",
            answer
        ));
    }
    let standard = match body.get("standard") {
        None | Some(Value::Null) => default_standard.to_string(),
        Some(s) => s
            .as_str()
            .map(|s| s.to_string())
            .unwrap_or_else(|| s.to_string()),
    };
    if standard != "all" && standard != "best" {
        return Err(format!(
            "standard must be all or best (got: {}) - one kept state, or the best of several",
            standard
        ));
    }
    Ok((answer, standard))
}
