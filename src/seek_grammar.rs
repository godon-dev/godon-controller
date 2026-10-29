//! The walk wish's grammar - the commissioned walk's body at the door.
//!
//! Mirrors the tender's own door (robots) and the planner's (causal):
//! one vocabulary across all three checks; the wire is never trusted.
//! The hold wish's grammar lives in wish_service (checked_steer_claim).
//! Error strings are the observable surface - they port verbatim from
//! controller/seek_grammar.py.

use serde_json::{json, Map, Value};

pub const WALK_FORMS: [&str; 2] = ["cover_now", "find_best"];
pub const DIRECTION_WORDS: [&str; 4] = ["maximize", "max", "minimize", "min"];

/// Python truthiness for the wire values this grammar reads.
fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// Render a wire value the way python's f-string rendered it in the
/// error strings (numbers plain, strings bare).
fn shown(v: &Value) -> String {
    v.as_str()
        .map(|s| s.to_string())
        .unwrap_or_else(|| v.to_string())
}

/// The walk body at the door (design §XIV; wire re-pinned §XVII):
/// form, region, criterion, budget, candidates. This door mirrors the
/// tender's own door - a wish that cannot run refuses here, before
/// anything plans on it; the tender's door re-checks (never trust the
/// wire).
pub fn checked_seek_body(body: &Value) -> Result<Value, String> {
    let Some(obj) = body.as_object() else {
        return Err(
            "the walk body must be an object: {form, region, criterion, budget, candidates}"
                .to_string(),
        );
    };

    let tender = match obj.get("tender") {
        Some(t) if truthy(t) => {
            let s = shown(t);
            if s.trim().is_empty() {
                return Err(
                    "the walk names its tender: the uuid of the walking tender (no omission)"
                        .to_string(),
                );
            }
            s
        }
        _ => {
            return Err(
                "the walk names its tender: the uuid of the walking tender (no omission)"
                    .to_string(),
            )
        }
    };

    let form = match obj.get("form") {
        Some(f) if truthy(f) => {
            let s = shown(f);
            if !WALK_FORMS.contains(&s.as_str()) {
                return Err(format!("unknown walk form: {} (find_best or cover_now)", s));
            }
            s
        }
        _ => {
            return Err(
                "the walk speaks its form: find_best or cover_now (no omission)".to_string(),
            )
        }
    };

    let region = obj.get("region").and_then(|r| r.as_object());
    let region_obj = match region {
        Some(r) if !r.is_empty() => r,
        _ => {
            return Err("region is required: at least one dial, each [lo, hi, is_int]".to_string())
        }
    };
    let mut checked_region = Map::new();
    for (dial, spec) in region_obj {
        let cells = match spec.as_array() {
            Some(a) if a.len() >= 2 => a,
            _ => return Err(format!("region.{} must be [lo, hi, is_int]", dial)),
        };
        let lo = &cells[0];
        let hi = &cells[1];
        let is_int = cells.get(2).and_then(|c| c.as_bool()).unwrap_or(false);
        // python refused bools and non-numbers with the same wording
        // as an inverted range - one message, the door's own choice
        let num = |v: &Value| -> Option<f64> {
            if v.is_boolean() {
                None
            } else {
                v.as_f64()
            }
        };
        let (Some(lo_n), Some(hi_n)) = (num(lo), num(hi)) else {
            return Err(format!("region.{}: hi must exceed lo", dial));
        };
        if hi_n <= lo_n {
            return Err(format!("region.{}: hi must exceed lo", dial));
        }
        if is_int && (lo_n.fract() != 0.0 || hi_n.fract() != 0.0) {
            return Err(format!(
                "region.{}: integer dial needs integer bounds",
                dial
            ));
        }
        let lo_v = if is_int {
            json!(lo_n as i64)
        } else {
            json!(lo_n)
        };
        let hi_v = if is_int {
            json!(hi_n as i64)
        } else {
            json!(hi_n)
        };
        checked_region.insert(dial.clone(), json!([lo_v, hi_v, is_int]));
    }

    let mut criterion = Value::Null;
    if form == "find_best" {
        let c = obj.get("criterion");
        let ok = c
            .and_then(|c| c.as_object())
            .map(|o| o.get("outcome").map(truthy).unwrap_or(false))
            .unwrap_or(false);
        if !ok {
            return Err("find_best declares a criterion: {outcome, direction}".to_string());
        }
        let direction = c
            .and_then(|c| c.get("direction"))
            .and_then(|d| d.as_str())
            .map(|d| d.to_string())
            .unwrap_or_default();
        if !DIRECTION_WORDS.contains(&direction.as_str()) {
            return Err(
                "criterion.direction must be one of ('maximize', 'max', 'minimize', 'min')"
                    .to_string(),
            );
        }
        let outcome = c
            .and_then(|c| c.get("outcome"))
            .and_then(|o| o.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        criterion = json!({"outcome": outcome, "direction": direction});
    }

    let budget_val = obj.get("budget").filter(|b| truthy(b));
    let budget: Option<i64> =
        if form == "find_best" {
            match budget_val.and_then(|b| b.as_i64()) {
                Some(b) if b > 0 => Some(b),
                _ => return Err(
                    "find_best declares a budget: a positive integer (the feelers it may spend)"
                        .to_string(),
                ),
            }
        } else {
            if budget_val.is_some() {
                return Err("cover_now walks on standing duty - it carries no budget".to_string());
            }
            None
        };

    let candidates = match obj.get("candidates") {
        Some(v) if truthy(v) => {
            let n = v.as_i64();
            if v.is_boolean() || n.is_none() || n.unwrap_or(0) < 0 {
                return Err(
                    "candidates must be a non-negative integer (omitted = none)".to_string()
                );
            }
            n.unwrap_or(0)
        }
        _ => 0,
    };
    if form == "find_best" && budget.is_some() && candidates > budget.unwrap_or(0) {
        return Err(format!(
            "candidates ({}) exceeds the budget ({}) - an impossible wish never starts",
            candidates,
            budget.unwrap_or(0)
        ));
    }

    Ok(json!({
        "tender": tender,
        "form": form,
        "region": Value::Object(checked_region),
        "criterion": criterion,
        "budget": budget,
        "candidates": candidates,
    }))
}

/// The walk wish's body at the public door (design §XVII): the wish
/// carries type 'seek' and its walk fields flat - the walking tender
/// (its uuid — dial names are only unambiguous inside one dial-set),
/// form (find_best or cover_now, both stated, no omission), region,
/// criterion, budget, candidates. One dialect end to end.
pub fn checked_wish_body(payload: &Value) -> Result<Value, String> {
    let pick = |k: &str| payload.get(k).cloned().unwrap_or(Value::Null);
    checked_seek_body(&json!({
        "tender": pick("tender"),
        "form": pick("form"),
        "region": pick("region"),
        "criterion": pick("criterion"),
        "budget": pick("budget"),
        "candidates": pick("candidates"),
    }))
}
