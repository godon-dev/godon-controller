//! The wish family verb behind the f/controller/steerwish_update windmill path
//! (python's controller/steerwish_update.py, renamed wish-generic per the family ruling);
//! ported verbatim (validate at the door, call the service, envelope the result).

use serde_json::json;
use serde_json::Value;
use crate::wish_service::WishService;

pub fn run(rd: &Value) -> Value {
    let wish_id = rd.get("wish_id").and_then(|v| v.as_str());
    let Some(wish_id) = wish_id.filter(|s| !s.is_empty()) else {
        return json!({"result": "FAILURE", "error": "Missing wish_id"});
    };
    let claims = rd.get("claims");
    if claims.map(|c| c.is_null() || c.as_array().map(|a| a.is_empty()).unwrap_or(true)).unwrap_or(true) {
        return json!({
            "result": "FAILURE",
            "error": "Missing claims ([{outcome, band}]) - the sugar shape is retired"
        });
    }
    let service = WishService::new(true);
    let payload = json!({
        "claims": rd.get("claims").cloned().unwrap_or(Value::Null),
        "terms": rd.get("terms").cloned().unwrap_or(Value::Null),
        "limits": rd.get("limits").cloned().unwrap_or(Value::Null),
        "budget": rd.get("budget").cloned().unwrap_or(Value::Null),
        "reason": rd.get("reason").cloned().unwrap_or(Value::Null),
    });
    match service.update_wish(wish_id, &payload) {
        Ok(wish) => wish,
        Err(e) => json!({"result": "FAILURE", "error": e}),
    }
}
