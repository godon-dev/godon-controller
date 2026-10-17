//! The wish family's plan-ask verb behind the f/controller/steerwish_plan
//! windmill path (python's controller/steerwish_plan.py): the ask rides
//! its own job when the door answered 'later' - this verb asks causal to
//! plan from the stored terms and books every answer (planned, refused
//! with its numbers, plan_error) on the wish's event record. Ported
//! verbatim (validate at the door, call the service, envelope the
//! result). The arguments arrive flat - the job binds wish_id directly.

use crate::wish_service::WishService;
use serde_json::json;
use serde_json::Value;

pub fn run(rd: &Value) -> Value {
    let wish_id = rd.get("wish_id").and_then(|v| v.as_str());
    let Some(wish_id) = wish_id.filter(|s| !s.is_empty()) else {
        return json!({"result": "FAILURE", "error": "Missing required field: wish_id"});
    };
    let service = WishService::new(true);
    match service.plan_wish(wish_id) {
        Some(wish) => json!({
            "result": "SUCCESS",
            "data": {
                "wish_id": wish_id,
                "state": wish.get("state").cloned().unwrap_or(Value::Null),
            },
        }),
        None => json!({
            "result": "FAILURE",
            "error": format!("unknown wish: {}", wish_id),
        }),
    }
}
