//! The f/controller/systemtender_get windmill script - python's controller/systemtender_get.py,
//! ported verbatim (validate at the door, call the service, envelope the result).

use crate::systemtender_service::SystemtenderService;
use serde_json::json;
use serde_json::Value;

pub fn run(rd: &Value) -> Value {
    let id = rd.get("systemtender_id").and_then(|v| v.as_str());
    let Some(systemtender_id) = id.filter(|s| !s.is_empty()) else {
        return json!({"result": "FAILURE", "error": "Missing systemtender_id"});
    };
    let service = SystemtenderService::new();
    service.get_systemtender(systemtender_id)
}
