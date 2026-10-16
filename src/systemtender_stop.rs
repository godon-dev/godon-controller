//! The f/controller/systemtender_stop windmill script - python's controller/systemtender_stop.py,
//! ported verbatim (validate at the door, call the service, envelope the result).

use serde_json::json;
use serde_json::Value;
use crate::systemtender_service::SystemtenderService;

pub fn run(rd: &Value) -> Value {
    let id = rd.get("systemtender_id").and_then(|v| v.as_str());
    let Some(systemtender_id) = id.filter(|s| !s.is_empty()) else {
        return json!({"result": "FAILURE", "error": "Missing systemtender_id"});
    };
    let service = SystemtenderService::new();
    service.stop_systemtender(systemtender_id)
}
