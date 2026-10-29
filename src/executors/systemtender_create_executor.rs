//! The f/controller/systemtender_create_executor windmill script - python's controller/executors/systemtender_create_executor.py,
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
    service.create_systemtender_executor(systemtender_id)
}
