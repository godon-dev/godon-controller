//! The f/controller/systemtender_update windmill script - python's controller/systemtender_update.py,
//! ported verbatim (validate at the door, call the service, envelope the result).

use crate::systemtender_service::SystemtenderService;
use serde_json::json;
use serde_json::Value;

pub fn run(rd: &Value) -> Value {
    let id = rd.get("systemtender_id").and_then(|v| v.as_str());
    let Some(systemtender_id) = id.filter(|s| !s.is_empty()) else {
        return json!({"result": "FAILURE", "error": "Missing systemtender_id"});
    };
    let Some(new_config) = rd.get("config").filter(|c| !c.is_null()) else {
        return json!({"result": "FAILURE", "error": "Missing config"});
    };
    let force = rd.get("force").cloned().unwrap_or(Value::Bool(false));
    let service = SystemtenderService::new();
    service.update_systemtender(
        systemtender_id,
        new_config.clone(),
        force == Value::Bool(true),
    )
}
