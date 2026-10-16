//! The f/controller/systemtender_create windmill script - python's controller/systemtender_create.py,
//! ported verbatim (validate at the door, call the service, envelope the result).

use serde_json::json;
use serde_json::Value;
use crate::systemtender_service::SystemtenderService;
use crate::verbs::truthy_rd;

pub fn run(rd: &Value) -> Value {
    if !truthy_rd(rd) {
        return json!({"result": "FAILURE", "error": "Missing request data"});
    }
    let Some(name) = rd.get("name") else {
        return json!({"result": "FAILURE", "error": "Missing required field: name"});
    };
    let name_ok = name.as_str().map(|n| !n.trim().is_empty()).unwrap_or(false);
    if !name_ok {
        return json!({"result": "FAILURE", "error": "Invalid name: name cannot be empty"});
    }
    let Some(config) = rd.get("config") else {
        return json!({"result": "FAILURE", "error": "Missing required field: config"});
    };
    if !(config.is_object() && !config.as_object().unwrap().is_empty()) {
        return json!({"result": "FAILURE", "error": "Invalid config: config must be a non-empty object"});
    }
    if !config
        .as_object()
        .map(|o| o.contains_key("systemtender"))
        .unwrap_or(false)
    {
        return json!({
            "result": "FAILURE",
            "error": "Missing systemtender configuration: 'systemtender' key not found in config"
        });
    }
    let service = SystemtenderService::new();
    service.create_systemtender(config.clone(), name.as_str().unwrap_or(""))
}
