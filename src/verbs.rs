//! Verb dispatch - one verb per windmill script path (f/controller/*),
//! envelopes exactly as the python verbs answered them.
//!
//! Landed: the systemtender lifecycle set (minus the create executor,
//! which stays a python leaf - it owns the optuna schema init) and the
//! steerwish set. Next: credentials and targets.

use crate::steerwish_service::SteerwishService;
use crate::systemtender_service::SystemtenderService;
use serde_json::{json, Value};

fn request_data(args: &Value) -> &Value {
    args
}

pub fn dispatch(verb: &str, args: &Value) -> Value {
    let rd = request_data(args);
    match verb {
        "systemtender_create" => systemtender_create(rd),
        "systemtender_get" => systemtender_get(rd),
        "systemtender_update" => systemtender_update(rd),
        "systemtender_delete" => systemtender_delete(rd),
        "systemtender_start" => systemtender_start(rd),
        "systemtender_stop" => systemtender_stop(rd),
        "systemtenders_get" => systemtenders_get(rd),
        "steerwish_create" => steerwish_create(rd),
        "steerwish_get" => steerwish_get(rd),
        "steerwish_close" => steerwish_close(rd),
        "steerwish_update" => steerwish_update(rd),
        "steerwish_delete" => steerwish_delete(rd),
        "steerwishes_get" => steerwishes_get(rd),
        _ => {
            let mut envelope = serde_json::Map::new();
            envelope.insert("result".to_string(), Value::String("FAILURE".to_string()));
            envelope.insert(
                "error".to_string(),
                Value::String(format!("unknown verb: {}", verb)),
            );
            Value::Object(envelope)
        }
    }
}

fn truthy_rd(rd: &Value) -> bool {
    rd.as_object().map(|o| !o.is_empty()).unwrap_or(false)
}

// ── systemtenders ──────────────────────────────────────────────────

fn systemtender_create(rd: &Value) -> Value {
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

fn systemtender_get(rd: &Value) -> Value {
    let id = rd.get("systemtender_id").and_then(|v| v.as_str());
    let Some(systemtender_id) = id.filter(|s| !s.is_empty()) else {
        return json!({"result": "FAILURE", "error": "Missing systemtender_id"});
    };
    let service = SystemtenderService::new();
    service.get_systemtender(systemtender_id)
}

fn systemtender_update(rd: &Value) -> Value {
    let id = rd.get("systemtender_id").and_then(|v| v.as_str());
    let Some(systemtender_id) = id.filter(|s| !s.is_empty()) else {
        return json!({"result": "FAILURE", "error": "Missing systemtender_id"});
    };
    let Some(new_config) = rd.get("config").filter(|c| !c.is_null()) else {
        return json!({"result": "FAILURE", "error": "Missing config"});
    };
    let force = rd.get("force").cloned().unwrap_or(Value::Bool(false));
    let service = SystemtenderService::new();
    service.update_systemtender(systemtender_id, new_config.clone(), force == Value::Bool(true))
}

fn systemtender_delete(rd: &Value) -> Value {
    let id = rd.get("systemtender_id").and_then(|v| v.as_str());
    let Some(systemtender_id) = id.filter(|s| !s.is_empty()) else {
        return json!({"result": "FAILURE", "error": "Missing systemtender_id"});
    };
    let force = rd.get("force").cloned().unwrap_or(Value::Bool(false));
    let service = SystemtenderService::new();
    service.delete_systemtender(systemtender_id, force == Value::Bool(true))
}

fn systemtender_start(rd: &Value) -> Value {
    let id = rd.get("systemtender_id").and_then(|v| v.as_str());
    let Some(systemtender_id) = id.filter(|s| !s.is_empty()) else {
        return json!({"result": "FAILURE", "error": "Missing systemtender_id"});
    };
    let service = SystemtenderService::new();
    service.start_systemtender(systemtender_id)
}

fn systemtender_stop(rd: &Value) -> Value {
    let id = rd.get("systemtender_id").and_then(|v| v.as_str());
    let Some(systemtender_id) = id.filter(|s| !s.is_empty()) else {
        return json!({"result": "FAILURE", "error": "Missing systemtender_id"});
    };
    let service = SystemtenderService::new();
    service.stop_systemtender(systemtender_id)
}

fn systemtenders_get(_rd: &Value) -> Value {
    let service = SystemtenderService::new();
    service.list_systemtenders()
}

// ── steerwishes ────────────────────────────────────────────────────

fn steerwish_create(rd: &Value) -> Value {
    let service = SteerwishService::new(true);
    match service.create_steerwish(rd) {
        Ok(wish) => wish,
        Err(e) => {
            log::error!("Failed to declare steerwish: {}", e);
            json!({"result": "FAILURE", "error": e})
        }
    }
}

fn steerwish_get(rd: &Value) -> Value {
    let wish_id = rd.get("wish_id").and_then(|v| v.as_str());
    let Some(wish_id) = wish_id.filter(|s| !s.is_empty()) else {
        return json!({"result": "FAILURE", "error": "Missing wish_id"});
    };
    let service = SteerwishService::new(false);
    match service.get_steerwish(wish_id) {
        Ok(Some(wish)) => wish,
        Ok(None) => json!({"result": "FAILURE", "error": format!("steerwish not found: {}", wish_id)}),
        Err(e) => {
            log::error!("Failed to fetch steerwish: {}", e);
            json!({"result": "FAILURE", "error": e})
        }
    }
}

fn steerwish_close(rd: &Value) -> Value {
    let wish_id = rd.get("wish_id").and_then(|v| v.as_str());
    let Some(wish_id) = wish_id.filter(|s| !s.is_empty()) else {
        return json!({"result": "FAILURE", "error": "Missing wish_id"});
    };
    let service = SteerwishService::new(true);
    match service.close_steerwish(wish_id) {
        Ok(Some(wish)) => wish,
        Ok(None) => json!({"result": "FAILURE", "error": format!("steerwish not found: {}", wish_id)}),
        Err(e) => {
            log::error!("Failed to close steerwish: {}", e);
            json!({"result": "FAILURE", "error": e})
        }
    }
}

fn steerwish_update(rd: &Value) -> Value {
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
    let service = SteerwishService::new(true);
    let payload = json!({
        "claims": rd.get("claims").cloned().unwrap_or(Value::Null),
        "terms": rd.get("terms").cloned().unwrap_or(Value::Null),
        "limits": rd.get("limits").cloned().unwrap_or(Value::Null),
        "budget": rd.get("budget").cloned().unwrap_or(Value::Null),
        "reason": rd.get("reason").cloned().unwrap_or(Value::Null),
    });
    match service.update_steerwish(wish_id, &payload) {
        Ok(wish) => wish,
        Err(e) => json!({"result": "FAILURE", "error": e}),
    }
}

fn steerwish_delete(rd: &Value) -> Value {
    let wish_id = rd.get("wish_id").and_then(|v| v.as_str());
    let Some(wish_id) = wish_id.filter(|s| !s.is_empty()) else {
        return json!({"result": "FAILURE", "error": "Missing wish_id"});
    };
    let service = SteerwishService::new(true);
    service.delete_steerwish(wish_id)
}

fn steerwishes_get(_rd: &Value) -> Value {
    let service = SteerwishService::new(false);
    match service.list_steerwishes() {
        Ok(summaries) => Value::Array(summaries),
        Err(e) => {
            log::error!("Failed to list steerwishes: {}", e);
            json!({"result": "FAILURE", "error": e})
        }
    }
}
