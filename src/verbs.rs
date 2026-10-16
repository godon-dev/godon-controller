//! Verb dispatch - one verb per windmill script path (f/controller/*),
//! envelopes exactly as the python verbs answered them.
//!
//! Landed: the systemtender lifecycle set (minus the create executor,
//! which stays a python leaf - it owns the optuna schema init) and the
//! steerwish set. Next: credentials and targets.

use crate::database::{DbConfig, MetadataDatabaseRepository};
use crate::wish_service::WishService;
use crate::systemtender_service::SystemtenderService;
use crate::timefmt::pg_ts_to_iso;
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
        "steerwish_create" => wish_create(rd),
        "steerwish_get" => wish_get(rd),
        "steerwish_close" => wish_close(rd),
        "steerwish_update" => wish_update(rd),
        "steerwish_delete" => wish_delete(rd),
        "steerwishes_get" => wishes_get(rd),
        "credential_create" => credential_create(rd),
        "credential_get" => credential_get(rd),
        "credential_delete" => credential_delete(rd),
        "credentials_get" => credentials_get(rd),
        "target_create" => target_create(rd),
        "target_get" => target_get(rd),
        "target_delete" => target_delete(rd),
        "targets_get" => targets_get(rd),
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

// ── wishes ────────────────────────────────────────────────────

fn wish_create(rd: &Value) -> Value {
    let service = WishService::new(true);
    match service.create_wish(rd) {
        Ok(wish) => wish,
        Err(e) => {
            log::error!("Failed to declare wish: {}", e);
            json!({"result": "FAILURE", "error": e})
        }
    }
}

fn wish_get(rd: &Value) -> Value {
    let wish_id = rd.get("wish_id").and_then(|v| v.as_str());
    let Some(wish_id) = wish_id.filter(|s| !s.is_empty()) else {
        return json!({"result": "FAILURE", "error": "Missing wish_id"});
    };
    let service = WishService::new(false);
    match service.get_wish(wish_id) {
        Ok(Some(wish)) => wish,
        Ok(None) => json!({"result": "FAILURE", "error": format!("steerwish not found: {}", wish_id)}),
        Err(e) => {
            log::error!("Failed to fetch wish: {}", e);
            json!({"result": "FAILURE", "error": e})
        }
    }
}

fn wish_close(rd: &Value) -> Value {
    let wish_id = rd.get("wish_id").and_then(|v| v.as_str());
    let Some(wish_id) = wish_id.filter(|s| !s.is_empty()) else {
        return json!({"result": "FAILURE", "error": "Missing wish_id"});
    };
    let service = WishService::new(true);
    match service.close_wish(wish_id) {
        Ok(Some(wish)) => wish,
        Ok(None) => json!({"result": "FAILURE", "error": format!("steerwish not found: {}", wish_id)}),
        Err(e) => {
            log::error!("Failed to close wish: {}", e);
            json!({"result": "FAILURE", "error": e})
        }
    }
}

fn wish_update(rd: &Value) -> Value {
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

fn wish_delete(rd: &Value) -> Value {
    let wish_id = rd.get("wish_id").and_then(|v| v.as_str());
    let Some(wish_id) = wish_id.filter(|s| !s.is_empty()) else {
        return json!({"result": "FAILURE", "error": "Missing wish_id"});
    };
    let service = WishService::new(true);
    service.delete_wish(wish_id)
}

fn wishes_get(_rd: &Value) -> Value {
    let service = WishService::new(false);
    match service.list_wishes() {
        Ok(summaries) => Value::Array(summaries),
        Err(e) => {
            log::error!("Failed to list wishes: {}", e);
            json!({"result": "FAILURE", "error": e})
        }
    }
}

// ── credentials ────────────────────────────────────────────────────

fn name_chars_ok(name: &str) -> bool {
    !name.is_empty()
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

fn cell_json(cell: &Option<String>) -> Value {
    cell.as_ref()
        .and_then(|t| serde_json::from_str(t).ok())
        .unwrap_or(Value::Null)
}

fn credential_create(rd: &Value) -> Value {
    if !truthy_rd(rd) {
        return json!({"result": "FAILURE", "error": "Missing request data"});
    }
    let outcome = (|| -> Result<Value, String> {
        let name = rd.get("name");
        let Some(name) = name else {
            return Ok(json!({"result": "FAILURE", "error": "Missing required field: name"}));
        };
        let Some(name_str) = name.as_str() else {
            return Ok(json!({"result": "FAILURE", "error": "Missing required field: name"}));
        };
        if name_str.trim().is_empty() {
            return Ok(json!({"result": "FAILURE", "error": "Invalid name: name cannot be empty"}));
        }
        let Some(credential_type) = rd.get("credentialType") else {
            return Ok(json!({"result": "FAILURE", "error": "Missing required field: credentialType"}));
        };
        if !name_chars_ok(name_str) {
            return Ok(json!({
                "result": "FAILURE",
                "error": format!(
                    "Invalid name format: '{}'. Use only alphanumeric characters, hyphens, and underscores (1-255 characters)",
                    name_str
                )
            }));
        }
        let valid_types = "['ssh_private_key', 'api_token', 'database_connection', 'http_basic_auth']";
        let ct = credential_type.as_str().unwrap_or("");
        if !["ssh_private_key", "api_token", "database_connection", "http_basic_auth"].contains(&ct) {
            return Ok(json!({
                "result": "FAILURE",
                "error": format!("Invalid credentialType: '{}'. Must be one of: {}", ct, valid_types)
            }));
        }
        let description = rd.get("description").and_then(|d| d.as_str()).unwrap_or("");
        let credential_id = uuid::Uuid::new_v4().to_string();
        let windmill_variable = format!("f/vars/{}", name_str);

        let repo = MetadataDatabaseRepository::new(DbConfig::meta_from_env());
        repo.create_credentials_table().map_err(|e| e.0)?;
        if let Err(e) = repo.insert_credential(
            &credential_id,
            name_str,
            ct,
            Some(description),
            &windmill_variable,
            "windmill_variable",
            Some(&json!({})),
        ) {
            let lower = e.0.to_lowercase();
            if lower.contains("duplicate key") || lower.contains("unique constraint") {
                return Ok(json!({
                    "result": "FAILURE",
                    "error": format!("Credential with name '{}' already exists", name_str)
                }));
            }
            return Ok(json!({"result": "FAILURE", "error": format!("Failed to create credential: {}", e.0)}));
        }

        Ok(json!({
            "result": "SUCCESS",
            "data": {
                "id": credential_id,
                "name": name_str,
                "credentialType": ct,
                "description": description,
                "windmillVariable": windmill_variable,
                "createdAt": "now",
            }
        }))
    })();
    match outcome {
        Ok(v) => v,
        Err(e) => {
            log::error!("Failed to create credential: {}", e);
            json!({"result": "FAILURE", "error": e})
        }
    }
}

fn credential_get(rd: &Value) -> Value {
    let cid = rd.get("credentialId").and_then(|v| v.as_str());
    let Some(credential_id) = cid.filter(|s| !s.is_empty()) else {
        return json!({"result": "FAILURE", "error": "Missing credentialId parameter"});
    };
    let outcome = (|| -> Result<Value, String> {
        let repo = MetadataDatabaseRepository::new(DbConfig::meta_from_env());
        repo.create_credentials_table().map_err(|e| e.0)?;
        let Some(c) = repo.fetch_credential_by_id(credential_id).map_err(|e| e.0)? else {
            return Ok(json!({
                "result": "FAILURE",
                "error": format!("Credential with ID '{}' not found", credential_id)
            }));
        };
        Ok(json!({
            "result": "SUCCESS",
            "data": {
                "id": c[0].clone().unwrap_or_default(),
                "name": c[1].clone().unwrap_or_default(),
                "credentialType": c[2].clone().unwrap_or_default(),
                "description": c[3].clone().map(Value::String).unwrap_or(Value::Null),
                "windmillVariable": c[4].clone().unwrap_or_default(),
                "storeType": c[5].clone().unwrap_or_default(),
                "metadata": cell_json(c.get(6).unwrap_or(&None)),
                "createdAt": c.get(7).and_then(|x| x.as_ref()).and_then(|t| pg_ts_to_iso(t)),
                "lastUsedAt": c.get(8).and_then(|x| x.as_ref()).and_then(|t| pg_ts_to_iso(t)),
            }
        }))
    })();
    match outcome {
        Ok(v) => v,
        Err(e) => {
            log::error!("Failed to fetch credential: {}", e);
            json!({"result": "FAILURE", "error": e})
        }
    }
}

fn credential_delete(rd: &Value) -> Value {
    let cid = rd.get("credentialId").and_then(|v| v.as_str());
    let Some(credential_id) = cid.filter(|s| !s.is_empty()) else {
        return json!({"result": "FAILURE", "error": "Missing credentialId parameter"});
    };
    let outcome = (|| -> Result<Value, String> {
        let repo = MetadataDatabaseRepository::new(DbConfig::meta_from_env());
        repo.create_credentials_table().map_err(|e| e.0)?;
        if repo.fetch_credential_by_id(credential_id).map_err(|e| e.0)?.is_none() {
            return Ok(json!({
                "result": "FAILURE",
                "error": format!("Credential with ID '{}' not found", credential_id)
            }));
        }
        repo.delete_credential(credential_id).map_err(|e| e.0)?;
        Ok(json!({"result": "SUCCESS", "data": Value::Null}))
    })();
    match outcome {
        Ok(v) => v,
        Err(e) => {
            log::error!("Failed to delete credential: {}", e);
            json!({"result": "FAILURE", "error": e})
        }
    }
}

fn credentials_get(_rd: &Value) -> Value {
    let outcome = (|| -> Result<Value, String> {
        let repo = MetadataDatabaseRepository::new(DbConfig::meta_from_env());
        repo.create_credentials_table().map_err(|e| e.0)?;
        let creds = repo.fetch_credentials_list().map_err(|e| e.0)?;
        let data: Vec<Value> = creds
            .iter()
            .map(|c| {
                json!({
                    "id": c[0].clone().unwrap_or_default(),
                    "name": c[1].clone().unwrap_or_default(),
                    "credentialType": c[2].clone().unwrap_or_default(),
                    "description": c[3].clone().map(Value::String).unwrap_or(Value::Null),
                    "windmillVariable": c[4].clone().unwrap_or_default(),
                    "createdAt": c.get(5).and_then(|x| x.as_ref()).and_then(|t| pg_ts_to_iso(t)),
                    "lastUsedAt": c.get(6).and_then(|x| x.as_ref()).and_then(|t| pg_ts_to_iso(t)),
                })
            })
            .collect();
        Ok(json!({ "result": "SUCCESS", "data": data }))
    })();
    match outcome {
        Ok(v) => v,
        Err(e) => {
            log::error!("Failed to fetch credentials: {}", e);
            json!({"result": "FAILURE", "error": e})
        }
    }
}

// ── targets ────────────────────────────────────────────────────────

fn target_create(rd: &Value) -> Value {
    if !truthy_rd(rd) {
        return json!({"result": "FAILURE", "error": "Missing request data"});
    }
    let outcome = (|| -> Result<Value, String> {
        let name = rd.get("name").and_then(|n| n.as_str()).unwrap_or("");
        let target_type = rd.get("targetType").and_then(|t| t.as_str());
        let spec = rd.get("spec").cloned().unwrap_or_else(|| json!({}));
        let metadata = rd.get("metadata").cloned().unwrap_or_else(|| json!({}));

        if name.is_empty() {
            return Ok(json!({"result": "FAILURE", "error": "Missing required field: name"}));
        }
        if name.trim().is_empty() {
            return Ok(json!({"result": "FAILURE", "error": "Invalid name: name cannot be empty"}));
        }
        let Some(target_type) = target_type.filter(|t| !t.is_empty()) else {
            return Ok(json!({"result": "FAILURE", "error": "Missing required field: targetType"}));
        };
        if !name_chars_ok(name) {
            return Ok(json!({
                "result": "FAILURE",
                "error": format!(
                    "Invalid name format: '{}'. Use only alphanumeric characters, hyphens, and underscores",
                    name
                )
            }));
        }
        let valid_types = "['ssh', 'http']";
        if !["ssh", "http"].contains(&target_type) {
            return Ok(json!({
                "result": "FAILURE",
                "error": format!("Invalid targetType: '{}'. Must be one of: {}", target_type, valid_types)
            }));
        }
        if !spec.is_object() {
            return Ok(json!({"result": "FAILURE", "error": "spec must be a JSON object"}));
        }
        let required_fields: &[&str] = match target_type {
            "ssh" => &["address"],
            _ => &["url"],
        };
        let missing: Vec<&str> = required_fields
            .iter()
            .filter(|f| {
                let v = spec.get(**f);
                v.map(|v| v.is_null() || v.as_str().map(|s| s.is_empty()).unwrap_or(false))
                    .unwrap_or(true)
            })
            .copied()
            .collect();
        if !missing.is_empty() {
            return Ok(json!({
                "result": "FAILURE",
                "error": format!(
                    "Missing required spec fields for {}: {}",
                    target_type,
                    missing.join(", ")
                )
            }));
        }

        let target_id = uuid::Uuid::new_v4().to_string();
        let repo = MetadataDatabaseRepository::new(DbConfig::meta_from_env());
        repo.create_targets_table().map_err(|e| e.0)?;
        if let Err(e) = repo.insert_target(&target_id, name, target_type, &spec, Some(&metadata)) {
            let lower = e.0.to_lowercase();
            if lower.contains("duplicate key") || lower.contains("unique constraint") {
                return Ok(json!({
                    "result": "FAILURE",
                    "error": format!("Target with name '{}' already exists", name)
                }));
            }
            return Ok(json!({"result": "FAILURE", "error": format!("Failed to create target: {}", e.0)}));
        }

        Ok(json!({
            "result": "SUCCESS",
            "data": {
                "id": target_id,
                "name": name,
                "targetType": target_type,
                "spec": spec,
                "metadata": metadata,
                "createdAt": "now",
            }
        }))
    })();
    match outcome {
        Ok(v) => v,
        Err(e) => {
            log::error!("Failed to create target: {}", e);
            json!({"result": "FAILURE", "error": e})
        }
    }
}

fn format_target(t: &[Option<String>]) -> Value {
    json!({
        "id": t[0].clone().unwrap_or_default(),
        "name": t[1].clone().unwrap_or_default(),
        "targetType": t[2].clone().unwrap_or_default(),
        "spec": cell_json(t.get(3).unwrap_or(&None)),
        "metadata": cell_json(t.get(4).unwrap_or(&None)),
        "createdAt": t.get(5).and_then(|x| x.as_ref()).and_then(|x| pg_ts_to_iso(x)),
        "lastUsedAt": t.get(6).and_then(|x| x.as_ref()).and_then(|x| pg_ts_to_iso(x)),
    })
}

fn target_get(rd: &Value) -> Value {
    let tid = rd.get("targetId").and_then(|v| v.as_str());
    let Some(target_id) = tid.filter(|s| !s.is_empty()) else {
        return json!({"result": "FAILURE", "error": "Missing targetId parameter"});
    };
    let outcome = (|| -> Result<Value, String> {
        let repo = MetadataDatabaseRepository::new(DbConfig::meta_from_env());
        repo.create_targets_table().map_err(|e| e.0)?;
        let Some(t) = repo.fetch_target_by_id(target_id).map_err(|e| e.0)? else {
            return Ok(json!({
                "result": "FAILURE",
                "error": format!("Target with ID '{}' not found", target_id)
            }));
        };
        Ok(json!({"result": "SUCCESS", "data": format_target(&t)}))
    })();
    match outcome {
        Ok(v) => v,
        Err(e) => {
            log::error!("Failed to fetch target: {}", e);
            json!({"result": "FAILURE", "error": e})
        }
    }
}

fn target_delete(rd: &Value) -> Value {
    let tid = rd.get("targetId").and_then(|v| v.as_str());
    let Some(target_id) = tid.filter(|s| !s.is_empty()) else {
        return json!({"result": "FAILURE", "error": "Missing targetId parameter"});
    };
    let outcome = (|| -> Result<Value, String> {
        let repo = MetadataDatabaseRepository::new(DbConfig::meta_from_env());
        repo.create_targets_table().map_err(|e| e.0)?;
        if repo.fetch_target_by_id(target_id).map_err(|e| e.0)?.is_none() {
            return Ok(json!({
                "result": "FAILURE",
                "error": format!("Target with ID '{}' not found", target_id)
            }));
        }
        repo.delete_target(target_id).map_err(|e| e.0)?;
        Ok(json!({"result": "SUCCESS", "data": Value::Null}))
    })();
    match outcome {
        Ok(v) => v,
        Err(e) => {
            log::error!("Failed to delete target: {}", e);
            json!({"result": "FAILURE", "error": e})
        }
    }
}

fn targets_get(_rd: &Value) -> Value {
    let outcome = (|| -> Result<Value, String> {
        let repo = MetadataDatabaseRepository::new(DbConfig::meta_from_env());
        repo.create_targets_table().map_err(|e| e.0)?;
        let targets = repo.fetch_targets_list().map_err(|e| e.0)?;
        let data: Vec<Value> = targets.iter().map(|t| format_target(t)).collect();
        Ok(json!({ "result": "SUCCESS", "data": data }))
    })();
    match outcome {
        Ok(v) => v,
        Err(e) => {
            log::error!("Failed to fetch targets: {}", e);
            json!({"result": "FAILURE", "error": e})
        }
    }
}
