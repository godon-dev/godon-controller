//! The f/controller/target_create windmill script - python's controller/target_create.py,
//! ported verbatim (validate at the door, call the service, envelope the result).

use serde_json::json;
use serde_json::Value;
use crate::verbs::truthy_rd;
use crate::credential_create::name_chars_ok;
use crate::database::DbConfig;
use crate::database::MetadataDatabaseRepository;

pub fn run(rd: &Value) -> Value {
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
