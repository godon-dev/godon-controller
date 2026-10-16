//! The f/controller/credential_create windmill script - python's controller/credential_create.py,
//! ported verbatim (validate at the door, call the service, envelope the result).

use serde_json::json;
use serde_json::Value;
use crate::verbs::truthy_rd;
use crate::database::DbConfig;
use crate::database::MetadataDatabaseRepository;

pub fn run(rd: &Value) -> Value {
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

pub(crate) fn name_chars_ok(name: &str) -> bool {
    !name.is_empty()
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}
