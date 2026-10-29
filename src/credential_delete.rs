//! The f/controller/credential_delete windmill script - python's controller/credential_delete.py,
//! ported verbatim (validate at the door, call the service, envelope the result).

use crate::database::DbConfig;
use crate::database::MetadataDatabaseRepository;
use serde_json::json;
use serde_json::Value;

pub fn run(rd: &Value) -> Value {
    let cid = rd.get("credentialId").and_then(|v| v.as_str());
    let Some(credential_id) = cid.filter(|s| !s.is_empty()) else {
        return json!({"result": "FAILURE", "error": "Missing credentialId parameter"});
    };
    let outcome = (|| -> Result<Value, String> {
        let repo = MetadataDatabaseRepository::new(DbConfig::meta_from_env());
        repo.create_credentials_table().map_err(|e| e.0)?;
        if repo
            .fetch_credential_by_id(credential_id)
            .map_err(|e| e.0)?
            .is_none()
        {
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
