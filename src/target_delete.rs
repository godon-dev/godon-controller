//! The f/controller/target_delete windmill script - python's controller/target_delete.py,
//! ported verbatim (validate at the door, call the service, envelope the result).

use serde_json::json;
use serde_json::Value;
use crate::database::DbConfig;
use crate::database::MetadataDatabaseRepository;

pub fn run(rd: &Value) -> Value {
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
