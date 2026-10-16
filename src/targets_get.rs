//! The f/controller/targets_get windmill script - python's controller/targets_get.py,
//! ported verbatim (validate at the door, call the service, envelope the result).

use serde_json::json;
use serde_json::Value;
use crate::target_get::format_target;
use crate::database::DbConfig;
use crate::database::MetadataDatabaseRepository;

pub fn run(_rd: &Value) -> Value {
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
