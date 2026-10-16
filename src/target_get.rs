//! The f/controller/target_get windmill script - python's controller/target_get.py,
//! ported verbatim (validate at the door, call the service, envelope the result).

use serde_json::json;
use serde_json::Value;
use crate::timefmt::pg_ts_to_iso;
use crate::database::DbConfig;
use crate::database::MetadataDatabaseRepository;
use crate::credential_get::cell_json;

pub fn run(rd: &Value) -> Value {
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

pub(crate) fn format_target(t: &[Option<String>]) -> Value {
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
