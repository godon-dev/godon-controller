//! The f/controller/credential_get windmill script - python's controller/credential_get.py,
//! ported verbatim (validate at the door, call the service, envelope the result).

use crate::database::DbConfig;
use crate::database::MetadataDatabaseRepository;
use crate::timefmt::pg_ts_to_iso;
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
        let Some(c) = repo
            .fetch_credential_by_id(credential_id)
            .map_err(|e| e.0)?
        else {
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

pub(crate) fn cell_json(cell: &Option<String>) -> Value {
    cell.as_ref()
        .and_then(|t| serde_json::from_str(t).ok())
        .unwrap_or(Value::Null)
}
