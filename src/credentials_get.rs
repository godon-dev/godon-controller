//! The f/controller/credentials_get windmill script - python's controller/credentials_get.py,
//! ported verbatim (validate at the door, call the service, envelope the result).

use serde_json::json;
use serde_json::Value;
use crate::timefmt::pg_ts_to_iso;
use crate::database::DbConfig;
use crate::database::MetadataDatabaseRepository;

pub fn run(_rd: &Value) -> Value {
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
