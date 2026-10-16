//! The wish family verb behind the f/controller/steerwishes_get windmill path
//! (python's controller/steerwishes_get.py, renamed wish-generic per the family ruling);
//! ported verbatim (validate at the door, call the service, envelope the result).

use serde_json::json;
use serde_json::Value;
use crate::wish_service::WishService;

pub fn run(_rd: &Value) -> Value {
    let service = WishService::new(false);
    match service.list_wishes() {
        Ok(summaries) => Value::Array(summaries),
        Err(e) => {
            log::error!("Failed to list wishes: {}", e);
            json!({"result": "FAILURE", "error": e})
        }
    }
}
