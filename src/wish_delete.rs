//! The wish family verb behind the f/controller/steerwish_delete windmill path
//! (python's controller/steerwish_delete.py, renamed wish-generic per the family ruling);
//! ported verbatim (validate at the door, call the service, envelope the result).

use serde_json::json;
use serde_json::Value;
use crate::wish_service::WishService;

pub fn run(rd: &Value) -> Value {
    let wish_id = rd.get("wish_id").and_then(|v| v.as_str());
    let Some(wish_id) = wish_id.filter(|s| !s.is_empty()) else {
        return json!({"result": "FAILURE", "error": "Missing wish_id"});
    };
    let service = WishService::new(true);
    service.delete_wish(wish_id)
}
