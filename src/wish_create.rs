//! The wish family verb behind the f/controller/steerwish_create windmill path
//! (python's controller/steerwish_create.py, renamed wish-generic per the family ruling);
//! ported verbatim (validate at the door, call the service, envelope the result).

use serde_json::json;
use serde_json::Value;
use crate::wish_service::WishService;

pub fn run(rd: &Value) -> Value {
    let service = WishService::new(true);
    match service.create_wish(rd) {
        Ok(wish) => wish,
        Err(e) => {
            log::error!("Failed to declare wish: {}", e);
            json!({"result": "FAILURE", "error": e})
        }
    }
}
