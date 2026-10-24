//! The wish family verb behind the f/controller/steerwish_get windmill path
//! (python's controller/steerwish_get.py, renamed wish-generic per the family ruling);
//! ported verbatim (validate at the door, call the service, envelope the result).

use serde_json::json;
use serde_json::Value;
use crate::wish_service::WishService;

pub fn run(rd: &Value) -> Value {
    let wish_id = rd.get("wish_id").and_then(|v| v.as_str());
    let Some(wish_id) = wish_id.filter(|s| !s.is_empty()) else {
        return json!({"result": "FAILURE", "error": "Missing wish_id"});
    };
    // with_archive: the read door mirrors the seek's return from the
    // serving tender's ledger (seek_return::<id>) into the event trail
    // on first read — without the archive repo the mirror silently
    // skips and every served seek sticks at "assigned" forever (found
    // live 2026-10-23/24: two served-to-RETURNED seeks, both stuck).
    let service = WishService::new(true);
    match service.get_wish(wish_id) {
        Ok(Some(wish)) => wish,
        Ok(None) => json!({"result": "FAILURE", "error": format!("steerwish not found: {}", wish_id)}),
        Err(e) => {
            log::error!("Failed to fetch wish: {}", e);
            json!({"result": "FAILURE", "error": e})
        }
    }
}
