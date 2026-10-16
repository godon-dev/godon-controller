//! The f/controller/systemtenders_get windmill script - python's controller/systemtenders_get.py,
//! ported verbatim (validate at the door, call the service, envelope the result).

use serde_json::Value;
use crate::systemtender_service::SystemtenderService;

pub fn run(_rd: &Value) -> Value {
    let service = SystemtenderService::new();
    service.list_systemtenders()
}
