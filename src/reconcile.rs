//! The f/controller/controller_reconcile windmill script - one tick of the
//! eternal reconciler (designs/2026-10-18-worker-death-door.md). The
//! tick body lives in the service with the rest of the lifecycle; this
//! module is only the platform door, like every other verb.

use serde_json::Value;
use crate::systemtender_service::SystemtenderService;

pub fn run(_rd: &Value) -> Value {
    let service = SystemtenderService::new();
    service.reconcile_tick()
}
