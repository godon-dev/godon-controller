//! The platform run - an empty-body sandbox script calls the binary as
//! the container entrypoint with no argv; the job's own arguments (fetched
//! through the platform API, the caller's keyed object verbatim) carry the
//! verb. Two generic entries serve every verb: the fast lane's call and the
//! slow lane's call_slow (windmill routes lanes per script - the split is
//! the point). The same receipts as the robots lane's self-serve door.

use serde_json::Value;

/// Serve one platform run: the verb comes from the arguments' "verb" field,
/// the rest of the object is the request. Every refusal is the loud FAILURE
/// envelope, like every other door.
pub fn serve(args: Value) -> Value {
    let verb = match args.get("verb").and_then(Value::as_str) {
        Some(v) if !v.trim().is_empty() => v.trim().to_string(),
        _ => {
            return serde_json::json!({
                "result": "FAILURE",
                "error": "no verb in the arguments: {verb: <name>, ...}",
            })
        }
    };
    crate::verbs::dispatch(&verb, &args)
}
