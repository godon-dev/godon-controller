//! The platform run - a declared sandbox script runs this binary as the
//! container entrypoint with no argv; the job's own path names the verb
//! (its last segment) and the job's own arguments - fetched through the
//! platform API, the caller's keyed object verbatim - are the request.
//! One entry per path, the image composed by the seeder from the pin.

use serde_json::Value;

/// Serve one platform run: the verb is the called path's last segment,
/// the fetched arguments are the request. Every refusal is the loud
/// FAILURE envelope, like every other door.
pub fn serve(job_path: &str, args: Value) -> Value {
    let verb = job_path.rsplit('/').next().unwrap_or("").trim();
    if verb.is_empty() {
        return serde_json::json!({
            "result": "FAILURE",
            "error": format!("no verb in the job path: {job_path}"),
        });
    }
    crate::verbs::dispatch(verb, &args)
}
