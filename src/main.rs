// The controller binary - one verb per windmill script path (f/controller/*).
//
// Three doors, one dispatch. Manually: the verb as the first argument and
// the JSON as the second (or on stdin). On the windmill sandbox rail: an
// empty-body script runs this binary as the container entrypoint with no
// argv - the platform env routes (WM_JOB_PATH's last segment is the verb)
// and the job's own arguments (carrying the verb) ride the job row,
// fetched verbatim through the platform API. The envelope goes to stdout (the log) and to
// ./result.json (the result file - how the python verbs' return reached
// the api). The binding is deliberately ONE thin layer so the rail can
// adjust without touching any verb logic. Unknown verbs answer loudly.

use std::io::Read;

use godon_controller::platform;
use serde_json::Value;

fn main() {
    godon_controller::otel_logging::init_telemetry();

    let mut argv = std::env::args().skip(1);
    let result = match argv.next() {
        Some(verb) => {
            let input = match argv.next() {
                Some(payload) => payload,
                None => {
                    let mut buf = String::new();
                    std::io::stdin()
                        .read_to_string(&mut buf)
                        .expect("read arguments from stdin");
                    buf
                }
            };
            let args: Value = if input.trim().is_empty() {
                Value::Null
            } else {
                serde_json::from_str(&input).expect("arguments must be JSON")
            };
            godon_controller::verbs::dispatch(&verb, &args)
        }
        None => match std::env::var("WM_JOB_PATH") {
            Ok(_) => match fetch_job_args() {
                Ok(args) => platform::serve(args),
                Err(e) => serde_json::json!({
                    "result": "FAILURE",
                    "error": format!("platform run cannot read its arguments: {e}"),
                }),
            },
            Err(_) => {
                eprintln!("usage: godon-controller <verb> [args-json] (or the platform env)");
                std::process::exit(2);
            }
        },
    };
    finish(result);
}

fn finish(result: Value) {
    let envelope = serde_json::to_string(&result).expect("serialize result");
    println!("{}", envelope);
    let _ = std::fs::write("result.json", &envelope);
}

/// This job's own args from the platform API - the caller's keyed object
/// rides the job row verbatim (GET jobs_u/get_args with the job token).
fn fetch_job_args() -> Result<Value, String> {
    let base = std::env::var("BASE_INTERNAL_URL")
        .or_else(|_| std::env::var("WM_BASE_URL"))
        .map_err(|_| "no platform base url (BASE_INTERNAL_URL or WM_BASE_URL)".to_string())?;
    let workspace =
        std::env::var("WM_WORKSPACE").map_err(|_| "no WM_WORKSPACE".to_string())?;
    let job_id = std::env::var("WM_JOB_ID").map_err(|_| "no WM_JOB_ID".to_string())?;
    let token = std::env::var("WM_TOKEN").map_err(|_| "no WM_TOKEN".to_string())?;

    let resp = reqwest::blocking::Client::new()
        .get(format!(
            "{}/api/w/{}/jobs_u/get_args/{}",
            base, workspace, job_id
        ))
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .map_err(|e| format!("the args call failed: {e}"))?;
    resp.json::<Value>()
        .map_err(|e| format!("the args body is not json: {e}"))
}
