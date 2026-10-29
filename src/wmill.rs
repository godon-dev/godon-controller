//! The windmill and causal seams.
//!
//! Python reached windmill through the wmill SDK and causal through
//! urllib; the verbs only need five windmill calls and three causal
//! calls, so the seam is exactly that, injectable for tests the way
//! python's tests patched wmill. The jobs endpoints below are the
//! python's own paths, verbatim. The script-dispatch route is
//! windmill's stable run-by-path shape - the wiring-checks receipts
//! verify it against the flying version before anything relies on it.

use serde_json::Value;
use std::time::Duration;

pub trait WindmillGateway: Send + Sync {
    /// POST the queue cancel. Err carries the failure text.
    fn cancel_job(&self, job_id: &str, reason: Option<&str>) -> Result<(), String>;
    /// GET jobs_u/get. Ok(None) = no job row (gone); Ok(Some(job)).
    fn get_job(&self, job_id: &str) -> Result<Option<Value>, String>;
    /// GET the completed-probe ({completed, result, success, started}).
    fn get_completed_maybe(&self, job_id: &str) -> Result<Option<Value>, String>;
    /// wmill.run_script_by_path_async - dispatch a job; returns its id.
    fn run_script_by_path_async(
        &self,
        path: &str,
        args: &Value,
        tag: Option<&str>,
    ) -> Result<String, String>;
    /// wmill.run_script_by_path - run synchronously, return the result.
    fn run_script_by_path(&self, path: &str, args: &Value) -> Result<Value, String>;
}

pub trait CausalClient: Send + Sync {
    /// GET {causal}/steer/plan/{wish_id} - the book page; None on any
    /// failure (the registry stays as stale as it was).
    fn fetch_book_page(&self, wish_id: &str) -> Option<Value>;

    /// POST {causal}/steer/plan - the plan ask.
    fn ask_to_plan(&self, plan_request: &Value) -> Result<Value, String>;

    /// DELETE {causal}/curves/{id} - best-effort curve purge.
    fn purge_curves(&self, systemtender_id: &str) -> Result<u16, String>;

    /// DELETE {causal}/steer/plan/{wish_id} - purge the book card for
    /// a wish the door refused: the book must not keep a rejected
    /// wish as planned, waiting for a judge to read the ambient
    /// stream and stamp a verdict no tender earned.
    fn purge_book_page(&self, wish_id: &str) -> Result<(), String>;
}

fn windmill_base() -> String {
    // The platform's internal url first, then the job-local API - the
    // family order (the engine's client, this binary's own args fetch).
    // BASE_URL is the platform's ADVERTISED url; in-sandbox it can name
    // a route only the outside resolves, so it never leads.
    std::env::var("BASE_INTERNAL_URL")
        .or_else(|_| std::env::var("WM_BASE_URL"))
        .or_else(|_| std::env::var("BASE_URL"))
        .unwrap_or_else(|_| "http://localhost:8000".to_string())
}

/// The API root - every endpoint hangs off {platform base}/api, the
/// platform's own clients compose it the same way.
fn api_base() -> String {
    format!("{}/api", windmill_base())
}

fn windmill_token() -> String {
    std::env::var("WM_TOKEN").unwrap_or_default()
}

fn windmill_workspace() -> String {
    std::env::var("WM_WORKSPACE").unwrap_or_else(|_| "godon".to_string())
}

fn causal_base() -> String {
    std::env::var("GODON_CAUSAL_URL")
        .unwrap_or_else(|_| "http://godon-godon-causal:9091".to_string())
}

fn client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(35))
        .build()
        .expect("http client")
}

/// The production windmill seam (env: BASE_URL, WM_TOKEN, WM_WORKSPACE
/// - windmill injects these into every job).
pub struct HttpWindmill;

impl WindmillGateway for HttpWindmill {
    fn cancel_job(&self, job_id: &str, reason: Option<&str>) -> Result<(), String> {
        let payload = match reason {
            Some(r) => serde_json::json!({ "reason": r }),
            None => serde_json::json!({}),
        };
        let resp = client()
            .post(format!(
                "{}/w/{}/jobs_u/queue/cancel/{}",
                windmill_base(),
                windmill_workspace(),
                job_id
            ))
            .bearer_auth(windmill_token())
            .json(&payload)
            .send()
            .map_err(|e| e.to_string())?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(format!("HTTP {}", resp.status()))
        }
    }

    fn get_job(&self, job_id: &str) -> Result<Option<Value>, String> {
        let resp = client()
            .get(format!(
                "{}/w/{}/jobs_u/get/{}",
                api_base(),
                windmill_workspace(),
                job_id
            ))
            .bearer_auth(windmill_token())
            .send()
            .map_err(|e| e.to_string())?;
        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(format!("HTTP {}", resp.status()));
        }
        resp.json::<Value>()
            .map(Some)
            .map_err(|e| format!("job row not json: {e}"))
    }

    fn get_completed_maybe(&self, job_id: &str) -> Result<Option<Value>, String> {
        let resp = client()
            .get(format!(
                "{}/w/{}/jobs_u/completed/get_result_maybe/{}?get_started=true",
                api_base(),
                windmill_workspace(),
                job_id
            ))
            .bearer_auth(windmill_token())
            .send()
            .map_err(|e| e.to_string())?;
        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(format!("HTTP {}", resp.status()));
        }
        resp.json::<Value>()
            .map(Some)
            .map_err(|e| format!("result poll body not json: {e}"))
    }

    fn run_script_by_path_async(
        &self,
        path: &str,
        args: &Value,
        tag: Option<&str>,
    ) -> Result<String, String> {
        // The dispatch endpoint takes the args as the body and the job
        // tree linkage as query params - the platform client's shape.
        let mut query: Vec<(&str, String)> = Vec::new();
        if let Ok(job) = std::env::var("WM_JOB_ID") {
            if !job.is_empty() {
                query.push(("parent_job", job));
            }
        }
        if let Ok(root) = std::env::var("WM_ROOT_FLOW_JOB_ID") {
            if !root.is_empty() {
                query.push(("root_job", root));
            }
        }
        if let Some(tag) = tag {
            query.push(("tag", tag.to_string()));
        }
        let resp = client()
            .post(format!(
                "{}/w/{}/jobs/run/p/{}",
                api_base(),
                windmill_workspace(),
                path
            ))
            .query(&query)
            .bearer_auth(windmill_token())
            .json(args)
            .send()
            .map_err(|e| e.to_string())?;
        if !resp.status().is_success() {
            return Err(format!("HTTP {}", resp.status()));
        }
        // The answer is the job id as plain text - the platform client
        // reads it the same way.
        let job_id = resp.text().map_err(|e| e.to_string())?;
        let job_id = job_id.trim();
        if job_id.is_empty() {
            return Err("dispatch answer is empty".to_string());
        }
        Ok(job_id.to_string())
    }

    fn run_script_by_path(&self, path: &str, args: &Value) -> Result<Value, String> {
        // The platform client's synchronous run: dispatch, then poll
        // the completed endpoint until the job answers.
        let job_id = self.run_script_by_path_async(path, args, None)?;
        loop {
            if let Some(done) = self.get_completed_maybe(&job_id)? {
                let success = done
                    .get("success")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if success {
                    return Ok(done.get("result").cloned().unwrap_or(Value::Null));
                }
                let error = done
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown error");
                return Err(format!("Job {job_id} was not successful: {error}"));
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    }
}

/// The production causal seam.
pub struct HttpCausal;

impl CausalClient for HttpCausal {
    fn fetch_book_page(&self, wish_id: &str) -> Option<Value> {
        let short = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .ok()?;
        let resp = short
            .get(format!("{}/steer/plan/{}", causal_base(), wish_id))
            .send()
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        resp.json::<Value>().ok()
    }

    fn ask_to_plan(&self, plan_request: &Value) -> Result<Value, String> {
        let resp = client()
            .post(format!("{}/steer/plan", causal_base()))
            .json(plan_request)
            .send()
            .map_err(|e| e.to_string())?;
        if !resp.status().is_success() {
            return Err(format!("HTTP {}", resp.status()));
        }
        resp.json::<Value>().map_err(|e| e.to_string())
    }

    fn purge_curves(&self, systemtender_id: &str) -> Result<u16, String> {
        let short = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| e.to_string())?;
        let resp = short
            .delete(format!("{}/curves/{}", causal_base(), systemtender_id))
            .send()
            .map_err(|e| e.to_string())?;
        Ok(resp.status().as_u16())
    }

    fn purge_book_page(&self, wish_id: &str) -> Result<(), String> {
        let short = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| e.to_string())?;
        let resp = short
            .delete(format!("{}/steer/plan/{}", causal_base(), wish_id))
            .send()
            .map_err(|e| e.to_string())?;
        if resp.status().is_success() || resp.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(format!("HTTP {}", resp.status()))
        }
    }
}
