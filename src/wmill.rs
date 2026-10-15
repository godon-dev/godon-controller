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
}

fn windmill_base() -> String {
    std::env::var("BASE_URL")
        .or_else(|_| std::env::var("WM_BASE_URL"))
        .unwrap_or_else(|_| "http://localhost:8000".to_string())
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
                windmill_base(),
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
        Ok(resp.json::<Value>().ok())
    }

    fn get_completed_maybe(&self, job_id: &str) -> Result<Option<Value>, String> {
        let resp = client()
            .get(format!(
                "{}/w/{}/jobs_u/completed/get_result_maybe/{}?get_started=true",
                windmill_base(),
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
        Ok(resp.json::<Value>().ok())
    }

    fn run_script_by_path_async(
        &self,
        path: &str,
        args: &Value,
        tag: Option<&str>,
    ) -> Result<String, String> {
        let mut payload = serde_json::json!({ "args": args });
        if let Some(tag) = tag {
            payload["tag"] = Value::String(tag.to_string());
        }
        let resp = client()
            .post(format!(
                "{}/w/{}/scripts/run/p/{}",
                windmill_base(),
                windmill_workspace(),
                path
            ))
            .bearer_auth(windmill_token())
            .json(&payload)
            .send()
            .map_err(|e| e.to_string())?;
        if !resp.status().is_success() {
            return Err(format!("HTTP {}", resp.status()));
        }
        let body = resp.json::<Value>().map_err(|e| e.to_string())?;
        match &body {
            Value::String(s) => Ok(s.clone()),
            Value::Object(_) => {
                if let Some(id) = body.get("job_id").or_else(|| body.get("id")) {
                    Ok(id.as_str().unwrap_or_default().to_string())
                } else {
                    Err(format!("unexpected dispatch answer: {}", body))
                }
            }
            other => Err(format!("unexpected dispatch answer: {}", other)),
        }
    }

    fn run_script_by_path(&self, path: &str, args: &Value) -> Result<Value, String> {
        let resp = client()
            .post(format!(
                "{}/w/{}/scripts/run/p/{}?wait=true",
                windmill_base(),
                windmill_workspace(),
                path
            ))
            .bearer_auth(windmill_token())
            .json(&serde_json::json!({ "args": args }))
            .send()
            .map_err(|e| e.to_string())?;
        if !resp.status().is_success() {
            return Err(format!("HTTP {}", resp.status()));
        }
        resp.json::<Value>().map_err(|e| e.to_string())
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
}
