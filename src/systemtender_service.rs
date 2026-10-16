//! The systemtender lifecycle service, ported from
//! controller/systemtender_service.py. Envelope key orders, status
//! strings, and error texts are the parity surface.
//!
//! The CREATE EXECUTOR is not here: it stays a python leaf on the
//! script layer (python code windmill runs until cutover; its healed
//! #402 contract is untouched). create dispatches it by path, exactly
//! as python did.
//!
//! The trial-compatibility read goes straight at the optuna tables
//! the executor creates (studies/trials/trial_params/trial_values) -
//! python went through optuna's ORM; the statements here carry the
//! same meaning. The executor leaf owns that schema.

use crate::config::validate_minimal;
use crate::database::{
    ArchiveDatabaseRepository, DbConfig, DbGateway, MetadataDatabaseRepository, TextRow,
};
use crate::timefmt::{now_iso_naive, now_iso_utc_aware, pg_ts_to_iso};
use crate::wmill::{CausalClient, WindmillGateway};
use log::{error, info, warn};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::thread::sleep;
use uuid::Uuid;

pub const JOB_QUIESCE_POLL_SECONDS: u64 = 2;
pub const JOB_QUIESCE_TIMEOUT_SECONDS: u64 = 30;
pub const JOB_CLAIM_STALE_SECONDS: i64 = 300;
const TERMINAL_JOB_STATES: [&str; 3] = ["completed", "canceled", "failed"];

pub fn cancel_job_by_id(w: &dyn WindmillGateway, job_id: &str, reason: Option<&str>) -> bool {
    match w.cancel_job(job_id, reason) {
        Ok(()) => {
            info!("Successfully canceled Windmill job {}", job_id);
            true
        }
        Err(e) => {
            error!("Failed to cancel Windmill job {}: {}", job_id, e);
            false
        }
    }
}

pub fn job_reached_terminal_state(w: &dyn WindmillGateway, job_id: &str) -> bool {
    let job = match w.get_job(job_id) {
        Ok(j) => j,
        Err(e) => {
            // The queue endpoint can hard-error for a job whose
            // completed row already exists - probe the completed
            // endpoint the SDK itself uses for sync waits.
            warn!("jobs_u/get error polling {}: {} - probing completed endpoint", job_id, e);
            let text = e.to_lowercase();
            if text.contains("404") || text.contains("not found") {
                return true;
            }
            return match w.get_completed_maybe(job_id) {
                Ok(Some(res)) => {
                    if res.get("completed") == Some(&Value::Bool(true)) {
                        true
                    } else {
                        res.get("started") == Some(&Value::Bool(false))
                    }
                }
                Ok(None) => true,
                Err(e2) => {
                    warn!("completed-probe error for {}: {}", job_id, e2);
                    false
                }
            };
        }
    };
    let Some(job) = job else {
        // No job row at all: an accepted cancel removes queued jobs
        // outright. Nothing that never ran can hold a connection.
        return true;
    };
    let raw = job
        .get("status")
        .or_else(|| job.get("type"))
        .cloned()
        .unwrap_or_else(|| Value::String(String::new()));
    let mut status = raw.as_str().unwrap_or("").to_lowercase();
    if let Some(stripped) = status.strip_suffix("job") {
        status = stripped.to_string();
    }
    TERMINAL_JOB_STATES.contains(&status.as_str())
}

pub fn wait_for_jobs_to_quiesce(
    w: &dyn WindmillGateway,
    job_ids: &[String],
    timeout_seconds: u64,
) -> Vec<String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_seconds);
    let mut pending: Vec<String> = job_ids.to_vec();
    while !pending.is_empty() && std::time::Instant::now() < deadline {
        pending.retain(|j| !job_reached_terminal_state(w, j));
        if !pending.is_empty() {
            sleep(std::time::Duration::from_secs(JOB_QUIESCE_POLL_SECONDS));
        }
    }
    pending
}

/// Hash-based deterministic shard assignment with 10% overlap. The
/// modular arithmetic reproduces python's int(sha256_hex, 16) %
/// total_shards exactly, digit by digit.
fn sha256_mod(hex_digest: &str, modulus: u64) -> u64 {
    let mut acc: u64 = 0;
    for c in hex_digest.chars() {
        let d = c.to_digit(16).unwrap_or(0) as u64;
        acc = (acc.wrapping_mul(16).wrapping_add(d)) % modulus.max(1);
    }
    acc
}

pub fn determine_config_shard(
    run_id: i64,
    target_id: usize,
    targets_count: usize,
    config: &mut Value,
    parallel_runs_count: i64,
) {
    let supported_categories = ["sysctl", "sysfs", "cpufreq", "ethtool"];
    let Some(settings) = config.get_mut("settings").and_then(|s| s.as_object_mut()) else {
        return;
    };

    for category in supported_categories {
        let Some(settings_space) = settings.get_mut(category).and_then(|c| c.as_object_mut()) else {
            continue;
        };
        let keys: Vec<String> = settings_space.keys().cloned().collect();
        for setting_key in keys {
            let Some(setting_value) = settings_space.get_mut(&setting_key) else {
                continue;
            };
            if !setting_value.is_object() {
                continue;
            }
            let Some(constraints_list) =
                setting_value.get_mut("constraints").and_then(|c| c.as_array_mut())
            else {
                continue;
            };
            if constraints_list.is_empty() {
                continue;
            }
            if constraints_list[0].get("values").is_some() {
                continue; // categorical parameters are not shardable
            }

            let total_shards = (targets_count as i64 * parallel_runs_count).max(1) as u64;
            for (constraint_idx, constraint) in constraints_list.iter_mut().enumerate() {
                let Some(obj) = constraint.as_object_mut() else {
                    continue;
                };
                if !(obj.contains_key("step") && obj.contains_key("lower") && obj.contains_key("upper")) {
                    continue;
                }
                let lower = obj.get("lower").and_then(|v| v.as_f64());
                let upper = obj.get("upper").and_then(|v| v.as_f64());
                let (Some(lower), Some(upper)) = (lower, upper) else {
                    continue;
                };

                let worker_id = format!(
                    "{}_{}_{}_{}_{}",
                    run_id, target_id, category, setting_key, constraint_idx
                );
                let mut hasher = Sha256::new();
                hasher.update(worker_id.as_bytes());
                let digest = format!("{:x}", hasher.finalize());
                let shard_index = sha256_mod(&digest, total_shards);

                let delta = (upper - lower).abs();
                let shard_size = delta / total_shards as f64;
                let overlap = (shard_size * 0.10) as i64; // python int() truncates

                let mut new_lower = lower + shard_size * shard_index as f64;
                let mut new_upper = lower + shard_size * (shard_index + 1) as f64;
                new_lower = lower.max(new_lower - overlap as f64);
                new_upper = upper.min(new_upper + overlap as f64);

                obj.insert("lower".to_string(), json!(new_lower));
                obj.insert("upper".to_string(), json!(new_upper));
            }
        }
    }
}

pub fn start_optimization_flow(
    w: &dyn WindmillGateway,
    flow_id: &str,
    shard_config: &Value,
    run_id: i64,
    target_id: usize,
    systemtender_id: &str,
) -> Result<(String, String), String> {
    let script_path = "f/systemtender/engine/systemtender_worker";
    let script_inputs = json!({
        "config": shard_config,
        "systemtender_id": systemtender_id,
        "run_id": run_id,
        "target_id": target_id,
    });

    info!("Starting script {} at path: {}", flow_id, script_path);

    // Tag must be passed explicitly - windmill does NOT inherit the
    // script's stored tag for job routing.
    let job_id = w.run_script_by_path_async(script_path, &script_inputs, Some("systemtender"))?;
    info!("Flow {} started with job ID: {}", flow_id, job_id);
    Ok((flow_id.to_string(), job_id))
}

pub struct SystemtenderService {
    pub archive_repo: ArchiveDatabaseRepository,
    pub metadata_repo: MetadataDatabaseRepository,
    pub wmill: Arc<dyn WindmillGateway>,
    pub causal: Arc<dyn CausalClient>,
}

impl SystemtenderService {
    pub fn new() -> Self {
        Self {
            archive_repo: ArchiveDatabaseRepository::new(DbConfig::archive_from_env()),
            metadata_repo: MetadataDatabaseRepository::new(DbConfig::meta_from_env()),
            wmill: Arc::new(crate::wmill::HttpWindmill),
            causal: Arc::new(crate::wmill::HttpCausal),
        }
    }

    pub fn with_parts(
        db: Arc<dyn DbGateway>,
        wmill: Arc<dyn WindmillGateway>,
        causal: Arc<dyn CausalClient>,
    ) -> Self {
        Self {
            archive_repo: ArchiveDatabaseRepository::with_gateway(
                DbConfig::archive_from_env(),
                db.clone(),
            ),
            metadata_repo: MetadataDatabaseRepository::with_gateway(
                DbConfig::meta_from_env(),
                db,
            ),
            wmill,
            causal,
        }
    }

    fn normalize_constraints(&self, config: &mut Value) {
        fn normalize_dict(obj: Value) -> Value {
            match obj {
                Value::Object(mut m) => {
                    if m.len() == 1 && m.contains_key("values") {
                        return Value::Array(vec![Value::Object(m)]);
                    }
                    let keys: Vec<String> = m.keys().cloned().collect();
                    for k in keys {
                        if let Some(v) = m.remove(&k) {
                            m.insert(k, normalize_dict(v));
                        }
                    }
                    Value::Object(m)
                }
                Value::Array(a) => Value::Array(a.into_iter().map(normalize_dict).collect()),
                other => other,
            }
        }
        if let Some(settings) = config.get_mut("settings") {
            let normalized = normalize_dict(settings.clone());
            *settings = normalized;
        }
    }

    pub fn assign_watermark_slot(&self, systemtender_config: &mut Value) {
        let max_slots = 6;
        let mut used_slots: Vec<i64> = Vec::new();

        let list = self
            .metadata_repo
            .create_table()
            .ok()
            .and_then(|_| self.metadata_repo.fetch_systemtenders_list().ok());
        if let Some(rows) = list {
            for row in &rows {
                let existing_id = row[0].clone().unwrap_or_default();
                if let Ok(meta_row) = self.metadata_repo.fetch_meta_data(&existing_id) {
                    if let Some(first) = meta_row.first() {
                        if let Some(cfg) = first
                            .get(3)
                            .and_then(|c| c.as_deref())
                            .and_then(|t| serde_json::from_str::<Value>(t).ok())
                        {
                                if let Some(slot) = cfg
                                    .get("systemtender")
                                    .and_then(|s| s.get("watermark_slot"))
                                    .and_then(|v| v.as_i64())
                                {
                                    used_slots.push(slot);
                                }
                            }
                    }
                }
            }
        }

        if (used_slots.len() as i64) < max_slots {
            let assigned = (0..max_slots).find(|s| !used_slots.contains(s)).unwrap_or(0);
            systemtender_config
                .as_object_mut()
                .and_then(|o| o.get_mut("systemtender"))
                .and_then(|s| s.as_object_mut())
                .map(|s| s.insert("watermark_slot".to_string(), json!(assigned)));
            info!(
                "Assigned watermark slot {} (used: {:?})",
                assigned, used_slots
            );
        } else {
            warn!(
                "All {} watermark slots in use. Systemtender may share frequencies.",
                max_slots
            );
        }
    }

    fn resolve_target_refs(&self, systemtender_config: &mut Value) -> Result<(), String> {
        let target_refs: Vec<String> = systemtender_config
            .get("effectuation")
            .and_then(|e| e.get("targetRefs"))
            .and_then(|t| t.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        if target_refs.is_empty() {
            return Ok(());
        }

        info!("Resolving {} target references", target_refs.len());

        let mut resolved_targets: Vec<Value> = Vec::new();
        self.metadata_repo.create_targets_table().map_err(|e| e.0)?;

        for ref_id in &target_refs {
            let target_row = self
                .metadata_repo
                .fetch_target_by_id(ref_id)
                .ok()
                .flatten()
                .or_else(|| {
                    self.metadata_repo
                        .fetch_target_by_name(ref_id)
                        .ok()
                        .flatten()
                });
            let Some(row) = target_row else {
                return Err(format!(
                    "Cannot resolve target reference '{}'. Target not found in catalog by ID or name.",
                    ref_id
                ));
            };

            let target_type = row[2].clone().unwrap_or_default();
            let spec: Value = row
                .get(3)
                .and_then(|c| c.as_ref())
                .and_then(|t| serde_json::from_str(t).ok())
                .unwrap_or_else(|| json!({}));

            let mut entry = Map::new();
            entry.insert("id".to_string(), json!(row[0].clone().unwrap_or_default()));
            entry.insert("type".to_string(), Value::String(target_type));
            if let Some(spec_obj) = spec.as_object() {
                for (k, v) in spec_obj {
                    entry.insert(k.clone(), v.clone());
                }
            }
            resolved_targets.push(Value::Object(entry));
            info!(
                "Resolved target ref '{}' -> {}",
                ref_id,
                row[1].clone().unwrap_or_default()
            );
        }

        systemtender_config
            .as_object_mut()
            .and_then(|o| o.get_mut("effectuation"))
            .and_then(|e| e.as_object_mut())
            .map(|e| e.insert("targets".to_string(), Value::Array(resolved_targets.clone())));
        info!(
            "Resolved {} targets from {} references",
            resolved_targets.len(),
            target_refs.len()
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn launch_workers(
        &self,
        systemtender_id: &str,
        instance_name: &str,
        config: &Value,
    ) -> (Vec<String>, Vec<Value>) {
        let parallel_runs = config
            .get("run")
            .and_then(|r| r.get("parallel"))
            .and_then(|p| p.as_i64())
            .unwrap_or(1);
        let targets: Vec<Value> = config
            .get("effectuation")
            .and_then(|e| e.get("targets"))
            .and_then(|t| t.as_array())
            .cloned()
            .unwrap_or_default();
        let targets_count = targets.len();
        let is_cooperative = config
            .get("cooperation")
            .and_then(|c| c.get("active"))
            == Some(&Value::Bool(true));

        let mut failures: Vec<Value> = Vec::new();
        let mut job_ids: Vec<String> = Vec::new();

        for (target_count, _target) in targets.iter().enumerate() {
            for run_id in 0..parallel_runs {
                let mut flow_config = config.clone();
                let flow_id = format!("{}_{}_{}", instance_name, target_count, run_id);
                if !is_cooperative {
                    determine_config_shard(
                        run_id,
                        target_count,
                        targets_count,
                        &mut flow_config,
                        parallel_runs,
                    );
                }
                match start_optimization_flow(
                    self.wmill.as_ref(),
                    &flow_id,
                    &flow_config,
                    run_id,
                    target_count,
                    systemtender_id,
                ) {
                    Ok((_, job_id)) => job_ids.push(job_id),
                    Err(e) => {
                        failures.push(json!({
                            "flow_id": flow_id,
                            "target": target_count,
                            "run": run_id,
                            "error": e,
                        }));
                    }
                }
            }
        }
        (job_ids, failures)
    }

    pub fn create_systemtender(&self, mut systemtender_config: Value, name: &str) -> Value {
        let mut systemtender_uuid: Option<String> = None;
        let _ = &systemtender_uuid;

        let outcome = (|| -> Result<Value, String> {
            self.resolve_target_refs(&mut systemtender_config)?;
            validate_minimal(&systemtender_config, true).map_err(|e| e)?;

            self.metadata_repo.create_table().map_err(|e| e.0)?;

            // Duplicate by name answers from the visible state.
            let existing = self
                .metadata_repo
                .fetch_systemtenders_list()
                .map_err(|e| e.0)?;
            for eb in &existing {
                let eb_name = eb.get(1).cloned().flatten();
                let eb_id = eb.get(0).cloned().flatten();
                if eb_name.as_deref() == Some(name) {
                    if let Some(id) = &eb_id {
                        let lifecycle = self.metadata_repo.get_lifecycle(id).ok().flatten();
                        let dup_status = lifecycle
                            .as_ref()
                            .and_then(|l| l.get("state"))
                            .and_then(|s| s.as_str())
                            .unwrap_or("active")
                            .to_string();
                        let dup_message = match dup_status.as_str() {
                            "creating" => "Systemtender is still being created; poll GET until active or create-failed",
                            "create-failed" => "Systemtender creation failed; GET for the reason, DELETE to clear, then retry create",
                            "deleting" => "Systemtender is still deleting; poll GET until 404, then retry create",
                            "deletion-failed" => "Systemtender deletion failed; re-DELETE to retry, or GET for the reason",
                            _ => "Systemtender already exists, skipping duplicate creation",
                        };
                        info!(
                            "Systemtender '{}' already exists: {} (status: {}). Skipping creation to prevent duplicate workers.",
                            name, id, dup_status
                        );
                        let existing_row = self.metadata_repo.fetch_meta_data(id).ok();
                        let dup_created_at = existing_row
                            .as_ref()
                            .and_then(|r| r.first())
                            .and_then(|row| row.get(2))
                            .and_then(|c| c.as_ref())
                            .and_then(|t| pg_ts_to_iso(t));
                        return Ok(json!({
                            "result": "SUCCESS",
                            "duplicate": true,
                            "data": {
                                "id": id,
                                "name": name,
                                "status": dup_status,
                                "createdAt": dup_created_at,
                                "message": dup_message,
                            }
                        }));
                    }
                }
            }

            self.normalize_constraints(&mut systemtender_config);

            let uuid = Uuid::new_v4().to_string();
            systemtender_uuid = Some(uuid.clone());
            systemtender_config
                .as_object_mut()
                .and_then(|o| o.get_mut("systemtender"))
                .and_then(|s| s.as_object_mut())
                .map(|s| s.insert("uuid".to_string(), json!(uuid)));

            self.assign_watermark_slot(&mut systemtender_config);
            let creation_ts = now_iso_naive();

            self.metadata_repo
                .insert_systemtender_meta(&uuid, name, &creation_ts, &systemtender_config)
                .map_err(|e| e.0)?;
            self.metadata_repo
                .set_lifecycle(&uuid, "creating", None)
                .map_err(|e| e.0)?;

            self.wmill
                .run_script_by_path_async(
                    "f/controller/systemtender_create_executor",
                    &json!({"request_data": {"systemtender_id": uuid}}),
                    None,
                )
                .map_err(|e| e)?;

            info!(
                "Create accepted for systemtender '{}' ({}); executor dispatched, row in creating",
                name, uuid
            );
            Ok(json!({
                "result": "SUCCESS",
                "data": {
                    "id": uuid,
                    "name": name,
                    "status": "creating",
                    "createdAt": creation_ts,
                }
            }))
        })();

        match outcome {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to accept systemtender create: {}", e);
                if let Some(uuid) = &systemtender_uuid {
                    let db_name = format!("systemtender_{}", uuid.replace('-', "_"));
                    if let Err(cleanup_error) =
                        self.rollback_systemtender_creation(uuid, &db_name)
                    {
                        error!(
                            "Failed to cleanup after systemtender create failure: {}",
                            cleanup_error
                        );
                    }
                }
                json!({ "result": "FAILURE", "error": e })
            }
        }
    }

    fn rollback_systemtender_creation(&self, systemtender_uuid: &str, db_name: &str) -> Result<(), String> {
        warn!("Rolling back systemtender creation: {}", systemtender_uuid);
        if let Err(e) = self.metadata_repo.remove_systemtender_meta(systemtender_uuid) {
            error!(
                "Failed to delete metadata for systemtender {}: {}",
                systemtender_uuid, e.0
            );
        }
        if let Err(e) = self.metadata_repo.remove_lifecycle(systemtender_uuid) {
            error!(
                "Failed to remove lifecycle marker for systemtender {}: {}",
                systemtender_uuid, e.0
            );
        }
        if let Err(e) = self.archive_repo.drop_database(db_name) {
            error!(
                "Failed to delete archive database for systemtender {}: {}",
                systemtender_uuid, e.0
            );
        }
        Ok(())
    }

    pub fn get_systemtender(&self, systemtender_id: &str) -> Value {
        let outcome = (|| -> Result<Value, String> {
            self.metadata_repo.create_table().map_err(|e| e.0)?;
            let rows = self
                .metadata_repo
                .fetch_meta_data(systemtender_id)
                .map_err(|e| e.0)?;
            let Some(row) = rows.first() else {
                return Ok(json!({"result": "FAILURE", "error": "Systemtender not found"}));
            };

            let db_name = format!("systemtender_{}", systemtender_id.replace('-', "_"));
            let mut status = "active".to_string();
            let mut liveness: Value = Value::Null;
            let state = self
                .archive_repo
                .read_state_verdict(&db_name)
                .map_err(|e| e.0)?;
            if let Some(state) = &state {
                let interval = state
                    .get("interval_secs")
                    .and_then(|v| v.as_f64())
                    .filter(|v| *v != 0.0)
                    .unwrap_or(60.0);
                // python's max keeps its argument: the floor wins as
                // the int 90, the interval wins as a float - the
                // envelope carries that shape (parity)
                let threshold = (90.0f64).max(6.0 * interval);
                let threshold_json = if 6.0 * interval > 90.0 { json!(threshold) } else { json!(90) };
                let finished = state.get("finished") == Some(&Value::Bool(true));
                let age = state.get("age_secs").and_then(|v| v.as_f64());
                if finished {
                    status = "finished".to_string();
                } else if age.map(|a| a > threshold).unwrap_or(false) {
                    status = "presumed_dead".to_string();
                } else {
                    status = "running".to_string();
                }
                liveness = json!({
                    "age_secs": state.get("age_secs"),
                    "interval_secs": state.get("interval_secs"),
                    "stale_threshold_secs": threshold_json,
                });
            }

            // The worker's instruments (rung-7 field-sets), stamped in
            // its own state row - passed through verbatim; absent until
            // the worker has stamped once.
            let instruments = state
                .as_ref()
                .and_then(|s| s.get("instruments"))
                .cloned()
                .unwrap_or(Value::Null);

            let lifecycle = self.metadata_repo.get_lifecycle(systemtender_id).ok().flatten();
            let mut deletion_reason: Value = Value::Null;
            let mut creation_reason: Value = Value::Null;
            if let Some(lifecycle) = &lifecycle {
                if let Some(state) = lifecycle.get("state").and_then(|s| s.as_str()) {
                    status = state.to_string();
                    let reason = lifecycle.get("reason").cloned().unwrap_or(Value::Null);
                    if matches!(state, "deleting" | "deletion-failed") {
                        deletion_reason = reason;
                    } else if matches!(state, "creating" | "create-failed") {
                        creation_reason = reason;
                    }
                }
            }

            let config: Value = row
                .get(3)
                .and_then(|c| c.as_ref())
                .and_then(|t| serde_json::from_str(t).ok())
                .unwrap_or(Value::Null);

            // Rung 8: a death-resume is history, never covered - the
            // count and the last one ride every read.
            let history: Vec<Value> = config
                .get("relaunch_history")
                .and_then(|h| h.as_array())
                .cloned()
                .unwrap_or_default();

            Ok(json!({
                "result": "SUCCESS",
                "data": {
                    "id": row[0].clone().unwrap_or_default(),
                    "name": row[1].clone().unwrap_or_default(),
                    "status": status,
                    "deletion_reason": deletion_reason,
                    "creation_reason": creation_reason,
                    "liveness": liveness,
                    "instruments": instruments,
                    "createdAt": row.get(2).and_then(|c| c.as_ref()).and_then(|t| pg_ts_to_iso(t)),
                    "config": config,
                    "relaunches": json!(history.len()),
                    "last_relaunch": history.last().cloned().unwrap_or(Value::Null),
                }
            }))
        })();
        match outcome {
            Ok(v) => v,
            Err(e) => json!({ "result": "FAILURE", "error": e }),
        }
    }

    pub fn start_systemtender(&self, systemtender_id: &str) -> Value {
        let outcome = (|| -> Result<Value, String> {
            self.metadata_repo.create_table().map_err(|e| e.0)?;
            let rows = self
                .metadata_repo
                .fetch_meta_data(systemtender_id)
                .map_err(|e| e.0)?;
            let Some(row) = rows.first() else {
                warn!("Systemtender with ID '{}' not found", systemtender_id);
                return Ok(json!({
                    "result": "FAILURE",
                    "error": format!("Systemtender with ID '{}' not found", systemtender_id)
                }));
            };

            let mut config: Value = row
                .get(3)
                .and_then(|c| c.as_ref())
                .and_then(|t| serde_json::from_str(t).ok())
                .unwrap_or_else(|| json!({}));
            let instance_name = row[1].clone().unwrap_or_default();

            let db_name = format!("systemtender_{}", systemtender_id.replace('-', "_"));

            // Rung 8 (designs/2026-10-11 XX): the resume is explicit -
            // name the case this start resolves, never cover a death
            // as a fresh start. Same reads the GET verdict uses: a set
            // stop flag means STOPPED (a deliberate exit); heartbeat
            // silence without the flag means the worker DIED. A failed
            // case read answers nothing - the case stays fresh.
            let mut resume_case = "fresh";
            let mut death_age: Option<f64> = None;
            let mut stopped: Option<bool> = None;
            let state_read = (|| -> Result<Option<Value>, crate::database::DbError> {
                stopped = self.archive_repo.read_shutdown_requested(&db_name);
                self.archive_repo.read_state_verdict(&db_name)
            })();
            let state = match state_read {
                Ok(s) => s,
                Err(e) => {
                    warn!("Start case read failed for {}: {}", systemtender_id, e.0);
                    stopped = None;
                    None
                }
            };
            if stopped == Some(true) {
                resume_case = "stopped";
            } else if let Some(state) = &state {
                if state.get("finished") != Some(&Value::Bool(true)) {
                    let interval = state
                        .get("interval_secs")
                        .and_then(|v| v.as_f64())
                        .filter(|v| *v != 0.0)
                        .unwrap_or(60.0);
                    let threshold = (90.0f64).max(6.0 * interval);
                    let age = state.get("age_secs").and_then(|v| v.as_f64());
                    if age.map(|a| a > threshold).unwrap_or(false) {
                        resume_case = "presumed_dead";
                        death_age = age;
                    } else if age.map(|_| true).unwrap_or(false) {
                        resume_case = "running";
                    }
                }
            }

            // A death-resume is history, not identity: recorded, never
            // covered (rides the same metadata update as the job ids).
            if resume_case == "presumed_dead" {
                let mut history: Vec<Value> = config
                    .get("relaunch_history")
                    .and_then(|h| h.as_array())
                    .cloned()
                    .unwrap_or_default();
                history.push(json!({
                    "ts": now_iso_utc_aware(),
                    "resumed_from": "presumed_dead",
                    "liveness_age_secs": death_age,
                }));
                config
                    .as_object_mut()
                    .map(|o| o.insert("relaunch_history".to_string(), json!(history)));
            }

            // Clear the shutdown flag in archive DB
            self.archive_repo
                .set_shutdown_requested(&db_name, false)
                .map_err(|e| e.0)?;

            // Relaunch workers using the same logic as create_systemtender
            let (job_ids, failures) = self.launch_workers(systemtender_id, &instance_name, &config);

            // Update worker job IDs in metadata
            if !job_ids.is_empty() {
                config
                    .as_object_mut()
                    .map(|o| o.insert("worker_job_ids".to_string(), json!(job_ids)));
                self.metadata_repo
                    .update_systemtender_meta(systemtender_id, &config)
                    .map_err(|e| e.0)?;
            }

            // Handle any launch failures
            if !failures.is_empty() {
                error!(
                    "Failed to launch {} workers for systemtender {}",
                    failures.len(),
                    systemtender_id
                );
                return Ok(json!({
                    "result": "PARTIAL_SUCCESS",
                    "error": format!("Failed to launch {} worker(s)", failures.len()),
                    "workers_started": job_ids.len(),
                    "workers_failed": failures.len(),
                    "resumed_from": resume_case
                }));
            }

            info!("Successfully started/resumed systemtender: {}", systemtender_id);
            Ok(json!({
                "result": "SUCCESS",
                "data": {
                    "systemtender_id": systemtender_id,
                    "workers_started": job_ids.len(),
                    "status": "ACTIVE",
                    "resumed_from": resume_case
                }
            }))
        })();
        match outcome {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to start systemtender {}: {}", systemtender_id, e);
                json!({ "result": "FAILURE", "error": e })
            }
        }
    }

    pub fn stop_systemtender(&self, systemtender_id: &str) -> Value {
        let outcome = (|| -> Result<Value, String> {
            self.metadata_repo.create_table().map_err(|e| e.0)?;
            let rows = self
                .metadata_repo
                .fetch_meta_data(systemtender_id)
                .map_err(|e| e.0)?;
            if rows.is_empty() {
                warn!("Systemtender with ID '{}' not found", systemtender_id);
                return Ok(json!({
                    "result": "FAILURE",
                    "error": format!("Systemtender with ID '{}' not found", systemtender_id)
                }));
            }

            let db_name = format!("systemtender_{}", systemtender_id.replace('-', "_"));
            self.archive_repo
                .set_shutdown_requested(&db_name, true)
                .map_err(|e| e.0)?;

            info!("Graceful shutdown requested for systemtender: {}", systemtender_id);
            Ok(json!({
                "result": "SUCCESS",
                "message": "Graceful shutdown requested. Workers will stop after completing current trials.",
                "data": {
                    "systemtender_id": systemtender_id,
                    "shutdown_type": "graceful",
                    "note": "Monitor metrics for worker termination",
                }
            }))
        })();
        match outcome {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to request shutdown for systemtender {}: {}", systemtender_id, e);
                json!({ "result": "FAILURE", "error": e })
            }
        }
    }

    pub fn delete_systemtender(&self, systemtender_id: &str, _force: bool) -> Value {
        let outcome = (|| -> Result<Value, String> {
            self.metadata_repo.create_table().map_err(|e| e.0)?;
            let rows = self
                .metadata_repo
                .fetch_meta_data(systemtender_id)
                .map_err(|e| e.0)?;

            if rows.is_empty() {
                self.metadata_repo
                    .remove_lifecycle(systemtender_id)
                    .map_err(|e| e.0)?;
                info!("Delete of systemtender {}: already gone", systemtender_id);
                return Ok(json!({
                    "result": "SUCCESS",
                    "data": {
                        "systemtender_id": systemtender_id,
                        "delete_type": "already-gone",
                    }
                }));
            }

            if !self
                .metadata_repo
                .claim_for_deletion(systemtender_id, JOB_CLAIM_STALE_SECONDS)
                .map_err(|e| e.0)?
            {
                let lifecycle = self.metadata_repo.get_lifecycle(systemtender_id).ok().flatten();
                let state = lifecycle
                    .as_ref()
                    .and_then(|l| l.get("state"))
                    .cloned()
                    .unwrap_or(json!("deleting"));
                let reason = lifecycle
                    .as_ref()
                    .and_then(|l| l.get("reason"))
                    .cloned()
                    .unwrap_or(Value::Null);
                return Ok(json!({
                    "result": "SUCCESS",
                    "data": {
                        "systemtender_id": systemtender_id,
                        "status": state,
                        "reason": reason,
                        "note": "deletion already in progress",
                    }
                }));
            }

            let fail = |reason: String| -> Value {
                error!("Deletion failed for systemtender {}: {}", systemtender_id, reason);
                let _ = self
                    .metadata_repo
                    .set_lifecycle(systemtender_id, "deletion-failed", Some(&reason));
                json!({
                    "result": "FAILURE",
                    "error": reason,
                    "data": {
                        "systemtender_id": systemtender_id,
                        "status": "deletion-failed",
                    }
                })
            };

            let config: Value = rows[0]
                .get(3)
                .and_then(|c| c.as_ref())
                .and_then(|t| serde_json::from_str(t).ok())
                .unwrap_or_else(|| json!({}));
            let worker_job_ids: Vec<String> = config
                .get("worker_job_ids")
                .and_then(|w| w.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();

            let db_name = format!("systemtender_{}", systemtender_id.replace('-', "_"));

            let mut canceled_count = 0;
            for job_id in &worker_job_ids {
                if !cancel_job_by_id(
                    self.wmill.as_ref(),
                    job_id,
                    Some(&format!("Deleting systemtender {}", systemtender_id)),
                ) {
                    return Ok(fail(format!(
                        "cancel refused by windmill API for job {} - nothing dropped; retry the delete",
                        job_id
                    )));
                }
                canceled_count += 1;
            }
            info!(
                "Cancelled {}/{} worker jobs for systemtender {}",
                canceled_count,
                worker_job_ids.len(),
                systemtender_id
            );

            let stragglers =
                wait_for_jobs_to_quiesce(self.wmill.as_ref(), &worker_job_ids, JOB_QUIESCE_TIMEOUT_SECONDS);
            if !stragglers.is_empty() {
                return Ok(fail(format!(
                    "worker job(s) not terminal within {}s - windmill wedged, nothing dropped: {}",
                    JOB_QUIESCE_TIMEOUT_SECONDS,
                    stragglers.join(", ")
                )));
            }

            let group_id = config
                .get("interference_detection")
                .or_else(|| config.get("detection"))
                .and_then(|d| d.get("group"))
                .or_else(|| config.get("group"))
                .and_then(|g| g.as_str())
                .unwrap_or("default")
                .to_string();

            self.archive_repo.drop_database(&db_name).map_err(|e| e.0)?;
            self.archive_repo
                .cleanup_coordination_state(systemtender_id)
                .map_err(|e| e.0)?;

            match self.causal.purge_curves(systemtender_id) {
                Ok(status) => info!("Causal curve purge for {}: HTTP {}", systemtender_id, status),
                Err(e) => warn!(
                    "Causal curve purge failed for {} (ghost curves may linger in /curves): {}",
                    systemtender_id, e
                ),
            }

            let remaining = self.count_systemtenders_in_group(&group_id);
            if remaining == 0 {
                let _ = self.archive_repo.cleanup_group_lease(&group_id);
                info!("Purged group lease - last systemtender in group '{}' deleted", group_id);
            }

            self.metadata_repo
                .remove_systemtender_meta(systemtender_id)
                .map_err(|e| e.0)?;
            self.metadata_repo
                .remove_lifecycle(systemtender_id)
                .map_err(|e| e.0)?;

            info!("Successfully deleted systemtender: {}", systemtender_id);
            Ok(json!({
                "result": "SUCCESS",
                "data": {
                    "systemtender_id": systemtender_id,
                    "delete_type": "executed",
                    "workers_cancelled": canceled_count,
                }
            }))
        })();
        match outcome {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to delete systemtender {}: {}", systemtender_id, e);
                json!({ "result": "FAILURE", "error": e })
            }
        }
    }

    pub fn list_systemtenders(&self) -> Value {
        let outcome = (|| -> Result<Value, String> {
            self.metadata_repo.create_table().map_err(|e| e.0)?;
            let rows = self
                .metadata_repo
                .fetch_systemtenders_list()
                .map_err(|e| e.0)?;
            if rows.is_empty() {
                return Ok(json!({ "result": "SUCCESS", "data": [] }));
            }

            let lifecycle_states = self
                .metadata_repo
                .get_all_lifecycle()
                .map_err(|e| e.0)?;

            let mut configured: Vec<Value> = Vec::new();
            for row in &rows {
                let created_at = row
                    .get(2)
                    .and_then(|c| c.as_ref())
                    .and_then(|t| pg_ts_to_iso(t));
                let id = row[0].clone().unwrap_or_default();
                let state = lifecycle_states
                    .get(&id)
                    .cloned()
                    .unwrap_or(json!("active"));
                configured.push(json!({
                    "id": id,
                    "name": row[1].clone().unwrap_or_default(),
                    "status": state,
                    "createdAt": created_at,
                }));
            }
            Ok(json!({ "result": "SUCCESS", "data": configured }))
        })();
        match outcome {
            Ok(v) => v,
            Err(e) => json!({ "result": "FAILURE", "error": e }),
        }
    }

    fn count_systemtenders_in_group(&self, group_id: &str) -> i64 {
        let rows = match self
            .metadata_repo
            .create_table()
            .and_then(|_| self.metadata_repo.fetch_systemtenders_list())
        {
            Ok(r) => r,
            Err(e) => {
                warn!("Failed to count systemtenders in group {}: {}", group_id, e.0);
                return -1;
            }
        };
        let mut count = 0;
        for row in &rows {
            if let Ok(meta) = self.metadata_repo.fetch_meta_data(&row[0].clone().unwrap_or_default()) {
                if let Some(first) = meta.first() {
                    if let Some(cfg) = first
                        .get(3)
                        .and_then(|c| c.as_deref())
                        .and_then(|t| serde_json::from_str::<Value>(t).ok())
                    {
                        let bgroup = cfg
                            .get("interference_detection")
                            .or_else(|| cfg.get("detection"))
                            .and_then(|d| d.get("group"))
                            .or_else(|| cfg.get("group"))
                            .and_then(|g| g.as_str())
                            .unwrap_or("default");
                        if bgroup == group_id {
                            count += 1;
                        }
                    }
                }
            }
        }
        count
    }

    fn count_config_params(&self, config: &Value) -> i64 {
        let mut new_param_count: i64 = 0;
        let empty = Map::new();
        let settings = config.get("settings").and_then(|s| s.as_object()).unwrap_or(&empty);
        for category in ["sysctl", "sysfs", "cpufreq", "ethtool"] {
            if let Some(category_settings) = settings.get(category).and_then(|c| c.as_object()) {
                for (_param_name, param_config) in category_settings {
                    if category == "ethtool" {
                        if let Some(nested) = param_config.as_object() {
                            new_param_count += nested.len() as i64;
                        }
                    } else {
                        new_param_count += 1;
                    }
                }
            }
        }
        if new_param_count == 0 {
            for (_name, category_params) in settings {
                if let Some(params) = category_params.as_object() {
                    for (_pname, param_config) in params {
                        if param_config.is_object() && param_config.get("constraints").is_some() {
                            new_param_count += 1;
                        }
                    }
                }
            }
        }
        new_param_count
    }

    /// The optuna tables read directly (the executor leaf owns their
    /// schema; python read them through optuna's ORM).
    fn check_trial_compatibility(&self, systemtender_id: &str, new_config: &Value, force: bool) -> (bool, String) {
        if force {
            return (true, "Force mode: skipping compatibility check".to_string());
        }

        let db_name = format!("systemtender_{}", systemtender_id.replace('-', "_"));
        let run = |sql: &str| -> Result<Option<Vec<TextRow>>, String> {
            let cfg = DbConfig::archive_from_env();
            let repo = ArchiveDatabaseRepository::with_gateway(cfg, self.archive_repo_gateway());
            repo.raw_query(&db_name, sql).map_err(|e| e.0)
        };
        let _ = run;

        match (|| -> Result<(bool, String), String> {
            let studies = run("SELECT COUNT(*) FROM studies;")?;
            let count: i64 = studies
                .and_then(|r| r.first().cloned())
                .and_then(|row| row[0].clone())
                .and_then(|t| t.parse().ok())
                .unwrap_or(0);
            if count == 0 {
                return Ok((true, "No existing trial data found".to_string()));
            }
            let last = run(
                "SELECT trial_id FROM trials WHERE state = 'COMPLETE' ORDER BY trial_id DESC LIMIT 1;",
            )?;
            let Some(last) = last else {
                return Ok((true, "No completed trials".to_string()));
            };
            let trial_id = last[0][0].clone().unwrap_or_default();
            let params = run(&format!(
                "SELECT COUNT(*) FROM trial_params WHERE trial_id = '{}';",
                trial_id
            ))?;
            let old_param_count: i64 = params
                .and_then(|r| r.first().cloned())
                .and_then(|row| row[0].clone())
                .and_then(|t| t.parse().ok())
                .unwrap_or(0);
            let values = run(&format!(
                "SELECT COUNT(*) FROM trial_values WHERE trial_id = '{}';",
                trial_id
            ))?;
            let old_obj_count: i64 = values
                .and_then(|r| r.first().cloned())
                .and_then(|row| row[0].clone())
                .and_then(|t| t.parse().ok())
                .unwrap_or(0);

            let new_param_count = self.count_config_params(new_config);
            let new_obj_count = new_config
                .get("objectives")
                .and_then(|o| o.as_array())
                .map(|a| a.len() as i64)
                .unwrap_or(0);

            let mut incompatibilities: Vec<String> = Vec::new();
            if old_param_count != new_param_count {
                incompatibilities.push(format!(
                    "Parameter count: existing trials have {}, new config has {}",
                    old_param_count, new_param_count
                ));
            }
            if old_obj_count != new_obj_count {
                incompatibilities.push(format!(
                    "Objective count: existing trials have {}, new config has {}",
                    old_obj_count, new_obj_count
                ));
            }
            if !incompatibilities.is_empty() {
                let detail = incompatibilities.join("; ");
                return Ok((
                    false,
                    format!(
                        "Trial data incompatible: {}. Use force=true to clear trial history and start fresh.",
                        detail
                    ),
                ));
            }
            Ok((true, format!("Compatible: {} params, {} objectives", old_param_count, old_obj_count)))
        })() {
            Ok(v) => v,
            Err(e) => {
                warn!("Could not check trial compatibility: {}", e);
                (true, format!("Compatibility check skipped: {}", e))
            }
        }
    }

    fn archive_repo_gateway(&self) -> Arc<dyn DbGateway> {
        // the repository already carries its gateway; re-expose for the
        // raw compatibility reads
        self.archive_repo.gateway()
    }

    fn clear_trial_data(&self, systemtender_id: &str) {
        let db_name = format!("systemtender_{}", systemtender_id.replace('-', "_"));
        let cfg = DbConfig::archive_from_env();
        let repo = ArchiveDatabaseRepository::with_gateway(cfg, self.archive_repo_gateway());
        let tables = [
            "trials", "study_directions", "study_user_attributes",
            "study_system_attributes", "trial_user_attributes",
            "trial_system_attributes", "trial_params", "trial_values",
            "trial_intermediate_values", "study", "alembic_version",
        ];
        for table in tables {
            let _ = repo.raw_query(&db_name, &format!("DROP TABLE IF EXISTS {} CASCADE;", table));
        }
        info!("Cleared trial data for systemtender: {}", systemtender_id);
    }

    pub fn update_systemtender(&self, systemtender_id: &str, mut new_config: Value, force: bool) -> Value {
        let outcome = (|| -> Result<Value, String> {
            self.metadata_repo.create_table().map_err(|e| e.0)?;
            let rows = self
                .metadata_repo
                .fetch_meta_data(systemtender_id)
                .map_err(|e| e.0)?;
            let Some(row) = rows.first() else {
                return Ok(json!({
                    "result": "FAILURE",
                    "error": format!("Systemtender with ID '{}' not found", systemtender_id)
                }));
            };

            let old_config: Value = row
                .get(3)
                .and_then(|c| c.as_ref())
                .and_then(|t| serde_json::from_str(t).ok())
                .unwrap_or_else(|| json!({}));
            let instance_name = row[1].clone().unwrap_or_default();

            // new systemtender section defaults to the old one
            let old_st = old_config
                .get("systemtender")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let new_st = new_config.get("systemtender").cloned().unwrap_or(old_st);
            if let Some(o) = new_config.as_object_mut() {
                o.insert("systemtender".to_string(), new_st);
                o.get_mut("systemtender")
                    .and_then(|s| s.as_object_mut())
                    .map(|s| s.insert("uuid".to_string(), json!(systemtender_id)));
            }

            self.resolve_target_refs(&mut new_config)?;
            validate_minimal(&new_config, true).map_err(|e| e)?;

            let (compatible, compat_detail) =
                self.check_trial_compatibility(systemtender_id, &new_config, force);
            if !compatible {
                return Ok(json!({ "result": "FAILURE", "error": compat_detail }));
            }
            info!("Trial compatibility: {}", compat_detail);

            let db_name = format!("systemtender_{}", systemtender_id.replace('-', "_"));
            self.archive_repo
                .set_shutdown_requested(&db_name, true)
                .map_err(|e| e.0)?;

            sleep(std::time::Duration::from_secs(2));

            let old_worker_job_ids: Vec<String> = old_config
                .get("worker_job_ids")
                .and_then(|w| w.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();
            if !old_worker_job_ids.is_empty() {
                for job_id in &old_worker_job_ids {
                    cancel_job_by_id(
                        self.wmill.as_ref(),
                        job_id,
                        Some(&format!("Updating systemtender {}", systemtender_id)),
                    );
                }
                info!("Cancelled {} old worker jobs", old_worker_job_ids.len());
            }

            if force {
                self.clear_trial_data(systemtender_id);
                info!("Force mode: cleared trial data for systemtender {}", systemtender_id);
            }

            let mut config_history: Vec<Value> = old_config
                .get("config_history")
                .and_then(|h| h.as_array())
                .cloned()
                .unwrap_or_default();
            let mut old_snapshot = old_config.clone();
            if let Some(o) = old_snapshot.as_object_mut() {
                o.remove("config_history");
            }
            config_history.push(json!({
                "config": old_snapshot,
                "updated_at": now_iso_naive(),
            }));

            if let Some(o) = new_config.as_object_mut() {
                o.insert("config_history".to_string(), json!(config_history));
                if let Some(creation_ts) = old_config.get("creation_ts") {
                    if !creation_ts.is_null() {
                        o.insert("creation_ts".to_string(), creation_ts.clone());
                    }
                }
            }

            self.metadata_repo
                .update_systemtender_meta(systemtender_id, &new_config)
                .map_err(|e| e.0)?;

            let (job_ids, failures) = self.launch_workers(systemtender_id, &instance_name, &new_config);

            if !job_ids.is_empty() {
                if let Some(o) = new_config.as_object_mut() {
                    o.insert("worker_job_ids".to_string(), json!(job_ids));
                }
                self.metadata_repo
                    .update_systemtender_meta(systemtender_id, &new_config)
                    .map_err(|e| e.0)?;
            }

            if !failures.is_empty() {
                return Ok(json!({
                    "result": "PARTIAL_SUCCESS",
                    "error": format!("Failed to launch {} worker(s)", failures.len()),
                    "workers_started": job_ids.len(),
                    "workers_failed": failures.len(),
                    "trials_cleared": force,
                }));
            }

            info!("Successfully updated systemtender: {} (force={})", systemtender_id, force);
            Ok(json!({
                "result": "SUCCESS",
                "data": {
                    "systemtender_id": systemtender_id,
                    "name": instance_name,
                    "status": "active",
                    "workers_started": job_ids.len(),
                    "trials_cleared": force,
                    "config_history_entries": config_history.len(),
                }
            }))
        })();
        match outcome {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to update systemtender {}: {}", systemtender_id, e);
                json!({ "result": "FAILURE", "error": e })
            }
        }
    }
}
