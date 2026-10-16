//! Port of tests/unit/test_systemtenders.py — the lifecycle suite.
//!
//! Python's service-class mocks port one layer down: the service over
//! the fakes, asserting the same envelopes the mocked tests asserted
//! (the real code paths, not Mock passthroughs). Verb-level cases ride
//! verbs::dispatch, which answers before any service is built.

use godon_controller::database::TextRow;
use godon_controller::systemtender_service::{
    job_reached_terminal_state, wait_for_jobs_to_quiesce, SystemtenderService,
};
use godon_controller::verbs::dispatch;
use godon_controller::wmill::WindmillGateway;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Instant;

use crate::support::{arc_db, row, FakeCausal, FakeDb, FakeWindmill};

const DEFINITION: &str = r#"{"worker_job_ids": ["job-1"], "interference_detection": {"group": "bench"}}"#;

fn service_with_jobs(
    job_ids: &[&str],
    wmill: Arc<FakeWindmill>,
) -> (SystemtenderService, Arc<FakeDb>) {
    let ids = job_ids.to_vec();
    let definition = {
        let mut d = json!({});
        if !ids.is_empty() {
            d["worker_job_ids"] = json!(ids);
        }
        d["interference_detection"] = json!({"group": "bench"});
        serde_json::to_string(&d).unwrap()
    };
    let db = arc_db(move |db_name, q| {
        // the claim is an INSERT that RETURNS rows - answer it before
        // the insert-catcher swallows it
        if q.contains("RETURNING systemtender_id") {
            return Ok(Some(vec![row(&[Some("row-id")])]));
        }
        if q.contains("CREATE TABLE") || q.trim_start().starts_with("INSERT INTO") {
            return Ok(None);
        }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            return Ok(Some(vec![row(&[
                Some("row-id"),
                Some("bench-tender"),
                Some("2026-09-30 18:00:00+00"),
                Some(definition.as_str()),
            ])]));
        }
        if q.contains("SELECT systemtender_id, state FROM systemtender_lifecycle") {
            return Ok(Some(vec![]));
        }
        if q.contains("FROM systemtender_lifecycle WHERE systemtender_id") {
            return Ok(Some(vec![]));
        }
        if q.contains("FROM systemtender_state") {
            // a live tender: finished, fresh beat
            return Ok(Some(vec![row(&[Some("f"), Some("5.0"), Some("10.0")])]));
        }
        if q.contains("SELECT id, name, creation_tsz FROM") {
            return Ok(Some(vec![]));
        }
        let _ = db_name;
        Ok(None)
    });
    let svc = SystemtenderService::with_parts(db.clone(), wmill, Arc::new(FakeCausal::default()));
    (svc, db)
}

fn plain_service() -> (SystemtenderService, Arc<FakeDb>) {
    service_with_jobs(&[], Arc::new(FakeWindmill::default()))
}

// ── retrieval ──────────────────────────────────────────────────────

#[test]
fn get_missing_id_fails() {
    let r = dispatch("systemtender_get", &json!({}));
    assert_eq!(r["result"], "FAILURE");
    assert!(r["error"].as_str().unwrap().contains("Missing systemtender_id"));
}

#[test]
fn get_unknown_tender_answers_not_found() {
    let db = arc_db(|_db, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    });
    let svc = SystemtenderService::with_parts(db, Arc::new(FakeWindmill::default()), Arc::new(FakeCausal::default()));
    let r = svc.get_systemtender("no-such-tender");
    assert_eq!(r["result"], "FAILURE");
    assert_eq!(r["error"], "Systemtender not found");
}

#[test]
fn get_returns_the_row_with_its_liveness() {
    let db = arc_db(|_db, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            return Ok(Some(vec![row(&[
                Some("t-1"),
                Some("test-systemtender"),
                Some("2026-01-01 00:00:00+00"),
                Some(r#"{"type": "linux_performance"}"#),
            ])]));
        }
        if q.contains("FROM systemtender_state") {
            return Ok(Some(vec![row(&[Some("f"), Some("5.0"), Some("10.0")])]));
        }
        if q.contains("FROM systemtender_lifecycle WHERE systemtender_id") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    });
    let svc = SystemtenderService::with_parts(db, Arc::new(FakeWindmill::default()), Arc::new(FakeCausal::default()));
    let r = svc.get_systemtender("t-1");
    assert_eq!(r["result"], "SUCCESS");
    assert_eq!(r["data"]["id"], "t-1");
    assert_eq!(r["data"]["name"], "test-systemtender");
    assert_eq!(r["data"]["status"], "running");
    assert_eq!(r["data"]["createdAt"], "2026-01-01T00:00:00+00:00");
}

// ── listing ────────────────────────────────────────────────────────

#[test]
fn list_empty_answers_success_with_empty_data() {
    let db = arc_db(|_db, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.contains("SELECT id, name, creation_tsz FROM") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    });
    let svc = SystemtenderService::with_parts(db, Arc::new(FakeWindmill::default()), Arc::new(FakeCausal::default()));
    let r = svc.list_systemtenders();
    assert_eq!(r["result"], "SUCCESS");
    assert_eq!(r["data"], json!([]));
}

#[test]
fn list_answers_every_row() {
    let db = arc_db(|_db, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.contains("SELECT id, name, creation_tsz FROM") {
            return Ok(Some(vec![
                row(&[Some("id-1"), Some("systemtender1"), Some("2026-01-01 00:00:00+00")]),
                row(&[Some("id-2"), Some("systemtender2"), Some("2026-01-02 00:00:00+00")]),
            ]));
        }
        if q.contains("SELECT systemtender_id, state FROM systemtender_lifecycle") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    });
    let svc = SystemtenderService::with_parts(db, Arc::new(FakeWindmill::default()), Arc::new(FakeCausal::default()));
    let r = svc.list_systemtenders();
    assert_eq!(r["result"], "SUCCESS");
    let data = r["data"].as_array().unwrap();
    assert_eq!(data.len(), 2);
    assert_eq!(data[0]["id"], "id-1");
    assert_eq!(data[0]["name"], "systemtender1");
    assert_eq!(data[1]["id"], "id-2");
}

// ── deletion: the verb door ────────────────────────────────────────

#[test]
fn delete_missing_id_fails() {
    let r = dispatch("systemtender_delete", &json!({}));
    assert_eq!(r["result"], "FAILURE");
    assert!(r["error"].as_str().unwrap().contains("Missing systemtender_id"));
}

// ── deletion: the quiesce machine (#399's guard) ───────────────────

#[test]
fn delete_fails_closed_when_cancel_request_fails() {
    let wmill = FakeWindmill::default();
    wmill.cancel_results.lock().unwrap().push_back(Err("HTTP 500".to_string()));
    let (svc, db) = service_with_jobs(&["job-1"], Arc::new(wmill));
    let r = svc.delete_systemtender("bench-tender", true);
    assert_eq!(r["result"], "FAILURE");
    assert!(r["error"].as_str().unwrap().contains("cancel refused"));
    // deletion-failed is flagged with the job named...
    let flagged = db
        .queries()
        .iter()
        .any(|q| q.contains("'deletion-failed'") && q.contains("job-1"));
    assert!(flagged, "the lifecycle row carries the reason");
    // ...and nothing was dropped: the relcache race stays shut (#399)
    assert!(!db.queries().iter().any(|q| q.contains("DROP DATABASE")));
    assert!(!db.queries().iter().any(|q| q.contains("DELETE FROM systemtender_meta_data")));
}

#[test]
fn delete_is_idempotent_when_tender_already_gone() {
    let db = arc_db(|_db, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            return Ok(Some(vec![]));
        }
        if q.trim_start().starts_with("DELETE FROM systemtender_lifecycle") {
            return Ok(None);
        }
        Ok(None)
    });
    let svc = SystemtenderService::with_parts(db.clone(), Arc::new(FakeWindmill::default()), Arc::new(FakeCausal::default()));
    let r = svc.delete_systemtender("bench-tender", true);
    assert_eq!(r["result"], "SUCCESS");
    assert_eq!(r["data"]["delete_type"], "already-gone");
    // the stale lifecycle row is swept with it
    assert!(db
        .queries()
        .iter()
        .any(|q| q.trim_start().starts_with("DELETE FROM systemtender_lifecycle")));
}

#[test]
fn delete_rides_along_when_an_executor_holds_the_claim() {
    // claim answers no row: another executor owns the deletion
    let db = arc_db(|_db, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.contains("RETURNING systemtender_id") {
            return Ok(Some(vec![]));
        }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            return Ok(Some(vec![row(&[
                Some("row-id"), Some("bench-tender"), Some("2026-09-30 18:00:00+00"), Some(DEFINITION),
            ])]));
        }
        if q.contains("FROM systemtender_lifecycle WHERE systemtender_id") {
            return Ok(Some(vec![row(&[Some("deleting"), None])]));
        }
        Ok(None)
    });
    let svc = SystemtenderService::with_parts(db.clone(), Arc::new(FakeWindmill::default()), Arc::new(FakeCausal::default()));
    let r = svc.delete_systemtender("bench-tender", true);
    assert_eq!(r["result"], "SUCCESS");
    assert_eq!(r["data"]["status"], "deleting");
    assert_eq!(r["data"]["note"], "deletion already in progress");
    assert!(!db.queries().iter().any(|q| q.contains("DROP DATABASE")));
}

#[test]
fn delete_waits_for_terminal_jobs_before_drop() {
    let wmill = FakeWindmill::default();
    // rounds 1 and 2: both jobs still running; round 3: both gone
    wmill.queue_job(Some(json!({"status": "Running"})));
    wmill.queue_job(Some(json!({"status": "Running"})));
    wmill.queue_job(Some(json!({"status": "Running"})));
    wmill.queue_job(Some(json!({"status": "Running"})));
    let (svc, db) = service_with_jobs(&["job-1", "job-2"], Arc::new(wmill));
    let started = Instant::now();
    let r = svc.delete_systemtender("bench-tender", true);
    let elapsed = started.elapsed().as_secs_f64();
    assert_eq!(r["result"], "SUCCESS");
    assert!(db.queries().iter().any(|q| q.contains("DROP DATABASE")));
    // two poll rounds elapsed before both jobs read terminal
    assert!(elapsed >= 3.5, "waited only {:.1}s", elapsed);
}

#[test]
fn wait_fails_closed_when_a_job_never_quiesces() {
    // the bounded wait itself (the service path rides the same fn with
    // the estate's 30s bound - too slow for CI, pinned here at 1s)
    let wmill = Arc::new(FakeWindmill::default());
    wmill.queue_job(Some(json!({"status": "Running"})));
    let stragglers = wait_for_jobs_to_quiesce(wmill.as_ref(), &["job-stuck".to_string()], 1);
    assert_eq!(stragglers, vec!["job-stuck".to_string()]);
}

#[test]
fn delete_proceeds_when_job_history_is_gone() {
    // get_job's default answer is gone (no row) - terminal
    let (svc, db) = service_with_jobs(&["job-gone"], Arc::new(FakeWindmill::default()));
    let r = svc.delete_systemtender("bench-tender", true);
    assert_eq!(r["result"], "SUCCESS");
    assert!(db.queries().iter().any(|q| q.contains("DROP DATABASE")));
}

// ── the terminal-state reader (shape-proof across windmill generations) ──

#[test]
fn terminal_state_maps_status_wordings() {
    let w = Arc::new(FakeWindmill::default());
    w.queue_job(Some(json!({"status": "Canceled"})));
    assert!(job_reached_terminal_state(w.as_ref(), "job-x"));
    let w = Arc::new(FakeWindmill::default());
    w.queue_job(Some(json!({"status": "Canceling"})));
    assert!(!job_reached_terminal_state(w.as_ref(), "job-x"));
    let w = Arc::new(FakeWindmill::default());
    w.queue_job(Some(json!({"status": "completed"})));
    assert!(job_reached_terminal_state(w.as_ref(), "job-x"));
}

#[test]
fn terminal_state_reads_v2_type_shape() {
    for word in ["CompletedJob", "CanceledJob", "FailedJob"] {
        let w = Arc::new(FakeWindmill::default());
        w.queue_job(Some(json!({"type": word, "workspace_id": "godon"})));
        assert!(job_reached_terminal_state(w.as_ref(), "job-x"), "{word}");
    }
    for word in ["RunningJob", "QueuedJob"] {
        let w = Arc::new(FakeWindmill::default());
        w.queue_job(Some(json!({"type": word})));
        assert!(!job_reached_terminal_state(w.as_ref(), "job-x"), "{word}");
    }
}

#[test]
fn terminal_state_counts_404_as_gone() {
    let w = Arc::new(FakeWindmill::default());
    w.job_results.lock().unwrap().push_back(Err("HTTP 404 Not Found".to_string()));
    assert!(job_reached_terminal_state(w.as_ref(), "job-x"));
}

#[test]
fn no_job_row_at_all_is_terminal() {
    let w = Arc::new(FakeWindmill::default()); // default: gone
    assert!(job_reached_terminal_state(w.as_ref(), "job-x"));
}

#[test]
fn plain_service_never_panics_on_empty_ground() {
    let (_svc, db) = plain_service();
    assert!(db.queries().is_empty());
}

// ── the terminal-state reader: probe fallbacks ─────────────────────

#[test]
fn terminal_state_falls_back_to_completed_on_endpoint_error() {
    let w = Arc::new(FakeWindmill::default());
    w.job_results.lock().unwrap().push_back(Err("HTTP 400: Bad Request".to_string()));
    w.queue_completed(Some(json!({"completed": true, "success": false, "started": true})));
    assert!(job_reached_terminal_state(w.as_ref(), "job-x"));
}

#[test]
fn terminal_state_counts_cancelled_never_started_as_gone() {
    let w = Arc::new(FakeWindmill::default());
    w.job_results.lock().unwrap().push_back(Err("HTTP 404: Not Found".to_string()));
    assert!(job_reached_terminal_state(w.as_ref(), "job-x"));

    let w = Arc::new(FakeWindmill::default());
    w.job_results.lock().unwrap().push_back(Err("HTTP 400: Bad Request".to_string()));
    w.queue_completed(Some(json!({"completed": false, "started": false})));
    assert!(job_reached_terminal_state(w.as_ref(), "job-x"));
}

#[test]
fn terminal_state_raw_surface_running_stays_non_terminal() {
    let w = Arc::new(FakeWindmill::default());
    w.queue_job(Some(json!({"type": "RunningJob"})));
    w.queue_completed(Some(json!({"completed": false, "started": true})));
    assert!(!job_reached_terminal_state(w.as_ref(), "job-x"));
}

#[test]
fn terminal_state_fails_closed_when_running_or_probe_errors() {
    let w = Arc::new(FakeWindmill::default());
    w.job_results.lock().unwrap().push_back(Err("HTTP 400: Bad Request".to_string()));
    w.queue_completed(Some(json!({"completed": false, "started": true})));
    assert!(!job_reached_terminal_state(w.as_ref(), "job-x"));
    let w = Arc::new(FakeWindmill::default());
    w.job_results.lock().unwrap().push_back(Err("HTTP 400: Bad Request".to_string()));
    w.completed_results.lock().unwrap().push_back(Err("HTTP 500".to_string()));
    assert!(!job_reached_terminal_state(w.as_ref(), "job-x"));
}

// ── stop / start doors ─────────────────────────────────────────────

#[test]
fn stop_missing_id_fails() {
    let r = dispatch("systemtender_stop", &json!({}));
    assert_eq!(r["result"], "FAILURE");
    assert!(r["error"].as_str().unwrap().contains("Missing systemtender_id"));
}

fn missing_tender_db() -> Arc<FakeDb> {
    arc_db(|_db, q| {
        if q.contains("CREATE TABLE") { return Ok(None); }
        if q.contains("FROM systemtender_meta_data WHERE id") { return Ok(Some(vec![])); }
        Ok(None)
    })
}

#[test]
fn stop_unknown_tender_answers_not_found() {
    let svc = SystemtenderService::with_parts(missing_tender_db(), Arc::new(FakeWindmill::default()), Arc::new(FakeCausal::default()));
    let r = svc.stop_systemtender("no-such");
    assert_eq!(r["result"], "FAILURE");
    assert!(r["error"].as_str().unwrap().contains("not found"));
}

#[test]
fn stop_sets_the_graceful_flag() {
    let db = arc_db(|_db, q| {
        if q.contains("CREATE TABLE") { return Ok(None); }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            return Ok(Some(vec![row(&[Some("t-1"), Some("bench-tender"), Some("2026-10-10 09:00:00+00"), Some("{}")])]));
        }
        Ok(None)
    });
    let svc = SystemtenderService::with_parts(db.clone(), Arc::new(FakeWindmill::default()), Arc::new(FakeCausal::default()));
    let r = svc.stop_systemtender("t-1");
    assert_eq!(r["result"], "SUCCESS");
    assert_eq!(r["data"]["shutdown_type"], "graceful");
    assert!(db.queries().iter().any(|q| q.contains("shutdown_requested = TRUE")));
}

#[test]
fn start_missing_id_fails() {
    let r = dispatch("systemtender_start", &json!({}));
    assert_eq!(r["result"], "FAILURE");
    assert!(r["error"].as_str().unwrap().contains("Missing systemtender_id"));
}

#[test]
fn start_unknown_tender_answers_not_found() {
    let svc = SystemtenderService::with_parts(missing_tender_db(), Arc::new(FakeWindmill::default()), Arc::new(FakeCausal::default()));
    let r = svc.start_systemtender("no-such");
    assert_eq!(r["result"], "FAILURE");
    assert!(r["error"].as_str().unwrap().contains("not found"));
}

// NOTE start-success + TestDeathRelaunch BLOCKED: the python service
// gained the death-relaunch surfaces after this port's service read;
// a service-parity motion ports those first.

// ── worker cancellation ────────────────────────────────────────────

#[test]
fn cancel_job_success_and_failure() {
    use godon_controller::systemtender_service::cancel_job_by_id;
    let w = Arc::new(FakeWindmill::default());
    assert!(cancel_job_by_id(w.as_ref(), "test-job-id", None));
    let w = Arc::new(FakeWindmill::default());
    w.cancel_results.lock().unwrap().push_back(Err("refused".to_string()));
    assert!(!cancel_job_by_id(w.as_ref(), "test-job-id", None));
}

#[test]
fn cancel_carries_the_reason_to_the_api() {
    use godon_controller::systemtender_service::cancel_job_by_id;
    let w = Arc::new(FakeWindmill::default());
    assert!(cancel_job_by_id(w.as_ref(), "test-job-id", Some("Test cancellation")));
    let calls = w.cancelled.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "test-job-id");
    assert_eq!(calls[0].1.as_deref(), Some("Test cancellation"));
}

// ── response format + the liveness floor ───────────────────────────

#[test]
fn get_response_carries_the_contract_keys() {
    let db = arc_db(|_db, q| {
        if q.contains("CREATE TABLE") { return Ok(None); }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            return Ok(Some(vec![row(&[Some("t-1"), Some("n"), Some("2026-01-01 00:00:00+00"), Some(r#"{"type": "x"}"#)])]));
        }
        if q.contains("FROM systemtender_state") {
            return Ok(Some(vec![row(&[Some("f"), Some("5.0"), Some("10.0")])]));
        }
        if q.contains("FROM systemtender_lifecycle WHERE systemtender_id") { return Ok(Some(vec![])); }
        Ok(None)
    });
    let svc = SystemtenderService::with_parts(db, Arc::new(FakeWindmill::default()), Arc::new(FakeCausal::default()));
    let r = svc.get_systemtender("t-1");
    for key in ["id", "name", "status", "createdAt", "config"] {
        assert!(r["data"].get(key).is_some(), "missing {key}");
    }
}

fn liveness_db(finished: &str, age: &str, interval: &str) -> Arc<FakeDb> {
    let finished = finished.to_string();
    let age = age.to_string();
    let interval = interval.to_string();
    arc_db(move |_db, q| {
        if q.contains("CREATE TABLE") { return Ok(None); }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            return Ok(Some(vec![row(&[Some("row-id"), Some("test-tender"), Some("2026-09-26 18:00:00+00"), Some("{}")])]));
        }
        if q.contains("FROM systemtender_state") {
            return Ok(Some(vec![row(&[Some(finished.as_str()), Some(age.as_str()), Some(interval.as_str())])]));
        }
        if q.contains("FROM systemtender_lifecycle WHERE systemtender_id") { return Ok(Some(vec![])); }
        Ok(None)
    })
}

#[test]
fn abort_burst_within_floor_reads_running() {
    let svc = SystemtenderService::with_parts(liveness_db("f", "45", "10"), Arc::new(FakeWindmill::default()), Arc::new(FakeCausal::default()));
    let out = svc.get_systemtender("some-uuid");
    assert_eq!(out["result"], "SUCCESS");
    assert_eq!(out["data"]["status"], "running");
    assert_eq!(out["data"]["liveness"]["stale_threshold_secs"], 90);
}

#[test]
fn true_death_past_floor_reads_presumed_dead() {
    let svc = SystemtenderService::with_parts(liveness_db("f", "300", "10"), Arc::new(FakeWindmill::default()), Arc::new(FakeCausal::default()));
    assert_eq!(svc.get_systemtender("some-uuid")["data"]["status"], "presumed_dead");
}

#[test]
fn finished_stamp_wins_over_liveness() {
    let svc = SystemtenderService::with_parts(liveness_db("t", "999", "10"), Arc::new(FakeWindmill::default()), Arc::new(FakeCausal::default()));
    assert_eq!(svc.get_systemtender("some-uuid")["data"]["status"], "finished");
}

// ── the receiver note (run 36307488912) ────────────────────────────

use godon_controller::wish_service::WishService;

fn planned_response() -> Value {
    json!({
        "status": "planned",
        "moves": [{"param": "param_1", "sender": "f0e0fb34-aaaa-bbbb-cccc-ddddeeeeffff", "setting": 50.0, "bars": 0.02}],
        "path": ["f0e0fb34-aaaa-bbbb-cccc-ddddeeeeffff", "92a33fa9-1111-2222-3333-444455556666"],
        "predicted": {"value": 1.85, "bars": 0.02},
    })
}

fn wish_door_db(heartbeat: &'static str) -> Arc<FakeDb> {
    arc_db(move |_db, q| {
        if q.contains("CREATE TABLE") || q.trim_start().starts_with("INSERT INTO") { return Ok(None); }
        if q.contains("EXTRACT(EPOCH") {
            return Ok(Some(vec![row(&[Some(heartbeat), Some("10.0")])]));
        }
        if q.contains("WHERE id = 'f0e0fb34") || q.contains("WHERE id = '92a33fa9") {
            let id = if q.contains("f0e0fb34") { "f0e0fb34-aaaa-bbbb-cccc-ddddeeeeffff" } else { "92a33fa9-1111-2222-3333-444455556666" };
            return Ok(Some(vec![row(&[Some(id), Some("tender"), None, Some("{}")])]));
        }
        if q.contains("FROM steerwishes") {
            return Ok(Some(vec![row(&[
                Some("wish-1"), Some("rcv/objective_0"), None, None, None,
                Some("standing"), Some("2026-10-12 19:00:00+00"), Some("declared"),
                None, None, None, Some("later"), Some("best"),
            ])]));
        }
        if q.contains("FROM steerwish") { return Ok(Some(vec![])); }
        Ok(None)
    })
}

fn sender_and_receiver_notes(planned: Value) -> (WishService, Arc<FakeDb>, Arc<FakeCausal>) {
    let causal = Arc::new(FakeCausal::default());
    causal.queue_ask(Ok(planned));
    let db = wish_door_db("5.0");
    let svc = WishService::with_parts(db.clone(), Arc::new(FakeWindmill::default()), causal.clone(), true);
    (svc, db, causal)
}

fn declare_wish_payload() -> Value {
    json!({
        "type": "steer",
        "claims": [{"outcome": "rcv/objective_0", "band": {"lo": 1.7, "hi": 2.0, "target": 1.85}}],
        "answer": "now",
    })
}

#[test]
fn sender_and_receiver_notes_planted() {
    let (svc, db, _c) = sender_and_receiver_notes(planned_response());
    let declared = svc.create_wish(&declare_wish_payload());
    let _ = declared.unwrap_or_else(|e| panic!("create_wish errored: {e}"));
    let queries = db.queries();
    let assignments: Vec<&String> = queries
        .iter()
        .filter(|q| q.contains("INSERT INTO wish_assignments"))
        .collect();
    assert!(
        !assignments.is_empty() || true,
        "debug: {} assignment inserts, queries: {:#?}",
        assignments.len(),
        queries
    );
    assert!(queries.iter().any(|q| q.contains("INSERT INTO wish_assignments") && q.contains("systemtender_f0e0fb34") && q.contains("'sender'")));
    assert!(queries.iter().any(|q| q.contains("INSERT INTO wish_assignments") && q.contains("systemtender_92a33fa9") && q.contains("'receiver'")));
}

#[test]
fn write_wish_assignment_defaults_to_sender_role() {
    let (svc, db, _c) = sender_and_receiver_notes(planned_response());
    let declared = svc.create_wish(&declare_wish_payload());
    let _ = declared.unwrap_or_else(|e| panic!("create_wish errored: {e}"));
    let first = db.queries().into_iter().find(|q| q.contains("INSERT INTO wish_assignments")).expect("planted");
    assert!(first.contains("'sender'"));
}

#[test]
fn no_receiver_note_when_self_wish() {
    let mut plan = planned_response();
    plan["path"] = json!(["f0e0fb34-aaaa-bbbb-cccc-ddddeeeeffff"]);
    plan["moves"][0]["sender"] = json!("f0e0fb34-aaaa-bbbb-cccc-ddddeeeeffff");
    let (svc, db, _c) = sender_and_receiver_notes(plan);
    let declared = svc.create_wish(&declare_wish_payload());
    let _ = declared.unwrap_or_else(|e| panic!("create_wish errored: {e}"));
    let count = db.queries().iter().filter(|q| q.contains("INSERT INTO wish_assignments")).count();
    assert_eq!(count, 1);
}

// ── the async create contract (designs/2026-10-10) ────────────────

fn create_config() -> Value {
    json!({
        "meta": {"configVersion": "0.3"},
        "systemtender": {"type": "linux_performance"},
        "objectives": [{"name": "latency", "reconnaissance": {"service": "prometheus", "query": "rate(x)"}}],
        "run": {"parallel": 2},
        "effectuation": {"targetRefs": ["t1"]},
        "settings": {"sysctl": {"vm.swappiness": {"constraints": [{"step": 5, "lower": 0, "upper": 100}]}}},
        "cooperation": {"active": true},
    })
}

fn fast_create_db() -> Arc<FakeDb> {
    arc_db(|_db, q| {
        if q.contains("FROM targets") {
            return Ok(Some(vec![row(&[Some("t1"), Some("t1"), Some("ssh"), Some(r#"{"address": "host-a"}"#), None, Some("2026-10-10 09:00:00+00"), None])])); 
        }
        if q.contains("CREATE TABLE") || q.trim_start().starts_with("INSERT INTO") { return Ok(None); }
        if q.contains("SELECT id, name, creation_tsz FROM") { return Ok(Some(vec![])); }
        if q.contains("FROM systemtender_meta_data WHERE id") { return Ok(Some(vec![])); }
        if q.contains("FROM systemtender_lifecycle WHERE systemtender_id") { return Ok(Some(vec![])); }
        if q.contains("FROM steerwishes") || q.contains("FROM steerwish") { return Ok(Some(vec![])); }
        Ok(None)
    })
}

#[test]
fn create_plants_row_creating_and_dispatches_executor() {
    let wmill = Arc::new(FakeWindmill::default());
    wmill.queue_async(Ok("executor-job".to_string()));
    let db = fast_create_db();
    let svc = SystemtenderService::with_parts(db.clone(), wmill.clone(), Arc::new(FakeCausal::default()));
    let result = svc.create_systemtender(create_config(), "bench-tender");
    assert_eq!(result["result"], "SUCCESS");
    assert_eq!(result["data"]["status"], "creating");
    assert_eq!(result["data"]["name"], "bench-tender");
    let id = result["data"]["id"].as_str().unwrap().to_string();
    assert!(uuid::Uuid::parse_str(&id).is_ok());
    assert!(db.queries().iter().any(|q| q.contains("INSERT INTO systemtender_meta_data")));
    assert!(db.queries().iter().any(|q| q.contains("'creating'")));
    let dispatched = wmill.dispatched.lock().unwrap();
    assert_eq!(dispatched.len(), 1);
    assert_eq!(dispatched[0].0, "f/controller/systemtender_create_executor");
    assert_eq!(dispatched[0].1["request_data"]["systemtender_id"], json!(id));
    assert!(!db.queries().iter().any(|q| q.contains("CREATE DATABASE")));
}

#[test]
fn create_dispatch_failure_rolls_the_row_back() {
    let wmill = FakeWindmill::default();
    wmill.queue_async(Err("windmill unreachable".to_string()));
    let db = fast_create_db();
    let svc = SystemtenderService::with_parts(db.clone(), Arc::new(wmill), Arc::new(FakeCausal::default()));
    let result = svc.create_systemtender(create_config(), "bench-tender");
    assert_eq!(result["result"], "FAILURE");
    assert!(db.queries().iter().any(|q| q.contains("DELETE FROM systemtender_meta_data")));
    assert!(db.queries().iter().any(|q| q.trim_start().starts_with("DELETE FROM systemtender_lifecycle")));
}

#[test]
fn duplicate_name_answers_the_visible_state() {
    for state in ["creating", "create-failed", "deleting", "deletion-failed", "active"] {
        let existing_id = "550e8400-e29b-41d4-a716-446655440000";
        let db = arc_db(move |_db, q| {
            if q.contains("CREATE TABLE") || q.trim_start().starts_with("INSERT INTO") { return Ok(None); }
            if q.contains("FROM targets") {
                return Ok(Some(vec![row(&[Some("t1"), Some("t1"), Some("ssh"), Some(r#"{"address": "host-a"}"#), None, Some("2026-10-10 09:00:00+00"), None])]));
            }
            if q.contains("SELECT id, name, creation_tsz FROM") {
                return Ok(Some(vec![row(&[Some(existing_id), Some("bench-tender"), Some("2026-10-10 09:00:00+00")])]));
            }
            if q.contains("FROM systemtender_lifecycle WHERE systemtender_id") {
                return Ok(Some(vec![row(&[Some(state), None])]));
            }
            if q.contains("FROM systemtender_meta_data WHERE id") {
                return Ok(Some(vec![row(&[Some(existing_id), Some("bench-tender"), Some("2026-10-10 09:00:00+00"), Some("{}")])]));
            }
            Ok(None)
        });
        let svc = SystemtenderService::with_parts(db.clone(), Arc::new(FakeWindmill::default()), Arc::new(FakeCausal::default()));
        let result = svc.create_systemtender(create_config(), "bench-tender");
        assert_eq!(result["result"], "SUCCESS", "state {state}");
        assert_eq!(result["duplicate"], json!(true), "state {state}");
        assert_eq!(result["data"]["id"], existing_id, "state {state}");
        assert_eq!(result["data"]["status"], state, "state {state}");
        assert!(!db.queries().iter().any(|q| q.contains("INSERT INTO systemtender_meta_data")));
    }
}

#[test]
fn get_answers_creating_with_creation_reason() {
    let db = arc_db(|_db, q| {
        if q.contains("CREATE TABLE") { return Ok(None); }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            return Ok(Some(vec![row(&[Some("row-id"), Some("bench-tender"), Some("2026-10-10 09:00:00+00"), Some("{}")])]));
        }
        if q.contains("FROM systemtender_lifecycle WHERE systemtender_id") {
            return Ok(Some(vec![row(&[Some("create-failed"), Some("Optuna init failed")])]));
        }
        Ok(None)
    });
    let svc = SystemtenderService::with_parts(db, Arc::new(FakeWindmill::default()), Arc::new(FakeCausal::default()));
    let data = &svc.get_systemtender("row-id")["data"];
    assert_eq!(data["status"], "create-failed");
    assert_eq!(data["creation_reason"], "Optuna init failed");
    assert_eq!(data["deletion_reason"], Value::Null);
}

#[test]
fn deletion_claim_takes_over_create_failed_rows() {
    use godon_controller::database::{DbConfig, MetadataDatabaseRepository};
    let db = arc_db(|_db, q| {
        if q.contains("RETURNING systemtender_id") {
            return Ok(Some(vec![row(&[Some("claimed")])]));
        }
        Ok(None)
    });
    let repo = MetadataDatabaseRepository::with_gateway(DbConfig::default(), db.clone());
    assert!(repo.claim_for_deletion("some-uuid", 300).unwrap());
    assert!(db.queries().iter().any(|q| q.contains("state = 'create-failed'")));
}

#[test]
fn get_survives_a_missing_archive_db() {
    use godon_controller::database::{ArchiveDatabaseRepository, DbConfig, DbError};
    let db = arc_db(|_db, _q| {
        Err(DbError("connection to server at \"localhost\" (::1), port 5432 failed: FATAL:  database \"systemtender_x\" does not exist".to_string()))
    });
    let repo = ArchiveDatabaseRepository::with_gateway(DbConfig::default(), db);
    assert!(repo.read_state_verdict("systemtender_x").unwrap().is_none());
}

#[test]
fn get_survives_the_table_build_window() {
    use godon_controller::database::{ArchiveDatabaseRepository, DbConfig, DbError};
    let unreadable = [
        "relation \"systemtender_state\" does not exist",
        "column \"finished_at\" does not exist",
        "database \"systemtender_x\" does not exist",
        "Table <unknown_table_name> (000040010000300080010000000004e1) not found in Raft group 0000000000000000000000000000000000",
    ];
    for text in unreadable {
        let db = arc_db(move |_db, _q| Err(DbError(text.to_string())));
        let repo = ArchiveDatabaseRepository::with_gateway(DbConfig::default(), db);
        assert!(repo.read_state_verdict("systemtender_x").unwrap().is_none(), "{text}");
    }
    let db = arc_db(|_db, _q| Err(DbError("connection refused".to_string())));
    let repo = ArchiveDatabaseRepository::with_gateway(DbConfig::default(), db);
    assert!(repo.read_state_verdict("systemtender_x").is_err());
}

// SKIPPED, named: the create-executor tests (python leaf by ruling).
// BLOCKED, named: TestDeathRelaunch + start-success shapes (the rust
// service predates the relaunch surfaces; parity motion owed first).
