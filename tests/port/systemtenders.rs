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
use serde_json::{json, Value};
use std::sync::Arc;

use crate::support::{arc_db, row, FakeCausal, FakeDb, FakeWindmill};

const DEFINITION: &str =
    r#"{"worker_job_ids": ["job-1"], "interference_detection": {"group": "bench"}}"#;

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
            // a live tender: finished, fresh stamp
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

/// The reconciler's view: deleting rows (id, forced, attempts, claimed age),
/// creating rows (id, attempts, claimed age), and the tender's own
/// state-row answer (the witness: live / terminal / empty = gone).
fn reconcile_service(
    deleting: &[(&str, bool, i64, f64)],
    creating: &[(&str, i64, f64)],
    state_rows: Vec<TextRow>,
) -> (SystemtenderService, Arc<FakeDb>, Arc<FakeWindmill>) {
    let wmill = Arc::new(FakeWindmill::default());
    reconcile_service_with_wmill(deleting, creating, state_rows, wmill)
}

fn reconcile_service_claim_refused(
    deleting: &[(&str, bool, i64, f64)],
    state_rows: Vec<TextRow>,
) -> (SystemtenderService, Arc<FakeDb>, Arc<FakeWindmill>) {
    let wmill = Arc::new(FakeWindmill::default());
    let deleting: Vec<(String, bool, i64, f64)> = deleting
        .iter()
        .map(|(id, f, a, age)| (id.to_string(), *f, *a, *age))
        .collect();
    let _creating: Vec<(String, i64, f64)> = Vec::new();
    let db = arc_db(move |_db_name, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        // the claim answers no row: another tick holds it
        if q.contains("UPDATE reconcile_state") && q.contains("RETURNING id") {
            return Ok(Some(vec![]));
        }
        if q.contains("FROM systemtender_lifecycle WHERE state = 'deleting'") {
            let rows: Vec<TextRow> = deleting
                .iter()
                .map(|(id, f, a, age)| {
                    let (a, age) = (a.to_string(), age.to_string());
                    row(&[
                        Some(id.as_str()),
                        Some(if *f { "true" } else { "false" }),
                        Some(a.as_str()),
                        Some(age.as_str()),
                    ])
                })
                .collect();
            return Ok(Some(rows));
        }
        if q.contains("FROM systemtender_lifecycle WHERE state = 'creating'") {
            return Ok(Some(vec![]));
        }
        if q.contains("FROM systemtender_state") {
            return Ok(Some(state_rows.clone()));
        }
        Ok(None)
    });
    let svc =
        SystemtenderService::with_parts(db.clone(), wmill.clone(), Arc::new(FakeCausal::default()));
    (svc, db, wmill)
}

fn reconcile_service_with_wmill(
    deleting: &[(&str, bool, i64, f64)],
    creating: &[(&str, i64, f64)],
    state_rows: Vec<TextRow>,
    wmill: Arc<FakeWindmill>,
) -> (SystemtenderService, Arc<FakeDb>, Arc<FakeWindmill>) {
    let deleting: Vec<(String, bool, i64, f64)> = deleting
        .iter()
        .map(|(id, f, a, age)| (id.to_string(), *f, *a, *age))
        .collect();
    let creating: Vec<(String, i64, f64)> = creating
        .iter()
        .map(|(id, a, age)| (id.to_string(), *a, *age))
        .collect();
    let state_rows = state_rows.clone();
    let db = arc_db(move |_db_name, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.contains("UPDATE reconcile_state") && q.contains("RETURNING id") {
            return Ok(Some(vec![row(&[Some("1")])]));
        }
        if q.contains("UPDATE reconcile_state") || q.contains("INSERT INTO reconcile_state") {
            return Ok(None);
        }
        if q.contains("FROM systemtender_lifecycle WHERE state = 'deleting'") {
            let rows: Vec<TextRow> = deleting
                .iter()
                .map(|(id, f, a, age)| {
                    let (a, age) = (a.to_string(), age.to_string());
                    row(&[
                        Some(id.as_str()),
                        Some(if *f { "true" } else { "false" }),
                        Some(a.as_str()),
                        Some(age.as_str()),
                    ])
                })
                .collect();
            return Ok(Some(rows));
        }
        if q.contains("FROM systemtender_lifecycle WHERE state = 'creating'") {
            let rows: Vec<TextRow> = creating
                .iter()
                .map(|(id, a, age)| {
                    let (a, age) = (a.to_string(), age.to_string());
                    row(&[
                        Some(id.as_str()),
                        Some("false"),
                        Some(a.as_str()),
                        Some(age.as_str()),
                    ])
                })
                .collect();
            return Ok(Some(rows));
        }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            return Ok(Some(vec![row(&[
                Some("row-id"),
                Some("bench-tender"),
                Some("2026-09-30 18:00:00+00"),
                Some(DEFINITION),
            ])]));
        }
        if q.contains("FROM systemtender_state") {
            return Ok(Some(state_rows.clone()));
        }
        if q.contains("SELECT id, name, creation_tsz FROM") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    });
    let svc =
        SystemtenderService::with_parts(db.clone(), wmill.clone(), Arc::new(FakeCausal::default()));
    (svc, db, wmill)
}

// ── retrieval ──────────────────────────────────────────────────────

#[test]
fn get_missing_id_fails() {
    let r = dispatch("systemtender_get", &json!({}));
    assert_eq!(r["result"], "FAILURE");
    assert!(r["error"]
        .as_str()
        .unwrap()
        .contains("Missing systemtender_id"));
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
    let svc = SystemtenderService::with_parts(
        db,
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
    );
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
    let svc = SystemtenderService::with_parts(
        db,
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
    );
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
    let svc = SystemtenderService::with_parts(
        db,
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
    );
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
                row(&[
                    Some("id-1"),
                    Some("systemtender1"),
                    Some("2026-01-01 00:00:00+00"),
                ]),
                row(&[
                    Some("id-2"),
                    Some("systemtender2"),
                    Some("2026-01-02 00:00:00+00"),
                ]),
            ]));
        }
        if q.contains("SELECT systemtender_id, state FROM systemtender_lifecycle") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    });
    let svc = SystemtenderService::with_parts(
        db,
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
    );
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
    assert!(r["error"]
        .as_str()
        .unwrap()
        .contains("Missing systemtender_id"));
}

// ── deletion: the quiesce machine (#399's guard) ───────────────────

#[test]
fn delete_marks_and_delivers_the_stop_flag() {
    // the death door's verb half (designs/2026-10-18): mark + deliver,
    // never cancel, never wait. Convergence lives in the reconciler.
    let wmill = Arc::new(FakeWindmill::default());
    let (svc, db) = service_with_jobs(&["job-1"], wmill.clone());
    let r = svc.delete_systemtender("bench-tender", true);
    assert_eq!(r["result"], "SUCCESS");
    assert_eq!(r["data"]["delete_type"], "marked");
    assert_eq!(r["data"]["status"], "deleting");
    // the delivery: the stop flag the worker's existing read answers to
    assert!(db
        .queries()
        .iter()
        .any(|q| q.contains("UPDATE systemtender_state SET shutdown_requested = TRUE")));
    // the verb never reaches the platform's kill verb and never drops
    assert!(wmill.cancelled.lock().unwrap().is_empty());
    assert!(!db.queries().iter().any(|q| q.contains("DROP DATABASE")));
    assert!(!db
        .queries()
        .iter()
        .any(|q| q.contains("DELETE FROM systemtender_meta_data")));
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
        if q.trim_start()
            .starts_with("DELETE FROM systemtender_lifecycle")
        {
            return Ok(None);
        }
        Ok(None)
    });
    let svc = SystemtenderService::with_parts(
        db.clone(),
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
    );
    let r = svc.delete_systemtender("bench-tender", true);
    assert_eq!(r["result"], "SUCCESS");
    assert_eq!(r["data"]["delete_type"], "already-gone");
    // the stale lifecycle row is swept with it
    assert!(db.queries().iter().any(|q| q
        .trim_start()
        .starts_with("DELETE FROM systemtender_lifecycle")));
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
                Some("row-id"),
                Some("bench-tender"),
                Some("2026-09-30 18:00:00+00"),
                Some(DEFINITION),
            ])]));
        }
        if q.contains("FROM systemtender_lifecycle WHERE systemtender_id") {
            return Ok(Some(vec![row(&[Some("deleting"), None])]));
        }
        Ok(None)
    });
    let svc = SystemtenderService::with_parts(
        db.clone(),
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
    );
    let r = svc.delete_systemtender("bench-tender", true);
    assert_eq!(r["result"], "SUCCESS");
    assert_eq!(r["data"]["status"], "deleting");
    assert_eq!(r["data"]["note"], "deletion already in progress");
    assert!(!db.queries().iter().any(|q| q.contains("DROP DATABASE")));
}

#[test]
fn reconcile_waits_while_the_witness_reads_live() {
    // stamps fresh: the worker lives, the door waits - and the cancel
    // hint still went out (untrusted, sent every tick)
    let (svc, db, wmill) = reconcile_service(
        &[("bench-tender", false, 0, 1000.0)],
        &[],
        vec![row(&[Some("f"), Some("5.0"), Some("10.0")])],
    );
    let r = svc.reconcile_tick();
    assert_eq!(r["result"], "SUCCESS");
    assert_eq!(r["data"]["deletions"]["waiting"], 1);
    assert_eq!(r["data"]["deletions"]["converged"], 0);
    assert!(!db.queries().iter().any(|q| q.contains("DROP DATABASE")));
    let cancelled = wmill.cancelled.lock().unwrap().clone();
    assert_eq!(cancelled.len(), 1, "the hint is sent even while waiting");
    assert_eq!(cancelled[0].0, "job-1");
    // the tick stamped itself
    assert!(db
        .queries()
        .iter()
        .any(|q| q.contains("UPDATE reconcile_state SET tick_at")));
}

#[test]
fn reconcile_drops_when_stamps_fall_silent_past_the_threshold() {
    // age 4000s vs threshold max(90, 6*60): presumed dead - the drop runs
    let (svc, db, _wmill) = reconcile_service(
        &[("bench-tender", false, 0, 1000.0)],
        &[],
        vec![row(&[Some("f"), Some("4000.0"), Some("60.0")])],
    );
    let r = svc.reconcile_tick();
    assert_eq!(r["data"]["deletions"]["converged"], 1);
    assert!(db.queries().iter().any(|q| q.contains("DROP DATABASE")));
    assert!(db
        .queries()
        .iter()
        .any(|q| q.contains("DELETE FROM systemtender_meta_data")));
    assert!(db.queries().iter().any(|q| q
        .trim_start()
        .starts_with("DELETE FROM systemtender_lifecycle")));
}

#[test]
fn reconcile_drops_when_the_tender_finished() {
    let (svc, db, _wmill) = reconcile_service(
        &[("bench-tender", false, 0, 1000.0)],
        &[],
        vec![row(&[Some("t"), Some("5.0"), Some("10.0")])],
    );
    let r = svc.reconcile_tick();
    assert_eq!(r["data"]["deletions"]["converged"], 1);
    assert!(db.queries().iter().any(|q| q.contains("DROP DATABASE")));
}

#[test]
fn reconcile_force_drops_without_waiting() {
    // force's gate is the only difference: one drop path, immediate
    let (svc, db, _wmill) = reconcile_service(
        &[("bench-tender", true, 0, 0.0)],
        &[],
        vec![row(&[Some("f"), Some("5.0"), Some("10.0")])],
    );
    let r = svc.reconcile_tick();
    assert_eq!(r["data"]["deletions"]["converged"], 1);
    assert!(db.queries().iter().any(|q| q.contains("DROP DATABASE")));
}

#[test]
fn reconcile_drops_when_the_store_is_gone() {
    // no state row at all: nothing alive to protect
    let (svc, db, _wmill) = reconcile_service(&[("bench-tender", false, 0, 1000.0)], &[], vec![]);
    let r = svc.reconcile_tick();
    assert_eq!(r["data"]["deletions"]["converged"], 1);
    assert!(db.queries().iter().any(|q| q.contains("DROP DATABASE")));
}

#[test]
fn reconcile_skips_when_another_tick_holds_the_claim() {
    let (svc, db, _wmill) = reconcile_service_claim_refused(
        &[("bench-tender", false, 0, 1000.0)],
        vec![row(&[Some("f"), Some("5.0"), Some("10.0")])],
    );
    let r = svc.reconcile_tick();
    assert_eq!(r["result"], "SUCCESS");
    assert_eq!(r["data"]["skipped"], true);
    assert!(!db.queries().iter().any(|q| q.contains("DROP DATABASE")));
    assert!(!db
        .queries()
        .iter()
        .any(|q| q.contains("UPDATE reconcile_state SET tick_at")));
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
fn reconcile_redrives_a_stale_creating_row() {
    // executor crashed mid-create: the row sits creating, claimed long
    // ago - the reconciler re-dispatches it and counts the attempt
    let wmill = Arc::new(FakeWindmill::default());
    wmill.queue_async(Ok("job-redrive-1".to_string()));
    let (svc, db, _w) =
        reconcile_service_with_wmill(&[], &[("bench-tender", 0, 1000.0)], vec![], wmill);
    let r = svc.reconcile_tick();
    assert_eq!(r["data"]["creations"]["redriven"], 1);
    assert!(db
        .queries()
        .iter()
        .any(|q| q.contains("UPDATE systemtender_lifecycle SET attempts = attempts + 1")));
}

#[test]
fn reconcile_leaves_a_fresh_creating_row_alone() {
    // claimed 10s ago, patience 300s: nothing to do
    let (svc, db, _wmill) = reconcile_service(&[], &[("bench-tender", 0, 10.0)], vec![]);
    let r = svc.reconcile_tick();
    assert_eq!(r["data"]["creations"]["waiting"], 1);
    assert!(!db
        .queries()
        .iter()
        .any(|q| q.contains("attempts = attempts + 1")));
}

#[test]
fn reconcile_fails_a_poison_creating_row_loud() {
    // three re-drives burned and the last patience (300<<3 = 2400s)
    // elapsed too: create-failed, the honest verdict - the backoff
    // ladder must complete before the poison call
    let (svc, db, _wmill) = reconcile_service(&[], &[("bench-tender", 3, 3000.0)], vec![]);
    let r = svc.reconcile_tick();
    assert_eq!(r["data"]["creations"]["failed"], 1);
    assert!(db.queries().iter().any(|q| q.contains("'create-failed'")));
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
    w.job_results
        .lock()
        .unwrap()
        .push_back(Err("HTTP 404 Not Found".to_string()));
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
    w.job_results
        .lock()
        .unwrap()
        .push_back(Err("HTTP 400: Bad Request".to_string()));
    w.queue_completed(Some(
        json!({"completed": true, "success": false, "started": true}),
    ));
    assert!(job_reached_terminal_state(w.as_ref(), "job-x"));
}

#[test]
fn terminal_state_counts_cancelled_never_started_as_gone() {
    let w = Arc::new(FakeWindmill::default());
    w.job_results
        .lock()
        .unwrap()
        .push_back(Err("HTTP 404: Not Found".to_string()));
    assert!(job_reached_terminal_state(w.as_ref(), "job-x"));

    let w = Arc::new(FakeWindmill::default());
    w.job_results
        .lock()
        .unwrap()
        .push_back(Err("HTTP 400: Bad Request".to_string()));
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
    w.job_results
        .lock()
        .unwrap()
        .push_back(Err("HTTP 400: Bad Request".to_string()));
    w.queue_completed(Some(json!({"completed": false, "started": true})));
    assert!(!job_reached_terminal_state(w.as_ref(), "job-x"));
    let w = Arc::new(FakeWindmill::default());
    w.job_results
        .lock()
        .unwrap()
        .push_back(Err("HTTP 400: Bad Request".to_string()));
    w.completed_results
        .lock()
        .unwrap()
        .push_back(Err("HTTP 500".to_string()));
    assert!(!job_reached_terminal_state(w.as_ref(), "job-x"));
}

// ── stop / start doors ─────────────────────────────────────────────

#[test]
fn stop_missing_id_fails() {
    let r = dispatch("systemtender_stop", &json!({}));
    assert_eq!(r["result"], "FAILURE");
    assert!(r["error"]
        .as_str()
        .unwrap()
        .contains("Missing systemtender_id"));
}

fn missing_tender_db() -> Arc<FakeDb> {
    arc_db(|_db, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    })
}

#[test]
fn stop_unknown_tender_answers_not_found() {
    let svc = SystemtenderService::with_parts(
        missing_tender_db(),
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
    );
    let r = svc.stop_systemtender("no-such");
    assert_eq!(r["result"], "FAILURE");
    assert!(r["error"].as_str().unwrap().contains("not found"));
}

#[test]
fn stop_sets_the_graceful_flag() {
    let db = arc_db(|_db, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            return Ok(Some(vec![row(&[
                Some("t-1"),
                Some("bench-tender"),
                Some("2026-10-10 09:00:00+00"),
                Some("{}"),
            ])]));
        }
        Ok(None)
    });
    let svc = SystemtenderService::with_parts(
        db.clone(),
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
    );
    let r = svc.stop_systemtender("t-1");
    assert_eq!(r["result"], "SUCCESS");
    assert_eq!(r["data"]["shutdown_type"], "graceful");
    assert!(db
        .queries()
        .iter()
        .any(|q| q.contains("shutdown_requested = TRUE")));
}

#[test]
fn start_missing_id_fails() {
    let r = dispatch("systemtender_start", &json!({}));
    assert_eq!(r["result"], "FAILURE");
    assert!(r["error"]
        .as_str()
        .unwrap()
        .contains("Missing systemtender_id"));
}

#[test]
fn start_unknown_tender_answers_not_found() {
    let svc = SystemtenderService::with_parts(
        missing_tender_db(),
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
    );
    let r = svc.start_systemtender("no-such");
    assert_eq!(r["result"], "FAILURE");
    assert!(r["error"].as_str().unwrap().contains("not found"));
}

// start-success + the death-relaunch shapes: the section at the
// bottom (the relaunch surfaces are ported; the three BLOCKED tests
// landed there with them).

// ── worker cancellation ────────────────────────────────────────────

#[test]
fn cancel_job_success_and_failure() {
    use godon_controller::systemtender_service::cancel_job_by_id;
    let w = Arc::new(FakeWindmill::default());
    assert!(cancel_job_by_id(w.as_ref(), "test-job-id", None));
    let w = Arc::new(FakeWindmill::default());
    w.cancel_results
        .lock()
        .unwrap()
        .push_back(Err("refused".to_string()));
    assert!(!cancel_job_by_id(w.as_ref(), "test-job-id", None));
}

#[test]
fn cancel_carries_the_reason_to_the_api() {
    use godon_controller::systemtender_service::cancel_job_by_id;
    let w = Arc::new(FakeWindmill::default());
    assert!(cancel_job_by_id(
        w.as_ref(),
        "test-job-id",
        Some("Test cancellation")
    ));
    let calls = w.cancelled.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "test-job-id");
    assert_eq!(calls[0].1.as_deref(), Some("Test cancellation"));
}

// ── response format + the liveness floor ───────────────────────────

#[test]
fn get_response_carries_the_contract_keys() {
    let db = arc_db(|_db, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            return Ok(Some(vec![row(&[
                Some("t-1"),
                Some("n"),
                Some("2026-01-01 00:00:00+00"),
                Some(r#"{"type": "x"}"#),
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
    let svc = SystemtenderService::with_parts(
        db,
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
    );
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
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            return Ok(Some(vec![row(&[
                Some("row-id"),
                Some("test-tender"),
                Some("2026-09-26 18:00:00+00"),
                Some("{}"),
            ])]));
        }
        if q.contains("FROM systemtender_state") {
            return Ok(Some(vec![row(&[
                Some(finished.as_str()),
                Some(age.as_str()),
                Some(interval.as_str()),
            ])]));
        }
        if q.contains("FROM systemtender_lifecycle WHERE systemtender_id") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    })
}

#[test]
fn abort_burst_within_floor_reads_running() {
    let svc = SystemtenderService::with_parts(
        liveness_db("f", "45", "10"),
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
    );
    let out = svc.get_systemtender("some-uuid");
    assert_eq!(out["result"], "SUCCESS");
    assert_eq!(out["data"]["status"], "running");
    assert_eq!(out["data"]["liveness"]["stale_threshold_secs"], 90);
}

#[test]
fn true_death_past_floor_reads_presumed_dead() {
    let svc = SystemtenderService::with_parts(
        liveness_db("f", "300", "10"),
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
    );
    assert_eq!(
        svc.get_systemtender("some-uuid")["data"]["status"],
        "presumed_dead"
    );
}

#[test]
fn finished_stamp_wins_over_liveness() {
    let svc = SystemtenderService::with_parts(
        liveness_db("t", "999", "10"),
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
    );
    assert_eq!(
        svc.get_systemtender("some-uuid")["data"]["status"],
        "finished"
    );
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
        if q.contains("CREATE TABLE") || q.trim_start().starts_with("INSERT INTO") {
            return Ok(None);
        }
        if q.contains("EXTRACT(EPOCH") {
            return Ok(Some(vec![row(&[Some(heartbeat), Some("10.0")])]));
        }
        if q.contains("WHERE id = 'f0e0fb34") || q.contains("WHERE id = '92a33fa9") {
            let id = if q.contains("f0e0fb34") {
                "f0e0fb34-aaaa-bbbb-cccc-ddddeeeeffff"
            } else {
                "92a33fa9-1111-2222-3333-444455556666"
            };
            return Ok(Some(vec![row(&[
                Some(id),
                Some("tender"),
                None,
                Some("{}"),
            ])]));
        }
        if q.contains("FROM steerwishes") {
            return Ok(Some(vec![row(&[
                Some("wish-1"),
                Some("rcv/objective_0"),
                None,
                None,
                None,
                Some("standing"),
                Some("2026-10-12 19:00:00+00"),
                Some("declared"),
                None,
                None,
                None,
                Some("later"),
                Some("best"),
            ])]));
        }
        if q.contains("FROM steerwish") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    })
}

fn sender_and_receiver_notes(planned: Value) -> (WishService, Arc<FakeDb>, Arc<FakeCausal>) {
    let causal = Arc::new(FakeCausal::default());
    causal.queue_ask(Ok(planned));
    let db = wish_door_db("5.0");
    let svc = WishService::with_parts(
        db.clone(),
        Arc::new(FakeWindmill::default()),
        causal.clone(),
        true,
    );
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
    // the db name is the call's TARGET, never the query text - read
    // the recorded (database, query) pairs
    let calls = db.calls.lock().unwrap().clone();
    let note = |db_frag: &str, role: &str| {
        calls.iter().any(|(d, q)| {
            d.contains(db_frag) && q.contains("INSERT INTO wish_assignments") && q.contains(role)
        })
    };
    assert!(
        note("systemtender_f0e0fb34", "'sender'"),
        "calls: {:#?}",
        calls
    );
    assert!(
        note("systemtender_92a33fa9", "'receiver'"),
        "calls: {:#?}",
        calls
    );
}

#[test]
fn write_wish_assignment_defaults_to_sender_role() {
    let (svc, db, _c) = sender_and_receiver_notes(planned_response());
    let declared = svc.create_wish(&declare_wish_payload());
    let _ = declared.unwrap_or_else(|e| panic!("create_wish errored: {e}"));
    let first = db
        .queries()
        .into_iter()
        .find(|q| q.contains("INSERT INTO wish_assignments"))
        .expect("planted");
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
    let count = db
        .queries()
        .iter()
        .filter(|q| q.contains("INSERT INTO wish_assignments"))
        .count();
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
            return Ok(Some(vec![row(&[
                Some("t1"),
                Some("t1"),
                Some("ssh"),
                Some(r#"{"address": "host-a"}"#),
                None,
                Some("2026-10-10 09:00:00+00"),
                None,
            ])]));
        }
        if q.contains("CREATE TABLE") || q.trim_start().starts_with("INSERT INTO") {
            return Ok(None);
        }
        if q.contains("SELECT id, name, creation_tsz FROM") {
            return Ok(Some(vec![]));
        }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            return Ok(Some(vec![]));
        }
        if q.contains("FROM systemtender_lifecycle WHERE systemtender_id") {
            return Ok(Some(vec![]));
        }
        if q.contains("FROM steerwishes") || q.contains("FROM steerwish") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    })
}

#[test]
fn create_plants_row_creating_and_dispatches_executor() {
    let wmill = Arc::new(FakeWindmill::default());
    wmill.queue_async(Ok("executor-job".to_string()));
    let db = fast_create_db();
    let svc =
        SystemtenderService::with_parts(db.clone(), wmill.clone(), Arc::new(FakeCausal::default()));
    let result = svc.create_systemtender(create_config(), "bench-tender");
    assert_eq!(result["result"], "SUCCESS");
    assert_eq!(result["data"]["status"], "creating");
    assert_eq!(result["data"]["name"], "bench-tender");
    let id = result["data"]["id"].as_str().unwrap().to_string();
    assert!(uuid::Uuid::parse_str(&id).is_ok());
    assert!(db
        .queries()
        .iter()
        .any(|q| q.contains("INSERT INTO systemtender_meta_data")));
    assert!(db.queries().iter().any(|q| q.contains("'creating'")));
    let dispatched = wmill.dispatched.lock().unwrap();
    assert_eq!(dispatched.len(), 1);
    assert_eq!(dispatched[0].0, "f/controller/systemtender_create_executor");
    assert_eq!(
        dispatched[0].1["request_data"]["systemtender_id"],
        json!(id)
    );
    assert!(!db.queries().iter().any(|q| q.contains("CREATE DATABASE")));
}

#[test]
fn create_dispatch_failure_rolls_the_row_back() {
    let wmill = FakeWindmill::default();
    wmill.queue_async(Err("windmill unreachable".to_string()));
    let db = fast_create_db();
    let svc = SystemtenderService::with_parts(
        db.clone(),
        Arc::new(wmill),
        Arc::new(FakeCausal::default()),
    );
    let result = svc.create_systemtender(create_config(), "bench-tender");
    assert_eq!(result["result"], "FAILURE");
    assert!(db
        .queries()
        .iter()
        .any(|q| q.contains("DELETE FROM systemtender_meta_data")));
    assert!(db.queries().iter().any(|q| q
        .trim_start()
        .starts_with("DELETE FROM systemtender_lifecycle")));
}

#[test]
fn duplicate_name_answers_the_visible_state() {
    for state in [
        "creating",
        "create-failed",
        "deleting",
        "deletion-failed",
        "active",
    ] {
        let existing_id = "550e8400-e29b-41d4-a716-446655440000";
        let db = arc_db(move |_db, q| {
            if q.contains("CREATE TABLE") || q.trim_start().starts_with("INSERT INTO") {
                return Ok(None);
            }
            if q.contains("FROM targets") {
                return Ok(Some(vec![row(&[
                    Some("t1"),
                    Some("t1"),
                    Some("ssh"),
                    Some(r#"{"address": "host-a"}"#),
                    None,
                    Some("2026-10-10 09:00:00+00"),
                    None,
                ])]));
            }
            if q.contains("SELECT id, name, creation_tsz FROM") {
                return Ok(Some(vec![row(&[
                    Some(existing_id),
                    Some("bench-tender"),
                    Some("2026-10-10 09:00:00+00"),
                ])]));
            }
            if q.contains("FROM systemtender_lifecycle WHERE systemtender_id") {
                return Ok(Some(vec![row(&[Some(state), None])]));
            }
            if q.contains("FROM systemtender_meta_data WHERE id") {
                return Ok(Some(vec![row(&[
                    Some(existing_id),
                    Some("bench-tender"),
                    Some("2026-10-10 09:00:00+00"),
                    Some("{}"),
                ])]));
            }
            Ok(None)
        });
        let svc = SystemtenderService::with_parts(
            db.clone(),
            Arc::new(FakeWindmill::default()),
            Arc::new(FakeCausal::default()),
        );
        let result = svc.create_systemtender(create_config(), "bench-tender");
        assert_eq!(result["result"], "SUCCESS", "state {state}");
        assert_eq!(result["duplicate"], json!(true), "state {state}");
        assert_eq!(result["data"]["id"], existing_id, "state {state}");
        assert_eq!(result["data"]["status"], state, "state {state}");
        assert!(!db
            .queries()
            .iter()
            .any(|q| q.contains("INSERT INTO systemtender_meta_data")));
    }
}

#[test]
fn get_answers_creating_with_creation_reason() {
    let db = arc_db(|_db, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            return Ok(Some(vec![row(&[
                Some("row-id"),
                Some("bench-tender"),
                Some("2026-10-10 09:00:00+00"),
                Some("{}"),
            ])]));
        }
        if q.contains("FROM systemtender_lifecycle WHERE systemtender_id") {
            return Ok(Some(vec![row(&[
                Some("create-failed"),
                Some("Optuna init failed"),
            ])]));
        }
        Ok(None)
    });
    let svc = SystemtenderService::with_parts(
        db,
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
    );
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
    assert!(repo.claim_for_deletion("some-uuid", 300, false).unwrap());
    assert!(db
        .queries()
        .iter()
        .any(|q| q.contains("state = 'create-failed'")));
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
        assert!(
            repo.read_state_verdict("systemtender_x").unwrap().is_none(),
            "{text}"
        );
    }
    let db = arc_db(|_db, _q| Err(DbError("connection refused".to_string())));
    let repo = ArchiveDatabaseRepository::with_gateway(DbConfig::default(), db);
    assert!(repo.read_state_verdict("systemtender_x").is_err());
}

// ── the create executor (TestAsyncCreate's executor half) ────────
// The last python body, ported: the heavy half of create's 202 with
// no caller - preflight, archive DB + state table, worker launches,
// the marker drop. python's tests patched _await_state_table out;
// the fake answers the state-table read (ready at once, no sleeps).

const EXECUTOR_ROW_ID: &str = "550e8400-e29b-41d4-a716-446655440001";

fn executor_db() -> Arc<FakeDb> {
    let definition = serde_json::to_string(&json!({
        "systemtender": {"type": "linux_performance"},
        "run": {"parallel": 1},
        "effectuation": {"targets": [{"id": "t1", "type": "ssh_host", "address": "host-a"}]},
        "cooperation": {"active": true},
    }))
    .unwrap();
    arc_db(move |_db, q| {
        // the state-table build (its own multi-statement string, with
        // the seed INSERT) - before any generic CREATE catcher
        if q.contains("CREATE TABLE IF NOT EXISTS systemtender_state") {
            return Ok(None);
        }
        // the await read: the table answers ready at once
        if q.contains("SELECT COUNT(*) FROM systemtender_state") {
            return Ok(Some(vec![]));
        }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            return Ok(Some(vec![row(&[
                Some(EXECUTOR_ROW_ID),
                Some("bench-tender"),
                Some("2026-10-10 09:00:00+00"),
                Some(&definition),
            ])]));
        }
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.trim_start().starts_with("UPDATE systemtender_meta_data") {
            return Ok(None);
        }
        if q.contains("INSERT INTO systemtender_lifecycle") {
            return Ok(None);
        }
        if q.contains("DELETE FROM systemtender_lifecycle") {
            return Ok(None);
        }
        Ok(None)
    })
}

#[test]
fn executor_flips_the_row_live() {
    let wmill = Arc::new(FakeWindmill::default());
    wmill.queue_sync(json!({"result": "SUCCESS"})); // the preflight
    wmill.queue_async(Ok("job-1".to_string())); // the worker launch
    let db = executor_db();
    let svc =
        SystemtenderService::with_parts(db.clone(), wmill.clone(), Arc::new(FakeCausal::default()));

    let r = svc.create_systemtender_executor(EXECUTOR_ROW_ID);

    assert_eq!(r["result"], "SUCCESS");
    assert_eq!(r["data"]["status"], "active");
    assert_eq!(r["data"]["id"], json!(EXECUTOR_ROW_ID));
    // the DB ts renders as python's aware isoformat (createdAt and
    // the worker's creation_ts arg are the same string)
    assert_eq!(r["data"]["createdAt"], "2026-10-10T09:00:00+00:00");
    // heavy half ran: db + state table + the preflight + one worker
    assert_eq!(
        db.queries()
            .iter()
            .filter(|q| q.contains("CREATE DATABASE"))
            .count(),
        1
    );
    assert_eq!(
        db.queries()
            .iter()
            .filter(|q| q.contains("CREATE TABLE IF NOT EXISTS systemtender_state"))
            .count(),
        1
    );
    assert_eq!(wmill.sync_ran.lock().unwrap().len(), 1);
    assert_eq!(
        wmill.sync_ran.lock().unwrap()[0].0,
        "f/systemtender/strains/linux_performance/preflight"
    );
    assert_eq!(wmill.dispatched.lock().unwrap().len(), 1);
    {
        let d = wmill.dispatched.lock().unwrap();
        assert_eq!(d[0].0, "f/systemtender/engine/systemtender_worker");
        // the engine's lane: the estate's robot worker group
        assert_eq!(d[0].2.as_deref(), Some("robot"));
        assert_eq!(d[0].1["config"]["creation_ts"], "2026-10-10T09:00:00+00:00");
    }
    // job ids persisted for the delete path
    let booked = db
        .queries()
        .into_iter()
        .find(|q| q.trim_start().starts_with("UPDATE systemtender_meta_data"))
        .expect("the booking flew");
    assert!(booked.contains("\"worker_job_ids\":[\"job-1\"]"));
    // the stored definition keeps python's shape: creation_ts rode
    // the per-worker configs only, never the stored row
    assert!(!booked.contains("creation_ts"));
    // live: the marker drops, no failure lifecycle written
    assert_eq!(
        db.queries()
            .iter()
            .filter(|q| q.contains("DELETE FROM systemtender_lifecycle"))
            .count(),
        1
    );
    assert!(!db
        .queries()
        .iter()
        .any(|q| q.contains("INSERT INTO systemtender_lifecycle")));
}

#[test]
fn executor_failure_flags_create_failed_and_keeps_the_row() {
    let wmill = Arc::new(FakeWindmill::default());
    wmill.queue_sync(json!({"result": "SUCCESS"}));
    wmill.queue_async(Err("worker refused".to_string()));
    let db = executor_db();
    let svc =
        SystemtenderService::with_parts(db.clone(), wmill.clone(), Arc::new(FakeCausal::default()));

    let r = svc.create_systemtender_executor(EXECUTOR_ROW_ID);

    assert_eq!(r["result"], "FAILURE");
    assert_eq!(r["data"]["status"], "create-failed");
    assert!(r["error"].as_str().unwrap().contains("worker refused"));
    // the row is the receipt: create-failed + the reason, on the row
    let flagged = db
        .queries()
        .into_iter()
        .find(|q| q.contains("INSERT INTO systemtender_lifecycle"))
        .expect("the row is flagged");
    assert!(flagged.contains(EXECUTOR_ROW_ID));
    assert!(flagged.contains("'create-failed'"));
    assert!(flagged.contains("worker refused"));
    // half-made archive db gone, row stays
    assert_eq!(
        db.queries()
            .iter()
            .filter(|q| q.contains("DROP DATABASE IF EXISTS"))
            .count(),
        1
    );
    assert!(!db
        .queries()
        .iter()
        .any(|q| q.contains("DELETE FROM systemtender_meta_data")));
    assert!(!db
        .queries()
        .iter()
        .any(|q| q.contains("DELETE FROM systemtender_lifecycle")));
    // nothing launched, nothing to cancel
    assert!(wmill.cancelled.lock().unwrap().is_empty());
}

#[test]
fn executor_missing_row() {
    // row vanished before the executor ran (deleted mid-create):
    // honest FAILURE, no lifecycle written
    let db = arc_db(|_db, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    });
    let svc = SystemtenderService::with_parts(
        db.clone(),
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
    );
    let r = svc.create_systemtender_executor(EXECUTOR_ROW_ID);
    assert_eq!(r["result"], "FAILURE");
    assert_eq!(r["error"], "Systemtender not found");
    assert!(!db
        .queries()
        .iter()
        .any(|q| q.contains("INSERT INTO systemtender_lifecycle")));
}

#[test]
fn executor_script_passthrough() {
    // the wrapper's glue, pinned: a missing id answers before any
    // service exists; a present id is the whole call into
    // create_systemtender_executor (the success half rides the
    // service-level tests above - env-only services take no fakes)
    let r = dispatch("systemtender_create_executor", &json!({}));
    assert_eq!(r["result"], "FAILURE");
    assert_eq!(r["error"], "Missing systemtender_id");
    let r2 = dispatch(
        "systemtender_create_executor",
        &json!({"systemtender_id": ""}),
    );
    assert_eq!(r2["result"], "FAILURE");
    assert_eq!(r2["error"], "Missing systemtender_id");
}

#[test]
fn trial_check_reads_the_ledgers_trials_not_optuna() {
    // GROUND (10-17): the old rust read (studies/trial_params/
    // trial_values) queried tables nothing creates - on a live tender
    // it always fell to the skip arm. python's live read is the
    // worker's `trials` ledger row: attrs.effectuation_params +
    // value. Pinned here so the fantasy path cannot return.
    let new_config = json!({
        "settings": {"sysctl": {"a": {"constraints": [{}]}, "b": {"constraints": [{}]}}},
        "objectives": [{"name": "latency"}],
    });
    // a COMPLETE row with 2 stashed params and a value: compatible
    let db = arc_db(|_db, q| {
        if q.contains("FROM trials WHERE state = 'COMPLETE'") {
            return Ok(Some(vec![row(&[
                Some(r#"{"effectuation_params": {"a": 1, "b": 2}}"#),
                Some("12.5"),
            ])]));
        }
        Ok(None)
    });
    let svc = SystemtenderService::with_parts(
        db,
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
    );
    let (ok, detail) = svc.check_trial_compatibility(EXECUTOR_ROW_ID, &new_config, false);
    assert!(ok);
    assert_eq!(detail, "Compatible: 2 params, 1 objectives");
    // the objective count diverges: refused, with python's words
    let db = arc_db(|_db, q| {
        if q.contains("FROM trials WHERE state = 'COMPLETE'") {
            return Ok(Some(vec![row(&[
                Some(r#"{"effectuation_params": {"a": 1, "b": 2}}"#),
                Some("12.5"),
            ])]));
        }
        Ok(None)
    });
    let svc = SystemtenderService::with_parts(
        db,
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
    );
    let (ok, detail) =
        svc.check_trial_compatibility(EXECUTOR_ROW_ID, &json!({"objectives": []}), false);
    assert!(!ok);
    assert_eq!(
        detail,
        "Trial data incompatible: Parameter count: existing trials have 2, new config has 0; Objective count: existing trials have 1, new config has 0. Use force=true to clear trial history and start fresh."
    );
    // no COMPLETE rows: nothing to hold against
    let db = arc_db(|_db, q| {
        if q.contains("FROM trials WHERE state = 'COMPLETE'") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    });
    let svc = SystemtenderService::with_parts(
        db,
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
    );
    let (ok, detail) = svc.check_trial_compatibility(EXECUTOR_ROW_ID, &new_config, false);
    assert!(ok);
    assert_eq!(detail, "No existing trial data found");
    // the read itself fails (no DB): skipped, never a refusal
    let db = arc_db(|_db, _q| {
        Err(godon_controller::database::DbError(
            "db does not exist".to_string(),
        ))
    });
    let svc = SystemtenderService::with_parts(
        db,
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
    );
    let (ok, detail) = svc.check_trial_compatibility(EXECUTOR_ROW_ID, &new_config, false);
    assert!(ok);
    assert_eq!(detail, "Compatibility check skipped: db does not exist");
}

// ── death relaunch (TestDeathRelaunch) ─────────────────────────────
// The three formerly-BLOCKED tests: the service now carries the
// relaunch surfaces (the named resume, the history booking, the
// relaunches/last_relaunch reads), so they land unchanged in their
// asserted envelopes.

fn relaunch_meta_row(config: &'static str) -> Arc<FakeDb> {
    arc_db(move |_db, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            return Ok(Some(vec![row(&[
                Some("550e8400-e29b-41d4-a716-446655440000"),
                Some("t"),
                Some("2026-10-15 09:00:00+00"),
                Some(config),
            ])]));
        }
        // the stop flag read comes BEFORE the verdict arm: both name
        // systemtender_state, the flag query is the specific one
        if q.contains("SELECT shutdown_requested FROM systemtender_state") {
            return Ok(Some(vec![row(&[Some("f")])]));
        }
        // the verdict: finished, age, interval (+ instruments cell)
        if q.contains("FROM systemtender_state") {
            return Ok(Some(vec![row(&[
                Some("f"),
                Some("4000"),
                Some("60"),
                None,
            ])]));
        }
        Ok(None)
    })
}

#[test]
fn start_relaunches_a_presumed_dead_tender() {
    let wmill = Arc::new(FakeWindmill::default());
    wmill.queue_async(Ok("job-relaunched-1".to_string()));
    let db = relaunch_meta_row(
        r#"{"systemtender": {"name": "t", "type": "ssh"}, "effectuation": {"targets": [{"target_id": "t1"}]}}"#,
    );
    let svc =
        SystemtenderService::with_parts(db.clone(), wmill.clone(), Arc::new(FakeCausal::default()));

    let r = svc.start_systemtender("550e8400-e29b-41d4-a716-446655440000");

    assert_eq!(
        r["result"], "SUCCESS",
        "the dead are startable — no status guard refuses"
    );
    assert_eq!(
        r["data"]["resumed_from"], "presumed_dead",
        "the door names the death, never covers it"
    );
    // the stop flag cleared regardless of the death
    assert!(db
        .queries()
        .iter()
        .any(|q| q.contains("shutdown_requested = FALSE")));
    // workers relaunched create-identical, fresh job ids booked
    assert_eq!(wmill.dispatched.lock().unwrap().len(), 1);
    let booked = db
        .queries()
        .into_iter()
        .find(|q| q.trim_start().starts_with("UPDATE systemtender_meta_data"))
        .expect("the booking flew");
    assert!(booked.contains("\"worker_job_ids\":[\"job-relaunched-1\"]"));
    // the death-resume is history, recorded in the same booking (the
    // age rides as the read's float — the live EXTRACT shape)
    assert!(booked.contains("\"relaunch_history\":["));
    assert!(booked.contains("\"resumed_from\":\"presumed_dead\""));
    assert!(booked.contains("\"liveness_age_secs\":4000.0"));
}

#[test]
fn a_stopped_tender_resumes_as_stopped_not_dead() {
    let wmill = Arc::new(FakeWindmill::default());
    wmill.queue_async(Ok("job-1".to_string()));
    // the stop flag set: a deliberate exit, not a death (the verdict
    // numbers are death-shaped — the flag decides)
    let db = arc_db(move |_db, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            return Ok(Some(vec![row(&[
                Some("550e8400-e29b-41d4-a716-446655440000"),
                Some("t"),
                Some("2026-10-15 09:00:00+00"),
                Some(
                    r#"{"systemtender": {"name": "t", "type": "ssh"}, "effectuation": {"targets": [{"target_id": "t1"}]}}"#,
                ),
            ])]));
        }
        if q.contains("SELECT shutdown_requested FROM systemtender_state") {
            return Ok(Some(vec![row(&[Some("t")])]));
        }
        if q.contains("FROM systemtender_state") {
            return Ok(Some(vec![row(&[
                Some("f"),
                Some("4000"),
                Some("60"),
                None,
            ])]));
        }
        Ok(None)
    });
    let svc = SystemtenderService::with_parts(db.clone(), wmill, Arc::new(FakeCausal::default()));

    let r = svc.start_systemtender("550e8400-e29b-41d4-a716-446655440000");

    assert_eq!(r["result"], "SUCCESS");
    assert_eq!(
        r["data"]["resumed_from"], "stopped",
        "a deliberate exit is never covered as death"
    );
    let booked = db
        .queries()
        .into_iter()
        .find(|q| q.trim_start().starts_with("UPDATE systemtender_meta_data"))
        .expect("the booking flew");
    assert!(
        !booked.contains("relaunch_history"),
        "only deaths make history"
    );
}

#[test]
fn the_verdict_stays_loud_while_the_tender_stays_dead() {
    // one history entry already on the config: a relaunched tender
    // that died again still reads dead, with its numbers and its
    // resume count
    let db = arc_db(|_db, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            return Ok(Some(vec![row(&[
                Some("550e8400-e29b-41d4-a716-446655440000"),
                Some("t"),
                Some("2026-10-15 09:00:00+00"),
                Some(
                    r#"{"systemtender": {"name": "t", "type": "ssh"}, "effectuation": {"targets": [{"target_id": "t1"}]}, "relaunch_history": [{"ts": "2026-10-15T12:00:00+00:00", "resumed_from": "presumed_dead", "liveness_age_secs": 4000}]}"#,
                ),
            ])]));
        }
        if q.contains("FROM systemtender_state") {
            return Ok(Some(vec![row(&[
                Some("f"),
                Some("4000"),
                Some("60"),
                None,
            ])]));
        }
        if q.contains("FROM systemtender_lifecycle WHERE systemtender_id") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    });
    let svc = SystemtenderService::with_parts(
        db,
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
    );

    let got = svc.get_systemtender("550e8400-e29b-41d4-a716-446655440000");

    assert_eq!(got["data"]["status"], "presumed_dead");
    assert_eq!(
        got["data"]["liveness"]["age_secs"],
        json!(4000.0),
        "the verdict carries its numbers"
    );
    assert_eq!(
        got["data"]["relaunches"],
        json!(1),
        "the resume count rides every read"
    );
    assert_eq!(
        got["data"]["last_relaunch"]["resumed_from"],
        "presumed_dead"
    );
}

// ── the resurrection family (designs/2026-10-18-worker-death-door, round 3) ──

/// The resurrection fake: the registry, the lifecycle map, the tender's
/// own stop flag + state verdict (the witnesses), and its config.
/// Single-line SELECT heads only - the real queries never span lines
/// between FROM and WHERE where it could bite (the wishes-suite lesson).
fn resurrection_service(
    registry: &[&str],
    lifecycle: &[(&str, &str)],
    stop_flag: Option<bool>,
    verdict: Option<(&str, &str, &str)>, // (finished, age_secs, interval_secs)
    config: &str,
) -> (SystemtenderService, Arc<FakeDb>, Arc<FakeWindmill>) {
    let registry: Vec<TextRow> = registry
        .iter()
        .map(|id| {
            row(&[
                Some(*id),
                Some("bench-tender"),
                Some("2026-09-30 18:00:00+00"),
            ])
        })
        .collect();
    let lifecycle: Vec<TextRow> = lifecycle
        .iter()
        .map(|(id, state)| row(&[Some(*id), Some(*state)]))
        .collect();
    let wmill = Arc::new(FakeWindmill::default());
    wmill.queue_async(Ok("job-relaunch-1".to_string()));
    let config = config.to_string();
    let verdict = verdict.map(|(f, a, i)| (f.to_string(), a.to_string(), i.to_string()));
    let db = arc_db(move |_db, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.contains("UPDATE reconcile_state") && q.contains("RETURNING id") {
            return Ok(Some(vec![row(&[Some("1")])]));
        }
        if q.contains("UPDATE reconcile_state") || q.contains("INSERT INTO reconcile_state") {
            return Ok(None);
        }
        if q.contains("FROM systemtender_lifecycle WHERE state = 'deleting'")
            || q.contains("FROM systemtender_lifecycle WHERE state = 'creating'")
        {
            return Ok(Some(vec![]));
        }
        if q.contains("SELECT systemtender_id, state FROM") {
            return Ok(Some(lifecycle.clone()));
        }
        if q.contains("SELECT id, name, creation_tsz FROM") {
            return Ok(Some(registry.clone()));
        }
        if q.contains("SELECT shutdown_requested") {
            return Ok(Some(match stop_flag {
                Some(b) => vec![row(&[Some(if b { "t" } else { "f" })])],
                None => vec![],
            }));
        }
        if q.contains("SELECT finished_at IS NOT NULL") {
            return Ok(Some(match &verdict {
                Some((fin, age, interval)) => vec![row(&[
                    Some(fin.as_str()),
                    Some(age.as_str()),
                    Some(interval.as_str()),
                    None,
                ])],
                None => vec![],
            }));
        }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            return Ok(Some(vec![row(&[
                Some("row-id"),
                Some("bench-tender"),
                Some("2026-09-30 18:00:00+00"),
                Some(config.as_str()),
            ])]));
        }
        Ok(None)
    });
    let svc =
        SystemtenderService::with_parts(db.clone(), wmill.clone(), Arc::new(FakeCausal::default()));
    (svc, db, wmill)
}

fn ts_secs_ago(secs: i64) -> String {
    (chrono::Utc::now() - chrono::Duration::seconds(secs))
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn history_config(ages: &[i64]) -> String {
    let entries: Vec<Value> = ages
        .iter()
        .map(|s| json!({"ts": ts_secs_ago(*s), "resumed_from": "presumed_dead"}))
        .collect();
    serde_json::to_string(&json!({"relaunch_history": entries})).unwrap()
}

#[test]
fn resurrects_a_worker_dead_past_the_verdict_threshold() {
    // heartbeat silent 2h against a 6-minute threshold: the tick
    // relaunches through the start door
    let (svc, _db, _wmill) = resurrection_service(
        &["row-id"],
        &[],
        Some(false),
        Some(("f", "7200", "60")),
        "{}",
    );
    let r = svc.reconcile_tick();
    assert_eq!(
        r["data"]["resurrections"]["relaunched"], 1,
        "relaunched: {r}"
    );
    assert_eq!(r["data"]["resurrections"]["failed"], 0);
}

#[test]
fn leaves_a_live_worker_alone() {
    let (svc, _db, _wmill) =
        resurrection_service(&["row-id"], &[], Some(false), Some(("f", "10", "60")), "{}");
    let r = svc.reconcile_tick();
    assert_eq!(r["data"]["resurrections"]["relaunched"], 0, "alive: {r}");
    assert_eq!(r["data"]["resurrections"]["failed"], 0);
}

#[test]
fn a_deliberate_park_is_never_revived() {
    // the stop flag is the holder's own hand - silence means parked
    let (svc, _db, _wmill) = resurrection_service(
        &["row-id"],
        &[],
        Some(true),
        Some(("f", "7200", "60")),
        "{}",
    );
    let r = svc.reconcile_tick();
    assert_eq!(r["data"]["resurrections"]["relaunched"], 0, "parked: {r}");
}

#[test]
fn a_finished_worker_is_not_a_death() {
    let (svc, _db, _wmill) = resurrection_service(
        &["row-id"],
        &[],
        Some(false),
        Some(("t", "7200", "60")),
        "{}",
    );
    let r = svc.reconcile_tick();
    assert_eq!(r["data"]["resurrections"]["relaunched"], 0, "finished: {r}");
}

#[test]
fn no_verdict_answers_nothing() {
    let (svc, _db, _wmill) = resurrection_service(&["row-id"], &[], Some(false), None, "{}");
    let r = svc.reconcile_tick();
    assert_eq!(
        r["data"]["resurrections"]["relaunched"], 0,
        "no verdict: {r}"
    );
}

#[test]
fn other_families_own_their_states() {
    for state in ["creating", "create-failed", "deleting", "deletion-failed"] {
        let (svc, _db, _wmill) = resurrection_service(
            &["row-id"],
            &[("row-id", state)],
            Some(false),
            Some(("f", "7200", "60")),
            "{}",
        );
        let r = svc.reconcile_tick();
        assert_eq!(
            r["data"]["resurrections"]["relaunched"], 0,
            "state {state}: {r}"
        );
    }
}

#[test]
fn a_fresh_resurrection_gets_its_spacing() {
    // relaunched 2 minutes ago: the new worker has not stamped yet -
    // relaunching again would stack workers
    let (svc, _db, _wmill) = resurrection_service(
        &["row-id"],
        &[],
        Some(false),
        Some(("f", "7200", "60")),
        &history_config(&[120]),
    );
    let r = svc.reconcile_tick();
    assert_eq!(r["data"]["resurrections"]["relaunched"], 0, "spacing: {r}");
    assert_eq!(r["data"]["resurrections"]["waiting"], 1);
}

#[test]
fn a_worker_that_keeps_dying_goes_loud_not_looped() {
    // three resurrections in the last hour, none fresh: stand down
    let (svc, _db, _wmill) = resurrection_service(
        &["row-id"],
        &[],
        Some(false),
        Some(("f", "7200", "60")),
        &history_config(&[1200, 2400, 3000]),
    );
    let r = svc.reconcile_tick();
    assert_eq!(r["data"]["resurrections"]["relaunched"], 0, "capped: {r}");
    assert_eq!(r["data"]["resurrections"]["capped"], 1);
}

#[test]
fn an_empty_registry_is_quiet() {
    let (svc, _db, _wmill) = resurrection_service(&[], &[], None, None, "{}");
    let r = svc.reconcile_tick();
    assert_eq!(r["data"]["resurrections"]["relaunched"], 0);
    assert_eq!(r["data"]["resurrections"]["waiting"], 0);
    assert_eq!(r["data"]["resurrections"]["capped"], 0);
    assert_eq!(r["data"]["resurrections"]["failed"], 0);
}
