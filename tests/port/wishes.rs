//! Port of tests/unit/test_steerwishes.py — the wishes suite, the
//! last python test file. Python's Mock/patch-based tests port one
//! layer down: the service over the fakes, asserting the same
//! envelopes the mocked tests asserted (real code paths, not Mock
//! passthroughs). Entry-validation and missing-id cases ride
//! verbs::dispatch, which answers before any service is built.
//!
//! Layering notes (python mocked the whole service class; the rust
//! port pins the service contract the verb glue passes through):
//! - get/close/list verb envelopes for service answers are three
//!   lines of glue in verbs.rs; the tests here pin what flows
//!   through them (Ok(None) is the verb's "steerwish not found").
//! - python fed float('-inf')/float('nan') straight into the
//!   service — python dicts, never JSON. serde_json cannot
//!   represent non-finite numbers, so those two python tests port
//!   as wire receipts: the door refuses them at parse, before any
//!   service exists. The finite guards stay ported as defense.

use godon_controller::database::{DbConfig, DbError, MetadataDatabaseRepository, TextRow};
use godon_controller::verbs::dispatch;
use godon_controller::steer_grammar::checked_steer_band;
use godon_controller::wish_service::WishService;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

use crate::support::{arc_db, row, FakeCausal, FakeDb, FakeWindmill};

const WISH_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

// ── rows and answers ───────────────────────────────────────────────

fn wish_row(state: &str) -> TextRow {
    // the legacy sugar row: no stored claims/terms (cells 8/9 NULL),
    // the read-back synthesizes them from outcome+band
    row(&[
        Some(WISH_ID),
        Some("chainend.shift"),
        Some(r#"{"lo": -0.14, "hi": -0.06, "target": -0.10}"#),
        Some(r#"{"exclude": [], "maxChange": 0.5}"#),
        Some("2"),
        Some("standing"),
        Some("2026-09-10 10:30:00+00"),
        Some(state),
        None,
        None,
        None,
        None,
        None,
    ])
}

fn wish_row_with(state: &str, claims: &str, terms: &str) -> TextRow {
    let mut r = wish_row(state);
    r[8] = Some(claims.to_string());
    r[9] = Some(terms.to_string());
    r
}

fn events(kinds: &[&str]) -> Vec<TextRow> {
    kinds
        .iter()
        .enumerate()
        .map(|(i, kind)| {
            let detail = if *kind == "refused" {
                Some("target outside measured range")
            } else {
                None
            };
            row(&[
                Some(kind),
                Some(&format!("2026-09-10 10:{:02}:00+00", i)),
                detail,
            ])
        })
        .collect()
}

fn refused_answer() -> Value {
    json!({"status": "refused", "reason": "unmeasured_path", "detail": "no curve"})
}

fn planned_answer() -> Value {
    json!({
        "status": "planned",
        "moves": [{"sender": "abc-def", "param": "p", "setting": 1.0}],
        "predicted": {"value": -0.10, "bars": 0.02},
        "range_used": {},
    })
}

/// The registry fake behind the grammar/service tests: python's
/// patched repo methods, answered at the SQL the real repo speaks.
/// Ruled trap order: the deletion claim (INSERT..RETURNING) answers
/// BEFORE the insert-catcher; the wish-fetch (which embeds an events
/// subquery) answers before any events-shaped check.
fn registry_db(wish: TextRow, events: Vec<TextRow>) -> Arc<FakeDb> {
    arc_db(move |_db, q| {
        if q.contains("CREATE TABLE") || q.contains("ALTER TABLE") {
            return Ok(None);
        }
        if q.contains("RETURNING wish_id") {
            return Ok(Some(vec![row(&[Some("claimed")])]));
        }
        if q.trim_start().starts_with("INSERT INTO")
            || q.trim_start().starts_with("UPDATE ")
            || q.trim_start().starts_with("DELETE FROM")
        {
            return Ok(None);
        }
        if q.contains("SELECT w.id, w.outcome") {
            return Ok(Some(vec![wish.clone()]));
        }
        if q.contains("SELECT event_type, at, detail") {
            return Ok(Some(events.clone()));
        }
        if q.contains("SELECT COUNT(*)") {
            return Ok(Some(vec![row(&[Some("0")])]));
        }
        if q.contains("FROM steerwish_lifecycle") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    })
}

/// A declare (answer: now) over the registry fake; causal answers
/// refused (a refusal is a valid answer, not an error).
fn declare_over(payload: &Value) -> (Result<Value, String>, Arc<FakeDb>, Arc<FakeCausal>) {
    let db = registry_db(wish_row("declared"), events(&["declared"]));
    let causal = Arc::new(FakeCausal::default());
    causal.queue_ask(Ok(refused_answer()));
    let svc = WishService::with_parts(
        db.clone(),
        Arc::new(FakeWindmill::default()),
        causal.clone(),
        false,
    );
    (svc.create_wish(payload), db, causal)
}

/// The ask the service flew — the residue FakeCausal keeps behind the
/// queued answer (one residue per ask).
fn the_ask(causal: &FakeCausal) -> Value {
    causal
        .asks
        .lock()
        .unwrap()
        .pop_front()
        .expect("the ask flew")
        .unwrap()
}

/// The minted wish id, read out of the recorded INSERT.
fn minted_id(db: &FakeDb) -> String {
    let q = db
        .queries()
        .into_iter()
        .find(|q| q.trim_start().starts_with("INSERT INTO steerwishes"))
        .expect("the wish insert")
        .to_string();
    let at = q.find("VALUES('").expect("values head") + "VALUES('".len();
    q[at..at + 36].to_string()
}

/// Count of a kind's event INSERTs (the stamped events, not the fetch).
fn stamped(db: &FakeDb, kind: &str) -> usize {
    db.queries()
        .iter()
        .filter(|q| {
            q.trim_start().starts_with("INSERT INTO steerwish_events")
                && q.contains(&format!("'{}'", kind))
        })
        .count()
}

/// The assignment writes that flew, as (database, query) — the
/// database is the call's TARGET, never the query text.
fn assignment_calls(db: &FakeDb) -> Vec<(String, String)> {
    db.calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, q)| q.contains("INSERT INTO wish_assignments"))
        .cloned()
        .collect()
}

// ── entry validation (verb door; answers before any service work) ──

#[test]
fn create_missing_request_data_fails() {
    let r = dispatch("steerwish_create", &Value::Null);
    assert_eq!(r["result"], "FAILURE");
    assert!(r["error"].as_str().unwrap().contains("speaks its type"));
}

#[test]
fn sugar_shape_refused() {
    let r = dispatch(
        "steerwish_create",
        &json!({
            "type": "steer",
            "outcome": "chainend.shift",
            "band": {"lo": -0.14, "hi": -0.06},
        }),
    );
    assert_eq!(r["result"], "FAILURE");
    assert!(r["error"]
        .as_str()
        .unwrap()
        .contains("sugar shape is retired"));
}

#[test]
fn claims_required() {
    let r = dispatch("steerwish_create", &json!({"type": "steer"}));
    assert_eq!(r["result"], "FAILURE");
    assert!(r["error"].as_str().unwrap().contains("claims is required"));
}

#[test]
fn create_band_lo_not_below_hi() {
    let r = dispatch(
        "steerwish_create",
        &json!({
            "type": "steer",
            "claims": [{"outcome": "chainend.shift", "band": {"lo": 0.1, "hi": -0.1}}],
        }),
    );
    assert_eq!(r["result"], "FAILURE");
    assert!(r["error"]
        .as_str()
        .unwrap()
        .contains("claims[0].band lo must be below hi"));
}

#[test]
fn create_negative_budget() {
    let r = dispatch(
        "steerwish_create",
        &json!({
            "type": "steer",
            "claims": [{"outcome": "chainend.shift", "band": {"lo": -0.14, "hi": -0.06}}],
            "budget": -1,
        }),
    );
    assert_eq!(r["result"], "FAILURE");
    assert!(r["error"]
        .as_str()
        .unwrap()
        .to_lowercase()
        .contains("budget"));
}

#[test]
fn create_unknown_regime() {
    let r = dispatch(
        "steerwish_create",
        &json!({
            "type": "steer",
            "claims": [{"outcome": "chainend.shift", "band": {"lo": -0.14, "hi": -0.06}}],
            "regime": "episode",
        }),
    );
    assert_eq!(r["result"], "FAILURE");
    assert!(r["error"]
        .as_str()
        .unwrap()
        .to_lowercase()
        .contains("regime"));
}

// ── creation ───────────────────────────────────────────────────────

#[test]
fn create_success_stamps_declared() {
    // python mocked the service class; one layer down, the real
    // declare over the fakes returns the read-back wish
    let (r, _db, _c) = declare_over(&json!({
        "type": "steer",
        "claims": [{"outcome": "chainend.shift",
                    "band": {"lo": -0.14, "hi": -0.06, "target": -0.10}}],
        "answer": "now",
    }));
    let wish = r.unwrap_or_else(|e| panic!("create failed: {e}"));
    assert_eq!(wish["id"], WISH_ID);
    assert_eq!(wish["state"], "declared");
}

#[test]
fn create_validation_failure_maps_to_failure() {
    // the service's Err maps through the verb door into the envelope
    let r = dispatch(
        "steerwish_create",
        &json!({
            "type": "steer",
            "claims": [{"outcome": "chainend.shift", "band": {"lo": 0.1, "hi": -0.1}}],
        }),
    );
    assert_eq!(r["result"], "FAILURE");
    assert!(r["error"].as_str().unwrap().contains("lo must be below hi"));
}

// ── the claims grammar ─────────────────────────────────────────────

#[test]
fn claims_one_accepted_and_stored() {
    let (r, db, causal) = declare_over(&json!({
        "type": "steer",
        "claims": [{"outcome": " chainend.shift ",
                    "band": {"lo": -0.14, "hi": -0.06, "target": -0.10}}],
        "answer": "now",
    }));
    let wish = r.unwrap_or_else(|e| panic!("create failed: {e}"));
    let insert = db
        .queries()
        .into_iter()
        .find(|q| q.trim_start().starts_with("INSERT INTO steerwishes"))
        .expect("the wish insert");
    assert!(
        insert.contains("'chainend.shift'") && insert.contains("\"outcome\":\"chainend.shift\""),
        "claim names are trimmed like the sugar outcome"
    );
    assert_eq!(
        causal.asks.lock().unwrap().len(),
        1,
        "the declare flow asks causal to plan"
    );
    let ask = the_ask(&causal);
    let minted = minted_id(&db);
    assert!(uuid::Uuid::parse_str(&minted).is_ok());
    assert_eq!(
        ask["wish_id"].as_str().unwrap(),
        minted,
        "the minted wish id rides the ask"
    );
    // Claims verbatim, always (2026-10-22): N=1 rides claims like
    // every other shape — the sugar fold starved the book's re-walk.
    assert_eq!(ask["claims"][0]["outcome"], "chainend.shift");
    assert_eq!(wish["claims"][0]["outcome"], "chainend.shift");
}

#[test]
fn sugar_declare_refused() {
    let (r, _db, _c) = declare_over(&json!({
        "type": "steer",
        "outcome": "chainend.shift",
        "band": {"lo": -0.14, "hi": -0.06},
        "answer": "now",
    }));
    assert!(r.unwrap_err().contains("sugar shape is retired"));
}

#[test]
fn two_claims_accepted_and_stored() {
    let (r, db, causal) = declare_over(&json!({
        "type": "steer",
        "claims": [
            {"outcome": "latency.shift", "band": {"lo": -0.2, "hi": -0.1}},
            {"outcome": "throughput.rate", "band": {"lo": 0.4, "hi": 0.8}}],
        "answer": "now",
    }));
    r.unwrap_or_else(|e| panic!("create failed: {e}"));
    let insert = db
        .queries()
        .into_iter()
        .find(|q| q.trim_start().starts_with("INSERT INTO steerwishes"))
        .expect("the wish insert");
    assert_eq!(
        insert.matches("\"outcome\":").count(),
        2,
        "the stack grammar: N claims per wish, the compile sorts the dials"
    );
    assert!(
        insert.contains("'latency.shift'"),
        "claims[0] materializes the storage columns for existing readers"
    );
    let ask = the_ask(&causal);
    assert_eq!(ask["claims"].as_array().unwrap().len(), 2);
    assert_eq!(ask["claims"][0]["outcome"], "latency.shift");
}

#[test]
fn terms_accepted_and_stored() {
    let claims = r#"[{"outcome": "latency.shift", "band": {"lo": -0.2, "hi": -0.1}}]"#;
    let terms = r#"[{"outcome": "mem.level", "band": {"lo": 0.4, "hi": 1.0}}]"#;
    let db = registry_db(
        wish_row_with("declared", claims, terms),
        events(&["declared"]),
    );
    let causal = Arc::new(FakeCausal::default());
    causal.queue_ask(Ok(refused_answer()));
    let svc = WishService::with_parts(
        db.clone(),
        Arc::new(FakeWindmill::default()),
        causal.clone(),
        false,
    );
    let wish = svc
        .create_wish(&json!({
            "type": "steer",
            "claims": [{"outcome": "latency.shift", "band": {"lo": -0.2, "hi": -0.1}}],
            "terms": [{"outcome": "mem.level", "band": {"lo": 0.4, "hi": 1.0}}],
            "answer": "now",
        }))
        .unwrap_or_else(|e| panic!("create failed: {e}"));
    let insert = db
        .queries()
        .into_iter()
        .find(|q| q.trim_start().starts_with("INSERT INTO steerwishes"))
        .expect("the wish insert");
    assert!(
        insert.contains("\"outcome\":\"mem.level\""),
        "the price rides the wish: claims that carry no dial"
    );
    assert_eq!(
        wish["terms"],
        json!([{"outcome": "mem.level", "band": {"lo": 0.4, "hi": 1.0}}])
    );
}

#[test]
fn stack_shape_rides_the_ask() {
    let (r, _db, causal) = declare_over(&json!({
        "type": "steer",
        "claims": [
            {"outcome": "latency.shift", "band": {"lo": -0.2, "hi": -0.1}},
            {"outcome": "throughput.rate", "band": {"lo": 0.4, "hi": 0.8}}],
        "terms": [{"outcome": "mem.level", "band": {"lo": 0.4, "hi": 1.0}}],
        "answer": "now",
    }));
    r.unwrap_or_else(|e| panic!("create failed: {e}"));
    let ask = the_ask(&causal);
    assert_eq!(
        ask["claims"].as_array().unwrap().len(),
        2,
        "multi-claim wishes ride the stack shape"
    );
    assert!(!ask["terms"].as_array().unwrap().is_empty());
    assert!(ask.get("outcome").is_none(), "no sugar on the ask");
}

#[test]
fn single_claim_rides_the_engine_shape() {
    let (r, _db, causal) = declare_over(&json!({
        "type": "steer",
        "claims": [{"outcome": "chainend.shift", "band": {"lo": -0.14, "hi": -0.06}}],
        "answer": "now",
    }));
    r.unwrap_or_else(|e| panic!("create failed: {e}"));
    let ask = the_ask(&causal);
    // Claims verbatim, always (2026-10-22): the sugar fold starved the
    // book's claims-grammar machinery for single-claim wishes — the
    // stored terms carried claims=[] and the re-walk gates failed
    // silently. N=1 now rides claims like every other shape.
    assert_eq!(
        ask["claims"][0]["outcome"], "chainend.shift",
        "N=1 rides the claims wire like every other shape"
    );
    assert_eq!(ask["claims"][0]["band"]["lo"], -0.14);
    assert_eq!(ask["claims"][0]["band"]["hi"], -0.06);
    assert!(ask.get("outcome").is_none(), "the sugar shape is gone from the ask");
}

#[test]
fn duplicate_outcome_across_claims_and_terms_refused() {
    let (r, _db, _c) = declare_over(&json!({
        "type": "steer",
        "claims": [{"outcome": "latency.shift", "band": {"lo": -0.2, "hi": -0.1}}],
        "terms": [{"outcome": "latency.shift", "band": {"lo": -0.2, "hi": -0.1}}],
        "answer": "now",
    }));
    assert!(r.unwrap_err().contains("one claim per outcome"));
}

#[test]
fn claim_direction_refused() {
    let (r, _db, _c) = declare_over(&json!({
        "type": "steer",
        "claims": [{"outcome": "a/x", "band": {"lo": 0, "hi": 1}, "direction": "minimize"}],
        "answer": "now",
    }));
    assert!(r.unwrap_err().contains("direction rung"));
}

#[test]
fn claims_mixed_with_sugar_refused() {
    let (r, _db, _c) = declare_over(&json!({
        "type": "steer",
        "claims": [{"outcome": "a/x", "band": {"lo": 0, "hi": 1}}],
        "outcome": "a/x",
        "answer": "now",
    }));
    assert!(r.unwrap_err().contains("sugar shape is retired"));
}

#[test]
fn claims_not_a_list_refused() {
    let (r, _db, _c) = declare_over(&json!({
        "type": "steer",
        "claims": "a/x",
        "answer": "now",
    }));
    assert!(r.unwrap_err().contains("claims is required"));
}

#[test]
fn claim_band_gets_the_door_law() {
    let (r, _db, _c) = declare_over(&json!({
        "type": "steer",
        "claims": [{"outcome": "a/x", "band": {"lo": 1, "hi": 0}}],
        "answer": "now",
    }));
    assert!(r
        .unwrap_err()
        .contains("claims[0].band lo must be below hi"));
}

#[test]
fn read_back_synthesizes_claims_for_legacy_rows() {
    let svc = WishService::with_parts(
        registry_db(wish_row("declared"), events(&["declared"])),
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
        false,
    );
    let wish = svc.get_wish(WISH_ID).unwrap().expect("the wish");
    assert_eq!(
        wish["claims"],
        json!([{"outcome": "chainend.shift",
                "band": {"lo": -0.14, "hi": -0.06, "target": -0.10}}]),
        "rows older than the grammar read back as their N=1 sugar"
    );
}

#[test]
fn read_back_exposes_stored_claims() {
    let svc = WishService::with_parts(
        registry_db(
            wish_row_with(
                "declared",
                r#"[{"outcome": "other.shift", "band": {"lo": 0, "hi": 1}}]"#,
                r#"[{"outcome": "mem.level", "band": {"lo": 0.4, "hi": 1.0}}]"#,
            ),
            events(&["declared"]),
        ),
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
        false,
    );
    let wish = svc.get_wish(WISH_ID).unwrap().expect("the wish");
    assert_eq!(
        wish["claims"],
        json!([{"outcome": "other.shift", "band": {"lo": 0, "hi": 1}}])
    );
    assert_eq!(
        wish["terms"],
        json!([{"outcome": "mem.level", "band": {"lo": 0.4, "hi": 1.0}}])
    );
}

#[test]
fn read_back_defaults_terms_to_empty() {
    let svc = WishService::with_parts(
        registry_db(wish_row("declared"), events(&["declared"])),
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
        false,
    );
    let wish = svc.get_wish(WISH_ID).unwrap().expect("the wish");
    assert_eq!(
        wish["terms"],
        json!([]),
        "rows older than the grammar read back with no price"
    );
}

fn update_service(state: &str) -> (WishService, Arc<FakeDb>, Arc<FakeCausal>) {
    let db = registry_db(wish_row(state), events(&["declared", "missed"]));
    let causal = Arc::new(FakeCausal::default());
    causal.queue_ask(Ok(refused_answer()));
    let svc = WishService::with_parts(
        db.clone(),
        Arc::new(FakeWindmill::default()),
        causal.clone(),
        false,
    );
    (svc, db, causal)
}

#[test]
fn update_refuses_band_sugar() {
    let (svc, _db, _c) = update_service("missed");
    let r = svc.update_wish(WISH_ID, &json!({"band": {"lo": 0.2, "hi": 0.4}}));
    assert!(r.unwrap_err().contains("sugar shape is retired"));
}

#[test]
fn update_restatement_accepted() {
    let (svc, db, _c) = update_service("missed");
    svc.update_wish(
        WISH_ID,
        &json!({
            "claims": [{"outcome": "chainend.shift", "band": {"lo": 0.2, "hi": 0.4}}],
            "terms": [{"outcome": "mem.level", "band": {"lo": 0.4, "hi": 1.0}}],
        }),
    )
    .unwrap_or_else(|e| panic!("update failed: {e}"));
    let update = db
        .queries()
        .into_iter()
        .find(|q| q.trim_start().starts_with("UPDATE steerwishes"))
        .expect("the correction restates the wish");
    assert!(update.contains("\"outcome\":\"chainend.shift\""));
    assert!(
        update.contains("\"outcome\":\"mem.level\""),
        "the correction restates the wish on the same identity"
    );
    assert!(stamped(&db, "corrected") >= 1);
}

#[test]
fn update_requires_claims() {
    let (svc, _db, _c) = update_service("missed");
    let r = svc.update_wish(WISH_ID, &json!({}));
    assert!(r.unwrap_err().contains("corrections speak claims"));
}

// ── get / list / close doors ───────────────────────────────────────
// (python mocked the service class; the rust port pins the service
// contract the verb glue passes through — Ok(None) IS the verb's
// "steerwish not found" mapping)

#[test]
fn get_unknown_wish_maps_to_not_found() {
    let db = arc_db(|_db, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.contains("SELECT w.id, w.outcome") {
            return Ok(Some(vec![]));
        }
        if q.contains("SELECT event_type, at, detail") {
            return Ok(Some(vec![]));
        }
        if q.contains("FROM steerwish_lifecycle") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    });
    let svc = WishService::with_parts(
        db,
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
        false,
    );
    assert!(svc.get_wish(WISH_ID).unwrap().is_none());
}

#[test]
fn get_returns_full_wish() {
    let full_row = row(&[
        Some(WISH_ID),
        Some("chainend.shift"),
        Some(r#"{"lo": -0.14, "hi": -0.06}"#),
        None,
        None,
        Some("standing"),
        Some("2026-09-10 10:30:00+00"),
        Some("declared"),
        None,
        None,
        None,
        None,
        None,
    ]);
    let svc = WishService::with_parts(
        registry_db(full_row, events(&["declared"])),
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
        false,
    );
    let wish = svc.get_wish(WISH_ID).unwrap().expect("the wish");
    assert_eq!(
        wish,
        json!({
            "id": WISH_ID,
            "outcome": "chainend.shift",
            "band": {"lo": -0.14, "hi": -0.06},
            "claims": [{"outcome": "chainend.shift", "band": {"lo": -0.14, "hi": -0.06}}],
            "terms": [],
            "limits": null,
            "budget": null,
            "regime": "standing",
            "createdAt": "2026-09-10T10:30:00+00:00",
            "state": "declared",
            "seek": null,
            "answer": null,
            "standard": null,
            "events": [{"type": "declared", "at": "2026-09-10T10:00:00+00:00", "detail": null}],
        })
    );
}

#[test]
fn list_passes_summaries_through() {
    let db = arc_db(|_db, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.contains("ORDER BY w.created_at DESC") {
            return Ok(Some(vec![row(&[
                Some(WISH_ID),
                Some("chainend.shift"),
                Some("planned"),
                Some("2026-09-10 10:30:00+00"),
            ])]));
        }
        if q.contains("FROM steerwish_lifecycle") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    });
    let svc = WishService::with_parts(
        db,
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
        false,
    );
    let summaries = svc.list_wishes().unwrap();
    assert_eq!(
        summaries,
        vec![json!({
            "id": WISH_ID,
            "outcome": "chainend.shift",
            "state": "planned",
            "createdAt": "2026-09-10T10:30:00+00:00",
        })]
    );
}

#[test]
fn close_unknown_wish_maps_to_not_found() {
    let db = arc_db(|_db, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.contains("SELECT w.id, w.outcome") {
            return Ok(Some(vec![]));
        }
        if q.contains("SELECT event_type, at, detail") {
            return Ok(Some(vec![]));
        }
        if q.contains("FROM steerwish_lifecycle") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    });
    let svc = WishService::with_parts(
        db,
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
        false,
    );
    assert!(svc.close_wish(WISH_ID).unwrap().is_none());
}

#[test]
fn close_returns_closed_wish() {
    let svc = WishService::with_parts(
        registry_db(wish_row("closed"), events(&["declared", "closed"])),
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
        false,
    );
    let wish = svc.close_wish(WISH_ID).unwrap().expect("the wish");
    assert_eq!(wish["state"], "closed");
}

// ── the book page dies with the wish (found live 2026-10-23) ────────

#[test]
fn close_purges_causal_book_page() {
    // A closed wish whose causal card survives rides every future
    // plan compile as a ghost term - the room refuses new declares
    // with position_conflict naming a dead tender's hold. The close
    // must purge the card.
    let causal = Arc::new(FakeCausal::default());
    let svc = WishService::with_parts(
        registry_db(wish_row("assigned"), events(&["declared", "assigned"])),
        Arc::new(FakeWindmill::default()),
        causal.clone(),
        false,
    );
    svc.close_wish(WISH_ID).unwrap().expect("the wish");
    let purged = causal.book_purged.lock().unwrap().clone();
    assert!(
        purged.iter().any(|w| w == WISH_ID),
        "close must purge the causal book page, saw {purged:?}"
    );
}

#[test]
fn close_fails_clean_when_book_purge_fails() {
    // Purge first, stamp second: on a failed purge nothing is
    // forgotten and the door retries - the wish keeps its state.
    let causal = Arc::new(FakeCausal::default());
    causal.queue_book_purge(Err("causal unreachable".to_string()));
    let svc = WishService::with_parts(
        registry_db(wish_row("assigned"), events(&["declared", "assigned"])),
        Arc::new(FakeWindmill::default()),
        causal.clone(),
        false,
    );
    let err = svc.close_wish(WISH_ID).unwrap_err();
    assert!(
        err.contains("causal close failed"),
        "the refusal names the causal leg: {err}"
    );
}

// ── service logic against the stubbed registry ─────────────────────

#[test]
fn create_inserts_and_stamps_declared() {
    let (r, db, causal) = declare_over(&json!({
        "type": "steer",
        "claims": [{"outcome": "  chainend.shift  ",
                    "band": {"lo": -0.14, "hi": -0.06, "target": -0.10}}],
        "limits": {"exclude": [], "maxChange": 0.5},
        "budget": 2,
        "answer": "now",
    }));
    let wish = r.unwrap_or_else(|e| panic!("create failed: {e}"));
    let insert = db
        .queries()
        .into_iter()
        .find(|q| q.trim_start().starts_with("INSERT INTO steerwishes"))
        .expect("the wish insert");
    assert!(
        insert.contains("'chainend.shift'"),
        "outcome must be trimmed"
    );
    assert!(insert.contains("\"outcome\":\"chainend.shift\""));
    assert!(
        insert.contains("'standing'"),
        "omitted regime defaults to standing"
    );
    assert!(insert.contains("\"maxChange\":0.5"));
    assert_eq!(stamped(&db, "declared"), 1);
    assert_eq!(
        causal.asks.lock().unwrap().len(),
        1,
        "the declare flow asks causal to plan"
    );
    let ask = the_ask(&causal);
    assert_eq!(
        ask["wish_id"].as_str().unwrap(),
        minted_id(&db),
        "the minted wish id rides the ask"
    );
    // Claims verbatim, always (2026-10-22) — no sugar fold at the ask.
    assert_eq!(ask["claims"][0]["outcome"], "chainend.shift");
    assert_eq!(wish["state"], "declared");
    assert_eq!(wish["budget"], 2);
}

fn planned_flow_db(age: &str, interval: Option<&str>) -> Arc<FakeDb> {
    let age = age.to_string();
    let interval = interval.map(|s| s.to_string());
    let wish = wish_row("declared");
    let evs = events(&["declared", "planned", "assigned"]);
    arc_db(move |_db, q| {
        if q.contains("CREATE TABLE") || q.contains("ALTER TABLE") {
            return Ok(None);
        }
        if q.trim_start().starts_with("INSERT INTO") {
            return Ok(None);
        }
        if q.contains("SELECT w.id, w.outcome") {
            return Ok(Some(vec![wish.clone()]));
        }
        if q.contains("SELECT event_type, at, detail") {
            return Ok(Some(evs.clone()));
        }
        if q.contains("EXTRACT(EPOCH") {
            return Ok(Some(vec![row(&[Some(age.as_str()), interval.as_deref()])]));
        }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            return Ok(Some(vec![row(&[
                Some(WISH_ID),
                Some("t"),
                Some("2026-09-10 10:30:00+00"),
                Some(r#"{"worker_job_ids": ["j1"]}"#),
            ])]));
        }
        if q.contains("SELECT COUNT(*)") {
            return Ok(Some(vec![row(&[Some("0")])]));
        }
        if q.contains("FROM steerwish_lifecycle") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    })
}

fn declare_planned(db: Arc<FakeDb>) -> Arc<FakeCausal> {
    let causal = Arc::new(FakeCausal::default());
    causal.queue_ask(Ok(planned_answer()));
    let svc = WishService::with_parts(db, Arc::new(FakeWindmill::default()), causal.clone(), true);
    svc.create_wish(&json!({
        "type": "steer",
        "claims": [{"outcome": "chainend.shift",
                    "band": {"lo": -0.14, "hi": -0.06, "target": -0.10}}],
        "answer": "now",
    }))
    .unwrap_or_else(|e| panic!("create failed: {e}"));
    causal
}

#[test]
fn create_planned_plants_assignment_row() {
    let db = planned_flow_db("1.0", Some("10.0"));
    let _causal = declare_planned(db.clone());
    // the plan named the dial: the assignment row lands in that
    // tender's own archive DB, causal's book is the plan's home
    let calls = assignment_calls(&db);
    assert_eq!(calls.len(), 1, "one assignment row, exactly");
    assert_eq!(calls[0].0, "systemtender_abc_def");
    assert!(stamped(&db, "planned") >= 1);
    assert!(stamped(&db, "assigned") >= 1);
}

#[test]
fn create_refusal_is_recorded_not_raised() {
    let (r, db, _c) = declare_over(&json!({
        "type": "steer",
        "claims": [{"outcome": "chainend.shift", "band": {"lo": -0.14, "hi": -0.06}}],
        "answer": "now",
    }));
    let wish = r.unwrap_or_else(|e| panic!("a refusal is a book entry, not an error: {e}"));
    assert!(
        stamped(&db, "refused") >= 1,
        "a refusal is a book entry, not an error"
    );
    assert_eq!(wish["state"], "declared");
}

#[test]
fn state_is_latest_event_not_stored() {
    let svc = WishService::with_parts(
        registry_db(wish_row("landed"), events(&["declared", "acted", "landed"])),
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
        false,
    );
    let wish = svc.get_wish(WISH_ID).unwrap().expect("the wish");
    assert_eq!(wish["state"], "landed");
    let types: Vec<&str> = wish["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["type"].as_str().unwrap())
        .collect();
    assert_eq!(types, vec!["declared", "acted", "landed"]);
}

#[test]
fn close_appends_event_once() {
    // python's side_effect [open_row, closed_row]: the first read sees
    // the open wish, the post-close read sees it closed
    let counter = Arc::new(Mutex::new(0u32));
    let open_row = wish_row("landed");
    let closed_row = wish_row("closed");
    let evs = events(&["declared", "landed", "closed"]);
    let counter2 = counter.clone();
    let db = arc_db(move |_db, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.trim_start().starts_with("INSERT INTO") {
            return Ok(None);
        }
        if q.contains("SELECT w.id, w.outcome") {
            let mut n = counter2.lock().unwrap();
            *n += 1;
            return Ok(Some(vec![if *n == 1 {
                open_row.clone()
            } else {
                closed_row.clone()
            }]));
        }
        if q.contains("SELECT event_type, at, detail") {
            return Ok(Some(evs.clone()));
        }
        if q.contains("SELECT COUNT(*)") {
            return Ok(Some(vec![row(&[Some("0")])]));
        }
        if q.contains("FROM steerwish_lifecycle") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    });
    let svc = WishService::with_parts(
        db.clone(),
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
        false,
    );
    let wish = svc.close_wish(WISH_ID).unwrap().expect("the wish");
    assert_eq!(stamped(&db, "closed"), 1);
    assert_eq!(wish["state"], "closed");
}

#[test]
fn close_is_idempotent_on_closed_wish() {
    let db = registry_db(wish_row("closed"), events(&["declared", "closed"]));
    let svc = WishService::with_parts(
        db.clone(),
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
        false,
    );
    let wish = svc.close_wish(WISH_ID).unwrap().expect("the wish");
    assert_eq!(
        stamped(&db, "closed"),
        0,
        "already closed, nothing appended"
    );
    assert_eq!(wish["state"], "closed");
}

#[test]
fn get_unknown_wish_is_none() {
    let db = arc_db(|_db, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.contains("SELECT w.id, w.outcome") {
            return Ok(Some(vec![]));
        }
        if q.contains("SELECT event_type, at, detail") {
            return Ok(Some(vec![]));
        }
        if q.contains("FROM steerwish_lifecycle") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    });
    let svc = WishService::with_parts(
        db,
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
        false,
    );
    assert!(svc.get_wish(WISH_ID).unwrap().is_none());
}

// ── the lazy fold-in (reading is asking) ───────────────────────────

fn book_page() -> Value {
    json!({
        "wish_id": WISH_ID,
        "status": "missed",
        "instruction": "remeasure",
        "events": [
            {"tsz": 1000.0, "event": "planned", "detail": null},
            {"tsz": 2000.0, "event": "missed",
             "detail": {"median": -0.17, "bar": 0.02, "n": 5}},
            {"tsz": 2500.0, "event": "walk_opened",
             "detail": {"held": 0.62, "separated": true}},
        ],
    })
}

fn mirrored(db: &FakeDb) -> Vec<String> {
    db.queries()
        .into_iter()
        .filter(|q| q.contains("TO_TIMESTAMP") && q.trim_start().starts_with("INSERT INTO"))
        .collect()
}

#[test]
fn get_mirrors_unseen_book_events() {
    let db = registry_db(wish_row("missed"), events(&["declared"]));
    let causal = Arc::new(FakeCausal::default());
    causal.queue_page(Some(book_page()));
    let svc = WishService::with_parts(
        db.clone(),
        Arc::new(FakeWindmill::default()),
        causal.clone(),
        false,
    );
    let _ = svc.get_wish(WISH_ID).unwrap();
    let m = mirrored(&db);
    assert!(m
        .iter()
        .any(|q| q.contains("'planned'") && q.contains("TO_TIMESTAMP(1000)")));
    assert!(m
        .iter()
        .any(|q| q.contains("'missed'") && q.contains("TO_TIMESTAMP(2000)")));
    assert!(
        m.iter()
            .any(|q| q.contains("'walk_opened'") && q.contains("TO_TIMESTAMP(2500)")),
        "the full story, not just state words"
    );
    assert_eq!(
        db.queries()
            .iter()
            .filter(|q| q.contains("SELECT COUNT(*)"))
            .count(),
        3
    );
}

#[test]
fn get_fold_in_dedupes_by_type_and_time() {
    let db = {
        // the 'missed' event is already mirrored; the rest are not
        arc_db(move |_db, q| {
            if q.contains("CREATE TABLE") {
                return Ok(None);
            }
            if q.trim_start().starts_with("INSERT INTO") {
                return Ok(None);
            }
            if q.contains("SELECT w.id, w.outcome") {
                return Ok(Some(vec![wish_row("missed")]));
            }
            if q.contains("SELECT event_type, at, detail") {
                return Ok(Some(events(&["declared"])));
            }
            if q.contains("SELECT COUNT(*)") {
                let seen = q.contains("event_type = 'missed'");
                return Ok(Some(vec![row(&[Some(if seen { "1" } else { "0" })])]));
            }
            if q.contains("FROM steerwish_lifecycle") {
                return Ok(Some(vec![]));
            }
            Ok(None)
        })
    };
    let causal = Arc::new(FakeCausal::default());
    causal.queue_page(Some(book_page()));
    let svc = WishService::with_parts(
        db.clone(),
        Arc::new(FakeWindmill::default()),
        causal.clone(),
        false,
    );
    let _ = svc.get_wish(WISH_ID).unwrap();
    // the mirrored INSERT is VALUES('<wish_id>', '<kind>', '<detail>',
    // TO_TIMESTAMP(...)) - the kind is the second quoted value
    let mirrored_kinds: Vec<String> = mirrored(&db)
        .iter()
        .map(|q| {
            let start = q.find("VALUES('").expect("values") + "VALUES('".len();
            let after_id = start + WISH_ID.len() + 4; // skip id + "', '"
            let end = q[after_id..].find('\'').expect("kind end") + after_id;
            q[after_id..end].to_string()
        })
        .collect();
    assert_eq!(
        mirrored_kinds,
        vec!["planned".to_string(), "walk_opened".to_string()],
        "seen events stay single"
    );
}

#[test]
fn get_is_silent_when_the_book_is_unreachable() {
    let db = registry_db(wish_row("declared"), events(&["declared"]));
    let causal = Arc::new(FakeCausal::default());
    causal.queue_page(None);
    let svc = WishService::with_parts(
        db.clone(),
        Arc::new(FakeWindmill::default()),
        causal.clone(),
        false,
    );
    let wish = svc.get_wish(WISH_ID).unwrap().expect("the wish");
    assert!(mirrored(&db).is_empty());
    assert_eq!(wish["state"], "declared", "stale mirror, no drama");
}

#[test]
fn list_folds_only_open_wishes() {
    let db = arc_db(|_db, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.trim_start().starts_with("INSERT INTO") {
            return Ok(None);
        }
        if q.contains("ORDER BY w.created_at DESC") {
            return Ok(Some(vec![
                row(&[
                    Some("w-open"),
                    Some("chainend.shift"),
                    Some("serving"),
                    Some("2026-09-10 10:30:00+00"),
                ]),
                row(&[
                    Some("w-closed"),
                    Some("chainend.shift"),
                    Some("closed"),
                    Some("2026-09-10 10:30:00+00"),
                ]),
            ]));
        }
        if q.contains("SELECT COUNT(*)") {
            return Ok(Some(vec![row(&[Some("0")])]));
        }
        if q.contains("FROM steerwish_lifecycle") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    });
    let causal = Arc::new(FakeCausal::default());
    causal.queue_page(Some(book_page()));
    let svc = WishService::with_parts(
        db.clone(),
        Arc::new(FakeWindmill::default()),
        causal.clone(),
        false,
    );
    let _ = svc.list_wishes().unwrap();
    let counts = db
        .queries()
        .into_iter()
        .filter(|q| q.contains("SELECT COUNT(*)"))
        .collect::<Vec<_>>();
    assert!(
        counts.iter().any(|q| q.contains("wish_id = 'w-open'")),
        "the open wish re-asks the book"
    );
    assert!(
        !counts.iter().any(|q| q.contains("w-closed")),
        "closed wishes never re-ask"
    );
}

// ── budget rides the ask ───────────────────────────────────────────

#[test]
fn create_sends_budget_in_plan_ask() {
    let (_r, _db, causal) = declare_over(&json!({
        "type": "steer",
        "claims": [{"outcome": "chainend.shift",
                    "band": {"lo": -0.14, "hi": -0.06, "target": -0.10}}],
        "budget": 2,
        "answer": "now",
    }));
    let ask = the_ask(&causal);
    assert_eq!(ask["budget"], 2, "the allowance rides the ask");
}

#[test]
fn create_without_budget_omits_the_field() {
    let (_r, _db, causal) = declare_over(&json!({
        "type": "steer",
        "claims": [{"outcome": "chainend.shift", "band": {"lo": -0.14, "hi": -0.06}}],
        "answer": "now",
    }));
    let ask = the_ask(&causal);
    assert!(ask.get("budget").is_none(), "standing wishes carry no cap");
}

// ── door validation (finiteness, limits shape, maxChange range) ────

fn refused_at_the_door(payload: Value, fragment: &str) {
    let (r, _db, _c) = declare_over(&payload);
    match r {
        Err(e) => assert!(e.contains(fragment), "expected {fragment:?} in {e:?}"),
        Ok(_) => panic!("wish with {fragment:?} must be refused at the door"),
    }
}

#[test]
fn infinite_band_edges_never_reach_the_door() {
    // python fed float('-inf') straight into the service (a python
    // dict, never JSON). serde_json cannot carry non-finite numbers:
    // the wire refuses them at parse, before any service exists. The
    // finite guards stay ported as defense; this is the receipt.
    match serde_json::from_str::<Value>(r#"{ "lo": 1e999, "hi": -0.06 }"#) {
        Err(_) => { /* the wire refuses non-finite numbers at parse */ }
        Ok(band) => {
            assert!(checked_steer_band(&band, "claims[0].band").is_err());
        }
    }
}

#[test]
fn nan_target_never_reaches_the_door() {
    // python's float('nan') rode a python dict; the JSON wire has no
    // NaN literal — the parse itself refuses it
    let parsed: Result<Value, _> =
        serde_json::from_str(r#"{ "lo": -0.14, "hi": -0.06, "target": NaN }"#);
    assert!(parsed.is_err(), "NaN is not JSON; it must not parse");
}

#[test]
fn exclude_must_be_names() {
    refused_at_the_door(
        json!({
            "type": "steer",
            "claims": [{"outcome": "chainend.shift", "band": {"lo": -0.14, "hi": -0.06}}],
            "limits": {"exclude": [7]},
            "answer": "now",
        }),
        "list of param names",
    );
}

#[test]
fn max_change_bounds_refused() {
    let base = json!({
        "type": "steer",
        "claims": [{"outcome": "chainend.shift", "band": {"lo": -0.14, "hi": -0.06}}],
        "answer": "now",
    });
    for bad in [json!(0.0), json!(1.0), json!(1.5), json!(-0.2), json!(true)] {
        let mut payload = base.clone();
        payload["limits"] = json!({ "maxChange": bad });
        refused_at_the_door(payload, "(0, 1)");
    }
}

#[test]
fn max_change_half_range_accepted() {
    let (r, _db, _c) = declare_over(&json!({
        "type": "steer",
        "claims": [{"outcome": "chainend.shift", "band": {"lo": -0.14, "hi": -0.06}}],
        "limits": {"exclude": ["param_1"], "maxChange": 0.5},
        "answer": "now",
    }));
    let wish = r.unwrap_or_else(|e| panic!("half-range must pass: {e}"));
    assert_eq!(wish["state"], "declared");
}

// ── sender liveness at the door ────────────────────────────────────
// Never plant into a house that is not breathing (flight 9's
// archaeology). The door reads the tender's heartbeat and refuses
// loudly.

fn liveness_db(
    mode: &str, // "plain" | "retry" | "unreachable"
    age: f64,
    interval: Option<f64>,
    meta_empty: bool,
) -> (Arc<FakeDb>, Arc<Mutex<u32>>, Arc<Mutex<u32>>) {
    let wish = wish_row("declared");
    let evs = events(&["declared", "planned", "plan_error"]);
    let heartbeat_reads = Arc::new(Mutex::new(0u32));
    let ageonly_reads = Arc::new(Mutex::new(0u32));
    let age_s = age.to_string();
    let interval_s = interval.map(|v| v.to_string());
    let mode = mode.to_string();
    let hb = heartbeat_reads.clone();
    let ao = ageonly_reads.clone();
    let db = arc_db(move |_db, q| {
        if q.contains("CREATE TABLE") || q.contains("ALTER TABLE") {
            return Ok(None);
        }
        if q.trim_start().starts_with("INSERT INTO") {
            return Ok(None);
        }
        if q.contains("SELECT w.id, w.outcome") {
            return Ok(Some(vec![wish.clone()]));
        }
        if q.contains("SELECT event_type, at, detail") {
            return Ok(Some(evs.clone()));
        }
        if q.contains("beat_interval_secs") {
            if mode == "unreachable" {
                return Err(DbError("conn refused".to_string()));
            }
            if mode == "retry" {
                let mut n = hb.lock().unwrap();
                *n += 1;
                if *n == 1 {
                    return Err(DbError(
                        r#"column "beat_interval_secs" does not exist"#.to_string(),
                    ));
                }
                return Ok(Some(vec![row(&[Some("299.0"), Some("10.0")])]));
            }
            return Ok(Some(vec![row(&[
                Some(age_s.as_str()),
                interval_s.as_deref(),
            ])]));
        }
        if q.contains("EXTRACT(EPOCH") {
            if mode == "retry" {
                let mut n = ao.lock().unwrap();
                *n += 1;
                return Ok(Some(vec![row(&[Some("55.0"), None])]));
            }
        }
        if q.contains("FROM systemtender_meta_data WHERE id") {
            if meta_empty {
                return Ok(Some(vec![]));
            }
            return Ok(Some(vec![row(&[
                Some(WISH_ID),
                Some("t"),
                Some("2026-09-10 10:30:00+00"),
                Some(r#"{"worker_job_ids": ["j1"]}"#),
            ])]));
        }
        if q.contains("SELECT COUNT(*)") {
            return Ok(Some(vec![row(&[Some("0")])]));
        }
        if q.contains("FROM steerwish_lifecycle") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    });
    (db, heartbeat_reads, ageonly_reads)
}

fn liveness_declare(db: Arc<FakeDb>) {
    let causal = Arc::new(FakeCausal::default());
    causal.queue_ask(Ok(planned_answer()));
    let svc = WishService::with_parts(db, Arc::new(FakeWindmill::default()), causal, true);
    svc.create_wish(&json!({
        "type": "steer",
        "claims": [{"outcome": "chainend.shift", "band": {"lo": -0.14, "hi": -0.06}}],
        "answer": "now",
    }))
    .unwrap_or_else(|e| panic!("declare failed: {e}"));
}

fn plan_errors(db: &FakeDb) -> Vec<String> {
    db.queries()
        .into_iter()
        .filter(|q| {
            q.trim_start().starts_with("INSERT INTO steerwish_events") && q.contains("'plan_error'")
        })
        .collect()
}

/// The door's refusals (reason no_tender), as captured inserts.
fn refused_events(db: &FakeDb) -> Vec<String> {
    db.queries()
        .into_iter()
        .filter(|q| {
            q.trim_start().starts_with("INSERT INTO steerwish_events") && q.contains("'refused'")
        })
        .collect()
}

#[test]
fn fresh_heartbeat_plants() {
    let (db, _hb, _ao) = liveness_db("plain", 1.0, Some(10.0), false);
    liveness_declare(db.clone());
    assert_eq!(assignment_calls(&db).len(), 1);
    assert!(stamped(&db, "assigned") >= 1);
    assert!(!plan_errors(&db).iter().any(|q| q.contains("not alive")));
}

#[test]
fn window_derives_from_measured_cadence() {
    // a slow-but-honest cadence widens its own window: 250s is dead
    // under a 10s cadence (window 30) and alive under a 100s one
    let (alive_db, _hb, _ao) = liveness_db("plain", 250.0, Some(100.0), false);
    liveness_declare(alive_db.clone());
    assert_eq!(assignment_calls(&alive_db).len(), 1);
    let (dead_db, _hb2, _ao2) = liveness_db("plain", 250.0, Some(10.0), false);
    liveness_declare(dead_db.clone());
    assert!(assignment_calls(&dead_db).is_empty());
}

#[test]
fn stale_heartbeat_refused_loudly() {
    // The acceptance door (2026-10-21): a stale heartbeat means no
    // living tender will take the tending - refused (no_tender), the
    // stale heartbeat named inside the refusal's dead list, nothing
    // planted.
    let (db, _hb, _ao) = liveness_db("plain", 99999.0, Some(10.0), false);
    liveness_declare(db.clone());
    assert!(assignment_calls(&db).is_empty());
    let refusals = refused_events(&db);
    assert!(
        refusals.iter().any(|q| q.contains("no_tender") && q.contains("heartbeat stale")),
        "the refusal must name the stale heartbeat: {refusals:?}"
    );
    assert!(
        !plan_errors(&db)
            .iter()
            .any(|q| q.contains("not alive")),
        "a dead tender is a refusal, not an error"
    );
}

#[test]
fn fallback_window_before_first_measured_beat() {
    // interval NULL (no second beat yet): the fixed fallback window
    // (60s unless the env overrides it)
    let (fresh_db, _hb, _ao) = liveness_db("plain", 59.0, None, false);
    liveness_declare(fresh_db.clone());
    assert_eq!(assignment_calls(&fresh_db).len(), 1);
    let (stale_db, _hb2, _ao2) = liveness_db("plain", 61.0, None, false);
    liveness_declare(stale_db.clone());
    assert!(assignment_calls(&stale_db).is_empty());
}

#[test]
fn age_only_fallback_when_column_missing() {
    // a state row born before the interval column: the read falls
    // back to age-only, and the fallback window decides
    let (db, heartbeat_reads, ageonly_reads) = liveness_db("retry", 0.0, None, false);
    liveness_declare(db.clone());
    assert_eq!(
        *heartbeat_reads.lock().unwrap(),
        1,
        "one full read (it refused on the missing column)"
    );
    assert_eq!(*ageonly_reads.lock().unwrap(), 1, "one age-only retry");
    assert_eq!(assignment_calls(&db).len(), 1);
    assert!(stamped(&db, "assigned") >= 1);
}

#[test]
fn unknown_tender_refused() {
    // registry returned no row: refused before any heartbeat read -
    // the refusal names the unknown tender
    let (db, _hb, _ao) = liveness_db("plain", 1.0, Some(10.0), true);
    liveness_declare(db.clone());
    assert!(assignment_calls(&db).is_empty());
    let refusals = refused_events(&db);
    assert!(
        refusals.iter().any(|q| q.contains("no_tender") && q.contains("unknown tender")),
        "{refusals:?}"
    );
}

#[test]
fn unreachable_archive_refused() {
    // the heartbeat read itself failed: the tender cannot answer, so
    // it cannot tend - the refusal names the unreachable archive
    let (db, _hb, _ao) = liveness_db("unreachable", 0.0, None, false);
    liveness_declare(db.clone());
    assert!(assignment_calls(&db).is_empty());
    let refusals = refused_events(&db);
    assert!(
        refusals.iter().any(|q| q.contains("no_tender") && q.contains("unreachable")),
        "{refusals:?}"
    );
}

// ── the wish deletion state machine ────────────────────────────────
// single-flight claim, close before forgetting, registry row as
// finalizer, deletion-failed on causal trouble.

fn delete_db(mode: &str) -> Arc<FakeDb> {
    let wish = wish_row("declared");
    let evs = events(&["declared"]);
    let counter = Arc::new(Mutex::new(0u32));
    let mode = mode.to_string();
    let counter2 = counter.clone();
    arc_db(move |_db, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        // the claim is an INSERT that RETURNS rows - answer it before
        // the insert-catcher swallows it
        if q.contains("RETURNING wish_id") {
            if mode == "rides-along" {
                return Ok(Some(vec![]));
            }
            return Ok(Some(vec![row(&[Some("claimed")])]));
        }
        if q.contains("SELECT state, reason FROM steerwish_lifecycle WHERE wish_id") {
            if mode == "rides-along" {
                return Ok(Some(vec![row(&[Some("deleting"), None])]));
            }
            return Ok(Some(vec![]));
        }
        if q.contains("SELECT w.id, w.outcome") {
            if mode == "gone" {
                return Ok(Some(vec![]));
            }
            if mode == "close-fails" {
                let mut n = counter2.lock().unwrap();
                *n += 1;
                if *n >= 2 {
                    return Err(DbError("causal unreachable".to_string()));
                }
            }
            return Ok(Some(vec![wish.clone()]));
        }
        if q.trim_start().starts_with("INSERT INTO")
            || q.trim_start().starts_with("UPDATE ")
            || q.trim_start().starts_with("DELETE FROM")
        {
            return Ok(None);
        }
        if q.contains("SELECT event_type, at, detail") {
            return Ok(Some(evs.clone()));
        }
        if q.contains("SELECT COUNT(*)") {
            return Ok(Some(vec![row(&[Some("0")])]));
        }
        if q.contains("FROM steerwish_lifecycle") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    })
}

fn delete_over(mode: &str) -> (Value, Arc<FakeDb>) {
    let db = delete_db(mode);
    let svc = WishService::with_parts(
        db.clone(),
        Arc::new(FakeWindmill::default()),
        Arc::new(FakeCausal::default()),
        false,
    );
    (svc.delete_wish(WISH_ID), db)
}

#[test]
fn purge_closes_then_removes_rows_last() {
    let (r, db) = delete_over("executes");
    assert_eq!(r["result"], "SUCCESS");
    assert_eq!(r["data"]["delete_type"], "executed");
    let qs = db.queries();
    let closed_at = qs
        .iter()
        .position(|q| {
            q.trim_start().starts_with("INSERT INTO steerwish_events") && q.contains("'closed'")
        })
        .expect("the owner's act ran");
    let registry_at = qs
        .iter()
        .position(|q| q.trim_start().starts_with("DELETE FROM steerwishes"))
        .expect("the registry row was dropped");
    let lifecycle_at = qs
        .iter()
        .position(|q| {
            q.trim_start()
                .starts_with("DELETE FROM steerwish_lifecycle")
        })
        .expect("the finalizer row was swept");
    assert_eq!(stamped(&db, "closed"), 1);
    assert!(
        closed_at < registry_at && registry_at < lifecycle_at,
        "close first, registry row last"
    );
}

#[test]
fn purge_is_idempotent_when_wish_already_gone() {
    let (r, db) = delete_over("gone");
    assert_eq!(r["result"], "SUCCESS");
    assert_eq!(r["data"]["delete_type"], "already-gone");
    // the stale lifecycle row is swept with it
    assert!(db.queries().iter().any(|q| q
        .trim_start()
        .starts_with("DELETE FROM steerwish_lifecycle")));
    assert_eq!(stamped(&db, "closed"), 0, "nothing to close");
    assert!(!db
        .queries()
        .iter()
        .any(|q| q.trim_start().starts_with("DELETE FROM steerwishes")));
}

#[test]
fn purge_rides_along_when_an_executor_holds_the_claim() {
    let (r, db) = delete_over("rides-along");
    assert_eq!(r["result"], "SUCCESS");
    assert_eq!(r["data"]["status"], "deleting");
    assert_eq!(r["data"]["note"], "deletion already in progress");
    assert_eq!(stamped(&db, "closed"), 0);
    assert!(!db
        .queries()
        .iter()
        .any(|q| q.trim_start().starts_with("DELETE FROM steerwishes")));
}

#[test]
fn purge_fails_closed_when_causal_close_fails() {
    let (r, db) = delete_over("close-fails");
    assert_eq!(r["result"], "FAILURE");
    assert!(r["error"].as_str().unwrap().contains("causal close failed"));
    // deletion-failed flags the state machine; nothing forgotten
    assert!(db.queries().iter().any(|q| {
        q.contains("INSERT INTO steerwish_lifecycle") && q.contains("'deletion-failed'")
    }));
    assert!(db
        .queries()
        .iter()
        .any(|q| q.contains("INSERT INTO steerwish_lifecycle") && q.contains("causal")));
    assert!(!db
        .queries()
        .iter()
        .any(|q| q.trim_start().starts_with("DELETE FROM steerwises")));
    assert!(!db.queries().iter().any(|q| q
        .trim_start()
        .starts_with("DELETE FROM steerwish_lifecycle")));
}

// ── the list query's summary contract ──────────────────────────────
// The list query's columns must match format_summary's reads (estate
// receipt 2026-10-13: regime rode at position 2 and the summary
// isoformat'd the state string). This pin reads the REAL query text.

#[test]
fn list_columns_in_summary_order() {
    let db = arc_db(|_db, _q| Ok(None));
    let repo = MetadataDatabaseRepository::with_gateway(DbConfig::default(), db.clone());
    repo.fetch_wishes_list().unwrap();
    let q = db.queries()[0].clone();
    let head_end = q.find("COALESCE").expect("the derived-state subquery");
    let head = &q[..head_end];
    assert!(
        !head.contains("w.regime"),
        "list query column drift: a column between outcome and state shifts \
         every read after it - the summary isoformats the state string and \
         the list 500s"
    );
    assert!(head.contains("SELECT w.id, w.outcome"));
    assert!(q[head_end..].contains("w.created_at"));
}


// ── the reconcile family (designs/2026-10-18-worker-death-door, round 2) ──

fn reconcile_db(
    stale_deletions: Vec<String>,
    unasked: Vec<String>,
    claim_answers: bool,
) -> Arc<FakeDb> {
    arc_db(move |_db, q| {
        if q.contains("CREATE TABLE") {
            return Ok(None);
        }
        if q.contains("FROM steerwish_lifecycle") && q.contains("state IN") {
            return Ok(Some(
                stale_deletions.iter().map(|id| row(&[Some(id.as_str())])).collect(),
            ));
        }
        if q.contains("DISTINCT ON (wish_id)") {
            return Ok(Some(
                unasked.iter().map(|id| row(&[Some(id.as_str())])).collect(),
            ));
        }
        if q.contains("RETURNING wish_id") {
            return Ok(Some(if claim_answers {
                vec![row(&[Some("claimed")])]
            } else {
                vec![]
            }));
        }
        // trap order (registry_db's rule): the wish-fetch answers before
        // the events check - its COALESCE subquery reads the events table.
        // Both match single-line SELECT heads: the real queries break
        // lines between FROM and WHERE, so FROM-clause spans never match.
        if q.contains("SELECT w.id, w.outcome, w.band") {
            return Ok(Some(vec![wish_row("standing")]));
        }
        if q.contains("SELECT event_type, at, detail") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    })
}

#[test]
fn reconcile_converges_a_stale_wish_deletion() {
    // the close-and-drop the failure envelope prescribes, automated:
    // closed event stamped, row dropped, marker swept
    let wmill = Arc::new(FakeWindmill::default());
    let db = reconcile_db(vec![WISH_ID.to_string()], vec![], true);
    let svc = WishService::with_parts(db.clone(), wmill, Arc::new(FakeCausal::default()), false);
    let r = svc.reconcile_wishes();
    assert_eq!(r["converged"], 1, "one deletion converged: {r}");
    assert!(db.queries().iter().any(|q| q.contains("'closed'")));
    assert!(db
        .queries()
        .iter()
        .any(|q| q.contains("DELETE FROM steerwishes")));
    assert!(db
        .queries()
        .iter()
        .any(|q| q.contains("DELETE FROM steerwish_lifecycle")));
}

#[test]
fn reconcile_leaves_a_fresh_claim_alone() {
    // a live executor holds the slot: not ours to take
    let db = reconcile_db(vec![WISH_ID.to_string()], vec![], false);
    let svc = WishService::with_parts(db.clone(), Arc::new(FakeWindmill::default()), Arc::new(FakeCausal::default()), false);
    let r = svc.reconcile_wishes();
    assert_eq!(r["converged"], 0);
    assert!(!db
        .queries()
        .iter()
        .any(|q| q.contains("DELETE FROM steerwishes")));
}

#[test]
fn reconcile_redrives_an_unasked_wish() {
    let wmill = Arc::new(FakeWindmill::default());
    wmill.queue_async(Ok("job-plan-1".to_string()));
    let db = reconcile_db(vec![], vec![WISH_ID.to_string()], false);
    let svc = WishService::with_parts(db, wmill, Arc::new(FakeCausal::default()), false);
    let r = svc.reconcile_wishes();
    assert_eq!(r["redriven"], 1, "the ask re-dispatched: {r}");
}

#[test]
fn reconcile_is_quiet_when_every_ask_landed() {
    let wmill = Arc::new(FakeWindmill::default());
    let db = reconcile_db(vec![], vec![], false);
    let svc = WishService::with_parts(db, wmill, Arc::new(FakeCausal::default()), false);
    let r = svc.reconcile_wishes();
    assert_eq!(r["redriven"], 0);
    assert_eq!(r["converged"], 0);
    assert_eq!(r["failed"], 0);
}
