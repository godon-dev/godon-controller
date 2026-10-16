//! Port of test_seek_wish_door.py — the seek wish at the controller's
//! door: validation, declaration, the ask.
//!
//! Each test names the ruling it pins:
//! - an impossible wish never starts (candidates over budget refuses
//!   HERE, before anything plans on it)
//! - cover_now admits with empty search fields and REFUSES a budget
//!   (it walks on standing duty)
//! - the registry row carries regime 'seek' and the body in its own
//!   column — claims keep their pinned meaning
//! - the ask rides the re-pinned wire (type 'seek', walk body under
//!   'seek', form-keyed); a planned answer plants the sender's
//!   assignment through the same alive-door holds use; a refusal lands
//!   as an event carrying reason+detail verbatim — map_reads_it's
//!   reading reaches the declarer inside the refusal.

use crate::harness::{row, FakeCausal, FakeDb, FakeWindmill};
use godon_controller::seek_grammar::checked_seek_body;
use godon_controller::wish_service::WishService;
use serde_json::{json, Value};

const SENDER: &str = "670eee18-0216-4628-942b-94ae89effcee";
const SENDER_DB: &str = "systemtender_670eee18_0216_4628_942b_94ae89effcee";
const CREATED: &str = "2026-10-12 19:00:00+00";

fn seek_payload(over: Value) -> Value {
    let mut body = json!({
        "type": "seek",
        "form": "find_best",
        "region": {"param_0": [0.0, 100.0, false]},
        "criterion": {"outcome": "room/objective_1", "direction": "maximize"},
        "budget": 50,
        "candidates": 5,
    });
    if let (Some(target), Some(source)) = (body.as_object_mut(), over.as_object()) {
        for (k, v) in source {
            target.insert(k.clone(), v.clone());
        }
    }
    body
}

fn walk_body() -> Value {
    checked_seek_body(&seek_payload(json!({}))).unwrap()
}

/// The service over one fake db: the wish row served for the fetch,
/// the sender's registry row + a young heartbeat for the alive door.
fn service(known_sender: bool) -> (WishService, std::sync::Arc<FakeDb>, std::sync::Arc<FakeWindmill>) {
    let walk_text = serde_json::to_string(&walk_body()).unwrap();
    let db = FakeDb::with_handler(move |q| {
        if q.contains("INSERT INTO") {
            return Ok(None);
        }
        if q.contains("EXTRACT(EPOCH")) {
            return Ok(Some(vec![row(&[Some("5.0"), Some("10.0")])]));
        }
        if let Some(pos) = q.find("WHERE id = '") {
            let rest = &q[pos + "WHERE id = '".len()..];
            let id = &rest[..rest.find("';").unwrap_or(rest.len())];
            if id == SENDER {
                return Ok(Some(vec![row(&[Some(SENDER), Some("tender-1"), None, Some("{}")])]));
            }
            return Ok(Some(vec![]));
        }
        if q.contains("FROM steerwishes") {
            return Ok(Some(vec![row(&[
                Some("w1"),
                Some("room/objective_1"),
                None,
                None,
                Some("50"),
                Some("seek"),
                Some(CREATED),
                Some("declared"),
                None,
                None,
                Some(walk_text.as_str()),
                Some("now"),
                Some("best"),
            ])]));
        }
        if q.contains("FROM steerwish_events") {
            return Ok(Some(vec![]));
        }
        if q.contains("FROM steerwish_lifecycle") {
            return Ok(Some(vec![]));
        }
        if q.contains("FROM ledger_state") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    });
    let wmill = FakeWindmill::new();
    let svc = WishService::with_parts(
        db.clone(),
        wmill.clone(),
        FakeCausal::new(),
        known_sender,
    );
    (svc, db, wmill)
}

fn events_inserted(db: &FakeDb) -> Vec<String> {
    db.queries()
        .into_iter()
        .filter(|q| q.trim_start().starts_with("INSERT INTO steerwish_events"))
        .collect()
}

// ── validation door ─────────────────────────────────────────────────

#[test]
fn candidates_over_budget_never_starts() {
    let (svc, _, _) = service(false);
    let err = svc
        .create_wish(&seek_payload(json!({"candidates": 51, "budget": 50})))
        .unwrap_err();
    assert!(err.contains("exceeds the budget"), "{}", err);
}

#[test]
fn find_best_without_criterion_refuses() {
    let (svc, _, _) = service(false);
    let err = svc.create_wish(&seek_payload(json!({"criterion": null}))).unwrap_err();
    assert!(err.contains("criterion"), "{}", err);
}

#[test]
fn unknown_direction_refuses() {
    let (svc, _, _) = service(false);
    let err = svc
        .create_wish(&seek_payload(json!({"criterion": {"outcome": "x", "direction": "toward"}})))
        .unwrap_err();
    assert!(err.contains("direction"), "{}", err);
}

#[test]
fn find_best_without_budget_refuses() {
    let (svc, _, _) = service(false);
    let err = svc.create_wish(&seek_payload(json!({"budget": null}))).unwrap_err();
    assert!(err.to_lowercase().contains("budget"), "{}", err);
}

#[test]
fn empty_region_refuses() {
    let (svc, _, _) = service(false);
    let err = svc.create_wish(&seek_payload(json!({"region": {}}))).unwrap_err();
    assert!(err.contains("region"), "{}", err);
}

#[test]
fn inverted_range_refuses_named() {
    let (svc, _, _) = service(false);
    let err = svc
        .create_wish(&seek_payload(json!({"region": {"param_0": [100.0, 0.0, false]}})))
        .unwrap_err();
    assert!(err.contains("param_0"), "{}", err);
}

#[test]
fn cover_now_refuses_a_budget() {
    let (svc, _, _) = service(false);
    let err = svc
        .create_wish(&seek_payload(json!({"form": "cover_now", "criterion": null, "budget": 10})))
        .unwrap_err();
    assert!(err.contains("no budget"), "{}", err);
}

#[test]
fn cover_now_admits_without_search_fields() {
    let body = checked_seek_body(&json!({
        "form": "cover_now",
        "region": {"param_0": [0.0, 100.0, false]},
    }))
    .unwrap();
    assert_eq!(body["criterion"], json!(null));
    assert_eq!(body["candidates"], json!(0));
}

#[test]
fn region_normalizes_shapes() {
    let body = checked_seek_body(&json!({
        "form": "find_best",
        "region": {"n": [1, 8, true], "f": [0.0, 10.0, false]},
        "criterion": {"outcome": "o", "direction": "min"},
        "budget": 10,
    }))
    .unwrap();
    assert_eq!(body["region"]["n"], json!([1, 8, true]));
    assert_eq!(body["region"]["f"], json!([0.0, 10.0, false]));
}

#[test]
fn estate_direction_words_admit() {
    for word in ["maximize", "max", "minimize", "min"] {
        let body = checked_seek_body(&json!({
            "form": "find_best",
            "region": {"param_0": [0.0, 1.0, false]},
            "criterion": {"outcome": "o", "direction": word},
            "budget": 10,
        }))
        .unwrap_or_else(|e| panic!("{} refused: {}", word, e));
        assert_eq!(body["criterion"]["direction"], json!(word));
    }
}

// ── declaration ──────────────────────────────────────────────────────

#[test]
fn declared_row_carries_the_body_in_its_own_column() {
    // answer 'later': the ask rides its own dispatch, the row stands.
    let (svc, db, wmill) = service(true);
    svc.create_wish(&seek_payload(json!({}))).expect("declared");
    let insert = db
        .queries()
        .into_iter()
        .find(|q| q.starts_with("INSERT INTO steerwishes"))
        .expect("registry insert");
    assert!(insert.contains("'seek'"), "regime: {}", insert);
    assert!(insert.contains("'room/objective_1'"), "{}", insert);
    assert!(insert.contains("NULL, NULL, '{"), "claims/terms stay NULL: {}", insert);
    assert!(insert.contains("\"form\":\"find_best\""), "{}", insert);
    assert!(insert.contains(", 50,"), "{}", insert);
    // the later answer dispatched the plan job, no causal ask
    assert_eq!(wmill.async_dispatches.lock().unwrap().len(), 1);
    assert_eq!(
        wmill.async_dispatches.lock().unwrap()[0].0,
        "f/controller/steerwish_plan"
    );
}

#[test]
fn planned_answer_plants_the_senders_assignment() {
    let (svc, db, _) = service(true);
    svc.causal_ask_override_for_test();
    let causal = FakeCausal::new();
    causal.push_plan(Ok(json!({"status": "planned", "sender": SENDER})));
    let svc = WishService::with_parts(db.clone(), FakeWindmill::new(), causal.clone(), true);
    svc.create_wish(&seek_payload(json!({"answer": "now"})))
        .expect("declared");

    let asks = causal.plan_asks.lock().unwrap();
    assert_eq!(asks.len(), 1);
    assert_eq!(asks[0]["type"], json!("seek"));
    assert_eq!(asks[0]["seek"]["form"], json!("find_best"));
    assert!(asks[0]["seek"].get("region").is_some());

    let assignment = db
        .queries_for(SENDER_DB)
        .into_iter()
        .filter(|q| q.starts_with("INSERT INTO wish_assignments"))
        .count();
    assert_eq!(assignment, 1, "the sender's assignment is planted");

    let events: Vec<String> = events_inserted(&db)
        .into_iter()
        .filter(|q| q.contains("'planned'") || q.contains("'assigned'"))
        .collect();
    assert_eq!(events.len(), 2, "planned + assigned: {:?}", events);
}

#[test]
fn dead_sender_refuses_loudly_no_note_planted() {
    // the sender is not in the registry: unknown tender, no note.
    let db_only = service(false);
    let (svc, db, _) = (db_only.0, db_only.1, db_only.2);
    let causal = FakeCausal::new();
    causal.push_plan(Ok(json!({"status": "planned", "sender": "dead-one"})));
    let svc = WishService::with_parts(db.clone(), FakeWindmill::new(), causal, true);
    svc.create_wish(&seek_payload(json!({"answer": "now"})))
        .expect("declared");

    let assignments = db
        .queries()
        .into_iter()
        .filter(|q| q.starts_with("INSERT INTO wish_assignments"))
        .count();
    assert_eq!(assignments, 0);
    assert!(
        events_inserted(&db)
            .iter()
            .any(|q| q.contains("'plan_error'")),
        "the refusal is loud"
    );
}

#[test]
fn map_reads_it_refusal_carries_the_reading() {
    let (svc, db, _) = service(false);
    svc.create_wish(&seek_payload(json!({"answer": "now"})))
        .expect("declared");
    let causal = FakeCausal::new();
    causal.push_plan(Ok(json!({
        "status": "refused",
        "reason": "map_reads_it",
        "detail": {"reading": 0.83, "params": {"param_0": 35.2}},
    })));
    let svc = WishService::with_parts(db.clone(), FakeWindmill::new(), causal, true);
    svc.create_wish(&seek_payload(json!({"answer": "now"})))
        .expect("declared");

    let refused: Vec<String> = events_inserted(&db)
        .into_iter()
        .filter(|q| q.contains("'refused'"))
        .collect();
    assert_eq!(refused.len(), 1);
    assert!(refused[0].contains("\"reason\":\"map_reads_it\""), "{}", refused[0]);
    assert!(refused[0].contains("\"reading\":0.83"), "{}", refused[0]);
    assert_eq!(
        db.queries()
            .into_iter()
            .filter(|q| q.starts_with("INSERT INTO wish_assignments"))
            .count(),
        0
    );
}

// ── type dispatch (§XVII) ────────────────────────────────────────────

#[test]
fn type_seek_routes_to_the_walk_door() {
    let (svc, _, _) = service(false);
    match svc.create_wish(&seek_payload(json!({}))) {
        Ok(_) => {}
        Err(e) => assert!(!e.contains("claims"), "{}", e),
    }
}

#[test]
fn missing_type_refuses_no_omission() {
    let (svc, _, _) = service(false);
    let err = svc
        .create_wish(&json!({"claims": [
            {"outcome": "room/objective_1", "band": {"lo": 0.0, "hi": 1.0}}
        ]}))
        .unwrap_err();
    assert!(err.contains("speaks its type"), "{}", err);
}

#[test]
fn walk_without_form_refuses_no_omission() {
    let (svc, _, _) = service(false);
    let err = svc.create_wish(&seek_payload(json!({"form": null}))).unwrap_err();
    assert!(err.contains("speaks its form"), "{}", err);
}

#[test]
fn unknown_type_refuses_named() {
    let (svc, _, _) = service(false);
    let err = svc
        .create_wish(&seek_payload(json!({"type": "guard"})))
        .unwrap_err();
    assert!(err.contains("unknown wish type"), "{}", err);
}

#[test]
fn form_on_a_hold_refuses_loudly() {
    let (svc, _, _) = service(false);
    let err = svc
        .create_wish(&json!({
            "type": "steer",
            "form": "find_best",
            "claims": [{"outcome": "room/objective_1", "band": {"lo": 0.0, "hi": 1.0}}],
            "terms": [],
        }))
        .unwrap_err();
    assert!(err.contains("form belongs to seek wishes"), "{}", err);
}
