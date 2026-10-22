//! The acceptance door for hold wishes (the holder's spec, 2026-10-21):
//! a wish is only accepted if it has living tenders that tend the
//! band - otherwise rejected. Every tender the plan hands work to
//! (the moves' senders, the path's receiver) is checked alive before
//! anything is planted; a refusal names the dead tenders and what IS
//! assignable, and purges the book card. The zero-move shape is the
//! live receipt: wish a6638bee planned with moves [], nothing planted,
//! the judge stamping "landed" on the walk's ambient stream.

use crate::harness::{row, FakeCausal, FakeDb, FakeWindmill};
use godon_controller::wish_service::WishService;
use serde_json::{json, Value};

const SENDER: &str = "tsend";
const RECEIVER: &str = "trx";
const OTHER: &str = "tother";
const CREATED: &str = "2026-10-21 19:00:00+00";

fn db_name(id: &str) -> String {
    format!("systemtender_{}", id)
}

/// The service over one fake db. `registry` names the tenders the
/// registry knows (each with a young heartbeat); anything else is an
/// unknown tender - dead at the door.
fn service(registry: &[&str]) -> (WishService, std::sync::Arc<FakeDb>) {
    let known: Vec<String> = registry.iter().map(|s| s.to_string()).collect();
    let db = FakeDb::with_handler(move |q| {
        if q.contains("INSERT INTO") {
            return Ok(None);
        }
        if q.contains("EXTRACT(EPOCH") {
            // every registry member beats young (5s against a 30s window)
            return Ok(Some(vec![row(&[Some("5.0"), Some("10.0")])]));
        }
        if let Some(pos) = q.find("WHERE id = '") {
            let rest = &q[pos + "WHERE id = '".len()..];
            let id = &rest[..rest.find("';").unwrap_or(rest.len())];
            if known.iter().any(|k| k == id) {
                return Ok(Some(vec![row(&[Some(id), Some("tender"), None, Some("{}")])]));
            }
            return Ok(Some(vec![]));
        }
        if q.contains("FROM systemtender_meta_data") {
            return Ok(Some(
                known
                    .iter()
                    .map(|id| row(&[Some(id), Some("tender"), Some(CREATED)]))
                    .collect(),
            ));
        }
        if q.contains("FROM steerwishes") {
            return Ok(Some(vec![row(&[
                Some("w1"),
                Some("trx/objective_0"),
                None,
                None,
                None,
                Some("standing"),
                Some(CREATED),
                Some("declared"),
                Some(r#"[{"outcome":"trx/objective_0","band":{"lo":0.83,"hi":0.87}}]"#),
                Some("[]"),
                None,
                Some("now"),
                Some("all"),
            ])]));
        }
        if q.contains("FROM steerwish_events") {
            return Ok(Some(vec![]));
        }
        if q.contains("FROM steerwish_lifecycle") {
            return Ok(Some(vec![]));
        }
        Ok(None)
    });
    let svc = WishService::with_parts(db.clone(), FakeWindmill::new(), FakeCausal::new(), true);
    (svc, db)
}

fn service_with_causal(
    registry: &[&str],
    plan: Value,
) -> (
    WishService,
    std::sync::Arc<FakeDb>,
    std::sync::Arc<FakeCausal>,
) {
    let (_, db) = service(registry);
    let causal = FakeCausal::new();
    causal.push_plan(Ok(plan));
    let svc = WishService::with_parts(db.clone(), FakeWindmill::new(), causal.clone(), true);
    (svc, db, causal)
}

fn hold_payload() -> Value {
    json!({
        "type": "steer",
        "claims": [{"outcome": "trx/objective_0", "band": {"lo": 0.83, "hi": 0.87}}],
        "answer": "now",
    })
}

fn events_of(db: &FakeDb, kind: &str) -> Vec<String> {
    db.queries()
        .into_iter()
        .filter(|q| {
            q.trim_start().starts_with("INSERT INTO steerwish_events") && q.contains(kind)
        })
        .collect()
}

fn assignment_rows(db: &FakeDb, tender: &str) -> usize {
    db.queries_for(&db_name(tender))
        .into_iter()
        .filter(|q| q.starts_with("INSERT INTO wish_assignments"))
        .count()
}

// ── the zero-move shape: the live receipt's cure ────────────────────

#[test]
fn zero_move_plan_plants_the_receiver_row() {
    // moves [], path [trx]: the receiver alone tends - its stillness
    // and published readings are the whole service. Live receiver:
    // accepted, the receiver's row planted, no refusal.
    let (svc, db, _) = service_with_causal(
        &[SENDER, RECEIVER, OTHER],
        json!({"status": "planned", "moves": [], "hold": true, "receivers": [RECEIVER]}),
    );
    svc.create_wish(&hold_payload()).expect("declared");

    assert_eq!(
        assignment_rows(&db, RECEIVER),
        1,
        "the zero-move hold plants the receiver's row"
    );
    assert_eq!(
        assignment_rows(&db, SENDER),
        0,
        "no sender is named - nothing planted for one"
    );
    assert_eq!(events_of(&db, "'refused'").len(), 0);
    let assigned = events_of(&db, "'assigned'");
    assert_eq!(assigned.len(), 1, "one assigned event: {:?}", assigned);
    assert!(
        assigned[0].contains(db_name(RECEIVER).as_str()),
        "the event names the receiver's db: {}",
        assigned[0]
    );
}

#[test]
fn zero_move_plan_with_dead_receiver_refuses_naming_the_assignable() {
    // the receiver is not in the registry: no living tender will take
    // the tending - refused, reason no_tender, the dead tender named,
    // the live tenders named as assignable, the book card purged.
    let (svc, db, causal) = service_with_causal(
        &[SENDER, OTHER],
        json!({"status": "planned", "moves": [], "hold": true, "receivers": [RECEIVER]}),
    );
    svc.create_wish(&hold_payload()).expect("declared");

    assert_eq!(assignment_rows(&db, RECEIVER), 0, "nothing planted");
    let refused = events_of(&db, "'refused'");
    assert_eq!(refused.len(), 1, "the door refuses: {:?}", refused);
    assert!(refused[0].contains("no_tender"), "{}", refused[0]);
    assert!(refused[0].contains(RECEIVER), "dead named: {}", refused[0]);
    assert!(
        refused[0].contains(SENDER) && refused[0].contains(OTHER),
        "assignable names the live tenders: {}",
        refused[0]
    );
    assert!(!refused[0].contains(RECEIVER.to_string().as_str())
        || refused[0].contains("dead"),
        "the receiver appears as dead, not assignable");
    assert_eq!(
        causal.book_purges.lock().unwrap().len(),
        1,
        "the book card is purged"
    );
    assert_eq!(events_of(&db, "'assigned'").len(), 0);
}

#[test]
fn plan_naming_no_tender_at_all_refuses() {
    // moves [], path []: the plan hands work to nobody
    let (svc, db, causal) = service_with_causal(
        &[SENDER],
        json!({"status": "planned", "moves": [], "hold": true}),
    );
    svc.create_wish(&hold_payload()).expect("declared");

    let refused = events_of(&db, "'refused'");
    assert_eq!(refused.len(), 1);
    assert!(
        refused[0].contains("names no tender"),
        "the refusal says what is wrong: {}",
        refused[0]
    );
    assert_eq!(causal.book_purges.lock().unwrap().len(), 1);
}

// ── move plans: per-sender rows plus the receiver's ─────────────────

#[test]
fn move_plan_plants_sender_and_receiver_rows() {
    let (svc, db, _) = service_with_causal(
        &[SENDER, RECEIVER],
        json!({
            "status": "planned",
            "moves": [{"param": "param_1", "setting": 42.0, "sender": SENDER}],
            "path": [SENDER, RECEIVER],
        }),
    );
    svc.create_wish(&hold_payload()).expect("declared");

    assert_eq!(assignment_rows(&db, SENDER), 1, "the sender holds");
    assert_eq!(assignment_rows(&db, RECEIVER), 1, "the receiver parks");
    assert_eq!(events_of(&db, "'assigned'").len(), 2);
    assert_eq!(events_of(&db, "'refused'").len(), 0);
}

#[test]
fn dead_sender_on_move_plan_refuses_nothing_planted() {
    let (svc, db, causal) = service_with_causal(
        &[RECEIVER],
        json!({
            "status": "planned",
            "moves": [{"param": "param_1", "setting": 42.0, "sender": SENDER}],
            "path": [SENDER, RECEIVER],
        }),
    );
    svc.create_wish(&hold_payload()).expect("declared");

    assert_eq!(assignment_rows(&db, SENDER), 0);
    assert_eq!(
        assignment_rows(&db, RECEIVER),
        0,
        "a half-tendable wish plants nothing - the door refuses whole"
    );
    let refused = events_of(&db, "'refused'");
    assert_eq!(refused.len(), 1);
    assert!(refused[0].contains(SENDER), "{}", refused[0]);
    assert!(
        refused[0].contains(RECEIVER) && refused[0].contains("assignable"),
        "the live receiver named as assignable: {}",
        refused[0]
    );
    assert_eq!(causal.book_purges.lock().unwrap().len(), 1);
}

#[test]
fn joint_plan_plants_every_distinct_sender() {
    // python parity restored: one row per distinct sender, not
    // moves[0] only
    let (svc, db, _) = service_with_causal(
        &[SENDER, OTHER, RECEIVER],
        json!({
            "status": "planned",
            "moves": [
                {"param": "param_1", "setting": 42.0, "sender": SENDER},
                {"param": "param_2", "setting": 7.0, "sender": OTHER},
                {"param": "param_3", "setting": 9.0, "sender": SENDER},
            ],
            "path": [SENDER, RECEIVER],
        }),
    );
    svc.create_wish(&hold_payload()).expect("declared");

    assert_eq!(assignment_rows(&db, SENDER), 1, "one row, not two");
    assert_eq!(assignment_rows(&db, OTHER), 1, "the second sender too");
    assert_eq!(assignment_rows(&db, RECEIVER), 1);
}
