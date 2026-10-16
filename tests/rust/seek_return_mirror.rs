//! Port of test_seek_return_mirror.py — the seek's return mirror, the
//! declarer reads the receipt.
//!
//! The worker banks its RETURN in the serving tender's own ledger; the
//! wish GET pulls it into the event trail once (idempotent), and a
//! dead tender reads as no return, never an error.

use crate::harness::{row, FakeCausal, FakeDb, FakeWindmill};
use godon_controller::wish_service::WishService;
use serde_json::{json, Value};
use std::sync::Arc;

const CREATED: &str = "2026-10-12 19:00:00+00";

fn seek_body() -> Value {
    json!({
        "form": "find_best",
        "region": {"param_0": [0.0, 100.0, false]},
        "criterion": {"outcome": "r/objective_1", "direction": "maximize"},
        "budget": 50, "candidates": 2,
    })
}

fn return_payload() -> String {
    r#"{"verdict": "RETURNED", "stop_reason": "converged", "spend_count": 12, "finding": {"params": {"param_0": 62.5}, "reading": 0.91, "bars": 0.02}}"#.to_string()
}

/// The wish row: fetch_wish_by_id's 13 columns; index 10 is the seek
/// body, 11 answer, 12 standard.
fn wish_row(seek: Option<&str>) -> godon_controller::database::TextRow {
    row(&[
        Some("w1"),
        Some("r/objective_1"),
        None,
        None,
        Some("50"),
        Some("seek"),
        Some(CREATED),
        Some("planned"),
        None,
        None,
        seek,
        Some("later"),
        Some("best"),
    ])
}

fn assigned_event() -> godon_controller::database::TextRow {
    row(&[
        Some("assigned"),
        Some(CREATED),
        Some(r#"{"sender": "s1", "db": "systemtender_s1"}"#),
    ])
}

/// The service over one fake db: events rows served for the events
/// read, the ledger answer served for the seek_return read.
fn service(
    events: Vec<godon_controller::database::TextRow>,
    ledger: Option<String>,
    seek: Option<String>,
) -> (WishService, Arc<FakeDb>) {
    let db = FakeDb::with_handler(move |q| {
        if q.contains("INSERT INTO steerwish_events") {
            return Ok(None);
        }
        if q.contains("FROM steerwish_events") {
            return Ok(Some(events.clone()));
        }
        if q.contains("FROM steerwishes") {
            return Ok(Some(vec![wish_row(seek.as_deref())]));
        }
        if q.contains("FROM steerwish_lifecycle") {
            return Ok(Some(vec![]));
        }
        if q.contains("FROM ledger_state") {
            return Ok(match &ledger {
                Some(p) => Some(vec![row(&[Some(p.as_str())])]),
                None => Some(vec![]),
            });
        }
        Ok(None)
    });
    let svc = WishService::with_parts(db.clone(), FakeWindmill::new(), FakeCausal::new(), true);
    (svc, db)
}

fn returned_inserts(db: &FakeDb) -> Vec<String> {
    db.queries()
        .into_iter()
        .filter(|q| q.starts_with("INSERT INTO steerwish_events") && q.contains("'returned'"))
        .collect()
}

fn ledger_reads(db: &FakeDb) -> Vec<(String, String)> {
    db.calls()
        .into_iter()
        .filter(|(_, q)| q.contains("FROM ledger_state"))
        .collect()
}

#[test]
fn return_mirror_pulls_the_return_once() {
    let (svc, db) = service(vec![assigned_event()], Some(return_payload()), Some(serde_json::to_string(&seek_body()).unwrap()));
    let wish = svc.get_wish("w1").unwrap().expect("wish");

    let inserts = returned_inserts(&db);
    assert_eq!(inserts.len(), 1, "one mirror write, got {}", inserts.len());
    assert!(inserts[0].contains(&return_payload()));
    let parsed: Value = serde_json::from_str(&return_payload()).unwrap();
    assert_eq!(parsed["verdict"], json!("RETURNED"));
    assert_eq!(wish["seek"], seek_body());

    let ledger = ledger_reads(&db);
    assert_eq!(ledger.len(), 1, "the ledger read happens once");
    assert_eq!(ledger[0].0, "systemtender_s1");
    assert!(ledger[0].1.contains("seek_return::w1"));

    // Idempotent: a later read carries the returned event already —
    // no second mirror write.
    let (svc2, db2) = service(
        vec![assigned_event(), row(&[Some("returned"), Some(CREATED), Some(&return_payload())])],
        Some(return_payload()),
        Some(serde_json::to_string(&seek_body()).unwrap()),
    );
    svc2.get_wish("w1").unwrap().expect("wish");
    assert!(returned_inserts(&db2).is_empty());
    assert!(ledger_reads(&db2).is_empty());
}

#[test]
fn dead_tender_reads_as_no_return() {
    let (svc, db) = service(vec![assigned_event()], None, Some(serde_json::to_string(&seek_body()).unwrap()));
    let wish = svc.get_wish("w1").unwrap();
    assert!(wish.is_some(), "the read survives its tender's death");
    assert!(returned_inserts(&db).is_empty());
}

#[test]
fn non_seek_wishes_never_touch_the_tender_db() {
    let (svc, db) = service(vec![assigned_event()], Some(return_payload()), None);
    svc.get_wish("w1").unwrap().expect("wish");
    assert!(ledger_reads(&db).is_empty());
}
