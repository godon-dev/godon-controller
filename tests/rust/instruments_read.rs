//! Port of test_instruments_read.py — rung 9, the instruments door's
//! read side: the verdict read picks up the worker's JSON stamp with a
//! rollout-order guard (message-matched, the worker's own beat-guard
//! pattern); a torn payload reads as absent, never as a failed read;
//! the missing-table wordings still carry no verdict.

use crate::harness::{dberr, row, FakeDb};
use godon_controller::database::{ArchiveDatabaseRepository, DbConfig};
use serde_json::json;

fn repo(db: std::sync::Arc<FakeDb>) -> ArchiveDatabaseRepository {
    ArchiveDatabaseRepository::with_gateway(DbConfig::default(), db.clone())
}

#[test]
fn verdict_carries_the_stamp() {
    let db = FakeDb::with_handler(|_q| {
        Ok(Some(vec![row(&[
            Some("f"),
            Some("12.0"),
            Some("10.0"),
            Some(r#"{"strategies_run": ["hold"], "wish": null, "seek_spend": null}"#),
        ])]))
    });
    let verdict = repo(db).read_state_verdict("db").unwrap().expect("verdict");
    assert_eq!(
        verdict["instruments"],
        json!({"strategies_run": ["hold"], "wish": null, "seek_spend": null})
    );
    assert_eq!(verdict["finished"], json!(false));
}

#[test]
fn rollout_order_falls_back() {
    // A row born before the column: the three verdict columns alone,
    // instruments None — one retry, no ladder.
    let db = FakeDb::with_handler(|q| {
        if q.contains("instruments") {
            return Err(dberr("column \"instruments\" does not exist"));
        }
        Ok(Some(vec![row(&[Some("f"), Some("12.0"), Some("10.0")])]))
    });
    let r = repo(db.clone());
    let verdict = r.read_state_verdict("db").unwrap().expect("verdict");
    assert_eq!(verdict["instruments"], json!(null));
    assert_eq!(db.calls().len(), 2);
}

#[test]
fn missing_table_still_no_verdict() {
    let db = FakeDb::with_handler(|_q| Err(dberr("relation \"systemtender_state\" does not exist")));
    assert!(repo(db).read_state_verdict("db").unwrap().is_none());
}

#[test]
fn torn_payload_reads_absent() {
    let db = FakeDb::with_handler(|_q| {
        Ok(Some(vec![row(&[
            Some("f"),
            Some("12.0"),
            Some("10.0"),
            Some("{torn json"),
        ])]))
    });
    let verdict = repo(db).read_state_verdict("db").unwrap().expect("verdict");
    assert_eq!(verdict["instruments"], json!(null));
    assert_eq!(verdict["finished"], json!(false));
}
