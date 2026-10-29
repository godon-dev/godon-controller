//! Ported from tests/unit/test_instruments_read.py - the
//! instruments door's read side: the verdict read picks up the
//! worker's JSON stamp with a rollout-order guard (message-matched,
//! the worker's own beat-guard pattern); a torn payload reads as
//! absent, never as a failed read; the missing-table wordings still
//! carry no verdict.

use crate::support::{srow, FakeDb};
use godon_controller::database::{ArchiveDatabaseRepository, DbConfig};
use serde_json::json;
use std::sync::Arc;

fn repo_with(db: Arc<FakeDb>) -> ArchiveDatabaseRepository {
    ArchiveDatabaseRepository::with_gateway(DbConfig::archive_from_env(), db)
}

#[test]
fn verdict_carries_the_stamp() {
    let db = Arc::new(FakeDb::rows(vec![srow(&[
        "f",
        "12.0",
        "10.0",
        r#"{"strategies_run": ["hold"], "wish": null, "seek_spend": null}"#,
    ])]));
    let repo = repo_with(db);
    let verdict = repo.read_state_verdict("db").unwrap().unwrap();
    assert_eq!(
        verdict["instruments"],
        json!({"strategies_run": ["hold"], "wish": null, "seek_spend": null})
    );
    assert_eq!(verdict["finished"], json!(false));
}

#[test]
fn rollout_order_falls_back() {
    // A row born before the column: the three verdict columns alone,
    // instruments None - one retry, no ladder.
    let db = Arc::new(FakeDb::from_fn(|_db, query| {
        if query.contains("instruments") {
            return Err(godon_controller::database::DbError(
                r#"column "instruments" does not exist"#.to_string(),
            ));
        }
        Ok(Some(vec![srow(&["f", "12.0", "10.0"])]))
    }));
    let repo = repo_with(db.clone());
    let verdict = repo.read_state_verdict("db").unwrap().unwrap();
    assert_eq!(verdict["instruments"], json!(null));
    assert_eq!(db.queries().len(), 2);
}

#[test]
fn missing_table_still_no_verdict() {
    let db = Arc::new(FakeDb::err(
        r#"relation "systemtender_state" does not exist"#,
    ));
    let repo = repo_with(db);
    assert!(repo.read_state_verdict("db").unwrap().is_none());
}

#[test]
fn torn_payload_reads_absent() {
    let db = Arc::new(FakeDb::rows(vec![srow(&[
        "f",
        "12.0",
        "10.0",
        "{torn json",
    ])]));
    let repo = repo_with(db);
    let verdict = repo.read_state_verdict("db").unwrap().unwrap();
    assert_eq!(verdict["instruments"], json!(null));
    assert_eq!(verdict["finished"], json!(false));
}
