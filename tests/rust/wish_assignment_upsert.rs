//! Port of test_wish_assignment_upsert.py — the wish assignment write
//! survives a re-plan: an amendment re-plants the same wish_id, so the
//! write must upsert - a bare INSERT duplicates the primary key and
//! the whole amendment lands in plan_error (found live Sep 29: the
//! holder's amendment on a serving wish hit wish_assignments_pkey on
//! the re-plan's assignment write).

use crate::harness::FakeDb;
use godon_controller::database::{ArchiveDatabaseRepository, DbConfig};

/// Read write_wish_assignment's queries without touching a database:
/// the gateway is a capture, the method is invoked, and the three
/// statements (create, alter, insert) come back in order.
fn read_assignment_queries() -> Vec<String> {
    let db = FakeDb::capture();
    let repo = ArchiveDatabaseRepository::with_gateway(DbConfig::default(), db);
    repo.write_wish_assignment("systemtender_test_1", "wish-abc", "receiver")
        .unwrap();
    db.queries()
}

#[test]
fn assignment_write_upserts_on_replan() {
    let queries = read_assignment_queries();
    let inserts: Vec<&String> = queries
        .iter()
        .filter(|q| q.starts_with("INSERT INTO wish_assignments"))
        .collect();
    assert_eq!(
        inserts.len(),
        1,
        "expected one insert, got {}",
        inserts.len()
    );
    assert!(
        inserts[0].contains("ON CONFLICT (wish_id) DO UPDATE"),
        "the assignment write must upsert: an amendment re-plants the same wish_id"
    );
    assert!(inserts[0].contains("assigned_tsz = EXCLUDED.assigned_tsz"));
    assert!(inserts[0].contains("role = EXCLUDED.role"));
}

#[test]
fn assignment_upsert_refreshes_role() {
    // A role flip (sender -> receiver on a re-plan) rides the upsert.
    let queries = read_assignment_queries();
    let insert = queries
        .iter()
        .find(|q| q.starts_with("INSERT INTO wish_assignments"))
        .expect("insert statement");
    assert!(
        insert.contains("role = EXCLUDED.role"),
        "the upsert must refresh the role, not keep the stale one"
    );
}
