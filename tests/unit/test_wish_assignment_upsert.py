"""The wish assignment write survives a re-plan: an amendment re-plants
the same wish_id, so the write must upsert - a bare INSERT duplicates
the primary key and the whole amendment lands in plan_error (found live
Sep 29: the holder's amendment on a serving wish hit
wish_assignments_pkey on the re-plan's assignment write)."""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from controller.database import ArchiveDatabaseRepository


def _read_assignment_queries():
    """Read write_wish_assignment's queries without touching a database:
    execute_query is swapped for a capture, the method is invoked, and
    the three statements (create, alter, insert) come back in order."""
    captured = []

    def capture(db_config, query):
        captured.append(query)

    import controller.database as db_mod
    original = db_mod.execute_query
    db_mod.execute_query = capture
    try:
        repo = ArchiveDatabaseRepository({'database': 'test'})
        repo.write_wish_assignment("systemtender_test_1", "wish-abc", role="receiver")
    finally:
        db_mod.execute_query = original
    return captured


def test_assignment_write_upserts_on_replan():
    queries = _read_assignment_queries()
    insert = [q for q in queries if q.startswith("INSERT INTO wish_assignments")]
    assert len(insert) == 1, f"expected one insert, got {len(insert)}"
    assert "ON CONFLICT (wish_id) DO UPDATE" in insert[0], (
        "the assignment write must upsert: an amendment re-plants the same wish_id")
    assert "assigned_tsz = EXCLUDED.assigned_tsz" in insert[0]
    assert "role = EXCLUDED.role" in insert[0]
    print("  PASS")


def test_assignment_upsert_refreshes_role():
    """A role flip (sender -> receiver on a re-plan) rides the upsert."""
    queries = _read_assignment_queries()
    insert = next(q for q in queries if q.startswith("INSERT INTO wish_assignments"))
    assert "role = EXCLUDED.role" in insert, (
        "the upsert must refresh the role, not keep the stale one")
    print("  PASS")


if __name__ == "__main__":
    test_assignment_write_upserts_on_replan()
    test_assignment_upsert_refreshes_role()
