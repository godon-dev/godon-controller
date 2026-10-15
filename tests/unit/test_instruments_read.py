"""Rung 9 — the instruments door's read side: the verdict read picks
up the worker's JSON stamp with a rollout-order guard (message-
matched, the worker's own beat-guard pattern); a torn payload reads
as absent, never as a failed read; the missing-table wordings still
carry no verdict."""

import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from controller.database import ArchiveDatabaseRepository


def _repo():
    repo = ArchiveDatabaseRepository.__new__(ArchiveDatabaseRepository)
    repo.base_config = {}
    return repo


def test_verdict_carries_the_stamp():
    repo = _repo()
    row = (False, 12.0, 10.0,
           '{"strategies_run": ["hold"], "wish": null, "seek_spend": null}')
    with patch('controller.database.execute_query', return_value=[row]):
        verdict = repo.read_state_verdict('db')
    assert verdict is not None
    assert verdict['instruments'] == {
        'strategies_run': ['hold'], 'wish': None, 'seek_spend': None}
    assert verdict['finished'] is False


def test_rollout_order_falls_back():
    """A row born before the column: the three verdict columns alone,
    instruments None — one retry, no ladder."""
    repo = _repo()
    calls = []

    def fake_eq(db_config, query, with_result=False):
        calls.append(query)
        if 'instruments' in query:
            raise Exception('column "instruments" does not exist')
        return [(False, 12.0, 10.0)]

    with patch('controller.database.execute_query', side_effect=fake_eq):
        verdict = repo.read_state_verdict('db')
    assert verdict is not None
    assert verdict['instruments'] is None
    assert len(calls) == 2


def test_missing_table_still_no_verdict():
    repo = _repo()
    with patch('controller.database.execute_query',
               side_effect=Exception(
                   'relation "systemtender_state" does not exist')):
        assert repo.read_state_verdict('db') is None


def test_torn_payload_reads_absent():
    repo = _repo()
    row = (False, 12.0, 10.0, '{torn json')
    with patch('controller.database.execute_query', return_value=[row]):
        verdict = repo.read_state_verdict('db')
    assert verdict is not None
    assert verdict['instruments'] is None
    assert verdict['finished'] is False
