"""The seek's return mirror — the declarer reads the receipt.

The worker banks its RETURN in the serving tender's own ledger; the
wish GET pulls it into the event trail once (idempotent), and a dead
tender reads as no return, never an error.
"""

import json
import sys
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '../..'))

from controller.wish_service import WishService

CREATED = datetime(2026, 10, 12, 19, 0, 0, tzinfo=timezone.utc)
SEEK_BODY = {
    'species': 'find_best',
    'region': {'param_0': [0.0, 100.0, False]},
    'criterion': {'outcome': 'r/objective_1', 'direction': 'maximize'},
    'budget': 50, 'candidates': 2,
}
RETURN_PAYLOAD = json.dumps({
    'verdict': 'RETURNED', 'stop_reason': 'converged', 'spend_count': 12,
    'finding': {'params': {'param_0': 62.5}, 'reading': 0.91, 'bars': 0.02},
})


def _row(seek=SEEK_BODY):
    return ['w1', 'r/objective_1', None, None, 50, 'seek', CREATED,
            'planned', None, None, seek]


def _service(events, returned=RETURN_PAYLOAD):
    svc = WishService.__new__(WishService)
    svc.repo = MagicMock()
    svc.archive_repo = MagicMock()
    svc.repo.get_wish_lifecycle.return_value = None
    svc.repo.fetch_steerwish_by_id.return_value = _row()
    svc.repo.fetch_steerwish_events.side_effect = lambda wid: list(events)
    svc.archive_repo.read_seek_return.return_value = returned
    patch.object(svc, '_ensure_registry', lambda: None).start()
    patch.object(svc, '_fold_in_book', lambda wid: 0).start()
    return svc


ASSIGNED = [('assigned', CREATED, json.dumps({'sender': 's1', 'db': 'systemtender_s1'}))]


def test_return_mirror_pulls_the_return_once():
    svc = _service(ASSIGNED)
    wish = svc.get_steerwish('w1')

    events = svc.repo.insert_steerwish_event.call_args_list
    assert len(events) == 1 and events[0].args[1] == 'returned'
    assert json.loads(events[0].args[2])['verdict'] == 'RETURNED'
    assert wish['seek'] == SEEK_BODY
    svc.archive_repo.read_seek_return.assert_called_once_with(
        'systemtender_s1', 'w1')

    # Idempotent: a later read carries the returned event already —
    # no second mirror write.
    with_return = ASSIGNED + [('returned', CREATED, RETURN_PAYLOAD)]
    svc2 = _service(with_return)
    svc2.get_steerwish('w1')
    svc2.repo.insert_steerwish_event.assert_not_called()
    svc2.archive_repo.read_seek_return.assert_not_called()


def test_dead_tender_reads_as_no_return():
    svc = _service(ASSIGNED, returned=None)
    wish = svc.get_steerwish('w1')
    assert wish is not None  # the read survives its tender's death
    svc.repo.insert_steerwish_event.assert_not_called()


def test_non_seek_wishes_never_touch_the_tender_db():
    svc = _service(ASSIGNED)
    svc.repo.fetch_steerwish_by_id.return_value = _row(seek=None)
    svc.get_steerwish('w1')
    svc.archive_repo.read_seek_return.assert_not_called()
