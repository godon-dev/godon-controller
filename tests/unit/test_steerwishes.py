#
# Copyright (c) 2019 Matthias Tafelmeier.
#
# This file is part of godon
#
# godon is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# godon is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this godon. If not, see <http://www.gnu.org/licenses/>.
#

import pytest
import sys
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock, patch

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from controller.steerwish_create import main as create_main
from controller.steerwish_get import main as get_main
from controller.steerwishes_get import main as list_main
from controller.steerwish_close import main as close_main
from controller import steerwish_service

WISH_ID = '550e8400-e29b-41d4-a716-446655440000'

CREATED_AT = datetime(2026, 9, 10, 10, 30, 0, tzinfo=timezone.utc)


def _wish_row(state='declared'):
    return (
        WISH_ID,
        'chainend.shift',
        {'lo': -0.14, 'hi': -0.06, 'target': -0.10},
        {'exclude': [], 'maxChange': 0.5},
        2,
        'standing',
        CREATED_AT,
        state,
    )


def _events(*kinds):
    base = datetime(2026, 9, 10, 10, 30, 0, tzinfo=timezone.utc)
    rows = []
    for i, kind in enumerate(kinds):
        detail = 'target outside measured range' if kind == 'refused' else None
        rows.append((kind, base.replace(minute=i), detail))
    return rows


class TestSteerwishCreateValidation:
    """Entry validation: malformed wishes never reach the registry."""

    def test_create_missing_request_data(self):
        result = create_main(request_data=None)
        assert result['result'] == 'FAILURE'
        assert 'outcome' in result['error'].lower()

    def test_create_missing_outcome(self):
        result = create_main(request_data={'band': {'lo': -0.14, 'hi': -0.06}})
        assert result['result'] == 'FAILURE'
        assert 'outcome' in result['error'].lower()

    def test_create_missing_band(self):
        result = create_main(request_data={'outcome': 'chainend.shift'})
        assert result['result'] == 'FAILURE'
        assert 'band' in result['error'].lower()

    def test_create_band_lo_not_below_hi(self):
        result = create_main(request_data={
            'outcome': 'chainend.shift',
            'band': {'lo': 0.1, 'hi': -0.1},
        })
        assert result['result'] == 'FAILURE'
        assert 'lo must be below hi' in result['error']

    def test_create_negative_budget(self):
        result = create_main(request_data={
            'outcome': 'chainend.shift',
            'band': {'lo': -0.14, 'hi': -0.06},
            'budget': -1,
        })
        assert result['result'] == 'FAILURE'
        assert 'budget' in result['error'].lower()

    def test_create_unknown_regime(self):
        result = create_main(request_data={
            'outcome': 'chainend.shift',
            'band': {'lo': -0.14, 'hi': -0.06},
            'regime': 'episode',
        })
        assert result['result'] == 'FAILURE'
        assert 'regime' in result['error'].lower()


class TestSteerwishCreation:
    """Creation stamps 'declared' and returns the full wish."""

    def test_create_success_stamps_declared(self):
        with patch('controller.steerwish_create.SteerwishService') as svc_cls:
            svc = svc_cls.return_value
            svc.create_steerwish.return_value = {
                'id': WISH_ID,
                'outcome': 'chainend.shift',
                'band': {'lo': -0.14, 'hi': -0.06, 'target': -0.10},
                'state': 'declared',
                'createdAt': CREATED_AT.isoformat(),
            }

            result = create_main(request_data={
                'outcome': 'chainend.shift',
                'band': {'lo': -0.14, 'hi': -0.06, 'target': -0.10},
            })

        assert result['id'] == WISH_ID
        assert result['state'] == 'declared'

    def test_create_validation_failure_maps_to_failure(self):
        with patch('controller.steerwish_create.SteerwishService') as svc_cls:
            svc = svc_cls.return_value
            svc.create_steerwish.side_effect = \
                steerwish_service.SteerwishValidationError('band lo must be below hi')

            result = create_main(request_data={
                'outcome': 'chainend.shift',
                'band': {'lo': 0.1, 'hi': -0.1},
            })

        assert result['result'] == 'FAILURE'
        assert 'lo must be below hi' in result['error']


class TestSteerwishGetAndList:
    def test_get_unknown_wish_maps_to_not_found(self):
        with patch('controller.steerwish_get.SteerwishService') as svc_cls:
            svc_cls.return_value.get_steerwish.return_value = None

            result = get_main(request_data={'wish_id': WISH_ID})

        assert result['result'] == 'FAILURE'
        assert 'not found' in result['error']

    def test_get_returns_full_wish(self):
        with patch('controller.steerwish_get.SteerwishService') as svc_cls:
            wish = {
                'id': WISH_ID, 'outcome': 'chainend.shift',
                'band': {'lo': -0.14, 'hi': -0.06}, 'state': 'declared',
                'createdAt': CREATED_AT.isoformat(),
                'events': [{'type': 'declared', 'at': CREATED_AT.isoformat(),
                            'detail': None}],
            }
            svc_cls.return_value.get_steerwish.return_value = wish

            result = get_main(request_data={'wish_id': WISH_ID})

        assert result == wish
        assert result['events'][0]['type'] == 'declared'

    def test_list_passes_summaries_through(self):
        with patch('controller.steerwishes_get.SteerwishService') as svc_cls:
            summaries = [{
                'id': WISH_ID, 'outcome': 'chainend.shift',
                'state': 'planned', 'createdAt': CREATED_AT.isoformat(),
            }]
            svc_cls.return_value.list_steerwishes.return_value = summaries

            result = list_main(request_data=None)

        assert result == summaries


class TestSteerwishClose:
    def test_close_unknown_wish_maps_to_not_found(self):
        with patch('controller.steerwish_close.SteerwishService') as svc_cls:
            svc_cls.return_value.close_steerwish.return_value = None

            result = close_main(request_data={'wish_id': WISH_ID})

        assert result['result'] == 'FAILURE'
        assert 'not found' in result['error']

    def test_close_returns_closed_wish(self):
        with patch('controller.steerwish_close.SteerwishService') as svc_cls:
            svc_cls.return_value.close_steerwish.return_value = {
                'id': WISH_ID, 'state': 'closed',
            }

            result = close_main(request_data={'wish_id': WISH_ID})

        assert result['state'] == 'closed'


class TestSteerwishService:
    """Service logic against a stubbed registry (state derives from events)."""

    def _service_with_repo(self, wish_row, events):
        with patch('controller.steerwish_service.MetadataDatabaseRepository') as repo_cls:
            repo = repo_cls.return_value
            repo.fetch_steerwish_by_id.return_value = wish_row
            repo.fetch_steerwish_events.return_value = events
            service = steerwish_service.SteerwishService({'database': 'meta_data'})
        return service, repo

    def test_create_inserts_and_stamps_declared(self):
        service = steerwish_service.SteerwishService({'database': 'meta_data'})
        payload = {
            'outcome': '  chainend.shift  ',
            'band': {'lo': -0.14, 'hi': -0.06, 'target': -0.10},
            'limits': {'exclude': [], 'maxChange': 0.5},
            'budget': 2,
        }

        with patch.object(service.repo, 'fetch_steerwish_by_id',
                          return_value=_wish_row()), \
             patch.object(service.repo, 'fetch_steerwish_events',
                          return_value=_events('declared')), \
             patch.object(service.repo, 'create_steerwish_tables'), \
             patch.object(service.repo, 'insert_steerwish') as insert, \
             patch.object(service.repo, 'insert_steerwish_event') as event, \
             patch.object(service, '_ask_causal_to_plan') as ask:
            wish = service.create_steerwish(payload)

        args, kwargs = insert.call_args
        assert kwargs['outcome'] == 'chainend.shift', 'outcome must be trimmed'
        assert kwargs['regime'] == 'standing', 'omitted regime defaults to standing'
        event.assert_called_once()
        assert event.call_args[0][1] == 'declared'
        # the declare flow asks causal to plan, terms riding the request
        ask.assert_called_once()
        ask_args = ask.call_args[0]
        assert ask_args[0] == kwargs['wish_id'], 'the minted wish id rides the ask'
        assert ask_args[1] == 'chainend.shift'
        assert wish['state'] == 'declared'
        assert wish['budget'] == 2

    def test_create_planned_plants_assignment_row(self):
        import json as _json
        service = steerwish_service.SteerwishService(
            {'database': 'meta_data'}, {'database': 'archive_db'})
        payload = {
            'outcome': 'chainend.shift',
            'band': {'lo': -0.14, 'hi': -0.06, 'target': -0.10},
        }
        planned = {
            'status': 'planned',
            'moves': [{'sender': 'abc-def', 'param': 'p', 'setting': 1.0}],
            'predicted': {'value': -0.10, 'bars': 0.02},
            'range_used': {},
        }
        fake_resp = MagicMock()
        fake_resp.read.return_value = _json.dumps(planned).encode('utf-8')
        fake_resp.__enter__.return_value = fake_resp

        with patch.object(service.repo, 'fetch_steerwish_by_id',
                          return_value=_wish_row()), \
             patch.object(service.repo, 'fetch_steerwish_events',
                          return_value=_events('declared', 'planned', 'assigned')), \
             patch.object(service.repo, 'create_steerwish_tables'), \
             patch.object(service.repo, 'insert_steerwish'), \
             patch.object(service.repo, 'insert_steerwish_event') as event, \
             patch.object(service.archive_repo, 'write_wish_assignment') as write_a, \
             patch('urllib.request.urlopen', return_value=fake_resp):
            service.create_steerwish(payload)

        # the plan named the dial: the assignment row lands in that
        # tender's own archive DB, causal's book is the plan's home
        write_a.assert_called_once()
        assert write_a.call_args[0][0] == 'systemtender_abc_def'
        stamped = [c[0][1] for c in event.call_args_list]
        assert 'planned' in stamped and 'assigned' in stamped

    def test_create_refusal_is_recorded_not_raised(self):
        import json as _json
        service = steerwish_service.SteerwishService(
            {'database': 'meta_data'}, {'database': 'archive_db'})
        payload = {
            'outcome': 'chainend.shift',
            'band': {'lo': -0.14, 'hi': -0.06},
        }
        refused = {'status': 'refused',
                   'reason': 'unmeasured_path', 'detail': 'no curve'}
        fake_resp = MagicMock()
        fake_resp.read.return_value = _json.dumps(refused).encode('utf-8')
        fake_resp.__enter__.return_value = fake_resp

        with patch.object(service.repo, 'fetch_steerwish_by_id',
                          return_value=_wish_row()), \
             patch.object(service.repo, 'fetch_steerwish_events',
                          return_value=_events('declared', 'refused')), \
             patch.object(service.repo, 'create_steerwish_tables'), \
             patch.object(service.repo, 'insert_steerwish'), \
             patch.object(service.repo, 'insert_steerwish_event') as event, \
             patch('urllib.request.urlopen', return_value=fake_resp):
            wish = service.create_steerwish(payload)

        stamped = [c[0][1] for c in event.call_args_list]
        assert 'refused' in stamped, 'a refusal is a book entry, not an error'
        assert wish['state'] == 'declared'

    def test_state_is_latest_event_not_stored(self):
        service, _ = self._service_with_repo(_wish_row(state='landed'),
                                             _events('declared', 'acted', 'landed'))
        wish = service.get_steerwish(WISH_ID)
        assert wish['state'] == 'landed'
        assert [e['type'] for e in wish['events']] == ['declared', 'acted', 'landed']

    def test_close_appends_event_once(self):
        open_row = _wish_row(state='landed')
        closed_row = _wish_row(state='closed')
        with patch('controller.steerwish_service.MetadataDatabaseRepository') as repo_cls:
            repo = repo_cls.return_value
            repo.fetch_steerwish_by_id.side_effect = [open_row, closed_row]
            repo.fetch_steerwish_events.return_value = _events('declared', 'landed', 'closed')
            service = steerwish_service.SteerwishService({'database': 'meta_data'})

            wish = service.close_steerwish(WISH_ID)

        repo.insert_steerwish_event.assert_called_once()
        assert repo.insert_steerwish_event.call_args[0][1] == 'closed'
        assert wish['state'] == 'closed'

    def test_close_is_idempotent_on_closed_wish(self):
        closed_row = _wish_row(state='closed')
        service, repo = self._service_with_repo(closed_row, _events('declared', 'closed'))

        wish = service.close_steerwish(WISH_ID)

        repo.insert_steerwish_event.assert_not_called()
        assert wish['state'] == 'closed'

    def test_get_unknown_wish_is_none(self):
        service, repo = self._service_with_repo(None, [])
        assert service.get_steerwish(WISH_ID) is None


class TestLazyFoldIn:
    """Reading is asking: get/list mirror causal's book into the registry."""

    BOOK_PAGE = {
        'wish_id': WISH_ID,
        'status': 'missed',
        'instruction': 'remeasure',
        'events': [
            {'tsz': 1000.0, 'event': 'planned', 'detail': None},
            {'tsz': 2000.0, 'event': 'missed',
             'detail': {'median': -0.17, 'bar': 0.02, 'n': 5}},
            {'tsz': 2500.0, 'event': 'walk_opened',
             'detail': {'held': 0.62, 'separated': True}},
        ],
    }

    def test_get_mirrors_unseen_book_events(self):
        service, repo = self._service_with_repo(
            _wish_row(state='missed'), _events('declared'))
        with patch.object(service, '_fetch_causal_page',
                          return_value=self.BOOK_PAGE), \
                patch.object(repo, 'has_steerwish_event_at',
                             return_value=False) as seen, \
                patch.object(repo, 'insert_steerwish_event_at') as mirror:
            service.get_steerwish(WISH_ID)

        mirrored = [(c[0][1], c[0][3]) for c in mirror.call_args_list]
        assert ('planned', 1000.0) in mirrored
        assert ('missed', 2000.0) in mirrored
        assert ('walk_opened', 2500.0) in mirrored, 'the full story, not just state words'
        assert seen.call_count == 3

    def test_get_fold_in_dedupes_by_type_and_time(self):
        service, repo = self._service_with_repo(
            _wish_row(state='missed'), _events('declared'))

        def already_seen(wid, event, tsz):
            return event == 'missed'

        with patch.object(service, '_fetch_causal_page',
                          return_value=self.BOOK_PAGE), \
                patch.object(repo, 'has_steerwish_event_at',
                             side_effect=already_seen), \
                patch.object(repo, 'insert_steerwish_event_at') as mirror:
            service.get_steerwish(WISH_ID)

        mirrored_types = [c[0][1] for c in mirror.call_args_list]
        assert mirrored_types == ['planned', 'walk_opened'], 'seen events stay single'

    def test_get_is_silent_when_the_book_is_unreachable(self):
        service, repo = self._service_with_repo(
            _wish_row(), _events('declared'))
        with patch.object(service, '_fetch_causal_page', return_value=None), \
                patch.object(repo, 'insert_steerwish_event_at') as mirror:
            wish = service.get_steerwish(WISH_ID)

        mirror.assert_not_called()
        assert wish['state'] == 'declared', 'stale mirror, no drama'

    def test_list_folds_only_open_wishes(self):
        service = steerwish_service.SteerwishService({'database': 'meta_data'})
        rows = [
            ('w-open', 'chainend.shift', 'serving', CREATED_AT),
            ('w-closed', 'chainend.shift', 'closed', CREATED_AT),
        ]
        with patch.object(service.repo, 'fetch_steerwishes_list',
                          return_value=rows), \
                patch.object(service, '_fold_in_book') as fold:
            service.list_steerwishes()

        folded = [c[0][0] for c in fold.call_args_list]
        assert folded == ['w-open'], 'closed wishes never re-ask'


class TestBudgetRidesTheAsk:
    """The declared round allowance reaches causal — it enforces the
    outer total, the controller only declares it."""

    def test_create_sends_budget_in_plan_ask(self):
        import json as _json
        service = steerwish_service.SteerwishService(
            {'database': 'meta_data'}, {'database': 'archive_db'})
        payload = {
            'outcome': 'chainend.shift',
            'band': {'lo': -0.14, 'hi': -0.06, 'target': -0.10},
            'budget': 2,
        }
        planned = {
            'status': 'planned',
            'moves': [{'sender': 'abc-def', 'param': 'p', 'setting': 1.0}],
            'predicted': {'value': -0.10, 'bars': 0.02},
            'range_used': {},
        }
        fake_resp = MagicMock()
        fake_resp.read.return_value = _json.dumps(planned).encode('utf-8')
        fake_resp.__enter__.return_value = fake_resp

        with patch.object(service.repo, 'fetch_steerwish_by_id',
                          return_value=_wish_row()), \
                patch.object(service.repo, 'fetch_steerwish_events',
                             return_value=_events('declared', 'planned', 'assigned')), \
                patch.object(service.repo, 'create_steerwish_tables'), \
                patch.object(service.repo, 'insert_steerwish'), \
                patch.object(service.repo, 'insert_steerwish_event'), \
                patch.object(service.archive_repo, 'write_wish_assignment'), \
                patch('urllib.request.urlopen', return_value=fake_resp) as urlopen:
            service.create_steerwish(payload)

        body = _json.loads(urlopen.call_args[0][0].data.decode('utf-8'))
        assert body['budget'] == 2, 'the allowance rides the ask'

    def test_create_without_budget_omits_the_field(self):
        import json as _json
        service = steerwish_service.SteerwishService(
            {'database': 'meta_data'}, {'database': 'archive_db'})
        payload = {'outcome': 'chainend.shift',
                   'band': {'lo': -0.14, 'hi': -0.06}}
        planned = {'status': 'refused', 'reason': 'unmeasured_path',
                   'detail': 'no curve'}
        fake_resp = MagicMock()
        fake_resp.read.return_value = _json.dumps(planned).encode('utf-8')
        fake_resp.__enter__.return_value = fake_resp

        with patch.object(service.repo, 'fetch_steerwish_by_id',
                          return_value=_wish_row()), \
                patch.object(service.repo, 'fetch_steerwish_events',
                             return_value=_events('declared', 'refused')), \
                patch.object(service.repo, 'create_steerwish_tables'), \
                patch.object(service.repo, 'insert_steerwish'), \
                patch.object(service.repo, 'insert_steerwish_event'), \
                patch('urllib.request.urlopen', return_value=fake_resp) as urlopen:
            service.create_steerwish(payload)

        body = _json.loads(urlopen.call_args[0][0].data.decode('utf-8'))
        assert 'budget' not in body, 'standing wishes carry no cap'
