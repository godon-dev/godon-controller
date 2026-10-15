"""Rung 6 at the controller's door (design §XIX) — the two
caller-stated declaration options, the answer-timing split, and the
derived unservable state.

Each test names what it pins:
- answer timing: 'now' asks causal in the same breath (the consult's
  answer rides the very response); 'later' — the sealed default —
  dispatches the ask as its own job and the door returns at 'declared'
- divisibility: 'joint' defaults on holds (a half-kept state is a
  lying state), 'alternatives' on walks (a seek's candidates by
  nature); both ride the plan-ask wire
- the dispatched job re-asks from stored terms — one ask discipline,
  two timings; a dispatch that cannot fly asks inline (never unasked)
- a standing refusal derives as the state 'unservable', never stored:
  the translation lives in the read queries themselves (positional
  readers are pinned by query text, not mock shape)
"""

import inspect
import json
import sys
import os
from unittest.mock import MagicMock, Mock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '../..'))

from controller.steerwish_create import main as create_main
from controller import wish_service
from controller.wish_service import WishService
from controller import database
from controller.steer_grammar import checked_declaration_options


def hold_payload(**over):
    body = {
        'type': 'steer',
        'claims': [{'outcome': 'chainend.shift',
                    'band': {'lo': -0.14, 'hi': -0.06, 'target': -0.10}}],
        'terms': [],
    }
    body.update(over)
    return body


def seek_payload(**over):
    body = {
        'type': 'seek',
        'form': 'find_best',
        'region': {'param_0': [0.0, 100.0, False]},
        'criterion': {'outcome': 'room/objective_1', 'direction': 'maximize'},
        'budget': 50,
        'candidates': 5,
    }
    body.update(over)
    return body


def _service(wish=None):
    svc = WishService.__new__(WishService)
    svc.repo = MagicMock()
    svc.archive_repo = MagicMock()
    svc.get_steerwish = MagicMock(
        return_value=wish or {'id': 'w1', 'state': 'declared'})
    return svc


class TestDeclarationOptions:
    def test_defaults_hold_later_joint(self):
        opts = checked_declaration_options({}, 'joint')
        assert opts == {'answer': 'later', 'divisibility': 'joint'}

    def test_defaults_walk_alternatives(self):
        opts = checked_declaration_options({}, 'alternatives')
        assert opts == {'answer': 'later', 'divisibility': 'alternatives'}

    def test_explicit_now_alternatives_admit(self):
        opts = checked_declaration_options(
            {'answer': 'now', 'divisibility': 'alternatives'}, 'joint')
        assert opts == {'answer': 'now', 'divisibility': 'alternatives'}

    def test_unknown_answer_refuses_named(self):
        with pytest.raises(Exception) as e:
            checked_declaration_options({'answer': 'soon'}, 'joint')
        assert 'now' in str(e.value) and 'later' in str(e.value)

    def test_unknown_divisibility_refuses_named(self):
        with pytest.raises(Exception) as e:
            checked_declaration_options({'divisibility': 'partial'}, 'joint')
        assert 'joint' in str(e.value) and 'alternatives' in str(e.value)


class TestAnswerTiming:
    def test_now_asks_in_the_same_breath(self):
        svc = _service()
        with patch.object(svc, '_ask_causal_to_plan') as ask, \
             patch.object(svc, '_ensure_registry'):
            svc.create_steerwish(hold_payload(answer='now'))
        assert ask.call_args.kwargs.get('divisibility') == 'joint'

    def test_later_dispatches_and_returns_declared(self):
        svc = _service()
        dispatched = {}
        fake_wmill = MagicMock()
        fake_wmill.run_script_by_path_async = \
            lambda path, args: dispatched.update({'path': path, 'args': args})
        with patch.object(svc, '_ask_causal_to_plan') as ask, \
             patch.object(svc, '_ensure_registry'), \
             patch.dict(sys.modules, {'wmill': fake_wmill}):
            svc.create_steerwish(hold_payload())  # answer omitted = later
        assert dispatched['path'] == 'f/controller/steerwish_plan'
        assert dispatched['args']['wish_id'] == svc.repo.insert_steerwish.call_args.kwargs['wish_id']
        ask.assert_not_called()

    def test_seek_later_dispatches_too(self):
        svc = _service()
        dispatched = {}
        fake_wmill = MagicMock()
        fake_wmill.run_script_by_path_async = \
            lambda path, args: dispatched.update({'path': path})
        with patch.object(svc, '_ask_causal_to_plan_seek') as ask, \
             patch.object(svc, '_ensure_registry'), \
             patch.dict(sys.modules, {'wmill': fake_wmill}):
            svc.create_steerwish(seek_payload())
        assert dispatched.get('path') == 'f/controller/steerwish_plan'
        ask.assert_not_called()

    def test_dispatch_failure_falls_back_inline_never_unasked(self):
        svc = _service()
        broken_wmill = MagicMock()
        broken_wmill.run_script_by_path_async = \
            Mock(side_effect=RuntimeError('no windmill here'))
        with patch.object(svc, '_ask_causal_to_plan') as ask, \
             patch.object(svc, '_ensure_registry'), \
             patch.dict(sys.modules, {'wmill': broken_wmill}):
            svc.create_steerwish(hold_payload())
        ask.assert_called_once()

    def test_row_carries_both_options(self):
        svc = _service()
        with patch.object(svc, '_ask_causal_to_plan'), \
             patch.object(svc, '_ensure_registry'):
            svc.create_steerwish(
                hold_payload(answer='now', divisibility='alternatives'))
        kwargs = svc.repo.insert_steerwish.call_args.kwargs
        assert kwargs['answer'] == 'now'
        assert kwargs['divisibility'] == 'alternatives'

    def test_seek_row_defaults_alternatives(self):
        svc = _service()
        with patch.object(svc, '_ask_causal_to_plan_seek'), \
             patch.object(svc, '_ensure_registry'):
            svc.create_steerwish(seek_payload())
        kwargs = svc.repo.insert_steerwish.call_args.kwargs
        assert kwargs['answer'] == 'later'
        assert kwargs['divisibility'] == 'alternatives'


class TestPlanWish:
    """The dispatched job's body: the ask re-built from stored terms."""

    def _row(self, regime='standing', seek=None, divisibility='joint'):
        return ['w1', 'chainend.shift', {'lo': -0.14, 'hi': -0.06}, None,
                2, regime, None, 'declared',
                [{'outcome': 'chainend.shift',
                  'band': {'lo': -0.14, 'hi': -0.06}}], [], seek,
                'later', divisibility]

    def test_plan_wish_asks_from_stored_terms(self):
        svc = _service()
        svc.repo.fetch_steerwish_by_id.return_value = self._row()
        with patch.object(svc, '_ask_causal_to_plan') as ask, \
             patch.object(svc, '_ensure_registry'):
            svc.plan_wish('w1')
        kwargs = ask.call_args.kwargs
        assert kwargs['divisibility'] == 'joint'
        assert kwargs['claims'][0]['outcome'] == 'chainend.shift'

    def test_plan_wish_seek_row_uses_the_walk_door(self):
        svc = _service()
        walk = seek_payload()
        svc.repo.fetch_steerwish_by_id.return_value = self._row(
            regime='seek', seek=walk, divisibility='alternatives')
        with patch.object(svc, '_ask_causal_to_plan_seek') as ask, \
             patch.object(svc, '_ensure_registry'):
            svc.plan_wish('w1')
        assert ask.call_args.args[1]['form'] == 'find_best'
        assert ask.call_args.kwargs['divisibility'] == 'alternatives'

    def test_plan_wish_unknown_id_is_none(self):
        svc = _service()
        svc.repo.fetch_steerwish_by_id.return_value = None
        with patch.object(svc, '_ensure_registry'):
            assert svc.plan_wish('nope') is None

    def test_divisibility_rides_the_wire(self):
        svc = _service()
        asked = {}

        def fake_urlopen(req, timeout=None):
            asked['body'] = json.loads(req.data.decode('utf-8'))
            resp = MagicMock()
            resp.read.return_value = json.dumps(
                {'status': 'refused', 'reason': 'unmeasured_outcome',
                 'detail': 'no curve'}).encode('utf-8')
            resp.__enter__ = lambda s: s
            resp.__exit__ = Mock(return_value=False)
            return resp

        with patch.object(svc, '_ensure_registry'), \
             patch('controller.wish_service.urllib.request.urlopen',
                   side_effect=fake_urlopen):
            svc._ask_causal_to_plan(
                'w1', claims=[{'outcome': 'o',
                               'band': {'lo': 0.0, 'hi': 1.0}}],
                terms=[], divisibility='joint')
        assert asked['body']['divisibility'] == 'joint'


class TestUnservableDerivation:
    """A standing refusal reads as 'unservable' — the translation is in
    the read queries (positional readers are pinned by query text)."""

    def test_by_id_query_translates_refused(self):
        src = inspect.getsource(
            database.MetadataDatabaseRepository.fetch_steerwish_by_id)
        assert "CASE WHEN e.event_type = 'refused' THEN 'unservable'" in src

    def test_list_query_translates_refused(self):
        src = inspect.getsource(
            database.MetadataDatabaseRepository.fetch_steerwishes_list)
        assert "CASE WHEN e.event_type = 'refused' THEN 'unservable'" in src

    def test_columns_answer_divisibility_append_after_seek(self):
        src = inspect.getsource(
            database.MetadataDatabaseRepository.fetch_steerwish_by_id)
        assert 'w.claims, w.terms, w.seek, w.answer, w.divisibility' in src

    def test_insert_carries_the_options(self):
        src = inspect.getsource(
            database.MetadataDatabaseRepository.insert_steerwish)
        assert 'answer VARCHAR(16)' in src or 'answer_sql' in src

    def test_format_wish_surfaces_the_options(self):
        svc = _service()
        row = self._row()
        wish = svc._format_wish(row, [])
        assert wish['answer'] == 'later'
        assert wish['divisibility'] == 'joint'

    def _row(self):
        return ['w1', 'chainend.shift', {'lo': -0.14, 'hi': -0.06}, None,
                2, 'standing', None, 'unservable',
                [{'outcome': 'chainend.shift',
                  'band': {'lo': -0.14, 'hi': -0.06}}], [], None,
                'later', 'joint']


class TestCreateDoorRung6:
    def test_malformed_answer_refuses_at_the_door(self):
        result = create_main(request_data=hold_payload(answer='eventually'))
        assert result['result'] == 'FAILURE'
        assert 'later' in result['error']

    def test_malformed_divisibility_refuses_at_the_door(self):
        result = create_main(request_data=hold_payload(divisibility='some'))
        assert result['result'] == 'FAILURE'
        assert 'alternatives' in result['error']
