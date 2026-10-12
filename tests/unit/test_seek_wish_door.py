"""The seek wish at the controller's door — validation, declaration,
the ask, and the export-before-drop rider.

Each test names the ruling or rider it pins:
- an impossible wish never starts (candidates over budget refuses HERE,
  before anything plans on it)
- cover_now admits with empty search fields and REFUSES a budget
  (it walks on standing duty)
- the registry row carries regime 'seek' and the body in its own
  column — claims keep their pinned meaning
- the ask rides the pinned wire (species 'seek', body under 'seek');
  a planned answer plants the sender's assignment through the same
  alive-door holds use; a refusal lands as an event carrying
  reason+detail verbatim — map_reads_it's reading reaches the
  declarer inside the refusal
- export-before-drop: nothing drops unexported; the snapshot lands in
  a table the drop cannot touch
"""

import json
import sys
import os
import pytest
from unittest.mock import MagicMock, Mock, patch

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '../..'))

from controller.steerwish_create import main as create_main
from controller import steerwish_service
from controller.steerwish_service import SteerwishService, _checked_seek_body
from controller import database


def seek_payload(**over):
    body = {
        'species': 'find_best',
        'region': {'param_0': [0.0, 100.0, False]},
        'criterion': {'outcome': 'room/objective_1', 'direction': 'maximize'},
        'budget': 50,
        'candidates': 5,
    }
    body.update(over)
    return {'species': 'seek', 'seek': body}


class TestSeekValidationDoor:
    def test_candidates_over_budget_never_starts(self):
        result = create_main(request_data=seek_payload(candidates=51, budget=50))
        assert result['result'] == 'FAILURE'
        assert 'exceeds the budget' in result['error']

    def test_find_best_without_criterion_refuses(self):
        result = create_main(request_data=seek_payload(criterion=None))
        assert result['result'] == 'FAILURE'
        assert 'criterion' in result['error']

    def test_unknown_direction_refuses(self):
        result = create_main(request_data=seek_payload(
            criterion={'outcome': 'x', 'direction': 'toward'}))
        assert result['result'] == 'FAILURE'
        assert 'direction' in result['error']

    def test_find_best_without_budget_refuses(self):
        result = create_main(request_data=seek_payload(budget=None))
        assert result['result'] == 'FAILURE'
        assert 'budget' in result['error'].lower()

    def test_empty_region_refuses(self):
        result = create_main(request_data=seek_payload(region={}))
        assert result['result'] == 'FAILURE'
        assert 'region' in result['error']

    def test_inverted_range_refuses_named(self):
        result = create_main(request_data=seek_payload(
            region={'param_0': [100.0, 0.0, False]}))
        assert result['result'] == 'FAILURE'
        assert 'param_0' in result['error']

    def test_cover_now_refuses_a_budget(self):
        result = create_main(request_data=seek_payload(
            species='cover_now', criterion=None, budget=10))
        assert result['result'] == 'FAILURE'
        assert 'no budget' in result['error']

    def test_cover_now_admits_without_search_fields(self):
        body = _checked_seek_body({
            'species': 'cover_now',
            'region': {'param_0': [0.0, 100.0, False]},
        })
        assert body['criterion'] is None
        assert body['candidates'] == 0

    def test_region_normalizes_shapes(self):
        body = _checked_seek_body({
            'species': 'find_best',
            'region': {'n': [1, 8, True], 'f': (0.0, 10.0, False)},
            'criterion': {'outcome': 'o', 'direction': 'min'},
            'budget': 10,
        })
        assert body['region']['n'] == [1, 8, True]
        assert body['region']['f'] == [0.0, 10.0, False]

    def test_estate_direction_words_admit(self):
        for word in ('maximize', 'max', 'minimize', 'min'):
            body = _checked_seek_body({
                'species': 'find_best',
                'region': {'param_0': [0.0, 1.0, False]},
                'criterion': {'outcome': 'o', 'direction': word},
                'budget': 10,
            })
            assert body['criterion']['direction'] == word


def _service():
    svc = SteerwishService.__new__(SteerwishService)
    svc.repo = MagicMock()
    svc.archive_repo = MagicMock()
    svc.get_steerwish = MagicMock(return_value={'id': 'w1', 'state': 'declared'})
    return svc


def _causal_answer(status='planned', **body):
    payload = {'status': status}
    payload.update(body)
    resp = MagicMock()
    resp.read.return_value = json.dumps(payload).encode('utf-8')
    resp.__enter__ = lambda s: s
    resp.__exit__ = Mock(return_value=False)
    return resp


class TestSeekDeclaration:
    def test_declared_row_carries_the_body_in_its_own_column(self):
        svc = _service()
        with patch.object(svc, '_ask_causal_to_plan_seek'), \
             patch.object(svc, '_ensure_registry'):
            svc._create_seekwish(seek_payload())
        kwargs = svc.repo.insert_steerwish.call_args.kwargs
        assert kwargs['regime'] == 'seek'
        assert kwargs['outcome'] == 'room/objective_1'
        assert kwargs['claims'] is None and kwargs['terms'] is None
        assert kwargs['seek']['species'] == 'find_best'
        assert kwargs['budget'] == 50

    def test_planned_answer_plants_the_senders_assignment(self):
        svc = _service()
        svc._sender_alive = MagicMock(return_value=(True, 'beat'))
        asked = {}

        def fake_urlopen(req, timeout=None):
            asked['url'] = req.full_url
            asked['body'] = json.loads(req.data.decode('utf-8'))
            return _causal_answer(sender='670eee18-0216-4628-942b-94ae89effcee')

        with patch.object(svc, '_ensure_registry'), \
             patch('controller.steerwish_service.urllib.request.urlopen',
                   side_effect=fake_urlopen):
            svc._ask_causal_to_plan_seek('w1', _checked_seek_body(
                seek_payload()['seek']))
        assert asked['url'].endswith('/steer/plan')
        assert asked['body']['species'] == 'seek'
        assert 'region' in asked['body']['seek']
        db = svc.archive_repo.write_wish_assignment.call_args
        assert db.args[0] == 'systemtender_670eee18_0216_4628_942b_94ae89effcee'
        events = [c.args[1] for c in svc.repo.insert_steerwish_event.call_args_list]
        assert 'planned' in events and 'assigned' in events

    def test_dead_sender_refuses_loudly_no_note_planted(self):
        svc = _service()
        svc._sender_alive = MagicMock(return_value=(False, 'no beat'))
        with patch.object(svc, '_ensure_registry'), \
             patch('controller.steerwish_service.urllib.request.urlopen',
                   return_value=_causal_answer(sender='dead-one')):
            svc._ask_causal_to_plan_seek('w1', _checked_seek_body(
                seek_payload()['seek']))
        svc.archive_repo.write_wish_assignment.assert_not_called()
        events = [c.args[1] for c in svc.repo.insert_steerwish_event.call_args_list]
        assert 'plan_error' in events

    def test_map_reads_it_refusal_carries_the_reading(self):
        svc = _service()
        refusal = _causal_answer(
            status='refused', reason='map_reads_it',
            detail={'reading': 0.83, 'params': {'param_0': 35.2}})
        with patch.object(svc, '_ensure_registry'), \
             patch('controller.steerwish_service.urllib.request.urlopen',
                   return_value=refusal):
            svc._ask_causal_to_plan_seek('w1', _checked_seek_body(
                seek_payload()['seek']))
        svc.archive_repo.write_wish_assignment.assert_not_called()
        refused = [c for c in svc.repo.insert_steerwish_event.call_args_list
                   if c.args[1] == 'refused'][0]
        detail = json.loads(refused.args[2])
        assert detail['reason'] == 'map_reads_it'
        assert detail['detail']['reading'] == 0.83


class TestExportBeforeDrop:
    def _repo(self):
        repo = database.ArchiveDatabaseRepository.__new__(
            database.ArchiveDatabaseRepository)
        repo.base_config = {'host': 'h', 'port': 1, 'user': 'u', 'password': 'p'}
        return repo

    def test_snapshot_lands_in_a_table_the_drop_cannot_touch(self):
        repo = self._repo()
        queries = []

        def fake_execute(cfg, query, with_result=False):
            queries.append(query)
            if 'FROM trials' in query:
                return [{'number': 1, 'kind': 'push'}]
            if 'FROM ledger_state' in query:
                return [{'key': 'seek_return::w1', 'value_json': '{}'}]
            if 'FROM curve_points' in query:
                return [{'sender_id': 's', 'value': 0.5}]
            return None

        with patch('controller.database.execute_query',
                   side_effect=fake_execute):
            counts = repo.export_systemtender_state('t-1')
        assert counts == {'ledger_trials': 1, 'ledger_state': 1,
                          'curve_points': 1}
        inserts = [q for q in queries if 'INSERT INTO systemtender_exports' in q]
        assert len(inserts) == 2  # ledger + curves
        assert any("VALUES('t-1', 'ledger'" in q for q in inserts)
        assert any("VALUES('t-1', 'curves'" in q for q in inserts)
        assert any('CREATE TABLE IF NOT EXISTS systemtender_exports' in q
                   for q in queries)

    def test_absent_tender_db_skips_honestly(self):
        repo = self._repo()
        def fake_execute(cfg, query, with_result=False):
            if 'FROM trials' in query or 'FROM ledger_state' in query:
                raise RuntimeError('database "systemtender_t_1" does not exist')
            if 'FROM curve_points' in query:
                return []
            return None
        with patch('controller.database.execute_query',
                   side_effect=fake_execute):
            counts = repo.export_systemtender_state('t-1')
        assert counts['ledger_trials'] == 0

    def test_unreadable_tender_db_raises_rather_than_export_empty(self):
        repo = self._repo()
        def fake_execute(cfg, query, with_result=False):
            if 'FROM trials' in query:
                raise RuntimeError('connection refused')
            return None
        with patch('controller.database.execute_query',
                   side_effect=fake_execute), \
             pytest.raises(RuntimeError):
            repo.export_systemtender_state('t-1')
