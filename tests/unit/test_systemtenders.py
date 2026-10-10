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
import datetime
import json
from unittest.mock import MagicMock, Mock, patch
import uuid

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from controller.systemtender_get import main as get_systemtender
from controller.systemtenders_get import main as list_systemtenders
from controller.systemtender_create import main as create_systemtender
from controller.systemtender_delete import main as delete_systemtender
from controller.systemtender_create_executor import main as create_systemtender_executor_script
from controller.systemtender_stop import main as stop_systemtender
from controller.systemtender_start import main as start_systemtender
from controller.systemtender_service import SystemtenderService
from controller.database import MetadataDatabaseRepository
from controller.steerwish_service import SteerwishService


class TestSystemtenderRetrieval:
    """Test systemtender retrieval logic"""

    def test_get_systemtender_missing_id(self):
        """Test that missing systemtender_id parameter fails"""
        result = get_systemtender(request_data=None)
        assert result['result'] == 'FAILURE'
        assert 'Missing systemtender_id' in result['error']

    def test_get_systemtender_not_found(self):
        """Test retrieving non-existent systemtender"""
        with patch('controller.systemtender_get.SystemtenderService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service
            # Service returns FAILURE for non-existent systemtender
            mock_service.get_systemtender.return_value = {
                "result": "FAILURE",
                "systemtender_data": "{}"
            }

            fake_id = str(uuid.uuid4())
            result = get_systemtender(request_data={"systemtender_id": fake_id})

            assert result['result'] == 'FAILURE'

    def test_get_systemtender_success(self):
        """Test successful systemtender retrieval"""
        with patch('controller.systemtender_get.SystemtenderService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            # Service returns wrapped response with data field
            mock_service.get_systemtender.return_value = {
                "result": "SUCCESS",
                "data": {
                    "id": test_id,
                    "name": "test-systemtender",
                    "status": "active",
                    "createdAt": "2024-01-01T00:00:00Z",
                    "config": {"type": "linux_performance"}
                }
            }

            result = get_systemtender(request_data={"systemtender_id": test_id})

            # Command adapter passes through wrapped response
            assert result['result'] == 'SUCCESS'
            assert 'data' in result
            assert result['data']['id'] == test_id
            assert result['data']['name'] == 'test-systemtender'
            assert result['data']['status'] == 'active'


class TestSystemtenderListing:
    """Test systemtender listing logic"""

    def test_list_systemtenders_empty(self):
        """Test listing when no systemtenders exist"""
        with patch('controller.systemtenders_get.SystemtenderService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service
            mock_service.list_systemtenders.return_value = {
                "result": "SUCCESS",
                "data": []
            }

            result = list_systemtenders(request_data=None)

            # Command adapter passes through wrapped response
            assert result['result'] == 'SUCCESS'
            assert 'data' in result
            assert result['data'] == []

    def test_list_systemtenders_multiple(self):
        """Test listing multiple systemtenders"""
        with patch('controller.systemtenders_get.SystemtenderService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            id1 = str(uuid.uuid4())
            id2 = str(uuid.uuid4())
            from datetime import datetime
            now = datetime.now()

            # Service returns wrapped response with data field
            mock_service.list_systemtenders.return_value = {
                "result": "SUCCESS",
                "data": [
                    (id1, "systemtender1", now.isoformat()),
                    (id2, "systemtender2", now.isoformat())
                ]
            }

            result = list_systemtenders(request_data=None)

            # Command adapter passes through wrapped response
            assert result['result'] == 'SUCCESS'
            assert 'data' in result
            assert isinstance(result['data'], list)
            assert len(result['data']) == 2
            # Service returns tuples: (id, name, createdAt)
            assert result['data'][0][0] == id1
            assert result['data'][0][1] == 'systemtender1'
            assert result['data'][1][0] == id2
            assert result['data'][1][1] == 'systemtender2'

    def test_list_systemtenders_service_failure(self):
        """Test listing when service fails"""
        with patch('controller.systemtenders_get.SystemtenderService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service
            mock_service.list_systemtenders.return_value = {
                "result": "FAILURE",
                "systemtenders": [],
                "error": "Database error"
            }

            result = list_systemtenders(request_data=None)

            # Should return error as-is
            assert result['result'] == 'FAILURE'
            assert 'error' in result


class TestSystemtenderDeletion:
    """Test systemtender deletion logic"""

    def test_delete_systemtender_missing_id(self):
        """Test that missing systemtender_id parameter fails"""
        result = delete_systemtender(request_data=None)
        assert result['result'] == 'FAILURE'
        assert 'Missing systemtender_id' in result['error']

    def test_delete_systemtender_not_found(self):
        """Test deleting non-existent systemtender"""
        with patch('controller.systemtender_delete.SystemtenderService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service
            # Simulate deletion failure (systemtender doesn't exist)
            mock_service.delete_systemtender.return_value = {
                "result": "FAILURE",
                "error": "Systemtender not found"
            }

            fake_id = str(uuid.uuid4())
            result = delete_systemtender(request_data={"systemtender_id": fake_id})

            assert result['result'] == 'FAILURE'
            assert 'error' in result

    def test_delete_systemtender_success(self):
        """Test successful systemtender deletion"""
        with patch('controller.systemtender_delete.SystemtenderService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            mock_service.delete_systemtender.return_value = {
                "result": "SUCCESS"
            }

            result = delete_systemtender(request_data={"systemtender_id": test_id})

            assert result['result'] == 'SUCCESS'
            mock_service.delete_systemtender.assert_called_once_with(test_id, force=False)

    def test_delete_systemtender_with_force_true(self):
        """Test deletion with force=true parameter"""
        with patch('controller.systemtender_delete.SystemtenderService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            mock_service.delete_systemtender.return_value = {
                "result": "SUCCESS",
                "data": {
                    "systemtender_id": test_id,
                    "delete_type": "force",
                    "workers_cancelled": 3
                }
            }

            result = delete_systemtender(request_data={"systemtender_id": test_id, "force": True})

            assert result['result'] == 'SUCCESS'
            mock_service.delete_systemtender.assert_called_once_with(test_id, force=True)

    def test_delete_systemtender_with_force_false_default(self):
        """Test that force defaults to False (safe operation)"""
        with patch('controller.systemtender_delete.SystemtenderService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            mock_service.delete_systemtender.return_value = {
                "result": "SUCCESS",
                "data": {
                    "systemtender_id": test_id,
                    "delete_type": "graceful",
                    "workers_cancelled": 0
                }
            }

            # Don't pass force parameter - should default to False
            result = delete_systemtender(request_data={"systemtender_id": test_id})

            assert result['result'] == 'SUCCESS'
            mock_service.delete_systemtender.assert_called_once_with(test_id, force=False)

    def test_delete_systemtender_requires_stop_when_not_forced(self):
        """Test that deletion without force requires graceful stop first"""
        with patch('controller.systemtender_delete.SystemtenderService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            # Simulate error: workers still running and force=False
            mock_service.delete_systemtender.return_value = {
                "result": "FAILURE",
                "error": "Systemtender has active workers. Call stop_systemtender() first or use force=True",
                "active_workers": 3
            }

            result = delete_systemtender(request_data={"systemtender_id": test_id, "force": False})

            assert result['result'] == 'FAILURE'
            assert 'active_workers' in result

    def test_delete_systemtender_cancels_worker_jobs(self):
        """Test that delete cancels all worker jobs before dropping database"""
        with patch('controller.systemtender_delete.SystemtenderService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            mock_service.delete_systemtender.return_value = {
                "result": "SUCCESS",
                "data": {
                    "systemtender_id": test_id,
                    "delete_type": "force",
                    "workers_cancelled": 3
                }
            }

            result = delete_systemtender(request_data={"systemtender_id": test_id, "force": True})

            assert result['result'] == 'SUCCESS'
            assert result['data']['workers_cancelled'] == 3


class TestDeleteQuiesce:
    """The delete must not drop the archive DB while a worker job can
    still hold a connection: a drop racing a live pool is what poisons
    YB's relcache ("database may have been dropped and recreated"),
    first seen live on 2026-09-30 (#399). Cancel is async in Windmill
    (Canceling only becomes Canceled when the process dies), so the
    delete fails closed until every job is terminal."""

    def _service_with_jobs(self, job_ids):
        service = SystemtenderService.__new__(SystemtenderService)
        service.metadata_repo = Mock()
        service.metadata_repo.create_table.return_value = None
        # the executor owns the claim; no marker stands at entry
        service.metadata_repo.claim_for_deletion.return_value = True
        service.metadata_repo.get_lifecycle.return_value = None
        definition = {
            'worker_job_ids': list(job_ids),
            'interference_detection': {'group': 'bench'},
        }
        service.metadata_repo.fetch_meta_data.return_value = [
            ('row-id', 'bench-tender', datetime.datetime(2026, 9, 30, 18, 0, 0), definition)]
        service.archive_repo = Mock()
        return service

    class _FakeClock:
        """time.monotonic/sleep pair so the bounded wait costs nothing."""
        def __init__(self):
            self.now = 0.0
        def monotonic(self):
            return self.now
        def sleep(self, s):
            self.now += s

    def test_delete_fails_closed_when_cancel_request_fails(self):
        service = self._service_with_jobs(['job-1'])
        with patch('controller.systemtender_service.cancel_job_by_id', return_value=False), \
             patch('controller.systemtender_service.time', self._FakeClock()):
            result = service.delete_systemtender('bench-tender', force=True)
        assert result['result'] == 'FAILURE'
        assert 'cancel refused' in result['error']
        # the state machine flags deletion-failed with the reason...
        service.metadata_repo.set_lifecycle.assert_called_once()
        call_args = service.metadata_repo.set_lifecycle.call_args.args
        assert call_args[1] == 'deletion-failed'
        assert 'job-1' in call_args[2]
        # ...and nothing was dropped: the relcache race stays shut (#399)
        service.archive_repo.drop_database.assert_not_called()
        service.metadata_repo.remove_systemtender_meta.assert_not_called()

    def test_delete_is_idempotent_when_tender_already_gone(self):
        """Re-DELETE after a completed delete is a SUCCESS, not a 404."""
        service = self._service_with_jobs(['job-1'])
        service.metadata_repo.fetch_meta_data.return_value = []
        result = service.delete_systemtender('bench-tender', force=True)
        assert result['result'] == 'SUCCESS'
        assert result['data']['delete_type'] == 'already-gone'
        # the stale lifecycle row is swept with it
        service.metadata_repo.remove_lifecycle.assert_called_once()

    def test_delete_rides_along_when_an_executor_holds_the_claim(self):
        """Single-flight: a live claim means another executor is running;
        the re-DELETE answers the visible state and spawns nothing."""
        service = self._service_with_jobs(['job-1'])
        service.metadata_repo.claim_for_deletion.return_value = False
        service.metadata_repo.get_lifecycle.return_value = {
            'state': 'deleting', 'reason': None}
        with patch('controller.systemtender_service.cancel_job_by_id', return_value=True):
            result = service.delete_systemtender('bench-tender', force=True)
        assert result['result'] == 'SUCCESS'
        assert result['data']['status'] == 'deleting'
        service.archive_repo.drop_database.assert_not_called()

    def test_delete_waits_for_terminal_jobs_before_drop(self):
        service = self._service_with_jobs(['job-1', 'job-2'])
        clock = self._FakeClock()
        with patch('controller.systemtender_service.cancel_job_by_id', return_value=True), \
             patch('controller.systemtender_service.job_reached_terminal_state',
                   side_effect=[False, False, True, False, True]), \
             patch('controller.systemtender_service.time', clock):
            result = service.delete_systemtender('bench-tender', force=True)
        assert result['result'] == 'SUCCESS'
        service.archive_repo.drop_database.assert_called_once()
        # two poll rounds elapsed before both jobs read terminal
        assert clock.now >= 2 * 2

    def test_delete_fails_closed_when_job_never_quiesces(self):
        service = self._service_with_jobs(['job-stuck'])
        with patch('controller.systemtender_service.cancel_job_by_id', return_value=True), \
             patch('controller.systemtender_service.job_reached_terminal_state',
                   return_value=False), \
             patch('controller.systemtender_service.time', self._FakeClock()):
            result = service.delete_systemtender('bench-tender', force=True)
        assert result['result'] == 'FAILURE'
        assert 'job-stuck' in result['error']
        service.archive_repo.drop_database.assert_not_called()

    def test_delete_proceeds_when_job_history_is_gone(self):
        """A job unknown to the queue (cleaned history) holds no
        connection: the delete proceeds."""
        service = self._service_with_jobs(['job-gone'])
        with patch('controller.systemtender_service.cancel_job_by_id', return_value=True), \
             patch('controller.systemtender_service.job_reached_terminal_state',
                   return_value=True), \
             patch('controller.systemtender_service.time', self._FakeClock()):
            result = service.delete_systemtender('bench-tender', force=True)
        assert result['result'] == 'SUCCESS'
        service.archive_repo.drop_database.assert_called_once()

    def test_job_reached_terminal_state_maps_status(self):
        from controller.systemtender_service import job_reached_terminal_state
        fake_client = Mock()
        fake_client.workspace = 'godon'
        fake_client.get.return_value = {'status': 'Canceled'}
        with patch('controller.systemtender_service.Windmill', return_value=fake_client):
            assert job_reached_terminal_state('job-x') is True
        fake_client.get.return_value = {'status': 'Canceling'}
        with patch('controller.systemtender_service.Windmill', return_value=fake_client):
            assert job_reached_terminal_state('job-x') is False

    def test_job_reached_terminal_state_reads_v2_type_shape(self):
        """Windmill 1.623+ v2 shape: jobs_u/get returns type
        'CompletedJob' with NO status field (live receipt 10-06: eight
        orphans read as forever-running under the old reader)."""
        from controller.systemtender_service import job_reached_terminal_state
        fake_client = Mock()
        fake_client.workspace = 'godon'
        fake_client.get.return_value = {'type': 'CompletedJob', 'workspace_id': 'godon'}
        with patch('controller.systemtender_service.Windmill', return_value=fake_client):
            assert job_reached_terminal_state('job-x') is True
        fake_client.get.return_value = {'type': 'CanceledJob'}
        with patch('controller.systemtender_service.Windmill', return_value=fake_client):
            assert job_reached_terminal_state('job-x') is True
        fake_client.get.return_value = {'type': 'RunningJob'}
        with patch('controller.systemtender_service.Windmill', return_value=fake_client):
            assert job_reached_terminal_state('job-x') is False
        fake_client.get.return_value = {'type': 'QueuedJob'}
        with patch('controller.systemtender_service.Windmill', return_value=fake_client):
            assert job_reached_terminal_state('job-x') is False

    def test_job_reached_terminal_state_counts_404_as_gone(self):
        from controller.systemtender_service import job_reached_terminal_state
        fake_client = Mock()
        fake_client.workspace = 'godon'
        fake_client.get.side_effect = Exception('404 Not Found')
        with patch('controller.systemtender_service.Windmill', return_value=fake_client):
            assert job_reached_terminal_state('job-x') is True

    def test_job_reached_terminal_state_falls_back_to_completed_on_endpoint_error(self):
        """The queue endpoint hard-errored (400-class) live 10-02 for a
        job whose completed row existed; the completed-jobs probe must
        see it terminal instead of wedging the delete closed."""
        from controller.systemtender_service import job_reached_terminal_state
        fake_client = Mock()
        fake_client.workspace = 'godon'

        def url_aware_get(url):
            if 'get_result_maybe' not in url:
                raise Exception('HTTP Error 400: Bad Request')
            return {'completed': True, 'success': False,
                    'result': {'result': 'FAILURE'}, 'started': True}

        fake_client.get.side_effect = url_aware_get
        with patch('controller.systemtender_service.Windmill', return_value=fake_client):
            assert job_reached_terminal_state('job-x') is True

    def test_job_reached_terminal_state_counts_cancelled_never_started_as_gone(self):
        """Post-cancel, started:false means the job never ran and never
        will: an accepted windmill cancel removes queued job rows
        outright (10-05 source receipt; live 10-07: smoke stress
        deletions left jobs_u/get gone - 404 - and the probe answering
        started:false, completed:false, so the old fail-closed looped
        deletion-failed forever and re-DELETE repeated the miss
        deterministically). Nothing that never ran can hold an
        archive-DB connection: terminal. The falsy-body shape (the SDK
        surfacing the missing row as an empty answer) is the same
        receipt, terminal for the same reason."""
        from controller.systemtender_service import job_reached_terminal_state
        fake_client = Mock()
        fake_client.workspace = 'godon'

        def queued_answer(url):
            if 'get_result_maybe' not in url:
                raise Exception('HTTP Error 404: Not Found')
            return {'completed': False, 'started': False}

        fake_client.get.side_effect = queued_answer
        with patch('controller.systemtender_service.Windmill', return_value=fake_client):
            assert job_reached_terminal_state('job-x') is True

        fake_client.get = Mock(return_value=None)
        with patch('controller.systemtender_service.Windmill', return_value=fake_client):
            assert job_reached_terminal_state('job-x') is True

    def test_job_reached_terminal_state_handles_raw_response_surface(self):
        """The worker-image SDK returns raw httpx.Response objects, not
        dicts (live receipt 10-07: "jobs_u/get error polling ...:
        'Response' object has no attribute 'get'" - the reader misread
        every answer and the deletion looped deletion-failed forever).
        A 404 Response plus a started:false probe is the
        cancelled-never-started receipt: terminal. A RunningJob body on
        the same surface stays non-terminal."""
        from controller.systemtender_service import job_reached_terminal_state

        class FakeResponse:
            def __init__(self, status_code, payload):
                self.status_code = status_code
                self._payload = payload

            def json(self):
                if self._payload is None:
                    raise ValueError('no body')
                return self._payload

        fake_client = Mock()
        fake_client.workspace = 'godon'

        def gone_response(url):
            if 'get_result_maybe' not in url:
                return FakeResponse(404, None)
            return FakeResponse(200, {'completed': False, 'started': False})

        fake_client.get.side_effect = gone_response
        with patch('controller.systemtender_service.Windmill', return_value=fake_client):
            assert job_reached_terminal_state('job-x') is True

        def running_response(url):
            if 'get_result_maybe' not in url:
                return FakeResponse(200, {'type': 'RunningJob'})
            return FakeResponse(200, {'completed': False, 'started': True})

        fake_client.get.side_effect = running_response
        with patch('controller.systemtender_service.Windmill', return_value=fake_client):
            assert job_reached_terminal_state('job-x') is False

    def test_job_reached_terminal_state_fails_closed_when_running_or_probe_errors(self):
        """A completed-probe answer with started:true but no completed
        receipt (job running or mid-death), or an erroring probe, keeps
        the job non-terminal: the bounded wait decides, never a guessed
        terminal."""
        from controller.systemtender_service import job_reached_terminal_state
        fake_client = Mock()
        fake_client.workspace = 'godon'

        def running_answer(url):
            if 'get_result_maybe' not in url:
                raise Exception('HTTP Error 400: Bad Request')
            return {'completed': False, 'started': True}

        fake_client.get.side_effect = running_answer
        with patch('controller.systemtender_service.Windmill', return_value=fake_client):
            assert job_reached_terminal_state('job-x') is False

        def probing_error(url):
            raise Exception('HTTP Error 500: Internal Server Error')

        fake_client.get.side_effect = probing_error
        with patch('controller.systemtender_service.Windmill', return_value=fake_client):
            assert job_reached_terminal_state('job-x') is False


class TestSystemtenderStop:
    """Test systemtender stop functionality"""

    def test_stop_systemtender_missing_id(self):
        """Test that missing systemtender_id parameter fails"""
        result = stop_systemtender(request_data=None)
        assert result['result'] == 'FAILURE'
        assert 'Missing systemtender_id' in result['error']

    def test_stop_systemtender_not_found(self):
        """Test stopping non-existent systemtender"""
        with patch('controller.systemtender_stop.SystemtenderService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            mock_service.stop_systemtender.return_value = {
                "result": "FAILURE",
                "error": f"Systemtender with ID '{test_id}' not found"
            }

            result = stop_systemtender(request_data={"systemtender_id": test_id})

            assert result['result'] == 'FAILURE'
            assert 'error' in result

    def test_stop_systemtender_success(self):
        """Test successful graceful shutdown request"""
        with patch('controller.systemtender_stop.SystemtenderService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            mock_service.stop_systemtender.return_value = {
                "result": "SUCCESS",
                "message": "Graceful shutdown requested. Workers will stop after completing current trials.",
                "data": {
                    "systemtender_id": test_id,
                    "shutdown_type": "graceful"
                }
            }

            result = stop_systemtender(request_data={"systemtender_id": test_id})

            assert result['result'] == 'SUCCESS'
            assert result['data']['shutdown_type'] == 'graceful'
            mock_service.stop_systemtender.assert_called_once_with(test_id)


class TestSystemtenderStart:
    """Test systemtender start/resume functionality"""

    def test_start_systemtender_missing_id(self):
        """Test that missing systemtender_id parameter fails"""
        result = start_systemtender(request_data=None)
        assert result['result'] == 'FAILURE'
        assert 'Missing systemtender_id' in result['error']

    def test_start_systemtender_not_found(self):
        """Test starting non-existent systemtender"""
        with patch('controller.systemtender_start.SystemtenderService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            mock_service.start_systemtender.return_value = {
                "result": "FAILURE",
                "error": f"Systemtender with ID '{test_id}' not found"
            }

            result = start_systemtender(request_data={"systemtender_id": test_id})

            assert result['result'] == 'FAILURE'
            assert 'error' in result

    def test_start_systemtender_success(self):
        """Test successful systemtender start/resume"""
        with patch('controller.systemtender_start.SystemtenderService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            mock_service.start_systemtender.return_value = {
                "result": "SUCCESS",
                "data": {
                    "systemtender_id": test_id,
                    "workers_started": 3,
                    "status": "ACTIVE"
                }
            }

            result = start_systemtender(request_data={"systemtender_id": test_id})

            assert result['result'] == 'SUCCESS'
            assert result['data']['status'] == 'ACTIVE'
            assert result['data']['workers_started'] == 3
            mock_service.start_systemtender.assert_called_once_with(test_id)

    def test_start_systemtender_clears_shutdown_flag(self):
        """Test that start clears the shutdown flag"""
        with patch('controller.systemtender_start.SystemtenderService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            mock_service.start_systemtender.return_value = {
                "result": "SUCCESS",
                "data": {
                    "systemtender_id": test_id,
                    "workers_started": 2,
                    "status": "ACTIVE"
                }
            }

            result = start_systemtender(request_data={"systemtender_id": test_id})

            assert result['result'] == 'SUCCESS'
            # Verify the service was called and would clear the flag
            mock_service.start_systemtender.assert_called_once()


class TestWorkerCancellation:
    """Test worker job cancellation functionality"""

    @patch('controller.systemtender_service.cancel_job_by_id')
    def test_cancel_job_by_id_success(self, mock_cancel):
        """Test successful job cancellation"""
        from controller.systemtender_service import cancel_job_by_id

        mock_cancel.return_value = True

        result = cancel_job_by_id("test-job-id")

        assert result is True
        mock_cancel.assert_called_once_with("test-job-id")

    @patch('controller.systemtender_service.cancel_job_by_id')
    def test_cancel_job_by_id_failure(self, mock_cancel):
        """Test job cancellation failure"""
        from controller.systemtender_service import cancel_job_by_id

        mock_cancel.return_value = False

        result = cancel_job_by_id("test-job-id")

        assert result is False

    @patch('controller.systemtender_service.Windmill')
    def test_cancel_job_by_id_handles_windmill_init(self, mock_windmill):
        """Test that Windmill client is initialized and API is called"""
        from controller.systemtender_service import cancel_job_by_id

        # Mock Windmill client to avoid actual API calls
        mock_client = Mock()
        mock_windmill.return_value = mock_client

        # Call the real cancel_job_by_id function
        result = cancel_job_by_id("test-job-id", reason="Test cancellation")

        assert result is True
        mock_windmill.assert_called_once()
        mock_client.post.assert_called_once()



class TestSystemtenderResponseFormats:
    """Test that response formats match API expectations"""

    def test_get_systemtender_response_structure(self):
        """Test that get_systemtender returns correct structure"""
        with patch('controller.systemtender_get.SystemtenderService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            mock_service.get_systemtender.return_value = {
                "result": "SUCCESS",
                "data": {
                    "id": test_id,
                    "name": "test-systemtender",
                    "status": "active",
                    "createdAt": "2024-01-01T00:00:00Z",
                    "config": {"type": "linux_performance"}
                }
            }

            result = get_systemtender(request_data={"systemtender_id": test_id})

            # Command adapter passes through wrapped response
            assert result['result'] == 'SUCCESS'
            assert 'data' in result
            assert 'id' in result['data']
            assert 'name' in result['data']
            assert 'status' in result['data']
            assert 'createdAt' in result['data']
            assert 'config' in result['data']

    def test_list_systemtenders_response_structure(self):
        """Test that list_systemtenders returns correct structure"""
        with patch('controller.systemtenders_get.SystemtenderService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service
            from datetime import datetime
            now = datetime.now()

            mock_service.list_systemtenders.return_value = {
                "result": "SUCCESS",
                "data": [(str(uuid.uuid4()), "test", now.isoformat())]
            }

            result = list_systemtenders(request_data=None)

            # Command adapter passes through wrapped response
            assert result['result'] == 'SUCCESS'
            assert 'data' in result
            assert isinstance(result['data'], list)
            if len(result['data']) > 0:
                # Each item is a tuple (id, name, createdAt)
                assert len(result['data'][0]) == 3


class TestLivenessStalenessFloor:
    """yb abort bursts silenced fresh-connection beats for ~30s while
    the workers stayed alive and trialing: two living tenders were
    stamped presumed_dead at 3x10s and the cell cleaned up mid-run
    (run 36261279551, 2026-09-26). The verdict now rides a 90s floor."""

    def _service_with_state(self, state):
        service = SystemtenderService.__new__(SystemtenderService)
        service.metadata_repo = Mock()
        # no deletion marker: the liveness verdict decides alone
        service.metadata_repo.get_lifecycle.return_value = None
        # row structure: [id, name, creation_ts, definition]
        service.metadata_repo.fetch_meta_data.return_value = [
            ('row-id', 'test-tender', datetime.datetime(2026, 9, 26, 18, 0, 0), '{}')]
        service.archive_repo = Mock()
        service.archive_repo.read_state_verdict.return_value = state
        return service

    def test_abort_burst_within_floor_reads_running(self):
        service = self._service_with_state(
            {'interval_secs': 10, 'age_secs': 45, 'finished': None})
        out = service.get_systemtender('some-uuid')
        assert out['result'] == 'SUCCESS'
        assert out['data']['status'] == 'running'
        assert out['data']['liveness']['stale_threshold_secs'] == 90

    def test_true_death_past_floor_reads_presumed_dead(self):
        service = self._service_with_state(
            {'interval_secs': 10, 'age_secs': 300, 'finished': None})
        out = service.get_systemtender('some-uuid')
        assert out['data']['status'] == 'presumed_dead'

    def test_finished_stamp_wins_over_liveness(self):
        service = self._service_with_state(
            {'interval_secs': 10, 'age_secs': 999,
             'finished': datetime.datetime(2026, 9, 26, 18, 29, 36)})
        out = service.get_systemtender('some-uuid')
        assert out['data']['status'] == 'finished'


class TestReceiverNotePlanted:
    """The wish's reading lives on the receiver, but only the sender's
    house ever got a note - the receiver never adopted, never parked,
    and every wish froze at planned (five flights, found Sep 27, run
    36307488912). The door now plants a receiver note too."""

    def _service(self):
        service = SteerwishService.__new__(SteerwishService)
        service.repo = Mock()
        service.archive_repo = Mock()
        service._causal_url = lambda: "http://127.0.0.1:9091"
        service._sender_alive = lambda sid: (True, '')
        return service

    def _plan_response(self):
        return {
            'status': 'planned',
            'moves': [{'param': 'param_1', 'sender':
                       'f0e0fb34-aaaa-bbbb-cccc-ddddeeeeffff', 'setting': 50.0,
                       'bars': 0.02}],
            'path': ['f0e0fb34-aaaa-bbbb-cccc-ddddeeeeffff',
                     '92a33fa9-1111-2222-3333-444455556666'],
            'predicted': {'value': 1.85, 'bars': 0.02},
        }

    def test_sender_and_receiver_notes_planted(self):
        service = self._service()
        with patch('controller.steerwish_service.urllib.request.urlopen') as ur:
            resp = Mock()
            resp.read.return_value = json.dumps(self._plan_response()).encode()
            ur.return_value.__enter__.return_value = resp
            service._ask_causal_to_plan(
                'wish-1', 'rcv/objective_0',
                {'lo': 1.7, 'hi': 2.0, 'target': 1.85}, limits=None)
        calls = service.archive_repo.write_wish_assignment.call_args_list
        planted = [(c.args[0], c.args[1], c.kwargs.get('role')) for c in calls]
        assert ('systemtender_f0e0fb34_aaaa_bbbb_cccc_ddddeeeeffff',
                'wish-1', 'sender') in planted
        assert ('systemtender_92a33fa9_1111_2222_3333_444455556666',
                'wish-1', 'receiver') in planted

    def test_write_wish_assignment_defaults_to_sender_role(self):
        service = self._service()
        with patch('controller.steerwish_service.urllib.request.urlopen') as ur:
            resp = Mock()
            resp.read.return_value = json.dumps(self._plan_response()).encode()
            ur.return_value.__enter__.return_value = resp
            service._ask_causal_to_plan(
                'wish-2', 'rcv/objective_0',
                {'lo': 1.7, 'hi': 2.0, 'target': 1.85}, limits=None)
        sender_call = service.archive_repo.write_wish_assignment.call_args_list[0]
        assert sender_call.kwargs.get('role', 'sender') == 'sender'

    def test_no_receiver_note_when_self_wish(self):
        # a self-wish (sender == receiver) must not plant twice
        plan = self._plan_response()
        plan['path'] = [plan['path'][0]]
        plan['moves'][0]['sender'] = plan['path'][0]
        service = self._service()
        with patch('controller.steerwish_service.urllib.request.urlopen') as ur:
            resp = Mock()
            resp.read.return_value = json.dumps(plan).encode()
            ur.return_value.__enter__.return_value = resp
            service._ask_causal_to_plan(
                'wish-3', 'rcv/objective_0',
                {'lo': 1.7, 'hi': 2.0, 'target': 1.85}, limits=None)
        calls = service.archive_repo.write_wish_assignment.call_args_list
        assert len(calls) == 1

class TestAsyncCreate:
    """The async create contract (designs/2026-10-10): the create path
    plants a row in `creating` and answers; the executor owns the
    heavy half. The caller's wait is a view over durable state — an
    aborted wait owns nothing."""

    def _fast_service(self):
        service = SystemtenderService.__new__(SystemtenderService)
        service.metadata_repo = Mock()
        service.metadata_repo.fetch_systemtenders_list.return_value = []
        service.archive_repo = Mock()
        return service

    def _config(self):
        return {
            'systemtender': {'type': 'linux_performance'},
            'run': {'parallel': 1},
            'effectuation': {'targets': [{'id': 't1', 'type': 'ssh_host', 'address': 'host-a'}]},
            'cooperation': {'active': True},
        }

    def test_create_plants_row_creating_and_dispatches_executor(self):
        """202 path: row + `creating` marker go in, the executor is
        dispatched async, the caller gets the row back at once — and
        no archive work happens on the caller's connection."""
        service = self._fast_service()
        with patch.object(service, '_resolve_target_refs'), \
             patch.object(service, '_normalize_constraints'), \
             patch.object(service, '_assign_watermark_slot'), \
             patch('controller.systemtender_service.SystemtenderConfig.validate_minimal'), \
             patch('controller.systemtender_service.wmill') as mock_wmill:
            mock_wmill.run_script_by_path_async.return_value = 'executor-job'

            result = service.create_systemtender(self._config(), 'bench-tender')

        assert result['result'] == 'SUCCESS'
        assert result['data']['status'] == 'creating'
        assert result['data']['name'] == 'bench-tender'
        # a real uuid, greppable
        uuid.UUID(result['data']['id'])
        # row planted before dispatch, in creating
        service.metadata_repo.insert_systemtender_meta.assert_called_once()
        lifecycle_args = service.metadata_repo.set_lifecycle.call_args.args
        assert lifecycle_args[1] == 'creating'
        assert lifecycle_args[0] == result['data']['id']
        # the executor is its own job, keyed by the row id only
        dispatch_args = mock_wmill.run_script_by_path_async.call_args.kwargs
        assert dispatch_args['path'] == 'f/controller/systemtender_create_executor'
        assert dispatch_args['args']['request_data']['systemtender_id'] == result['data']['id']
        # fast path holds nothing heavy: no archive database touched
        service.archive_repo.create_database.assert_not_called()
        assert 'duplicate' not in result

    def test_create_dispatch_failure_rolls_the_row_back(self):
        """The executor dispatch never landed: row and marker are
        undone — nothing half-lives under the name, no 409 trap."""
        service = self._fast_service()
        with patch.object(service, '_resolve_target_refs'), \
             patch.object(service, '_normalize_constraints'), \
             patch.object(service, '_assign_watermark_slot'), \
             patch('controller.systemtender_service.SystemtenderConfig.validate_minimal'), \
             patch('controller.systemtender_service.wmill') as mock_wmill:
            mock_wmill.run_script_by_path_async.side_effect = Exception('windmill unreachable')

            result = service.create_systemtender(self._config(), 'bench-tender')

        assert result['result'] == 'FAILURE'
        service.metadata_repo.remove_systemtender_meta.assert_called_once()
        service.metadata_repo.remove_lifecycle.assert_called_once()
        service.archive_repo.drop_database.assert_called_once()

    def test_duplicate_name_answers_the_visible_state(self):
        """A name clash (incl. a tender still creating, or failed to
        create) answers the existing row + its state — never a second
        create."""
        for state in ('creating', 'create-failed', 'deleting', 'deletion-failed', 'active'):
            service = self._fast_service()
            existing_id = str(uuid.uuid4())
            service.metadata_repo.fetch_systemtenders_list.return_value = [
                {'id': existing_id, 'name': 'bench-tender'}]
            service.metadata_repo.get_lifecycle.return_value = {
                'state': state, 'reason': None}
            service.metadata_repo.fetch_meta_data.return_value = [
                (existing_id, 'bench-tender', datetime.datetime(2026, 10, 10, 9, 0, 0), {})]

            with patch.object(service, '_resolve_target_refs'), \
                 patch('controller.systemtender_service.SystemtenderConfig.validate_minimal'):
                result = service.create_systemtender(self._config(), 'bench-tender')

            assert result['result'] == 'SUCCESS'
            assert result['duplicate'] is True
            assert result['data']['id'] == existing_id
            assert result['data']['status'] == state
            assert result['data']['message']
            service.metadata_repo.insert_systemtender_meta.assert_not_called()

    def _executor_service(self):
        service = SystemtenderService.__new__(SystemtenderService)
        service.metadata_repo = Mock()
        service.archive_repo = Mock()
        row_id = str(uuid.uuid4())
        definition = {
            'systemtender': {'type': 'linux_performance'},
            'run': {'parallel': 1},
            'effectuation': {'targets': [{'id': 't1', 'type': 'ssh_host', 'address': 'host-a'}]},
            'cooperation': {'active': True},
        }
        service.metadata_repo.fetch_meta_data.return_value = [
            (row_id, 'bench-tender', datetime.datetime(2026, 10, 10, 9, 0, 0), definition)]
        return service, row_id

    def test_executor_flips_the_row_live(self):
        """The executor does the heavy half with no caller, stores the
        worker job ids, and drops the marker — the row reads active
        from its own state table."""
        service, row_id = self._executor_service()
        with patch.object(service, '_await_state_table'), \
             patch.object(service, '_init_optuna_schema'), \
             patch('controller.systemtender_service.wmill') as mock_wmill, \
             patch('controller.systemtender_service.start_optimization_flow',
                   return_value=('bench-tender_0_0', 'job-1')) as mock_start:
            mock_wmill.run_script_by_path.return_value = {'result': 'SUCCESS'}

            result = service.create_systemtender_executor(row_id)

        assert result['result'] == 'SUCCESS'
        assert result['data']['status'] == 'active'
        assert result['data']['id'] == row_id
        # heavy half ran: db + table + schema + one worker launch
        service.archive_repo.create_database.assert_called_once()
        service.archive_repo.create_systemtender_state_table.assert_called_once()
        mock_start.assert_called_once()
        # job ids persisted for the delete path
        stored = service.metadata_repo.update_systemtender_meta.call_args.kwargs
        assert stored['meta_state']['worker_job_ids'] == ['job-1']
        # live: the marker drops
        service.metadata_repo.remove_lifecycle.assert_called_once_with(row_id)
        service.metadata_repo.set_lifecycle.assert_not_called()

    def test_executor_failure_flags_create_failed_and_keeps_the_row(self):
        """A failed create cancels launched workers, drops the
        half-made archive DB, and keeps the row as the receipt:
        create-failed + reason, visible to the client's poll."""
        service, row_id = self._executor_service()
        with patch.object(service, '_await_state_table'), \
             patch.object(service, '_init_optuna_schema'), \
             patch('controller.systemtender_service.wmill') as mock_wmill, \
             patch('controller.systemtender_service.start_optimization_flow',
                   side_effect=Exception('worker refused')), \
             patch('controller.systemtender_service.cancel_job_by_id', return_value=True):
            mock_wmill.run_script_by_path.return_value = {'result': 'SUCCESS'}

            result = service.create_systemtender_executor(row_id)

        assert result['result'] == 'FAILURE'
        assert result['data']['status'] == 'create-failed'
        call_args = service.metadata_repo.set_lifecycle.call_args.args
        assert call_args[0] == row_id
        assert call_args[1] == 'create-failed'
        assert 'worker refused' in call_args[2]
        # half-made archive db gone, row stays
        service.archive_repo.drop_database.assert_called_once()
        service.metadata_repo.remove_systemtender_meta.assert_not_called()
        service.metadata_repo.remove_lifecycle.assert_not_called()

    def test_executor_missing_row(self):
        """Row vanished before the executor ran (deleted mid-create):
        honest FAILURE, no lifecycle written."""
        service, row_id = self._executor_service()
        service.metadata_repo.fetch_meta_data.return_value = []
        result = service.create_systemtender_executor(row_id)
        assert result['result'] == 'FAILURE'
        service.metadata_repo.set_lifecycle.assert_not_called()

    def test_executor_script_passthrough(self):
        with patch('controller.systemtender_create_executor.SystemtenderService') as mock_class:
            mock_class.return_value.create_systemtender_executor.return_value = {
                'result': 'SUCCESS', 'data': {'status': 'active'}}
            result = create_systemtender_executor_script(
                request_data={'systemtender_id': 'x'})
            assert result['result'] == 'SUCCESS'
            mock_class.return_value.create_systemtender_executor.assert_called_once_with('x')
        # missing id is a plain FAILURE
        assert create_systemtender_executor_script(request_data=None)['result'] == 'FAILURE'

    def test_get_answers_creating_with_creation_reason(self):
        service = SystemtenderService.__new__(SystemtenderService)
        service.metadata_repo = Mock()
        row_id = str(uuid.uuid4())
        service.metadata_repo.fetch_meta_data.return_value = [
            (row_id, 'bench-tender', datetime.datetime(2026, 10, 10, 9, 0, 0), {})]
        service.archive_repo = Mock()
        service.archive_repo.read_state_verdict.return_value = None

        # create-failed carries its reason on the creation axis only
        service.metadata_repo.get_lifecycle.return_value = {
            'state': 'create-failed', 'reason': 'Optuna init failed'}
        data = service.get_systemtender(row_id)['data']
        assert data['status'] == 'create-failed'
        assert data['creation_reason'] == 'Optuna init failed'
        assert data['deletion_reason'] is None

        # no marker + no state table yet: the post-create window reads
        # active — terminal-for-create
        service.metadata_repo.get_lifecycle.return_value = None
        data = service.get_systemtender(row_id)['data']
        assert data['status'] == 'active'
        assert data['creation_reason'] is None

    def test_deletion_claim_takes_over_create_failed_rows(self):
        """A create-failed row is deletable: its executor already
        returned (the rollback cancelled its workers) — the DELETE
        claim is the cleanup path, not a conflict."""
        captured = {}
        repo = MetadataDatabaseRepository.__new__(MetadataDatabaseRepository)
        with patch.object(MetadataDatabaseRepository, '_get_db_config', return_value={}), \
             patch('controller.database.execute_query',
                   side_effect=lambda cfg, query, with_result=False: captured.setdefault('query', query) or ['claimed']):
            claimed = repo.claim_for_deletion(str(uuid.uuid4()))
        assert claimed is True
        assert "state = 'create-failed'" in captured['query']

    def test_get_survives_a_missing_archive_db(self):
        """A row in `creating` has no archive DB yet; a create-failed
        one had it rolled back. The state-verdict read treats 'does
        not exist' as NO verdict - the lifecycle row speaks, GET
        never dies on it (live receipt: controller CI integration
        job, 2026-10-10). Anything else (server down) still raises."""
        from controller.database import ArchiveDatabaseRepository
        repo = ArchiveDatabaseRepository.__new__(ArchiveDatabaseRepository)
        repo.base_config = {'host': 'localhost', 'port': '5432', 'database': 'archive_db'}
        missing_db = Exception(
            'connection to server at "localhost" (::1), port 5432 failed: '
            'FATAL:  database "systemtender_x" does not exist')
        with patch('controller.database.execute_query', side_effect=missing_db):
            assert repo.read_state_verdict('systemtender_x') is None
        with patch('controller.database.execute_query',
                   side_effect=Exception('connection refused')), \
             pytest.raises(Exception):
            repo.read_state_verdict('systemtender_x')

    def test_get_survives_the_table_build_window(self):
        """Live receipt 10-10, smoke rerun 15:09: during the create
        executor's table-build, YB's per-backend DDL visibility
        served a relation-missing (and column-shaped) error for the
        fresh table - any 'does not exist' is NO verdict; anything
        else still raises (infra is not a verdict either, but it must
        be loud)."""
        from controller.database import ArchiveDatabaseRepository
        repo = ArchiveDatabaseRepository.__new__(ArchiveDatabaseRepository)
        repo.base_config = {'host': 'localhost', 'port': '5433', 'database': 'archive_db'}
        for unreadable in [
            Exception('relation "systemtender_state" does not exist'),
            Exception('column "finished_at" does not exist'),
            Exception('database "systemtender_x" does not exist'),
            # YB's internal wording for the same fact, live 10-10
            # (18:51:33, 18:57:30, both mid-create)
            Exception('Table <unknown_table_name> (000040010000300080010000000004e1) '
                      'not found in Raft group 0000000000000000000000000000000000'),
        ]:
            with patch('controller.database.execute_query', side_effect=unreadable):
                assert repo.read_state_verdict('systemtender_x') is None, str(unreadable)
        with patch('controller.database.execute_query',
                   side_effect=Exception('connection refused')), \
             pytest.raises(Exception):
            repo.read_state_verdict('systemtender_x')
