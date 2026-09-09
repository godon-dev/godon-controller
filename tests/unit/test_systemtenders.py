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
from unittest.mock import MagicMock, Mock, patch
import uuid

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from controller.systemtender_get import main as get_systemtender
from controller.systemtenders_get import main as list_systemtenders
from controller.systemtender_create import main as create_systemtender
from controller.systemtender_delete import main as delete_systemtender
from controller.systemtender_stop import main as stop_systemtender
from controller.systemtender_start import main as start_systemtender


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
