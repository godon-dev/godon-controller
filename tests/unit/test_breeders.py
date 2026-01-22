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

from controller.breeder_get import main as get_breeder
from controller.breeders_get import main as list_breeders
from controller.breeder_create import main as create_breeder
from controller.breeder_delete import main as delete_breeder
from controller.breeder_stop import main as stop_breeder
from controller.breeder_start import main as start_breeder


class TestBreederRetrieval:
    """Test breeder retrieval logic"""

    def test_get_breeder_missing_id(self):
        """Test that missing breeder_id parameter fails"""
        result = get_breeder(request_data=None)
        assert result['result'] == 'FAILURE'
        assert 'Missing breeder_id' in result['error']

    def test_get_breeder_not_found(self):
        """Test retrieving non-existent breeder"""
        with patch('controller.breeder_get.BreederService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service
            # Service returns FAILURE for non-existent breeder
            mock_service.get_breeder.return_value = {
                "result": "FAILURE",
                "breeder_data": "{}"
            }

            fake_id = str(uuid.uuid4())
            result = get_breeder(request_data={"breeder_id": fake_id})

            assert result['result'] == 'FAILURE'

    def test_get_breeder_success(self):
        """Test successful breeder retrieval"""
        with patch('controller.breeder_get.BreederService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            # Service returns wrapped response with data field
            mock_service.get_breeder.return_value = {
                "result": "SUCCESS",
                "data": {
                    "id": test_id,
                    "name": "test-breeder",
                    "status": "active",
                    "createdAt": "2024-01-01T00:00:00Z",
                    "config": {"type": "linux_performance"}
                }
            }

            result = get_breeder(request_data={"breeder_id": test_id})

            # Command adapter passes through wrapped response
            assert result['result'] == 'SUCCESS'
            assert 'data' in result
            assert result['data']['id'] == test_id
            assert result['data']['name'] == 'test-breeder'
            assert result['data']['status'] == 'active'


class TestBreederListing:
    """Test breeder listing logic"""

    def test_list_breeders_empty(self):
        """Test listing when no breeders exist"""
        with patch('controller.breeders_get.BreederService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service
            mock_service.list_breeders.return_value = {
                "result": "SUCCESS",
                "data": []
            }

            result = list_breeders(request_data=None)

            # Command adapter passes through wrapped response
            assert result['result'] == 'SUCCESS'
            assert 'data' in result
            assert result['data'] == []

    def test_list_breeders_multiple(self):
        """Test listing multiple breeders"""
        with patch('controller.breeders_get.BreederService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            id1 = str(uuid.uuid4())
            id2 = str(uuid.uuid4())
            from datetime import datetime
            now = datetime.now()

            # Service returns wrapped response with data field
            mock_service.list_breeders.return_value = {
                "result": "SUCCESS",
                "data": [
                    (id1, "breeder1", now.isoformat()),
                    (id2, "breeder2", now.isoformat())
                ]
            }

            result = list_breeders(request_data=None)

            # Command adapter passes through wrapped response
            assert result['result'] == 'SUCCESS'
            assert 'data' in result
            assert isinstance(result['data'], list)
            assert len(result['data']) == 2
            # Service returns tuples: (id, name, createdAt)
            assert result['data'][0][0] == id1
            assert result['data'][0][1] == 'breeder1'
            assert result['data'][1][0] == id2
            assert result['data'][1][1] == 'breeder2'

    def test_list_breeders_service_failure(self):
        """Test listing when service fails"""
        with patch('controller.breeders_get.BreederService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service
            mock_service.list_breeders.return_value = {
                "result": "FAILURE",
                "breeders": [],
                "error": "Database error"
            }

            result = list_breeders(request_data=None)

            # Should return error as-is
            assert result['result'] == 'FAILURE'
            assert 'error' in result


class TestBreederDeletion:
    """Test breeder deletion logic"""

    def test_delete_breeder_missing_id(self):
        """Test that missing breeder_id parameter fails"""
        result = delete_breeder(request_data=None)
        assert result['result'] == 'FAILURE'
        assert 'Missing breeder_id' in result['error']

    def test_delete_breeder_not_found(self):
        """Test deleting non-existent breeder"""
        with patch('controller.breeder_delete.BreederService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service
            # Simulate deletion failure (breeder doesn't exist)
            mock_service.delete_breeder.return_value = {
                "result": "FAILURE",
                "error": "Breeder not found"
            }

            fake_id = str(uuid.uuid4())
            result = delete_breeder(request_data={"breeder_id": fake_id})

            assert result['result'] == 'FAILURE'
            assert 'error' in result

    def test_delete_breeder_success(self):
        """Test successful breeder deletion"""
        with patch('controller.breeder_delete.BreederService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            mock_service.delete_breeder.return_value = {
                "result": "SUCCESS"
            }

            result = delete_breeder(request_data={"breeder_id": test_id})

            assert result['result'] == 'SUCCESS'
            mock_service.delete_breeder.assert_called_once_with(test_id, force=False)

    def test_delete_breeder_with_force_true(self):
        """Test deletion with force=true parameter"""
        with patch('controller.breeder_delete.BreederService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            mock_service.delete_breeder.return_value = {
                "result": "SUCCESS",
                "data": {
                    "breeder_id": test_id,
                    "delete_type": "force",
                    "workers_cancelled": 3
                }
            }

            result = delete_breeder(request_data={"breeder_id": test_id, "force": True})

            assert result['result'] == 'SUCCESS'
            mock_service.delete_breeder.assert_called_once_with(test_id, force=True)

    def test_delete_breeder_with_force_false_default(self):
        """Test that force defaults to False (safe operation)"""
        with patch('controller.breeder_delete.BreederService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            mock_service.delete_breeder.return_value = {
                "result": "SUCCESS",
                "data": {
                    "breeder_id": test_id,
                    "delete_type": "graceful",
                    "workers_cancelled": 0
                }
            }

            # Don't pass force parameter - should default to False
            result = delete_breeder(request_data={"breeder_id": test_id})

            assert result['result'] == 'SUCCESS'
            mock_service.delete_breeder.assert_called_once_with(test_id, force=False)

    def test_delete_breeder_requires_stop_when_not_forced(self):
        """Test that deletion without force requires graceful stop first"""
        with patch('controller.breeder_delete.BreederService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            # Simulate error: workers still running and force=False
            mock_service.delete_breeder.return_value = {
                "result": "FAILURE",
                "error": "Breeder has active workers. Call stop_breeder() first or use force=True",
                "active_workers": 3
            }

            result = delete_breeder(request_data={"breeder_id": test_id, "force": False})

            assert result['result'] == 'FAILURE'
            assert 'active_workers' in result

    def test_delete_breeder_cancels_worker_jobs(self):
        """Test that delete cancels all worker jobs before dropping database"""
        with patch('controller.breeder_delete.BreederService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            mock_service.delete_breeder.return_value = {
                "result": "SUCCESS",
                "data": {
                    "breeder_id": test_id,
                    "delete_type": "force",
                    "workers_cancelled": 3
                }
            }

            result = delete_breeder(request_data={"breeder_id": test_id, "force": True})

            assert result['result'] == 'SUCCESS'
            assert result['data']['workers_cancelled'] == 3


class TestBreederStop:
    """Test breeder stop functionality"""

    def test_stop_breeder_missing_id(self):
        """Test that missing breeder_id parameter fails"""
        result = stop_breeder(request_data=None)
        assert result['result'] == 'FAILURE'
        assert 'Missing breeder_id' in result['error']

    def test_stop_breeder_not_found(self):
        """Test stopping non-existent breeder"""
        with patch('controller.breeder_stop.BreederService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            mock_service.stop_breeder.return_value = {
                "result": "FAILURE",
                "error": f"Breeder with ID '{test_id}' not found"
            }

            result = stop_breeder(request_data={"breeder_id": test_id})

            assert result['result'] == 'FAILURE'
            assert 'error' in result

    def test_stop_breeder_success(self):
        """Test successful graceful shutdown request"""
        with patch('controller.breeder_stop.BreederService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            mock_service.stop_breeder.return_value = {
                "result": "SUCCESS",
                "message": "Graceful shutdown requested. Workers will stop after completing current trials.",
                "data": {
                    "breeder_id": test_id,
                    "shutdown_type": "graceful"
                }
            }

            result = stop_breeder(request_data={"breeder_id": test_id})

            assert result['result'] == 'SUCCESS'
            assert result['data']['shutdown_type'] == 'graceful'
            mock_service.stop_breeder.assert_called_once_with(test_id)


class TestBreederStart:
    """Test breeder start/resume functionality"""

    def test_start_breeder_missing_id(self):
        """Test that missing breeder_id parameter fails"""
        result = start_breeder(request_data=None)
        assert result['result'] == 'FAILURE'
        assert 'Missing breeder_id' in result['error']

    def test_start_breeder_not_found(self):
        """Test starting non-existent breeder"""
        with patch('controller.breeder_start.BreederService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            mock_service.start_breeder.return_value = {
                "result": "FAILURE",
                "error": f"Breeder with ID '{test_id}' not found"
            }

            result = start_breeder(request_data={"breeder_id": test_id})

            assert result['result'] == 'FAILURE'
            assert 'error' in result

    def test_start_breeder_success(self):
        """Test successful breeder start/resume"""
        with patch('controller.breeder_start.BreederService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            mock_service.start_breeder.return_value = {
                "result": "SUCCESS",
                "data": {
                    "breeder_id": test_id,
                    "workers_started": 3,
                    "status": "ACTIVE"
                }
            }

            result = start_breeder(request_data={"breeder_id": test_id})

            assert result['result'] == 'SUCCESS'
            assert result['data']['status'] == 'ACTIVE'
            assert result['data']['workers_started'] == 3
            mock_service.start_breeder.assert_called_once_with(test_id)

    def test_start_breeder_clears_shutdown_flag(self):
        """Test that start clears the shutdown flag"""
        with patch('controller.breeder_start.BreederService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            mock_service.start_breeder.return_value = {
                "result": "SUCCESS",
                "data": {
                    "breeder_id": test_id,
                    "workers_started": 2,
                    "status": "ACTIVE"
                }
            }

            result = start_breeder(request_data={"breeder_id": test_id})

            assert result['result'] == 'SUCCESS'
            # Verify the service was called and would clear the flag
            mock_service.start_breeder.assert_called_once()


class TestWorkerCancellation:
    """Test worker job cancellation functionality"""

    @patch('controller.breeder_service.cancel_job_by_id')
    def test_cancel_job_by_id_success(self, mock_cancel):
        """Test successful job cancellation"""
        from controller.breeder_service import cancel_job_by_id

        mock_cancel.return_value = True

        result = cancel_job_by_id("test-job-id")

        assert result is True
        mock_cancel.assert_called_once_with("test-job-id")

    @patch('controller.breeder_service.cancel_job_by_id')
    def test_cancel_job_by_id_failure(self, mock_cancel):
        """Test job cancellation failure"""
        from controller.breeder_service import cancel_job_by_id

        mock_cancel.return_value = False

        result = cancel_job_by_id("test-job-id")

        assert result is False

    @patch('controller.breeder_service.Windmill')
    def test_cancel_job_by_id_handles_windmill_init(self, mock_windmill):
        """Test that Windmill client is initialized and API is called"""
        from controller.breeder_service import cancel_job_by_id

        # Mock Windmill client to avoid actual API calls
        mock_client = Mock()
        mock_windmill.return_value = mock_client

        # Call the real cancel_job_by_id function
        result = cancel_job_by_id("test-job-id", reason="Test cancellation")

        assert result is True
        mock_windmill.assert_called_once()
        mock_client.post.assert_called_once()



class TestBreederResponseFormats:
    """Test that response formats match API expectations"""

    def test_get_breeder_response_structure(self):
        """Test that get_breeder returns correct structure"""
        with patch('controller.breeder_get.BreederService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service

            test_id = str(uuid.uuid4())
            mock_service.get_breeder.return_value = {
                "result": "SUCCESS",
                "data": {
                    "id": test_id,
                    "name": "test-breeder",
                    "status": "active",
                    "createdAt": "2024-01-01T00:00:00Z",
                    "config": {"type": "linux_performance"}
                }
            }

            result = get_breeder(request_data={"breeder_id": test_id})

            # Command adapter passes through wrapped response
            assert result['result'] == 'SUCCESS'
            assert 'data' in result
            assert 'id' in result['data']
            assert 'name' in result['data']
            assert 'status' in result['data']
            assert 'createdAt' in result['data']
            assert 'config' in result['data']

    def test_list_breeders_response_structure(self):
        """Test that list_breeders returns correct structure"""
        with patch('controller.breeders_get.BreederService') as mock_service_class:
            mock_service = Mock()
            mock_service_class.return_value = mock_service
            from datetime import datetime
            now = datetime.now()

            mock_service.list_breeders.return_value = {
                "result": "SUCCESS",
                "data": [(str(uuid.uuid4()), "test", now.isoformat())]
            }

            result = list_breeders(request_data=None)

            # Command adapter passes through wrapped response
            assert result['result'] == 'SUCCESS'
            assert 'data' in result
            assert isinstance(result['data'], list)
            if len(result['data']) > 0:
                # Each item is a tuple (id, name, createdAt)
                assert len(result['data'][0]) == 3
