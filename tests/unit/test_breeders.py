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
            mock_service.delete_breeder.assert_called_once_with(test_id)


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
