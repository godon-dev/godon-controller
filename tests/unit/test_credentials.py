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

from controller.credential_create import main as create_credential
from controller.credential_get import main as get_credential
from controller.credentials_get import main as list_credentials
from controller.credential_delete import main as delete_credential


class TestCredentialValidation:
    """Test credential input validation and error handling"""

    def test_create_credential_missing_data(self):
        """Test that missing credential data returns appropriate error"""
        result = create_credential(request_data=None)
        assert result['result'] == 'FAILURE'
        assert 'Missing request data' in result['error']

    def test_create_credential_missing_name(self):
        """Test that missing name field fails validation"""
        credential_data = {
            'credentialType': 'ssh_private_key',
            'description': 'Test credential'
        }
        result = create_credential(request_data=credential_data)
        assert result['result'] == 'FAILURE'
        assert 'Missing required fields' in result['error']
        assert 'name' in result['error']

    def test_create_credential_missing_type(self):
        """Test that missing credential_type field fails validation"""
        credential_data = {
            'name': 'test_credential',
            'description': 'Test credential'
        }
        result = create_credential(request_data=credential_data)
        assert result['result'] == 'FAILURE'
        assert 'Missing required fields' in result['error']
        assert 'credentialType' in result['error']

    def test_create_credential_invalid_name_spaces(self):
        """Test that names with spaces fail validation"""
        credential_data = {
            'name': 'test credential',
            'credentialType': 'ssh_private_key'
        }
        result = create_credential(request_data=credential_data)
        assert result['result'] == 'FAILURE'
        assert 'Invalid name format' in result['error']

    def test_create_credential_invalid_name_special_chars(self):
        """Test that names with special characters fail validation"""
        invalid_names = ['test@credential', 'test#credential', 'test$credential', 'test%credential']
        for invalid_name in invalid_names:
            credential_data = {
                'name': invalid_name,
                'credentialType': 'ssh_private_key'
            }
            result = create_credential(request_data=credential_data)
            assert result['result'] == 'FAILURE'
            assert 'Invalid name format' in result['error']

    def test_create_credential_valid_name_formats(self):
        """Test that valid name formats pass validation"""
        with patch('controller.credential_create.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            mock_repo.insert_credential.return_value = None
            
            valid_names = ['test_credential', 'test-credential', 'test123', 'Test_Credential-123']
            for valid_name in valid_names:
                credential_data = {
                    'name': valid_name,
                    'credentialType': 'ssh_private_key'
                }
                result = create_credential(request_data=credential_data)
                # Should pass validation and reach database insertion
                assert result['result'] == 'SUCCESS'
                assert result['data']['name'] == valid_name

    def test_create_credential_invalid_type(self):
        """Test that invalid credential types fail validation"""
        credential_data = {
            'name': 'test_credential',
            'credentialType': 'invalid_type'
        }
        result = create_credential(request_data=credential_data)
        assert result['result'] == 'FAILURE'
        assert 'Invalid credentialType' in result['error']

    def test_create_credential_valid_types(self):
        """Test that all valid credential types are accepted"""
        with patch('controller.credential_create.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            mock_repo.insert_credential.return_value = None
            
            valid_types = ['ssh_private_key', 'api_token', 'database_connection', 'http_basic_auth']
            for cred_type in valid_types:
                credential_data = {
                    'name': f'test_{cred_type}',
                    'credentialType': cred_type
                }
                result = create_credential(request_data=credential_data)
                # Should pass validation
                assert result['result'] == 'SUCCESS'
                assert result['data']['credentialType'] == cred_type

    def test_create_credential_empty_name(self):
        """Test that empty name fails validation"""
        credential_data = {
            'name': '',
            'credentialType': 'ssh_private_key'
        }
        result = create_credential(request_data=credential_data)
        assert result['result'] == 'FAILURE'
        # Empty strings are falsy, so it should trigger missing required fields

    def test_create_credential_none_fields(self):
        """Test that None values in required fields fail validation"""
        credential_data = {
            'name': None,
            'credentialType': None
        }
        result = create_credential(request_data=credential_data)
        assert result['result'] == 'FAILURE'
        assert 'Missing required fields' in result['error']


class TestCredentialCreation:
    """Test credential creation logic"""

    def test_create_credential_success(self):
        """Test successful credential creation"""
        with patch('controller.credential_create.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            mock_repo.insert_credential.return_value = None
            
            credential_data = {
                'name': 'test_ssh_key',
                'credentialType': 'ssh_private_key',
                'description': 'Test SSH key'
            }
            
            result = create_credential(request_data=credential_data)

            assert result['result'] == 'SUCCESS'
            assert 'data' in result
            assert result['data']['name'] == 'test_ssh_key'
            assert result['data']['credentialType'] == 'ssh_private_key'
            assert result['data']['description'] == 'Test SSH key'
            assert result['data']['windmillVariable'] == 'f/vars/test_ssh_key'
            assert 'id' in result['data']
            
            # Verify database operations were called
            mock_repo.create_credentials_table.assert_called_once()
            mock_repo.insert_credential.assert_called_once()

    def test_create_credential_with_description(self):
        """Test credential creation with description"""
        with patch('controller.credential_create.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            mock_repo.insert_credential.return_value = None
            
            credential_data = {
                'name': 'test_api_token',
                'credentialType': 'api_token',
                'description': 'API token for external service'
            }
            
            result = create_credential(request_data=credential_data)
            
            assert result['result'] == 'SUCCESS'
            assert result['data']['description'] == 'API token for external service'

    def test_create_credential_without_description(self):
        """Test credential creation without description (uses default)"""
        with patch('controller.credential_create.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            mock_repo.insert_credential.return_value = None
            
            credential_data = {
                'name': 'test_db_conn',
                'credentialType': 'database_connection'
            }
            
            result = create_credential(request_data=credential_data)
            
            assert result['result'] == 'SUCCESS'
            assert result['data']['description'] == ''

    def test_create_credential_duplicate_name(self):
        """Test that duplicate credential names are rejected"""
        with patch('controller.credential_create.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            # Simulate duplicate key error
            mock_repo.insert_credential.side_effect = Exception('duplicate key violation')
            
            credential_data = {
                'name': 'existing_credential',
                'credentialType': 'ssh_private_key'
            }
            
            result = create_credential(request_data=credential_data)
            
            assert result['result'] == 'FAILURE'
            assert 'already exists' in result['error'].lower()

    def test_create_credential_uuid_generation(self):
        """Test that unique UUIDs are generated for credentials"""
        with patch('controller.credential_create.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            mock_repo.insert_credential.return_value = None
            
            credential_data = {
                'name': 'test_uuid_gen',
                'credentialType': 'api_token'
            }
            
            result_1 = create_credential(request_data=credential_data)
            result_2 = create_credential(request_data={**credential_data, 'name': 'test_uuid_gen_2'})
            
            # Should generate different UUIDs
            assert result_1['data']['id'] != result_2['data']['id']

            # UUIDs should be valid UUID strings
            uuid.UUID(result_1['data']['id'])
            uuid.UUID(result_2['data']['id'])

    def test_create_credential_windmill_variable_format(self):
        """Test that Windmill variable paths are correctly formatted"""
        with patch('controller.credential_create.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            mock_repo.insert_credential.return_value = None
            
            test_names = [
                ('my_ssh_key', 'f/vars/my_ssh_key'),
                ('api-token-123', 'f/vars/api-token-123'),
                ('test_Credential', 'f/vars/test_Credential')
            ]
            
            for name, expected_var in test_names:
                credential_data = {
                    'name': name,
                    'credentialType': 'ssh_private_key'
                }
                
                result = create_credential(request_data=credential_data)
                assert result['data']['windmillVariable'] == expected_var

    def test_create_credential_database_exception(self):
        """Test handling of unexpected database errors"""
        with patch('controller.credential_create.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            # Simulate unexpected database error
            mock_repo.insert_credential.side_effect = Exception('Database connection failed')
            
            credential_data = {
                'name': 'test_error',
                'credentialType': 'api_token'
            }
            
            result = create_credential(request_data=credential_data)
            assert result['result'] == 'FAILURE'


class TestCredentialRetrieval:
    """Test credential retrieval logic"""

    def test_get_credential_missing_id(self):
        """Test that missing credential_id parameter fails"""
        result = get_credential(request_data=None)
        assert result['result'] == 'FAILURE'
        assert 'Missing credentialId' in result['error']

    def test_get_credential_not_found(self):
        """Test retrieving non-existent credential"""
        with patch('controller.credential_get.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            mock_repo.fetch_credential_by_id.return_value = None
            
            fake_id = str(uuid.uuid4())
            result = get_credential(request_data={"credentialId": fake_id})
            
            assert result['result'] == 'FAILURE'
            assert 'not found' in result['error'].lower()

    def test_get_credential_success(self):
        """Test successful credential retrieval"""
        with patch('controller.credential_get.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            
            # Mock database response
            test_id = str(uuid.uuid4())
            mock_credential = (
                test_id,  # id
                'test_ssh_key',  # name
                'ssh_private_key',  # credential_type
                'Test SSH key',  # description
                'f/vars/test_ssh_key',  # windmill_variable
                'windmillVariable',  # store_type
                {},  # metadata
                None,  # created_at
                None   # last_used_at
            )
            mock_repo.fetch_credential_by_id.return_value = mock_credential
            
            result = get_credential(request_data={"credentialId": test_id})
            
            assert result['result'] == 'SUCCESS'
            assert result['data']['id'] == test_id
            assert result['data']['name'] == 'test_ssh_key'
            assert result['data']['credentialType'] == 'ssh_private_key'
            assert result['data']['description'] == 'Test SSH key'
            assert result['data']['windmillVariable'] == 'f/vars/test_ssh_key'

    def test_get_credential_with_timestamps(self):
        """Test credential retrieval with timestamp fields"""
        from datetime import datetime
        
        with patch('controller.credential_get.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            
            test_id = str(uuid.uuid4())
            now = datetime.now()
            mock_credential = (
                test_id,
                'test_api_token',
                'api_token',
                'API token',
                'f/vars/test_api_token',
                'windmillVariable',
                {},
                now,
                now
            )
            mock_repo.fetch_credential_by_id.return_value = mock_credential
            
            result = get_credential(request_data={"credentialId": test_id})
            
            assert result['result'] == 'SUCCESS'
            assert result['data']['createdAt'] is not None
            assert result['data']['lastUsedAt'] is not None


class TestCredentialListing:
    """Test credential listing logic"""

    def test_list_credentials_empty(self):
        """Test listing when no credentials exist"""
        with patch('controller.credentials_get.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            mock_repo.fetch_credentials_list.return_value = []

            result = list_credentials(request_data=None)

            # Command adapter passes through wrapped response
            assert result['result'] == 'SUCCESS'
            assert 'data' in result
            assert isinstance(result['data'], list)
            assert result['data'] == []
            assert len(result['data']) == 0

    def test_list_credentials_multiple(self):
        """Test listing multiple credentials"""
        with patch('controller.credentials_get.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo

            # Mock database response with multiple credentials
            id1 = str(uuid.uuid4())
            id2 = str(uuid.uuid4())
            mock_credentials = [
                (id1, 'ssh_key', 'ssh_private_key', 'SSH', 'f/vars/ssh_key', None, None),
                (id2, 'api_token', 'api_token', 'API', 'f/vars/api_token', None, None)
            ]
            mock_repo.fetch_credentials_list.return_value = mock_credentials

            result = list_credentials(request_data=None)

            # Command adapter passes through wrapped response
            assert result['result'] == 'SUCCESS'
            assert 'data' in result
            assert isinstance(result['data'], list)
            assert len(result['data']) == 2
            assert result['data'][0]['name'] == 'ssh_key'
            assert result['data'][1]['name'] == 'api_token'

    def test_list_credentials_with_timestamps(self):
        """Test listing credentials with timestamp fields"""
        from datetime import datetime

        with patch('controller.credentials_get.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo

            now = datetime.now()
            mock_credentials = [
                (str(uuid.uuid4()), 'test_cred', 'api_token', 'Test', 'f/vars/test', now, now)
            ]
            mock_repo.fetch_credentials_list.return_value = mock_credentials

            result = list_credentials(request_data=None)

            # Command adapter passes through wrapped response
            assert result['result'] == 'SUCCESS'
            assert 'data' in result
            assert isinstance(result['data'], list)
            assert len(result['data']) == 1
            assert result['data'][0]['createdAt'] is not None
            assert result['data'][0]['lastUsedAt'] is not None


class TestCredentialDeletion:
    """Test credential deletion logic"""

    def test_delete_credential_missing_id(self):
        """Test that missing credential_id parameter fails"""
        result = delete_credential(request_data=None)
        assert result['result'] == 'FAILURE'
        assert 'Missing credentialId' in result['error']

    def test_delete_credential_not_found(self):
        """Test deleting non-existent credential"""
        with patch('controller.credential_delete.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            mock_repo.fetch_credential_by_id.return_value = None
            
            fake_id = str(uuid.uuid4())
            result = delete_credential(request_data={"credentialId": fake_id})
            
            assert result['result'] == 'FAILURE'
            assert 'not found' in result['error'].lower()

    def test_delete_credential_success(self):
        """Test successful credential deletion"""
        with patch('controller.credential_delete.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            
            test_id = str(uuid.uuid4())
            mock_credential = (
                test_id,
                'test_credential',
                'api_token',
                'Test credential',
                'f/vars/test_credential',
                'windmillVariable',
                {},
                None,
                None
            )
            mock_repo.fetch_credential_by_id.return_value = mock_credential
            
            result = delete_credential(request_data={"credentialId": test_id})

            assert result['result'] == 'SUCCESS'
            assert result['data'] is None
            mock_repo.delete_credential.assert_called_once_with(test_id)

    def test_delete_credential_verification(self):
        """Test that deletion verifies existence before deleting"""
        with patch('controller.credential_delete.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            
            test_id = str(uuid.uuid4())
            mock_credential = (
                test_id,
                'test_credential',
                'api_token',
                'Test credential',
                'f/vars/test_credential',
                'windmillVariable',
                {},
                None,
                None
            )
            mock_repo.fetch_credential_by_id.return_value = mock_credential
            
            result = delete_credential(request_data={"credentialId": test_id})
            
            # Should first check existence, then delete
            mock_repo.fetch_credential_by_id.assert_called_once_with(test_id)
            mock_repo.delete_credential.assert_called_once_with(test_id)


class TestCredentialErrorHandling:
    """Test error handling in credential operations"""

    def test_get_credential_database_error(self):
        """Test handling of database errors during retrieval"""
        with patch('controller.credential_get.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            mock_repo.fetch_credential_by_id.side_effect = Exception('Database error')
            
            result = get_credential(request_data={"credentialId": str(uuid.uuid4())})
            assert result['result'] == 'FAILURE'

    def test_list_credentials_database_error(self):
        """Test handling of database errors during listing"""
        with patch('controller.credentials_get.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            mock_repo.fetch_credentials_list.side_effect = Exception('Database error')
            
            result = list_credentials(request_data=None)
            assert result['result'] == 'FAILURE'

    def test_delete_credential_database_error(self):
        """Test handling of database errors during deletion"""
        with patch('controller.credential_delete.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            
            test_id = str(uuid.uuid4())
            mock_credential = (test_id, 'test', 'api_token', 'Test', 'f/vars/test', 'windmill', {}, None, None)
            mock_repo.fetch_credential_by_id.return_value = mock_credential
            mock_repo.delete_credential.side_effect = Exception('Database error')
            
            result = delete_credential(request_data={"credentialId": test_id})
            assert result['result'] == 'FAILURE'