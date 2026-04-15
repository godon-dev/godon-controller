import pytest
import sys
import os
from unittest.mock import MagicMock, Mock, patch
import uuid
import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from controller.target_create import main as create_target
from controller.target_get import main as get_target
from controller.targets_get import main as list_targets
from controller.target_delete import main as delete_target


class TestTargetValidation:

    def test_create_target_missing_data(self):
        result = create_target(request_data=None)
        assert result['result'] == 'FAILURE'
        assert 'Missing request data' in result['error']

    def test_create_target_missing_name(self):
        result = create_target(request_data={'targetType': 'ssh', 'spec': {'address': '10.0.0.1'}})
        assert result['result'] == 'FAILURE'
        assert 'Missing required field: name' in result['error']

    def test_create_target_missing_type(self):
        result = create_target(request_data={'name': 'test', 'spec': {'address': '10.0.0.1'}})
        assert result['result'] == 'FAILURE'
        assert 'Missing required field: targetType' in result['error']

    def test_create_target_empty_name(self):
        result = create_target(request_data={'name': '', 'targetType': 'ssh', 'spec': {'address': '10.0.0.1'}})
        assert result['result'] == 'FAILURE'
        assert 'name' in result['error'].lower()

    def test_create_target_invalid_name_spaces(self):
        result = create_target(request_data={'name': 'my target', 'targetType': 'ssh', 'spec': {'address': '10.0.0.1'}})
        assert result['result'] == 'FAILURE'
        assert 'Invalid name format' in result['error']

    def test_create_target_invalid_type(self):
        result = create_target(request_data={'name': 'test', 'targetType': 'invalid', 'spec': {'address': '10.0.0.1'}})
        assert result['result'] == 'FAILURE'
        assert 'Invalid targetType' in result['error']

    def test_create_target_spec_not_dict(self):
        result = create_target(request_data={'name': 'test', 'targetType': 'ssh', 'spec': 'not_a_dict'})
        assert result['result'] == 'FAILURE'
        assert 'spec must be a JSON object' in result['error']

    def test_create_target_ssh_missing_address(self):
        result = create_target(request_data={'name': 'test', 'targetType': 'ssh', 'spec': {'username': 'root'}})
        assert result['result'] == 'FAILURE'
        assert 'Missing required spec fields for ssh' in result['error']
        assert 'address' in result['error']

    def test_create_target_http_missing_url(self):
        result = create_target(request_data={'name': 'test', 'targetType': 'http', 'spec': {'auth_type': 'none'}})
        assert result['result'] == 'FAILURE'
        assert 'Missing required spec fields for http' in result['error']
        assert 'url' in result['error']


class TestTargetCreation:

    def test_create_ssh_target_success(self):
        with patch('controller.target_create.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo

            result = create_target(request_data={
                'name': 'test-server',
                'targetType': 'ssh',
                'spec': {
                    'address': '192.168.1.100',
                    'username': 'deploy',
                    'credential_id': 'cred-123',
                    'allows_downtime': False,
                },
                'metadata': {'description': 'Test server'}
            })

            assert result['result'] == 'SUCCESS'
            assert result['data']['name'] == 'test-server'
            assert result['data']['targetType'] == 'ssh'
            assert result['data']['spec']['address'] == '192.168.1.100'
            assert result['data']['spec']['username'] == 'deploy'
            assert 'id' in result['data']
            mock_repo.create_targets_table.assert_called_once()
            mock_repo.insert_target.assert_called_once()

    def test_create_http_target_success(self):
        with patch('controller.target_create.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo

            result = create_target(request_data={
                'name': 'greenhouse',
                'targetType': 'http',
                'spec': {
                    'url': 'http://greenhouse:8090',
                    'auth_type': 'none',
                },
            })

            assert result['result'] == 'SUCCESS'
            assert result['data']['targetType'] == 'http'
            assert result['data']['spec']['url'] == 'http://greenhouse:8090'

    def test_create_target_valid_types(self):
        with patch('controller.target_create.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo

            specs = {
                'ssh': {'address': '10.0.0.1'},
                'http': {'url': 'http://localhost:8090'},
            }

            for t, spec in specs.items():
                result = create_target(request_data={'name': f'test_{t}', 'targetType': t, 'spec': spec})
                assert result['result'] == 'SUCCESS'
                assert result['data']['targetType'] == t

    def test_create_target_duplicate_name(self):
        with patch('controller.target_create.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            mock_repo.insert_target.side_effect = Exception('duplicate key violation')

            result = create_target(request_data={
                'name': 'existing-server',
                'targetType': 'ssh',
                'spec': {'address': '10.0.0.1'}
            })

            assert result['result'] == 'FAILURE'
            assert 'already exists' in result['error'].lower()

    def test_create_target_minimal_fields(self):
        with patch('controller.target_create.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo

            result = create_target(request_data={
                'name': 'minimal-target',
                'targetType': 'ssh',
                'spec': {'address': '10.0.0.1'}
            })

            assert result['result'] == 'SUCCESS'
            assert result['data']['spec']['address'] == '10.0.0.1'


class TestTargetRetrieval:

    def test_get_target_missing_id(self):
        result = get_target(request_data=None)
        assert result['result'] == 'FAILURE'
        assert 'Missing targetId' in result['error']

    def test_get_target_not_found(self):
        with patch('controller.target_get.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            mock_repo.fetch_target_by_id.return_value = None

            result = get_target(request_data={"targetId": str(uuid.uuid4())})
            assert result['result'] == 'FAILURE'
            assert 'not found' in result['error'].lower()

    def test_get_target_success_ssh(self):
        with patch('controller.target_get.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo

            test_id = str(uuid.uuid4())
            now = datetime.datetime.now()
            mock_target = (
                test_id, 'test-server', 'ssh',
                {'address': '192.168.1.100', 'username': 'deploy', 'credential_id': 'cred-123'},
                {'description': 'Test server'},
                now, None
            )
            mock_repo.fetch_target_by_id.return_value = mock_target

            result = get_target(request_data={"targetId": test_id})

            assert result['result'] == 'SUCCESS'
            assert result['data']['id'] == test_id
            assert result['data']['name'] == 'test-server'
            assert result['data']['targetType'] == 'ssh'
            assert result['data']['spec']['address'] == '192.168.1.100'
            assert result['data']['metadata']['description'] == 'Test server'

    def test_get_target_success_http(self):
        with patch('controller.target_get.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo

            test_id = str(uuid.uuid4())
            now = datetime.datetime.now()
            mock_target = (
                test_id, 'greenhouse', 'http',
                {'url': 'http://greenhouse:8090', 'auth_type': 'none'},
                None,
                now, None
            )
            mock_repo.fetch_target_by_id.return_value = mock_target

            result = get_target(request_data={"targetId": test_id})

            assert result['result'] == 'SUCCESS'
            assert result['data']['spec']['url'] == 'http://greenhouse:8090'


class TestTargetListing:

    def test_list_targets_empty(self):
        with patch('controller.targets_get.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            mock_repo.fetch_targets_list.return_value = []

            result = list_targets(request_data=None)

            assert result['result'] == 'SUCCESS'
            assert result['data'] == []

    def test_list_targets_multiple(self):
        with patch('controller.targets_get.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo

            id1 = str(uuid.uuid4())
            id2 = str(uuid.uuid4())
            now = datetime.datetime.now()
            mock_targets = [
                (id1, 'server-1', 'ssh', {'address': '10.0.0.1', 'username': 'root'}, None, now, None),
                (id2, 'api-1', 'http', {'url': 'https://api.test.com'}, None, now, None)
            ]
            mock_repo.fetch_targets_list.return_value = mock_targets

            result = list_targets(request_data=None)

            assert result['result'] == 'SUCCESS'
            assert len(result['data']) == 2
            assert result['data'][0]['name'] == 'server-1'
            assert result['data'][0]['spec']['address'] == '10.0.0.1'
            assert result['data'][1]['name'] == 'api-1'
            assert result['data'][1]['spec']['url'] == 'https://api.test.com'


class TestTargetDeletion:

    def test_delete_target_missing_id(self):
        result = delete_target(request_data=None)
        assert result['result'] == 'FAILURE'
        assert 'Missing targetId' in result['error']

    def test_delete_target_not_found(self):
        with patch('controller.target_delete.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            mock_repo.fetch_target_by_id.return_value = None

            result = delete_target(request_data={"targetId": str(uuid.uuid4())})
            assert result['result'] == 'FAILURE'
            assert 'not found' in result['error'].lower()

    def test_delete_target_success(self):
        with patch('controller.target_delete.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo

            test_id = str(uuid.uuid4())
            now = datetime.datetime.now()
            mock_target = (test_id, 'test', 'ssh', {'address': '10.0.0.1'}, None, now, None)
            mock_repo.fetch_target_by_id.return_value = mock_target

            result = delete_target(request_data={"targetId": test_id})

            assert result['result'] == 'SUCCESS'
            mock_repo.delete_target.assert_called_once_with(test_id)


class TestTargetErrorHandling:

    def test_get_target_database_error(self):
        with patch('controller.target_get.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            mock_repo.fetch_target_by_id.side_effect = Exception('Database error')

            result = get_target(request_data={"targetId": str(uuid.uuid4())})
            assert result['result'] == 'FAILURE'

    def test_list_targets_database_error(self):
        with patch('controller.targets_get.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            mock_repo.fetch_targets_list.side_effect = Exception('Database error')

            result = list_targets(request_data=None)
            assert result['result'] == 'FAILURE'

    def test_delete_target_database_error(self):
        with patch('controller.target_delete.MetadataDatabaseRepository') as mock_repo_class:
            mock_repo = Mock()
            mock_repo_class.return_value = mock_repo
            mock_repo.fetch_target_by_id.side_effect = Exception('Database error')

            result = delete_target(request_data={"targetId": str(uuid.uuid4())})
            assert result['result'] == 'FAILURE'
