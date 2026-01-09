import pytest
from controller.config import BreederConfig, BREEDER_CAPABILITIES


class TestTargetTypeValidation:
    """Test target type validation and breeder compatibility"""

    def test_breeder_capabilities_loaded(self):
        """Test that breeder capabilities are properly defined"""
        assert 'linux_performance' in BREEDER_CAPABILITIES
        assert 'ssh' in BREEDER_CAPABILITIES['linux_performance']['supported_target_types']

    def test_valid_ssh_target_accepted(self):
        """Test that valid SSH target configuration passes validation"""
        config = {
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'tcp_rtt'}],
            'effectuation': {
                'targets': [
                    {
                        'type': 'ssh',
                        'address': '10.0.0.1',
                        'connection': {
                            'username': 'godon_robot',
                            'private_key': '/opt/godon/credentials/id_rsa'
                        }
                    }
                ]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': {'lower': 0, 'upper': 100}
                    }
                }
            }
        }
        
        result = BreederConfig.validate_minimal(config)
        assert result is True

    def test_missing_target_type_fails(self):
        """Test that missing target type field causes validation failure"""
        config = {
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'tcp_rtt'}],
            'effectuation': {
                'targets': [
                    {
                        'address': '10.0.0.1',
                        'connection': {
                            'username': 'godon_robot',
                            'private_key': '/opt/godon/credentials/id_rsa'
                        }
                    }
                ]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': {'lower': 0, 'upper': 100}
                    }
                }
            }
        }
        
        with pytest.raises(ValueError) as exc_info:
            BreederConfig.validate_minimal(config)
        
        error_msg = str(exc_info.value)
        assert "Missing required 'type' field" in error_msg

    def test_unsupported_target_type_fails(self):
        """Test that unsupported target type causes validation failure"""
        config = {
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'tcp_rtt'}],
            'effectuation': {
                'targets': [
                    {
                        'type': 'http',
                        'address': '10.0.0.1',
                        'connection': {
                            'api_token': 'secret_token'
                        }
                    }
                ]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': {'lower': 0, 'upper': 100}
                    }
                }
            }
        }
        
        with pytest.raises(ValueError) as exc_info:
            BreederConfig.validate_minimal(config)
        
        error_msg = str(exc_info.value)
        assert "not supported by breeder 'linux_performance'" in error_msg
        assert "ssh" in error_msg

    def test_multiple_targets_mixed_types_fails(self):
        """Test that mixing valid and invalid target types fails appropriately"""
        config = {
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'tcp_rtt'}],
            'effectuation': {
                'targets': [
                    {
                        'type': 'ssh',
                        'address': '10.0.0.1',
                        'connection': {
                            'username': 'godon_robot',
                            'private_key': '/opt/godon/credentials/id_rsa'
                        }
                    },
                    {
                        'type': 'api',
                        'address': '10.0.0.2',
                        'connection': {
                            'api_token': 'secret'
                        }
                    }
                ]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': {'lower': 0, 'upper': 100}
                    }
                }
            }
        }
        
        with pytest.raises(ValueError) as exc_info:
            BreederConfig.validate_minimal(config)
        
        error_msg = str(exc_info.value)
        assert '10.0.0.2' in error_msg
        assert "api" in error_msg

    def test_unknown_breeder_skips_type_validation(self):
        """Test that unknown breeder types don't crash validation"""
        config = {
            'breeder': {'type': 'future_breeder'},
            'objectives': [{'name': 'metric'}],
            'effectuation': {
                'targets': [
                    {
                        'type': 'future_type',
                        'address': '10.0.0.1'
                    }
                ]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': {'lower': 0, 'upper': 100}
                    }
                }
            }
        }
        
        # Should pass since 'future_breeder' not in BREEDER_CAPABILITIES
        result = BreederConfig.validate_minimal(config)
        assert result is True
