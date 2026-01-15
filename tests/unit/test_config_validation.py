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
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'tcp_rtt'}],
            'effectuation': {
                'targets': [
                    {
                        'type': 'ssh',
                        'address': '10.0.0.1',
                        'username': 'godon_robot',
                        'credentialName': 'my-ssh-key'
                    }
                ]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': [{'step': 1, 'lower': 0, 'upper': 100}]
                    }
                }
            }
        }

        result = BreederConfig.validate_minimal(config)
        assert result["success"] is True

    def test_missing_target_type_fails(self):
        """Test that missing target type field causes validation failure"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'tcp_rtt'}],
            'effectuation': {
                'targets': [
                    {
                        'address': '10.0.0.1',
                        'username': 'godon_robot',
                        'credentialName': 'my-ssh-key'
                    }
                ]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': [{'step': 1, 'lower': 0, 'upper': 100}]
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
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'tcp_rtt'}],
            'effectuation': {
                'targets': [
                    {
                        'type': 'http',
                        'address': '10.0.0.1'
                    }
                ]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': [{'step': 1, 'lower': 0, 'upper': 100}]
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
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'tcp_rtt'}],
            'effectuation': {
                'targets': [
                    {
                        'type': 'ssh',
                        'address': '10.0.0.1',
                        'username': 'admin',
                        'credentialName': 'my-key'
                    },
                    {
                        'type': 'api',
                        'address': '10.0.0.2'
                    }
                ]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': [{'step': 1, 'lower': 0, 'upper': 100}]
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
            'meta': {'configVersion': '0.3'},
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
                        'constraints': [{'step': 1, 'lower': 0, 'upper': 100}]
                    }
                }
            }
        }

        # Should pass since 'future_breeder' not in BREEDER_CAPABILITIES
        result = BreederConfig.validate_minimal(config)
        assert result["success"] is True


class TestConfigVersionValidation:
    """Test ConfigVersion validation """

    def test_missing_config_version_warns(self):
        """Test that missing ConfigVersion defaults to v0.2 and triggers warning"""
        config = {
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'tcp_rtt'}],
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    'address': '10.0.0.1',
                    'username': 'admin',
                    'credentialName': 'my-key'
                }]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': [{'step': 1, 'lower': 0, 'upper': 100}]
                    }
                }
            }
        }

        with pytest.raises(ValueError) as exc_info:
            BreederConfig.validate_minimal(config)

        error_msg = str(exc_info.value)
        assert "Config version '0.2' is outdated" in error_msg
        assert "0.3" in error_msg

    def test_v03_config_version_passes(self):
        """Test that ConfigVersion passes validation"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'tcp_rtt'}],
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    'address': '10.0.0.1',
                    'username': 'admin',
                    'credentialName': 'my-key'
                }]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': [{'step': 1, 'lower': 0, 'upper': 100}]
                    }
                }
            }
        }

        result = BreederConfig.validate_minimal(config)
        assert result["success"] is True


class TestV03ConstraintValidation:
    """Test v0.3 constraint structure validation (list of ranges/values)"""

    def test_v03_integer_range_constraint_passes(self):
        """Test that valid v0.3 integer range constraint passes"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'tcp_rtt'}],
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    'address': '10.0.0.1',
                    'username': 'admin',
                    'credentialName': 'my-key'
                }]
            },
            'settings': {
                'sysctl': {
                    'net.ipv4.tcp_rmem': {
                        'constraints': [
                            {'step': 100, 'lower': 4096, 'upper': 131072},
                            {'step': 100, 'lower': 131072, 'upper': 262144}
                        ]
                    }
                }
            }
        }

        result = BreederConfig.validate_minimal(config)
        assert result["success"] is True

    def test_v03_categorical_constraint_passes(self):
        """Test that valid v0.3 categorical constraint passes"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'tcp_rtt'}],
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    'address': '10.0.0.1',
                    'username': 'admin',
                    'credentialName': 'my-key'
                }]
            },
            'settings': {
                'sysfs': {
                    'cpu_governor': {
                        'constraints': [
                            {'values': ['performance', 'powersave', 'ondemand']}
                        ]
                    }
                }
            }
        }

        result = BreederConfig.validate_minimal(config)
        assert result["success"] is True

    def test_constraints_not_list_fails(self):
        """Test that dict constraints without 'values' key fail validation"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'tcp_rtt'}],
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    'address': '10.0.0.1',
                    'username': 'admin',
                    'credentialName': 'my-key'
                }]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': {'lower': 0, 'upper': 100}  # Dict without 'values' key
                    }
                }
            }
        }

        with pytest.raises(ValueError) as exc_info:
            BreederConfig.validate_minimal(config)

        error_msg = str(exc_info.value)
        assert "constraints dict must have 'values' key" in error_msg

    def test_constraint_lower_greater_than_upper_fails(self):
        """Test that lower >= upper fails validation"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'tcp_rtt'}],
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    'address': '10.0.0.1',
                    'username': 'admin',
                    'credentialName': 'my-key'
                }]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': [{'step': 1, 'lower': 100, 'upper': 0}]
                    }
                }
            }
        }

        with pytest.raises(ValueError) as exc_info:
            BreederConfig.validate_minimal(config)

        error_msg = str(exc_info.value)
        assert "lower" in error_msg.lower()
        assert "upper" in error_msg.lower()
        assert "must be less than" in error_msg.lower()

    def test_constraint_non_positive_step_fails(self):
        """Test that non-positive step fails validation"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'tcp_rtt'}],
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    'address': '10.0.0.1',
                    'username': 'admin',
                    'credentialName': 'my-key'
                }]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': [{'step': 0, 'lower': 0, 'upper': 100}]
                    }
                }
            }
        }

        with pytest.raises(ValueError) as exc_info:
            BreederConfig.validate_minimal(config)

        error_msg = str(exc_info.value)
        assert "step" in error_msg.lower()
        assert "positive" in error_msg.lower()

    def test_categorical_values_less_than_two_fails(self):
        """Test that categorical values with < 2 items fail validation"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'tcp_rtt'}],
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    'address': '10.0.0.1',
                    'username': 'admin',
                    'credentialName': 'my-key'
                }]
            },
            'settings': {
                'sysfs': {
                    'cpu_governor': {
                        'constraints': [{'values': ['performance']}]  # Only 1 value
                    }
                }
            }
        }

        with pytest.raises(ValueError) as exc_info:
            BreederConfig.validate_minimal(config)

        error_msg = str(exc_info.value)
        assert "values" in error_msg.lower()
        assert "at least 2" in error_msg.lower()


class TestEmptyStringValidation:
    """Test empty/whitespace string validation"""

    def test_empty_parameter_name_fails(self):
        """Test that empty parameter name fails validation"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'tcp_rtt'}],
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    'address': '10.0.0.1',
                    'username': 'admin',
                    'credentialName': 'my-key'
                }]
            },
            'settings': {
                'sysctl': {
                    '': {  # Empty parameter name
                        'constraints': [{'step': 1, 'lower': 0, 'upper': 100}]
                    }
                }
            }
        }

        with pytest.raises(ValueError) as exc_info:
            BreederConfig.validate_minimal(config)

        error_msg = str(exc_info.value)
        assert "parameter name cannot be empty" in error_msg.lower()

    def test_whitespace_parameter_name_fails(self):
        """Test that whitespace-only parameter name fails validation"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'tcp_rtt'}],
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    'address': '10.0.0.1',
                    'username': 'admin',
                    'credentialName': 'my-key'
                }]
            },
            'settings': {
                'sysctl': {
                    '   ': {  # Whitespace parameter name
                        'constraints': [{'step': 1, 'lower': 0, 'upper': 100}]
                    }
                }
            }
        }

        with pytest.raises(ValueError) as exc_info:
            BreederConfig.validate_minimal(config)

        error_msg = str(exc_info.value)
        assert "parameter name cannot be empty" in error_msg.lower() or "whitespace" in error_msg.lower()

    def test_empty_target_address_fails(self):
        """Test that empty target address fails validation"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'tcp_rtt'}],
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    'address': '',  # Empty address
                    'username': 'admin',
                    'credentialName': 'my-key'
                }]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': [{'step': 1, 'lower': 0, 'upper': 100}]
                    }
                }
            }
        }

        with pytest.raises(ValueError) as exc_info:
            BreederConfig.validate_minimal(config)

        error_msg = str(exc_info.value)
        assert "address" in error_msg.lower()
        assert "non-empty" in error_msg.lower() or "empty string" in error_msg.lower()


class TestSSHTargetValidation:
    """Test SSH target field validation"""

    def test_missing_ssh_address_fails(self):
        """Test that missing SSH address fails validation"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'tcp_rtt'}],
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    'username': 'admin',
                    'credentialName': 'my-key'
                    # Missing 'address'
                }]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': [{'step': 1, 'lower': 0, 'upper': 100}]
                    }
                }
            }
        }

        with pytest.raises(ValueError) as exc_info:
            BreederConfig.validate_minimal(config)

        error_msg = str(exc_info.value)
        assert "address" in error_msg.lower()
        assert "missing" in error_msg.lower()

    def test_missing_ssh_username_fails(self):
        """Test that missing SSH username fails validation"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'tcp_rtt'}],
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    'address': '10.0.0.1',
                    'credentialName': 'my-key'
                    # Missing 'username'
                }]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': [{'step': 1, 'lower': 0, 'upper': 100}]
                    }
                }
            }
        }

        with pytest.raises(ValueError) as exc_info:
            BreederConfig.validate_minimal(config)

        error_msg = str(exc_info.value)
        assert "username" in error_msg.lower()
        assert "missing" in error_msg.lower()

    def test_missing_credential_fails(self):
        """Test that missing both credentialName and credentialId fails validation"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'tcp_rtt'}],
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    'address': '10.0.0.1',
                    'username': 'admin'
                    # Missing both credentialName and credentialId
                }]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': [{'step': 1, 'lower': 0, 'upper': 100}]
                    }
                }
            }
        }

        with pytest.raises(ValueError) as exc_info:
            BreederConfig.validate_minimal(config)

        error_msg = str(exc_info.value)
        assert "credential" in error_msg.lower()
        assert "either" in error_msg.lower() or "requires" in error_msg.lower()


class TestObjectiveReconnaissanceValidation:
    """Test objective reconnaissance validation"""

    def test_missing_reconnaissance_service_fails(self):
        """Test that missing reconnaissance service fails validation"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{
                'name': 'latency',
                'goal': 'MINIMIZE',
                'reconnaissance': {
                    'query': 'rate(http_request_duration_seconds_sum[5m])'
                    # Missing 'service'
                }
            }],
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    'address': '10.0.0.1',
                    'username': 'admin',
                    'credentialName': 'my-key'
                }]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': [{'step': 1, 'lower': 0, 'upper': 100}]
                    }
                }
            }
        }

        with pytest.raises(ValueError) as exc_info:
            BreederConfig.validate_minimal(config)

        error_msg = str(exc_info.value)
        assert "reconnaissance" in error_msg.lower()
        assert "missing" in error_msg.lower()
        assert "service" in error_msg.lower()

    def test_empty_reconnaissance_query_fails(self):
        """Test that empty reconnaissance query fails validation"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{
                'name': 'latency',
                'goal': 'MINIMIZE',
                'reconnaissance': {
                    'service': 'prometheus',
                    'query': ''  # Empty query
                }
            }],
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    'address': '10.0.0.1',
                    'username': 'admin',
                    'credentialName': 'my-key'
                }]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': [{'step': 1, 'lower': 0, 'upper': 100}]
                    }
                }
            }
        }

        with pytest.raises(ValueError) as exc_info:
            BreederConfig.validate_minimal(config)

        error_msg = str(exc_info.value)
        assert "query" in error_msg.lower()
        assert "non-empty" in error_msg.lower() or "empty" in error_msg.lower()

    def test_samples_less_than_one_fails(self):
        """Test that samples < 1 fails validation"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{
                'name': 'latency',
                'goal': 'MINIMIZE',
                'reconnaissance': {
                    'service': 'prometheus',
                    'query': 'rate(http_request_duration_seconds_sum[5m])',
                    'samples': 0  # Invalid
                }
            }],
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    'address': '10.0.0.1',
                    'username': 'admin',
                    'credentialName': 'my-key'
                }]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': [{'step': 1, 'lower': 0, 'upper': 100}]
                    }
                }
            }
        }

        with pytest.raises(ValueError) as exc_info:
            BreederConfig.validate_minimal(config)

        error_msg = str(exc_info.value)
        assert "samples" in error_msg.lower()
        assert ">= 1" in error_msg

    def test_negative_stabilization_seconds_fails(self):
        """Test that negative stabilization_seconds fails validation"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{
                'name': 'latency',
                'goal': 'MINIMIZE',
                'reconnaissance': {
                    'service': 'prometheus',
                    'query': 'rate(http_request_duration_seconds_sum[5m])',
                    'stabilization_seconds': -10  # Invalid
                }
            }],
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    'address': '10.0.0.1',
                    'username': 'admin',
                    'credentialName': 'my-key'
                }]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': [{'step': 1, 'lower': 0, 'upper': 100}]
                    }
                }
            }
        }

        with pytest.raises(ValueError) as exc_info:
            BreederConfig.validate_minimal(config)

        error_msg = str(exc_info.value)
        assert "stabilization_seconds" in error_msg.lower()
        assert "non-negative" in error_msg.lower()


class TestRunCompletionValidation:
    """Test run completion criteria validation"""

    def test_iterations_min_greater_than_max_fails(self):
        """Test that iterations.min > iterations.max fails validation"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'tcp_rtt'}],
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    'address': '10.0.0.1',
                    'username': 'admin',
                    'credentialName': 'my-key'
                }]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': [{'step': 1, 'lower': 0, 'upper': 100}]
                    }
                }
            },
            'run': {
                'completion': {
                    'iterations': {
                        'min': 100,  # Invalid: > max
                        'max': 50
                    }
                }
            }
        }

        with pytest.raises(ValueError) as exc_info:
            BreederConfig.validate_minimal(config)

        error_msg = str(exc_info.value)
        assert "min" in error_msg.lower()
        assert "max" in error_msg.lower()
        assert "must be <=" in error_msg.lower()


class TestGuardrailsValidation:
    """Test guardrails validation """

    def test_valid_guardrails_passes(self):
        """Test that valid guardrails configuration passes"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'latency', 'goal': 'MINIMIZE'}],
            'guardrails': [{
                'name': 'cpu_usage',
                'hard_limit': 90.0,
                'reconnaissance': {
                    'service': 'prometheus',
                    'query': 'rate(process_cpu_seconds_total[5m]) * 100'
                }
            }],
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    'address': '10.0.0.1',
                    'username': 'admin',
                    'credentialName': 'my-key'
                }]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': [{'step': 1, 'lower': 0, 'upper': 100}]
                    }
                }
            }
        }

        result = BreederConfig.validate_minimal(config)
        assert result["success"] is True

    def test_guardrail_missing_hard_limit_fails(self):
        """Test that missing hard_limit fails validation"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'latency', 'goal': 'MINIMIZE'}],
            'guardrails': [{
                'name': 'cpu_usage'
                # Missing 'hard_limit'
            }],
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    'address': '10.0.0.1',
                    'username': 'admin',
                    'credentialName': 'my-key'
                }]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': [{'step': 1, 'lower': 0, 'upper': 100}]
                    }
                }
            }
        }

        with pytest.raises(ValueError) as exc_info:
            BreederConfig.validate_minimal(config)

        error_msg = str(exc_info.value)
        assert "hard_limit" in error_msg
        assert "missing" in error_msg.lower()


class TestRollbackStrategiesValidation:
    """Test rollback strategies validation """

    def test_valid_rollback_strategies_passes(self):
        """Test that valid rollback strategies configuration passes"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'latency', 'goal': 'MINIMIZE'}],
            'rollback_strategies': {
                'standard': {
                    'consecutive_failures': 3,
                    'target_state': 'previous',
                    'max_attempts': 3,
                    'on_failure': 'stop',
                    'timeout_seconds': 300,
                    'after': {
                        'action': 'pause',
                        'duration': 60
                    }
                }
            },
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    'address': '10.0.0.1',
                    'username': 'admin',
                    'credentialName': 'my-key',
                    'rollback': {
                        'enabled': True,
                        'strategy': 'standard'
                    }
                }]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': [{'step': 1, 'lower': 0, 'upper': 100}]
                    }
                }
            }
        }

        result = BreederConfig.validate_minimal(config)
        assert result["success"] is True

    def test_undefined_strategy_reference_fails(self):
        """Test that referencing undefined rollback strategy fails validation"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [{'name': 'latency', 'goal': 'MINIMIZE'}],
            'rollback_strategies': {
                'standard': {
                    'consecutive_failures': 3,
                    'target_state': 'previous',
                    'max_attempts': 3,
                    'on_failure': 'stop',
                    'timeout_seconds': 300,
                    'after': {
                        'action': 'pause',
                        'duration': 60
                    }
                }
            },
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    'address': '10.0.0.1',
                    'username': 'admin',
                    'credentialName': 'my-key',
                    'rollback': {
                        'enabled': True,
                        'strategy': 'aggressive'  # Undefined strategy
                    }
                }]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': [{'step': 1, 'lower': 0, 'upper': 100}]
                    }
                }
            }
        }

        with pytest.raises(ValueError) as exc_info:
            BreederConfig.validate_minimal(config)

        error_msg = str(exc_info.value)
        assert "undefined" in error_msg.lower()
        assert "aggressive" in error_msg
        assert "Available strategies" in error_msg or "standard" in error_msg


class TestMultipleErrorReporting:
    """Test aggregated error reporting with multiple errors"""

    def test_multiple_errors_reported_together(self):
        """Test that multiple validation errors are reported in a single message"""
        config = {
            'meta': {'configVersion': '0.3'},
            'breeder': {'type': 'linux_performance'},
            'objectives': [],  # Error 1: empty objectives
            'effectuation': {
                'targets': [{
                    'type': 'ssh',
                    # Missing address (Error 2)
                    'username': '',  # Error 3: empty username
                    # Missing credential (Error 4)
                }]
            },
            'settings': {
                'sysctl': {
                    'vm.swappiness': {
                        'constraints': [{'step': 1, 'lower': 100, 'upper': 0}]  # Error 5: lower > upper
                    }
                }
            }
        }

        with pytest.raises(ValueError) as exc_info:
            BreederConfig.validate_minimal(config)

        error_msg = str(exc_info.value)
        # Check that errors are numbered
        assert "[1/" in error_msg or "[2/" in error_msg or "[3/" in error_msg
        # Check that multiple errors are present
        assert "objectives" in error_msg.lower()
        assert ("address" in error_msg.lower() or "username" in error_msg.lower())

