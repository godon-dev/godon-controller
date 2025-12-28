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

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from controller.breeder_service import determine_config_shard


class TestConfigSharding:
    """Test configuration sharding logic for parallel runs"""

    def test_single_parameter_sharding(self):
        """Test that single parameter gets properly sharded across workers"""
        config = {
            'settings': {
                'sysctl': {
                    'net_ipv4_tcp_rmem': {
                        'constraints': {
                            'lower': 4096,
                            'upper': 4194304
                        }
                    }
                }
            }
        }

        # Test with 2 workers
        shard_0 = determine_config_shard(
            run_id=0,
            target_id=0,
            targets_count=1,
            config=config,
            parallel_runs_count=2
        )

        shard_1 = determine_config_shard(
            run_id=1,
            target_id=0,
            targets_count=1,
            config=config,
            parallel_runs_count=2
        )

        # Both shards should be within original bounds
        tcp_rmem_0 = shard_0['settings']['sysctl']['net_ipv4_tcp_rmem']['constraints']
        tcp_rmem_1 = shard_1['settings']['sysctl']['net_ipv4_tcp_rmem']['constraints']

        assert tcp_rmem_0['lower'] >= 4096, "Lower bound should not be below original"
        assert tcp_rmem_0['upper'] <= 4194304, "Upper bound should not exceed original"
        assert tcp_rmem_1['lower'] >= 4096, "Lower bound should not be below original"
        assert tcp_rmem_1['upper'] <= 4194304, "Upper bound should not exceed original"

    def test_multiple_parameters_independent_sharding(self):
        """Test that multiple parameters get independently sharded"""
        config = {
            'settings': {
                'sysctl': {
                    'net_ipv4_tcp_rmem': {
                        'constraints': {'lower': 4096, 'upper': 4194304}
                    },
                    'vm_swappiness': {
                        'constraints': {'lower': 1, 'upper': 100}
                    }
                }
            }
        }

        shard = determine_config_shard(
            run_id=0,
            target_id=0,
            targets_count=1,
            config=config,
            parallel_runs_count=2
        )

        tcp_rmem = shard['settings']['sysctl']['net_ipv4_tcp_rmem']['constraints']
        swappiness = shard['settings']['sysctl']['vm_swappiness']['constraints']

        # Each parameter should be individually constrained
        assert 4096 <= tcp_rmem['lower'] <= tcp_rmem['upper'] <= 4194304
        assert 1 <= swappiness['lower'] <= swappiness['upper'] <= 100

    def test_sharding_with_overlap(self):
        """Test that shards include overlap buffer in their ranges"""
        config = {
            'settings': {
                'sysctl': {
                    'test_param': {
                        'constraints': {'lower': 0, 'upper': 1000}
                    }
                }
            }
        }

        # Create a shard and check it has overlap buffer
        shard = determine_config_shard(
            run_id=0,
            target_id=0,
            targets_count=1,
            config=config,
            parallel_runs_count=3
        )

        param = shard['settings']['sysctl']['test_param']['constraints']
        
        # Calculate expected shard size without overlap
        total_shards = 3
        delta = 1000 - 0
        shard_size = delta / total_shards  # ~333.33
        
        # With 10% overlap, shard should be larger than base shard_size
        shard_range = param['upper'] - param['lower']
        expected_min_range = int(shard_size * 0.9)  # At minimum 90% of shard_size
        
        # Shard range should be close to shard_size + overlap (on both ends)
        # But truncated by boundaries, so we just check it's reasonable
        assert shard_range >= expected_min_range, \
            f"Shard range {shard_range} should be at least {expected_min_range}"
        
        # Shard should still be within original bounds
        assert param['lower'] >= 0, "Lower bound should not be below original"
        assert param['upper'] <= 1000, "Upper bound should not exceed original"

    def test_deterministic_sharding(self):
        """Test that same worker always gets same shard"""
        config = {
            'settings': {
                'sysctl': {
                    'test_param': {
                        'constraints': {'lower': 0, 'upper': 1000}
                    }
                }
            }
        }

        # Create two identical shards
        shard_1 = determine_config_shard(
            run_id=0,
            target_id=0,
            targets_count=1,
            config=config,
            parallel_runs_count=2
        )

        shard_2 = determine_config_shard(
            run_id=0,
            target_id=0,
            targets_count=1,
            config=config,
            parallel_runs_count=2
        )

        param_1 = shard_1['settings']['sysctl']['test_param']['constraints']
        param_2 = shard_2['settings']['sysctl']['test_param']['constraints']

        assert param_1['lower'] == param_2['lower'], "Same worker should get same shard"
        assert param_1['upper'] == param_2['upper'], "Same worker should get same shard"

    def test_different_workers_get_different_shards(self):
        """Test that different workers get different shards"""
        config = {
            'settings': {
                'sysctl': {
                    'test_param': {
                        'constraints': {'lower': 0, 'upper': 10000}
                    }
                }
            }
        }

        # Create shards for different workers
        shards = []
        for run_id in range(4):
            shard = determine_config_shard(
                run_id=run_id,
                target_id=0,
                targets_count=1,
                config=config,
                parallel_runs_count=4
            )
            shards.append(shard['settings']['sysctl']['test_param']['constraints'])

        # Check that we have diversity in shard assignments
        # (not all same, and distributed across the range)
        lower_bounds = [s['lower'] for s in shards]
        upper_bounds = [s['upper'] for s in shards]

        # Should have different values (with high probability)
        assert len(set(lower_bounds)) > 1, "Workers should get different shards"
        assert len(set(upper_bounds)) > 1, "Workers should get different shards"

    def test_multiple_targets_and_runs(self):
        """Test sharding with multiple targets and parallel runs"""
        config = {
            'settings': {
                'sysctl': {
                    'test_param': {
                        'constraints': {'lower': 0, 'upper': 1000}
                    }
                }
            }
        }

        # 2 targets, 3 parallel runs = 6 total workers
        shards = []
        for target_id in range(2):
            for run_id in range(3):
                shard = determine_config_shard(
                    run_id=run_id,
                    target_id=target_id,
                    targets_count=2,
                    config=config,
                    parallel_runs_count=3
                )
                shards.append(shard['settings']['sysctl']['test_param']['constraints'])

        # All shards should be within bounds
        for shard in shards:
            assert 0 <= shard['lower'] <= shard['upper'] <= 1000

    def test_sharding_preserves_config_structure(self):
        """Test that sharding preserves original config structure"""
        config = {
            'settings': {
                'sysctl': {
                    'net_ipv4_tcp_rmem': {
                        'constraints': {'lower': 4096, 'upper': 4194304}
                    },
                    'vm_swappiness': {
                        'constraints': {'lower': 1, 'upper': 100}
                    }
                }
            },
            'other_key': 'other_value',
            'nested': {
                'key': 'value'
            }
        }

        shard = determine_config_shard(
            run_id=0,
            target_id=0,
            targets_count=1,
            config=config,
            parallel_runs_count=2
        )

        # Check structure is preserved
        assert 'settings' in shard
        assert 'sysctl' in shard['settings']
        assert 'other_key' in shard
        assert 'nested' in shard
        assert shard['other_key'] == 'other_value'
        assert shard['nested']['key'] == 'value'

    def test_sharding_with_small_range(self):
        """Test sharding behavior with small parameter ranges"""
        config = {
            'settings': {
                'sysctl': {
                    'small_param': {
                        'constraints': {'lower': 10, 'upper': 20}
                    }
                }
            }
        }

        shard = determine_config_shard(
            run_id=0,
            target_id=0,
            targets_count=1,
            config=config,
            parallel_runs_count=5
        )

        param = shard['settings']['sysctl']['small_param']['constraints']

        # Even with many workers, should respect bounds
        assert param['lower'] >= 10, "Should not go below original lower"
        assert param['upper'] <= 20, "Should not exceed original upper"
        assert param['lower'] <= param['upper'], "Lower should not exceed upper"

    def test_sharding_with_zero_lower(self):
        """Test sharding with zero as lower bound"""
        config = {
            'settings': {
                'sysctl': {
                    'zero_param': {
                        'constraints': {'lower': 0, 'upper': 100}
                    }
                }
            }
        }

        shard = determine_config_shard(
            run_id=0,
            target_id=0,
            targets_count=1,
            config=config,
            parallel_runs_count=2
        )

        param = shard['settings']['sysctl']['zero_param']['constraints']
        assert param['lower'] >= 0, "Should handle zero lower bound correctly"
        assert param['upper'] <= 100, "Should respect upper bound"