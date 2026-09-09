"""
Tests for controller watermark slot assignment.
"""
import pytest
import sys
import types
from unittest.mock import MagicMock, patch

# Mock wmill before importing
sys.modules['wmill'] = types.ModuleType('wmill')
sys.modules['wmill'].Windmill = MagicMock
sys.modules['wmill'].run_script_by_path_async = MagicMock(return_value='job-123')

db_mock = types.ModuleType('f.controller.database')
db_mock.ArchiveDatabaseRepository = MagicMock
db_mock.MetadataDatabaseRepository = MagicMock
db_mock.execute_query = MagicMock
db_mock.get_db_connection = MagicMock
sys.modules['f.controller.database'] = db_mock

otel_mock = types.ModuleType('f.controller.shared.otel_logging')
otel_mock.get_logger = lambda name: type('Logger', (), {
    'info': lambda *a, **kw: None,
    'warning': lambda *a, **kw: None,
    'error': lambda *a, **kw: None,
    'debug': lambda *a, **kw: None,
})()
sys.modules['f.controller.shared.otel_logging'] = otel_mock

config_mock = types.ModuleType('f.controller.config')
config_mock.DatabaseConfig = type('DatabaseConfig', (), {
    'ARCHIVE_DB': {},
    'META_DB': {},
})
config_mock.SystemtenderConfig = type('SystemtenderConfig', (), {
    'validate_minimal': staticmethod(lambda x: None),
})
config_mock.SYSTEMTENDER_CAPABILITIES = {}
sys.modules['f.controller.config'] = config_mock

from controller.systemtender_service import SystemtenderService


def _make_service(systemtender_configs_by_id):
    """Create a SystemtenderService with mocked repositories.
    
    Args:
        systemtender_configs_by_id: dict of {systemtender_id: config_dict}
    """
    service = SystemtenderService(archive_db_config={}, meta_db_config={})
    service.metadata_repo = MagicMock()
    
    # fetch_systemtenders_list returns (id, name, creation_tsz) tuples
    systemtender_list = [
        (bid, f'systemtender-{bid}', None)
        for bid in systemtender_configs_by_id
    ]
    service.metadata_repo.fetch_systemtenders_list.return_value = systemtender_list
    
    # fetch_meta_data returns [[id, name, ts, config]] per systemtender
    def mock_fetch(systemtender_id):
        if systemtender_id in systemtender_configs_by_id:
            return [[systemtender_id, f'systemtender-{systemtender_id}', None, systemtender_configs_by_id[systemtender_id]]]
        return None
    
    service.metadata_repo.fetch_meta_data.side_effect = mock_fetch
    
    return service


class TestSlotAssignment:
    """Collision-free slot assignment."""

    def test_first_systemtender_gets_slot_0(self):
        service = _make_service({})
        config = {'systemtender': {'type': 'test'}}
        service._assign_watermark_slot(config)
        assert config['systemtender']['watermark_slot'] == 0

    def test_second_systemtender_gets_slot_1(self):
        existing = {
            'id-1': {'systemtender': {'watermark_slot': 0}},
        }
        service = _make_service(existing)
        config = {'systemtender': {'type': 'test'}}
        service._assign_watermark_slot(config)
        assert config['systemtender']['watermark_slot'] == 1

    def test_fills_gap_when_slot_freed(self):
        existing = {
            'id-1': {'systemtender': {'watermark_slot': 0}},
            'id-2': {'systemtender': {'watermark_slot': 2}},
            'id-3': {'systemtender': {'watermark_slot': 3}},
        }
        service = _make_service(existing)
        config = {'systemtender': {'type': 'test'}}
        service._assign_watermark_slot(config)
        assert config['systemtender']['watermark_slot'] == 1

    def test_all_six_slots_filled(self):
        existing = {
            'id-1': {'systemtender': {'watermark_slot': 0}},
            'id-2': {'systemtender': {'watermark_slot': 1}},
            'id-3': {'systemtender': {'watermark_slot': 2}},
            'id-4': {'systemtender': {'watermark_slot': 3}},
            'id-5': {'systemtender': {'watermark_slot': 4}},
            'id-6': {'systemtender': {'watermark_slot': 5}},
        }
        service = _make_service(existing)
        config = {'systemtender': {'type': 'test'}}
        service._assign_watermark_slot(config)
        # All 6 slots used — should log warning and not assign
        assert 'watermark_slot' not in config['systemtender']

    def test_no_collision_across_six_systemtenders(self):
        """Simulate creating 6 systemtenders sequentially."""
        used_slots = {}
        
        for i in range(6):
            existing = {
                f'id-{j}': {'systemtender': {'watermark_slot': slot}}
                for j, slot in used_slots.items()
            }
            service = _make_service(existing)
            config = {'systemtender': {'type': 'test'}}
            service._assign_watermark_slot(config)
            slot = config['systemtender']['watermark_slot']
            assert slot not in used_slots.values(), f"Slot {slot} assigned twice"
            used_slots[i] = slot
        
        assert set(used_slots.values()) == {0, 1, 2, 3, 4, 5}

    def test_systemtender_without_slot_is_skipped(self):
        """A systemtender with no watermark_slot in config should not occupy a slot."""
        existing = {
            'id-1': {'systemtender': {'watermark_slot': 0}},
            'id-2': {'systemtender': {}},  # no slot — legacy systemtender
        }
        service = _make_service(existing)
        config = {'systemtender': {'type': 'test'}}
        service._assign_watermark_slot(config)
        assert config['systemtender']['watermark_slot'] == 1
