"""
Tests for controller watermark slot assignment.

Verifies that:
- Slots are assigned collision-free across breeders
- Lowest available slot is always chosen
- Slot 0 is assigned when no breeders exist
- All 6 slots can be filled
- 7th breeder falls back gracefully (all slots used)
"""
import pytest
import sys
import types
from unittest.mock import MagicMock, patch

# Mock wmill before importing the module
sys.modules['wmill'] = types.ModuleType('wmill')
sys.modules['wmill'].Windmill = MagicMock
sys.modules['wmill'].run_script_by_path_async = MagicMock(return_value='job-123')

# Mock database modules
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
config_mock.BreederConfig = type('BreederConfig', (), {
    'validate_minimal': staticmethod(lambda x: None),
})
config_mock.BREEDER_CAPABILITIES = {}
sys.modules['f.controller.config'] = config_mock

# Now import
from controller.breeder_service import BreederService


def _make_service(existing_breeders=None):
    """Create a BreederService with mocked repositories."""
    service = BreederService(archive_db_config={}, meta_db_config={})
    service.metadata_repo = MagicMock()
    
    if existing_breeders is None:
        service.metadata_repo.fetch_breeders_list.return_value = []
    else:
        service.metadata_repo.fetch_breeders_list.return_value = existing_breeders
        # Each breeder: fetch_meta_data returns a list of tuples
        for breeder in existing_breeders:
            breeder_id = breeder[0]
            config = breeder[3] if len(breeder) > 3 else {}
            service.metadata_repo.fetch_meta_data.return_value = [[breeder_id, 'test', None, config]]
    
    return service


class TestSlotAssignment:
    """Collision-free slot assignment."""

    def test_first_breeder_gets_slot_0(self):
        service = _make_service(existing_breeders=[])
        config = {'breeder': {'type': 'test'}}
        service._assign_watermark_slot(config)
        assert config['breeder']['watermark_slot'] == 0

    def test_second_breeder_gets_slot_1(self):
        existing = [
            ('id-1', 'breeder-1', None, {'breeder': {'watermark_slot': 0}}),
        ]
        service = _make_service(existing_breeders=existing)
        config = {'breeder': {'type': 'test'}}
        service._assign_watermark_slot(config)
        assert config['breeder']['watermark_slot'] == 1

    def test_fills_gap_when_slot_freed(self):
        existing = [
            ('id-1', 'breeder-1', None, {'breeder': {'watermark_slot': 0}}),
            ('id-2', 'breeder-2', None, {'breeder': {'watermark_slot': 2}}),
            ('id-3', 'breeder-3', None, {'breeder': {'watermark_slot': 3}}),
        ]
        service = _make_service(existing_breeders=existing)
        config = {'breeder': {'type': 'test'}}
        service._assign_watermark_slot(config)
        assert config['breeder']['watermark_slot'] == 1

    def test_all_six_slots_filled(self):
        existing = [
            ('id-1', 'b1', None, {'breeder': {'watermark_slot': 0}}),
            ('id-2', 'b2', None, {'breeder': {'watermark_slot': 1}}),
            ('id-3', 'b3', None, {'breeder': {'watermark_slot': 2}}),
            ('id-4', 'b4', None, {'breeder': {'watermark_slot': 3}}),
            ('id-5', 'b5', None, {'breeder': {'watermark_slot': 4}}),
            ('id-6', 'b6', None, {'breeder': {'watermark_slot': 5}}),
        ]
        service = _make_service(existing_breeders=existing)
        config = {'breeder': {'type': 'test'}}
        service._assign_watermark_slot(config)
        # All slots used — should log warning and not assign
        assert 'watermark_slot' not in config['breeder']

    def test_no_collision_across_six_breeders(self):
        """Simulate creating 6 breeders sequentially."""
        service = _make_service(existing_breeders=[])
        used_slots = set()
        
        for i in range(6):
            # Update mock to reflect current state
            existing = [
                (f'id-{j}', f'b{j}', None, {'breeder': {'watermark_slot': slot}})
                for j, slot in enumerate(used_slots)
            ]
            service = _make_service(existing_breeders=existing)
            config = {'breeder': {'type': 'test'}}
            service._assign_watermark_slot(config)
            slot = config['breeder']['watermark_slot']
            assert slot not in used_slots, f"Slot {slot} assigned twice"
            used_slots.add(slot)
        
        assert used_slots == {0, 1, 2, 3, 4, 5}
