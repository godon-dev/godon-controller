import uuid
import hashlib
import datetime
import copy
import logging
from dateutil.parser import parse

from database import ArchiveDatabaseRepository, MetadataDatabaseRepository

logger = logging.getLogger(__name__)

def determine_config_shard(run_id, target_id, targets_count, config, parallel_runs_count):
    """Determine configuration shard for parallel runs using hash-based assignment with overlap
    
    Uses hash-based deterministic assignment to distribute parameter space across workers,
    with 10% overlap between shards to avoid boundary blind spots.
    """
    config_result = copy.deepcopy(config)
    settings_space = config_result.get('settings', {}).get('sysctl', {})

    for setting_key, setting_value in settings_space.items():
        if not isinstance(setting_value, dict):
            continue

        constraints = setting_value.get('constraints', {})
        upper = constraints.get('upper')
        lower = constraints.get('lower')

        if upper is None or lower is None:
            continue

        # Hash-based worker assignment for even distribution
        worker_id = f"{run_id}_{target_id}_{setting_key}"
        worker_hash = int(hashlib.sha256(worker_id.encode()).hexdigest(), 16)
        
        total_shards = targets_count * parallel_runs_count
        shard_index = worker_hash % total_shards
        
        # Calculate shard boundaries
        delta = abs(upper - lower)
        shard_size = delta / total_shards
        
        # Add overlap to avoid boundary blind spots
        overlap_percent = 0.10
        overlap = int(shard_size * overlap_percent)
        
        # Calculate shard boundaries with overlap
        new_lower = int(lower + shard_size * shard_index)
        new_upper = int(lower + shard_size * (shard_index + 1))
        
        # Respect original boundaries
        new_lower = max(lower, new_lower - overlap)
        new_upper = min(upper, new_upper + overlap)
        
        setting_value['constraints']['lower'] = new_lower
        setting_value['constraints']['upper'] = new_upper

    config_result['settings']['sysctl'] = settings_space
    return config_result

def start_optimization_flow(flow_id, config):
    """Start an optimization flow (placeholder for actual flow logic)"""
    logger.info(f"Starting optimization flow {flow_id} with config")
    logger.debug(f"Config: {config}")
    return flow_id, config

class BreederService:
    """Service for managing breeder lifecycle operations"""

    def __init__(self, archive_db_config, meta_db_config):
        self.archive_repo = ArchiveDatabaseRepository(archive_db_config)
        self.metadata_repo = MetadataDatabaseRepository(meta_db_config)

    def create_breeder(self, breeder_config):
        """Create a new breeder instance"""
        try:
            breeder_name = breeder_config.get('name', 'unnamed_breeder')
            parallel_runs = breeder_config.get('run', {}).get('parallel', 1)
            targets = breeder_config.get('effectuation', {}).get('targets', [])
            targets_count = len(targets)
            is_cooperative = breeder_config.get('cooperation', {}).get('active', False)

            breeder_uuid = str(uuid.uuid4())
            breeder_config['uuid'] = breeder_uuid

            __uuid_common_name = f"breeder_{breeder_uuid.replace('-', '_')}"
            breeder_id = f'{__uuid_common_name}'

            self.archive_repo.create_database(breeder_id)

            self.metadata_repo.create_table()
            self.metadata_repo.insert_breeder_meta(
                breeder_id=breeder_uuid,
                creation_ts=datetime.datetime.now(),
                meta_state=breeder_config
            )

            target_count = 0
            for target in targets:
                hash_suffix = hashlib.sha256(str.encode(target.get('address', ''))).hexdigest()[0:6]

                for run_id in range(parallel_runs):
                    flow_config = breeder_config
                    flow_id = f'{breeder_name}_{run_id}'

                    if not is_cooperative:
                        flow_config = determine_config_shard(
                            run_id=run_id,
                            target_id=target_count,
                            targets_count=targets_count,
                            config=breeder_config,
                            parallel_runs_count=parallel_runs
                        )

                    start_optimization_flow(flow_id, flow_config)

                target_count += 1

            logger.info(f"Successfully created breeder: {breeder_uuid}")
            return {"result": "SUCCESS", "breeder_id": breeder_uuid}

        except Exception as e:
            logger.error(f"Failed to create breeder: {e}")
            return {"result": "FAILURE", "error": str(e)}

    def get_breeder(self, breeder_id):
        """Get breeder information"""
        try:
            import json

            self.metadata_repo.create_table()
            breeder_meta_data_row = self.metadata_repo.fetch_meta_data(breeder_id)

            if breeder_meta_data_row and len(breeder_meta_data_row) > 0:
                breeder_data = json.dumps({
                    "creation_timestamp": breeder_meta_data_row[0][1].isoformat(),
                    "breeder_definition": breeder_meta_data_row[0][2]
                })
                result = "SUCCESS"
            else:
                breeder_data = json.dumps({})
                result = "FAILURE"

            return {
                "result": result,
                "breeder_data": breeder_data
            }

        except Exception as e:
            logger.error(f"Failed to get breeder {breeder_id}: {e}")
            return {
                "result": "FAILURE",
                "breeder_data": json.dumps({}),
                "error": str(e)
            }

    def delete_breeder(self, breeder_id):
        """Delete a breeder instance"""
        try:
            __uuid_common_name = f"_{breeder_id.replace('-', '_')}"

            self.archive_repo.drop_database(__uuid_common_name)

            self.metadata_repo.create_table()
            self.metadata_repo.remove_breeder_meta(breeder_id)

            logger.info(f"Successfully deleted breeder: {breeder_id}")
            return {"result": "SUCCESS"}

        except Exception as e:
            logger.error(f"Failed to delete breeder {breeder_id}: {e}")
            return {"result": "FAILURE", "error": str(e)}

    def list_breeders(self):
        """List all breeders"""
        try:
            self.metadata_repo.create_table()
            breeder_meta_data_list = self.metadata_repo.fetch_breeders_list()

            if breeder_meta_data_list:
                configured_breeders = [
                    (row[0], row[1], parse(str(row[2])).isoformat())
                    for row in breeder_meta_data_list
                ]
            else:
                configured_breeders = []

            return {
                "result": "SUCCESS",
                "breeders": configured_breeders
            }

        except Exception as e:
            logger.error(f"Failed to list breeders: {e}")
            return {
                "result": "FAILURE",
                "breeders": [],
                "error": str(e)
            }