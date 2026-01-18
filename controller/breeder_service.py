import uuid
import hashlib
import datetime
import copy
import logging
from dateutil.parser import parse

from f.controller.database import ArchiveDatabaseRepository, MetadataDatabaseRepository
from f.controller.config import BreederConfig, BREEDER_CAPABILITIES

# Import wmill at top level so Windmill can detect it for dependency resolution
import wmill

logger = logging.getLogger(__name__)

def determine_config_shard(run_id, target_id, targets_count, config, parallel_runs_count):
    """Determine configuration shard for parallel runs using hash-based assignment with overlap

    Uses hash-based deterministic assignment to distribute parameter space across workers,
    with 10% overlap between shards to avoid boundary blind spots.

    v0.3 updates:
    - Support multiple settings categories (sysctl, sysfs, cpufreq, ethtool)
    - Support list of constraints (multiple disjoint ranges)
    - Only shard integer ranges (not categorical values)
    """
    config_result = copy.deepcopy(config)

    # Support multiple settings categories
    supported_categories = ['sysctl', 'sysfs', 'cpufreq', 'ethtool']
    settings = config_result.get('settings', {})

    for category in supported_categories:
        if category not in settings:
            continue

        settings_space = settings[category]

        for setting_key, setting_value in settings_space.items():
            if not isinstance(setting_value, dict):
                continue

            # constraints is now a list, not a dict
            constraints_list = setting_value.get('constraints', [])

            if not isinstance(constraints_list, list):
                # Skip invalid constraint structures (validation will catch them)
                continue

            if len(constraints_list) == 0:
                continue

            # Check if first constraint is categorical (has 'values')
            # Categorical parameters are not shardable
            first_constraint = constraints_list[0]
            if 'values' in first_constraint:
                # Categorical parameter - skip sharding
                continue

            # Shard each integer range constraint
            for constraint_idx, constraint in enumerate(constraints_list):
                # Only shard integer ranges (has step, lower, upper)
                if 'step' not in constraint or 'lower' not in constraint or 'upper' not in constraint:
                    continue

                lower = constraint['lower']
                upper = constraint['upper']
                step = constraint['step']

                if not isinstance(lower, (int, float)) or not isinstance(upper, (int, float)):
                    continue

                # Hash-based worker assignment for even distribution
                worker_id = f"{run_id}_{target_id}_{category}_{setting_key}_{constraint_idx}"
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
                new_lower = lower + shard_size * shard_index
                new_upper = lower + shard_size * (shard_index + 1)

                # Respect original boundaries
                new_lower = max(lower, new_lower - overlap)
                new_upper = min(upper, new_upper + overlap)

                # Update the constraint in place
                constraint['lower'] = new_lower
                constraint['upper'] = new_upper

    return config_result

def start_optimization_flow(flow_id, shard_config, run_id, target_id, breeder_id):
    """Start breeder worker optimization flow in Windmill

    Launches a breeder worker flow with its sharded configuration.
    The full breeder config is stored in meta DB; workers receive their
    specific shard (either full config for cooperative mode, or sharded
    parameter ranges for non-cooperative mode).

    Args:
        flow_id: Identifier for this flow instance
        shard_config: Worker-specific configuration (full or sharded)
        run_id: Parallel run identifier for this worker
        target_id: Target identifier for this worker
        breeder_id: UUID of the breeder

    Returns:
        tuple: (flow_id, job_id) - Flow identifier and Windmill job ID
    """
    try:
        breeder_type = shard_config.get('breeder', {}).get('type', 'unknown_breeder')
        script_path = f"f/breeder/{breeder_type}/breeder_worker"

        # Pass shard_config directly - worker uses it, no DB fetch needed
        script_inputs = {
            'config': shard_config,
            'breeder_id': breeder_id,
            'run_id': run_id,
            'target_id': target_id
        }

        logger.info(f"Starting script {flow_id} at path: {script_path}")
        logger.debug(f"Script inputs: breeder_id={breeder_id}, run_id={run_id}, target_id={target_id}")
        logger.debug(f"Shard config: {shard_config.get('settings', {}).get('sysctl', {})}")

        # Launch the breeder worker script asynchronously with 'breeder' tag
        # This routes the job to breeder worker group only
        job_id = wmill.run_script_by_path_async(
            path=script_path,
            args=script_inputs,
            tags=["breeder"]
        )
        
        logger.info(f"Flow {flow_id} started with job ID: {job_id}")
        
        return flow_id, job_id
        
    except Exception as e:
        logger.error(f"Failed to start optimization flow {flow_id}: {e}")
        raise

class BreederService:
    """Service for managing breeder lifecycle operations"""

    def __init__(self, archive_db_config, meta_db_config):
        self.archive_repo = ArchiveDatabaseRepository(archive_db_config)
        self.metadata_repo = MetadataDatabaseRepository(meta_db_config)

    def create_breeder(self, breeder_config, name):
        """Create a new breeder instance

        Args:
            breeder_config: The breeder configuration
            name: Breeder instance name (required)

        Returns:
            dict with result status and either breeder_id or error details
        """
        breeder_uuid = None
        breeder_id = None

        try:
            BreederConfig.validate_minimal(breeder_config)

            breeder_instance_name = name
            breeder_type = breeder_config.get('breeder', {}).get('type', 'unknown_breeder')
            parallel_runs = breeder_config.get('run', {}).get('parallel', 1)
            targets = breeder_config.get('effectuation', {}).get('targets', [])
            targets_count = len(targets)
            is_cooperative = breeder_config.get('cooperation', {}).get('active', False)

            # Call breeder preflight check synchronously before launching workers
            # This validates that the breeder supports all parameters in the config
            # (semantic validation that controller can't do)
            logger.info(f"Running preflight check for breeder type: {breeder_type}")
            preflight_script_path = f"f/breeder/{breeder_type}/preflight"

            try:
                preflight_result = wmill.run_script_by_path(
                    path=preflight_script_path,
                    args={'config': breeder_config}
                )

                if preflight_result.get('result') != 'SUCCESS':
                    error_msg = preflight_result.get('error', 'Unknown preflight error')
                    logger.error(f"Preflight validation failed: {error_msg}")
                    return {
                        "result": "FAILURE",
                        "error": f"Preflight validation failed: {error_msg}"
                    }

                logger.info("Preflight validation passed")

            except Exception as e:
                # If preflight script doesn't exist or fails, log warning but continue
                # (for backwards compatibility with breeders that don't have preflight yet)
                logger.warning(f"Preflight check failed or not found: {e}. Continuing with worker launch.")

            breeder_uuid = str(uuid.uuid4())
            breeder_config['breeder']['uuid'] = breeder_uuid
            creation_ts = datetime.datetime.now()

            __uuid_common_name = f"breeder_{breeder_uuid.replace('-', '_')}"
            breeder_id = f'{__uuid_common_name}'

            # Create database and metadata records
            self.archive_repo.create_database(breeder_id)

            self.metadata_repo.create_table()
            self.metadata_repo.insert_breeder_meta(
                breeder_id=breeder_uuid,
                name=breeder_instance_name,
                creation_ts=creation_ts,
                meta_state=breeder_config
            )

            # Launch worker scripts with error handling
            worker_launch_failures = []
            target_count = 0

            for target in targets:
                hash_suffix = hashlib.sha256(str.encode(target.get('address', ''))).hexdigest()[0:6]

                for run_id in range(parallel_runs):
                    flow_config = breeder_config.copy()
                    flow_config['creation_ts'] = creation_ts.isoformat()
                    flow_id = f'{breeder_instance_name}_{target_count}_{run_id}'

                    if not is_cooperative:
                        flow_config = determine_config_shard(
                            run_id=run_id,
                            target_id=target_count,
                            targets_count=targets_count,
                            config=flow_config,
                            parallel_runs_count=parallel_runs
                        )

                    try:
                        start_optimization_flow(
                            flow_id=flow_id,
                            shard_config=flow_config,
                            run_id=run_id,
                            target_id=target_count,
                            breeder_id=breeder_uuid
                        )
                    except Exception as e:
                        # Collect worker launch failures but continue trying others
                        error_details = {
                            "flow_id": flow_id,
                            "target": target_count,
                            "run": run_id,
                            "error": str(e),
                            "error_type": type(e).__name__
                        }
                        worker_launch_failures.append(error_details)
                        logger.error(f"Failed to launch worker {flow_id}: {e}")

                target_count += 1

            # If any workers failed to launch, clean up and raise error
            if worker_launch_failures:
                logger.error(f"Failed to launch {len(worker_launch_failures)} workers for breeder {breeder_uuid}")
                # Attempt cleanup
                try:
                    self._rollback_breeder_creation(breeder_uuid, breeder_id)
                except Exception as cleanup_error:
                    logger.error(f"Failed to cleanup after worker launch failures: {cleanup_error}")

                return {
                    "result": "FAILURE",
                    "error": f"Failed to launch {len(worker_launch_failures)} worker(s)"
                }

            logger.info(f"Successfully created breeder: {breeder_uuid}")
            return {
                "result": "SUCCESS",
                "data": {
                    "id": breeder_uuid,
                    "name": breeder_instance_name,
                    "status": "active",
                    "createdAt": creation_ts.isoformat()
                }
            }

        except Exception as e:
            logger.error(f"Failed to create breeder: {e}")
            # Attempt cleanup if we had created the breeder
            if breeder_uuid and breeder_id:
                try:
                    self._rollback_breeder_creation(breeder_uuid, breeder_id)
                except Exception as cleanup_error:
                    logger.error(f"Failed to cleanup after breeder creation failure: {cleanup_error}")

            return {
                "result": "FAILURE",
                "error": str(e)
            }

    def _rollback_breeder_creation(self, breeder_uuid: str, breeder_id: str):
        """Rollback breeder creation by cleaning up database records

        Args:
            breeder_uuid: UUID of the breeder to rollback
            breeder_id: Internal database identifier
        """
        logger.warning(f"Rolling back breeder creation: {breeder_uuid}")

        # Delete metadata record
        try:
            self.metadata_repo.remove_breeder_meta(breeder_uuid)
            logger.info(f"Deleted metadata for breeder: {breeder_uuid}")
        except Exception as e:
            logger.error(f"Failed to delete metadata for breeder {breeder_uuid}: {e}")

        # Delete archive database
        try:
            self.archive_repo.drop_database(breeder_id)
            logger.info(f"Deleted archive database for breeder: {breeder_uuid}")
        except Exception as e:
            logger.error(f"Failed to delete archive database for breeder {breeder_uuid}: {e}")

    def get_breeder(self, breeder_id):
        """Get breeder information"""
        try:
            import json

            self.metadata_repo.create_table()
            breeder_meta_data_row = self.metadata_repo.fetch_meta_data(breeder_id)

            if not breeder_meta_data_row or len(breeder_meta_data_row) == 0:
                return {
                    "result": "FAILURE",
                    "error": "Breeder not found"
                }

            # Row structure: [id, name, creation_ts, definition]
            return {
                "result": "SUCCESS",
                "data": {
                    "id": breeder_meta_data_row[0][0],
                    "name": breeder_meta_data_row[0][1],
                    "status": "active",
                    "createdAt": breeder_meta_data_row[0][2].isoformat(),
                    "config": breeder_meta_data_row[0][3]
                }
            }
        except Exception as e:
            return {
                "result": "FAILURE",
                "error": str(e)
            }

    def delete_breeder(self, breeder_id):
        """Delete a breeder instance"""
        try:
            # Check if breeder exists first
            self.metadata_repo.create_table()
            breeder_meta_data_row = self.metadata_repo.fetch_meta_data(breeder_id)

            if not breeder_meta_data_row or len(breeder_meta_data_row) == 0:
                logger.warning(f"Breeder with ID '{breeder_id}' not found")
                return {
                    "result": "FAILURE",
                    "error": f"Breeder with ID '{breeder_id}' not found"
                }

            __uuid_common_name = f"breeder_{breeder_id.replace('-', '_')}"

            self.archive_repo.drop_database(__uuid_common_name)

            self.metadata_repo.remove_breeder_meta(breeder_id)

            logger.info(f"Successfully deleted breeder: {breeder_id}")
            return {"result": "SUCCESS", "data": None}
        except Exception as e:
            logger.error(f"Failed to delete breeder {breeder_id}: {e}")
            return {"result": "FAILURE", "error": str(e)}

    def list_breeders(self):
        """List all breeders"""
        try:
            import dateutil.parser

            self.metadata_repo.create_table()
            breeder_meta_data_list = self.metadata_repo.fetch_breeders_list()

            if not breeder_meta_data_list:
                return {
                    "result": "SUCCESS",
                    "data": []
                }

            configured_breeders = []
            for row in breeder_meta_data_list:
                breeder_id = row[0]
                name = row[1]
                creation_ts = row[2]

                # Format creation timestamp
                if isinstance(creation_ts, str):
                    created_at = dateutil.parser.parse(creation_ts).isoformat()
                else:
                    created_at = creation_ts.isoformat()

                configured_breeders.append({
                    "id": breeder_id,
                    "name": name,
                    "status": "active",
                    "createdAt": created_at
                })

            return {
                "result": "SUCCESS",
                "data": configured_breeders
            }
        except Exception as e:
            return {
                "result": "FAILURE",
                "error": str(e)
            }