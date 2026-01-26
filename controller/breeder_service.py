import uuid
import hashlib
import datetime
import copy
import logging
import os
from dateutil.parser import parse

from f.controller.database import ArchiveDatabaseRepository, MetadataDatabaseRepository
from f.controller.config import BreederConfig, BREEDER_CAPABILITIES, DatabaseConfig

# Import wmill at top level so Windmill can detect it for dependency resolution
import wmill
from wmill import Windmill

# Import optuna for schema initialization
import optuna.storages

logger = logging.getLogger(__name__)

def cancel_job_by_id(job_id: str, reason: str = None) -> bool:
    """Cancel a Windmill job by its ID

    Args:
        job_id: The UUID of the job to cancel
        reason: Optional reason for cancellation

    Returns:
        True if cancellation succeeded, False otherwise
    """
    try:
        # Initialize Windmill client using environment variables
        client = Windmill()

        # Call the cancel endpoint
        payload = {"reason": reason} if reason else {}
        client.post(
            f"/w/{client.workspace}/jobs_u/queue/cancel/{job_id}",
            json=payload
        )

        logger.info(f"Successfully canceled Windmill job {job_id}")
        return True

    except Exception as e:
        logger.error(f"Failed to cancel Windmill job {job_id}: {e}")
        return False

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

        # Launch the breeder worker script asynchronously
        # Script tag is set at deployment time, Windmill routes based on script's tag
        job_id = wmill.run_script_by_path_async(
            path=script_path,
            args=script_inputs
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

    def _normalize_constraints(self, config):
        """Normalize constraint formats in config for breeder workers

        Recursively converts dict format {"values": [...]} to list format [{"values": [...]}]
        to ensure workers receive consistent constraint structure. Handles arbitrary nesting
        (e.g., ethtool -> interface -> param -> constraints).

        Args:
            config: Breeder configuration dict (modified in place)
        """
        def normalize_dict(obj):
            """Recursively normalize constraint dicts to list format"""
            if isinstance(obj, dict):
                # Check if this is a constraints dict that needs normalization
                if 'values' in obj and len(obj) == 1:
                    # This is a simple constraints dict: {"values": [...]}
                    return [obj]  # Wrap in list

                # Recursively process all values in the dict
                for key, value in obj.items():
                    obj[key] = normalize_dict(value)
                return obj
            elif isinstance(obj, list):
                # Process lists (though constraints shouldn't be nested in lists)
                return [normalize_dict(item) for item in obj]
            else:
                return obj

        # Normalize only the settings section
        if 'settings' in config:
            config['settings'] = normalize_dict(config['settings'])

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

            # Normalize constraint formats for workers
            # Convert dict format {"values": [...]} to list format [{"values": [...]}]
            # This ensures breeder_workers receive consistent constraint structure
            self._normalize_constraints(breeder_config)

            breeder_uuid = str(uuid.uuid4())
            breeder_config['breeder']['uuid'] = breeder_uuid
            creation_ts = datetime.datetime.now()

            __uuid_common_name = f"breeder_{breeder_uuid.replace('-', '_')}"
            breeder_id = f'{__uuid_common_name}'

            # Create database and metadata records
            self.archive_repo.create_database(breeder_id)

            # Create breeder state table for shutdown signaling in the archive DB
            self.archive_repo.create_breeder_state_table(breeder_id)

            # Wait for the breeder_state table to be fully committed and accessible
            # This prevents YugabyteDB serialization conflicts when Optuna starts its DDL operations
            from f.controller.database import get_db_connection
            max_wait = 10  # seconds
            check_interval = 0.5  # seconds
            import time
            table_ready = False

            for attempt in range(int(max_wait / check_interval)):
                try:
                    # Try to query the table - if it succeeds, the transaction is fully committed
                    db_config = self.archive_repo.base_config.copy()
                    db_config['database'] = breeder_id
                    with get_db_connection(db_config) as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("SELECT COUNT(*) FROM breeder_state;")
                            count = cursor.fetchone()[0]
                            table_ready = True
                            logger.info(f"Breeder state table is ready for {breeder_uuid}")
                            break
                except Exception as e:
                    if attempt < (max_wait / check_interval) - 1:
                        logger.debug(f"Waiting for breeder_state table to be ready... (attempt {attempt + 1})")
                        time.sleep(check_interval)
                    else:
                        logger.error(f"Breeder state table still not ready after {max_wait}s: {e}")
                        raise

            if not table_ready:
                raise Exception(f"Breeder state table did not become ready within {max_wait}s")

            # Initialize Optuna schema to prevent race conditions during worker startup
            # Multiple workers starting simultaneously would otherwise conflict trying to create tables
            # Retry logic for YugabyteDB serialization failures and timeouts
            max_retries = 5
            storage = None
            last_error = None

            for attempt in range(max_retries):
                try:
                    db_url = self.archive_repo.get_connection_url(breeder_id)
                    storage = optuna.storages.RDBStorage(url=db_url)
                    logger.info(f"Initialized Optuna schema for breeder {breeder_uuid}")
                    break
                except Exception as e:
                    last_error = e
                    error_str = str(e)
                    # Check for YugabyteDB-specific errors that should be retried
                    is_retryable = (
                        'SerializationFailure' in error_str or
                        '40001' in error_str or
                        'Transaction aborted' in error_str or
                        'Timed out waiting' in error_str or
                        'InternalError_' in error_str
                    )

                    if attempt < max_retries - 1 and is_retryable:
                        wait_time = 2 ** attempt  # Exponential backoff: 2s, 4s, 8s, 16s
                        logger.warning(f"Optuna schema initialization attempt {attempt + 1}/{max_retries} failed for breeder {breeder_uuid}: {e}")
                        logger.info(f"Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"Failed to initialize Optuna schema for breeder {breeder_uuid} after {max_retries} attempts: {e}")
                        # Clean up database if schema initialization fails
                        try:
                            self.archive_repo.drop_database(breeder_id)
                        except Exception as drop_error:
                            logger.error(f"Failed to cleanup database after Optuna init failure: {drop_error}")
                        raise

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
            worker_job_ids = []  # Track all worker job IDs for later cancellation

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
                        _, job_id = start_optimization_flow(
                            flow_id=flow_id,
                            shard_config=flow_config,
                            run_id=run_id,
                            target_id=target_count,
                            breeder_id=breeder_uuid
                        )
                        # Collect job ID for this worker
                        worker_job_ids.append(job_id)
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

            # Store job IDs in breeder metadata for cleanup on deletion
            if worker_job_ids:
                breeder_config['worker_job_ids'] = worker_job_ids
                self.metadata_repo.update_breeder_meta(
                    breeder_id=breeder_uuid,
                    meta_state=breeder_config
                )
                logger.info(f"Stored {len(worker_job_ids)} worker job IDs for breeder {breeder_uuid}")

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

    def start_breeder(self, breeder_id):
        """Start or resume a stopped breeder

        Clears the shutdown flag and relaunches all worker jobs.

        Args:
            breeder_id: UUID of the breeder to start

        Returns:
            Success/failure response with details
        """
        try:
            # Check if breeder exists
            self.metadata_repo.create_table()
            breeder_meta_data_row = self.metadata_repo.fetch_meta_data(breeder_id)

            if not breeder_meta_data_row or len(breeder_meta_data_row) == 0:
                logger.warning(f"Breeder with ID '{breeder_id}' not found")
                return {
                    "result": "FAILURE",
                    "error": f"Breeder with ID '{breeder_id}' not found"
                }

            # Extract metadata
            breeder_config = breeder_meta_data_row[0][3]
            breeder_instance_name = breeder_meta_data_row[0][1]
            breeder_type = breeder_config.get('breeder', {}).get('type', 'unknown_breeder')
            parallel_runs = breeder_config.get('run', {}).get('parallel', 1)
            targets = breeder_config.get('effectuation', {}).get('targets', [])
            targets_count = len(targets)
            is_cooperative = breeder_config.get('cooperation', {}).get('active', False)

            __uuid_common_name = f"breeder_{breeder_id.replace('-', '_')}"

            # Clear the shutdown flag in archive DB
            self.archive_repo.set_shutdown_requested(__uuid_common_name, value=False)

            # Relaunch workers using the same logic as create_breeder
            worker_launch_failures = []
            target_count = 0
            worker_job_ids = []

            for target in targets:
                for run_id in range(parallel_runs):
                    flow_config = breeder_config.copy()
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
                        _, job_id = start_optimization_flow(
                            flow_id=flow_id,
                            shard_config=flow_config,
                            run_id=run_id,
                            target_id=target_count,
                            breeder_id=breeder_id
                        )
                        worker_job_ids.append(job_id)
                    except Exception as e:
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

            # Update worker job IDs in metadata
            if worker_job_ids:
                breeder_config['worker_job_ids'] = worker_job_ids
                self.metadata_repo.update_breeder_meta(
                    breeder_id=breeder_id,
                    meta_state=breeder_config
                )

            # Handle any launch failures
            if worker_launch_failures:
                logger.error(f"Failed to launch {len(worker_launch_failures)} workers for breeder {breeder_id}")
                return {
                    "result": "PARTIAL_SUCCESS",
                    "error": f"Failed to launch {len(worker_launch_failures)} worker(s)",
                    "workers_started": len(worker_job_ids),
                    "workers_failed": len(worker_launch_failures)
                }

            logger.info(f"Successfully started/resumed breeder: {breeder_id}")
            return {
                "result": "SUCCESS",
                "data": {
                    "breeder_id": breeder_id,
                    "workers_started": len(worker_job_ids),
                    "status": "ACTIVE"
                }
            }

        except Exception as e:
            logger.error(f"Failed to start breeder {breeder_id}: {e}")
            return {"result": "FAILURE", "error": str(e)}

    def stop_breeder(self, breeder_id):
        """Request graceful shutdown of a breeder's workers

        Sets a flag in the breeder's archive database that workers check
        to gracefully stop after completing their current trial.

        This is a quick async operation - returns immediately.
        Workers will stop on their own timeline (check every trial).

        Monitor via Prometheus metrics for actual worker termination.
        """
        try:
            # Check if breeder exists
            self.metadata_repo.create_table()
            breeder_meta_data_row = self.metadata_repo.fetch_meta_data(breeder_id)

            if not breeder_meta_data_row or len(breeder_meta_data_row) == 0:
                logger.warning(f"Breeder with ID '{breeder_id}' not found")
                return {
                    "result": "FAILURE",
                    "error": f"Breeder with ID '{breeder_id}' not found"
                }

            # Get the actual database name (breeder_id is UUID, need DB name)
            __uuid_common_name = f"breeder_{breeder_id.replace('-', '_')}"

            # Set the shutdown flag in the breeder's archive DB
            self.archive_repo.set_shutdown_requested(__uuid_common_name)

            logger.info(f"Graceful shutdown requested for breeder: {breeder_id}")
            return {
                "result": "SUCCESS",
                "message": "Graceful shutdown requested. Workers will stop after completing current trials.",
                "data": {
                    "breeder_id": breeder_id,
                    "shutdown_type": "graceful",
                    "note": "Monitor metrics for worker termination"
                }
            }
        except Exception as e:
            logger.error(f"Failed to request shutdown for breeder {breeder_id}: {e}")
            return {"result": "FAILURE", "error": str(e)}

    def delete_breeder(self, breeder_id, force=False):
        """Delete a breeder instance

        Args:
            breeder_id: UUID of the breeder to delete
            force: If True, cancel workers immediately (for smoke test/cleanup)
                   If False, check if shutdown flag is set first

        For now: force=True for smoke test, force=False reserved for future graceful shutdown
        """
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

            # Extract worker job IDs from metadata (4th column is definition/JSONB)
            breeder_config = breeder_meta_data_row[0][3]
            worker_job_ids = breeder_config.get('worker_job_ids', [])

            __uuid_common_name = f"breeder_{breeder_id.replace('-', '_')}"

            # Cancel all running worker jobs before dropping database
            if worker_job_ids:
                if not force:
                    # Future: Check if graceful shutdown was requested
                    shutdown_requested = self.archive_repo.get_shutdown_requested(__uuid_common_name)
                    if not shutdown_requested:
                        return {
                            "result": "FAILURE",
                            "error": "Breeder has active workers. Use force=True to cancel immediately",
                            "active_workers": len(worker_job_ids),
                            "note": "Future: call stop_breeder() for graceful shutdown first"
                        }
                    logger.info(f"Shutdown flag set, proceeding with delete for breeder {breeder_id}")

                # Cancel workers (forced or graceful-shutdown-complete)
                logger.info(f"Cancelling {len(worker_job_ids)} worker jobs for breeder {breeder_id}")
                canceled_count = 0
                failed_count = 0

                for job_id in worker_job_ids:
                    if cancel_job_by_id(job_id, reason=f"Deleting breeder {breeder_id}"):
                        canceled_count += 1
                    else:
                        failed_count += 1
                        logger.warning(f"Failed to cancel worker job {job_id}")

                logger.info(f"Cancelled {canceled_count}/{len(worker_job_ids)} worker jobs")
                if failed_count > 0:
                    logger.warning(f"{failed_count} worker jobs could not be cancelled")

            # Drop the archive database
            self.archive_repo.drop_database(__uuid_common_name)

            # Remove metadata
            self.metadata_repo.remove_breeder_meta(breeder_id)

            logger.info(f"Successfully deleted breeder: {breeder_id}")
            return {
                "result": "SUCCESS",
                "data": {
                    "breeder_id": breeder_id,
                    "delete_type": "force" if force else "graceful",
                    "workers_cancelled": len(worker_job_ids)
                }
            }
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