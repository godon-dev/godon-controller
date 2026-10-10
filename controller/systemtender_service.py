import uuid
import hashlib
import datetime
import copy
import os
import time
from dateutil.parser import parse

from f.controller.database import ArchiveDatabaseRepository, MetadataDatabaseRepository, execute_query
from f.controller.config import SystemtenderConfig, SYSTEMTENDER_CAPABILITIES, DatabaseConfig
from f.controller.shared.otel_logging import get_logger

import wmill
from wmill import Windmill

import optuna.storages

logger = get_logger(__name__)

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

# ── delete quiesce bounds ────────────────────────────────────────────
# A Windmill cancel is async: the job only reaches a terminal state
# when the worker process dies. Dropping the archive DB before that
# races a live connection pool - the exact race that poisons YB's
# relcache ("database may have been dropped and recreated"; live
# evidence 2026-09-30, godon#399).
JOB_QUIESCE_POLL_SECONDS = 2  # poll cadence: cancels normally land in seconds
# Outer bound for the death wait. Windmill ENFORCES cancels against
# running jobs — the worker polls queue.canceled every 500ms (2s on
# agent workers) and escalates SIGTERM (5s) -> SIGKILL + reap, so
# expected cancel->death is ~8s (designs/2026-10-03, source-verified).
# This bound exists to catch a wedged windmill, not a slow worker;
# crossing it flags deletion-failed instead of waiting longer.
JOB_QUIESCE_TIMEOUT_SECONDS = 30
# A deletion claim older than this proves its executor dead: every
# executor run is bounded by the script's component timeout (300s),
# so no live owner can exist past this age. Re-DELETE takes over.
JOB_CLAIM_STALE_SECONDS = 300

# A job in one of these states has no running process, hence no open
# archive DB connection.
TERMINAL_JOB_STATES = ('completed', 'canceled', 'failed')


def _unwrap_wmill(resp):
    """Normalize the wmill SDK surface to a dict or None.

    The SDK differs across images: some return parsed JSON dicts, the
    worker image (windmill-full) returns raw httpx.Response objects -
    live receipt 10-07: "jobs_u/get error polling ...: 'Response'
    object has no attribute 'get'" - the reader then misread every
    answer and the deletion looped deletion-failed forever.
    """
    if resp is None:
        return None
    if isinstance(resp, dict):
        return resp
    body = getattr(resp, "json", None)
    if body is None:
        return None
    try:
        data = body()
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _wmill_status(resp):
    """HTTP status off a raw Response surface (None for dict surfaces)."""
    status = getattr(resp, "status_code", None)
    return status if isinstance(status, int) else None


def job_reached_terminal_state(job_id):
    """True when the Windmill job is terminal (no process, no connection).

    Shape-proof across windmill generations: pre-v2 jobs_u/get returns
    {status: "completed"|"canceled"|"failed"|"running"|...}; v2 (1.623+)
    returns {type: "CompletedJob"|"CanceledJob"|...} with NO status
    field — a clean 200 the old reader parsed as forever-running (live
    receipt 10-06: eight orphan markers flagged deletion-failed while
    every target job lay completed). Read status, then type, strip the
    v2 'Job' suffix, and match the terminal set. The SDK surface itself
    also varies: the worker image hands back raw httpx.Response objects
    (live receipt 10-07) - _unwrap_wmill normalizes before any read.
    """
    try:
        client = Windmill()
        resp = client.get(f"/w/{client.workspace}/jobs_u/get/{job_id}")
        if _wmill_status(resp) == 404:
            resp = None  # no job row: normalized to the gone receipt
        job = _unwrap_wmill(resp)
        if not job:
            # No job row at all: an accepted cancel removes queued jobs
            # outright (10-05 source receipt; live 10-07: smoke stress
            # deletions - row gone, probe started:false). Nothing that
            # never ran can hold an archive-DB connection.
            return True
        raw = job.get('status') or job.get('type') or ''
        status = str(raw).lower().removesuffix('job')
        return status in TERMINAL_JOB_STATES
    except Exception as e:
        # A job unknown to the queue (cleaned history) cannot hold a
        # connection; any other error stays non-terminal so the bounded
        # wait decides.
        text = str(e).lower()
        if '404' in text or 'not found' in text:
            return True
        # The queue endpoint can hard-error (400-class, seen live 10-02)
        # for a job whose completed row already exists - the poller then
        # reports a straggler forever while the job is long dead. Probe
        # the completed-jobs endpoint the wmill SDK itself uses for sync
        # waits; its answer is {completed, result, success, started}
        # (GetCompletedJobResultMaybe200Response) - completed=True is the
        # terminal receipt. Fail closed on anything else.
        logger.warning(
            f"jobs_u/get error polling {job_id}: {e} - probing completed endpoint")
        try:
            client2 = Windmill()
            res2 = client2.get(
                f"/w/{client2.workspace}"
                f"/jobs_u/completed/get_result_maybe/{job_id}?get_started=true")
            res = _unwrap_wmill(res2)
            if isinstance(res, dict):
                if res.get('completed') is True:
                    return True
                if res.get('started') is False:
                    # Post-cancel started:false: the job never ran and,
                    # the cancel being accepted, never will - windmill
                    # removes queued jobs on cancel (10-05 source
                    # receipt; live 10-07: smoke stress deletions looped
                    # deletion-failed here and re-DELETE repeated the
                    # miss deterministically). No process ever existed,
                    # hence no archive-DB connection.
                    return True
            return False
        except Exception as e2:
            logger.warning(f"completed-probe error for {job_id}: {e2}")
            return False


def wait_for_jobs_to_quiesce(job_ids,
                             timeout_seconds=JOB_QUIESCE_TIMEOUT_SECONDS):
    """Bounded wait until every job is terminal. Returns the stragglers."""
    deadline = time.monotonic() + timeout_seconds
    pending = list(job_ids)
    while pending and time.monotonic() < deadline:
        pending = [j for j in pending if not job_reached_terminal_state(j)]
        if pending:
            time.sleep(JOB_QUIESCE_POLL_SECONDS)
    return pending


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

def start_optimization_flow(flow_id, shard_config, run_id, target_id, systemtender_id):
    """Start systemtender worker optimization flow in Windmill

    Launches a systemtender worker flow with its sharded configuration.
    The full systemtender config is stored in meta DB; workers receive their
    specific shard (either full config for cooperative mode, or sharded
    parameter ranges for non-cooperative mode).

    Args:
        flow_id: Identifier for this flow instance
        shard_config: Worker-specific configuration (full or sharded)
        run_id: Parallel run identifier for this worker
        target_id: Target identifier for this worker
        systemtender_id: UUID of the systemtender

    Returns:
        tuple: (flow_id, job_id) - Flow identifier and Windmill job ID
    """
    try:
        systemtender_type = shard_config.get('systemtender', {}).get('type', 'unknown_systemtender')
        script_path = "f/systemtender/engine/systemtender_worker"

        # Pass shard_config directly - worker uses it, no DB fetch needed
        script_inputs = {
            'config': shard_config,
            'systemtender_id': systemtender_id,
            'run_id': run_id,
            'target_id': target_id
        }

        logger.info(f"Starting script {flow_id} at path: {script_path}")
        logger.debug(f"Script inputs: systemtender_id={systemtender_id}, run_id={run_id}, target_id={target_id}")
        logger.debug(f"Shard config: {shard_config.get('settings', {}).get('sysctl', {})}")

        # Launch the systemtender worker script asynchronously
        # Tag must be passed explicitly — Windmill does NOT inherit
        # the script's stored tag for job routing. Without this, jobs
        # get routed to default workers instead of systemtender workers.
        job_id = wmill.run_script_by_path_async(
            path=script_path,
            args=script_inputs,
            tag="systemtender"
        )
        
        logger.info(f"Flow {flow_id} started with job ID: {job_id}")
        
        return flow_id, job_id
        
    except Exception as e:
        logger.error(f"Failed to start optimization flow {flow_id}: {e}")
        raise

class SystemtenderService:
    """Service for managing systemtender lifecycle operations"""

    def __init__(self, archive_db_config, meta_db_config):
        self.archive_repo = ArchiveDatabaseRepository(archive_db_config)
        self.metadata_repo = MetadataDatabaseRepository(meta_db_config)

    def _normalize_constraints(self, config):
        """Normalize constraint formats in config for systemtender workers

        Recursively converts dict format {"values": [...]} to list format [{"values": [...]}]
        to ensure workers receive consistent constraint structure. Handles arbitrary nesting
        (e.g., ethtool -> interface -> param -> constraints).

        Args:
            config: Systemtender configuration dict (modified in place)
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


    def _assign_watermark_slot(self, systemtender_config):
        """Assign a collision-free watermark slot to a new systemtender.

        Reads all active systemtenders' configs from metadata DB to find
        which slots are already in use, then assigns the lowest available slot.
        Falls back to hash-based assignment if all slots are taken or metadata
        is unavailable.

        The slot maps to prime periods in the systemtender's watermark encoding:
        slot 0 -> periods [17, 23], slot 1 -> [29, 37], ..., slot 5 -> [67, 71]
        With 12 primes and 2 per systemtender, max 6 systemtenders can have unique slots.

        Args:
            systemtender_config: Systemtender configuration dict (modified in place)
        """
        max_slots = 6  # 12 primes / 2 per systemtender
        used_slots = set()

        try:
            self.metadata_repo.create_table()
            systemtender_list = self.metadata_repo.fetch_systemtenders_list()
            if systemtender_list:
                for row in systemtender_list:
                    # systemtender_list returns (id, name, creation_tsz) — fetch config separately
                    existing_systemtender_id = row[0]
                    meta_row = self.metadata_repo.fetch_meta_data(existing_systemtender_id)
                    if meta_row and len(meta_row) > 0:
                        existing_config = meta_row[0][3] if len(meta_row[0]) > 3 else None
                        if existing_config and isinstance(existing_config, dict):
                            slot = existing_config.get('systemtender', {}).get('watermark_slot')
                            if slot is not None:
                                used_slots.add(slot)
        except Exception as e:
            logger.warning(f"Could not read existing systemtender slots: {e}. Falling back.")

        if len(used_slots) < max_slots:
            # Assign lowest available slot
            assigned_slot = min(s for s in range(max_slots) if s not in used_slots)
            systemtender_config['systemtender']['watermark_slot'] = assigned_slot
            logger.info(f"Assigned watermark slot {assigned_slot} (used: {sorted(used_slots)})")
        else:
            logger.warning(f"All {max_slots} watermark slots in use. Systemtender may share frequencies.")

    def _resolve_target_refs(self, systemtender_config):
        """Resolve targetRefs to inline targets from the targets catalog

        If effectuation.targetRefs is present, fetches each target from
        the metadata DB and populates effectuation.targets with the resolved data.
        Backward compatible: if targetRefs is absent, targets are used as-is.

        Row tuple: (id, name, target_type, spec JSONB, metadata JSONB, created_at, last_used_at)
        Resolver produces type-appropriate fields from spec JSONB.

        Args:
            systemtender_config: Systemtender configuration dict (modified in place)

        Raises:
            ValueError: If any target ref cannot be resolved
        """
        target_refs = systemtender_config.get('effectuation', {}).get('targetRefs', [])
        if not target_refs:
            return

        logger.info(f"Resolving {len(target_refs)} target references")

        resolved_targets = []
        meta_db = MetadataDatabaseRepository(DatabaseConfig.META_DB)
        meta_db.create_targets_table()

        for ref_id in target_refs:
            target_row = meta_db.fetch_target_by_id(ref_id)
            if not target_row:
                target_row = meta_db.fetch_target_by_name(ref_id)
            if not target_row:
                raise ValueError(
                    f"Cannot resolve target reference '{ref_id}'. "
                    f"Target not found in catalog by ID or name."
                )

            target_type = target_row[2]
            spec = target_row[3] or {}

            target_entry = {
                'id': str(target_row[0]),
                'type': target_type,
            }

            target_entry.update(spec)

            resolved_targets.append(target_entry)
            logger.info(f"Resolved target ref '{ref_id}' -> {target_row[1]} (type={target_type})")

        systemtender_config['effectuation']['targets'] = resolved_targets
        logger.info(f"Resolved {len(resolved_targets)} targets from {len(target_refs)} references")

    def create_systemtender(self, systemtender_config, name):
        """Accept a create: validate, claim the name, plant the row in
        `creating`, dispatch the executor, answer immediately.

        The async create contract (designs/2026-10-10): the caller's
        connection is held only for fast reads — the heavy half
        (preflight, archive DB, Optuna schema, worker launches) is the
        executor's own job. The row is the state machine; an aborted
        caller wait owns nothing.

        Args:
            systemtender_config: The systemtender configuration
            name: Systemtender instance name (required)

        Returns:
            dict with result status and either systemtender_id or error details
        """
        systemtender_uuid = None

        try:
            self._resolve_target_refs(systemtender_config)

            SystemtenderConfig.validate_minimal(systemtender_config)

            systemtender_instance_name = name

            # Ensure metadata table exists before querying it (idempotent check)
            self.metadata_repo.create_table()

            # Check if a systemtender with this name already exists.
            # If it does, return the existing systemtender instead of creating a duplicate
            # and dispatching duplicate workers.
            existing_systemtenders = self.metadata_repo.fetch_systemtenders_list()
            for eb in existing_systemtenders:
                eb_name = eb.get('name') if isinstance(eb, dict) else (eb[1] if isinstance(eb, (list, tuple)) and len(eb) > 1 else None)
                eb_id = eb.get('id') if isinstance(eb, dict) else (eb[0] if isinstance(eb, (list, tuple)) else None)
                if eb_name == systemtender_instance_name and eb_id:
                    # Answer from the visible state, never a bare
                    # "already exists" the caller can't parse
                    # (receipt 10-03): a deleting tender says so, a
                    # deletion-failed one points at the retry — and a
                    # tender still creating, or failed to create, says
                    # that too (receipt 10-10).
                    existing_lifecycle = self.metadata_repo.get_lifecycle(str(eb_id))
                    dup_status = existing_lifecycle['state'] if existing_lifecycle else "active"
                    if dup_status == 'creating':
                        dup_message = "Systemtender is still being created; poll GET until active or create-failed"
                    elif dup_status == 'create-failed':
                        dup_message = "Systemtender creation failed; GET for the reason, DELETE to clear, then retry create"
                    elif dup_status == 'deleting':
                        dup_message = "Systemtender is still deleting; poll GET until 404, then retry create"
                    elif dup_status == 'deletion-failed':
                        dup_message = "Systemtender deletion failed; re-DELETE to retry, or GET for the reason"
                    else:
                        dup_message = "Systemtender already exists, skipping duplicate creation"
                    logger.info(f"Systemtender '{systemtender_instance_name}' already exists: {eb_id} (status: {dup_status}). Skipping creation to prevent duplicate workers.")
                    existing_row = self.metadata_repo.fetch_meta_data(str(eb_id))
                    dup_created_at = existing_row[0][2].isoformat() if existing_row else None
                    return {
                        "result": "SUCCESS",
                        "duplicate": True,
                        "data": {
                            "id": str(eb_id),
                            "name": systemtender_instance_name,
                            "status": dup_status,
                            "createdAt": dup_created_at,
                            "message": dup_message
                        }
                    }

            # Fast, bounded work only: everything past this point that
            # can take seconds belongs to the executor.
            self._normalize_constraints(systemtender_config)

            systemtender_uuid = str(uuid.uuid4())
            systemtender_config['systemtender']['uuid'] = systemtender_uuid

            # Assign collision-free watermark slot for interference detection
            self._assign_watermark_slot(systemtender_config)
            creation_ts = datetime.datetime.now()

            # The row goes in BEFORE the executor dispatch, planted in
            # `creating`: the visible state owns the wait, and a
            # re-create under this name clashes against the live row.
            self.metadata_repo.insert_systemtender_meta(
                systemtender_id=systemtender_uuid,
                name=systemtender_instance_name,
                creation_ts=creation_ts,
                meta_state=systemtender_config
            )
            self.metadata_repo.set_lifecycle(systemtender_uuid, 'creating')

            wmill.run_script_by_path_async(
                path="f/controller/systemtender_create_executor",
                args={'request_data': {'systemtender_id': systemtender_uuid}}
            )

            logger.info(f"Create accepted for systemtender '{systemtender_instance_name}' ({systemtender_uuid}); executor dispatched, row in creating")
            return {
                "result": "SUCCESS",
                "data": {
                    "id": systemtender_uuid,
                    "name": systemtender_instance_name,
                    "status": "creating",
                    "createdAt": creation_ts.isoformat()
                }
            }

        except Exception as e:
            logger.error(f"Failed to accept systemtender create: {e}")
            # The dispatch never landed: undo row and marker so nothing
            # half-lives under this name.
            if systemtender_uuid:
                try:
                    self._rollback_systemtender_creation(
                        systemtender_uuid,
                        f"systemtender_{systemtender_uuid.replace('-', '_')}"
                    )
                except Exception as cleanup_error:
                    logger.error(f"Failed to cleanup after systemtender create failure: {cleanup_error}")

            return {
                "result": "FAILURE",
                "error": str(e)
            }

    def create_systemtender_executor(self, systemtender_id):
        """The async executor behind create's 202 (designs/2026-10-10).

        Runs as its own windmill job; no caller is waiting. Reads the
        row the fast path planted in `creating` and does the heavy
        half: preflight, archive DB + state table, Optuna schema,
        worker launches.

        Success drops the lifecycle marker — the row reads active from
        its own state table (running once the workers heartbeat). Any
        failure flags `create-failed` + reason on the KEPT row (the
        receipt for the client's poll); launched workers are cancelled
        and the half-made archive DB dropped, so a failed create holds
        nothing but its row. DELETE claims a create-failed row like
        any other — cleanup stays one verb.
        """
        systemtender_db_name = f"systemtender_{systemtender_id.replace('-', '_')}"
        launched_job_ids = []

        def _fail(reason):
            """Flag create-failed and stop. The row stays as the
            receipt; GET answers state + reason. Launched workers die,
            the half-made archive DB goes."""
            logger.error(f"Create executor failed for systemtender {systemtender_id}: {reason}")
            for job_id in launched_job_ids:
                if not cancel_job_by_id(job_id, reason=f"Create failed for systemtender {systemtender_id}"):
                    logger.error(f"Failed to cancel launched worker {job_id}; cancel it manually before retrying")
            try:
                self.archive_repo.drop_database(systemtender_db_name)
            except Exception as drop_error:
                logger.error(f"Failed to drop half-made archive database {systemtender_db_name}: {drop_error}")
            self.metadata_repo.set_lifecycle(systemtender_id, 'create-failed', reason)
            return {
                "result": "FAILURE",
                "error": reason,
                "data": {
                    "id": systemtender_id,
                    "status": "create-failed"
                }
            }

        try:
            self.metadata_repo.create_table()
            meta_row = self.metadata_repo.fetch_meta_data(systemtender_id)
            if not meta_row or len(meta_row) == 0:
                return {
                    "result": "FAILURE",
                    "error": "Systemtender not found"
                }

            systemtender_instance_name = meta_row[0][1]
            creation_ts = meta_row[0][2]
            systemtender_config = meta_row[0][3]
            systemtender_type = systemtender_config.get('systemtender', {}).get('type', 'unknown_systemtender')
            parallel_runs = systemtender_config.get('run', {}).get('parallel', 1)
            targets = systemtender_config.get('effectuation', {}).get('targets', [])
            targets_count = len(targets)
            is_cooperative = systemtender_config.get('cooperation', {}).get('active', False)

            # Call systemtender preflight check before launching workers
            # This validates that the systemtender supports all parameters in the config
            # (semantic validation that controller can't do; moved here
            # from the synchronous path — it runs a script)
            logger.info(f"Running preflight check for systemtender type: {systemtender_type}")
            preflight_script_path = f"f/systemtender/strains/{systemtender_type}/preflight"

            try:
                preflight_result = wmill.run_script_by_path(
                    path=preflight_script_path,
                    args={'config': systemtender_config}
                )

                if preflight_result.get('result') != 'SUCCESS':
                    error_msg = preflight_result.get('error', 'Unknown preflight error')
                    logger.error(f"Preflight validation failed: {error_msg}")
                    return _fail(f"Preflight validation failed: {error_msg}")

                logger.info("Preflight validation passed")

            except Exception as e:
                # If preflight script doesn't exist or fails, log warning but continue
                # (for backwards compatibility with systemtenders that don't have preflight yet)
                logger.warning(f"Preflight check failed or not found: {e}. Continuing with worker launch.")

            # Create database and metadata records
            self.archive_repo.create_database(systemtender_db_name)

            # Create systemtender state table for shutdown signaling in the archive DB
            self.archive_repo.create_systemtender_state_table(systemtender_db_name)

            # Wait for the systemtender_state table to be fully committed and accessible
            # (prevents YugabyteDB serialization conflicts when Optuna starts its DDL)
            self._await_state_table(systemtender_db_name)

            # Initialize Optuna schema to prevent race conditions during worker startup
            # (retries on YugabyteDB serialization failures and timeouts)
            self._init_optuna_schema(systemtender_db_name)

            # Launch worker scripts with error handling
            worker_launch_failures = []
            target_count = 0
            worker_job_ids = []  # Track all worker job IDs for later cancellation

            for target in targets:
                hash_suffix = hashlib.sha256(str.encode(target.get('address', ''))).hexdigest()[0:6]

                for run_id in range(parallel_runs):
                    flow_config = systemtender_config.copy()
                    flow_config['creation_ts'] = creation_ts.isoformat()
                    flow_id = f'{systemtender_instance_name}_{target_count}_{run_id}'

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
                            systemtender_id=systemtender_id
                        )
                        # Collect job ID for this worker
                        worker_job_ids.append(job_id)
                        launched_job_ids.append(job_id)
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

            # Store job IDs in systemtender metadata for cleanup on deletion
            if worker_job_ids:
                systemtender_config['worker_job_ids'] = worker_job_ids
                self.metadata_repo.update_systemtender_meta(
                    systemtender_id=systemtender_id,
                    meta_state=systemtender_config
                )
                logger.info(f"Stored {len(worker_job_ids)} worker job IDs for systemtender {systemtender_id}")

            # If any workers failed to launch, the create failed:
            # cancel the launched siblings, drop the half-made DB,
            # flag the row — the receipt stands (first underlying
            # error in the reason; the poller sees the cause, not a
            # count).
            if worker_launch_failures:
                first_error = worker_launch_failures[0]['error']
                return _fail(
                    f"Failed to launch {len(worker_launch_failures)} worker(s): {first_error}"
                )

            # Live: the marker drops, the row stands on its own state
            # table (active until the first heartbeat refines it).
            self.metadata_repo.remove_lifecycle(systemtender_id)

            logger.info(f"Successfully created systemtender: {systemtender_id}")
            return {
                "result": "SUCCESS",
                "data": {
                    "id": systemtender_id,
                    "name": systemtender_instance_name,
                    "status": "active",
                    "createdAt": creation_ts.isoformat()
                }
            }

        except Exception as e:
            logger.error(f"Failed to create systemtender: {e}")
            return _fail(f"create executor error: {e}")

    def _rollback_systemtender_creation(self, systemtender_uuid: str, systemtender_id: str):
        """Rollback systemtender creation by cleaning up database records

        Args:
            systemtender_uuid: UUID of the systemtender to rollback
            systemtender_id: Internal database identifier
        """
        logger.warning(f"Rolling back systemtender creation: {systemtender_uuid}")

        # Delete metadata record
        try:
            self.metadata_repo.remove_systemtender_meta(systemtender_uuid)
            logger.info(f"Deleted metadata for systemtender: {systemtender_uuid}")
        except Exception as e:
            logger.error(f"Failed to delete metadata for systemtender {systemtender_uuid}: {e}")

        # Sweep the lifecycle marker (creating) with it — a rolled-back
        # create must not 409 the next attempt under the same name
        try:
            self.metadata_repo.remove_lifecycle(systemtender_uuid)
        except Exception as e:
            logger.error(f"Failed to remove lifecycle marker for systemtender {systemtender_uuid}: {e}")

        # Delete archive database
        try:
            self.archive_repo.drop_database(systemtender_id)
            logger.info(f"Deleted archive database for systemtender: {systemtender_uuid}")
        except Exception as e:
            logger.error(f"Failed to delete archive database for systemtender {systemtender_uuid}: {e}")

    def _await_state_table(self, systemtender_db_name):
        """Wait for the systemtender_state table to be fully committed
        and accessible.

        This prevents YugabyteDB serialization conflicts when Optuna
        starts its DDL operations. Executor-side step
        (designs/2026-10-10): the wait lives where no caller holds a
        connection for it.
        """
        from f.controller.database import get_db_connection
        max_wait = 10  # seconds
        check_interval = 0.5  # seconds
        table_ready = False

        for attempt in range(int(max_wait / check_interval)):
            try:
                # Try to query the table - if it succeeds, the transaction is fully committed
                db_config = self.archive_repo.base_config.copy()
                db_config['database'] = systemtender_db_name
                with get_db_connection(db_config) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT COUNT(*) FROM systemtender_state;")
                        cursor.fetchone()
                        table_ready = True
                        logger.info(f"Systemtender state table is ready in {systemtender_db_name}")
                        break
            except Exception as e:
                if attempt < (max_wait / check_interval) - 1:
                    logger.debug(f"Waiting for systemtender_state table to be ready... (attempt {attempt + 1})")
                    time.sleep(check_interval)
                else:
                    logger.error(f"Systemtender state table still not ready after {max_wait}s: {e}")
                    raise

        if not table_ready:
            raise Exception(f"Systemtender state table did not become ready within {max_wait}s")

    def _init_optuna_schema(self, systemtender_db_name):
        """Initialize Optuna schema to prevent race conditions during
        worker startup: multiple workers starting simultaneously would
        otherwise conflict trying to create tables.

        Retry logic for YugabyteDB serialization failures and timeouts.
        Executor-side step (designs/2026-10-10).
        """
        max_retries = 5

        for attempt in range(max_retries):
            try:
                db_url = self.archive_repo.get_connection_url(systemtender_db_name)
                optuna.storages.RDBStorage(url=db_url)
                logger.info(f"Initialized Optuna schema in {systemtender_db_name}")
                return
            except Exception as e:
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
                    logger.warning(f"Optuna schema initialization attempt {attempt + 1}/{max_retries} failed for {systemtender_db_name}: {e}")
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to initialize Optuna schema for {systemtender_db_name} after {max_retries} attempts: {e}")
                    raise

    def get_systemtender(self, systemtender_id):
        """Get systemtender information"""
        try:
            import json

            self.metadata_repo.create_table()
            systemtender_meta_data_row = self.metadata_repo.fetch_meta_data(systemtender_id)

            if not systemtender_meta_data_row or len(systemtender_meta_data_row) == 0:
                return {
                    "result": "FAILURE",
                    "error": "Systemtender not found"
                }

            # Row structure: [id, name, creation_ts, definition]
            # Status verdict: the tender's declared end (finished stamp
            # in its own state table) wins; silence falls back to the
            # heartbeat age — presumed dead past 3x the beat interval
            # (same constant as the liveness door), fresh otherwise.
            # A row with no state table yet answers `active` — the
            # post-create window where workers are still booting
            # (designs/2026-10-10: `active` is terminal-for-create).
            db_name = f"systemtender_{systemtender_id.replace('-', '_')}"
            status = 'active'
            liveness = None
            state = self.archive_repo.read_state_verdict(db_name)
            if state:
                interval = state['interval_secs'] if state['interval_secs'] else 60
                # yb abort bursts can silence fresh-connection beats for
                # 30s+ while the worker stays alive and trialing (run
                # 36261279551, 2026-09-26: two living workers stamped
                # presumed_dead at 3x10s, cell cleaned up mid-run).
                # 6x with a 90s floor lets the burst heal before the
                # death verdict; true deaths cost the wait-loop seconds
                # it never misses against a 5400s cell budget.
                threshold = max(90, 6 * interval)
                if state['finished']:
                    status = 'finished'
                elif state['age_secs'] is not None and state['age_secs'] > threshold:
                    status = 'presumed_dead'
                else:
                    status = 'running'
                liveness = {
                    'age_secs': state['age_secs'],
                    'interval_secs': state['interval_secs'],
                    'stale_threshold_secs': threshold,
                }

            # Lifecycle state machine overrides the liveness verdict:
            # while the marker stands, the tender IS its state —
            # deleting/deletion-failed (designs/2026-10-03) or
            # creating/create-failed (designs/2026-10-10). GET is how
            # the client watches the state machine: poll until the
            # resource is live (active/running) or gone (404).
            lifecycle = self.metadata_repo.get_lifecycle(systemtender_id)
            deletion_reason = None
            creation_reason = None
            if lifecycle:
                status = lifecycle['state']
                if status in ('deleting', 'deletion-failed'):
                    deletion_reason = lifecycle.get('reason')
                elif status in ('creating', 'create-failed'):
                    creation_reason = lifecycle.get('reason')

            return {
                "result": "SUCCESS",
                "data": {
                    "id": systemtender_meta_data_row[0][0],
                    "name": systemtender_meta_data_row[0][1],
                    "status": status,
                    "deletion_reason": deletion_reason,
                    "creation_reason": creation_reason,
                    "liveness": liveness,
                    "createdAt": systemtender_meta_data_row[0][2].isoformat(),
                    "config": systemtender_meta_data_row[0][3]
                }
            }
        except Exception as e:
            return {
                "result": "FAILURE",
                "error": str(e)
            }

    def start_systemtender(self, systemtender_id):
        """Start or resume a stopped systemtender

        Clears the shutdown flag and relaunches all worker jobs.

        Args:
            systemtender_id: UUID of the systemtender to start

        Returns:
            Success/failure response with details
        """
        try:
            # Check if systemtender exists
            self.metadata_repo.create_table()
            systemtender_meta_data_row = self.metadata_repo.fetch_meta_data(systemtender_id)

            if not systemtender_meta_data_row or len(systemtender_meta_data_row) == 0:
                logger.warning(f"Systemtender with ID '{systemtender_id}' not found")
                return {
                    "result": "FAILURE",
                    "error": f"Systemtender with ID '{systemtender_id}' not found"
                }

            # Extract metadata
            systemtender_config = systemtender_meta_data_row[0][3]
            systemtender_instance_name = systemtender_meta_data_row[0][1]
            systemtender_type = systemtender_config.get('systemtender', {}).get('type', 'unknown_systemtender')
            parallel_runs = systemtender_config.get('run', {}).get('parallel', 1)
            targets = systemtender_config.get('effectuation', {}).get('targets', [])
            targets_count = len(targets)
            is_cooperative = systemtender_config.get('cooperation', {}).get('active', False)

            __uuid_common_name = f"systemtender_{systemtender_id.replace('-', '_')}"

            # Clear the shutdown flag in archive DB
            self.archive_repo.set_shutdown_requested(__uuid_common_name, value=False)

            # Relaunch workers using the same logic as create_systemtender
            worker_launch_failures = []
            target_count = 0
            worker_job_ids = []

            for target in targets:
                for run_id in range(parallel_runs):
                    flow_config = systemtender_config.copy()
                    flow_id = f'{systemtender_instance_name}_{target_count}_{run_id}'

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
                            systemtender_id=systemtender_id
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
                systemtender_config['worker_job_ids'] = worker_job_ids
                self.metadata_repo.update_systemtender_meta(
                    systemtender_id=systemtender_id,
                    meta_state=systemtender_config
                )

            # Handle any launch failures
            if worker_launch_failures:
                logger.error(f"Failed to launch {len(worker_launch_failures)} workers for systemtender {systemtender_id}")
                return {
                    "result": "PARTIAL_SUCCESS",
                    "error": f"Failed to launch {len(worker_launch_failures)} worker(s)",
                    "workers_started": len(worker_job_ids),
                    "workers_failed": len(worker_launch_failures)
                }

            logger.info(f"Successfully started/resumed systemtender: {systemtender_id}")
            return {
                "result": "SUCCESS",
                "data": {
                    "systemtender_id": systemtender_id,
                    "workers_started": len(worker_job_ids),
                    "status": "ACTIVE"
                }
            }

        except Exception as e:
            logger.error(f"Failed to start systemtender {systemtender_id}: {e}")
            return {"result": "FAILURE", "error": str(e)}

    def stop_systemtender(self, systemtender_id):
        """Request graceful shutdown of a systemtender's workers

        Sets a flag in the systemtender's archive database that workers check
        to gracefully stop after completing their current trial.

        This is a quick async operation - returns immediately.
        Workers will stop on their own timeline (check every trial).

        Monitor via Prometheus metrics for actual worker termination.
        """
        try:
            # Check if systemtender exists
            self.metadata_repo.create_table()
            systemtender_meta_data_row = self.metadata_repo.fetch_meta_data(systemtender_id)

            if not systemtender_meta_data_row or len(systemtender_meta_data_row) == 0:
                logger.warning(f"Systemtender with ID '{systemtender_id}' not found")
                return {
                    "result": "FAILURE",
                    "error": f"Systemtender with ID '{systemtender_id}' not found"
                }

            # Get the actual database name (systemtender_id is UUID, need DB name)
            __uuid_common_name = f"systemtender_{systemtender_id.replace('-', '_')}"

            # Set the shutdown flag in the systemtender's archive DB
            self.archive_repo.set_shutdown_requested(__uuid_common_name)

            logger.info(f"Graceful shutdown requested for systemtender: {systemtender_id}")
            return {
                "result": "SUCCESS",
                "message": "Graceful shutdown requested. Workers will stop after completing current trials.",
                "data": {
                    "systemtender_id": systemtender_id,
                    "shutdown_type": "graceful",
                    "note": "Monitor metrics for worker termination"
                }
            }
        except Exception as e:
            logger.error(f"Failed to request shutdown for systemtender {systemtender_id}: {e}")
            return {"result": "FAILURE", "error": str(e)}

    def delete_systemtender(self, systemtender_id, force=False):
        """Mark-and-destroy: the async executor behind DELETE's 202.

        The api enqueues this script and answers 202 immediately; THIS
        run is the deletion executor. Idempotent, single-flight
        (designs/2026-10-03):

        - claim the executor slot; a live claim means another executor
          is mid-flight — re-DELETE rides along, the client polls
        - cancel worker jobs once: windmill ENFORCES death against
          running processes (poll 500ms/2s, SIGTERM 5s, SIGKILL+reap;
          ~8s bounded, source-verified 10-05)
        - wait bounded for terminal job states; past the bound windmill
          itself is wedged -> deletion-failed, the client retries
        - drop the archive database ONLY after confirmed death — the
          relcache race (#399) is the one non-negotiable
        - remove the metadata row LAST: the row is the finalizer, the
          tender stays visible in "deleting" until it goes

        `force` is legacy: accepted for CLI compatibility, no longer
        branches — there is no graceful-delete path left to bypass,
        and shutdown_requested stays the graceful STOP mechanism.
        """
        try:
            self.metadata_repo.create_table()
            systemtender_meta_data_row = self.metadata_repo.fetch_meta_data(systemtender_id)

            if not systemtender_meta_data_row or len(systemtender_meta_data_row) == 0:
                # The tender is gone — deleted by an earlier executor,
                # or never existed. Both are a completed delete.
                self.metadata_repo.remove_lifecycle(systemtender_id)
                logger.info(f"Delete of systemtender {systemtender_id}: already gone")
                return {
                    "result": "SUCCESS",
                    "data": {
                        "systemtender_id": systemtender_id,
                        "delete_type": "already-gone",
                    }
                }

            # Single-flight claim: exactly one executor per tender.
            if not self.metadata_repo.claim_for_deletion(
                    systemtender_id, stale_after_seconds=JOB_CLAIM_STALE_SECONDS):
                lifecycle = self.metadata_repo.get_lifecycle(systemtender_id) or {}
                return {
                    "result": "SUCCESS",
                    "data": {
                        "systemtender_id": systemtender_id,
                        "status": lifecycle.get("state", "deleting"),
                        "reason": lifecycle.get("reason"),
                        "note": "deletion already in progress",
                    }
                }

            # Extract worker job IDs from metadata (4th column is definition/JSONB)
            systemtender_config = systemtender_meta_data_row[0][3]
            worker_job_ids = systemtender_config.get('worker_job_ids', [])

            __uuid_common_name = f"systemtender_{systemtender_id.replace('-', '_')}"

            def _fail(reason):
                """Flag deletion-failed and stop. The client (or a human)
                retries with re-DELETE, which takes a fresh claim."""
                logger.error(f"Deletion failed for systemtender {systemtender_id}: {reason}")
                self.metadata_repo.set_lifecycle(systemtender_id, 'deletion-failed', reason)
                return {
                    "result": "FAILURE",
                    "error": reason,
                    "data": {
                        "systemtender_id": systemtender_id,
                        "status": "deletion-failed",
                    }
                }

            # Cancel once. A cancel that did not LAND (API error) leaves
            # a live worker behind — dropping now is the relcache race
            # (#399). Fail closed into deletion-failed.
            canceled_count = 0
            for job_id in worker_job_ids:
                if not cancel_job_by_id(job_id, reason=f"Deleting systemtender {systemtender_id}"):
                    return _fail(
                        f"cancel refused by windmill API for job {job_id} - "
                        "nothing dropped; retry the delete"
                    )
                canceled_count += 1
            logger.info(f"Cancelled {canceled_count}/{len(worker_job_ids)} worker jobs for systemtender {systemtender_id}")

            # Bounded wait for confirmed death. Expected ~8s (windmill
            # SIGTERM->SIGKILL); past the bound, windmill is wedged.
            stragglers = wait_for_jobs_to_quiesce(worker_job_ids)
            if stragglers:
                return _fail(
                    f"worker job(s) not terminal within "
                    f"{JOB_QUIESCE_TIMEOUT_SECONDS}s - windmill wedged, "
                    "nothing dropped: " + ", ".join(stragglers)
                )

            # Read group before metadata is removed
            det_cfg = systemtender_config.get('interference_detection', systemtender_config.get('detection', {}))
            group_id = det_cfg.get('group', systemtender_config.get('group', 'default'))

            # Drop the archive database — IF EXISTS, so a retry that
            # already dropped is a no-op. Death is confirmed above: the
            # relcache race (#399) is behind us from here on.
            self.archive_repo.drop_database(__uuid_common_name)

            # Clean coordination state — safe now because the worker
            # processes are confirmed dead (canceled + reaped above).
            self.archive_repo.cleanup_coordination_state(systemtender_id)

            # Curves follow the systemtender: clear causal's in-memory registry
            # AND its persisted rows. Best-effort with error log — deletion
            # succeeds even if causal is unreachable; leftover ghosts are
            # visible in GET /curves and sweepable via the same endpoint
            # (rows alone would replay back into the registry on restart).
            try:
                import urllib.request
                causal_url = os.environ.get(
                    'GODON_CAUSAL_URL', 'http://godon-godon-causal:9091')
                req = urllib.request.Request(
                    f"{causal_url}/curves/{systemtender_id}", method='DELETE')
                with urllib.request.urlopen(req, timeout=10) as resp:
                    logger.info(
                        f"Causal curve purge for {systemtender_id}: HTTP {resp.status}")
            except Exception as e:
                logger.warning(
                    f"Causal curve purge failed for {systemtender_id} "
                    f"(ghost curves may linger in /curves): {e}")

            # If this was the last systemtender in the group, purge the lease row
            remaining_in_group = self._count_systemtenders_in_group(group_id)
            if remaining_in_group == 0:
                self.archive_repo.cleanup_group_lease(group_id)
                logger.info(f"Purged group lease — last systemtender in group '{group_id}' deleted")

            # The metadata row is the finalizer: removed LAST, so the
            # tender stays visible in "deleting" until everything above
            # has landed. From the client's side, GET answering 404 is
            # the deletion-done receipt.
            self.metadata_repo.remove_systemtender_meta(systemtender_id)
            self.metadata_repo.remove_lifecycle(systemtender_id)

            logger.info(f"Successfully deleted systemtender: {systemtender_id}")
            return {
                "result": "SUCCESS",
                "data": {
                    "systemtender_id": systemtender_id,
                    "delete_type": "executed",
                    "workers_cancelled": canceled_count
                }
            }
        except Exception as e:
            logger.error(f"Failed to delete systemtender {systemtender_id}: {e}")
            return {"result": "FAILURE", "error": str(e)}

    def list_systemtenders(self):
        """List all systemtenders"""
        try:
            import dateutil.parser

            self.metadata_repo.create_table()
            systemtender_meta_data_list = self.metadata_repo.fetch_systemtenders_list()

            if not systemtender_meta_data_list:
                return {
                    "result": "SUCCESS",
                    "data": []
                }

            # Deletion markers ride on top: a tender being deleted (or
            # deletion-failed) shows its state machine status, not the
            # hardcoded "active" (designs/2026-10-03).
            lifecycle_states = self.metadata_repo.get_all_lifecycle()

            configured_systemtenders = []
            for row in systemtender_meta_data_list:
                systemtender_id = row[0]
                name = row[1]
                creation_ts = row[2]

                if isinstance(creation_ts, str):
                    created_at = dateutil.parser.parse(creation_ts).isoformat()
                else:
                    created_at = creation_ts.isoformat()

                configured_systemtenders.append({
                    "id": systemtender_id,
                    "name": name,
                    "status": lifecycle_states.get(str(systemtender_id), "active"),
                    "createdAt": created_at
                })

            return {
                "result": "SUCCESS",
                "data": configured_systemtenders
            }
        except Exception as e:
            return {
                "result": "FAILURE",
                "error": str(e)
            }

    def _count_systemtenders_in_group(self, group_id):
        """Count remaining systemtenders in the same group after a deletion."""
        try:
            self.metadata_repo.create_table()
            systemtenders = self.metadata_repo.fetch_systemtenders_list()
            count = 0
            for row in systemtenders:
                bid = row[0]
                meta = self.metadata_repo.fetch_meta_data(bid)
                if meta and len(meta) > 0:
                    cfg = meta[0][3]
                    if isinstance(cfg, dict):
                        det_cfg = cfg.get('interference_detection', cfg.get('detection', {}))
                        bgroup = det_cfg.get('group', cfg.get('group', 'default'))
                        if bgroup == group_id:
                            count += 1
            return count
        except Exception as e:
            logger.warning(f"Failed to count systemtenders in group {group_id}: {e}")
            return -1

    def _count_config_params(self, config):
        new_param_count = 0
        for category in ['sysctl', 'sysfs', 'cpufreq', 'ethtool']:
            category_settings = config.get('settings', {}).get(category, {})
            for param_name, param_config in category_settings.items():
                if category == 'ethtool':
                    if isinstance(param_config, dict):
                        new_param_count += len(param_config)
                else:
                    new_param_count += 1
        if new_param_count == 0:
            for category_name, category_params in config.get('settings', {}).items():
                if isinstance(category_params, dict):
                    for param_name, param_config in category_params.items():
                        if isinstance(param_config, dict) and 'constraints' in param_config:
                            new_param_count += 1
        return new_param_count

    def _check_trial_compatibility(self, systemtender_id, new_config, force=False):
        if force:
            return True, "Force mode: skipping compatibility check"

        try:
            import optuna

            db_name = f"systemtender_{systemtender_id.replace('-', '_')}"
            db_url = self.archive_repo.get_connection_url(db_name)
            storage = optuna.storages.RDBStorage(url=db_url)

            studies = storage.get_all_study_names()
            if not studies:
                return True, "No existing trial data found"

            study = optuna.load_study(study_name=studies[0], storage=storage)
            completed_trials = [t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE]
            if not completed_trials:
                return True, "No completed trials"

            sample_trial = completed_trials[-1]
            old_param_count = len(sample_trial.params)
            old_obj_count = len(sample_trial.values) if sample_trial.values else 0

            new_param_count = self._count_config_params(new_config)
            new_obj_count = len(new_config.get('objectives', []))

            incompatibilities = []
            if old_param_count != new_param_count:
                incompatibilities.append(
                    f"Parameter count: existing trials have {old_param_count}, new config has {new_param_count}"
                )
            if old_obj_count != new_obj_count:
                incompatibilities.append(
                    f"Objective count: existing trials have {old_obj_count}, new config has {new_obj_count}"
                )

            if incompatibilities:
                detail = "; ".join(incompatibilities)
                return False, f"Trial data incompatible: {detail}. Use force=true to clear trial history and start fresh."

            return True, f"Compatible: {old_param_count} params, {old_obj_count} objectives"

        except Exception as e:
            logger.warning(f"Could not check trial compatibility: {e}")
            return True, f"Compatibility check skipped: {e}"

    def _clear_trial_data(self, systemtender_id):
        db_name = f"systemtender_{systemtender_id.replace('-', '_')}"
        db_config = self.archive_repo.base_config.copy()
        db_config['database'] = db_name

        tables_to_drop = ['trials', 'study_directions', 'study_user_attributes',
                          'study_system_attributes', 'trial_user_attributes',
                          'trial_system_attributes', 'trial_params',
                          'trial_values', 'trial_intermediate_values',
                          'study', 'alembic_version']

        for table in tables_to_drop:
            try:
                execute_query(db_config, f"DROP TABLE IF EXISTS {table} CASCADE;")
            except Exception:
                pass

        logger.info(f"Cleared trial data for systemtender: {systemtender_id}")

    def update_systemtender(self, systemtender_id, new_config, force=False):
        try:
            self.metadata_repo.create_table()
            systemtender_meta_data_row = self.metadata_repo.fetch_meta_data(systemtender_id)

            if not systemtender_meta_data_row or len(systemtender_meta_data_row) == 0:
                return {"result": "FAILURE", "error": f"Systemtender with ID '{systemtender_id}' not found"}

            old_config = systemtender_meta_data_row[0][3]
            systemtender_instance_name = systemtender_meta_data_row[0][1]

            new_config['systemtender'] = new_config.get('systemtender', old_config.get('systemtender', {}))
            new_config['systemtender']['uuid'] = systemtender_id

            self._resolve_target_refs(new_config)
            SystemtenderConfig.validate_minimal(new_config)

            compatible, compat_detail = self._check_trial_compatibility(systemtender_id, new_config, force=force)
            if not compatible:
                return {"result": "FAILURE", "error": compat_detail}

            logger.info(f"Trial compatibility: {compat_detail}")

            __uuid_common_name = f"systemtender_{systemtender_id.replace('-', '_')}"
            self.archive_repo.set_shutdown_requested(__uuid_common_name, value=True)

            import time
            time.sleep(2)

            old_worker_job_ids = old_config.get('worker_job_ids', [])
            if old_worker_job_ids:
                for job_id in old_worker_job_ids:
                    cancel_job_by_id(job_id, reason=f"Updating systemtender {systemtender_id}")
                logger.info(f"Cancelled {len(old_worker_job_ids)} old worker jobs")

            if force:
                self._clear_trial_data(systemtender_id)
                logger.info(f"Force mode: cleared trial data for systemtender {systemtender_id}")

            config_history = old_config.get('config_history', [])
            config_history.append({
                'config': {k: v for k, v in old_config.items() if k != 'config_history'},
                'updated_at': datetime.datetime.now().isoformat()
            })
            new_config['config_history'] = config_history

            creation_ts_str = old_config.get('creation_ts')
            if creation_ts_str:
                new_config['creation_ts'] = creation_ts_str

            self.metadata_repo.update_systemtender_meta(systemtender_id=systemtender_id, meta_state=new_config)

            parallel_runs = new_config.get('run', {}).get('parallel', 1)
            targets = new_config.get('effectuation', {}).get('targets', [])
            targets_count = len(targets)
            is_cooperative = new_config.get('cooperation', {}).get('active', False)

            worker_launch_failures = []
            target_count = 0
            worker_job_ids = []

            for target in targets:
                for run_id in range(parallel_runs):
                    flow_config = new_config.copy()
                    flow_id = f'{systemtender_instance_name}_{target_count}_{run_id}'

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
                            systemtender_id=systemtender_id
                        )
                        worker_job_ids.append(job_id)
                    except Exception as e:
                        worker_launch_failures.append({
                            "flow_id": flow_id,
                            "target": target_count,
                            "run": run_id,
                            "error": str(e),
                            "error_type": type(e).__name__
                        })
                        logger.error(f"Failed to launch worker {flow_id}: {e}")

                target_count += 1

            if worker_job_ids:
                new_config['worker_job_ids'] = worker_job_ids
                self.metadata_repo.update_systemtender_meta(systemtender_id=systemtender_id, meta_state=new_config)

            if worker_launch_failures:
                return {
                    "result": "PARTIAL_SUCCESS",
                    "error": f"Failed to launch {len(worker_launch_failures)} worker(s)",
                    "workers_started": len(worker_job_ids),
                    "workers_failed": len(worker_launch_failures),
                    "trials_cleared": force
                }

            logger.info(f"Successfully updated systemtender: {systemtender_id} (force={force})")
            return {
                "result": "SUCCESS",
                "data": {
                    "systemtender_id": systemtender_id,
                    "name": systemtender_instance_name,
                    "status": "active",
                    "workers_started": len(worker_job_ids),
                    "trials_cleared": force,
                    "config_history_entries": len(config_history)
                }
            }

        except Exception as e:
            logger.error(f"Failed to update systemtender {systemtender_id}: {e}")
            return {"result": "FAILURE", "error": str(e)}