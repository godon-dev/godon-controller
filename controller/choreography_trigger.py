import uuid
import json
from f.controller.config import DatabaseConfig
from f.controller.database import ArchiveDatabaseRepository
from f.controller.shared.otel_logging import get_logger

logger = get_logger(__name__)


def main(request_data=None):
    if not request_data:
        return {"result": "FAILURE", "error": "Missing request_data"}

    breeder_ids = request_data.get('breeder_ids', [])

    if not breeder_ids or len(breeder_ids) < 2:
        return {"result": "FAILURE", "error": "Need at least 2 breeder_ids"}

    repo = ArchiveDatabaseRepository(DatabaseConfig.ARCHIVE_DB)
    repo.create_choreography_table()

    return _trigger_choreography(repo, breeder_ids)


def _trigger_choreography(repo, breeder_ids):
    choreography_id = str(uuid.uuid4())

    phases = []
    for breeder_id in breeder_ids:
        phases.append({"observe_breeder": None, "label": "baseline"})
        phases.append({"observe_breeder": breeder_id, "label": "observe"})
        phases.append({"observe_breeder": None, "label": "recovery"})

    repo.insert_choreography(choreography_id, breeder_ids, phases)

    logger.info(f"Triggered choreography {choreography_id} for breeders {breeder_ids}")
    return {
        "result": "SUCCESS",
        "choreography_id": choreography_id,
        "participants": breeder_ids,
        "phases": len(phases)
    }
