from f.controller.config import DatabaseConfig
from f.controller.database import MetadataDatabaseRepository
from f.controller.shared.otel_logging import get_logger

logger = get_logger(__name__)


def _format_target(row):
    return {
        "id": str(row[0]),
        "name": row[1],
        "targetType": row[2],
        "spec": row[3],
        "metadata": row[4],
        "createdAt": row[5].isoformat() if row[5] else None,
        "lastUsedAt": row[6].isoformat() if row[6] else None,
    }


def main(request_data=None):
    """Get a specific target by ID"""
    target_id = request_data.get('targetId') if request_data else None
    if not target_id:
        return {"result": "FAILURE", "error": "Missing targetId parameter"}

    try:
        meta_db = MetadataDatabaseRepository(DatabaseConfig.META_DB)
        meta_db.create_targets_table()

        target = meta_db.fetch_target_by_id(target_id)

        if not target:
            return {
                "result": "FAILURE",
                "error": f"Target with ID '{target_id}' not found"
            }

        return {
            "result": "SUCCESS",
            "data": _format_target(target)
        }

    except Exception as e:
        logger.error(f"Failed to fetch target: {e}", exc_info=True)
        return {"result": "FAILURE", "error": str(e)}
