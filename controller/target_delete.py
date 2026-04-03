from f.controller.config import DatabaseConfig
from f.controller.database import MetadataDatabaseRepository
from f.controller.shared.otel_logging import get_logger

logger = get_logger(__name__)

def main(request_data=None):
    """Delete a target by ID"""
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

        meta_db.delete_target(target_id)

        return {
            "result": "SUCCESS",
            "data": None
        }

    except Exception as e:
        logger.error(f"Failed to delete target: {e}", exc_info=True)
        return {"result": "FAILURE", "error": str(e)}
