from f.controller.config import DatabaseConfig
from f.controller.database import MetadataDatabaseRepository
from f.controller.shared.otel_logging import get_logger

logger = get_logger(__name__)

def main(request_data=None):
    """Get list of all targets"""
    try:
        meta_db = MetadataDatabaseRepository(DatabaseConfig.META_DB)
        meta_db.create_targets_table()

        targets = meta_db.fetch_targets_list()

        return {
            "result": "SUCCESS",
            "data": [
                {
                    "id": str(t[0]),
                    "name": t[1],
                    "targetType": t[2],
                    "address": t[3],
                    "username": t[4],
                    "credentialId": t[5],
                    "description": t[6],
                    "allowsDowntime": t[7],
                    "createdAt": t[8].isoformat() if t[8] else None,
                    "lastUsedAt": t[9].isoformat() if t[9] else None
                }
                for t in targets
            ]
        }

    except Exception as e:
        logger.error(f"Failed to fetch targets: {e}", exc_info=True)
        return {"result": "FAILURE", "error": str(e)}
