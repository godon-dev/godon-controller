from f.controller.config import DatabaseConfig
from f.controller.database import MetadataDatabaseRepository
from f.controller.shared.otel_logging import get_logger
import uuid
import re

logger = get_logger(__name__)

SPEC_REQUIRED_FIELDS = {
    "ssh": ["address"],
    "http": ["url"],
}


def main(request_data=None):
    """Create a new target catalog entry"""
    if not request_data:
        return {"result": "FAILURE", "error": "Missing request data"}

    try:
        name = request_data.get('name')
        target_type = request_data.get('targetType')
        spec = request_data.get('spec', {})
        metadata = request_data.get('metadata', {})

        if not name:
            return {"result": "FAILURE", "error": "Missing required field: name"}

        if name.strip() == "":
            return {"result": "FAILURE", "error": "Invalid name: name cannot be empty"}

        if not target_type:
            return {"result": "FAILURE", "error": "Missing required field: targetType"}

        if not re.match(r'^[a-zA-Z0-9_-]{1,}$', name):
            return {
                "result": "FAILURE",
                "error": f"Invalid name format: '{name}'. Use only alphanumeric characters, hyphens, and underscores"
            }

        valid_types = ["ssh", "http"]
        if target_type not in valid_types:
            return {
                "result": "FAILURE",
                "error": f"Invalid targetType: '{target_type}'. Must be one of: {valid_types}"
            }

        if not isinstance(spec, dict):
            return {"result": "FAILURE", "error": "spec must be a JSON object"}

        required_fields = SPEC_REQUIRED_FIELDS.get(target_type, [])
        missing = [f for f in required_fields if f not in spec or not spec[f]]
        if missing:
            return {
                "result": "FAILURE",
                "error": f"Missing required spec fields for {target_type}: {', '.join(missing)}"
            }

        target_id = str(uuid.uuid4())

        meta_db = MetadataDatabaseRepository(DatabaseConfig.META_DB)

        try:
            meta_db.create_targets_table()
            meta_db.insert_target(
                target_id=target_id,
                name=name,
                target_type=target_type,
                spec=spec,
                metadata=metadata
            )
        except Exception as e:
            error_str = str(e).lower()
            if "duplicate key" in error_str or "unique constraint" in error_str:
                return {
                    "result": "FAILURE",
                    "error": f"Target with name '{name}' already exists"
                }
            else:
                logger.error(f"Database error creating target: {e}", exc_info=True)
                return {
                    "result": "FAILURE",
                    "error": f"Failed to create target: {str(e)}"
                }

        response_data = {
            "id": target_id,
            "name": name,
            "targetType": target_type,
            "spec": spec,
            "metadata": metadata,
            "createdAt": "now"
        }

        return {
            "result": "SUCCESS",
            "data": response_data
        }

    except Exception as e:
        logger.error(f"Failed to create target: {e}", exc_info=True)
        return {"result": "FAILURE", "error": str(e)}
