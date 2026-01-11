from f.controller.config import DatabaseConfig
from f.controller.database import MetadataDatabaseRepository
import uuid
import logging

logger = logging.getLogger(__name__)

def main(request_data=None):
    """Create a new credential catalog entry"""
    if not request_data:
        return {"result": "FAILURE", "error": "Missing request data"}

    try:
        # Extract required fields directly from request_data
        name = request_data.get('name')
        credential_type = request_data.get('credentialType')  # Accept camelCase from API
        content = request_data.get('content')
        description = request_data.get('description', '')

        # Validate required fields
        if name is None:
            return {
                "result": "FAILURE",
                "error": "Missing required field: name"
            }

        if name.strip() == "":
            return {
                "result": "FAILURE",
                "error": "Invalid name: name cannot be empty"
            }

        if credential_type is None:
            return {
                "result": "FAILURE",
                "error": "Missing required field: credentialType"
            }

        if content is None or content.strip() == "":
            return {
                "result": "FAILURE",
                "error": "Missing required field: content (cannot be empty)"
            }

        # Validate name format (must be 1+ characters, alphanumeric/hyphen/underscore only)
        import re
        if not re.match(r'^[a-zA-Z0-9_-]{1,}$', name):
            return {
                "result": "FAILURE",
                "error": f"Invalid name format: '{name}'. Use only alphanumeric characters, hyphens, and underscores (1-255 characters)"
            }

        # Validate credential type
        valid_types = ["ssh_private_key", "api_token", "database_connection", "http_basic_auth"]
        if credential_type not in valid_types:
            return {
                "result": "FAILURE",
                "error": f"Invalid credentialType: '{credential_type}'. Must be one of: {valid_types}"
            }

        # Generate credential ID and Windmill variable path
        credential_id = str(uuid.uuid4())
        windmill_variable = f"f/vars/{name}"

        # Store credential catalog entry in database
        meta_db = MetadataDatabaseRepository(DatabaseConfig.META_DB)
        
        try:
            meta_db.create_credentials_table()
            meta_db.insert_credential(
                credential_id=credential_id,
                name=name,
                credential_type=credential_type,
                description=description,
                windmill_variable=windmill_variable,
                store_type='windmill_variable',
                metadata={}
            )
        except Exception as e:
            error_str = str(e).lower()
            if "duplicate key" in error_str or "unique constraint" in error_str:
                return {
                    "result": "FAILURE",
                    "error": f"Credential with name '{name}' already exists"
                }
            else:
                # Log the full error for debugging but return a clean error to client
                logger.error(f"Database error creating credential: {e}", exc_info=True)
                return {
                    "result": "FAILURE",
                    "error": f"Failed to create credential: {str(e)}"
                }

        return {
            "result": "SUCCESS",
            "data": {
                "id": credential_id,
                "name": name,
                "credentialType": credential_type,
                "description": description,
                "windmillVariable": windmill_variable,
                "createdAt": "now"
            }
        }

    except Exception as e:
        logger.error(f"Failed to create credential: {e}", exc_info=True)
        return {"result": "FAILURE", "error": str(e)}