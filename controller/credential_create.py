from f.controller.config import DatabaseConfig
from f.controller.database import MetadataDatabaseRepository
import uuid
import logging

logger = logging.getLogger(__name__)

def main(credential_data=None):
    """Create a new credential catalog entry"""
    if not credential_data:
        return {"result": "FAILURE", "error": "Missing credential data"}

    try:
        # Extract required fields
        name = credential_data.get('name')
        credential_type = credential_data.get('credential_type')
        description = credential_data.get('description', '')
        
        if not name or not credential_type:
            return {
                "result": "FAILURE", 
                "error": "Missing required fields: name, credential_type"
            }

        # Validate name format
        import re
        if not re.match(r'^[a-zA-Z0-9_-]+$', name):
            return {
                "result": "FAILURE",
                "error": "Invalid name format. Use only alphanumeric, hyphens, and underscores"
            }

        # Validate credential type
        valid_types = ["ssh_private_key", "api_token", "database_connection", "http_basic_auth"]
        if credential_type not in valid_types:
            return {
                "result": "FAILURE",
                "error": f"Invalid credential_type. Must be one of: {valid_types}"
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
            if "duplicate key" in str(e).lower():
                return {
                    "result": "FAILURE",
                    "error": f"Credential with name '{name}' already exists"
                }
            raise

        return {
            "result": "SUCCESS",
            "credential": {
                "id": credential_id,
                "name": name,
                "credential_type": credential_type,
                "description": description,
                "windmill_variable": windmill_variable,
                "created_at": "now"
            }
        }

    except Exception as e:
        logger.error(f"Failed to create credential: {e}", exc_info=True)
        return {"result": "FAILURE", "error": str(e)}