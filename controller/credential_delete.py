from f.controller.config import DatabaseConfig
from f.controller.database import MetadataDatabaseRepository
import logging

logger = logging.getLogger(__name__)

def main(request_data=None):
    """Delete a credential by ID"""
    credential_id = request_data.get('credentialId') if request_data else None
    if not credential_id:
        return {"result": "FAILURE", "error": "Missing credentialId parameter"}

    try:
        meta_db = MetadataDatabaseRepository(DatabaseConfig.META_DB)
        meta_db.create_credentials_table()

        # Check if credential exists first
        credential = meta_db.fetch_credential_by_id(credential_id)
        if not credential:
            return {
                "result": "FAILURE",
                "error": f"Credential with ID '{credential_id}' not found"
            }
        
        # Delete from database catalog
        meta_db.delete_credential(credential_id)

        return {
            "result": "SUCCESS",
            "data": None
        }
        
    except Exception as e:
        logger.error(f"Failed to delete credential: {e}", exc_info=True)
        return {"result": "FAILURE", "error": str(e)}