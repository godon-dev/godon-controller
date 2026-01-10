from f.controller.config import DatabaseConfig
from f.controller.database import MetadataDatabaseRepository
import logging

logger = logging.getLogger(__name__)

def main(request_data=None):
    """Get a specific credential by ID"""
    credential_id = request_data.get('credentialId') if request_data else None
    if not credential_id:
        return {"result": "FAILURE", "error": "Missing credentialId parameter"}

    try:
        meta_db = MetadataDatabaseRepository(DatabaseConfig.META_DB)
        
        credential = meta_db.fetch_credential_by_id(credential_id)
        
        if not credential:
            return {
                "result": "FAILURE",
                "error": f"Credential with ID '{credential_id}' not found"
            }
        
        return {
            "result": "SUCCESS",
            "data": {
                "id": str(credential[0]),
                "name": credential[1],
                "credentialType": credential[2],
                "description": credential[3],
                "windmillVariable": credential[4],
                "storeType": credential[5],
                "metadata": credential[6],
                "createdAt": credential[7].isoformat() if credential[7] else None,
                "lastUsedAt": credential[8].isoformat() if credential[8] else None
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to fetch credential: {e}", exc_info=True)
        return {"result": "FAILURE", "error": str(e)}