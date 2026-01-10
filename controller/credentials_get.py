from f.controller.config import DatabaseConfig
from f.controller.database import MetadataDatabaseRepository
import logging

logger = logging.getLogger(__name__)

def main(request_data=None):
    """Get list of all credentials"""
    try:
        meta_db = MetadataDatabaseRepository(DatabaseConfig.META_DB)

        credentials = meta_db.fetch_credentials_list()

        return {
            "result": "SUCCESS",
            "data": [
                {
                    "id": str(cred[0]),
                    "name": cred[1],
                    "credentialType": cred[2],
                    "description": cred[3],
                    "windmillVariable": cred[4],
                    "createdAt": cred[5].isoformat() if cred[5] else None,
                    "lastUsedAt": cred[6].isoformat() if cred[6] else None
                }
                for cred in credentials
            ]
        }

    except Exception as e:
        logger.error(f"Failed to fetch credentials: {e}", exc_info=True)
        return {"result": "FAILURE", "error": str(e)}