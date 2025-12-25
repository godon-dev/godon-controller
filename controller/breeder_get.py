from config import DatabaseConfig
from breeder_service import BreederService

def main(breeder_id=None):
    if not breeder_id:
        return {"result": "FAILURE", "error": "Missing breeder_id"}

    service = BreederService(
        archive_db_config=DatabaseConfig.ARCHIVE_DB,
        meta_db_config=DatabaseConfig.META_DB
    )

    return service.get_breeder(breeder_id)
