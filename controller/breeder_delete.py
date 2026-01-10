from f.controller.config import DatabaseConfig
from f.controller.breeder_service import BreederService

def main(request_data=None):
    breeder_id = request_data.get('breeder_id') if request_data else None
    if not breeder_id:
        return {"result": "FAILURE", "error": "Missing breeder_id"}

    service = BreederService(
        archive_db_config=DatabaseConfig.ARCHIVE_DB,
        meta_db_config=DatabaseConfig.META_DB
    )

    return service.delete_breeder(breeder_id)

