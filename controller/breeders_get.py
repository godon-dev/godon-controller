from f.controller.config import DatabaseConfig
from f.controller.breeder_service import BreederService

def main(request_data=None):
    service = BreederService(
        archive_db_config=DatabaseConfig.ARCHIVE_DB,
        meta_db_config=DatabaseConfig.META_DB
    )

    return service.list_breeders()
