from controller.config import DatabaseConfig, BreederConfig
from breeder_service import BreederService

def main(breeder_config=None):
    if not breeder_config or 'breeder' not in breeder_config:
        return {"result": "FAILURE", "error": "Missing breeder configuration"}

    breeder_config = BreederConfig.extract_breeder_config(breeder_config)

    service = BreederService(
        archive_db_config=DatabaseConfig.ARCHIVE_DB,
        meta_db_config=DatabaseConfig.META_DB
    )

    return service.create_breeder(breeder_config)
