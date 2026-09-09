from f.controller.config import DatabaseConfig
from f.controller.systemtender_service import SystemtenderService

def main(request_data=None):
    service = SystemtenderService(
        archive_db_config=DatabaseConfig.ARCHIVE_DB,
        meta_db_config=DatabaseConfig.META_DB
    )

    return service.list_systemtenders()
