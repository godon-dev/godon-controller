from f.controller.config import DatabaseConfig
from f.controller.systemtender_service import SystemtenderService

def main(request_data=None):
    systemtender_id = request_data.get('systemtender_id') if request_data else None
    if not systemtender_id:
        return {"result": "FAILURE", "error": "Missing systemtender_id"}

    service = SystemtenderService(
        archive_db_config=DatabaseConfig.ARCHIVE_DB,
        meta_db_config=DatabaseConfig.META_DB
    )

    return service.start_systemtender(systemtender_id)
