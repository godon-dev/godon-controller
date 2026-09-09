from f.controller.config import DatabaseConfig
from f.controller.systemtender_service import SystemtenderService

def main(request_data=None):
    systemtender_id = request_data.get('systemtender_id') if request_data else None
    if not systemtender_id:
        return {"result": "FAILURE", "error": "Missing systemtender_id"}

    new_config = request_data.get('config')
    if not new_config:
        return {"result": "FAILURE", "error": "Missing config"}

    force = request_data.get('force', False)

    service = SystemtenderService(
        archive_db_config=DatabaseConfig.ARCHIVE_DB,
        meta_db_config=DatabaseConfig.META_DB
    )

    return service.update_systemtender(systemtender_id, new_config, force=force)
