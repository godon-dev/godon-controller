from f.controller.config import DatabaseConfig
from f.controller.systemtender_service import SystemtenderService

def main(request_data=None):
    """The create executor: the async half of create's 202
    (designs/2026-10-10). Dispatched as its own job by the fast
    create path; no caller is waiting on this run. The row it reads
    is already planted in `creating` — this run does the heavy half
    and flips it live (or flags create-failed + reason)."""

    systemtender_id = request_data.get('systemtender_id') if request_data else None
    if not systemtender_id:
        return {"result": "FAILURE", "error": "Missing systemtender_id"}

    service = SystemtenderService(
        archive_db_config=DatabaseConfig.ARCHIVE_DB,
        meta_db_config=DatabaseConfig.META_DB
    )

    return service.create_systemtender_executor(systemtender_id)
