from f.controller.config import DatabaseConfig
from f.controller.steerwish_service import SteerwishService

def main(request_data=None):
    data = request_data or {}
    wish_id = data.get('wish_id')
    if not wish_id:
        return {"result": "FAILURE", "error": "Missing wish_id"}

    service = SteerwishService(
        archive_db_config=DatabaseConfig.ARCHIVE_DB,
        meta_db_config=DatabaseConfig.META_DB
    )
    try:
        result = service.delete_steerwish(wish_id)
        if result is None:
            return {"result": "FAILURE", "error": f"Unknown wish: {wish_id}"}
        return result
    except Exception as e:
        return {"result": "FAILURE", "error": str(e)}
