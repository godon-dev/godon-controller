from f.controller.config import DatabaseConfig
from f.controller.steerwish_service import SteerwishService

def main(request_data=None):
    data = request_data or {}
    wish_id = data.get('wish_id')
    if not wish_id:
        return {"result": "FAILURE", "error": "Missing wish_id"}
    band = data.get('band')
    if not band:
        return {"result": "FAILURE", "error": "Missing band (lo/hi/target)"}

    service = SteerwishService(
        archive_db_config=DatabaseConfig.ARCHIVE_DB,
        meta_db_config=DatabaseConfig.META_DB
    )
    try:
        return service.update_steerwish(wish_id, {
            'band': band,
            'limits': data.get('limits'),
            'budget': data.get('budget'),
            'reason': data.get('reason'),
        })
    except Exception as e:
        return {"result": "FAILURE", "error": str(e)}
