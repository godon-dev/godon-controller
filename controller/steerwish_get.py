from f.controller.config import DatabaseConfig
from f.controller.steerwish_service import SteerwishService
from f.controller.shared.otel_logging import get_logger

logger = get_logger(__name__)


def main(request_data=None):
    """Fetch one steerwish with its full event history.

    Unknown wish_id fails with 'not found' in the error text
    (the API maps that to HTTP 404).
    """
    wish_id = (request_data or {}).get('wish_id')
    if not wish_id:
        return {"result": "FAILURE", "error": "Missing wish_id"}

    try:
        service = SteerwishService(DatabaseConfig.META_DB)
        wish = service.get_steerwish(wish_id)
        if wish is None:
            return {"result": "FAILURE", "error": f"steerwish not found: {wish_id}"}
        return wish
    except Exception as e:
        logger.error(f"Failed to fetch steerwish: {e}", exc_info=True)
        return {"result": "FAILURE", "error": str(e)}
