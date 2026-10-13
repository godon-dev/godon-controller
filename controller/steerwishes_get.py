from f.controller.config import DatabaseConfig
from f.controller.wish_service import WishService
from f.controller.shared.otel_logging import get_logger

logger = get_logger(__name__)


def main(request_data=None):
    """List all steerwishes (summaries, no event history), newest first."""
    try:
        service = WishService(DatabaseConfig.META_DB)
        return service.list_steerwishes()
    except Exception as e:
        logger.error(f"Failed to list steerwishes: {e}", exc_info=True)
        return {"result": "FAILURE", "error": str(e)}
