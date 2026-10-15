from f.controller.config import DatabaseConfig
from f.controller.wish_service import WishService
from f.controller.shared.otel_logging import get_logger

logger = get_logger(__name__)


def main(wish_id=None):
    """The plan ask as its own job (answer 'later', design §XIX): the
    door returned at 'declared'; this job asks causal to plan from the
    stored terms. Every answer - planned, refused with its numbers,
    plan_error - lands on the wish's event record and surfaces at
    serving time and in status. Same ask discipline as the inline
    path, one job per ask.
    """
    if not wish_id:
        return {"result": "FAILURE", "error": "Missing required field: wish_id"}
    try:
        service = WishService(DatabaseConfig.META_DB, DatabaseConfig.ARCHIVE_DB)
        wish = service.plan_wish(wish_id)
        if wish is None:
            return {"result": "FAILURE", "error": f"unknown wish: {wish_id}"}
        return {
            "result": "SUCCESS",
            "data": {"wish_id": wish_id, "state": wish.get("state")},
        }
    except Exception as e:
        logger.error(f"Failed to run plan ask for wish {wish_id}: {e}",
                     exc_info=True)
        return {"result": "FAILURE", "error": str(e)}
