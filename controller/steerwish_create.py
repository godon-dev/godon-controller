from f.controller.config import DatabaseConfig
from f.controller.steerwish_service import SteerwishService, SteerwishValidationError
from f.controller.shared.otel_logging import get_logger

logger = get_logger(__name__)


def main(request_data=None):
    """Declare a steerwish: validate at the door, stamp 'declared', return it.

    Malformed wishes are rejected before anything plans on them;
    the FAILURE error names what was wrong.
    """
    try:
        service = SteerwishService(DatabaseConfig.META_DB)
        return service.create_steerwish(request_data)
    except SteerwishValidationError as e:
        return {"result": "FAILURE", "error": str(e)}
    except Exception as e:
        logger.error(f"Failed to declare steerwish: {e}", exc_info=True)
        return {"result": "FAILURE", "error": str(e)}
