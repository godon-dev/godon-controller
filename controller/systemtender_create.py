from f.controller.config import DatabaseConfig, SystemtenderConfig
from f.controller.systemtender_service import SystemtenderService

def main(request_data=None):
    if not request_data:
        return {"result": "FAILURE", "error": "Missing request data"}

    # Extract name from request (mandatory field)
    if 'name' not in request_data:
        return {"result": "FAILURE", "error": "Missing required field: name"}
    name = request_data['name']

    # Validate name is not empty
    if not name or name.strip() == "":
        return {"result": "FAILURE", "error": "Invalid name: name cannot be empty"}

    # Extract systemtender config from request
    if 'config' not in request_data:
        return {"result": "FAILURE", "error": "Missing required field: config"}
    systemtender_config = request_data['config']

    # Validate config is not empty
    if not systemtender_config or not isinstance(systemtender_config, dict):
        return {"result": "FAILURE", "error": "Invalid config: config must be a non-empty object"}

    # Validate we have the systemtender key in the config
    if 'systemtender' not in systemtender_config:
        return {"result": "FAILURE", "error": "Missing systemtender configuration: 'systemtender' key not found in config"}

    service = SystemtenderService(
        archive_db_config=DatabaseConfig.ARCHIVE_DB,
        meta_db_config=DatabaseConfig.META_DB
    )

    return service.create_systemtender(systemtender_config, name)
