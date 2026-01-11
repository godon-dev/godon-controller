from f.controller.config import DatabaseConfig, BreederConfig
from f.controller.breeder_service import BreederService

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

    # Extract breeder config from request
    if 'config' not in request_data:
        return {"result": "FAILURE", "error": "Missing required field: config"}
    breeder_config = request_data['config']

    # Validate config is not empty
    if not breeder_config or not isinstance(breeder_config, dict):
        return {"result": "FAILURE", "error": "Invalid config: config must be a non-empty object"}

    # Validate we have the breeder key in the config
    if 'breeder' not in breeder_config:
        return {"result": "FAILURE", "error": "Missing breeder configuration: 'breeder' key not found in config"}

    service = BreederService(
        archive_db_config=DatabaseConfig.ARCHIVE_DB,
        meta_db_config=DatabaseConfig.META_DB
    )

    return service.create_breeder(breeder_config, name)
