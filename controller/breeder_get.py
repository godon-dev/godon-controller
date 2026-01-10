from f.controller.config import DatabaseConfig
from f.controller.breeder_service import BreederService

def main(request_data=None):
    breeder_id = request_data.get('breeder_id') if request_data else None
    if not breeder_id:
        return {"result": "FAILURE", "error": "Missing breeder_id"}

    service = BreederService(
        archive_db_config=DatabaseConfig.ARCHIVE_DB,
        meta_db_config=DatabaseConfig.META_DB
    )

    result = service.get_breeder(breeder_id)

    # If service call failed, return error as-is
    if result.get('result') == 'FAILURE':
        return result

    # Extract and transform the breeder data to expected format
    import json
    breeder_data = json.loads(result['breeder_data'])

    # Transform to API-expected format
    return {
        'id': breeder_id,
        'name': breeder_data.get('breeder_definition', {}).get('name', breeder_id),
        'status': 'active',
        'createdAt': breeder_data.get('creation_timestamp'),
        'config': breeder_data.get('breeder_definition', {})
    }
