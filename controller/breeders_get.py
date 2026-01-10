from f.controller.config import DatabaseConfig
from f.controller.breeder_service import BreederService

def main(request_data=None):
    service = BreederService(
        archive_db_config=DatabaseConfig.ARCHIVE_DB,
        meta_db_config=DatabaseConfig.META_DB
    )

    result = service.list_breeders()

    # If service call failed, return error as-is
    if result.get('result') == 'FAILURE':
        return result

    # Convert tuples to breeder summary objects
    breeders = []
    for breeder_id, name, created_at in result.get('breeders', []):
        breeders.append({
            'id': breeder_id,
            'name': name,
            'status': 'active',
            'createdAt': created_at
        })

    return breeders
