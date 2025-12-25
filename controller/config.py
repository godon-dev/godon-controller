import os

class DatabaseConfig:
    ARCHIVE_DB = dict(
        user="yugabyte",
        password="yugabyte", 
        host="yb-tservers.godon.svc.cluster.local",
        port=os.environ.get('YB_TSERVER_SERVICE_SERVICE_PORT_TCP_YSQL_PORT')
    )

    META_DB = dict(
        user="meta_data",
        password="meta_data",
        host=os.environ.get('GODON_METADATA_DB_SERVICE_HOST'),
        port=os.environ.get('GODON_METADATA_DB_SERVICE_PORT')
    )

class BreederConfig:
    @staticmethod
    def extract_breeder_config(request_data):
        if not request_data or 'breeder' not in request_data:
            raise ValueError("Invalid breeder configuration: missing 'breeder' key")

        return request_data.get('breeder', {})