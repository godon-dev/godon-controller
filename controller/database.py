import psycopg2
import logging
import json
from contextlib import contextmanager

logger = logging.getLogger(__name__)

@contextmanager
def get_db_connection(db_config):
    """Context manager for database connections"""
    connection = None
    try:
        connection = psycopg2.connect(**db_config)
        yield connection
    except psycopg2.OperationalError as e:
        logger.error(f"Database connection error: {e}")
        logger.error(f"Database config: {db_config}")
        raise
    finally:
        if connection:
            connection.close()
            logger.debug("Closed database connection")

def execute_query(db_config, query, with_result=False):
    """Execute a database query within a transaction"""
    try:
        with get_db_connection(db_config) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)

                if with_result:
                    return cursor.fetchall()

                connection.commit()
                return None

    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise

def execute_ddl_query(db_config, query):
    """Execute DDL statements that require autocommit (CREATE DATABASE, etc.)"""
    try:
        with get_db_connection(db_config) as connection:
            connection.set_session(autocommit=True)
            with connection.cursor() as cursor:
                cursor.execute(query)
                logger.debug(f"DDL executed: {query[:50]}...")
    except Exception as e:
        logger.error(f"DDL execution failed: {e}")
        raise

class ArchiveDatabaseRepository:
    """Repository for archive database operations"""

    def __init__(self, base_config):
        self.base_config = base_config.copy()

    def create_database(self, breeder_id):
        """Create a new database for a breeder"""
        db_config = self.base_config.copy()
        db_config['database'] = "archive_db"

        query = f"CREATE DATABASE {breeder_id};"
        execute_ddl_query(db_config, query)
        logger.info(f"Created archive database: {breeder_id}")

    def drop_database(self, breeder_id):
        """Drop a breeder database"""
        db_config = self.base_config.copy()
        db_config['database'] = "archive_db"

        query = f"DROP DATABASE IF EXISTS {breeder_id};"
        execute_ddl_query(db_config, query)
        logger.info(f"Dropped archive database: {breeder_id}")

class MetadataDatabaseRepository:
    """Repository for metadata database operations"""

    def __init__(self, base_config):
        self.base_config = base_config.copy()
        self.table_name = 'breeder_meta_data'

    def _get_db_config(self):
        """Get database config with metadata database name"""
        db_config = self.base_config.copy()
        db_config['database'] = 'meta_data'
        return db_config

    def create_table(self):
        """Create the breeder metadata table"""
        db_config = self._get_db_config()

        query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name}
        (
        id uuid PRIMARY KEY,
        creation_tsz TIMESTAMPTZ,
        definition jsonb NOT NULL
        );
        """

        execute_query(db_config, query)
        logger.info(f"Ensured metadata table exists: {self.table_name}")

    def insert_breeder_meta(self, breeder_id, creation_ts, meta_state):
        """Insert breeder metadata"""
        db_config = self._get_db_config()
        json_string = json.dumps(meta_state).replace("'", "''")

        query = f"""
        INSERT INTO {self.table_name} (id, creation_tsz, definition)
        VALUES('{breeder_id}', '{creation_ts}', '{json_string}');
        """

        execute_query(db_config, query)
        logger.info(f"Inserted metadata for breeder: {breeder_id}")

    def remove_breeder_meta(self, breeder_id):
        """Remove breeder metadata"""
        db_config = self._get_db_config()

        query = f"DELETE FROM {self.table_name} WHERE id = '{breeder_id}';"
        execute_query(db_config, query)
        logger.info(f"Removed metadata for breeder: {breeder_id}")

    def fetch_meta_data(self, breeder_id):
        """Fetch metadata for a specific breeder"""
        db_config = self._get_db_config()

        query = f"""
        SELECT id, creation_tsz, definition FROM {self.table_name} WHERE id = '{breeder_id}';
        """

        return execute_query(db_config, query, with_result=True)

    def fetch_breeders_list(self):
        """Fetch list of all breeders"""
        db_config = self._get_db_config()

        query = f"""
        SELECT id, definition->>'name', creation_tsz FROM {self.table_name};
        """

        result = execute_query(db_config, query, with_result=True)
        return result if result else []