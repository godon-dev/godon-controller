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

    def get_connection_url(self, breeder_id):
        """Get PostgreSQL connection URL for a breeder database"""
        return (
            f"postgresql://{self.base_config['user']}:"
            f"{self.base_config['password']}@"
            f"{self.base_config['host']}:"
            f"{self.base_config['port']}/"
            f"{breeder_id}"
        )

class MetadataDatabaseRepository:
    """Repository for metadata database operations"""

    def __init__(self, base_config):
        self.base_config = base_config.copy()
        self.breeder_table_name = 'breeder_meta_data'
        self.credentials_table_name = 'credentials'

    def _get_db_config(self):
        """Get database config with metadata database name"""
        db_config = self.base_config.copy()
        db_config['database'] = 'meta_data'
        return db_config

    def create_table(self):
        """Create the breeder metadata table"""
        db_config = self._get_db_config()

        query = f"""
        CREATE TABLE IF NOT EXISTS {self.breeder_table_name}
        (
        id uuid PRIMARY KEY,
        name VARCHAR(255) NOT NULL DEFAULT '',
        creation_tsz TIMESTAMPTZ,
        definition jsonb NOT NULL
        );
        """

        execute_query(db_config, query)
        logger.info(f"Ensured metadata table exists: {self.breeder_table_name}")

    def create_credentials_table(self):
        """Create the credentials catalog table"""
        db_config = self._get_db_config()

        query = f"""
        CREATE TABLE IF NOT EXISTS {self.credentials_table_name}
        (
        id uuid PRIMARY KEY,
        name VARCHAR(255) UNIQUE NOT NULL,
        credential_type VARCHAR(50) NOT NULL,
        description TEXT,
        windmill_variable VARCHAR(255) NOT NULL,
        store_type VARCHAR(50) DEFAULT 'windmill_variable',
        metadata JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        last_used_at TIMESTAMPTZ,
        last_verified_at TIMESTAMPTZ
        );
        """

        execute_query(db_config, query)
        logger.info(f"Ensured credentials table exists: {self.credentials_table_name}")

    def insert_breeder_meta(self, breeder_id, name, creation_ts, meta_state):
        """Insert breeder metadata"""
        db_config = self._get_db_config()
        json_string = json.dumps(meta_state).replace("'", "''")
        name_escaped = name.replace("'", "''")

        query = f"""
        INSERT INTO {self.breeder_table_name} (id, name, creation_tsz, definition)
        VALUES('{breeder_id}', '{name_escaped}', '{creation_ts}', '{json_string}');
        """

        execute_query(db_config, query)
        logger.info(f"Inserted metadata for breeder: {breeder_id}")

    def remove_breeder_meta(self, breeder_id):
        """Remove breeder metadata"""
        db_config = self._get_db_config()

        query = f"DELETE FROM {self.breeder_table_name} WHERE id = '{breeder_id}';"
        execute_query(db_config, query)
        logger.info(f"Removed metadata for breeder: {breeder_id}")

    def fetch_meta_data(self, breeder_id):
        """Fetch metadata for a specific breeder"""
        db_config = self._get_db_config()

        query = f"""
        SELECT id, name, creation_tsz, definition FROM {self.breeder_table_name} WHERE id = '{breeder_id}';
        """

        return execute_query(db_config, query, with_result=True)

    def fetch_breeders_list(self):
        """Fetch list of all breeders"""
        db_config = self._get_db_config()

        query = f"""
        SELECT id, name, creation_tsz FROM {self.breeder_table_name};
        """

        result = execute_query(db_config, query, with_result=True)
        return result if result else []
    
    # Credential management methods
    
    def insert_credential(self, credential_id, name, credential_type, description, windmill_variable, store_type='windmill_variable', metadata=None):
        """Insert credential catalog entry"""
        db_config = self._get_db_config()
        metadata_json = json.dumps(metadata) if metadata else 'NULL'
        description_escaped = "'" + description.replace("'", "''") + "'" if description else 'NULL'
        
        query = f"""
        INSERT INTO {self.credentials_table_name} 
        (id, name, credential_type, description, windmill_variable, store_type, metadata)
        VALUES('{credential_id}', '{name}', '{credential_type}', {description_escaped}, 
                '{windmill_variable}', '{store_type}', {metadata_json}::jsonb);
        """
        
        execute_query(db_config, query)
        logger.info(f"Inserted credential catalog entry: {name}")
    
    def fetch_credentials_list(self):
        """Fetch list of all credentials"""
        db_config = self._get_db_config()
        
        query = f"""
        SELECT id, name, credential_type, description, windmill_variable, created_at, last_used_at 
        FROM {self.credentials_table_name} 
        ORDER BY created_at DESC;
        """
        
        result = execute_query(db_config, query, with_result=True)
        return result if result else []
    
    def fetch_credential_by_id(self, credential_id):
        """Fetch credential by ID"""
        db_config = self._get_db_config()
        
        query = f"""
        SELECT id, name, credential_type, description, windmill_variable, store_type, metadata, created_at, last_used_at 
        FROM {self.credentials_table_name} 
        WHERE id = '{credential_id}';
        """
        
        result = execute_query(db_config, query, with_result=True)
        return result[0] if result else None
    
    def fetch_credential_by_name(self, name):
        """Fetch credential by name"""
        db_config = self._get_db_config()
        
        query = f"""
        SELECT id, name, credential_type, description, windmill_variable, store_type, metadata, created_at, last_used_at 
        FROM {self.credentials_table_name} 
        WHERE name = '{name}';
        """
        
        result = execute_query(db_config, query, with_result=True)
        return result[0] if result else None
    
    def delete_credential(self, credential_id):
        """Delete credential from catalog"""
        db_config = self._get_db_config()
        
        query = f"DELETE FROM {self.credentials_table_name} WHERE id = '{credential_id}';"
        execute_query(db_config, query)
        logger.info(f"Deleted credential catalog entry: {credential_id}")
    
    def update_credential_last_used(self, credential_id):
        """Update the last_used_at timestamp for a credential"""
        db_config = self._get_db_config()
        
        query = f"""
        UPDATE {self.credentials_table_name} 
        SET last_used_at = NOW() 
        WHERE id = '{credential_id}';
        """
        
        execute_query(db_config, query)