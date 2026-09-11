#extra_requirements:
#psycopg2-binary

import psycopg2
import json
from contextlib import contextmanager

from f.controller.shared.otel_logging import get_logger

logger = get_logger(__name__)

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

    def create_systemtender_state_table(self, systemtender_id):
        """Create the systemtender state table for shutdown signaling"""
        db_config = self.base_config.copy()
        db_config['database'] = systemtender_id

        query = """
        CREATE TABLE IF NOT EXISTS systemtender_state (
            id SERIAL PRIMARY KEY,
            shutdown_requested BOOLEAN DEFAULT FALSE,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );

        INSERT INTO systemtender_state (shutdown_requested) VALUES (FALSE)
        ON CONFLICT DO NOTHING;
        """

        execute_query(db_config, query)
        logger.info(f"Created systemtender state table for: {systemtender_id}")

    def set_shutdown_requested(self, systemtender_id, value=True):
        """Set or clear the shutdown requested flag in the systemtender's archive database

        Args:
            systemtender_id: The systemtender database name
            value: True to set shutdown flag, False to clear it
        """
        db_config = self.base_config.copy()
        db_config['database'] = systemtender_id

        query = f"""
        UPDATE systemtender_state SET shutdown_requested = {str(value).upper()}, updated_at = NOW();
        """

        execute_query(db_config, query)
        logger.info(f"Set shutdown_requested={value} in archive DB for systemtender: {systemtender_id}")

    def get_shutdown_requested(self, systemtender_id):
        """Check if shutdown has been requested for a systemtender"""
        db_config = self.base_config.copy()
        db_config['database'] = systemtender_id

        query = """
        SELECT shutdown_requested FROM systemtender_state;
        """

        result = execute_query(db_config, query, with_result=True)
        if result and len(result) > 0:
            return result[0][0]
        return False

    def create_database(self, systemtender_id):
        """Create a new database for a systemtender"""
        db_config = self.base_config.copy()
        db_config['database'] = "archive_db"

        query = f"CREATE DATABASE {systemtender_id};"
        execute_ddl_query(db_config, query)
        logger.info(f"Created archive database: {systemtender_id}")

    def drop_database(self, systemtender_id):
        """Drop a systemtender database"""
        db_config = self.base_config.copy()
        db_config['database'] = "archive_db"

        query = f"DROP DATABASE IF EXISTS {systemtender_id};"
        execute_ddl_query(db_config, query)
        logger.info(f"Dropped archive database: {systemtender_id}")

    def cleanup_coordination_state(self, systemtender_id):
        """Remove a systemtender's rows from coordination tables in archive_db.

        Called on systemtender deletion. Removes the systemtender from
        interference_active_systemtenders and detection_readiness so stale
        coordination state doesn't block other systemtenders in the group.
        Also removes the systemtender's receiver_observations and curve_points
        rows — measurement state follows the systemtender lifecycle (no ghost
        rows; causal's registry is cleared via its API in delete_systemtender).
        """
        db_config = self.base_config.copy()
        db_config['database'] = "archive_db"

        queries = [
            f"DELETE FROM interference_active_systemtenders WHERE systemtender_id = '{systemtender_id}';",
            f"DELETE FROM detection_readiness WHERE systemtender_id = '{systemtender_id}';",
            f"DELETE FROM receiver_observations WHERE receiver_id = '{systemtender_id}';",
            f"DELETE FROM curve_points WHERE sender_id = '{systemtender_id}';",
        ]

        for query in queries:
            try:
                execute_query(db_config, query)
            except Exception as e:
                logger.warning(f"Coordination cleanup query failed (table may not exist yet): {e}")

        logger.info(f"Cleaned coordination state for systemtender: {systemtender_id}")

    def cleanup_group_lease(self, group_id):
        """Remove the sender_lease row for a group with no remaining systemtenders.

        Called automatically when the last systemtender in a group is deleted.
        Per-systemtender coordination rows (interference_active_systemtenders,
        detection_readiness) are already cleaned by cleanup_coordination_state.
        """
        db_config = self.base_config.copy()
        db_config['database'] = "archive_db"

        try:
            execute_query(db_config, f"DELETE FROM sender_lease WHERE group_id = '{group_id}';")
        except Exception as e:
            logger.warning(f"Group lease cleanup failed (table may not exist yet): {e}")

    def get_connection_url(self, systemtender_id):
        """Get PostgreSQL connection URL for a systemtender database"""
        return (
            f"postgresql://{self.base_config['user']}:"
            f"{self.base_config['password']}@"
            f"{self.base_config['host']}:"
            f"{self.base_config['port']}/"
            f"{systemtender_id}"
        )


class MetadataDatabaseRepository:
    """Repository for metadata database operations"""

    def __init__(self, base_config):
        self.base_config = base_config.copy()
        self.systemtender_table_name = 'systemtender_meta_data'
        self.credentials_table_name = 'credentials'
        self.targets_table_name = 'targets'
        self.steerwishes_table_name = 'steerwishes'
        self.steerwish_events_table_name = 'steerwish_events'

    def _get_db_config(self):
        """Get database config with metadata database name"""
        db_config = self.base_config.copy()
        db_config['database'] = 'meta_data'
        return db_config

    def create_table(self):
        """Create the systemtender metadata table"""
        db_config = self._get_db_config()

        query = f"""
        CREATE TABLE IF NOT EXISTS {self.systemtender_table_name}
        (
        id uuid PRIMARY KEY,
        name VARCHAR(255) NOT NULL DEFAULT '',
        creation_tsz TIMESTAMPTZ,
        definition jsonb NOT NULL
        );
        """

        execute_query(db_config, query)
        logger.info(f"Ensured metadata table exists: {self.systemtender_table_name}")

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

    def insert_systemtender_meta(self, systemtender_id, name, creation_ts, meta_state):
        """Insert systemtender metadata"""
        db_config = self._get_db_config()
        json_string = json.dumps(meta_state).replace("'", "''")
        name_escaped = name.replace("'", "''")

        query = f"""
        INSERT INTO {self.systemtender_table_name} (id, name, creation_tsz, definition)
        VALUES('{systemtender_id}', '{name_escaped}', '{creation_ts}', '{json_string}');
        """

        execute_query(db_config, query)
        logger.info(f"Inserted metadata for systemtender: {systemtender_id}")

    def update_systemtender_meta(self, systemtender_id, meta_state):
        """Update systemtender metadata (e.g., to add job IDs)"""
        db_config = self._get_db_config()
        json_string = json.dumps(meta_state).replace("'", "''")

        query = f"""
        UPDATE {self.systemtender_table_name}
        SET definition = '{json_string}'
        WHERE id = '{systemtender_id}';
        """

        execute_query(db_config, query)
        logger.info(f"Updated metadata for systemtender: {systemtender_id}")

    def remove_systemtender_meta(self, systemtender_id):
        """Remove systemtender metadata"""
        db_config = self._get_db_config()

        query = f"DELETE FROM {self.systemtender_table_name} WHERE id = '{systemtender_id}';"
        execute_query(db_config, query)
        logger.info(f"Removed metadata for systemtender: {systemtender_id}")

    def fetch_meta_data(self, systemtender_id):
        """Fetch metadata for a specific systemtender"""
        db_config = self._get_db_config()

        query = f"""
        SELECT id, name, creation_tsz, definition FROM {self.systemtender_table_name} WHERE id = '{systemtender_id}';
        """

        return execute_query(db_config, query, with_result=True)

    def fetch_systemtenders_list(self):
        """Fetch list of all systemtenders"""
        db_config = self._get_db_config()

        query = f"""
        SELECT id, name, creation_tsz FROM {self.systemtender_table_name};
        """

        result = execute_query(db_config, query, with_result=True)
        return result if result else []
    
    # Credential management methods
    
    def insert_credential(self, credential_id, name, credential_type, description, windmill_variable, store_type='windmill_variable', metadata=None):
        """Insert credential catalog entry"""
        db_config = self._get_db_config()
        metadata_json = "'" + json.dumps(metadata).replace("'", "''") + "'" if metadata else 'NULL'
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
    
    def create_steerwish_tables(self):
        """Create the steerwish registry + lifecycle event tables.

        The wish OBJECT and its lifecycle live here (controller-owned);
        all steering CALCULATION lives in causal, keyed by wish id.
        State is never stored - it derives from the latest event.
        """
        db_config = self._get_db_config()

        query = f"""
        CREATE TABLE IF NOT EXISTS {self.steerwishes_table_name}
        (
        id uuid PRIMARY KEY,
        outcome VARCHAR(255) NOT NULL,
        band JSONB NOT NULL,
        limits JSONB,
        budget INTEGER,
        regime VARCHAR(50) NOT NULL DEFAULT 'standing',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
        execute_query(db_config, query)

        query = f"""
        CREATE TABLE IF NOT EXISTS {self.steerwish_events_table_name}
        (
        id SERIAL PRIMARY KEY,
        wish_id uuid NOT NULL REFERENCES {self.steerwishes_table_name}(id) ON DELETE CASCADE,
        event_type VARCHAR(50) NOT NULL,
        detail TEXT,
        at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_steerwish_events_wish
        ON {self.steerwish_events_table_name}(wish_id, at);
        """
        execute_query(db_config, query)
        logger.info("Ensured steerwish tables exist")

    def insert_steerwish(self, wish_id, outcome, band, limits, budget, regime):
        """Insert a declared steerwish (the 'declared' event is appended by the caller)"""
        db_config = self._get_db_config()
        band_json = "'" + json.dumps(band).replace("'", "''") + "'"
        limits_json = "'" + json.dumps(limits).replace("'", "''") + "'::jsonb" if limits else 'NULL'
        budget_sql = str(int(budget)) if budget is not None else 'NULL'
        outcome_sql = outcome.replace("'", "''")
        regime_sql = regime.replace("'", "''")

        query = f"""
        INSERT INTO {self.steerwishes_table_name}
        (id, outcome, band, limits, budget, regime)
        VALUES('{wish_id}', '{outcome_sql}', {band_json}::jsonb, {limits_json}, {budget_sql}, '{regime_sql}');
        """
        execute_query(db_config, query)
        logger.info(f"Inserted steerwish {wish_id} for outcome: {outcome}")

    def fetch_steerwish_by_id(self, wish_id):
        """Fetch one steerwish row with its derived state (latest event)"""
        db_config = self._get_db_config()
        wish_id_sql = str(wish_id).replace("'", "''")

        query = f"""
        SELECT w.id, w.outcome, w.band, w.limits, w.budget, w.regime, w.created_at,
               COALESCE((SELECT e.event_type FROM {self.steerwish_events_table_name} e
                         WHERE e.wish_id = w.id
                         ORDER BY e.at DESC, e.id DESC LIMIT 1), 'declared') AS state
        FROM {self.steerwishes_table_name} w
        WHERE w.id = '{wish_id_sql}';
        """

        result = execute_query(db_config, query, with_result=True)
        return result[0] if result else None

    def fetch_steerwishes_list(self):
        """Fetch all steerwishes with derived state, newest first"""
        db_config = self._get_db_config()

        query = f"""
        SELECT w.id, w.outcome,
               COALESCE((SELECT e.event_type FROM {self.steerwish_events_table_name} e
                         WHERE e.wish_id = w.id
                         ORDER BY e.at DESC, e.id DESC LIMIT 1), 'declared') AS state,
               w.created_at
        FROM {self.steerwishes_table_name} w
        ORDER BY w.created_at DESC;
        """

        result = execute_query(db_config, query, with_result=True)
        return result if result else []

    def insert_steerwish_event(self, wish_id, event_type, detail=None):
        """Append a lifecycle event to a wish"""
        db_config = self._get_db_config()
        wish_id_sql = str(wish_id).replace("'", "''")
        event_type_sql = event_type.replace("'", "''")
        detail_sql = "'" + str(detail).replace("'", "''") + "'" if detail is not None else 'NULL'

        query = f"""
        INSERT INTO {self.steerwish_events_table_name}
        (wish_id, event_type, detail)
        VALUES('{wish_id_sql}', '{event_type_sql}', {detail_sql});
        """
        execute_query(db_config, query)

    def fetch_steerwish_events(self, wish_id):
        """Fetch a wish's lifecycle events, oldest first"""
        db_config = self._get_db_config()
        wish_id_sql = str(wish_id).replace("'", "''")

        query = f"""
        SELECT event_type, at, detail
        FROM {self.steerwish_events_table_name}
        WHERE wish_id = '{wish_id_sql}'
        ORDER BY at ASC, id ASC;
        """

        result = execute_query(db_config, query, with_result=True)
        return result if result else []

    def create_targets_table(self):
        """Create the targets catalog table"""
        db_config = self._get_db_config()

        query = f"""
        CREATE TABLE IF NOT EXISTS {self.targets_table_name}
        (
        id uuid PRIMARY KEY,
        name VARCHAR(255) UNIQUE NOT NULL,
        target_type VARCHAR(50) NOT NULL,
        spec JSONB NOT NULL,
        metadata JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        last_used_at TIMESTAMPTZ
        );
        """

        execute_query(db_config, query)
        logger.info(f"Ensured targets table exists: {self.targets_table_name}")

    def insert_target(self, target_id, name, target_type, spec, metadata=None):
        """Insert target catalog entry"""
        db_config = self._get_db_config()
        spec_json = json.dumps(spec) if isinstance(spec, dict) else spec
        metadata_json = "'" + json.dumps(metadata).replace("'", "''") + "'" if metadata else 'NULL'

        query = f"""
        INSERT INTO {self.targets_table_name}
        (id, name, target_type, spec, metadata)
        VALUES('{target_id}', '{name}', '{target_type}', '{spec_json}'::jsonb, {metadata_json}::jsonb);
        """

        execute_query(db_config, query)
        logger.info(f"Inserted target catalog entry: {name}")

    def fetch_targets_list(self):
        """Fetch list of all targets"""
        db_config = self._get_db_config()

        query = f"""
        SELECT id, name, target_type, spec, metadata, created_at, last_used_at
        FROM {self.targets_table_name}
        ORDER BY created_at DESC;
        """

        result = execute_query(db_config, query, with_result=True)
        return result if result else []

    def fetch_target_by_id(self, target_id):
        """Fetch target by ID"""
        db_config = self._get_db_config()

        query = f"""
        SELECT id, name, target_type, spec, metadata, created_at, last_used_at
        FROM {self.targets_table_name}
        WHERE id = '{target_id}';
        """

        result = execute_query(db_config, query, with_result=True)
        return result[0] if result else None

    def fetch_target_by_name(self, name):
        """Fetch target by name"""
        db_config = self._get_db_config()

        query = f"""
        SELECT id, name, target_type, spec, metadata, created_at, last_used_at
        FROM {self.targets_table_name}
        WHERE name = '{name}';
        """

        result = execute_query(db_config, query, with_result=True)
        return result[0] if result else None

    def delete_target(self, target_id):
        """Delete target from catalog"""
        db_config = self._get_db_config()

        query = f"DELETE FROM {self.targets_table_name} WHERE id = '{target_id}';"
        execute_query(db_config, query)
        logger.info(f"Deleted target catalog entry: {target_id}")

    def update_credential_last_used(self, credential_id):
        """Update the last_used_at timestamp for a credential"""
        db_config = self._get_db_config()
        
        query = f"""
        UPDATE {self.credentials_table_name} 
        SET last_used_at = NOW() 
        WHERE id = '{credential_id}';
        """
        
        execute_query(db_config, query)