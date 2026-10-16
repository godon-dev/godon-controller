//! The data layer, ported statement by statement from
//! controller/database.py (psycopg2).
//!
//! SQL strings are byte-identical to the python originals - including
//! their embedded whitespace - because the wire calls, the windmill
//! paths, and the table schemas are the cross-lane contract. The
//! python's parameterless cursor.execute() IS the simple query
//! protocol; the sync postgres crate's simple_query is the same call,
//! so multi-statement strings behave identically. Cells come back as
//! text and are decoded per statement, exactly where psycopg2's
//! implicit typing sat.
//!
//! One deliberate divergence, named: python logged the database config
//! (password included) on connection errors; this port logs it redacted.

use log::{debug, error, info, warn};
use serde_json::{json, Map, Value};
use std::sync::Arc;

/// DatabaseConfig.ARCHIVE_DB / META_DB from the environment
/// (controller/config.py).
#[derive(Debug, Clone, Default)]
pub struct DbConfig {
    pub user: Option<String>,
    pub password: Option<String>,
    pub host: Option<String>,
    pub port: Option<String>,
    pub database: Option<String>,
}

impl DbConfig {
    pub fn archive_from_env() -> Self {
        Self {
            user: std::env::var("GODON_ARCHIVE_DB_USER").ok(),
            password: std::env::var("GODON_ARCHIVE_DB_PASSWORD").ok(),
            host: std::env::var("GODON_ARCHIVE_DB_SERVICE_HOST").ok(),
            port: std::env::var("GODON_ARCHIVE_DB_SERVICE_PORT").ok(),
            database: None,
        }
    }

    pub fn meta_from_env() -> Self {
        Self {
            user: Some(
                std::env::var("GODON_METADATA_DB_SERVICE_USER")
                    .unwrap_or_else(|_| "meta_data".to_string()),
            ),
            password: Some(
                std::env::var("GODON_METADATA_DB_SERVICE_PASSWORD")
                    .unwrap_or_else(|_| "meta_data".to_string()),
            ),
            host: std::env::var("GODON_METADATA_DB_SERVICE_HOST").ok(),
            port: std::env::var("GODON_METADATA_DB_SERVICE_PORT").ok(),
            database: None,
        }
    }

    fn with_database(&self, database: &str) -> Self {
        let mut c = self.clone();
        c.database = Some(database.to_string());
        c
    }
}

#[derive(Debug)]
pub struct DbError(pub String);

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for DbError {}

pub type DbResult<T> = Result<T, DbError>;

/// Text-cell rows - what the simple query protocol returns.
pub type TextRow = Vec<Option<String>>;

fn text_cell(cell: Option<&str>) -> Option<String> {
    cell.map(|s| s.to_string())
}

/// Python stored NULL for falsy JSON values, and the api's defaults
/// ARE falsy (metadata {} arrives on every credential/target create) -
/// so the NULL-vs-'{}' split is live stored state, not a quirk.
fn py_falsy(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::Bool(b) => !*b,
        Value::Number(n) => n.as_f64().map(|f| f == 0.0).unwrap_or(false),
        Value::String(s) => s.is_empty(),
        Value::Array(a) => a.is_empty(),
        Value::Object(o) => o.is_empty(),
    }
}

fn rows_from_simple(messages: &[postgres::SimpleQueryMessage]) -> Vec<TextRow> {
    messages
        .iter()
        .filter_map(|m| match m {
            postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .map(|r| (0..r.columns().len()).map(|i| text_cell(r.get(i))).collect())
        .collect()
}

/// The database seam. Python's module-level execute_query, injectable
/// here the way python's tests patched it.
pub trait DbGateway: Send + Sync {
    fn execute_query(
        &self,
        db_config: &DbConfig,
        query: &str,
        with_result: bool,
    ) -> DbResult<Option<Vec<TextRow>>>;
    fn execute_ddl_query(&self, db_config: &DbConfig, query: &str) -> DbResult<()>;
}

fn redacted(db_config: &DbConfig) -> String {
    format!(
        "{{'user': {:?}, 'password': '***', 'host': {:?}, 'port': {:?}, 'database': {:?}}}",
        db_config.user, db_config.host, db_config.port, db_config.database
    )
}

/// The production gateway: one fresh connection per call, exactly as
/// the python did (one short connection per verb).
pub struct PgGateway;

fn connect(db_config: &DbConfig) -> DbResult<postgres::Client> {
    let mut cfg = postgres::Config::new();
    if let Some(user) = &db_config.user {
        cfg.user(user);
    }
    if let Some(password) = &db_config.password {
        cfg.password(password);
    }
    if let Some(host) = &db_config.host {
        cfg.host(host);
    }
    if let Some(port) = &db_config.port {
        let p: u16 = port
            .parse()
            .map_err(|_| DbError(format!("invalid port: {}", port)))?;
        cfg.port(p);
    }
    if let Some(db) = &db_config.database {
        cfg.dbname(db);
    }
    cfg.connect(postgres::NoTls).map_err(|e| DbError(e.to_string()))
}

impl DbGateway for PgGateway {
    fn execute_query(
        &self,
        db_config: &DbConfig,
        query: &str,
        with_result: bool,
    ) -> DbResult<Option<Vec<TextRow>>> {
        let mut client = connect(db_config).map_err(|e| {
            error!("Database connection error: {}", e.0);
            error!("Database config: {}", redacted(db_config));
            e
        })?;
        let rows = client.simple_query(query).map_err(|e| {
            error!("Query execution failed: {}", e);
            DbError(e.to_string())
        })?;
        if with_result {
            Ok(Some(rows_from_simple(&rows)))
        } else {
            Ok(None)
        }
    }

    fn execute_ddl_query(&self, db_config: &DbConfig, query: &str) -> DbResult<()> {
        let mut client = connect(db_config).map_err(|e| {
            error!("Database connection error: {}", e.0);
            error!("Database config: {}", redacted(db_config));
            e
        })?;
        client.simple_query(query).map_err(|e| {
            error!("DDL execution failed: {}", e);
            DbError(e.to_string())
        })?;
        let head = &query[..query.len().min(50)];
        debug!("DDL executed: {}...", head);
        Ok(())
    }
}

/// psycopg2's implicit typing, per statement: the decodings the python
/// got for free, done beside the query that needs them.
fn cell_f64(cell: &Option<String>) -> Option<f64> {
    cell.as_ref().and_then(|t| t.parse::<f64>().ok())
}

fn cell_bool(cell: &Option<String>) -> Option<bool> {
    cell.as_ref().map(|t| t == "t")
}

fn cell_str(cell: &Option<String>) -> Option<String> {
    cell.clone()
}

// ---------------------------------------------------------------------------
// ArchiveDatabaseRepository
// ---------------------------------------------------------------------------

pub struct ArchiveDatabaseRepository {
    base_config: DbConfig,
    db: Arc<dyn DbGateway>,
}

impl ArchiveDatabaseRepository {
    pub fn new(base_config: DbConfig) -> Self {
        Self {
            base_config,
            db: Arc::new(PgGateway),
        }
    }

    pub fn with_gateway(base_config: DbConfig, db: Arc<dyn DbGateway>) -> Self {
        Self { base_config, db }
    }

    /// The gateway the repository carries (the services' raw reads ride it).
    pub fn gateway(&self) -> Arc<dyn DbGateway> {
        self.db.clone()
    }

    /// A one-off read against a per-tender database (the trial
    /// compatibility checks).
    pub fn raw_query(&self, db_name: &str, sql: &str) -> DbResult<Option<Vec<TextRow>>> {
        let db_config = self.base_config.with_database(db_name);
        self.db.execute_query(&db_config, sql, true)
    }

    /// Create the systemtender state table for shutdown signaling.
    pub fn create_systemtender_state_table(&self, systemtender_id: &str) -> DbResult<()> {
        let db_config = self.base_config.with_database(systemtender_id);

        let query = r#"
        CREATE TABLE IF NOT EXISTS systemtender_state (
            id SERIAL PRIMARY KEY,
            shutdown_requested BOOLEAN DEFAULT FALSE,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );

        INSERT INTO systemtender_state (shutdown_requested) VALUES (FALSE)
        ON CONFLICT DO NOTHING;
        "#;

        self.db.execute_query(&db_config, query, false)?;
        info!("Created systemtender state table for: {}", systemtender_id);
        Ok(())
    }

    /// Set or clear the shutdown requested flag in the systemtender's
    /// archive database.
    pub fn set_shutdown_requested(&self, systemtender_id: &str, value: bool) -> DbResult<()> {
        let db_config = self.base_config.with_database(systemtender_id);

        let flag = if value { "TRUE" } else { "FALSE" };
        let query = format!(
            r#"
        UPDATE systemtender_state SET shutdown_requested = {}, updated_at = NOW();
        "#,
            flag
        );

        self.db.execute_query(&db_config, &query, false)?;
        info!(
            "Set shutdown_requested={} in archive DB for systemtender: {}",
            value, systemtender_id
        );
        Ok(())
    }

    /// The tender's pulse: (age_secs, beat_interval_secs). Falls back
    /// to an age-only read when the row predates the interval column;
    /// None when the row (or the db) is absent.
    pub fn read_heartbeat(&self, systemtender_db_name: &str) -> DbResult<Option<(f64, Option<f64>)>> {
        let db_config = self.base_config.with_database(systemtender_db_name);
        let rows = match self.db.execute_query(
            &db_config,
            "SELECT EXTRACT(EPOCH FROM (now() - updated_at)), \
             beat_interval_secs FROM systemtender_state LIMIT 1",
            true,
        ) {
            Ok(r) => r,
            Err(e) => {
                // a state row born before the interval column: age still
                // decides, with the caller's fallback window
                if e.0.contains("beat_interval_secs") && e.0.contains("does not exist") {
                    self.db.execute_query(
                        &db_config,
                        "SELECT EXTRACT(EPOCH FROM (now() - updated_at)), NULL \
                         FROM systemtender_state LIMIT 1",
                        true,
                    )?
                } else {
                    return Err(e);
                }
            }
        };
        if let Some(rows) = rows {
            if let Some(row) = rows.first() {
                if let Some(age_text) = &row[0] {
                    let age: f64 = age_text.parse().unwrap_or(0.0);
                    let interval = row[1].as_ref().and_then(|t| t.parse::<f64>().ok());
                    return Ok(Some((age, interval)));
                }
            }
        }
        Ok(None)
    }

    /// The tender's declared end plus its pulse, one read. No verdict
    /// when the table cannot be read - both live wordings for "the
    /// table is not there" (pgsql's "does not exist" and YB's
    /// "not found in Raft group").
    pub fn read_state_verdict(&self, systemtender_db_name: &str) -> DbResult<Option<Value>> {
        let db_config = self.base_config.with_database(systemtender_db_name);
        // The instruments column is rollout-order guarded, message-matched
        // (the worker's own beat-guard pattern): a state row born before
        // the column - fresh tender, worker has not stamped yet - reads
        // the three verdict columns alone, instruments None.
        let attempted = match self.db.execute_query(
            &db_config,
            "SELECT finished_at IS NOT NULL, \
             EXTRACT(EPOCH FROM (now() - updated_at)), \
             beat_interval_secs, instruments \
             FROM systemtender_state LIMIT 1",
            true,
        ) {
            Err(e) if e.0.contains("instruments") && e.0.contains("does not exist") => {
                self.db.execute_query(
                    &db_config,
                    "SELECT finished_at IS NOT NULL, \
                     EXTRACT(EPOCH FROM (now() - updated_at)), \
                     beat_interval_secs FROM systemtender_state LIMIT 1",
                    true,
                )
            }
            other => other,
        };
        let rows = match attempted {
            Ok(r) => r,
            Err(e) => {
                let error_text = e.0.clone();
                if error_text.contains("does not exist")
                    || error_text.to_lowercase().contains("not found in raft group")
                {
                    // Nothing readable, no verdict. The lifecycle row
                    // speaks.
                    return Ok(None);
                }
                return Err(e);
            }
        };
        if let Some(rows) = rows {
            if let Some(row) = rows.first() {
                if let Some(finished) = cell_bool(&row[0]) {
                    let mut m = Map::new();
                    m.insert("finished".to_string(), Value::Bool(finished));
                    m.insert(
                        "age_secs".to_string(),
                        match cell_f64(&row[1]) {
                            Some(v) => json!(v),
                            None => Value::Null,
                        },
                    );
                    m.insert(
                        "interval_secs".to_string(),
                        match cell_f64(&row[2]) {
                            Some(v) => json!(v),
                            None => Value::Null,
                        },
                    );
                    // The worker's JSON stamp, parsed leniently - a torn
                    // payload reads as absent, never as a failed read.
                    let mut instruments = Value::Null;
                    if let Some(Some(cell)) = row.get(3) {
                        if !cell.is_empty() {
                            instruments =
                                serde_json::from_str::<Value>(cell).unwrap_or(Value::Null);
                        }
                    }
                    m.insert("instruments".to_string(), instruments);
                    return Ok(Some(Value::Object(m)));
                }
            }
        }
        Ok(None)
    }

    /// Plant the wish assignment row in the tender's own archive DB.
    /// role: 'sender' owns the dial move; 'receiver' parks and
    /// publishes the wished reading.
    pub fn write_wish_assignment(
        &self,
        systemtender_db_name: &str,
        wish_id: &str,
        role: &str,
    ) -> DbResult<()> {
        let db_config = self.base_config.with_database(systemtender_db_name);

        let wish_id_sql = wish_id.replace('\'', "''");
        let role_sql = role.replace('\'', "''");

        let create_query = "CREATE TABLE IF NOT EXISTS wish_assignments (\
                            wish_id TEXT PRIMARY KEY, assigned_tsz DOUBLE PRECISION NOT NULL);";
        let alter_query = "ALTER TABLE wish_assignments \
                           ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'sender';";
        let insert_query = format!(
            "INSERT INTO wish_assignments (wish_id, assigned_tsz, role) \
             VALUES ('{}', EXTRACT(EPOCH FROM NOW()), '{}') \
             ON CONFLICT (wish_id) DO UPDATE SET \
             assigned_tsz = EXCLUDED.assigned_tsz, role = EXCLUDED.role;",
            wish_id_sql, role_sql
        );
        self.db.execute_query(&db_config, create_query, false)?;
        self.db.execute_query(&db_config, alter_query, false)?;
        self.db.execute_query(&db_config, &insert_query, false)?;
        info!(
            "Wish assignment written into archive DB {} (wish: {}, role: {})",
            systemtender_db_name, wish_id, role
        );
        Ok(())
    }

    /// Remove the assignment row - the tender's pulse reverts the dial
    /// to neutral on the next boundary.
    pub fn delete_wish_assignment(&self, systemtender_db_name: &str, wish_id: &str) -> DbResult<()> {
        let db_config = self.base_config.with_database(systemtender_db_name);

        let wish_id_sql = wish_id.replace('\'', "''");
        let query = format!("DELETE FROM wish_assignments WHERE wish_id = '{}';", wish_id_sql);
        self.db.execute_query(&db_config, &query, false)?;
        info!(
            "Wish assignment removed from archive DB {} (wish: {})",
            systemtender_db_name, wish_id
        );
        Ok(())
    }

    /// Check if shutdown has been requested for a systemtender.
    pub fn get_shutdown_requested(&self, systemtender_id: &str) -> DbResult<bool> {
        let db_config = self.base_config.with_database(systemtender_id);

        let query = r#"
        SELECT shutdown_requested FROM systemtender_state;
        "#;

        let result = self.db.execute_query(&db_config, query, true)?;
        if let Some(rows) = result {
            if !rows.is_empty() {
                return Ok(rows[0][0].as_deref() == Some("t"));
            }
        }
        Ok(false)
    }

    /// Create a new database for a systemtender.
    pub fn create_database(&self, systemtender_id: &str) -> DbResult<()> {
        let db_config = self.base_config.with_database("archive_db");

        let query = format!("CREATE DATABASE {};", systemtender_id);
        self.db.execute_ddl_query(&db_config, &query)?;
        info!("Created archive database: {}", systemtender_id);
        Ok(())
    }

    /// Drop a systemtender database.
    pub fn drop_database(&self, systemtender_id: &str) -> DbResult<()> {
        let db_config = self.base_config.with_database("archive_db");

        let query = format!("DROP DATABASE IF EXISTS {};", systemtender_id);
        self.db.execute_ddl_query(&db_config, &query)?;
        info!("Dropped archive database: {}", systemtender_id);
        Ok(())
    }

    /// Remove a systemtender's rows from coordination tables in
    /// archive_db - stale coordination state must not block other
    /// systemtenders in the group.
    pub fn cleanup_coordination_state(&self, systemtender_id: &str) -> DbResult<()> {
        let db_config = self.base_config.with_database("archive_db");

        let queries = [
            format!("DELETE FROM interference_active_systemtenders WHERE systemtender_id = '{}';", systemtender_id),
            format!("DELETE FROM detection_readiness WHERE systemtender_id = '{}';", systemtender_id),
            format!("DELETE FROM receiver_observations WHERE receiver_id = '{}';", systemtender_id),
            format!("DELETE FROM curve_points WHERE sender_id = '{}';", systemtender_id),
        ];

        for query in &queries {
            if let Err(e) = self.db.execute_query(&db_config, query, false) {
                warn!(
                    "Coordination cleanup query failed (table may not exist yet): {}",
                    e.0
                );
            }
        }

        info!("Cleaned coordination state for systemtender: {}", systemtender_id);
        Ok(())
    }

    /// Remove the sender_lease row for a group with no remaining
    /// systemtenders.
    pub fn cleanup_group_lease(&self, group_id: &str) -> DbResult<()> {
        let db_config = self.base_config.with_database("archive_db");

        let query = format!("DELETE FROM sender_lease WHERE group_id = '{}';", group_id);
        if let Err(e) = self.db.execute_query(&db_config, &query, false) {
            warn!("Group lease cleanup failed (table may not exist yet): {}", e.0);
        }
        Ok(())
    }

    /// Get PostgreSQL connection URL for a systemtender database (the
    /// sqlalchemy-pinned dialect string, password masked as python
    /// masked it).
    pub fn get_connection_url(&self, systemtender_id: &str) -> String {
        format!(
            "postgresql+psycopg2:***@{}:{}/{}",
            self.base_config.host.as_deref().unwrap_or("None"),
            self.base_config.port.as_deref().unwrap_or("None"),
            systemtender_id
        )
    }
}

// ---------------------------------------------------------------------------
// MetadataDatabaseRepository
// ---------------------------------------------------------------------------

pub struct MetadataDatabaseRepository {
    base_config: DbConfig,
    db: Arc<dyn DbGateway>,
    systemtender_table_name: &'static str,
    credentials_table_name: &'static str,
    targets_table_name: &'static str,
    steerwishes_table_name: &'static str,
    steerwish_events_table_name: &'static str,
}

impl MetadataDatabaseRepository {
    pub fn new(base_config: DbConfig) -> Self {
        Self {
            base_config,
            db: Arc::new(PgGateway),
            systemtender_table_name: "systemtender_meta_data",
            credentials_table_name: "credentials",
            targets_table_name: "targets",
            steerwishes_table_name: "steerwishes",
            steerwish_events_table_name: "steerwish_events",
        }
    }

    pub fn with_gateway(base_config: DbConfig, db: Arc<dyn DbGateway>) -> Self {
        Self {
            base_config,
            db,
            systemtender_table_name: "systemtender_meta_data",
            credentials_table_name: "credentials",
            targets_table_name: "targets",
            steerwishes_table_name: "steerwishes",
            steerwish_events_table_name: "steerwish_events",
        }
    }

    fn _get_db_config(&self) -> DbConfig {
        self.base_config.with_database("meta_data")
    }

    /// Create the systemtender metadata table.
    pub fn create_table(&self) -> DbResult<()> {
        let db_config = self._get_db_config();

        let query = format!(
            r#"
        CREATE TABLE IF NOT EXISTS {}
        (
        id uuid PRIMARY KEY,
        name VARCHAR(255) NOT NULL DEFAULT '',
        creation_tsz TIMESTAMPTZ,
        definition jsonb NOT NULL
        );
        "#,
            self.systemtender_table_name
        );

        self.db.execute_query(&db_config, &query, false)?;
        info!(
            "Ensured metadata table exists: {}",
            self.systemtender_table_name
        );
        self.ensure_lifecycle_table()
    }

    /// Create the systemtender lifecycle table (deletion state machine
    /// bookkeeping, designs/2026-10-03).
    pub fn ensure_lifecycle_table(&self) -> DbResult<()> {
        let db_config = self._get_db_config();

        let query = r#"
        CREATE TABLE IF NOT EXISTS systemtender_lifecycle
        (
        systemtender_id uuid PRIMARY KEY,
        state VARCHAR(32) NOT NULL,
        reason TEXT,
        claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        "#;

        self.db.execute_query(&db_config, query, false)?;
        info!("Ensured lifecycle table exists: systemtender_lifecycle");
        Ok(())
    }

    /// Atomically claim the deletion executor slot. Single-flight:
    /// exactly one executor per tender.
    pub fn claim_for_deletion(&self, systemtender_id: &str, stale_after_seconds: i64) -> DbResult<bool> {
        let db_config = self._get_db_config();

        let query = format!(
            r#"
        INSERT INTO systemtender_lifecycle (systemtender_id, state, reason, claimed_at)
        VALUES ('{}', 'deleting', NULL, NOW())
        ON CONFLICT (systemtender_id) DO UPDATE
        SET state = 'deleting', reason = NULL, claimed_at = NOW()
        WHERE systemtender_lifecycle.state = 'deletion-failed'
           OR systemtender_lifecycle.state = 'create-failed'
           OR systemtender_lifecycle.claimed_at < NOW() - INTERVAL '{} seconds'
        RETURNING systemtender_id;
        "#,
            systemtender_id, stale_after_seconds
        );

        let rows = self.db.execute_query(&db_config, &query, true)?;
        let claimed = rows.map(|r| !r.is_empty()).unwrap_or(false);
        if claimed {
            info!("Deletion claim taken for systemtender: {}", systemtender_id);
        }
        Ok(claimed)
    }

    /// Deletion state for one tender, or None when no marker stands.
    pub fn get_lifecycle(&self, systemtender_id: &str) -> DbResult<Option<Value>> {
        let db_config = self._get_db_config();

        let query = format!(
            "SELECT state, reason FROM systemtender_lifecycle WHERE systemtender_id = '{}';",
            systemtender_id
        );
        let rows = self.db.execute_query(&db_config, &query, true)?.unwrap_or_default();
        if rows.is_empty() {
            return Ok(None);
        }
        let mut m = Map::new();
        m.insert("state".to_string(), json!(cell_str(&rows[0][0])));
        m.insert("reason".to_string(), json!(cell_str(&rows[0][1])));
        Ok(Some(Value::Object(m)))
    }

    /// Deletion states for all tenders: {id_str: state}.
    pub fn get_all_lifecycle(&self) -> DbResult<Value> {
        let db_config = self._get_db_config();

        let query = "SELECT systemtender_id, state FROM systemtender_lifecycle;";
        let rows = self.db.execute_query(&db_config, query, true)?.unwrap_or_default();
        let mut m = Map::new();
        for r in &rows {
            m.insert(
                cell_str(&r[0]).unwrap_or_default(),
                json!(cell_str(&r[1])),
            );
        }
        Ok(Value::Object(m))
    }

    /// Record a lifecycle transition (executor-provided state + reason).
    pub fn set_lifecycle(&self, systemtender_id: &str, state: &str, reason: Option<&str>) -> DbResult<()> {
        let db_config = self._get_db_config();
        let reason_escaped = reason.unwrap_or("").replace('\'', "''");

        let query = format!(
            r#"
        INSERT INTO systemtender_lifecycle (systemtender_id, state, reason, claimed_at)
        VALUES ('{}', '{}', '{}', NOW())
        ON CONFLICT (systemtender_id) DO UPDATE
        SET state = '{}', reason = '{}', claimed_at = NOW();
        "#,
            systemtender_id, state, reason_escaped, state, reason_escaped
        );

        self.db.execute_query(&db_config, &query, false)?;
        info!("Lifecycle '{}' for systemtender: {}", state, systemtender_id);
        Ok(())
    }

    /// Remove the lifecycle row (tender gone, or was never there).
    pub fn remove_lifecycle(&self, systemtender_id: &str) -> DbResult<()> {
        let db_config = self._get_db_config();

        let query = format!(
            "DELETE FROM systemtender_lifecycle WHERE systemtender_id = '{}';",
            systemtender_id
        );
        self.db.execute_query(&db_config, &query, false)?;
        Ok(())
    }

    /// Create the credentials catalog table.
    pub fn create_credentials_table(&self) -> DbResult<()> {
        let db_config = self._get_db_config();

        let query = format!(
            r#"
        CREATE TABLE IF NOT EXISTS {}
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
        "#,
            self.credentials_table_name
        );

        self.db.execute_query(&db_config, &query, false)?;
        info!(
            "Ensured credentials table exists: {}",
            self.credentials_table_name
        );
        Ok(())
    }

    /// Insert systemtender metadata.
    pub fn insert_systemtender_meta(
        &self,
        systemtender_id: &str,
        name: &str,
        creation_ts: &str,
        meta_state: &Value,
    ) -> DbResult<()> {
        let db_config = self._get_db_config();
        let json_string = serde_json::to_string(meta_state).unwrap_or_default().replace('\'', "''");
        let name_escaped = name.replace('\'', "''");

        let query = format!(
            r#"
        INSERT INTO {} (id, name, creation_tsz, definition)
        VALUES('{}', '{}', '{}', '{}');
        "#,
            self.systemtender_table_name, systemtender_id, name_escaped, creation_ts, json_string
        );

        self.db.execute_query(&db_config, &query, false)?;
        info!("Inserted metadata for systemtender: {}", systemtender_id);
        Ok(())
    }

    /// Update systemtender metadata (e.g., to add job IDs).
    pub fn update_systemtender_meta(&self, systemtender_id: &str, meta_state: &Value) -> DbResult<()> {
        let db_config = self._get_db_config();
        let json_string = serde_json::to_string(meta_state).unwrap_or_default().replace('\'', "''");

        let query = format!(
            r#"
        UPDATE {}
        SET definition = '{}'
        WHERE id = '{}';
        "#,
            self.systemtender_table_name, json_string, systemtender_id
        );

        self.db.execute_query(&db_config, &query, false)?;
        info!("Updated metadata for systemtender: {}", systemtender_id);
        Ok(())
    }

    /// Remove systemtender metadata.
    pub fn remove_systemtender_meta(&self, systemtender_id: &str) -> DbResult<()> {
        let db_config = self._get_db_config();

        let query = format!(
            "DELETE FROM {} WHERE id = '{}';",
            self.systemtender_table_name, systemtender_id
        );
        self.db.execute_query(&db_config, &query, false)?;
        info!("Removed metadata for systemtender: {}", systemtender_id);
        Ok(())
    }

    /// Fetch metadata for a specific systemtender.
    pub fn fetch_meta_data(&self, systemtender_id: &str) -> DbResult<Vec<TextRow>> {
        let db_config = self._get_db_config();

        let query = format!(
            r#"
        SELECT id, name, creation_tsz, definition FROM {} WHERE id = '{}';
        "#,
            self.systemtender_table_name, systemtender_id
        );

        Ok(self
            .db
            .execute_query(&db_config, &query, true)?
            .unwrap_or_default())
    }

    /// Fetch list of all systemtenders.
    pub fn fetch_systemtenders_list(&self) -> DbResult<Vec<TextRow>> {
        let db_config = self._get_db_config();

        let query = format!(
            r#"
        SELECT id, name, creation_tsz FROM {};
        "#,
            self.systemtender_table_name
        );

        Ok(self
            .db
            .execute_query(&db_config, &query, true)?
            .unwrap_or_default())
    }

    /// Insert credential catalog entry.
    pub fn insert_credential(
        &self,
        credential_id: &str,
        name: &str,
        credential_type: &str,
        description: Option<&str>,
        windmill_variable: &str,
        store_type: &str,
        metadata: Option<&Value>,
    ) -> DbResult<()> {
        let db_config = self._get_db_config();
        let metadata_json = match metadata {
            Some(m) if !py_falsy(m) => format!("'{}'", serde_json::to_string(m).unwrap_or_default().replace('\'', "''")),
            _ => "NULL".to_string(),
        };
        let description_escaped = match description {
            Some(d) if !d.is_empty() => {
                format!("'{}'", d.replace('\'', "''"))
            }
            _ => "NULL".to_string(),
        };

        let query = format!(
            r#"
        INSERT INTO {}
        (id, name, credential_type, description, windmill_variable, store_type, metadata)
        VALUES('{}', '{}', '{}', {},
                '{}', '{}', {}::jsonb);
        "#,
            self.credentials_table_name,
            credential_id,
            name,
            credential_type,
            description_escaped,
            windmill_variable,
            store_type,
            metadata_json
        );

        self.db.execute_query(&db_config, &query, false)?;
        info!("Inserted credential catalog entry: {}", name);
        Ok(())
    }

    /// Fetch list of all credentials.
    pub fn fetch_credentials_list(&self) -> DbResult<Vec<TextRow>> {
        let db_config = self._get_db_config();

        let query = format!(
            r#"
        SELECT id, name, credential_type, description, windmill_variable, created_at, last_used_at
        FROM {}
        ORDER BY created_at DESC;
        "#,
            self.credentials_table_name
        );

        Ok(self
            .db
            .execute_query(&db_config, &query, true)?
            .unwrap_or_default())
    }

    /// Fetch credential by ID.
    pub fn fetch_credential_by_id(&self, credential_id: &str) -> DbResult<Option<TextRow>> {
        let db_config = self._get_db_config();

        let query = format!(
            r#"
        SELECT id, name, credential_type, description, windmill_variable, store_type, metadata, created_at, last_used_at
        FROM {}
        WHERE id = '{}';
        "#,
            self.credentials_table_name, credential_id
        );

        Ok(self
            .db
            .execute_query(&db_config, &query, true)?
            .and_then(|r| r.into_iter().next()))
    }

    /// Fetch credential by name.
    pub fn fetch_credential_by_name(&self, name: &str) -> DbResult<Option<TextRow>> {
        let db_config = self._get_db_config();

        let query = format!(
            r#"
        SELECT id, name, credential_type, description, windmill_variable, store_type, metadata, created_at, last_used_at
        FROM {}
        WHERE name = '{}';
        "#,
            self.credentials_table_name, name
        );

        Ok(self
            .db
            .execute_query(&db_config, &query, true)?
            .and_then(|r| r.into_iter().next()))
    }

    /// Delete credential from catalog.
    pub fn delete_credential(&self, credential_id: &str) -> DbResult<()> {
        let db_config = self._get_db_config();

        let query = format!(
            "DELETE FROM {} WHERE id = '{}';",
            self.credentials_table_name, credential_id
        );
        self.db.execute_query(&db_config, &query, false)?;
        info!("Deleted credential catalog entry: {}", credential_id);
        Ok(())
    }

    /// Create the wish registry + lifecycle event tables. The wish
    /// OBJECT and its lifecycle live here (controller-owned); all
    /// steering CALCULATION lives in causal, keyed by wish id.
    pub fn create_wish_tables(&self) -> DbResult<()> {
        let db_config = self._get_db_config();

        let query = format!(
            r#"
        CREATE TABLE IF NOT EXISTS {}
        (
        id uuid PRIMARY KEY,
        outcome VARCHAR(255) NOT NULL,
        band JSONB NOT NULL,
        limits JSONB,
        budget INTEGER,
        regime VARCHAR(50) NOT NULL DEFAULT 'standing',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        ALTER TABLE {}
        ADD COLUMN IF NOT EXISTS claims JSONB,
        ADD COLUMN IF NOT EXISTS terms JSONB;
        "#,
            self.steerwishes_table_name, self.steerwishes_table_name
        );
        self.db.execute_query(&db_config, &query, false)?;

        let query = format!(
            r#"
        CREATE TABLE IF NOT EXISTS {}
        (
        id SERIAL PRIMARY KEY,
        wish_id uuid NOT NULL REFERENCES {}(id) ON DELETE CASCADE,
        event_type VARCHAR(50) NOT NULL,
        detail TEXT,
        at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_steerwish_events_wish
        ON {}(wish_id, at);
        "#,
            self.steerwish_events_table_name,
            self.steerwishes_table_name,
            self.steerwish_events_table_name
        );
        self.db.execute_query(&db_config, &query, false)?;

        let query = r#"
        CREATE TABLE IF NOT EXISTS steerwish_lifecycle
        (
        wish_id uuid PRIMARY KEY,
        state VARCHAR(32) NOT NULL,
        reason TEXT,
        claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        "#;
        self.db.execute_query(&db_config, query, false)?;

        info!("Ensured steerwish tables exist");
        Ok(())
    }

    /// Atomically claim the steerwish deletion executor slot - the same
    /// single-flight idiom as the systemtender lifecycle.
    pub fn claim_wish_deletion(&self, wish_id: &str, stale_after_seconds: i64) -> DbResult<bool> {
        let db_config = self._get_db_config();

        let query = format!(
            r#"
        INSERT INTO steerwish_lifecycle (wish_id, state, reason, claimed_at)
        VALUES ('{}', 'deleting', NULL, NOW())
        ON CONFLICT (wish_id) DO UPDATE
        SET state = 'deleting', reason = NULL, claimed_at = NOW()
        WHERE steerwish_lifecycle.state = 'deletion-failed'
           OR steerwish_lifecycle.claimed_at < NOW() - INTERVAL '{} seconds'
        RETURNING wish_id;
        "#,
            wish_id, stale_after_seconds
        );

        let rows = self.db.execute_query(&db_config, &query, true)?;
        let claimed = rows.map(|r| !r.is_empty()).unwrap_or(false);
        if claimed {
            info!("Deletion claim taken for steerwish: {}", wish_id);
        }
        Ok(claimed)
    }

    /// Deletion state for one wish, or None when no marker stands.
    pub fn get_wish_lifecycle(&self, wish_id: &str) -> DbResult<Option<Value>> {
        let db_config = self._get_db_config();

        let query = format!(
            "SELECT state, reason FROM steerwish_lifecycle WHERE wish_id = '{}';",
            wish_id
        );
        let rows = self.db.execute_query(&db_config, &query, true)?.unwrap_or_default();
        if rows.is_empty() {
            return Ok(None);
        }
        let mut m = Map::new();
        m.insert("state".to_string(), json!(cell_str(&rows[0][0])));
        m.insert("reason".to_string(), json!(cell_str(&rows[0][1])));
        Ok(Some(Value::Object(m)))
    }

    /// Deletion states for all wishes: {id_str: state}.
    pub fn get_all_wish_lifecycle(&self) -> DbResult<Value> {
        let db_config = self._get_db_config();

        let query = "SELECT wish_id, state FROM steerwish_lifecycle;";
        let rows = self.db.execute_query(&db_config, query, true)?.unwrap_or_default();
        let mut m = Map::new();
        for r in &rows {
            m.insert(
                cell_str(&r[0]).unwrap_or_default(),
                json!(cell_str(&r[1])),
            );
        }
        Ok(Value::Object(m))
    }

    /// Record a lifecycle transition (executor-provided state + reason).
    pub fn set_wish_lifecycle(&self, wish_id: &str, state: &str, reason: Option<&str>) -> DbResult<()> {
        let db_config = self._get_db_config();
        let reason_escaped = reason.unwrap_or("").replace('\'', "''");

        let query = format!(
            r#"
        INSERT INTO steerwish_lifecycle (wish_id, state, reason, claimed_at)
        VALUES ('{}', '{}', '{}', NOW())
        ON CONFLICT (wish_id) DO UPDATE
        SET state = '{}', reason = '{}', claimed_at = NOW();
        "#,
            wish_id, state, reason_escaped, state, reason_escaped
        );

        self.db.execute_query(&db_config, &query, false)?;
        info!("Lifecycle '{}' for steerwish: {}", state, wish_id);
        Ok(())
    }

    /// Remove the lifecycle row (wish gone, or was never there).
    pub fn remove_wish_lifecycle(&self, wish_id: &str) -> DbResult<()> {
        let db_config = self._get_db_config();

        let query = format!("DELETE FROM steerwish_lifecycle WHERE wish_id = '{}';", wish_id);
        self.db.execute_query(&db_config, &query, false)?;
        Ok(())
    }

    /// Holder correction: restate the terms on the same identity. NULL
    /// (absent) claims/terms leave the stored column untouched.
    pub fn update_wish_terms(
        &self,
        wish_id: &str,
        band: &Value,
        limits: Option<&Value>,
        budget: Option<i64>,
        claims: Option<&Value>,
        terms: Option<&Value>,
    ) -> DbResult<()> {
        let db_config = self._get_db_config();
        let band_json = format!("'{}'::jsonb", serde_json::to_string(band).unwrap_or_default().replace('\'', "''"));
        let limits_json = match limits {
            Some(l) if !py_falsy(l) => format!("'{}'::jsonb", serde_json::to_string(l).unwrap_or_default().replace('\'', "''")),
            _ => "NULL".to_string(),
        };
        let budget_sql = match budget {
            Some(b) => b.to_string(),
            None => "NULL".to_string(),
        };
        let wid = wish_id.replace('\'', "''");
        let claims_set = match claims {
            Some(c) => format!("'{}'::jsonb", serde_json::to_string(c).unwrap_or_default().replace('\'', "''")),
            None => "NULL".to_string(),
        };
        let terms_set = match terms {
            Some(t) => format!("'{}'::jsonb", serde_json::to_string(t).unwrap_or_default().replace('\'', "''")),
            None => "NULL".to_string(),
        };
        let query = format!(
            r#"
        UPDATE {}
        SET band = {}, limits = {}, budget = {},
            claims = COALESCE({}, claims),
            terms = COALESCE({}, terms)
        WHERE id = '{}';
        "#,
            self.steerwishes_table_name,
            band_json,
            limits_json,
            budget_sql,
            claims_set,
            terms_set,
            wid
        );
        self.db.execute_query(&db_config, &query, false)?;
        info!("Steerwish terms updated: {}", wish_id);
        Ok(())
    }

    /// Administrative purge: remove the registry row; events cascade.
    pub fn delete_wish(&self, wish_id: &str) -> DbResult<()> {
        let db_config = self._get_db_config();
        let wid = wish_id.replace('\'', "''");
        let query = format!("DELETE FROM {} WHERE id = '{}';", self.steerwishes_table_name, wid);
        self.db.execute_query(&db_config, &query, false)?;
        info!("Steerwish purged from registry: {}", wish_id);
        Ok(())
    }

    /// Insert a declared steerwish (the 'declared' event is appended by
    /// the caller).
    #[allow(clippy::too_many_arguments)]
    pub fn insert_wish(
        &self,
        wish_id: &str,
        outcome: &str,
        band: &Value,
        limits: Option<&Value>,
        budget: Option<i64>,
        regime: &str,
        claims: Option<&Value>,
        terms: Option<&Value>,
    ) -> DbResult<()> {
        let db_config = self._get_db_config();
        let band_json = format!("'{}'", serde_json::to_string(band).unwrap_or_default().replace('\'', "''"));
        let limits_json = match limits {
            Some(l) if !py_falsy(l) => format!("'{}'::jsonb", serde_json::to_string(l).unwrap_or_default().replace('\'', "''")),
            _ => "NULL".to_string(),
        };
        let budget_sql = match budget {
            Some(b) => b.to_string(),
            None => "NULL".to_string(),
        };
        let outcome_sql = outcome.replace('\'', "''");
        let regime_sql = regime.replace('\'', "''");
        let claims_json = match claims {
            Some(c) if !py_falsy(c) => format!("'{}'::jsonb", serde_json::to_string(c).unwrap_or_default().replace('\'', "''")),
            _ => "NULL".to_string(),
        };
        let terms_json = match terms {
            Some(t) if !py_falsy(t) => format!("'{}'::jsonb", serde_json::to_string(t).unwrap_or_default().replace('\'', "''")),
            _ => "NULL".to_string(),
        };

        let query = format!(
            r#"
        INSERT INTO {}
        (id, outcome, band, limits, budget, regime, claims, terms)
        VALUES('{}', '{}', {}::jsonb, {}, {}, '{}', {}, {});
        "#,
            self.steerwishes_table_name,
            wish_id,
            outcome_sql,
            band_json,
            limits_json,
            budget_sql,
            regime_sql,
            claims_json,
            terms_json
        );
        self.db.execute_query(&db_config, &query, false)?;
        info!("Inserted steerwish {} for outcome: {}", wish_id, outcome);
        Ok(())
    }

    /// Fetch one steerwish row with its derived state (latest event).
    pub fn fetch_wish_by_id(&self, wish_id: &str) -> DbResult<Option<TextRow>> {
        let db_config = self._get_db_config();
        let wish_id_sql = wish_id.replace('\'', "''");

        let query = format!(
            r#"
        SELECT w.id, w.outcome, w.band, w.limits, w.budget, w.regime, w.created_at,
               COALESCE((SELECT e.event_type FROM {} e
                         WHERE e.wish_id = w.id
                         AND e.event_type NOT IN ('walk_opened', 'walk_probe', 'walk_closed')
                         ORDER BY e.at DESC, e.id DESC LIMIT 1), 'declared') AS state,
               w.claims, w.terms
        FROM {} w
        WHERE w.id = '{}';
        "#,
            self.steerwish_events_table_name, self.steerwishes_table_name, wish_id_sql
        );

        Ok(self
            .db
            .execute_query(&db_config, &query, true)?
            .and_then(|r| r.into_iter().next()))
    }

    /// Fetch all steerwishes with derived state, newest first.
    pub fn fetch_wishes_list(&self) -> DbResult<Vec<TextRow>> {
        let db_config = self._get_db_config();

        let query = format!(
            r#"
        SELECT w.id, w.outcome,
               COALESCE((SELECT e.event_type FROM {} e
                         WHERE e.wish_id = w.id
                         AND e.event_type NOT IN ('walk_opened', 'walk_probe', 'walk_closed')
                         ORDER BY e.at DESC, e.id DESC LIMIT 1), 'declared') AS state,
               w.created_at
        FROM {} w
        ORDER BY w.created_at DESC;
        "#,
            self.steerwish_events_table_name, self.steerwishes_table_name
        );

        Ok(self
            .db
            .execute_query(&db_config, &query, true)?
            .unwrap_or_default())
    }

    /// Append a lifecycle event to a wish.
    pub fn insert_wish_event(
        &self,
        wish_id: &str,
        event_type: &str,
        detail: Option<&str>,
    ) -> DbResult<()> {
        let db_config = self._get_db_config();
        let wish_id_sql = wish_id.replace('\'', "''");
        let event_type_sql = event_type.replace('\'', "''");
        let detail_sql = match detail {
            Some(d) => format!("'{}'", d.replace('\'', "''")),
            None => "NULL".to_string(),
        };

        let query = format!(
            r#"
        INSERT INTO {}
        (wish_id, event_type, detail)
        VALUES('{}', '{}', {});
        "#,
            self.steerwish_events_table_name, wish_id_sql, event_type_sql, detail_sql
        );
        self.db.execute_query(&db_config, &query, false)?;
        Ok(())
    }

    /// Fetch a wish's lifecycle events, oldest first.
    pub fn fetch_wish_events(&self, wish_id: &str) -> DbResult<Vec<TextRow>> {
        let db_config = self._get_db_config();
        let wish_id_sql = wish_id.replace('\'', "''");

        let query = format!(
            r#"
        SELECT event_type, at, detail
        FROM {}
        WHERE wish_id = '{}'
        ORDER BY at ASC, id ASC;
        "#,
            self.steerwish_events_table_name, wish_id_sql
        );

        Ok(self
            .db
            .execute_query(&db_config, &query, true)?
            .unwrap_or_default())
    }

    /// Append an event stamped at a book timestamp - the lazy fold-in's
    /// write. Mirrored events carry causal's own time so the registry's
    /// ordering matches the book's.
    pub fn insert_wish_event_at(
        &self,
        wish_id: &str,
        event_type: &str,
        detail: &str,
        at_epoch: f64,
    ) -> DbResult<()> {
        let db_config = self._get_db_config();
        let wish_id_sql = wish_id.replace('\'', "''");
        let event_type_sql = event_type.replace('\'', "''");
        let detail_sql = format!("'{}'", detail.replace('\'', "''"));

        let query = format!(
            r#"
        INSERT INTO {}
        (wish_id, event_type, detail, at)
        VALUES('{}', '{}', {},
               TO_TIMESTAMP({}));
        "#,
            self.steerwish_events_table_name,
            wish_id_sql,
            event_type_sql,
            detail_sql,
            at_epoch
        );
        self.db.execute_query(&db_config, &query, false)?;
        Ok(())
    }

    /// The fold-in's dedupe: this book event already mirrored?
    pub fn has_wish_event_at(&self, wish_id: &str, event_type: &str, at_epoch: f64) -> DbResult<bool> {
        let db_config = self._get_db_config();
        let wish_id_sql = wish_id.replace('\'', "''");
        let event_type_sql = event_type.replace('\'', "''");

        let query = format!(
            r#"
        SELECT COUNT(*) FROM {}
        WHERE wish_id = '{}'
        AND event_type = '{}'
        AND at = TO_TIMESTAMP({});
        "#,
            self.steerwish_events_table_name,
            wish_id_sql,
            event_type_sql,
            at_epoch
        );
        let result = self.db.execute_query(&db_config, &query, true)?;
        let count = result
            .as_ref()
            .and_then(|r| r.first())
            .and_then(|row| row[0].as_ref())
            .and_then(|t| t.parse::<i64>().ok())
            .unwrap_or(0);
        Ok(count != 0)
    }

    /// Create the targets catalog table.
    pub fn create_targets_table(&self) -> DbResult<()> {
        let db_config = self._get_db_config();

        let query = format!(
            r#"
        CREATE TABLE IF NOT EXISTS {}
        (
        id uuid PRIMARY KEY,
        name VARCHAR(255) UNIQUE NOT NULL,
        target_type VARCHAR(50) NOT NULL,
        spec JSONB NOT NULL,
        metadata JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        last_used_at TIMESTAMPTZ
        );
        "#,
            self.targets_table_name
        );

        self.db.execute_query(&db_config, &query, false)?;
        info!("Ensured targets table exists: {}", self.targets_table_name);
        Ok(())
    }

    /// Insert target catalog entry.
    pub fn insert_target(
        &self,
        target_id: &str,
        name: &str,
        target_type: &str,
        spec: &Value,
        metadata: Option<&Value>,
    ) -> DbResult<()> {
        let db_config = self._get_db_config();
        let spec_json = if spec.is_object() {
            serde_json::to_string(spec).unwrap_or_default()
        } else {
            spec.as_str().map(|s| s.to_string()).unwrap_or_default()
        };
        let metadata_json = match metadata {
            Some(m) if !py_falsy(m) => format!("'{}'::jsonb", serde_json::to_string(m).unwrap_or_default().replace('\'', "''")),
            _ => "NULL".to_string(),
        };

        let query = format!(
            r#"
        INSERT INTO {}
        (id, name, target_type, spec, metadata)
        VALUES('{}', '{}', '{}', '{}'::jsonb, {}::jsonb);
        "#,
            self.targets_table_name,
            target_id,
            name,
            target_type,
            spec_json.replace('\'', "''"),
            metadata_json
        );

        self.db.execute_query(&db_config, &query, false)?;
        info!("Inserted target catalog entry: {}", name);
        Ok(())
    }

    /// Fetch list of all targets.
    pub fn fetch_targets_list(&self) -> DbResult<Vec<TextRow>> {
        let db_config = self._get_db_config();

        let query = format!(
            r#"
        SELECT id, name, target_type, spec, metadata, created_at, last_used_at
        FROM {}
        ORDER BY created_at DESC;
        "#,
            self.targets_table_name
        );

        Ok(self
            .db
            .execute_query(&db_config, &query, true)?
            .unwrap_or_default())
    }

    /// Fetch target by ID.
    pub fn fetch_target_by_id(&self, target_id: &str) -> DbResult<Option<TextRow>> {
        let db_config = self._get_db_config();

        let query = format!(
            r#"
        SELECT id, name, target_type, spec, metadata, created_at, last_used_at
        FROM {}
        WHERE id = '{}';
        "#,
            self.targets_table_name, target_id
        );

        Ok(self
            .db
            .execute_query(&db_config, &query, true)?
            .and_then(|r| r.into_iter().next()))
    }

    /// Fetch target by name.
    pub fn fetch_target_by_name(&self, name: &str) -> DbResult<Option<TextRow>> {
        let db_config = self._get_db_config();

        let query = format!(
            r#"
        SELECT id, name, target_type, spec, metadata, created_at, last_used_at
        FROM {}
        WHERE name = '{}';
        "#,
            self.targets_table_name, name
        );

        Ok(self
            .db
            .execute_query(&db_config, &query, true)?
            .and_then(|r| r.into_iter().next()))
    }

    /// Delete target from catalog.
    pub fn delete_target(&self, target_id: &str) -> DbResult<()> {
        let db_config = self._get_db_config();

        let query = format!(
            "DELETE FROM {} WHERE id = '{}';",
            self.targets_table_name, target_id
        );
        self.db.execute_query(&db_config, &query, false)?;
        info!("Deleted target catalog entry: {}", target_id);
        Ok(())
    }

    /// Update the last_used_at timestamp for a credential.
    pub fn update_credential_last_used(&self, credential_id: &str) -> DbResult<()> {
        let db_config = self._get_db_config();

        let query = format!(
            r#"
        UPDATE {}
        SET last_used_at = NOW()
        WHERE id = '{}';
        "#,
            self.credentials_table_name, credential_id
        );

        self.db.execute_query(&db_config, &query, false)?;
        Ok(())
    }
}

// Tests port beside the code they exercise (the python suite under
// tests/unit/ is the parity bar); this module stays empty.
