//! The shared fakes: what python's suite got from
//! `patch('controller.database.execute_query', ...)` and friends,
//! as gateways injected at construction.
//!
//! Rows are TEXT cells - the simple query protocol's shape - where
//! python's fakes handed typed tuples (psycopg2's decoded cells). A
//! python row `(False, 12.0, 'x')` ports as `srow(&["f", "12.0",
//! "x"])`.

use godon_controller::database::{DbConfig, DbError, DbGateway, DbResult, TextRow};
use godon_controller::wmill::{CausalClient, WindmillGateway};
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// All-Some row of text cells.
pub fn srow(cells: &[&str]) -> TextRow {
    cells.iter().map(|c| Some(c.to_string())).collect()
}

/// Row with explicit NULLs: `row(&[Some("x"), None])`.
pub fn row(cells: &[Option<&str>]) -> TextRow {
    cells.iter().map(|c| c.map(|s| s.to_string())).collect()
}

pub type DbResponder = Box<dyn Fn(&str, &str) -> DbResult<Option<Vec<TextRow>>> + Send + Sync>;

/// The fake behind every ported test that patched execute_query.
/// Records (database, query) per call; answers from the responder.
pub struct FakeDb {
    pub calls: Mutex<Vec<(String, String)>>,
    pub responder: DbResponder,
}

impl FakeDb {
    /// python's `side_effect=f(db_config, query)` - sees database name
    /// and full query text.
    pub fn from_fn(
        f: impl Fn(&str, &str) -> DbResult<Option<Vec<TextRow>>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            responder: Box::new(f),
        }
    }

    /// python's `return_value=[rows]` - every call answers the same.
    pub fn rows(rows: Vec<TextRow>) -> Self {
        Self::from_fn(move |_, _| Ok(Some(rows.clone())))
    }

    /// Answer nothing, successfully (the DDL/verb-write shape).
    pub fn ok() -> Self {
        Self::rows(Vec::new())
    }

    /// Always raise (python's `side_effect=Exception(...)`).
    pub fn err(text: &str) -> Self {
        let text = text.to_string();
        Self::from_fn(move |_, _| Err(DbError(text.clone())))
    }

    pub fn queries(&self) -> Vec<String> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .map(|(_, q)| q.clone())
            .collect()
    }

    pub fn databases(&self) -> Vec<String> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .map(|(d, _)| d.clone())
            .collect()
    }
}

impl DbGateway for FakeDb {
    fn execute_query(
        &self,
        db_config: &DbConfig,
        query: &str,
        _with_result: bool,
    ) -> DbResult<Option<Vec<TextRow>>> {
        let db_name = db_config.database.clone().unwrap_or_default();
        self.calls
            .lock()
            .unwrap()
            .push((db_name.clone(), query.to_string()));
        (self.responder)(&db_name, query)
    }

    fn execute_ddl_query(&self, db_config: &DbConfig, query: &str) -> DbResult<()> {
        let db_name = db_config.database.clone().unwrap_or_default();
        self.calls
            .lock()
            .unwrap()
            .push((db_name, query.to_string()));
        Ok(())
    }
}

fn pop<T>(q: &Mutex<VecDeque<T>>, default: T) -> T {
    q.lock().unwrap().pop_front().unwrap_or(default)
}

/// The fake behind python's `patch('...Windmill', return_value=
/// fake_client)`: per-call scripted outcomes (popped in order,
/// defaults when exhausted) plus a full record of what flew.
#[derive(Default)]
pub struct FakeWindmill {
    pub cancel_results: Mutex<VecDeque<Result<(), String>>>,
    pub job_results: Mutex<VecDeque<Result<Option<Value>, String>>>,
    pub completed_results: Mutex<VecDeque<Result<Option<Value>, String>>>,
    pub async_results: Mutex<VecDeque<Result<String, String>>>,
    pub sync_results: Mutex<VecDeque<Result<Value, String>>>,
    pub cancelled: Mutex<Vec<(String, Option<String>)>>,
    pub dispatched: Mutex<Vec<(String, Value, Option<String>)>>,
    pub sync_ran: Mutex<Vec<(String, Value)>>,
}

impl FakeWindmill {
    pub fn queue_async(&self, r: Result<String, String>) -> &Self {
        self.async_results.lock().unwrap().push_back(r);
        self
    }
    pub fn queue_completed(&self, v: Option<Value>) -> &Self {
        self.completed_results
            .lock()
            .unwrap()
            .push_back(Ok(v));
        self
    }
    pub fn queue_sync(&self, v: Value) -> &Self {
        self.sync_results.lock().unwrap().push_back(Ok(v));
        self
    }
    pub fn queue_job(&self, v: Option<Value>) -> &Self {
        self.job_results.lock().unwrap().push_back(Ok(v));
        self
    }
}

impl WindmillGateway for FakeWindmill {
    fn cancel_job(&self, job_id: &str, reason: Option<&str>) -> Result<(), String> {
        self.cancelled
            .lock()
            .unwrap()
            .push((job_id.to_string(), reason.map(|r| r.to_string())));
        pop(&self.cancel_results, Ok(()))
    }

    fn get_job(&self, _job_id: &str) -> Result<Option<Value>, String> {
        pop(&self.job_results, Ok(None))
    }

    fn get_completed_maybe(&self, _job_id: &str) -> Result<Option<Value>, String> {
        pop(&self.completed_results, Ok(None))
    }

    fn run_script_by_path_async(
        &self,
        path: &str,
        args: &Value,
        tag: Option<&str>,
    ) -> Result<String, String> {
        self.dispatched.lock().unwrap().push((
            path.to_string(),
            args.clone(),
            tag.map(|t| t.to_string()),
        ));
        pop(&self.async_results, Ok("job-fake".to_string()))
    }

    fn run_script_by_path(&self, path: &str, args: &Value) -> Result<Value, String> {
        self.sync_ran
            .lock()
            .unwrap()
            .push((path.to_string(), args.clone()));
        pop(&self.sync_results, Ok(Value::Null))
    }
}

/// The fake behind python's urlopen/ask patches on the causal seam.
#[derive(Default)]
pub struct FakeCausal {
    pub pages: Mutex<VecDeque<Option<Value>>>,
    pub asks: Mutex<VecDeque<Result<Value, String>>>,
    pub purge_results: Mutex<VecDeque<Result<u16, String>>>,
    pub asked: Mutex<Vec<Value>>,
    pub purged: Mutex<Vec<String>>,
}

impl FakeCausal {
    pub fn queue_page(&self, v: Option<Value>) -> &Self {
        self.pages.lock().unwrap().push_back(v);
        self
    }
    pub fn queue_ask(&self, r: Result<Value, String>) -> &Self {
        self.asks.lock().unwrap().push_back(r);
        self
    }
}

impl CausalClient for FakeCausal {
    fn fetch_book_page(&self, _wish_id: &str) -> Option<Value> {
        pop(&self.pages, None)
    }

    fn ask_to_plan(&self, plan_request: &Value) -> Result<Value, String> {
        self.asks.lock().unwrap().push_back(Ok(plan_request.clone()));
        pop(&self.asks, Ok(Value::Null))
    }

    fn purge_curves(&self, systemtender_id: &str) -> Result<u16, String> {
        self.purged.lock().unwrap().push(systemtender_id.to_string());
        pop(&self.purge_results, Ok(200))
    }

    fn purge_book_page(&self, wish_id: &str) -> Result<(), String> {
        self.purged.lock().unwrap().push(wish_id.to_string());
        Ok(())
    }
}

/// Shared convenience: a config with a database name pinned, for
/// responders that branch on the db (python branched on db_config).
pub fn cfg_with_db(name: &str) -> DbConfig {
    let mut c = DbConfig::archive_from_env();
    c.database = Some(name.to_string());
    c
}

/// The suite's constructors want the fake as a shared handle.
pub fn arc_db(
    f: impl Fn(&str, &str) -> DbResult<Option<Vec<TextRow>>> + Send + Sync + 'static,
) -> std::sync::Arc<FakeDb> {
    std::sync::Arc::new(FakeDb::from_fn(f))
}
