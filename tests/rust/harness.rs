//! Test seams: the fakes python's tests built by patching
//! execute_query and the wmill module functions at import time.
//! The gateway traits (DbGateway, WindmillGateway, CausalClient) are
//! the injection point the port built for exactly this.

use godon_controller::database::{DbConfig, DbError, DbGateway, DbResult, TextRow};
use godon_controller::wmill::{CausalClient, WindmillGateway};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

/// A row of text cells, None where the cell is NULL.
pub fn row(cells: &[Option<&str>]) -> TextRow {
    cells.iter().map(|c| c.map(|s| s.to_string())).collect()
}

/// All-Some row shorthand.
pub fn srow(cells: &[&str]) -> TextRow {
    cells.iter().map(|c| Some(c.to_string())).collect()
}

pub fn dberr(msg: &str) -> DbError {
    DbError(msg.to_string())
}

// ---------------------------------------------------------------------------
// FakeDb
// ---------------------------------------------------------------------------

/// Records every (database, query) call; answers from the handler.
/// The default handler answers Ok(None) - python's capture pattern.
pub struct FakeDb {
    calls: Mutex<Vec<(String, String)>>,
    handler: Box<dyn Fn(&str) -> DbResult<Option<Vec<TextRow>>> + Send + Sync>,
}

impl FakeDb {
    /// Capture-only: every call records, every answer is Ok(None).
    pub fn capture() -> Arc<Self> {
        Self::with_handler(|_q| Ok(None))
    }

    pub fn with_handler(
        f: impl Fn(&str) -> DbResult<Option<Vec<TextRow>>> + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
            handler: Box::new(f),
        })
    }

    /// Every query seen, in order.
    pub fn queries(&self) -> Vec<String> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .map(|(_, q)| q.clone())
            .collect()
    }

    /// Every (database, query) call, in order.
    pub fn calls(&self) -> Vec<(String, String)> {
        self.calls.lock().unwrap().clone()
    }

    /// The queries aimed at one database.
    pub fn queries_for(&self, database: &str) -> Vec<String> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(d, _)| d == database)
            .map(|(_, q)| q.clone())
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
        self.calls.lock().unwrap().push((
            db_config.database.clone().unwrap_or_default(),
            query.to_string(),
        ));
        (self.handler)(query)
    }

    fn execute_ddl_query(&self, db_config: &DbConfig, query: &str) -> DbResult<()> {
        self.calls.lock().unwrap().push((
            db_config.database.clone().unwrap_or_default(),
            query.to_string(),
        ));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// FakeWindmill
// ---------------------------------------------------------------------------

/// The windmill seam fake: every call is recorded; responses come
/// from per-method queues, and an unanswered call gets the benign
/// default (the permissiveness python's tests had with MagicMock).
#[derive(Default)]
pub struct FakeWindmill {
    pub cancels: Mutex<Vec<(String, Option<String>)>>,
    cancel_q: Mutex<VecDeque<Result<(), String>>>,
    pub job_reads: Mutex<Vec<String>>,
    job_q: Mutex<VecDeque<Result<Option<Value>, String>>>,
    completed_q: Mutex<VecDeque<Result<Option<Value>, String>>>,
    pub async_dispatches: Mutex<Vec<(String, Value, Option<String>)>>,
    async_q: Mutex<VecDeque<Result<String, String>>>,
    async_n: Mutex<u64>,
    pub sync_runs: Mutex<Vec<(String, Value)>>,
    sync_q: Mutex<VecDeque<Result<Value, String>>>,
}

impl FakeWindmill {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn push_cancel(&self, r: Result<(), String>) {
        self.cancel_q.lock().unwrap().push_back(r);
    }

    pub fn push_job(&self, r: Result<Option<Value>, String>) {
        self.job_q.lock().unwrap().push_back(r);
    }

    pub fn push_completed(&self, r: Result<Option<Value>, String>) {
        self.completed_q.lock().unwrap().push_back(r);
    }

    pub fn push_async(&self, r: Result<String, String>) {
        self.async_q.lock().unwrap().push_back(r);
    }

    pub fn push_sync(&self, r: Result<Value, String>) {
        self.sync_q.lock().unwrap().push_back(r);
    }
}

impl WindmillGateway for FakeWindmill {
    fn cancel_job(&self, job_id: &str, reason: Option<&str>) -> Result<(), String> {
        self.cancels
            .lock()
            .unwrap()
            .push((job_id.to_string(), reason.map(|r| r.to_string())));
        self.cancel_q
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Ok(()))
    }

    fn get_job(&self, job_id: &str) -> Result<Option<Value>, String> {
        self.job_reads.lock().unwrap().push(job_id.to_string());
        self.job_q.lock().unwrap().pop_front().unwrap_or(Ok(None))
    }

    fn get_completed_maybe(&self, _job_id: &str) -> Result<Option<Value>, String> {
        self.completed_q
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Ok(None))
    }

    fn run_script_by_path_async(
        &self,
        path: &str,
        args: &Value,
        tag: Option<&str>,
    ) -> Result<String, String> {
        self.async_dispatches.lock().unwrap().push((
            path.to_string(),
            args.clone(),
            tag.map(|t| t.to_string()),
        ));
        let queued = self.async_q.lock().unwrap().pop_front();
        match queued {
            Some(r) => r,
            None => {
                let n = {
                    let mut c = self.async_n.lock().unwrap();
                    *c += 1;
                    *c
                };
                Ok(format!("job-{}", n))
            }
        }
    }

    fn run_script_by_path(&self, path: &str, args: &Value) -> Result<Value, String> {
        self.sync_runs
            .lock()
            .unwrap()
            .push((path.to_string(), args.clone()));
        self.sync_q
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Ok(Value::Object(Default::default())))
    }
}

// ---------------------------------------------------------------------------
// FakeCausal
// ---------------------------------------------------------------------------

/// The causal seam fake: book pages served per wish id, plan asks and
/// curve purges recorded.
#[derive(Default)]
pub struct FakeCausal {
    pub book_pages: Mutex<HashMap<String, Option<Value>>>,
    pub plan_asks: Mutex<Vec<Value>>,
    plan_q: Mutex<VecDeque<Result<Value, String>>>,
    pub purges: Mutex<Vec<String>>,
    purge_q: Mutex<VecDeque<Result<u16, String>>>,
    /// Book-card purges (the door's refusals), by wish id.
    pub book_purges: Mutex<Vec<String>>,
}

impl FakeCausal {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn serve_book_page(&self, wish_id: &str, page: Option<Value>) {
        self.book_pages
            .lock()
            .unwrap()
            .insert(wish_id.to_string(), page);
    }

    pub fn push_plan(&self, r: Result<Value, String>) {
        self.plan_q.lock().unwrap().push_back(r);
    }

    pub fn push_purge(&self, r: Result<u16, String>) {
        self.purge_q.lock().unwrap().push_back(r);
    }
}

impl CausalClient for FakeCausal {
    fn fetch_book_page(&self, wish_id: &str) -> Option<Value> {
        self.book_pages
            .lock()
            .unwrap()
            .get(wish_id)
            .cloned()
            .flatten()
    }

    fn ask_to_plan(&self, plan_request: &Value) -> Result<Value, String> {
        self.plan_asks.lock().unwrap().push(plan_request.clone());
        self.plan_q
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Ok(Value::Object(Default::default())))
    }

    fn purge_curves(&self, systemtender_id: &str) -> Result<u16, String> {
        self.purges.lock().unwrap().push(systemtender_id.to_string());
        self.purge_q.lock().unwrap().pop_front().unwrap_or(Ok(200))
    }

    fn purge_book_page(&self, wish_id: &str) -> Result<(), String> {
        self.book_purges.lock().unwrap().push(wish_id.to_string());
        Ok(())
    }
}
