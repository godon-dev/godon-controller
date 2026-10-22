//! The wish registry + lifecycle, ported from
//! controller/wish_service.py. The controller owns the wish
//! OBJECT and its lifecycle; causal owns the steering CALCULATION -
//! this service asks, never computes. State is the latest lifecycle
//! event, derived on read, never stored.

use crate::database::{ArchiveDatabaseRepository, DbConfig, DbGateway, MetadataDatabaseRepository, TextRow};
use crate::timefmt::pg_ts_to_iso;
use crate::wmill::CausalClient;
use log::{debug, info, warn};
use serde_json::{json, Map, Value};
use std::sync::Arc;
use uuid::Uuid;
use crate::steer_grammar::{checked_declaration_options, checked_steer_band, checked_steer_claim};

pub const BEAT_MULTIPLIER: f64 = 3.0;

pub fn heartbeat_fallback_ttl_secs() -> f64 {
    std::env::var("GODON_HEARTBEAT_FALLBACK_TTL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60.0)
}


/// The reconciler's retry pace for wish deletions - a marker newer
/// than this belongs to a live executor, not to us.
pub const WISH_RECONCILE_STALE_SECONDS: i64 = 300;
/// How long a declared wish may stand unasked before the reconciler
/// re-dispatches its plan ask. Plan jobs answer in seconds-to-minutes;
/// fifteen minutes is patience, not eagerness.
pub const WISH_ASK_PATIENCE_SECONDS: i64 = 900;

pub struct WishService {
    pub repo: MetadataDatabaseRepository,
    pub archive_repo: Option<ArchiveDatabaseRepository>,
    pub causal: Arc<dyn CausalClient>,
    pub wmill: Arc<dyn crate::wmill::WindmillGateway>,
}

impl WishService {
    pub fn new(with_archive: bool) -> Self {
        Self {
            repo: MetadataDatabaseRepository::new(DbConfig::meta_from_env()),
            archive_repo: if with_archive {
                Some(ArchiveDatabaseRepository::new(DbConfig::archive_from_env()))
            } else {
                None
            },
            causal: Arc::new(crate::wmill::HttpCausal),
            wmill: Arc::new(crate::wmill::HttpWindmill),
        }
    }

    pub fn with_parts(
        db: Arc<dyn DbGateway>,
        wmill: Arc<dyn crate::wmill::WindmillGateway>,
        causal: Arc<dyn CausalClient>,
        with_archive: bool,
    ) -> Self {
        Self {
            repo: MetadataDatabaseRepository::with_gateway(DbConfig::meta_from_env(), db.clone()),
            archive_repo: if with_archive {
                Some(ArchiveDatabaseRepository::with_gateway(
                    DbConfig::archive_from_env(),
                    db,
                ))
            } else {
                None
            },
            causal,
            wmill,
        }
    }

    fn ensure_registry(&self) -> Result<(), String> {
        self.repo.create_wish_tables().map_err(|e| e.0)
    }

    pub fn create_wish(&self, payload: &Value) -> Result<Value, String> {
        let empty = Map::new();
        let payload_obj = payload.as_object().unwrap_or(&empty);

        // §XVII: two discriminators, flat - type picks the grammar
        // (steer or omitted-era hold; seek = the walk), form is a
        // field of the walk grammar only.
        let wish_type = match payload.get("type") {
            None | Some(Value::Null) => {
                return Err(
                    "the wish speaks its type: steer or seek (no omission)".to_string(),
                );
            }
            Some(t) => t
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| t.to_string()),
        };
        if wish_type == "seek" {
            // The walk wishes: a commissioned walk, not a hold. Their
            // door is their own grammar (form, region, criterion,
            // budget, candidates); the registry row carries the body
            // in its own column - never inside claims, whose meaning
            // stays pinned.
            return self.create_seek_wish(payload);
        }
        if wish_type != "steer" {
            return Err(format!(
                "unknown wish type: {} (steer or seek)",
                wish_type
            ));
        }
        if payload.get("form").map(|f| !f.is_null()).unwrap_or(false) {
            return Err(
                "form belongs to seek wishes - a hold speaks claims and terms".to_string(),
            );
        }

        if payload_obj.contains_key("outcome") && !payload_obj.get("outcome").unwrap().is_null()

            || payload_obj.contains_key("band") && !payload_obj.get("band").unwrap().is_null()
        {
            return Err(
                "the sugar shape is retired - the wish speaks claims: [{outcome, band}], terms: [{outcome, band}]"
                    .to_string(),
            );
        }
        let claims_field = payload.get("claims");
        let claims_list = match claims_field {
            Some(Value::Array(a)) if !a.is_empty() => a.clone(),
            _ => {
                return Err(
                    "claims is required: a non-empty list of {outcome, band} claims - the atom the gavel judges"
                        .to_string(),
                )
            }
        };
        let terms_list: Vec<Value> = payload
            .get("terms")
            .and_then(|t| t.as_array())
            .cloned()
            .unwrap_or_default();

        let mut claims: Vec<Value> = Vec::new();
        for (i, c) in claims_list.iter().enumerate() {
            claims.push(checked_steer_claim(c, &format!("claims[{}]", i))?);
        }
        let mut terms: Vec<Value> = Vec::new();
        for (i, t) in terms_list.iter().enumerate() {
            terms.push(checked_steer_claim(t, &format!("terms[{}]", i))?);
        }

        let mut names: Vec<String> = Vec::new();
        for c in claims.iter().chain(terms.iter()) {
            names.push(c.get("outcome").and_then(|o| o.as_str()).unwrap_or("").to_string());
        }
        let mut sorted_names = names.clone();
        sorted_names.sort();
        sorted_names.dedup();
        if sorted_names.len() != names.len() {
            let mut duplicated: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
            for n in &names {
                if names.iter().filter(|x| x == &n).count() > 1 {
                    duplicated.insert(n.clone());
                }
            }
            let first = duplicated.iter().next().cloned().unwrap_or_default();
            return Err(format!(
                "one claim per outcome per wish: {} appears twice across claims and terms",
                first
            ));
        }

        let outcome = claims[0]
            .get("outcome")
            .and_then(|o| o.as_str())
            .unwrap_or("")
            .to_string();
        let band = claims[0].get("band").cloned().unwrap_or(Value::Null);

        let budget: Option<i64> = match payload.get("budget") {
            None | Some(Value::Null) => None,
            Some(b) => {
                if b.is_boolean() || !(b.is_i64() || b.is_u64()) || b.as_i64().unwrap_or(0) < 0 {
                    return Err(
                        "budget must be a non-negative integer (omitted = upkeep indefinitely)"
                            .to_string(),
                    );
                }
                b.as_i64()
            }
        };

        let regime = payload
            .get("regime")
            .and_then(|r| r.as_str())
            .filter(|r| !r.is_empty())
            .unwrap_or("standing");
        if regime != "standing" {
            return Err(format!(
                "unknown regime: {} (only 'standing' exists today)",
                regime
            ));
        }

        // The two caller-stated declaration options (§XIX): answer
        // timing (default later) and standard (holds default
        // all - a half-kept state is a lying state).
        let options = checked_declaration_options(payload, "all")?;

        let limits = payload.get("limits");
        if let Some(l) = limits {
            if !l.is_null() && !l.is_object() {
                return Err("limits must be an object (exclude, maxChange)".to_string());
            }
            if let Some(exclude) = l.get("exclude") {
                if !exclude.is_null() {
                    let ok = exclude
                        .as_array()
                        .map(|a| a.iter().all(|p| p.is_string()))
                        .unwrap_or(false);
                    if !ok {
                        return Err("limits.exclude must be a list of param names".to_string());
                    }
                }
            }
            if let Some(mc) = l.get("maxChange") {
                if !mc.is_null() {
                    let ok = !mc.is_boolean()
                        && mc.as_f64().map(|v| 0.0 < v && v < 1.0).unwrap_or(false);
                    if !ok {
                        return Err(
                            "limits.maxChange must be a fraction of a param's range from neutral, in (0, 1)"
                                .to_string(),
                        );
                    }
                }
            }
        }

        let wish_id = Uuid::new_v4().to_string();
        self.ensure_registry()?;
        self.repo
            .insert_wish(
                &wish_id,
                &outcome,
                &band,
                limits.filter(|l| !l.is_null()),
                budget,
                regime,
                Some(&Value::Array(claims.clone())),
                Some(&Value::Array(terms.clone())),
                None,
                &options.0,
                &options.1,
            )
            .map_err(|e| e.0)?;
        self.repo
            .insert_wish_event(&wish_id, "declared", None)
            .map_err(|e| e.0)?;
        info!("Steerwish declared: {} (outcome: {})", wish_id, outcome);

        // ── the ask: causal learns the wish and plans it ────────────────
        // Answer timing (§XIX): 'now' asks in the same breath - the
        // consult's answer (planned, or a refusal with numbers) is in
        // this very response; 'later' (the default) dispatches the ask
        // as its own job - the wish stands declared, the answers land
        // as events and surface at serving time and in status.
        if options.0 == "now" {
            self.ask_causal_to_plan(
                &wish_id,
                Some(&claims),
                Some(&terms),
                limits.filter(|l| !l.is_null()),
                budget,
                Some(&options.1),
            );
        } else {
            self.dispatch_plan_ask(&wish_id);
        }

        match self.get_wish(&wish_id)? {
            Some(wish) => Ok(wish),
            None => Err(format!("inserted steerwish vanished: {}", wish_id)),
        }
    }

    fn create_seek_wish(&self, payload: &Value) -> Result<Value, String> {
        /// Declare a seek wish: validate its body at the door, insert
        /// with regime 'seek' (the body rides its own column - claims
        /// keep their pinned meaning), stamp 'declared', ask causal to
        /// plan.
        ///
        /// The ask's refusal is a valid answer: causal's map_reads_it
        /// carries the reading INSIDE the refusal detail, and the
        /// wish's events carry it to the declarer - the refusal is the
        /// answer.
        let walk = crate::seek_grammar::checked_wish_body(payload)?;
        let outcome = walk
            .get("criterion")
            .filter(|c| !c.is_null())
            .and_then(|c| c.get("outcome"))
            .and_then(|o| o.as_str())
            .unwrap_or("cover_now")
            .to_string();
        // Walk wishes default to best (§XIX): a seek's
        // candidates are partial service by nature.
        let options = checked_declaration_options(payload, "best")?;

        let wish_id = Uuid::new_v4().to_string();
        self.ensure_registry()?;
        self.repo
            .insert_wish(
                &wish_id,
                &outcome,
                &Value::Null,
                None,
                walk.get("budget").and_then(|b| b.as_i64()),
                "seek",
                None,
                None,
                Some(&walk),
                &options.0,
                &options.1,
            )
            .map_err(|e| e.0)?;
        self.repo
            .insert_wish_event(&wish_id, "declared", None)
            .map_err(|e| e.0)?;
        info!(
            "Seek wish declared: {} (form: {}, outcome: {})",
            wish_id,
            walk.get("form").and_then(|f| f.as_str()).unwrap_or(""),
            outcome
        );

        if options.0 == "now" {
            self.ask_causal_to_plan_seek(&wish_id, &walk, Some(&options.1));
        } else {
            self.dispatch_plan_ask(&wish_id);
        }

        self.get_wish(&wish_id)?
            .ok_or_else(|| format!("inserted seek wish vanished: {}", wish_id))
    }

    fn ask_causal_to_plan_seek(&self, wish_id: &str, walk: &Value, standard: Option<&str>) {
        /// One HTTP ask seeds causal's book with the seek. The wire is
        /// re-pinned (§XVII): the ask carries type 'seek' and the
        /// admitted walk body under 'seek' - one dialect end to end,
        /// form-keyed; a planned answer names the serving tender (its
        /// dials are the region's); any other answer is a refusal -
        /// map_reads_it arrives with the reading in its detail, and
        /// the event carries it to the declarer verbatim.
        let mut plan_request = json!({
            "wish_id": wish_id,
            "type": "seek",
            "seek": walk,
        });
        if let Some(s) = standard {
            plan_request["standard"] = json!(s);
        }
        let plan_response = match self.causal.ask_to_plan(&plan_request) {
            Ok(r) => r,
            Err(e) => {
                warn!("Wish {}: seek plan ask failed: {}", wish_id, e);
                let detail = json!({"error": e});
                let _ = self.repo.insert_wish_event(
                    wish_id,
                    "plan_error",
                    Some(&serde_json::to_string(&detail).unwrap_or_default()),
                );
                return;
            }
        };

        if plan_response.get("status") == Some(&Value::String("planned".to_string())) {
            let _ = self.repo.insert_wish_event(
                wish_id,
                "planned",
                Some(&serde_json::to_string(&plan_response).unwrap_or_default()),
            );
            let sender = plan_response
                .get("sender")
                .and_then(|s| s.as_str())
                .map(|s| s.to_string());
            let Some(sender) = sender else {
                warn!("Wish {}: planned but no serving tender named", wish_id);
                self.refuse_untendable(wish_id, &[], "the plan names no tender to tend it");
                return;
            };
            // The acceptance door, the same rule as holds: a serving
            // tender that is not alive means the wish is rejected, not
            // parked as planned with a silent plan_error.
            if !self.tending_is_assignable(wish_id, std::slice::from_ref(&sender), &[]) {
                return;
            }
            let Some(archive) = &self.archive_repo else {
                // unreachable: the door already refused on the
                // missing archive config
                return;
            };
            let db_name = format!("systemtender_{}", sender.replace('-', "_"));
            match archive.write_wish_assignment(&db_name, wish_id, "sender") {
                Ok(()) => {
                    let detail = json!({"sender": sender, "db": db_name});
                    let _ = self.repo.insert_wish_event(
                        wish_id,
                        "assigned",
                        Some(&serde_json::to_string(&detail).unwrap_or_default()),
                    );
                    info!("Seek wish {} assigned to {}", wish_id, db_name);
                }
                Err(e) => {
                    warn!("Wish {}: assignment write failed: {}", wish_id, e.0);
                    let detail = json!({"error": format!("assignment write failed: {}", e.0)});
                    let _ = self.repo.insert_wish_event(
                        wish_id,
                        "plan_error",
                        Some(&serde_json::to_string(&detail).unwrap_or_default()),
                    );
                }
            }
        } else {
            let detail = json!({
                "reason": plan_response.get("reason").cloned().unwrap_or(Value::Null),
                "detail": plan_response.get("detail").cloned().unwrap_or(Value::Null),
            });
            let _ = self.repo.insert_wish_event(
                wish_id,
                "refused",
                Some(&serde_json::to_string(&detail).unwrap_or_default()),
            );
            info!(
                "Wish {} refused: {}",
                wish_id,
                plan_response
                    .get("reason")
                    .and_then(|r| r.as_str())
                    .unwrap_or("")
            );
        }
    }

    fn dispatch_plan_ask(&self, wish_id: &str) {
        /// Answer 'later' (§XIX): the consult rides its own job and
        /// the door returns at 'declared'. Every answer - planned,
        /// refused with numbers, plan_error - lands on the wish's
        /// event record when the job runs; status and serving time
        /// surface it. A dispatch that cannot fly asks inline instead:
        /// the wish is never left unasked.
        let args = json!({"wish_id": wish_id});
        match self
            .wmill
            .run_script_by_path_async("f/controller/steerwish_plan", &args, None)
        {
            Ok(_) => info!("Wish {}: plan ask dispatched (answer: later)", wish_id),
            Err(e) => {
                warn!(
                    "Wish {}: plan ask dispatch failed ({}) - asking inline",
                    wish_id, e
                );
                self.plan_wish(wish_id);
            }
        }
    }

    /// The plan ask from stored terms (the dispatched job's body, and
    /// any re-ask): identical answers and event stamps as the inline
    /// path - one ask discipline, two timings. Returns the wish, or
    /// None when the id is unknown.
    pub fn plan_wish(&self, wish_id: &str) -> Option<Value> {
        if self.ensure_registry().is_err() {
            return None;
        }
        let row = match self.repo.fetch_wish_by_id(wish_id) {
            Ok(r) => r,
            Err(_) => return None,
        };
        let Some(row) = row else {
            warn!("Plan ask for unknown wish: {}", wish_id);
            return None;
        };
        let standard = row.get(12).and_then(|c| c.as_deref());
        if row[5].as_deref() == Some("seek") {
            let walk = row
                .get(10)
                .and_then(|c| c.as_deref())
                .and_then(|t| serde_json::from_str::<Value>(t).ok());
            let Some(walk) = walk else {
                warn!("Wish {}: seek row carries no walk body", wish_id);
                return None;
            };
            self.ask_causal_to_plan_seek(wish_id, &walk, standard);
        } else {
            let parse = |cell: &Option<String>| -> Value {
                cell.as_ref()
                    .and_then(|t| serde_json::from_str(t).ok())
                    .unwrap_or(Value::Null)
            };
            let stored_claims = parse(&row.get(8).cloned().flatten());
            let claims = if stored_claims.is_null() {
                json!([{
                    "outcome": row[1].clone().unwrap_or_default(),
                    "band": parse(&row[2].clone()),
                }])
            } else {
                stored_claims
            };
            let terms = parse(&row.get(9).cloned().flatten());
            let terms = if terms.is_null() { json!([]) } else { terms };
            let limits = parse(&row[3].clone());
            let limits = if limits.is_null() { None } else { Some(limits.clone()) };
            let budget = row[4].as_ref().and_then(|t| t.parse::<i64>().ok());
            self.ask_causal_to_plan(
                wish_id,
                claims.as_array(),
                terms.as_array(),
                limits.as_ref(),
                budget,
                standard,
            );
        }
        self.get_wish(wish_id).ok().flatten()
    }

    pub fn get_wish(&self, wish_id: &str) -> Result<Option<Value>, String> {
        self.ensure_registry()?;
        let mut row = match self.repo.fetch_wish_by_id(wish_id).map_err(|e| e.0)? {
            Some(r) => r,
            None => return Ok(None),
        };
        if self.fold_in_book(wish_id) > 0 {
            row = match self.repo.fetch_wish_by_id(wish_id).map_err(|e| e.0)? {
                Some(r) => r,
                None => return Ok(None),
            };
        }

        // The seek's return mirror: the worker banks its RETURN in the
        // serving tender's own ledger (seek_return::<wish_id>); whoever
        // reads the wish pulls it into the event trail ONCE - the same
        // lazy-mirror discipline the book fold-in uses. A dead tender
        // reads as no return, never an error.
        if row.len() > 10 && row[10].as_deref().map(|s| !s.is_empty()).unwrap_or(false) {
            if let Some(archive) = &self.archive_repo {
                let events_so_far = self.repo.fetch_wish_events(wish_id).map_err(|e| e.0)?;
                let has_returned = events_so_far
                    .iter()
                    .any(|e| e[0].as_deref() == Some("returned"));
                if !has_returned {
                    for e in &events_so_far {
                        if e[0].as_deref() != Some("assigned") {
                            continue;
                        }
                        let detail = e.get(2).and_then(|c| c.as_deref()).unwrap_or("");
                        let d: Value = serde_json::from_str(detail).unwrap_or(Value::Null);
                        let db_name = d
                            .get("db")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        let Some(db_name) = db_name else {
                            continue;
                        };
                        if let Some(payload) = archive.read_seek_return(&db_name, wish_id) {
                            self.repo
                                .insert_wish_event(wish_id, "returned", Some(&payload))
                                .map_err(|e| e.0)?;
                            row = match self.repo.fetch_wish_by_id(wish_id).map_err(|e| e.0)? {
                                Some(r) => r,
                                None => return Ok(None),
                            };
                            break;
                        }
                    }
                }
            }
        }

        let events = self.repo.fetch_wish_events(wish_id).map_err(|e| e.0)?;
        let mut wish = self.format_wish(&row, &events);

        if let Some(lifecycle) = self.repo.get_wish_lifecycle(wish_id).map_err(|e| e.0)? {
            if let Some(o) = wish.as_object_mut() {
                o.insert(
                    "state".to_string(),
                    lifecycle.get("state").cloned().unwrap_or(Value::Null),
                );
                o.insert(
                    "deletion_reason".to_string(),
                    lifecycle.get("reason").cloned().unwrap_or(Value::Null),
                );
            }
        }
        Ok(Some(wish))
    }

    pub fn list_wishes(&self) -> Result<Vec<Value>, String> {
        self.ensure_registry()?;
        let rows = self.repo.fetch_wishes_list().map_err(|e| e.0)?;
        for row in &rows {
            if row[2].as_deref() != Some("closed") {
                self.fold_in_book(&row[0].clone().unwrap_or_default());
            }
        }
        let rows = self.repo.fetch_wishes_list().map_err(|e| e.0)?;
        let markers = self.repo.get_all_wish_lifecycle().map_err(|e| e.0)?;
        let mut summaries: Vec<Value> = rows.iter().map(|r| self.format_summary(r)).collect();
        for summary in summaries.iter_mut() {
            if let Some(id) = summary.get("id").and_then(|i| i.as_str()) {
                if let Some(state) = markers.get(id) {
                    if let Some(o) = summary.as_object_mut() {
                        o.insert("state".to_string(), state.clone());
                    }
                }
            }
        }
        Ok(summaries)
    }

    /// The wishes' reconcile family (designs/2026-10-18-worker-death-door,
    /// round 2), driven by the repeating controller job's tick. Two
    /// convergence rules, both idempotent:
    /// - stale deletion markers: the close-and-drop the failure
    ///   envelope prescribes, retried automatically instead of by hand
    /// - unasked declared wishes: the plan ask re-dispatched once its
    ///   patience lapses (the ask is re-ask-safe by design - identical
    ///   answers and event stamps, one ask discipline, two timings)
    pub fn reconcile_wishes(&self) -> Value {
        let mut converged = 0;
        let mut redriven = 0;
        let mut failed = 0;

        let stale = match self.repo.scan_stale_wish_deletions(WISH_RECONCILE_STALE_SECONDS) {
            Ok(ids) => ids,
            Err(e) => {
                log::warn!("Wish deletion scan failed (next tick retries): {}", e.0);
                Vec::new()
            }
        };
        for wish_id in stale {
            // single-flight through the existing slot: a fresh claim
            // (a live executor, manual or otherwise) is not ours to take
            if !self
                .repo
                .claim_wish_deletion(&wish_id, WISH_RECONCILE_STALE_SECONDS)
                .unwrap_or(false)
            {
                continue;
            }
            match self.close_wish(&wish_id) {
                Ok(None) => {
                    // the wish vanished mid-deletion: sweep the marker
                    let _ = self.repo.remove_wish_lifecycle(&wish_id);
                    converged += 1;
                }
                Ok(Some(_)) => match self.repo.delete_wish(&wish_id) {
                    Ok(()) => {
                        let _ = self.repo.remove_wish_lifecycle(&wish_id);
                        log::info!("Reconcile converged wish deletion: {}", wish_id);
                        converged += 1;
                    }
                    Err(e) => {
                        let _ = self.repo.set_wish_lifecycle(
                            &wish_id,
                            "deletion-failed",
                            Some(&format!("row drop failed: {}", e.0)),
                        );
                        failed += 1;
                    }
                },
                Err(e) => {
                    let _ = self.repo.set_wish_lifecycle(
                        &wish_id,
                        "deletion-failed",
                        Some(&format!("causal close failed: {}", e)),
                    );
                    failed += 1;
                }
            }
        }

        let unasked = match self.repo.scan_unasked_wishes(WISH_ASK_PATIENCE_SECONDS) {
            Ok(ids) => ids,
            Err(e) => {
                log::warn!("Unasked-wish scan failed (next tick retries): {}", e.0);
                Vec::new()
            }
        };
        for wish_id in unasked {
            let args = json!({"wish_id": wish_id});
            match self
                .wmill
                .run_script_by_path_async("f/controller/steerwish_plan", &args, None)
            {
                Ok(_) => {
                    log::info!("Reconcile re-drove the plan ask for wish: {}", wish_id);
                    redriven += 1;
                }
                Err(e) => {
                    log::warn!(
                        "Reconcile plan re-dispatch failed for {} (next tick retries): {}",
                        wish_id,
                        e
                    );
                }
            }
        }

        json!({
            "converged": converged,
            "redriven": redriven,
            "failed": failed,
        })
    }

    pub fn close_wish(&self, wish_id: &str) -> Result<Option<Value>, String> {
        let Some(wish) = self.get_wish(wish_id)? else {
            return Ok(None);
        };
        if wish.get("state") == Some(&Value::String("closed".to_string())) {
            return Ok(Some(wish));
        }
        self.repo
            .insert_wish_event(wish_id, "closed", None)
            .map_err(|e| e.0)?;
        self.unassign_wish(&wish);
        self.get_wish(wish_id)
    }

    pub fn update_wish(&self, wish_id: &str, payload: &Value) -> Result<Value, String> {
        let empty = Map::new();
        let payload_obj = payload.as_object().unwrap_or(&empty);

        let Some(wish) = self.get_wish(wish_id)? else {
            return Err(format!("unknown wish: {}", wish_id));
        };
        if wish.get("state") == Some(&Value::String("closed".to_string())) {
            return Err("wish is closed - corrections apply to living wishes".to_string());
        }
        if payload_obj.contains_key("band") && !payload_obj.get("band").unwrap().is_null() {
            return Err(
                "the sugar shape is retired - correct via claims: [{outcome, band}], terms: [{outcome, band}]"
                    .to_string(),
            );
        }
        let claims_list = match payload.get("claims") {
            Some(Value::Array(a)) if !a.is_empty() => a.clone(),
            _ => {
                return Err(
                    "corrections speak claims: a non-empty list of {outcome, band} on the same identity"
                        .to_string(),
                )
            }
        };
        let mut claims: Vec<Value> = Vec::new();
        for (i, c) in claims_list.iter().enumerate() {
            claims.push(checked_steer_claim(c, &format!("claims[{}]", i))?);
        }
        let terms_list: Vec<Value> = payload
            .get("terms")
            .and_then(|t| t.as_array())
            .cloned()
            .unwrap_or_default();
        let mut terms: Vec<Value> = Vec::new();
        for (i, t) in terms_list.iter().enumerate() {
            terms.push(checked_steer_claim(t, &format!("terms[{}]", i))?);
        }

        let mut names: Vec<String> = Vec::new();
        for c in claims.iter().chain(terms.iter()) {
            names.push(c.get("outcome").and_then(|o| o.as_str()).unwrap_or("").to_string());
        }
        let mut sorted_names = names.clone();
        sorted_names.sort();
        sorted_names.dedup();
        if sorted_names.len() != names.len() {
            let mut duplicated: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
            for n in &names {
                if names.iter().filter(|x| x == &n).count() > 1 {
                    duplicated.insert(n.clone());
                }
            }
            let first = duplicated.iter().next().cloned().unwrap_or_default();
            return Err(format!(
                "one claim per outcome per wish: {} appears twice across claims and terms",
                first
            ));
        }

        let limits = payload.get("limits").filter(|l| !l.is_null());
        if let Some(l) = limits {
            if !l.is_object() {
                return Err("limits must be an object (exclude, maxChange)".to_string());
            }
        }

        let budget: Option<i64> = match payload.get("budget") {
            None | Some(Value::Null) => wish
                .get("budget")
                .and_then(|b| b.as_i64()),
            Some(b) => b.as_i64(),
        };

        let old_band = wish.get("band").cloned();
        let reason = payload
            .get("reason")
            .and_then(|r| r.as_str())
            .filter(|r| !r.is_empty())
            .unwrap_or("holder correction");

        self.ensure_registry()?;
        self.repo
            .update_wish_terms(
                wish_id,
                &claims[0].get("band").cloned().unwrap_or(Value::Null),
                limits,
                budget,
                Some(&Value::Array(claims.clone())),
                Some(&Value::Array(terms.clone())),
            )
            .map_err(|e| e.0)?;
        let corrected_detail = json!({"reason": reason, "previous_band": old_band});
        self.repo
            .insert_wish_event(
                wish_id,
                "corrected",
                Some(&serde_json::to_string(&corrected_detail).unwrap_or_default()),
            )
            .map_err(|e| e.0)?;
        info!(
            "Steerwish corrected: {} -> {} claim(s), {} term(s)",
            wish_id,
            claims.len(),
            terms.len()
        );

        // the world moved. The plan ask decides the assignment's fate:
        // a plan with moves re-plants the note (new dial); a zero-move
        // hold keeps the existing note and dial untouched. Tearing the
        // note out before the ask would release a wish the corrected
        // terms already satisfy. A correction is a deliberate holder
        // act - the mover's ask answers in the same breath (§XIX).
        let stored_standard = wish
            .get("standard")
            .and_then(|s| s.as_str())
            .map(|s| s.to_string());
        self.ask_causal_to_plan(
            wish_id,
            Some(&claims),
            Some(&terms),
            limits,
            budget,
            stored_standard.as_deref(),
        );
        self.get_wish(wish_id)?
            .ok_or_else(|| format!("inserted steerwish vanished: {}", wish_id))
    }

    pub fn delete_wish(&self, wish_id: &str) -> Value {
        let outcome = (|| -> Result<Value, String> {
            self.ensure_registry()?;
            if self.repo.fetch_wish_by_id(wish_id).map_err(|e| e.0)?.is_none() {
                self.repo.remove_wish_lifecycle(wish_id).map_err(|e| e.0)?;
                info!("Delete of steerwish {}: already gone", wish_id);
                return Ok(json!({
                    "result": "SUCCESS",
                    "data": {"wish_id": wish_id, "delete_type": "already-gone"},
                }));
            }

            if !self.repo.claim_wish_deletion(wish_id, 300).map_err(|e| e.0)? {
                let lifecycle = self.repo.get_wish_lifecycle(wish_id).ok().flatten();
                let state = lifecycle
                    .as_ref()
                    .and_then(|l| l.get("state"))
                    .cloned()
                    .unwrap_or(json!("deleting"));
                let reason = lifecycle
                    .as_ref()
                    .and_then(|l| l.get("reason"))
                    .cloned()
                    .unwrap_or(Value::Null);
                return Ok(json!({
                    "result": "SUCCESS",
                    "data": {
                        "wish_id": wish_id,
                        "status": state,
                        "reason": reason,
                        "note": "deletion already in progress",
                    }
                }));
            }

            let fail = |reason: String| -> Value {
                log::error!("Steerwish deletion failed for {}: {}", wish_id, reason);
                let _ = self.repo.set_wish_lifecycle(wish_id, "deletion-failed", Some(&reason));
                json!({
                    "result": "FAILURE",
                    "error": reason,
                    "data": {"wish_id": wish_id, "status": "deletion-failed"},
                })
            };

            match self.close_wish(wish_id) {
                Ok(None) => {
                    return Ok(fail("wish vanished mid-deletion; retry the delete".to_string()))
                }
 Err(e) => {
                    return Ok(fail(format!(
                        "causal close failed: {} - nothing forgotten; retry the delete",
                        e
                    )))
                }
                Ok(Some(_)) => {}
            }

            self.repo.delete_wish(wish_id).map_err(|e| e.0)?;
            self.repo.remove_wish_lifecycle(wish_id).map_err(|e| e.0)?;
            info!("Steerwish purged: {}", wish_id);
            Ok(json!({
                "result": "SUCCESS",
                "data": {"wish_id": wish_id, "delete_type": "executed"},
            }))
        })();
        match outcome {
            Ok(v) => v,
            Err(e) => {
                log::error!("Failed to delete steerwish {}: {}", wish_id, e);
                json!({ "result": "FAILURE", "error": e })
            }
        }
    }

    /// Liveness of any tender (the name says what it always checked:
    /// the registry row, then the heartbeat's age against its window).
    /// The door uses it for senders and receivers alike.
    fn tender_alive(&self, tender_uuid: &str) -> (bool, String) {
        let db_name = format!("systemtender_{}", tender_uuid.replace('-', "_"));
        match self.repo.fetch_meta_data(tender_uuid) {
            Ok(rows) if rows.is_empty() => {
                return (false, "unknown tender: not in registry".to_string())
            }
            Err(e) => return (false, format!("registry check failed: {}", e.0)),
            Ok(_) => {}
        }
        let Some(archive) = &self.archive_repo else {
            return (false, "no archive config".to_string());
        };
        let beat = match archive.read_heartbeat(&db_name) {
            Ok(b) => b,
            Err(e) => return (false, format!("archive db unreachable ({}): {}", db_name, e.0)),
        };
        let Some((age, interval)) = beat else {
            return (false, format!("no state row in {} (db missing or empty)", db_name));
        };
        let window = interval
            .map(|i| BEAT_MULTIPLIER * i)
            .unwrap_or_else(heartbeat_fallback_ttl_secs);
        if age <= window {
            (true, format!("heartbeat {:.0}s old (window {:.0}s)", age, window))
        } else {
            (
                false,
                format!("heartbeat stale: last beat {:.0}s ago (window {:.0}s)", age, window),
            )
        }
    }

    /// The acceptance door: every tender the plan hands work to must
    /// be alive, and at least one must exist. A wish nothing live
    /// will tend is refused with reason no_tender - the refusal names
    /// the dead tenders with their reasons and what IS assignable
    /// now, and purges causal's book card so the judge never reads a
    /// wish no one took.
    fn tending_is_assignable(
        &self,
        wish_id: &str,
        senders: &[String],
        receivers: &[String],
    ) -> bool {
        let mut tenders: Vec<&str> = senders.iter().map(|s| s.as_str()).collect();
        for r in receivers {
            tenders.push(r.as_str());
        }
        if tenders.is_empty() {
            self.refuse_untendable(wish_id, &[], "the plan names no tender to tend it");
            return false;
        }
        if self.archive_repo.is_none() {
            self.refuse_untendable(
                wish_id,
                &[],
                "no archive config - assignments cannot be planted",
            );
            return false;
        }
        let mut dead: Vec<Value> = Vec::new();
        for t in &tenders {
            let (alive, reason) = self.tender_alive(t);
            if !alive {
                dead.push(json!({"tender": t, "why": reason}));
            }
        }
        if !dead.is_empty() {
            self.refuse_untendable(wish_id, &dead, "no living tender will take the tending");
            return false;
        }
        true
    }

    /// Stamp the door's refusal and purge the book card. The event
    /// envelope matches causal's own refusals (reason + detail), so
    /// the declarer reads one shape.
    fn refuse_untendable(&self, wish_id: &str, dead: &[Value], why: &str) {
        let assignable = self.live_tenders();
        let detail = json!({
            "reason": "no_tender",
            "detail": {
                "why": why,
                "dead": dead,
                "assignable": assignable,
            },
        });
        let _ = self.repo.insert_wish_event(
            wish_id,
            "refused",
            Some(&serde_json::to_string(&detail).unwrap_or_default()),
        );
        if let Err(e) = self.causal.purge_book_page(wish_id) {
            warn!("Wish {}: book card purge failed: {}", wish_id, e);
        }
        info!("Wish {} refused at the door: no_tender ({})", wish_id, why);
    }

    /// The group's live tenders - what the refusal names as
    /// assignable: registry rows whose heartbeat answers inside its
    /// window.
    fn live_tenders(&self) -> Vec<String> {
        let rows = match self.repo.fetch_systemtenders_list() {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        rows.iter()
            .map(|r| r[0].clone().unwrap_or_default())
            .filter(|id| !id.is_empty())
            .filter(|id| self.tender_alive(id).0)
            .collect()
    }

    fn fold_in_book(&self, wish_id: &str) -> usize {
        let Some(page) = self.causal.fetch_book_page(wish_id) else {
            return 0;
        };
        let mut mirrored = 0;
        let empty = Vec::new();
        let events = page.get("events").and_then(|e| e.as_array()).cloned().unwrap_or(empty);
        for e in &events {
            let Some(tsz) = e.get("tsz").and_then(|t| t.as_f64()) else {
                continue;
            };
            let Some(event) = e.get("event").and_then(|ev| ev.as_str()) else {
                continue;
            };
            let Ok(seen) = self.repo.has_wish_event_at(wish_id, event, tsz) else {
                continue;
            };
            if seen {
                continue;
            }
            let detail = json!({"at": tsz, "book": e.get("detail").cloned().unwrap_or(Value::Null)});
            if let Err(ex) = self.repo.insert_wish_event_at(
                wish_id,
                event,
                &serde_json::to_string(&detail).unwrap_or_default(),
                tsz,
            ) {
                debug!("Wish {}: fold-in of {} skipped: {}", wish_id, event, ex.0);
                continue;
            }
            mirrored += 1;
        }
        mirrored
    }

    fn ask_causal_to_plan(
        &self,
        wish_id: &str,
        claims: Option<&Vec<Value>>,
        terms: Option<&Vec<Value>>,
        limits: Option<&Value>,
        budget: Option<i64>,
        standard: Option<&str>,
    ) {
        let claims = claims.cloned().unwrap_or_default();
        let terms_list = terms.cloned().unwrap_or_default();

        // Claims verbatim, always. The sugar fold (single claim, no
        // terms -> {outcome, band}) starved the book's claims-grammar
        // machinery for exactly the commonest wish shape: the stored
        // terms carried claims=[] and every claims-path gate (the
        // re-walk's remeasure hint, the re-serve) guard-failed silently
        // (found live 2026-10-22: three flights, probe:null walk:null
        // on every read). The declare door retired the sugar shape;
        // the ask side now matches it.
        let mut plan_request = json!({
            "wish_id": wish_id,
            "claims": claims,
            "terms": terms_list,
        });
        if let Some(b) = budget {
            plan_request["budget"] = json!(b);
        }
        if let Some(l) = limits {
            plan_request["limits"] = l.clone();
        }
        if let Some(s) = standard {
            plan_request["standard"] = json!(s);
        }

        let plan_response = match self.causal.ask_to_plan(&plan_request) {
            Ok(r) => r,
            Err(e) => {
                warn!("Wish {}: plan ask failed: {}", wish_id, e);
                let detail = json!({"error": e});
                let _ = self.repo.insert_wish_event(
                    wish_id,
                    "plan_error",
                    Some(&serde_json::to_string(&detail).unwrap_or_default()),
                );
                return;
            }
        };

        if plan_response.get("status") == Some(&Value::String("planned".to_string())) {
            let _ = self.repo.insert_wish_event(
                wish_id,
                "planned",
                Some(&serde_json::to_string(&plan_response).unwrap_or_default()),
            );
            let (senders, receivers) = plan_tenders(&plan_response);

            // ── The acceptance door (the holder's spec, 2026-10-21): a
            // wish is only accepted if it has living tenders that tend
            // the band - otherwise rejected. The door checks every
            // tender the plan hands work to BEFORE anything is
            // planted; a refusal names what IS assignable (the live
            // tenders) and purges the book card, so the judge never
            // reads a wish no one took. (Found live 2026-10-21: wish
            // a6638bee planned with moves [], no assignment planted,
            // no wish-serving line in any tender log - and the judge
            // stamped "landed" on the walk's ambient stream. A
            // spectator got a trophy.)
            if !self.tending_is_assignable(wish_id, &senders, &receivers) {
                return;
            }
            let Some(archive) = &self.archive_repo else {
                // unreachable: the door already refused on the
                // missing archive config
                return;
            };
            // Joint serving: every distinct sender named by the moves
            // gets its own row (each tender's pulse adopts exactly its
            // own moves). A zero-move hold names no sender - the
            // receiver alone tends (its stillness and its published
            // readings are the whole service).
            for sender in &senders {
                let db_name = format!("systemtender_{}", sender.replace('-', "_"));
                match archive.write_wish_assignment(&db_name, wish_id, "sender") {
                    Ok(()) => {
                        let detail = json!({"sender": sender, "db": db_name});
                        let _ = self.repo.insert_wish_event(
                            wish_id,
                            "assigned",
                            Some(&serde_json::to_string(&detail).unwrap_or_default()),
                        );
                        info!("Wish {} assigned to {}", wish_id, db_name);
                    }
                    Err(e) => {
                        warn!("Wish {}: assignment write failed: {}", wish_id, e.0);
                        let detail = json!({"error": format!("assignment write failed: {}", e.0)});
                        let _ = self.repo.insert_wish_event(
                            wish_id,
                            "plan_error",
                            Some(&serde_json::to_string(&detail).unwrap_or_default()),
                        );
                    }
                }
            }
            for receiver in &receivers {
                let receiver_db = format!("systemtender_{}", receiver.replace('-', "_"));
                match archive.write_wish_assignment(&receiver_db, wish_id, "receiver") {
                    Ok(()) => {
                        let detail = json!({
                            "receiver": receiver,
                            "db": receiver_db,
                            "role": "receiver",
                        });
                        let _ = self.repo.insert_wish_event(
                            wish_id,
                            "assigned",
                            Some(&serde_json::to_string(&detail).unwrap_or_default()),
                        );
                        info!("Wish {} receiver note in {}", wish_id, receiver_db);
                    }
                    Err(e) => {
                        warn!("Wish {}: receiver note failed: {}", wish_id, e.0);
                        let detail = json!({"error": format!("receiver note failed: {}", e.0)});
                        let _ = self.repo.insert_wish_event(
                            wish_id,
                            "plan_error",
                            Some(&serde_json::to_string(&detail).unwrap_or_default()),
                        );
                    }
                }
            }
        } else {
            let detail = json!({
                "reason": plan_response.get("reason").cloned().unwrap_or(Value::Null),
                "detail": plan_response.get("detail").cloned().unwrap_or(Value::Null),
            });
            let _ = self.repo.insert_wish_event(
                wish_id,
                "refused",
                Some(&serde_json::to_string(&detail).unwrap_or_default()),
            );
            info!(
                "Wish {} refused: {}",
                wish_id,
                plan_response.get("reason").and_then(|r| r.as_str()).unwrap_or("")
            );
        }
    }

    fn unassign_wish(&self, wish: &Value) {
        let Some(archive) = &self.archive_repo else {
            return;
        };
        let empty = Vec::new();
        let events = wish
            .get("events")
            .and_then(|e| e.as_array())
            .cloned()
            .unwrap_or(empty);
        for event in &events {
            if event.get("type") != Some(&Value::String("assigned".to_string())) {
                continue;
            }
            let detail = event.get("detail");
            let db_name = detail
                .and_then(|d| d.as_str())
                .and_then(|s| serde_json::from_str::<Value>(s).ok())
                .and_then(|d| d.get("db").and_then(|db| db.as_str()).map(|s| s.to_string()));
            if let Some(db_name) = db_name {
                if let Err(e) = archive.delete_wish_assignment(
                    &db_name,
                    wish.get("id").and_then(|i| i.as_str()).unwrap_or(""),
                ) {
                    warn!(
                        "Wish {}: assignment removal failed: {}",
                        wish.get("id").and_then(|i| i.as_str()).unwrap_or(""),
                        e.0
                    );
                }
            }
        }
    }

    fn format_summary(&self, row: &TextRow) -> Value {
        json!({
            "id": row[0].clone().unwrap_or_default(),
            "outcome": row[1].clone().unwrap_or_default(),
            "state": row[2].clone().unwrap_or_default(),
            "createdAt": row.get(3).and_then(|c| c.as_ref()).and_then(|t| pg_ts_to_iso(t)),
        })
    }

    fn format_wish(&self, row: &TextRow, events: &[TextRow]) -> Value {
        let parse_json = |cell: &Option<String>| -> Value {
            cell.as_ref()
                .and_then(|t| serde_json::from_str(t).ok())
                .unwrap_or(Value::Null)
        };
        let stored_claims = row.get(8).cloned().flatten();
        let stored_terms = row.get(9).cloned().flatten();
        let claims = match stored_claims
            .as_ref()
            .and_then(|t| serde_json::from_str::<Value>(t).ok())
        {
            Some(c) if !c.is_null() => c,
            _ => json!([{
                "outcome": row[1].clone().unwrap_or_default(),
                "band": parse_json(&row[2].clone()),
            }]),
        };
        let terms = stored_terms
            .as_ref()
            .and_then(|t| serde_json::from_str::<Value>(t).ok())
            .filter(|t| !t.is_null())
            .unwrap_or_else(|| json!([]));

        let events_val: Vec<Value> = events
            .iter()
            .map(|e| {
                json!({
                    "type": e[0].clone().unwrap_or_default(),
                    "at": e.get(1).and_then(|c| c.as_ref()).and_then(|t| pg_ts_to_iso(t)),
                    "detail": e[2].clone().map(Value::String).unwrap_or(Value::Null),
                })
            })
            .collect();

        json!({
            "id": row[0].clone().unwrap_or_default(),
            "outcome": row[1].clone().unwrap_or_default(),
            "band": parse_json(&row[2].clone()),
            "claims": claims,
            "terms": terms,
            "limits": parse_json(&row[3].clone()),
            "budget": row[4].as_ref().and_then(|t| t.parse::<i64>().ok()).map(|b| json!(b)).unwrap_or(Value::Null),
            "regime": row[5].clone().unwrap_or_default(),
            "createdAt": row.get(6).and_then(|c| c.as_ref()).and_then(|t| pg_ts_to_iso(t)),
            "state": row[7].clone().unwrap_or_default(),
            "seek": parse_json(&row.get(10).cloned().flatten()),
            "answer": row
                .get(11)
                .cloned()
                .flatten()
                .map(Value::String)
                .unwrap_or(Value::Null),
            "standard": row
                .get(12)
                .cloned()
                .flatten()
                .map(Value::String)
                .unwrap_or(Value::Null),
            "events": events_val,
        })
    }
}

/// The tending set a steer plan hands its work to: every distinct
/// sender named by the moves (joint serving - one assignment row
/// each), plus the receivers the plan names - the "receivers" field
/// (zero-move holds carry it: a hold names no sender and no path, so
/// the resolved receiver of every claim rides the answer - found
/// live 2026-10-22: the door refused a planned zero-move wish as
/// "names no tender" while its receiver stood alive in the assignable
/// list) and the path's last hop (chains), deduped, minus the
/// senders. A zero-move hold names no sender - the receiver alone
/// tends: its stillness and its published readings are the whole
/// service.
pub fn plan_tenders(plan_response: &Value) -> (Vec<String>, Vec<String>) {
    let mut senders: Vec<String> = Vec::new();
    for m in plan_response
        .get("moves")
        .and_then(|m| m.as_array())
        .cloned()
        .unwrap_or_default()
    {
        if let Some(s) = m.get("sender").and_then(|s| s.as_str()) {
            if !senders.iter().any(|x| x == s) {
                senders.push(s.to_string());
            }
        }
    }
    let mut receivers: Vec<String> = Vec::new();
    let mut offer = |r: &str| {
        if !senders.iter().any(|x| x == r) && !receivers.iter().any(|x| x == r) {
            receivers.push(r.to_string());
        }
    };
    if let Some(list) = plan_response.get("receivers").and_then(|r| r.as_array()) {
        for r in list {
            if let Some(r) = r.as_str() {
                offer(r);
            }
        }
    }
    if let Some(r) = plan_response
        .get("path")
        .and_then(|p| p.as_array())
        .and_then(|p| p.last())
        .and_then(|r| r.as_str())
    {
        offer(r);
    }
    (senders, receivers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_tenders_reads_every_distinct_sender() {
        // joint serving: two moves by two senders - both rows, no
        // duplicates, receiver alongside
        let plan = json!({
            "moves": [
                {"param": "a", "sender": "t1"},
                {"param": "b", "sender": "t2"},
                {"param": "c", "sender": "t1"},
            ],
            "path": ["t1", "rx"],
        });
        let (senders, receivers) = plan_tenders(&plan);
        assert_eq!(senders, vec!["t1".to_string(), "t2".to_string()]);
        assert_eq!(receivers, vec!["rx".to_string()]);
    }

    #[test]
    fn plan_tenders_receiver_in_senders_not_duplicated() {
        // a self-referential chain end: the sender IS the outcome's
        // own tender - one row, role sender
        let plan = json!({
            "moves": [{"param": "a", "sender": "t1"}],
            "path": ["t1"],
        });
        let (senders, receivers) = plan_tenders(&plan);
        assert_eq!(senders, vec!["t1".to_string()]);
        assert!(receivers.is_empty());
    }

    #[test]
    fn plan_tenders_zero_move_names_the_receiver_alone() {
        // the zero-move hold: no sender, the receiver alone tends
        // (found live 2026-10-21: this shape planted nothing and the
        // judge read the ambient stream)
        let plan = json!({"moves": [], "hold": true, "receivers": ["rx"]});
        let (senders, receivers) = plan_tenders(&plan);
        assert!(senders.is_empty());
        assert_eq!(receivers, vec!["rx".to_string()]);
    }
}
