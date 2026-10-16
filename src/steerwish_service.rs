//! The steerwish registry + lifecycle, ported from
//! controller/steerwish_service.py. The controller owns the wish
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

pub const BEAT_MULTIPLIER: f64 = 3.0;

pub fn heartbeat_fallback_ttl_secs() -> f64 {
    std::env::var("GODON_HEARTBEAT_FALLBACK_TTL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60.0)
}

pub fn checked_band(band: &Value, where_: &str) -> Result<Value, String> {
    let is_band = band
        .as_object()
        .map(|o| o.contains_key("lo") && o.contains_key("hi"))
        .unwrap_or(false);
    if !is_band {
        return Err(format!(
            "{} requires lo and hi (in the outcome measurement units)",
            where_
        ));
    }
    let lo = band.get("lo").and_then(|v| v.as_f64());
    let hi = band.get("hi").and_then(|v| v.as_f64());
    let (Some(lo), Some(hi)) = (lo, hi) else {
        return Err(format!("{} lo and hi must be numbers", where_));
    };
    if !(lo < hi) {
        return Err(format!("{} lo must be below hi", where_));
    }
    if !(lo.is_finite() && hi.is_finite()) {
        return Err(format!("{} lo and hi must be finite", where_));
    }
    if let Some(target) = band.get("target") {
        if !target.is_null() {
            let Some(t) = target.as_f64() else {
                return Err(format!("{} target must be a number", where_));
            };
            if !t.is_finite() {
                return Err(format!("{} target must be finite", where_));
            }
        }
    }
    Ok(band.clone())
}

pub fn checked_claim(claim: &Value, where_: &str) -> Result<Value, String> {
    if !claim.is_object() {
        return Err(format!("{} must be an object (outcome, band)", where_));
    }
    if claim.get("direction").map(|d| !d.is_null()).unwrap_or(false) {
        return Err(format!(
            "{}.direction is refused: the direction rung is not built - declare a band",
            where_
        ));
    }
    let name = claim.get("outcome");
    let name_ok = name
        .and_then(|n| n.as_str())
        .map(|n| !n.trim().is_empty())
        .unwrap_or(false);
    if !name_ok {
        return Err(format!(
            "{}.outcome is required: the plain name of one measured value",
            where_
        ));
    }
    let band = checked_claim_band(claim, where_)?;
    let mut m = Map::new();
    m.insert(
        "outcome".to_string(),
        Value::String(name.and_then(|n| n.as_str()).unwrap_or("").trim().to_string()),
    );
    m.insert("band".to_string(), band);
    Ok(Value::Object(m))
}

fn checked_claim_band(claim: &Value, where_: &str) -> Result<Value, String> {
    match claim.get("band") {
        Some(b) => checked_band(b, &format!("{}.band", where_)),
        None => Err(format!("{}.band requires lo and hi (in the outcome measurement units)", where_)),
    }
}

pub struct SteerwishService {
    pub repo: MetadataDatabaseRepository,
    pub archive_repo: Option<ArchiveDatabaseRepository>,
    pub causal: Arc<dyn CausalClient>,
}

impl SteerwishService {
    pub fn new(with_archive: bool) -> Self {
        Self {
            repo: MetadataDatabaseRepository::new(DbConfig::meta_from_env()),
            archive_repo: if with_archive {
                Some(ArchiveDatabaseRepository::new(DbConfig::archive_from_env()))
            } else {
                None
            },
            causal: Arc::new(crate::wmill::HttpCausal),
        }
    }

    pub fn with_parts(
        db: Arc<dyn DbGateway>,
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
        }
    }

    fn ensure_registry(&self) -> Result<(), String> {
        self.repo.create_steerwish_tables().map_err(|e| e.0)
    }

    pub fn create_steerwish(&self, payload: &Value) -> Result<Value, String> {
        let empty = Map::new();
        let payload_obj = payload.as_object().unwrap_or(&empty);

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
            claims.push(checked_claim(c, &format!("claims[{}]", i))?);
        }
        let mut terms: Vec<Value> = Vec::new();
        for (i, t) in terms_list.iter().enumerate() {
            terms.push(checked_claim(t, &format!("terms[{}]", i))?);
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
            .insert_steerwish(
                &wish_id,
                &outcome,
                &band,
                limits.filter(|l| !l.is_null()),
                budget,
                regime,
                Some(&Value::Array(claims.clone())),
                Some(&Value::Array(terms.clone())),
            )
            .map_err(|e| e.0)?;
        self.repo
            .insert_steerwish_event(&wish_id, "declared", None)
            .map_err(|e| e.0)?;
        info!("Steerwish declared: {} (outcome: {})", wish_id, outcome);

        self.ask_causal_to_plan(
            &wish_id,
            Some(&claims),
            Some(&terms),
            limits.filter(|l| !l.is_null()),
            budget,
        );

        match self.get_steerwish(&wish_id)? {
            Some(wish) => Ok(wish),
            None => Err(format!("inserted steerwish vanished: {}", wish_id)),
        }
    }

    pub fn get_steerwish(&self, wish_id: &str) -> Result<Option<Value>, String> {
        self.ensure_registry()?;
        let mut row = match self.repo.fetch_steerwish_by_id(wish_id).map_err(|e| e.0)? {
            Some(r) => r,
            None => return Ok(None),
        };
        if self.fold_in_book(wish_id) > 0 {
            row = match self.repo.fetch_steerwish_by_id(wish_id).map_err(|e| e.0)? {
                Some(r) => r,
                None => return Ok(None),
            };
        }
        let events = self.repo.fetch_steerwish_events(wish_id).map_err(|e| e.0)?;
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

    pub fn list_steerwishes(&self) -> Result<Vec<Value>, String> {
        self.ensure_registry()?;
        let rows = self.repo.fetch_steerwishes_list().map_err(|e| e.0)?;
        for row in &rows {
            if row[2].as_deref() != Some("closed") {
                self.fold_in_book(&row[0].clone().unwrap_or_default());
            }
        }
        let rows = self.repo.fetch_steerwishes_list().map_err(|e| e.0)?;
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

    pub fn close_steerwish(&self, wish_id: &str) -> Result<Option<Value>, String> {
        let Some(wish) = self.get_steerwish(wish_id)? else {
            return Ok(None);
        };
        if wish.get("state") == Some(&Value::String("closed".to_string())) {
            return Ok(Some(wish));
        }
        self.repo
            .insert_steerwish_event(wish_id, "closed", None)
            .map_err(|e| e.0)?;
        self.unassign_wish(&wish);
        self.get_steerwish(wish_id)
    }

    pub fn update_steerwish(&self, wish_id: &str, payload: &Value) -> Result<Value, String> {
        let empty = Map::new();
        let payload_obj = payload.as_object().unwrap_or(&empty);

        let Some(wish) = self.get_steerwish(wish_id)? else {
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
            claims.push(checked_claim(c, &format!("claims[{}]", i))?);
        }
        let terms_list: Vec<Value> = payload
            .get("terms")
            .and_then(|t| t.as_array())
            .cloned()
            .unwrap_or_default();
        let mut terms: Vec<Value> = Vec::new();
        for (i, t) in terms_list.iter().enumerate() {
            terms.push(checked_claim(t, &format!("terms[{}]", i))?);
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
            .update_steerwish_terms(
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
            .insert_steerwish_event(
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

        self.ask_causal_to_plan(wish_id, Some(&claims), Some(&terms), limits, budget);
        self.get_steerwish(wish_id)?
            .ok_or_else(|| format!("inserted steerwish vanished: {}", wish_id))
    }

    pub fn delete_steerwish(&self, wish_id: &str) -> Value {
        let outcome = (|| -> Result<Value, String> {
            self.ensure_registry()?;
            if self.repo.fetch_steerwish_by_id(wish_id).map_err(|e| e.0)?.is_none() {
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

            match self.close_steerwish(wish_id) {
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

            self.repo.delete_steerwish(wish_id).map_err(|e| e.0)?;
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

    fn sender_alive(&self, sender_uuid: &str) -> (bool, String) {
        let db_name = format!("systemtender_{}", sender_uuid.replace('-', "_"));
        match self.repo.fetch_meta_data(sender_uuid) {
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
            let Ok(seen) = self.repo.has_steerwish_event_at(wish_id, event, tsz) else {
                continue;
            };
            if seen {
                continue;
            }
            let detail = json!({"at": tsz, "book": e.get("detail").cloned().unwrap_or(Value::Null)});
            if let Err(ex) = self.repo.insert_steerwish_event_at(
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
    ) {
        let claims = claims.cloned().unwrap_or_default();
        let terms_list = terms.cloned().unwrap_or_default();

        let mut plan_request = if claims.len() == 1 && terms_list.is_empty() {
            let first = &claims[0];
            let first_band = first.get("band").cloned().unwrap_or(Value::Null);
            let mut band = Map::new();
            band.insert(
                "lo".to_string(),
                json!(first_band.get("lo").and_then(|v| v.as_f64()).unwrap_or(0.0)),
            );
            band.insert(
                "hi".to_string(),
                json!(first_band.get("hi").and_then(|v| v.as_f64()).unwrap_or(0.0)),
            );
            if let Some(t) = first_band.get("target") {
                if !t.is_null() {
                    band.insert("target".to_string(), json!(t.as_f64().unwrap_or(0.0)));
                }
            }
            json!({
                "wish_id": wish_id,
                "outcome": first.get("outcome").cloned().unwrap_or(Value::Null),
                "band": Value::Object(band),
            })
        } else {
            json!({
                "wish_id": wish_id,
                "claims": claims,
                "terms": terms_list,
            })
        };
        if let Some(b) = budget {
            plan_request["budget"] = json!(b);
        }
        if let Some(l) = limits {
            plan_request["limits"] = l.clone();
        }

        let plan_response = match self.causal.ask_to_plan(&plan_request) {
            Ok(r) => r,
            Err(e) => {
                warn!("Wish {}: plan ask failed: {}", wish_id, e);
                let detail = json!({"error": e});
                let _ = self.repo.insert_steerwish_event(
                    wish_id,
                    "plan_error",
                    Some(&serde_json::to_string(&detail).unwrap_or_default()),
                );
                return;
            }
        };

        if plan_response.get("status") == Some(&Value::String("planned".to_string())) {
            let _ = self.repo.insert_steerwish_event(
                wish_id,
                "planned",
                Some(&serde_json::to_string(&plan_response).unwrap_or_default()),
            );
            let moves = plan_response
                .get("moves")
                .and_then(|m| m.as_array())
                .cloned()
                .unwrap_or_default();
            let sender = moves
                .first()
                .and_then(|m| m.get("sender"))
                .and_then(|s| s.as_str())
                .map(|s| s.to_string());
            let path = plan_response
                .get("path")
                .and_then(|p| p.as_array())
                .cloned()
                .unwrap_or_default();
            let receiver = path
                .last()
                .and_then(|r| r.as_str())
                .map(|r| r.to_string());

            if let Some(sender) = &sender {
                let Some(archive) = &self.archive_repo else {
                    warn!(
                        "Wish {}: planned on {} but no archive config - assignment not written",
                        wish_id, sender
                    );
                    return;
                };
                let (alive, reason) = self.sender_alive(sender);
                if !alive {
                    let detail = json!({
                        "error": format!(
                            "sender {} not alive: {} - assignment not planted",
                            sender, reason
                        )
                    });
                    let _ = self.repo.insert_steerwish_event(
                        wish_id,
                        "plan_error",
                        Some(&serde_json::to_string(&detail).unwrap_or_default()),
                    );
                    warn!("Wish {}: sender {} not alive ({}) - no note planted", wish_id, sender, reason);
                } else {
                    let db_name = format!("systemtender_{}", sender.replace('-', "_"));
                    match archive.write_wish_assignment(&db_name, wish_id, "sender") {
                        Ok(()) => {
                            let detail = json!({"sender": sender, "db": db_name});
                            let _ = self.repo.insert_steerwish_event(
                                wish_id,
                                "assigned",
                                Some(&serde_json::to_string(&detail).unwrap_or_default()),
                            );
                            info!("Wish {} assigned to {}", wish_id, db_name);
                        }
                        Err(e) => {
                            warn!("Wish {}: assignment write failed: {}", wish_id, e.0);
                            let detail = json!({"error": format!("assignment write failed: {}", e.0)});
                            let _ = self.repo.insert_steerwish_event(
                                wish_id,
                                "plan_error",
                                Some(&serde_json::to_string(&detail).unwrap_or_default()),
                            );
                        }
                    }
                    if let Some(receiver) = receiver {
                        if receiver != *sender {
                            let receiver_db =
                                format!("systemtender_{}", receiver.replace('-', "_"));
                            match archive.write_wish_assignment(&receiver_db, wish_id, "receiver") {
                                Ok(()) => {
                                    let detail = json!({
                                        "receiver": receiver,
                                        "db": receiver_db,
                                        "role": "receiver",
                                    });
                                    let _ = self.repo.insert_steerwish_event(
                                        wish_id,
                                        "assigned",
                                        Some(&serde_json::to_string(&detail).unwrap_or_default()),
                                    );
                                    info!("Wish {} receiver note in {}", wish_id, receiver_db);
                                }
                                Err(e) => {
                                    warn!("Wish {}: receiver note failed: {}", wish_id, e.0);
                                    let detail =
                                        json!({"error": format!("receiver note failed: {}", e.0)});
                                    let _ = self.repo.insert_steerwish_event(
                                        wish_id,
                                        "plan_error",
                                        Some(&serde_json::to_string(&detail).unwrap_or_default()),
                                    );
                                }
                            }
                        }
                    }
                }
            } else {
                warn!("Wish {}: planned but no move named", wish_id);
            }
        } else {
            let detail = json!({
                "reason": plan_response.get("reason").cloned().unwrap_or(Value::Null),
                "detail": plan_response.get("detail").cloned().unwrap_or(Value::Null),
            });
            let _ = self.repo.insert_steerwish_event(
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
            "budget": row[4].as_ref().and_then(|t| t.parse::<i64>().ok()).map(json!).unwrap_or(Value::Null),
            "regime": row[5].clone().unwrap_or_default(),
            "createdAt": row.get(6).and_then(|c| c.as_ref()).and_then(|t| pg_ts_to_iso(t)),
            "state": row[7].clone().unwrap_or_default(),
            "events": events_val,
        })
    }
}
