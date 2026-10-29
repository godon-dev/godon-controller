//! The configuration layer, ported from controller/config.py. Error
//! strings are part of the response envelopes and therefore the parity
//! surface - they port verbatim, python quirks included (list renders,
//! aggregated numbering, the unused strict-mode flag).

use serde_json::{Map, Value};

/// The type words inside validation error strings. These exact words
/// are client-visible today - the python controller emitted python's
/// type names and the estate's clients read them - so the words port
/// as-is. Everything else in the port speaks rust natively.
fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "NoneType",
        Value::Bool(_) => "bool",
        Value::Number(n) if n.is_f64() => "float",
        Value::Number(_) => "int",
        Value::String(_) => "str",
        Value::Array(_) => "list",
        Value::Object(_) => "dict",
    }
}

/// Internal systemtender capabilities - not customer configurable.
pub fn systemtender_capabilities() -> Value {
    serde_json::json!({
        "linux_performance": {
            "supported_target_types": ["ssh"]
        }
    })
}

/// DatabaseConfig from the environment (controller/config.py).
pub struct DatabaseConfig;

impl DatabaseConfig {
    pub fn archive_db() -> Map<String, Value> {
        let mut m = Map::new();
        m.insert("user".into(), env_opt("GODON_ARCHIVE_DB_USER"));
        m.insert("password".into(), env_opt("GODON_ARCHIVE_DB_PASSWORD"));
        m.insert("host".into(), env_opt("GODON_ARCHIVE_DB_SERVICE_HOST"));
        m.insert("port".into(), env_opt("GODON_ARCHIVE_DB_SERVICE_PORT"));
        m
    }

    pub fn meta_db() -> Map<String, Value> {
        let mut m = Map::new();
        m.insert(
            "user".into(),
            env_or("GODON_METADATA_DB_SERVICE_USER", "meta_data"),
        );
        m.insert(
            "password".into(),
            env_or("GODON_METADATA_DB_SERVICE_PASSWORD", "meta_data"),
        );
        m.insert("host".into(), env_opt("GODON_METADATA_DB_SERVICE_HOST"));
        m.insert("port".into(), env_opt("GODON_METADATA_DB_SERVICE_PORT"));
        m
    }
}

fn env_opt(key: &str) -> Value {
    match std::env::var(key) {
        Ok(v) => Value::String(v),
        Err(_) => Value::Null,
    }
}

fn env_or(key: &str, default: &str) -> Value {
    match std::env::var(key) {
        Ok(v) => Value::String(v),
        Err(_) => Value::String(default.to_string()),
    }
}

/// SystemtenderConfig.extract_systemtender_config - the request's
/// systemtender section, or the python ValueError text.
pub fn extract_systemtender_config(request_data: &Value) -> Result<Value, String> {
    let has = match request_data.as_object() {
        Some(o) => !o.is_empty() && o.contains_key("systemtender"),
        None => false,
    };
    if !has {
        return Err("Invalid systemtender configuration: missing 'systemtender' key".to_string());
    }
    Ok(request_data
        .get("systemtender")
        .cloned()
        .unwrap_or_else(|| Value::Object(Map::new())))
}

/// SystemtenderConfig.validate_constraints_v03 - constraint structure
/// (list of ranges or values). Returns "int" or "categorical".
pub fn validate_constraints_v03(constraints: &Value, param_name: &str) -> Result<&'static str, String> {
    // Pragmatic approach: Accept both dict and list formats
    // - Dict with 'values' -> categorical, normalize to list
    // - List -> validate each item
    let normalized: Value;
    let constraints = match constraints {
        Value::Object(o) => {
            if o.contains_key("values") {
                normalized = Value::Array(vec![constraints.clone()]);
                &normalized
            } else {
                return Err(format!(
                    "Parameter '{}': constraints dict must have 'values' key for categorical parameters",
                    param_name
                ));
            }
        }
        other => other,
    };

    let list = match constraints {
        Value::Array(a) => a,
        _ => {
            return Err(format!(
                "Parameter '{}': constraints must be a list or dict with 'values', got {}",
                param_name,
                json_type_name(constraints)
            ))
        }
    };

    if list.is_empty() {
        return Err(format!(
            "Parameter '{}': constraints list cannot be empty",
            param_name
        ));
    }

    let first_constraint = &list[0];

    let first_obj = match first_constraint {
        Value::Object(o) => o,
        _ => {
            return Err(format!(
                "Parameter '{}': each constraint must be a dict, got {}",
                param_name,
                json_type_name(first_constraint)
            ))
        }
    };

    if first_obj.contains_key("values") {
        let values = match first_obj.get("values") {
            Some(Value::Array(a)) => a,
            Some(other) => {
                return Err(format!(
                    "Parameter '{}': 'values' must be a list, got {}",
                    param_name,
                    json_type_name(other)
                ))
            }
            None => {
                return Err(format!(
                    "Parameter '{}': 'values' must be a list, got NoneType",
                    param_name
                ))
            }
        };

        if values.len() < 2 {
            return Err(format!(
                "Parameter '{}': 'values' must have at least 2 items, got {}",
                param_name,
                values.len()
            ));
        }

        for (i, value) in values.iter().enumerate() {
            if !value.is_string() {
                return Err(format!(
                    "Parameter '{}': all values must be strings, item {} is {}",
                    i,
                    param_name,
                    json_type_name(value)
                ));
            }
        }

        Ok("categorical")
    } else if first_obj.contains_key("step")
        || first_obj.contains_key("lower")
        || first_obj.contains_key("upper")
    {
        for (i, constraint) in list.iter().enumerate() {
            let obj = match constraint {
                Value::Object(o) => o,
                _ => {
                    return Err(format!(
                        "Parameter '{}': constraint {}: must be a dict, got {}",
                        param_name,
                        i,
                        json_type_name(constraint)
                    ))
                }
            };

            let mut missing_fields: Vec<&str> = Vec::new();
            if !obj.contains_key("step") {
                missing_fields.push("step");
            }
            if !obj.contains_key("lower") {
                missing_fields.push("lower");
            }
            if !obj.contains_key("upper") {
                missing_fields.push("upper");
            }

            if !missing_fields.is_empty() {
                return Err(format!(
                    "Parameter '{}': constraint {}: missing required fields: {}",
                    param_name,
                    i,
                    missing_fields.join(", ")
                ));
            }

            for field in ["step", "lower", "upper"] {
                let v = obj.get(field).expect("checked above");
                if !v.is_number() {
                    return Err(format!(
                        "Parameter '{}': constraint {}: '{}' must be numeric, got {}",
                        param_name,
                        i,
                        field,
                        json_type_name(v)
                    ));
                }
            }

            let lower = obj.get("lower").expect("checked above").as_f64().unwrap_or(0.0);
            let upper = obj.get("upper").expect("checked above").as_f64().unwrap_or(0.0);
            if lower >= upper {
                return Err(format!(
                    "Parameter '{}': constraint {}: 'lower' ({}) must be less than 'upper' ({})",
                    param_name,
                    i,
                    obj.get("lower").expect("checked above"),
                    obj.get("upper").expect("checked above")
                ));
            }

            let step = obj.get("step").expect("checked above").as_f64().unwrap_or(0.0);
            if step <= 0.0 {
                return Err(format!(
                    "Parameter '{}': constraint {}: 'step' must be positive, got {}",
                    param_name,
                    i,
                    obj.get("step").expect("checked above")
                ));
            }
        }

        Ok("int")
    } else {
        Err(format!(
            "Parameter '{}': constraint must have either 'values' (categorical) or 'step/lower/upper' (integer range)",
            param_name
        ))
    }
}

/// SystemtenderConfig.validate_guardrails_v03.
pub fn validate_guardrails_v03(systemtender_config: &Value) -> Result<(), String> {
    let cfg_obj = match systemtender_config.as_object() {
        Some(o) => o,
        None => return Ok(()), // python: `'guardrails' not in <non-dict>` raises; see module note
    };

    let guardrails = match cfg_obj.get("guardrails") {
        Some(g) => g,
        None => return Ok(()), // Guardrails are optional
    };

    let list = match guardrails {
        Value::Array(a) => a,
        other => {
            return Err(format!(
                "'guardrails' must be a list, got {}",
                json_type_name(other)
            ))
        }
    };

    for (idx, guardrail) in list.iter().enumerate() {
        let obj = match guardrail {
            Value::Object(o) => o,
            other => {
                return Err(format!(
                    "Guardrail {}: must be a dict, got {}",
                    idx,
                    json_type_name(other)
                ))
            }
        };

        let name = match obj.get("name") {
            Some(n) => n,
            None => return Err(format!("Guardrail {}: missing required field 'name'", idx)),
        };

        let name_str = name.as_str().unwrap_or("").to_string();
        if !name.is_string() || name_str.trim().is_empty() {
            return Err(format!("Guardrail {}: 'name' must be a non-empty string", idx));
        }

        match obj.get("hard_limit") {
            Some(h) => {
                if !h.is_number() {
                    return Err(format!(
                        "Guardrail {} ({}): 'hard_limit' must be numeric, got {}",
                        idx,
                        name_str,
                        json_type_name(h)
                    ));
                }
            }
            None => {
                return Err(format!(
                    "Guardrail {} ({}): missing required field 'hard_limit'",
                    idx, name_str
                ))
            }
        }

        let recon = match obj.get("reconnaissance") {
            Some(r) => r,
            None => {
                return Err(format!(
                    "Guardrail {} ({}): missing required section 'reconnaissance'",
                    idx, name_str
                ))
            }
        };

        let recon_obj = match recon {
            Value::Object(o) => o,
            other => {
                return Err(format!(
                    "Guardrail {} ({}): 'reconnaissance' must be a dict, got {}",
                    idx,
                    name_str,
                    json_type_name(other)
                ))
            }
        };

        let required_recon_fields = ["service"];
        let missing_fields: Vec<&str> = required_recon_fields
            .iter()
            .filter(|f| !recon_obj.contains_key(**f))
            .copied()
            .collect();

        if !missing_fields.is_empty() {
            return Err(format!(
                "Guardrail {} ({}): 'reconnaissance' missing required fields: {}",
                idx,
                name_str,
                missing_fields.join(", ")
            ));
        }

        let valid_services = ["prometheus", "http"];
        let service = recon_obj.get("service").expect("checked above");
        let service_str = service.as_str().unwrap_or("");
        if !valid_services.contains(&service_str) {
            return Err(format!(
                "Guardrail {} ({}): service '{}' not supported. Valid services: {}",
                idx,
                name_str,
                service_str,
                valid_services.join(", ")
            ));
        }

        if let Some(q) = recon_obj.get("query") {
            let ok = q.as_str().map(|s| !s.trim().is_empty()).unwrap_or(false);
            if !ok {
                return Err(format!(
                    "Guardrail {} ({}): 'query' must be a non-empty string",
                    idx, name_str
                ));
            }
        }

        if let Some(s) = recon_obj.get("stabilization_seconds") {
            if !(s.is_i64() || s.is_u64()) || s.as_f64().unwrap_or(0.0) < 0.0 {
                return Err(format!(
                    "Guardrail {} ({}): 'stabilization_seconds' must be a non-negative integer",
                    idx, name_str
                ));
            }
        }

        if let Some(s) = recon_obj.get("samples") {
            if !(s.is_i64() || s.is_u64()) || s.as_f64().unwrap_or(0.0) < 1.0 {
                return Err(format!(
                    "Guardrail {} ({}): 'samples' must be an integer >= 1",
                    idx, name_str
                ));
            }
        }

        if let Some(iv) = recon_obj.get("interval") {
            if !iv.is_number() || iv.as_f64().unwrap_or(0.0) < 0.0 {
                return Err(format!(
                    "Guardrail {} ({}): 'interval' must be an integer >= 1",
                    idx, name_str
                ));
            }
        }
    }

    Ok(())
}

/// SystemtenderConfig.validate_rollback_strategies_v03.
pub fn validate_rollback_strategies_v03(systemtender_config: &Value) -> Result<(), String> {
    let cfg_obj = match systemtender_config.as_object() {
        Some(o) => o,
        None => return Ok(()),
    };

    let strategies = match cfg_obj.get("rollback_strategies") {
        Some(s) => s,
        None => return Ok(()), // Rollback strategies are optional
    };

    let strategies_obj = match strategies {
        Value::Object(o) => o,
        other => {
            return Err(format!(
                "'rollback_strategies' must be a dict, got {}",
                json_type_name(other)
            ))
        }
    };

    use std::collections::BTreeSet;
    let mut strategy_names: BTreeSet<String> = BTreeSet::new();

    for (strategy_name, strategy) in strategies_obj.iter() {
        let obj = match strategy {
            Value::Object(o) => o,
            other => {
                return Err(format!(
                    "Rollback strategy '{}': must be a dict, got {}",
                    strategy_name,
                    json_type_name(other)
                ))
            }
        };

        strategy_names.insert(strategy_name.clone());

        let required_fields = [
            "consecutive_failures",
            "target_state",
            "max_attempts",
            "on_failure",
            "timeout_seconds",
            "after",
        ];
        let missing_fields: Vec<&str> = required_fields
            .iter()
            .filter(|f| !obj.contains_key(**f))
            .copied()
            .collect();

        if !missing_fields.is_empty() {
            return Err(format!(
                "Rollback strategy '{}': missing required fields: {}",
                strategy_name,
                missing_fields.join(", ")
            ));
        }

        let consecutive_failures = obj.get("consecutive_failures").expect("checked above");
        if !(consecutive_failures.is_i64() || consecutive_failures.is_u64())
            || consecutive_failures.as_f64().unwrap_or(0.0) < 1.0
        {
            return Err(format!(
                "Rollback strategy '{}': 'consecutive_failures' must be an integer >= 1",
                strategy_name
            ));
        }

        let valid_states = "['previous', 'best', 'baseline']";
        let target_state = obj.get("target_state").expect("checked above");
        let target_state_str = target_state.as_str().unwrap_or("");
        if !["previous", "best", "baseline"].contains(&target_state_str) {
            return Err(format!(
                "Rollback strategy '{}': 'target_state' must be one of {}, got '{}'",
                strategy_name, valid_states, target_state_str
            ));
        }

        let max_attempts = obj.get("max_attempts").expect("checked above");
        if !(max_attempts.is_i64() || max_attempts.is_u64())
            || max_attempts.as_f64().unwrap_or(0.0) < 1.0
        {
            return Err(format!(
                "Rollback strategy '{}': 'max_attempts' must be an integer >= 1",
                strategy_name
            ));
        }

        let valid_failure_actions = "['stop', 'continue', 'skip_target']";
        let on_failure = obj.get("on_failure").expect("checked above");
        let on_failure_str = on_failure.as_str().unwrap_or("");
        if !["stop", "continue", "skip_target"].contains(&on_failure_str) {
            return Err(format!(
                "Rollback strategy '{}': 'on_failure' must be one of {}, got '{}'",
                strategy_name, valid_failure_actions, on_failure_str
            ));
        }

        let timeout_seconds = obj.get("timeout_seconds").expect("checked above");
        if !(timeout_seconds.is_i64() || timeout_seconds.is_u64())
            || timeout_seconds.as_f64().unwrap_or(0.0) < 1.0
        {
            return Err(format!(
                "Rollback strategy '{}': 'timeout_seconds' must be an integer >= 1",
                strategy_name
            ));
        }

        let after = obj.get("after").expect("checked above");
        let after_obj = match after {
            Value::Object(o) => o,
            other => {
                return Err(format!(
                    "Rollback strategy '{}': 'after' must be a dict, got {}",
                    strategy_name,
                    json_type_name(other)
                ))
            }
        };

        let action = match after_obj.get("action") {
            Some(a) => a,
            None => {
                return Err(format!(
                    "Rollback strategy '{}': 'after' section missing required field 'action'",
                    strategy_name
                ))
            }
        };

        let valid_after_actions = "['pause', 'continue', 'stop']";
        let action_str = action.as_str().unwrap_or("");
        if !["pause", "continue", "stop"].contains(&action_str) {
            return Err(format!(
                "Rollback strategy '{}': 'after.action' must be one of {}, got '{}'",
                strategy_name, valid_after_actions, action_str
            ));
        }

        if action_str == "pause" {
            match after_obj.get("duration") {
                None => {
                    return Err(format!(
                        "Rollback strategy '{}': 'after.duration' is required when 'after.action' is 'pause'",
                        strategy_name
                    ))
                }
                Some(d) => {
                    if !(d.is_i64() || d.is_u64()) || d.as_f64().unwrap_or(0.0) < 1.0 {
                        return Err(format!(
                            "Rollback strategy '{}': 'after.duration' must be an integer >= 1",
                            strategy_name
                        ));
                    }
                }
            }
        }
    }

    // Validate strategy references in targets
    if let Some(Value::Object(effectuation)) = cfg_obj.get("effectuation") {
        if let Some(Value::Array(targets)) = effectuation.get("targets") {
            for (idx, target) in targets.iter().enumerate() {
                let target_obj = match target.as_object() {
                    Some(o) => o,
                    None => continue, // python: `'rollback' in <non-dict>` raises; see module note
                };
                if let Some(Value::Object(rollback)) = target_obj.get("rollback") {
                    let enabled = rollback
                        .get("enabled")
                        .cloned()
                        .unwrap_or(Value::Bool(false));
                    if enabled == Value::Bool(true) {
                        let strategy_ref = match rollback.get("strategy") {
                            Some(s) => s,
                            None => {
                                return Err(format!(
                                    "Target {}: rollback.enabled=true but 'rollback.strategy' is not specified",
                                    idx
                                ))
                            }
                        };
                        let strategy_ref_str = strategy_ref.as_str().unwrap_or("");
                        if !strategy_names.contains(strategy_ref_str) {
                            let available = if strategy_names.is_empty() {
                                "none".to_string()
                            } else {
                                strategy_names
                                    .iter()
                                    .map(|s| s.as_str())
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            };
                            return Err(format!(
                                "Target {}: references undefined rollback strategy '{}'. Available strategies: {}",
                                idx, strategy_ref_str, available
                            ));
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

/// SystemtenderConfig.validate_minimal - catches catastrophic config
/// errors early, aggregating numbered errors exactly as python did.
/// The strict_mode parameter is accepted and read for meta overrides,
/// exactly as in python: it has no observable effect there either.
pub fn validate_minimal(systemtender_config: &Value, strict_mode: bool) -> Result<Value, String> {
    let mut _strict_mode = strict_mode;
    let mut errors: Vec<String> = Vec::new();
    let warnings: Vec<String> = Vec::new();

    let empty = Map::new();
    let meta_section = systemtender_config
        .get("meta")
        .and_then(|m| m.as_object())
        .unwrap_or(&empty);

    if let Some(Value::Bool(strict)) = meta_section.get("strict_validation") {
        _strict_mode = *strict;
    }

    let config_version = meta_section
        .get("configVersion")
        .and_then(|v| v.as_str())
        .unwrap_or("0.2");
    if config_version != "0.3" {
        errors.push(format!(
            "Config version '{}' is outdated. Current version is '0.3'. Please update your configuration to v0.3 format. See documentation for migration guide.",
            config_version
        ));
    }

    let systemtender_type = systemtender_config
        .get("systemtender")
        .and_then(|s| s.get("type"))
        .filter(|t| !t.is_null() && !(t.is_string() && t.as_str() == Some("")))
        .cloned();

    if systemtender_type.is_none() {
        errors.push(
            "Missing systemtender.type. Example: systemtender: {type: 'linux_performance'}"
                .to_string(),
        );
    }

    let objectives_ok = systemtender_config
        .get("objectives")
        .map(|o| o.as_array().map(|a| !a.is_empty()).unwrap_or(false))
        .unwrap_or(false);
    if !objectives_ok {
        errors.push(
            "Missing or empty objectives array. Example: objectives: [{name: 'latency', goal: 'MINIMIZE', reconnaissance: {...}}]".to_string(),
        );
    }

    let target_refs = systemtender_config.get("effectuation").and_then(|e| e.get("targetRefs"));
    let target_refs_ok = match target_refs {
        Some(Value::Array(a)) => !a.is_empty(),
        _ => false,
    };
    if !target_refs_ok {
        errors.push(
            "Missing or empty effectuation.targetRefs. Targets must be registered via the API first, then referenced by ID or name. Example: effectuation: {targetRefs: ['my-server', '550e8400-...']}".to_string(),
        );
    }

    let systemtender_type_str = systemtender_config
        .get("systemtender")
        .and_then(|s| s.get("type"))
        .and_then(|t| t.as_str())
        .unwrap_or("");
    let supported_categories = ["sysctl", "sysfs", "cpufreq", "ethtool"];
    let empty_settings = Map::new();
    let settings = systemtender_config
        .get("settings")
        .and_then(|s| s.as_object())
        .unwrap_or(&empty_settings);

    if systemtender_type_str == "linux_performance" {
        let has_any_category = supported_categories.iter().any(|c| settings.contains_key(*c));
        if !has_any_category {
            errors.push(format!(
                "Missing settings configuration. At least one category required: {}",
                supported_categories.join(", ")
            ));
        }
    }

    for category in supported_categories {
        if let Some(category_cfg) = settings.get(category) {
            let category_obj = match category_cfg.as_object() {
                Some(o) => o,
                None => {
                    errors.push(format!(
                        "settings.{} must be a dict, got {}",
                        category,
                        json_type_name(category_cfg)
                    ));
                    continue;
                }
            };

            for (param_name, param_config) in category_obj.iter() {
                if param_name.trim().is_empty() {
                    errors.push(format!(
                        "settings.{}: parameter name cannot be empty or whitespace",
                        category
                    ));
                    continue;
                }

                let param_obj = match param_config.as_object() {
                    Some(o) => o,
                    None => {
                        errors.push(format!(
                            "settings.{}.{}: must be a dict, got {}",
                            category,
                            param_name,
                            json_type_name(param_config)
                        ));
                        continue;
                    }
                };

                if category == "ethtool" {
                    for (ethtool_param, ethtool_config) in param_obj.iter() {
                        let ethtool_obj = match ethtool_config.as_object() {
                            Some(o) => o,
                            None => {
                                errors.push(format!(
                                    "settings.ethtool.{}.{}: must be a dict, got {}",
                                    param_name,
                                    ethtool_param,
                                    json_type_name(ethtool_config)
                                ));
                                continue;
                            }
                        };

                        let constraints = match ethtool_obj.get("constraints") {
                            Some(c) => c,
                            None => {
                                errors.push(format!(
                                    "settings.ethtool.{}.{}: missing 'constraints' field",
                                    param_name, ethtool_param
                                ));
                                continue;
                            }
                        };

                        if let Err(e) = validate_constraints_v03(
                            constraints,
                            &format!("ethtool.{}.{}", param_name, ethtool_param),
                        ) {
                            errors.push(e);
                        }
                    }
                    continue;
                }

                let constraints = match param_obj.get("constraints") {
                    Some(c) => c,
                    None => {
                        errors.push(format!(
                            "settings.{}.{}: missing 'constraints' field. Example: constraints: [{{step: 100, lower: 4096, upper: 131072}}]",
                            category, param_name
                        ));
                        continue;
                    }
                };

                if let Err(e) =
                    validate_constraints_v03(constraints, &format!("{}.{}", category, param_name))
                {
                    errors.push(e);
                }
            }
        }
    }

    if let Err(e) = validate_guardrails_v03(systemtender_config) {
        errors.push(e);
    }

    if let Err(e) = validate_rollback_strategies_v03(systemtender_config) {
        errors.push(e);
    }

    // The cooperation section is optimizer-era residue: its
    // cooperation-requires-parallel cross-check was dropped by #97,
    // resurrected here at some point, and is deleted again (2026-10-29,
    // the #96/#97 rewind). The section is ignored; live declarations
    // stopped carrying it (godon#418 pruned the templates).

    // The completion sections are optimizer-era and dropped by seal:
    // worker exits reduce to the stop flag and work-complete (robots
    // #236 — budgets, wall clock, quality exit deleted) and the door
    // refuses both spellings (controller #96; the #97 amendment refused
    // the run/cooperation sections outright, later walked back so
    // run.parallel could live). A systemtender runs until work is
    // complete, a stop is requested, or the worker fails. Refuse the
    // words loudly — declarations carry only live vocabulary.
    for spelling in ["completion", "completion_criteria"] {
        if systemtender_config
            .get("run")
            .and_then(|r| r.get(spelling))
            .is_some()
        {
            errors.push(format!(
                "'run.{spelling}' is optimizer-era and dropped (robots #236, controller #96/#97) - a systemtender runs until work is complete, a stop is requested, or the worker fails"
            ));
        }
    }

    if let Some(Value::Array(refs)) = systemtender_config
        .get("effectuation")
        .and_then(|e| e.get("targetRefs"))
    {
        for (idx, r) in refs.iter().enumerate() {
            let ok = r.as_str().map(|s| !s.trim().is_empty()).unwrap_or(false);
            if !ok {
                errors.push(format!(
                    "targetRefs[{}]: must be a non-empty string (target ID or name)",
                    idx
                ));
            }
        }
    }

    if let Some(Value::Array(objectives)) = systemtender_config.get("objectives") {
        for (idx, objective) in objectives.iter().enumerate() {
            let obj_obj = match objective.as_object() {
                Some(o) => o,
                None => {
                    errors.push(format!(
                        "Objective {}: must be a dict, got {}",
                        idx,
                        json_type_name(objective)
                    ));
                    continue;
                }
            };

            if let Some(recon) = obj_obj.get("reconnaissance") {
                let recon_obj = match recon.as_object() {
                    Some(o) => o,
                    None => {
                        errors.push(format!(
                            "Objective {}: 'reconnaissance' must be a dict, got {}",
                            idx,
                            json_type_name(recon)
                        ));
                        continue;
                    }
                };

                let mut required_recon_fields: Vec<&str> = vec!["service"];
                if systemtender_type_str == "linux_performance" {
                    required_recon_fields.push("query");
                }
                let missing_fields: Vec<&str> = required_recon_fields
                    .iter()
                    .filter(|f| !recon_obj.contains_key(**f))
                    .copied()
                    .collect();

                if !missing_fields.is_empty() {
                    let obj_name = obj_obj
                        .get("name")
                        .and_then(|n| n.as_str())
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| format!("#{}", idx));
                    errors.push(format!(
                        "Objective {} ({}): 'reconnaissance' missing required fields: {}. Example: reconnaissance: {{service: 'prometheus', query: 'rate(...)'}}",
                        idx,
                        obj_name,
                        missing_fields.join(", ")
                    ));
                }

                if let Some(service) = recon_obj.get("service") {
                    let valid_services = ["prometheus", "http"];
                    let service_str = service.as_str().unwrap_or("");
                    if !valid_services.contains(&service_str) {
                        let obj_name = obj_obj
                            .get("name")
                            .and_then(|n| n.as_str())
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| format!("#{}", idx));
                        errors.push(format!(
                            "Objective {} ({}): service '{}' not supported. Valid services: {}",
                            idx,
                            obj_name,
                            service_str,
                            valid_services.join(", ")
                        ));
                    }
                }

                if let Some(q) = recon_obj.get("query") {
                    let ok = q.as_str().map(|s| !s.trim().is_empty()).unwrap_or(false);
                    if !ok {
                        let obj_name = obj_obj
                            .get("name")
                            .and_then(|n| n.as_str())
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| format!("#{}", idx));
                        errors.push(format!(
                            "Objective {} ({}): 'query' must be a non-empty string",
                            idx, obj_name
                        ));
                    }
                }

                let obj_name_for = |key: &str| -> String {
                    obj_obj
                        .get("name")
                        .and_then(|n| n.as_str())
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| format!("#{}", idx))
                };
                let _ = obj_name_for;

                if let Some(s) = recon_obj.get("stabilization_seconds") {
                    if !(s.is_i64() || s.is_u64()) || s.as_f64().unwrap_or(0.0) < 0.0 {
                        let obj_name = obj_obj
                            .get("name")
                            .and_then(|n| n.as_str())
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| format!("#{}", idx));
                        errors.push(format!(
                            "Objective {} ({}): 'stabilization_seconds' must be a non-negative integer",
                            idx, obj_name
                        ));
                    }
                }

                if let Some(s) = recon_obj.get("samples") {
                    if !(s.is_i64() || s.is_u64()) || s.as_f64().unwrap_or(0.0) < 1.0 {
                        let obj_name = obj_obj
                            .get("name")
                            .and_then(|n| n.as_str())
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| format!("#{}", idx));
                        errors.push(format!(
                            "Objective {} ({}): 'samples' must be an integer >= 1",
                            idx, obj_name
                        ));
                    }
                }

                if let Some(iv) = recon_obj.get("interval") {
                    if !iv.is_number() || iv.as_f64().unwrap_or(0.0) < 0.0 {
                        let obj_name = obj_obj
                            .get("name")
                            .and_then(|n| n.as_str())
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| format!("#{}", idx));
                        errors.push(format!(
                            "Objective {} ({}): 'interval' must be an integer >= 1",
                            idx, obj_name
                        ));
                    }
                }
            }
        }
    }

    if !errors.is_empty() {
        let error_count = errors.len();
        let plural = if error_count > 1 { "s" } else { "" };
        let mut error_msg = format!("Config validation failed ({} error{}):\n", error_count, plural);
        let numbered: Vec<String> = errors
            .iter()
            .enumerate()
            .map(|(i, err)| format!("  [{}/{}] {}", i + 1, error_count, err))
            .collect();
        error_msg.push_str(&numbered.join("\n"));
        return Err(error_msg);
    }

    let mut result = Map::new();
    result.insert("success".to_string(), Value::Bool(true));
    if !warnings.is_empty() {
        result.insert("warnings".to_string(), Value::Array(warnings.into_iter().map(Value::String).collect()));
    }
    Ok(Value::Object(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn categorical_dict_normalizes() {
        let c = json!({"values": ["a", "b"]});
        assert_eq!(validate_constraints_v03(&c, "p").unwrap(), "categorical");
    }

    #[test]
    fn int_range_validates() {
        let c = json!([{"step": 100, "lower": 4096, "upper": 131072}]);
        assert_eq!(validate_constraints_v03(&c, "p").unwrap(), "int");
    }

    #[test]
    fn lower_ge_upper_fails() {
        let c = json!([{"step": 1, "lower": 10, "upper": 10}]);
        let err = validate_constraints_v03(&c, "p").unwrap_err();
        assert!(err.contains("'lower' (10) must be less than 'upper' (10)"), "{err}");
    }

    #[test]
    fn optimizer_era_completion_sections_are_refused() {
        let base = json!({
            "meta": {"configVersion": "0.3"},
            "systemtender": {"type": "linux_performance"},
            "objectives": [{"name": "latency", "reconnaissance": {"service": "prometheus", "query": "rate(x)"}}],
            "run": {"parallel": 1},
            "effectuation": {"targetRefs": ["t1"]},
            "settings": {"sysctl": {"vm.swappiness": {"constraints": [{"step": 5, "lower": 0, "upper": 100}]}}},
        });
        // both spellings of the dropped era are refused loudly
        // (the #96 rewind; the #97 amendment's breadth minus the
        // run-section refusal that was walked back for parallel)
        for spelling in ["completion", "completion_criteria"] {
            let mut cfg = base.clone();
            cfg["run"][spelling] = json!({"iterations": {"min": 1, "max": 10}});
            let err = validate_minimal(&cfg, false).unwrap_err();
            assert!(
                err.contains(&format!("'run.{spelling}' is optimizer-era")),
                "{err}"
            );
        }
        // the clean shape still passes
        validate_minimal(&base, false).unwrap();
        // cooperation rides silently (post-#97: ignored, not checked -
        // its cross-check was deleted twice now)
        let mut coop = base.clone();
        coop["cooperation"] = json!({"active": true});
        validate_minimal(&coop, false).unwrap();
    }

    #[test]
    fn values_need_two_items() {
        let c = json!({"values": ["a"]});
        let err = validate_constraints_v03(&c, "p").unwrap_err();
        assert!(err.contains("must have at least 2 items, got 1"), "{err}");
    }

    #[test]
    fn minimal_config_aggregates_numbered_errors() {
        let cfg = json!({});
        let err = validate_minimal(&cfg, true).unwrap_err();
        assert!(err.starts_with("Config validation failed ("), "{err}");
        assert!(err.contains("[1/"), "{err}");
        assert!(err.contains("Missing systemtender.type."), "{err}");
    }

    #[test]
    fn a_working_config_passes() {
        let cfg = json!({
            "meta": {"configVersion": "0.3"},
            "systemtender": {"type": "linux_performance"},
            "objectives": [{"name": "latency", "reconnaissance": {"service": "prometheus", "query": "rate(x)"}}],
            "effectuation": {"targetRefs": ["my-server"]},
            "settings": {"sysctl": {"vm.swappiness": {"constraints": [{"step": 5, "lower": 0, "upper": 100}]}}}
        });
        let result = validate_minimal(&cfg, true).unwrap();
        assert_eq!(result["success"], json!(true));
    }
}
