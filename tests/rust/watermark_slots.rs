//! Port of test_watermark_slots.py — collision-free slot assignment.

use crate::harness::{row, FakeCausal, FakeDb, FakeWindmill};
use godon_controller::systemtender_service::SystemtenderService;
use serde_json::{json, Map, Value};
use std::sync::Arc;

/// The service whose metadata repo answers from a map of
/// {systemtender_id: config} — the python test's mocked repositories.
fn make_service(configs_by_id: &Map<String, Value>) -> SystemtenderService {
    let list_rows: Vec<godon_controller::database::TextRow> = configs_by_id
        .keys()
        .map(|id| row(&[Some(id), Some(&format!("systemtender-{}", id)), None]))
        .collect();
    let configs: std::collections::HashMap<String, String> = configs_by_id
        .iter()
        .map(|(id, cfg)| (id.clone(), serde_json::to_string(cfg).unwrap()))
        .collect();

    let db = FakeDb::with_handler(move |q| {
        if q.contains("SELECT id, name, creation_tsz FROM") {
            return Ok(Some(list_rows.clone()));
        }
        if let Some(pos) = q.find("WHERE id = '") {
            let rest = &q[pos + "WHERE id = '".len()..];
            if let Some(end) = rest.find("';") {
                let id = &rest[..end];
                if let Some(cfg_text) = configs.get(id) {
                    return Ok(Some(vec![row(&[
                        Some(id),
                        Some(&format!("systemtender-{}", id)),
                        None,
                        Some(cfg_text),
                    ])]));
                }
                return Ok(Some(vec![]));
            }
        }
        Ok(None)
    });

    SystemtenderService::with_parts(db, FakeWindmill::new(), FakeCausal::new())
}

fn configs(pairs: &[(&str, Option<i64>)]) -> Map<String, Value> {
    let mut m = Map::new();
    for (id, slot) in pairs {
        let mut inner = Map::new();
        if let Some(s) = slot {
            inner.insert("watermark_slot".to_string(), json!(s));
        }
        m.insert(
            id.to_string(),
            json!({"systemtender": Value::Object(inner)}),
        );
    }
    m
}

fn fresh_config() -> Value {
    json!({"systemtender": {"type": "test"}})
}

#[test]
fn first_systemtender_gets_slot_0() {
    let service = make_service(&configs(&[]));
    let mut config = fresh_config();
    service.assign_watermark_slot(&mut config);
    assert_eq!(config["systemtender"]["watermark_slot"], json!(0));
}

#[test]
fn second_systemtender_gets_slot_1() {
    let service = make_service(&configs(&[("id-1", Some(0))]));
    let mut config = fresh_config();
    service.assign_watermark_slot(&mut config);
    assert_eq!(config["systemtender"]["watermark_slot"], json!(1));
}

#[test]
fn fills_gap_when_slot_freed() {
    let service = make_service(&configs(&[
        ("id-1", Some(0)),
        ("id-2", Some(2)),
        ("id-3", Some(3)),
    ]));
    let mut config = fresh_config();
    service.assign_watermark_slot(&mut config);
    assert_eq!(config["systemtender"]["watermark_slot"], json!(1));
}

#[test]
fn all_six_slots_filled() {
    let service = make_service(&configs(&[
        ("id-1", Some(0)),
        ("id-2", Some(1)),
        ("id-3", Some(2)),
        ("id-4", Some(3)),
        ("id-5", Some(4)),
        ("id-6", Some(5)),
    ]));
    let mut config = fresh_config();
    service.assign_watermark_slot(&mut config);
    // All 6 slots used — should log warning and not assign.
    assert!(config["systemtender"].get("watermark_slot").is_none());
}

#[test]
fn no_collision_across_six_systemtenders() {
    // Simulate creating 6 systemtenders sequentially.
    let mut used_slots: Vec<i64> = Vec::new();
    for _ in 0..6 {
        let pairs: Vec<(&str, Option<i64>)> = used_slots
            .iter()
            .enumerate()
            .map(|(i, slot)| {
                (
                    Box::leak(format!("id-{}", i + 1).into_boxed_str()) as &str,
                    Some(*slot),
                )
            })
            .collect();
        let service = make_service(&configs(&pairs));
        let mut config = fresh_config();
        service.assign_watermark_slot(&mut config);
        let slot = config["systemtender"]["watermark_slot"].as_i64().unwrap();
        assert!(!used_slots.contains(&slot), "Slot {} assigned twice", slot);
        used_slots.push(slot);
    }
    let mut all = used_slots.clone();
    all.sort();
    assert_eq!(all, vec![0, 1, 2, 3, 4, 5]);
}

#[test]
fn systemtender_without_slot_is_skipped() {
    // A systemtender with no watermark_slot in config should not
    // occupy a slot.
    let service = make_service(&configs(&[("id-1", Some(0)), ("id-2", None)]));
    let mut config = fresh_config();
    service.assign_watermark_slot(&mut config);
    assert_eq!(config["systemtender"]["watermark_slot"], json!(1));
}
