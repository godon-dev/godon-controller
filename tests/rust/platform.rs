//! The platform door - the verb rides the fetched arguments. Two generic
//! entries serve every verb (the lanes ride the script tags); the verb
//! dispatch itself is the ported suite's record.

use godon_controller::platform::serve;
use serde_json::json;

#[test]
fn an_unknown_verb_refuses_loudly() {
    let out = serve(json!({"verb": "definitely_not_a_verb"}));
    assert_eq!(out["result"], "FAILURE");
    assert_eq!(out["error"], json!("unknown verb: definitely_not_a_verb"));
}

#[test]
fn a_missing_verb_refuses_loudly() {
    let out = serve(json!({"wish_id": "w-1"}));
    assert_eq!(out["result"], "FAILURE");
    assert_eq!(out["error"], json!("no verb in the arguments: {verb: <name>, ...}"));
}

#[test]
fn a_non_string_verb_refuses_loudly() {
    let out = serve(json!({"verb": 42}));
    assert_eq!(out["result"], "FAILURE");
    assert_eq!(out["error"], json!("no verb in the arguments: {verb: <name>, ...}"));
}

#[test]
fn an_empty_verb_refuses_loudly() {
    let out = serve(json!({"verb": "  "}));
    assert_eq!(out["result"], "FAILURE");
    assert_eq!(out["error"], json!("no verb in the arguments: {verb: <name>, ...}"));
}
