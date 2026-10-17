//! The platform door - the job's own path names the verb, the fetched
//! arguments are the request. One entry per path (the seeder composes
//! each from the spec row and the version pin); the verb dispatch
//! itself is the ported suite's record.

use godon_controller::platform::serve;
use serde_json::json;

#[test]
fn an_unknown_path_verb_refuses_loudly() {
    let out = serve("f/controller/definitely_not_a_verb", json!({}));
    assert_eq!(out["result"], "FAILURE");
    assert_eq!(out["error"], json!("unknown verb: definitely_not_a_verb"));
}

#[test]
fn a_path_without_a_verb_refuses_loudly() {
    let out = serve("f/controller", json!({}));
    assert_eq!(out["result"], "FAILURE");
    assert_eq!(out["error"], json!("no verb in the job path: f/controller"));
}

#[test]
fn a_request_data_wrapper_reaches_the_verb_as_the_inner_object() {
    // Python's script signature bound the job arguments' keys to
    // main()'s parameters, so a caller's {"request_data": {...}}
    // reached the verb as the inner object. The door reproduces that
    // descent at the dispatch seam - the refusal below names the
    // INNER type, proving the verb saw the inner object, not the
    // wrapper.
    let out = serve(
        "f/controller/steerwish_create",
        json!({"request_data": {"type": "bogus"}}),
    );
    assert_eq!(out["error"], json!("unknown wish type: bogus (steer or seek)"));
}

#[test]
fn flat_arguments_serve_unchanged() {
    // The plan-ask verb's own shape: the job binds wish_id directly,
    // no wrapper key - the arguments pass through as they arrive.
    let out = serve("f/controller/steerwish_create", json!({"type": "bogus"}));
    assert_eq!(out["error"], json!("unknown wish type: bogus (steer or seek)"));
}

#[test]
fn the_plan_ask_verb_refuses_without_a_wish_id() {
    let out = serve("f/controller/steerwish_plan", json!({}));
    assert_eq!(out["result"], "FAILURE");
    assert_eq!(out["error"], json!("Missing required field: wish_id"));
    let out = serve("f/controller/steerwish_plan", json!({"wish_id": ""}));
    assert_eq!(out["result"], "FAILURE");
    assert_eq!(out["error"], json!("Missing required field: wish_id"));
}
