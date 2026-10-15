//! Verb dispatch - one verb per windmill script path (f/controller/*).
//! Verbs port one by one behind unchanged paths and signatures; the
//! lane order is: data layer first, verbs next, the rung-8 verbs
//! (named resume, relaunch) last, after their flight proves them live.
//!
//! The envelope convention ports unchanged: verbs answer
//! {"result": "SUCCESS" | "FAILURE", ...} - failures are values, not
//! exceptions, exactly as the python's without-raising contract.

use serde_json::{Map, Value};

pub fn dispatch(verb: &str, args: &Value) -> Value {
    // The verb table fills in as verbs land; an unknown verb is a loud
    // failure, never a silent one.
    let mut envelope = Map::new();
    envelope.insert("result".to_string(), Value::String("FAILURE".to_string()));
    envelope.insert(
        "error".to_string(),
        Value::String(format!("unknown verb: {}", verb)),
    );
    let _ = args;
    Value::Object(envelope)
}
