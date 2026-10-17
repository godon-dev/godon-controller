//! Verb dispatch - one verb per windmill script path (f/controller/*),
//! envelopes exactly as the python verbs answered them. Each verb's body
//! lives in its own module, mirroring python's one-file-per-script layout
//! (controller/<stem>.py -> <stem>.rs, controller/executors/ -> executors/).
//! This file is only the wiring: the verb name arrives as argv[1], the
//! job's arguments as a JSON document on stdin, the envelope on stdout.

use serde_json::Value;

fn request_data(args: &Value) -> &Value {
    // Python's script signature bound the job arguments' keys to
    // main()'s parameters, so a caller's {"request_data": {...}}
    // reached the verb as the inner object. The platform door receives
    // the raw arguments (fetched verbatim from the job row), so the
    // descent happens here - one thin layer at the dispatch seam, no
    // verb logic touched. Arguments without a request_data key (the
    // flat kwarg shape) pass through unchanged.
    args.get("request_data").unwrap_or(args)
}

pub fn dispatch(verb: &str, args: &Value) -> Value {
    let rd = request_data(args);
    match verb {
        "systemtender_create" => crate::systemtender_create::run(rd),
        "systemtender_create_executor" => crate::executors::systemtender_create_executor::run(rd),
        "systemtender_get" => crate::systemtender_get::run(rd),
        "systemtender_update" => crate::systemtender_update::run(rd),
        "systemtender_delete" => crate::executors::systemtender_delete::run(rd),
        "systemtender_start" => crate::systemtender_start::run(rd),
        "systemtender_stop" => crate::systemtender_stop::run(rd),
        "systemtenders_get" => crate::systemtenders_get::run(rd),
        "steerwish_create" => crate::wish_create::run(rd),
        "steerwish_get" => crate::wish_get::run(rd),
        "steerwish_close" => crate::wish_close::run(rd),
        "steerwish_update" => crate::wish_update::run(rd),
        "steerwish_delete" => crate::wish_delete::run(rd),
        "steerwish_plan" => crate::wish_plan::run(rd),
        "steerwishes_get" => crate::wishes_get::run(rd),
        "credential_create" => crate::credential_create::run(rd),
        "credential_get" => crate::credential_get::run(rd),
        "credential_delete" => crate::credential_delete::run(rd),
        "credentials_get" => crate::credentials_get::run(rd),
        "target_create" => crate::target_create::run(rd),
        "target_get" => crate::target_get::run(rd),
        "target_delete" => crate::target_delete::run(rd),
        "targets_get" => crate::targets_get::run(rd),
        _ => {
            let mut envelope = serde_json::Map::new();
            envelope.insert("result".to_string(), Value::String("FAILURE".to_string()));
            envelope.insert(
                "error".to_string(),
                Value::String(format!("unknown verb: {}", verb)),
            );
            Value::Object(envelope)
        }
    }
}

pub(crate) fn truthy_rd(rd: &Value) -> bool {
    rd.as_object().map(|o| !o.is_empty()).unwrap_or(false)
}
