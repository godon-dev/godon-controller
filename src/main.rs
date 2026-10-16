// The controller binary - one verb per windmill script path (f/controller/*).
//
// The verb name arrives as the first argument; the job's arguments arrive
// as a JSON document on stdin. The binding is deliberately ONE thin
// function so the wiring-checks receipts (how the sandbox rail hands
// arguments to the image entrypoint) can adjust it without touching any
// verb logic. Verbs land one by one; until then the dispatch answers
// "unknown verb" loudly.

use std::io::Read;

fn main() {
    godon_controller::otel_logging::init_telemetry();

    let verb = std::env::args().nth(1).unwrap_or_default();
    let mut input = String::new();
    std::io::stdin()
        .read_to_string(&mut input)
        .expect("read arguments from stdin");

    let args: serde_json::Value = if input.trim().is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_str(&input).expect("arguments must be JSON")
    };

    let result = godon_controller::verbs::dispatch(&verb, &args);
    println!("{}", serde_json::to_string(&result).expect("serialize result"));
}
