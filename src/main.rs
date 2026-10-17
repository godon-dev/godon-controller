// The controller binary - one verb per windmill script path (f/controller/*).
//
// The verb name arrives as the first argument; the job's arguments arrive
// as a JSON document - from the second argument when a wrapper hands it
// explicitly (the bash rail binds job arguments as $1..), otherwise read
// from stdin (the manual path). The envelope goes to stdout (the log) and
// to ./result.json (windmill's result file for plain scripts - how the
// python verbs' return reached the api). The binding is deliberately ONE
// thin function so the rail can adjust it without touching any verb
// logic. Unknown verbs answer loudly.

use std::io::Read;

fn main() {
    godon_controller::otel_logging::init_telemetry();

    let mut argv = std::env::args().skip(1);
    let verb = argv.next().unwrap_or_default();
    let input = match argv.next() {
        Some(payload) => payload,
        None => {
            let mut buf = String::new();
            std::io::stdin()
                .read_to_string(&mut buf)
                .expect("read arguments from stdin");
            buf
        }
    };

    let args: serde_json::Value = if input.trim().is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_str(&input).expect("arguments must be JSON")
    };

    let result = godon_controller::verbs::dispatch(&verb, &args);
    let envelope = serde_json::to_string(&result).expect("serialize result");
    println!("{}", envelope);
    let _ = std::fs::write("result.json", &envelope);
}
