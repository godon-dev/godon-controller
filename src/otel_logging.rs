//! Python's shared/otel_logging.py, ported: the log path only.
//!
//! Python wired a tracer provider too, but no verb ever started a
//! span - the dead half is not ported. Logs export OTLP/HTTP to the
//! observability collector; the `log` macros the verbs already speak
//! (python's exact instrumented points) ride the bridge. Endpoint and
//! service name come from the same two env vars python read, with the
//! same defaults. Like python's BatchLogProcessor, export failures
//! drop in the background - the door never waits on telemetry.

use opentelemetry::logs::SdkLoggerProvider;
use opentelemetry::KeyValue;
use opentelemetry::Resource;
use opentelemetry_appender_log::OpentelemetryLogBridge;
use opentelemetry_otlp::LogExporter;

pub fn init_telemetry() {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").unwrap_or_else(|_| {
        "http://godon-observability-opentelemetry-collector.godon-observability.svc.cluster.local:4318"
            .to_string()
    });
    let service_name =
        std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "godon-controller".to_string());

    let exporter = LogExporter::builder()
        .with_http_endpoint(format!("{}/v1/logs", endpoint))
        .build()
        .expect("build the OTLP log exporter");

    let resource = Resource::builder()
        .with_attributes([KeyValue::new("service.name", service_name)])
        .build();

    let provider = SdkLoggerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    // python's get_logger attached the OTLP handler and set DEBUG;
    // the bridge makes every log:: macro in the crate flow to it
    let bridge = OpentelemetryLogBridge::new(provider);
    log::set_boxed_logger(Box::new(bridge)).expect("install the otel log bridge");
    log::set_max_level(log::LevelFilter::Debug);
}
