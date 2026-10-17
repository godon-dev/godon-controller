//! Python's shared/otel_logging.py, ported: the log path only.
//!
//! Python wired a tracer provider too, but no verb ever started a
//! span - the dead half is not ported. Logs export OTLP/HTTP to the
//! observability collector; the `log` macros the verbs already speak
//! (python's exact instrumented points) ride the bridge. Endpoint and
//! service name come from the same two env vars python read, with the
//! same defaults; the env var carries the collector BASE url and the
//! signal path (/v1/logs) is appended here, exactly as python's
//! exporter and the causal organ do.
//! Like python's BatchLogProcessor, export failures drop in the
//! background - the door never waits on telemetry. The exit path
//! flushes the provider: the batch timer thread dies with the
//! process, and python's SDK covered this with its atexit hook.

use std::sync::OnceLock;

use opentelemetry::logs::LoggerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_appender_log::OpenTelemetryLogBridge;
use opentelemetry_otlp::LogExporter;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::Resource;

/// The provider stays reachable for the whole process so the exit path
/// can flush it (the bridge only holds an internal handle).
static LOGGER_PROVIDER: OnceLock<SdkLoggerProvider> = OnceLock::new();

pub fn init_telemetry() {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").unwrap_or_else(|_| {
        "http://godon-observability-opentelemetry-collector.godon-observability.svc.cluster.local:4318"
            .to_string()
    });
    let service_name =
        std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "godon-controller".to_string());

    let exporter = LogExporter::builder()
        .with_http()
        .with_endpoint(format!("{endpoint}/v1/logs"))
        .build()
        .expect("build the OTLP log exporter");

    let resource = Resource::builder()
        .with_attributes([KeyValue::new("service.name", service_name)])
        .build();

    let provider = SdkLoggerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();
    let provider = LOGGER_PROVIDER.get_or_init(|| provider);

    // python's get_logger attached the OTLP handler and set DEBUG;
    // the bridge makes every log:: macro in the crate flow to it
    let bridge = OpenTelemetryLogBridge::new(provider);
    log::set_boxed_logger(Box::new(bridge)).expect("install the otel log bridge");
    log::set_max_level(log::LevelFilter::Debug);
}

/// Flush pending records to the collector before the process exits.
/// The batch exporter ships on a timer thread that dies with the
/// process - python's SDK flushed at interpreter exit (atexit), and
/// this is that half: a one-shot verb run hands its logs over on the
/// way out instead of racing the timer. Never blocks on retries.
pub fn shutdown_telemetry() {
    if let Some(provider) = LOGGER_PROVIDER.get() {
        let _ = provider.shutdown();
    }
}
