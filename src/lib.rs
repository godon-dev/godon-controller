//! The controller's rust body: lifecycle verbs and the data layer,
//! ported from the python original. The windmill script paths
//! and signatures (f/controller/*), the SQL statements, and the table
//! schemas are the cross-lane contract - they port byte-identical.
//! Module layout mirrors python's controller/ file for file.

pub mod config;
pub mod credential_create;
pub mod credential_delete;
pub mod credential_get;
pub mod credentials_get;
pub mod database;
pub mod executors;
pub mod otel_logging;
pub mod platform;
pub mod reconcile;
pub mod seek_grammar;
pub mod steer_grammar;
pub mod systemtender_create;
pub mod systemtender_get;
pub mod systemtender_service;
pub mod systemtender_start;
pub mod systemtender_stop;
pub mod systemtender_update;
pub mod systemtenders_get;
pub mod target_create;
pub mod target_delete;
pub mod target_get;
pub mod targets_get;
pub mod timefmt;
pub mod verbs;
pub mod wish_close;
pub mod wish_create;
pub mod wish_delete;
pub mod wish_get;
pub mod wish_plan;
pub mod wish_service;
pub mod wish_update;
pub mod wishes_get;
pub mod wmill;
