//! The controller's rust body: lifecycle verbs and the data layer,
//! ported from the python original (rung 10). The windmill script paths
//! and signatures (f/controller/*), the SQL statements, and the table
//! schemas are the cross-lane contract - they port byte-identical.

pub mod config;
pub mod database;
pub mod seek_grammar;
pub mod wish_service;
pub mod systemtender_service;
pub mod timefmt;
pub mod verbs;
pub mod wmill;
