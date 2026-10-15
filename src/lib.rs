//! The controller's rust body: lifecycle verbs and the data layer,
//! ported from the python original (rung 10). The windmill script paths
//! and signatures (f/controller/*), the SQL statements, and the table
//! schemas are the cross-lane contract - they port byte-identical.

pub mod config;
pub mod database;
pub mod pyval;
pub mod verbs;
