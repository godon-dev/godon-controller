//! The python test-suite port (rung 10, the parity bar).
//!
//! One rust module per ported python test file - the file map and
//! every assertion trace back to the python originals, which died
//! with the deletion motion; this suite is the standing record. Where a ported test
//! exposed a gap in the first rust body, the fix landed beside the
//! port and is named in the PR.

mod harness;

mod instruments_read;
mod seek_return_mirror;
mod watermark_slots;
mod wish_assignment_upsert;
