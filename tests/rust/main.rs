//! The python test-suite port (rung 10, the parity bar).
//!
//! One rust module per python test file under tests/unit/ - the file
//! map and every assertion trace back to the python originals, which
//! stay the reference until the estate cutover. Where a ported test
//! exposed a gap in the first rust body, the fix landed beside the
//! port and is named in the PR.

mod harness;

mod instruments_read;
mod seek_return_mirror;
mod watermark_slots;
mod wish_assignment_upsert;
