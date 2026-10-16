//! The python unit suite, ported module by module - rung 10's parity
//! bar. One file here per python file under tests/unit/; the fakes
//! python got from unittest.mock.patch live in support, injected
//! through the DbGateway / WindmillGateway / CausalClient seams
//! instead of module patching.

mod support;

mod instruments_read;
mod systemtenders;
mod wishes;
