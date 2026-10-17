//! The python unit suite, ported module by module - the parity
//! bar (the python died with the deletion motion; this suite is the
//! standing parity record). One file per ported suite; the fakes
//! python got from unittest.mock.patch live in support, injected
//! through the DbGateway / WindmillGateway / CausalClient seams
//! instead of module patching.

mod support;

mod instruments_read;
mod systemtenders;
mod wishes;
