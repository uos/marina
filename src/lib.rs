//! Marina — a dataset manager for robotics.
//!
//! The dataset kernel (config, cache, registry drivers, pull, and streaming)
//! lives in [`mt_dataset`] so Minot and Marina can share it without either
//! application depending on the other. This crate re-exports that kernel as
//! Marina's stable public API and adds the application layer on top: the
//! command-line interface, the C ABI, and the Minot registry server.

pub use mt_dataset::*;

pub mod cli;
pub mod ffi;

#[cfg(feature = "minot-registry")]
pub mod server;
