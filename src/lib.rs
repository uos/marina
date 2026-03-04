//! marina is a dataset manager for robotics to organize, share, and discover datasets and metadata across storage backends.
//!
//! It supports:
//! - resolving bag references to local cache paths,
//! - pushing/pulling packed bag bundles,
//! - optional phase-based progress reporting via [`ProgressSink`].

pub mod cleanup;
pub mod cli;
pub mod core;
pub mod ffi;
pub mod io;
pub mod model;
pub mod progress;
pub mod registry;
pub mod storage;

pub use core::{
    CachedBagInfo, CachedSizeStats, Marina, PullOptions, PushOptions, RemoteBagHit,
    RemovedRegistry, ResolveResult,
};
/// Parsed bag reference like `namespace/name:tag1:tag2[attachment.txt]`.
pub use model::bag_ref::BagRef;
/// Progress primitives to receive phase-based feedback from push/pull operations.
pub use progress::{ProgressEvent, ProgressReporter, ProgressSink, WriterProgress};
