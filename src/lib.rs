//! Marina: a dataset manager for robotics.
//!
//! The dataset kernel (config, cache, registry drivers, pull, and streaming)
//! lives in [`mt_dataset`] so Minot and Marina can share it without either
//! application depending on the other. This crate re-exports that kernel as
//! Marina's stable public API and adds the application layer on top: the
//! command-line interface, the C ABI, and the Minot registry server.

pub use mt_dataset::*;

pub mod cli;

/// This release's `default.rl`, attached to the release by CI. The
/// configuration marina writes on first use points at it with `# @schema`.
pub const SCHEMA_URL: &str = concat!(
    env!("CARGO_PKG_REPOSITORY"),
    "/releases/download/v",
    env!("CARGO_PKG_VERSION"),
    "/default.rl"
);

/// Make the configuration written on first use name [`SCHEMA_URL`]. Every
/// entry point calls this before marina reads its configuration.
pub fn use_release_schema() {
    mt_dataset::storage::config::set_schema_url(SCHEMA_URL);
}
mod completions;
pub mod ffi;
pub mod format;
pub mod tui;

#[cfg(feature = "minot-registry")]
pub mod server;

#[cfg(test)]
mod tests {
    #[test]
    fn the_schema_is_this_releases_default_rl() {
        assert_eq!(
            super::SCHEMA_URL,
            format!(
                "https://codeberg.org/stelzo/marina/releases/download/v{}/default.rl",
                env!("CARGO_PKG_VERSION")
            )
        );
    }
}
