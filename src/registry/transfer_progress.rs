use std::io::IsTerminal;
use std::time::Duration;

use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};

/// Progress bar for a network transfer (upload or download), shared by all
/// registry drivers. Shows a live bytes/sec estimate; falls back to a
/// spinner when the total size isn't known ahead of time.
pub(super) fn transfer_bar(total: u64, message: &str) -> ProgressBar {
    let pb = if total > 0 {
        ProgressBar::new(total)
    } else {
        ProgressBar::new_spinner()
    };
    if !std::io::stdout().is_terminal() {
        pb.set_draw_target(ProgressDrawTarget::hidden());
    }
    let style = if total > 0 {
        ProgressStyle::with_template(
            "{msg} [{bar:40.cyan/blue}] {bytes}/{total_bytes} {bytes_per_sec} ({eta})",
        )
        .unwrap_or_else(|_| ProgressStyle::default_bar())
    } else {
        ProgressStyle::with_template("{spinner} {msg} {bytes} {bytes_per_sec}")
            .unwrap_or_else(|_| ProgressStyle::default_spinner())
            .tick_chars("|/-\\ ")
    };
    pb.set_style(style);
    pb.set_message(message.to_string());
    pb.enable_steady_tick(Duration::from_millis(100));
    pb
}
