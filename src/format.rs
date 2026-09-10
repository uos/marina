//! Shared display formatting for the CLI and the TUI.
//!
//! Both front-ends render the same registry metadata, so the byte, timestamp,
//! and `BagInfo` formatting lives here instead of being duplicated per view.

use mt_dataset::registry::driver::BagInfo;
use mt_dataset::storage::config::TimeDisplay;

/// Scales `bytes` into `units`, promoting once more when the value would round
/// up to a full 1000 at the requested precision — 999_999 B is `1.00 MB`, never
/// `1000.00 kB`.
fn scale(bytes: u64, units: &[&'static str], decimals: usize) -> (f64, &'static str) {
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1000.0 && unit < units.len() - 1 {
        value /= 1000.0;
        unit += 1;
    }
    let rounding_edge = 1000.0 - 0.5 * 0.1_f64.powi(decimals as i32);
    if value >= rounding_edge && unit < units.len() - 1 {
        value /= 1000.0;
        unit += 1;
    }
    (value, units[unit])
}

/// Decimal byte units, the way drive capacities and dataset sizes are quoted:
/// `1.24 GB`, not `1.16 GiB`.
pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "kB", "MB", "GB", "TB"];
    let (value, unit) = scale(bytes, &UNITS, 2);
    if unit == "B" {
        format!("{bytes} B")
    } else {
        format!("{value:.2} {unit}")
    }
}

/// Column-friendly size: one significant decimal below ten, whole numbers
/// above, and no space — `898MB`, `9.8GB`, `62GB`. Five characters at most,
/// where [`human_bytes`] needs ten.
pub fn human_bytes_compact(bytes: u64) -> String {
    const UNITS: [&str; 7] = ["B", "kB", "MB", "GB", "TB", "PB", "EB"];
    // Values of ten and up print without decimals, so the promotion edge is a
    // whole unit.
    let (value, unit) = scale(bytes, &UNITS, 0);
    if unit == "B" {
        format!("{bytes}B")
    } else if value < 10.0 {
        format!("{value:.1}{unit}")
    } else {
        format!("{value:.0}{unit}")
    }
}

pub fn format_pushed_at(pushed_at: Option<u64>, display: TimeDisplay) -> String {
    let Some(ts) = pushed_at else {
        return "-".into();
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    if display == TimeDisplay::Absolute {
        // Format as YYYY-MM-DD using only the timestamp
        let secs_per_day = 86400u64;
        let days_since_epoch = ts / secs_per_day;
        // Compute Gregorian date from days since 1970-01-01
        let mut y = 1970u32;
        let mut d = days_since_epoch as u32;
        loop {
            let days_in_year = if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
                366
            } else {
                365
            };
            if d < days_in_year {
                break;
            }
            d -= days_in_year;
            y += 1;
        }
        let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
        let month_days = [
            31u32,
            if leap { 29 } else { 28 },
            31,
            30,
            31,
            30,
            31,
            31,
            30,
            31,
            30,
            31,
        ];
        let mut m = 1u32;
        for md in &month_days {
            if d < *md {
                break;
            }
            d -= md;
            m += 1;
        }
        return format!("{:04}-{:02}-{:02}", y, m, d + 1);
    }

    let elapsed = now.saturating_sub(ts);
    match elapsed {
        0..=59 => format!("{}s ago", elapsed),
        60..=3599 => format!("{}m ago", elapsed / 60),
        3600..=86399 => format!("{}h ago", elapsed / 3600),
        86400..=604799 => format!("{}d ago", elapsed / 86400),
        604800..=2591999 => format!("{}w ago", elapsed / 604800),
        2592000..=31535999 => format!("{}mo ago", elapsed / 2592000),
        _ => format!("{}y ago", elapsed / 31536000),
    }
}

/// Columns shared by every table view: hash, size, pointcloud mode, archive
/// compression, and push time. Only the original size is reported — the packed
/// size is a detail, and `marina inspect` prints it.
pub fn format_bag_info(
    info: Option<&BagInfo>,
    time_display: TimeDisplay,
) -> (String, String, String, String, String) {
    match info {
        None => ("-".into(), "-".into(), "-".into(), "-".into(), "-".into()),
        Some(i) => (
            i.bundle_hash.clone().unwrap_or_else(|| "-".into()),
            human_bytes_compact(i.original_bytes),
            i.pointcloud.clone().unwrap_or_else(|| "-".into()),
            i.mcap_compression.clone().unwrap_or_else(|| "-".into()),
            format_pushed_at(i.pushed_at, time_display),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_sizes_stay_within_five_columns() {
        for bytes in [
            0,
            1,
            999,
            1_024,
            999_949,
            999_999,
            999_999_999,
            1_073_741_824,
            u64::MAX,
        ] {
            let rendered = human_bytes_compact(bytes);
            assert!(
                rendered.len() <= 5,
                "{bytes} rendered as {rendered}, wider than the column"
            );
        }
    }

    #[test]
    fn compact_sizes_keep_a_decimal_only_below_ten() {
        assert_eq!(human_bytes_compact(0), "0B");
        assert_eq!(human_bytes_compact(512), "512B");
        assert_eq!(human_bytes_compact(1_000), "1.0kB");
        assert_eq!(human_bytes_compact(897_581_056), "898MB");
        assert_eq!(human_bytes_compact(9_800_000_000), "9.8GB");
        assert_eq!(human_bytes_compact(66_855_596_032), "67GB");
    }

    #[test]
    fn sizes_promote_instead_of_rounding_to_a_full_thousand() {
        assert_eq!(human_bytes_compact(999_999), "1.0MB");
        assert_eq!(human_bytes(999_999_999), "1.00 GB");
    }

    #[test]
    fn sizes_are_decimal_not_binary() {
        assert_eq!(human_bytes(1_000), "1.00 kB");
        assert_eq!(human_bytes(1_500_000_000), "1.50 GB");
        assert_eq!(human_bytes_compact(1_000_000_000), "1.0GB");
    }
}
