use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

pub const MCAP_CONVERSION_URL: &str = "https://mcap.dev/guides/getting-started";
pub const MCAP_CLI_URL: &str = "https://mcap.dev/guides/cli";
pub const MCAP_ROS2_URL: &str = "https://mcap.dev/guides/getting-started/ros-2";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BagSource {
    pub root: PathBuf,
    pub mcap: Option<PathBuf>,
    pub has_db3: bool,
    pub attachments: Vec<PathBuf>,
    pub original_bytes: u64,
}

pub fn discover_bag(path: &Path) -> Result<BagSource> {
    if !path.exists() {
        return Err(anyhow!("source path does not exist: {}", path.display()));
    }

    if path.is_file() {
        if path.extension().and_then(|e| e.to_str()) == Some("mcap") {
            let meta = fs::metadata(path)?;
            return Ok(BagSource {
                root: path.to_path_buf(),
                mcap: Some(path.to_path_buf()),
                has_db3: false,
                attachments: Vec::new(),
                original_bytes: meta.len(),
            });
        }

        if path.extension().and_then(|e| e.to_str()) == Some("db3") {
            let meta = fs::metadata(path)?;
            return Ok(BagSource {
                root: path.to_path_buf(),
                mcap: None,
                has_db3: true,
                attachments: Vec::new(),
                original_bytes: meta.len(),
            });
        }

        return Err(anyhow!(
            "expected a bag directory, .mcap file, or .db3 file, got file {}",
            path.display()
        ));
    }

    let mut mcap = None;
    let mut attachments = Vec::new();
    let mut total: u64 = 0;
    let mut saw_ros1_bag = false;
    let mut saw_ros2_db3 = false;

    for entry in WalkDir::new(path) {
        let entry = entry?;
        let p = entry.path();
        if p.is_dir() {
            continue;
        }

        let meta = fs::metadata(p)?;
        total += meta.len();

        let ext = p.extension().and_then(|v| v.to_str()).unwrap_or_default();
        match ext {
            "mcap" => {
                if mcap.is_none() {
                    mcap = Some(p.to_path_buf());
                }
            }
            "bag" => {
                saw_ros1_bag = true;
                attachments.push(p.to_path_buf());
            }
            "db3" => {
                saw_ros2_db3 = true;
                attachments.push(p.to_path_buf());
            }
            _ => attachments.push(p.to_path_buf()),
        }
    }

    if mcap.is_none() && saw_ros1_bag {
        return Err(anyhow!(
            "directory has ROS 1 bag files (*.bag) but no .mcap. Convert first with `mcap convert input.bag output.mcap`. See {}",
            MCAP_CLI_URL
        ));
    }

    if mcap.is_none() && !saw_ros2_db3 {
        return Err(anyhow!(
            "directory has no .mcap or .db3 file. Convert your bag to MCAP first: {}",
            MCAP_CONVERSION_URL
        ));
    }

    Ok(BagSource {
        root: path.to_path_buf(),
        mcap,
        has_db3: saw_ros2_db3,
        attachments,
        original_bytes: total,
    })
}

pub fn has_direct_mcap(path: &Path) -> Result<bool> {
    if !path.exists() || !path.is_dir() {
        return Ok(false);
    }
    for entry in fs::read_dir(path).with_context(|| format!("reading {}", path.display()))? {
        let entry = entry?;
        let p = entry.path();
        if p.is_file() && p.extension().and_then(|e| e.to_str()) == Some("mcap") {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn has_direct_db3(path: &Path) -> Result<bool> {
    if !path.exists() || !path.is_dir() {
        return Ok(false);
    }
    for entry in fs::read_dir(path).with_context(|| format!("reading {}", path.display()))? {
        let entry = entry?;
        let p = entry.path();
        if p.is_file() && p.extension().and_then(|e| e.to_str()) == Some("db3") {
            return Ok(true);
        }
    }
    Ok(false)
}
