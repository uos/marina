use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

pub const MCAP_CONVERSION_URL: &str = "https://mcap.dev/guides/getting-started";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BagSource {
    pub root: PathBuf,
    pub metadata_yaml: PathBuf,
    pub mcap: PathBuf,
    pub attachments: Vec<PathBuf>,
    pub original_bytes: u64,
}

pub fn discover_bag(path: &Path) -> Result<BagSource> {
    if !path.exists() {
        return Err(anyhow!("source path does not exist: {}", path.display()));
    }

    if path.is_file() {
        return Err(anyhow!(
            "expected a bag directory with metadata yaml + .mcap, got file {}",
            path.display()
        ));
    }

    let mut mcap = None;
    let mut yaml = None;
    let mut attachments = Vec::new();
    let mut total: u64 = 0;

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
            "yaml" | "yml" => {
                if yaml.is_none() {
                    yaml = Some(p.to_path_buf());
                }
            }
            _ => attachments.push(p.to_path_buf()),
        }
    }

    let mcap = mcap.ok_or_else(|| {
        anyhow!(
            "directory has no .mcap file. Convert your bag to MCAP first: {}",
            MCAP_CONVERSION_URL
        )
    })?;
    let metadata_yaml = yaml.ok_or_else(|| {
        anyhow!("directory has no metadata yaml/yml (required for ROS bag bundle)")
    })?;

    Ok(BagSource {
        root: path.to_path_buf(),
        metadata_yaml,
        mcap,
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
