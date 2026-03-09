use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::model::bag_ref::BagRef;
use crate::storage::config;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry {
    pub bag: BagRef,
    pub local_dir: PathBuf,
    pub packed_bytes: u64,
    // original_bytes is intentionally absent — always derived from the local
    // directory via discover_bag so it stays accurate after a recording.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundle_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Catalog {
    pub entries: HashMap<String, CacheEntry>,
}

fn catalog_path() -> Result<PathBuf> {
    Ok(config::config_dir()?.join("catalog.json"))
}

pub fn load_catalog() -> Result<Catalog> {
    let path = catalog_path()?;
    if !path.exists() {
        return Ok(Catalog::default());
    }

    let content =
        fs::read_to_string(&path).with_context(|| format!("failed reading {}", path.display()))?;
    let parsed = serde_json::from_str(&content)
        .with_context(|| format!("failed parsing {}", path.display()))?;
    Ok(parsed)
}

pub fn save_catalog(catalog: &Catalog) -> Result<()> {
    let path = catalog_path()?;
    let text = serde_json::to_string_pretty(catalog)?;
    fs::write(&path, text).with_context(|| format!("failed writing {}", path.display()))?;
    Ok(())
}

pub fn bag_cache_dir(bag: &BagRef) -> Result<PathBuf> {
    let root = config::cache_dir()?.join("bags").join(bag.cache_key());
    fs::create_dir_all(&root)?;
    Ok(root)
}
