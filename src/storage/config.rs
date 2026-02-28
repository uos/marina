use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryConfig {
    pub name: String,
    pub kind: String,
    pub uri: String,
    pub auth_env: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RegistryFile {
    pub registry: Vec<RegistryConfig>,
}

pub fn config_dir() -> Result<PathBuf> {
    let dir = if let Some(override_dir) = std::env::var_os("MARINA_CONFIG_DIR") {
        PathBuf::from(override_dir)
    } else if let Some(home) = std::env::var_os("HOME") {
        PathBuf::from(home).join(".config").join("marina")
    } else {
        dirs::config_dir()
            .context("unable to locate config dir")?
            .join("marina")
    };
    fs::create_dir_all(&dir)?;
    Ok(dir)
}

pub fn cache_dir() -> Result<PathBuf> {
    let dir = if let Some(override_dir) = std::env::var_os("MARINA_CACHE_DIR") {
        PathBuf::from(override_dir)
    } else if let Some(home) = std::env::var_os("HOME") {
        PathBuf::from(home).join(".cache").join("marina")
    } else {
        dirs::cache_dir()
            .context("unable to locate cache dir")?
            .join("marina")
    };
    fs::create_dir_all(&dir)?;
    Ok(dir)
}

pub fn registry_file_path() -> Result<PathBuf> {
    Ok(config_dir()?.join("registries.toml"))
}

pub fn load_registries() -> Result<RegistryFile> {
    let path = registry_file_path()?;
    if !path.exists() {
        return Ok(RegistryFile::default());
    }

    let content =
        fs::read_to_string(&path).with_context(|| format!("failed reading {}", path.display()))?;
    let parsed: RegistryFile =
        toml::from_str(&content).with_context(|| format!("failed parsing {}", path.display()))?;
    Ok(parsed)
}

pub fn save_registries(file: &RegistryFile) -> Result<()> {
    let path = registry_file_path()?;
    let text = toml::to_string_pretty(file)?;
    fs::write(&path, text).with_context(|| format!("failed writing {}", path.display()))?;
    Ok(())
}

pub fn remove_local_state(all: bool) -> Result<()> {
    let cdir = cache_dir()?;
    if cdir.exists() {
        fs::remove_dir_all(&cdir)?;
    }

    if all {
        let cfg = config_dir()?;
        let registry_path = cfg.join("registries.toml");
        if registry_path.exists() {
            fs::remove_file(&registry_path)?;
        }
        let catalog = cfg.join("catalog.json");
        if catalog.exists() {
            fs::remove_file(&catalog)?;
        }
    }

    Ok(())
}

pub fn ensure_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    Ok(())
}
