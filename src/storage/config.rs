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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TimeDisplay {
    /// Show time as "3 days ago", "2 hours ago", etc.
    #[default]
    Relative,
    /// Show time as a date: "2024-01-15".
    Absolute,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RegistryFile {
    pub registry: Vec<RegistryConfig>,
    #[serde(default)]
    pub compression: CompressionConfig,
    #[serde(default)]
    pub settings: Settings,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Settings {
    #[serde(default)]
    pub time_display: TimeDisplay,
    /// Registry used when no --registry flag is given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_registry: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ConfigPointcloudMode {
    Off,
    #[default]
    Lossy,
    Lossless,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ConfigMcapCompression {
    None,
    #[default]
    Zstd,
    Lz4,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ConfigArchiveCompression {
    #[default]
    Gzip,
    None,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(default)]
pub struct CompressionConfig {
    pub pointcloud_mode: ConfigPointcloudMode,
    pub pointcloud_accuracy_mm: f64,
    pub packed_mcap_compression: ConfigMcapCompression,
    pub packed_archive_compression: ConfigArchiveCompression,
    pub unpacked_mcap_compression: ConfigMcapCompression,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            pointcloud_mode: ConfigPointcloudMode::Lossy,
            pointcloud_accuracy_mm: 1.0,
            packed_mcap_compression: ConfigMcapCompression::Zstd,
            packed_archive_compression: ConfigArchiveCompression::None,
            unpacked_mcap_compression: ConfigMcapCompression::Lz4,
        }
    }
}

pub fn config_dir() -> Result<PathBuf> {
    let dir = if let Some(override_dir) = std::env::var_os("MARINA_CONFIG_DIR") {
        PathBuf::from(override_dir)
    } else if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME") {
        PathBuf::from(xdg).join("marina")
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

pub const DEFAULT_REGISTRY_NAME: &str = "osnabotics-public";
pub const DEFAULT_GDRIVE_FOLDER_ID: &str = "10hjoMIyWTOVNOo3zDOfHoSb1S55gO3rJ";

#[cfg(feature = "osnabotics-default-registry")]
fn default_registry() -> RegistryConfig {
    RegistryConfig {
        name: DEFAULT_REGISTRY_NAME.to_string(),
        kind: "gdrive".to_string(),
        uri: format!("gdrive://{}", DEFAULT_GDRIVE_FOLDER_ID),
        auth_env: None,
    }
}

pub fn load_registries() -> Result<RegistryFile> {
    let path = registry_file_path()?;
    if !path.exists() {
        let mut base = RegistryFile::default();
        #[cfg(feature = "osnabotics-default-registry")]
        {
            base.registry.push(default_registry());
        }

        save_registries(&base)?;
        return Ok(base);
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

pub fn load_compression_config() -> Result<CompressionConfig> {
    Ok(load_registries()?.compression)
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
