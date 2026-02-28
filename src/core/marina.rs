use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};

use crate::io::{bag, pack};
use crate::model::bag_ref::BagRef;
use crate::registry::aws::AwsRegistry;
use crate::registry::driver::RegistryDriver;
use crate::registry::folder::FolderRegistry;
use crate::registry::gdrive::GDriveRegistry;
use crate::registry::ssh::SshRegistry;
use crate::registry::stub::StubRegistry;
use crate::storage::cache::{self, CacheEntry, Catalog};
use crate::storage::config::{self, RegistryConfig};

#[derive(Debug, Clone)]
pub enum ResolveResult {
    LocalPath(PathBuf),
    Cached(PathBuf),
    RemoteAvailable {
        registry: String,
        bag: BagRef,
        needs_pull: bool,
    },
}

#[derive(Debug, Clone)]
pub struct RemovedRegistry {
    pub name: String,
    pub kind: String,
    pub uri: String,
    pub data_deleted: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct CachedSizeStats {
    pub original_bytes: u64,
    pub packed_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct CachedBagInfo {
    pub bag: BagRef,
    pub local_dir: PathBuf,
    pub original_bytes: u64,
    pub packed_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct RemoteBagHit {
    pub registry: String,
    pub bag: BagRef,
}

pub struct Marina {
    registries: HashMap<String, (RegistryConfig, Box<dyn RegistryDriver>)>,
    catalog: Catalog,
}

impl Marina {
    pub fn load() -> Result<Self> {
        let registry_file = config::load_registries()?;
        let mut registries = HashMap::new();

        for reg in registry_file.registry {
            let driver: Box<dyn RegistryDriver> = match reg.kind.as_str() {
                "folder" | "directory" => Box::new(FolderRegistry::from_uri(&reg.name, &reg.uri)?),
                "ssh" => Box::new(SshRegistry::from_uri(
                    &reg.name,
                    &reg.uri,
                    reg.auth_env.clone(),
                )?),
                "gdrive" => Box::new(GDriveRegistry::from_uri(
                    &reg.name,
                    &reg.uri,
                    reg.auth_env.clone(),
                )?),
                "aws" => Box::new(AwsRegistry::from_uri(
                    &reg.name,
                    &reg.uri,
                    reg.auth_env.clone(),
                )?),
                other => Box::new(StubRegistry::new(other, &reg.uri, reg.auth_env.clone())),
            };
            registries.insert(reg.name.clone(), (reg, driver));
        }

        let catalog = cache::load_catalog()?;
        Ok(Self {
            registries,
            catalog,
        })
    }

    pub fn add_registry(&mut self, registry: RegistryConfig) -> Result<()> {
        let mut existing = config::load_registries()?;
        if existing.registry.iter().any(|r| r.name == registry.name) {
            return Err(anyhow!("registry '{}' already exists", registry.name));
        }
        existing.registry.push(registry.clone());
        config::save_registries(&existing)?;

        let driver: Box<dyn RegistryDriver> = match registry.kind.as_str() {
            "folder" | "directory" => {
                Box::new(FolderRegistry::from_uri(&registry.name, &registry.uri)?)
            }
            "ssh" => Box::new(SshRegistry::from_uri(
                &registry.name,
                &registry.uri,
                registry.auth_env.clone(),
            )?),
            "gdrive" => Box::new(GDriveRegistry::from_uri(
                &registry.name,
                &registry.uri,
                registry.auth_env.clone(),
            )?),
            "aws" => Box::new(AwsRegistry::from_uri(
                &registry.name,
                &registry.uri,
                registry.auth_env.clone(),
            )?),
            other => Box::new(StubRegistry::new(
                other,
                &registry.uri,
                registry.auth_env.clone(),
            )),
        };

        self.registries
            .insert(registry.name.clone(), (registry, driver));
        Ok(())
    }

    pub fn list_registry_configs(&self) -> Vec<&RegistryConfig> {
        let mut out = self
            .registries
            .values()
            .map(|(cfg, _)| cfg)
            .collect::<Vec<_>>();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    pub fn remove_registry(&mut self, name: &str, delete_data: bool) -> Result<RemovedRegistry> {
        let mut existing = config::load_registries()?;
        let idx = existing
            .registry
            .iter()
            .position(|r| r.name == name)
            .ok_or_else(|| anyhow!("registry '{}' not found", name))?;
        let removed = existing.registry.remove(idx);
        config::save_registries(&existing)?;
        self.registries.remove(name);

        let mut data_deleted = false;
        if delete_data && matches!(removed.kind.as_str(), "folder" | "directory") {
            let path = normalize_local_registry_path(&removed.uri);
            if path.exists() {
                fs::remove_dir_all(&path).with_context(|| {
                    format!("failed deleting data for registry at {}", path.display())
                })?;
            }
            data_deleted = true;
        }

        Ok(RemovedRegistry {
            name: removed.name,
            kind: removed.kind,
            uri: removed.uri,
            data_deleted,
        })
    }

    fn choose_registry(
        &self,
        name: Option<&str>,
    ) -> Result<(&RegistryConfig, &dyn RegistryDriver)> {
        if self.registries.is_empty() {
            return Err(anyhow!(
                "no registries configured. Add one with: marina registry add <uri> --name <name> --kind <kind>"
            ));
        }

        match name {
            Some(n) => {
                let (cfg, drv) = self
                    .registries
                    .get(n)
                    .ok_or_else(|| anyhow!("registry '{}' not found", n))?;
                Ok((cfg, drv.as_ref()))
            }
            None => {
                let (_, (cfg, drv)) = self
                    .registries
                    .iter()
                    .next()
                    .ok_or_else(|| anyhow!("no registries available"))?;
                Ok((cfg, drv.as_ref()))
            }
        }
    }

    fn ensure_auth(cfg: &RegistryConfig) -> Result<()> {
        if let Some(var) = &cfg.auth_env {
            if std::env::var(var).is_err() {
                return Err(anyhow!(
                    "registry '{}' requires auth env var '{}'",
                    cfg.name,
                    var
                ));
            }
        }
        Ok(())
    }

    pub fn push(&mut self, bag: &BagRef, source_dir: &Path, registry: Option<&str>) -> Result<()> {
        let source = bag::discover_bag(source_dir)?;
        let (cfg, driver) = self.choose_registry(registry)?;
        Self::ensure_auth(cfg)?;

        let cache_dir = cache::bag_cache_dir(&bag.without_attachment())?;
        let packed_file = cache_dir.join("bundle.marina.tar.gz");
        let packed_meta = pack::pack_bag(&source, &packed_file)?;

        driver.push(
            &cfg.name,
            &bag.without_attachment(),
            &packed_file,
            packed_meta.original_bytes,
            packed_meta.packed_bytes,
        )?;

        let ready_dir = cache_dir.join("ready");
        copy_dir(source_dir, &ready_dir)?;

        self.catalog.entries.insert(
            bag.without_attachment().to_string(),
            CacheEntry {
                bag: bag.without_attachment(),
                local_dir: ready_dir,
                packed_bytes: packed_meta.packed_bytes,
                original_bytes: packed_meta.original_bytes,
            },
        );
        cache::save_catalog(&self.catalog)?;
        Ok(())
    }

    pub fn pull_pattern(&mut self, pattern: &str, registry: Option<&str>) -> Result<Vec<BagRef>> {
        let (_cfg, driver) = self.choose_registry(registry)?;
        let refs = driver.list(pattern)?;
        let mut pulled = Vec::new();

        for bag in refs {
            self.pull_exact(&bag, registry)?;
            pulled.push(bag);
        }

        Ok(pulled)
    }

    pub fn pull_exact(&mut self, bag: &BagRef, registry: Option<&str>) -> Result<PathBuf> {
        let (cfg, driver) = self.choose_registry(registry)?;
        Self::ensure_auth(cfg)?;

        let cache_dir = cache::bag_cache_dir(&bag.without_attachment())?;
        let packed_file = cache_dir.join("bundle.remote.tar.gz");
        let descriptor = driver.pull(&bag.without_attachment(), &packed_file)?;
        let ready_dir = cache_dir.join("ready");
        if ready_dir.exists() {
            fs::remove_dir_all(&ready_dir)?;
        }
        fs::create_dir_all(&ready_dir)?;
        pack::unpack_bag(&packed_file, &ready_dir)?;

        self.catalog.entries.insert(
            bag.without_attachment().to_string(),
            CacheEntry {
                bag: bag.without_attachment(),
                local_dir: ready_dir.clone(),
                packed_bytes: descriptor.packed_bytes,
                original_bytes: descriptor.original_bytes,
            },
        );
        cache::save_catalog(&self.catalog)?;

        Ok(ready_dir)
    }

    pub fn resolve_target(&self, target: &str) -> Result<ResolveResult> {
        let path = Path::new(target);
        if bag::has_direct_mcap(path)? {
            return Ok(ResolveResult::LocalPath(path.to_path_buf()));
        }

        let bag_ref: BagRef = target.parse()?;
        if let Some(entry) = self
            .catalog
            .entries
            .get(&bag_ref.without_attachment().to_string())
        {
            if bag_ref.attachment.is_some() {
                let attachment = bag_ref
                    .attachment
                    .as_ref()
                    .ok_or_else(|| anyhow!("invalid attachment target"))?;
                let attach_path = entry.local_dir.join(attachment);
                if attach_path.exists() {
                    return Ok(ResolveResult::Cached(attach_path));
                }
            } else {
                return Ok(ResolveResult::Cached(entry.local_dir.clone()));
            }
        }

        for (name, (_cfg, drv)) in &self.registries {
            if let Ok(list) = drv.list(&bag_ref.without_attachment().to_string()) {
                if list.iter().any(|b| b == &bag_ref.without_attachment()) {
                    return Ok(ResolveResult::RemoteAvailable {
                        registry: name.clone(),
                        bag: bag_ref.without_attachment(),
                        needs_pull: true,
                    });
                }
            }
        }

        Err(anyhow!(
            "target '{}' is neither a local mcap bag directory nor known in cache/registries",
            target
        ))
    }

    pub fn export(&self, bag: &BagRef, out: &Path) -> Result<()> {
        let key = bag.without_attachment().to_string();
        let entry = self
            .catalog
            .entries
            .get(&key)
            .ok_or_else(|| anyhow!("bag '{}' not found in local cache", key))?;

        if let Some(att) = &bag.attachment {
            let src = entry.local_dir.join(att);
            if !src.exists() {
                return Err(anyhow!("attachment '{}' not found in cached bag", att));
            }
            if let Some(parent) = out.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(src, out)?;
            return Ok(());
        }

        copy_dir(&entry.local_dir, out)?;
        Ok(())
    }

    pub fn remove_local(&mut self, bag: &BagRef) -> Result<()> {
        let key = bag.without_attachment().to_string();
        if let Some(entry) = self.catalog.entries.remove(&key) {
            if entry.local_dir.exists() {
                fs::remove_dir_all(&entry.local_dir)?;
            }
            let root = cache::bag_cache_dir(&bag.without_attachment())?;
            if root.exists() {
                fs::remove_dir_all(root)?;
            }
            cache::save_catalog(&self.catalog)?;
        }
        Ok(())
    }

    pub fn remove_remote(&self, bag: &BagRef, registry: Option<&str>) -> Result<()> {
        let (cfg, driver) = self.choose_registry(registry)?;
        Self::ensure_auth(cfg)?;
        driver.remove(&bag.without_attachment())
    }

    pub fn search_remote(&self, pattern: &str, registry: Option<&str>) -> Result<Vec<BagRef>> {
        let (_cfg, driver) = self.choose_registry(registry)?;
        driver.list(pattern)
    }

    pub fn clean(&mut self, all: bool) -> Result<()> {
        self.catalog.entries.clear();
        cache::save_catalog(&self.catalog)?;
        config::remove_local_state(all)?;
        Ok(())
    }

    pub fn format_cached_size_line(&self, bag: &BagRef) -> Option<String> {
        self.catalog
            .entries
            .get(&bag.without_attachment().to_string())
            .map(|entry| {
                format!(
                    "{}: original {} bytes, packed {} bytes",
                    bag.without_attachment(),
                    entry.original_bytes,
                    entry.packed_bytes
                )
            })
    }

    pub fn cached_size_stats(&self, bag: &BagRef) -> Option<CachedSizeStats> {
        self.catalog
            .entries
            .get(&bag.without_attachment().to_string())
            .map(|entry| CachedSizeStats {
                original_bytes: entry.original_bytes,
                packed_bytes: entry.packed_bytes,
            })
    }

    pub fn list_cached_bags(&self) -> Vec<CachedBagInfo> {
        let mut out = self
            .catalog
            .entries
            .values()
            .map(|entry| CachedBagInfo {
                bag: entry.bag.clone(),
                local_dir: entry.local_dir.clone(),
                original_bytes: entry.original_bytes,
                packed_bytes: entry.packed_bytes,
            })
            .collect::<Vec<_>>();
        out.sort_by(|a, b| a.bag.to_string().cmp(&b.bag.to_string()));
        out
    }

    pub fn search_all_remotes(&self, pattern: &str) -> Vec<RemoteBagHit> {
        let mut names = self.registries.keys().cloned().collect::<Vec<_>>();
        names.sort();
        let mut hits = Vec::new();

        for name in names {
            if let Some((_, driver)) = self.registries.get(&name)
                && let Ok(items) = driver.list(pattern)
            {
                for bag in items {
                    hits.push(RemoteBagHit {
                        registry: name.clone(),
                        bag,
                    });
                }
            }
        }

        hits.sort_by(|a, b| {
            a.registry
                .cmp(&b.registry)
                .then_with(|| a.bag.to_string().cmp(&b.bag.to_string()))
        });
        hits
    }
}

fn normalize_local_registry_path(uri: &str) -> PathBuf {
    if let Some(rest) = uri.strip_prefix("folder://") {
        PathBuf::from(rest)
    } else if let Some(rest) = uri.strip_prefix("folder::") {
        PathBuf::from(rest)
    } else if let Some(rest) = uri.strip_prefix("directory://") {
        PathBuf::from(rest)
    } else if let Some(rest) = uri.strip_prefix("directory::") {
        PathBuf::from(rest)
    } else {
        PathBuf::from(uri)
    }
}

fn copy_dir(src: &Path, dst: &Path) -> Result<()> {
    if !src.exists() {
        return Err(anyhow!("source does not exist: {}", src.display()));
    }
    fs::create_dir_all(dst)?;
    for entry in walkdir::WalkDir::new(src) {
        let entry = entry?;
        let path = entry.path();
        let rel = path.strip_prefix(src).with_context(|| {
            format!(
                "failed to strip prefix {} from {}",
                src.display(),
                path.display()
            )
        })?;
        let target = dst.join(rel);
        if path.is_dir() {
            fs::create_dir_all(&target)?;
        } else {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(path, &target)?;
        }
    }
    Ok(())
}
