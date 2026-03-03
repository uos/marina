use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use sha2::{Digest, Sha256};

use crate::io::mcap_transform::{McapChunkCompression, PointCloudCompressionMode};
use crate::io::{bag, pack};
use crate::model::bag_ref::BagRef;
use crate::progress::ProgressReporter;
use crate::registry::driver::{BagInfo, PushMeta, RegistryDriver};
use crate::registry::folder::FolderRegistry;
use crate::registry::gdrive::GDriveRegistry;
use crate::registry::http::HttpRegistry;
use crate::registry::ssh::SshRegistry;
use crate::registry::stub::StubRegistry;
use crate::storage::cache::{self, CacheEntry, Catalog};
use crate::storage::config::{self, RegistryConfig};

use log::warn;

pub fn connection_warning(name: &str, uri: &str, driver: &dyn RegistryDriver) -> Option<String> {
    if let Err(e) = driver.check_connection() {
        Some(format!(
            "warning: default registry '{}' ({}) appears unreachable: {}",
            name, uri, e
        ))
    } else {
        None
    }
}

#[derive(Debug, Clone)]
pub enum ResolveResult {
    /// Target string resolved directly to an existing local bag directory.
    LocalPath(PathBuf),
    /// Target resolved to an existing decompressed cache directory/file.
    Cached(PathBuf),
    /// Target exists in a remote registry and can be pulled.
    RemoteAvailable {
        registry: String,
        bag: BagRef,
        needs_pull: bool,
    },
    /// Target found in multiple registries; caller must pick one.
    Ambiguous { candidates: Vec<(String, BagRef)> },
}

/// Result of removing a registry configuration.
#[derive(Debug, Clone)]
pub struct RemovedRegistry {
    pub name: String,
    pub kind: String,
    pub uri: String,
    pub data_deleted: bool,
}

/// Size information for one cached bag entry.
#[derive(Debug, Clone, Copy)]
pub struct CachedSizeStats {
    pub original_bytes: u64,
    pub packed_bytes: u64,
}

/// Cached bag metadata returned by [`Marina::list_cached_bags`].
#[derive(Debug, Clone)]
pub struct CachedBagInfo {
    pub bag: BagRef,
    pub local_dir: PathBuf,
    pub original_bytes: u64,
    pub packed_bytes: u64,
}

/// A single remote search hit across configured registries.
#[derive(Debug, Clone)]
pub struct RemoteBagHit {
    pub registry: String,
    pub bag: BagRef,
}

/// Compression options used while pushing a bag into a packed archive.
#[derive(Debug, Clone, Copy)]
pub struct PushOptions {
    pub pointcloud_mode: PointCloudCompressionMode,
    pub pointcloud_precision_m: f64,
    pub packed_mcap_compression: McapChunkCompression,
    pub packed_archive_compression: pack::ArchiveCompression,
    pub write_http_index: bool,
}

impl Default for PushOptions {
    fn default() -> Self {
        Self {
            pointcloud_mode: PointCloudCompressionMode::Lossy,
            pointcloud_precision_m: 0.001,
            packed_mcap_compression: McapChunkCompression::Zstd,
            packed_archive_compression: pack::ArchiveCompression::Gzip,
            write_http_index: false,
        }
    }
}

/// Compression options used while unpacking a pulled archive into ready cache.
#[derive(Debug, Clone, Copy)]
pub struct PullOptions {
    pub unpacked_mcap_compression: McapChunkCompression,
}

impl Default for PullOptions {
    fn default() -> Self {
        Self {
            unpacked_mcap_compression: McapChunkCompression::Zstd,
        }
    }
}

/// High-level marina runtime that owns registry drivers and local catalog state.
pub struct Marina {
    registries: HashMap<String, (RegistryConfig, Box<dyn RegistryDriver>)>,
    catalog: Catalog,
}

impl Marina {
    /// Loads marina configuration, registry drivers, and local cache catalog.
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
                "http" => Box::new(HttpRegistry::from_uri(&reg.name, &reg.uri)?),
                other => Box::new(StubRegistry::new(other, &reg.uri, reg.auth_env.clone())),
            };

            if let Some(msg) = connection_warning(&reg.name, &reg.uri, driver.as_ref()) {
                warn!("{}", msg);
            }

            registries.insert(reg.name.clone(), (reg, driver));
        }

        let catalog = cache::load_catalog()?;
        Ok(Self {
            registries,
            catalog,
        })
    }

    /// Adds a new registry and persists it to `registries.toml`.
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
            "http" => Box::new(HttpRegistry::from_uri(&registry.name, &registry.uri)?),
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

    /// Lists configured registry records sorted by name.
    pub fn list_registry_configs(&self) -> Vec<&RegistryConfig> {
        let mut out = self
            .registries
            .values()
            .map(|(cfg, _)| cfg)
            .collect::<Vec<_>>();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// Removes a registry configuration.
    ///
    /// If `delete_data` is true and the registry is local folder-based,
    /// marina also removes the folder contents.
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

    fn ensure_auth(cfg: &RegistryConfig, required: bool) -> Result<()> {
        if !required {
            return Ok(());
        }
        if let Some(var) = &cfg.auth_env
            && std::env::var(var).is_err()
        {
            return Err(anyhow!(
                "registry '{}' requires auth env var '{}'",
                cfg.name,
                var
            ));
        }
        Ok(())
    }

    pub fn push(&mut self, bag: &BagRef, source_dir: &Path, registry: Option<&str>) -> Result<()> {
        let mut progress = ProgressReporter::silent();
        self.push_with_progress_and_options(
            bag,
            source_dir,
            registry,
            PushOptions::default(),
            &mut progress,
        )
    }

    /// Pushes a bag to a registry and emits phase progress events.
    pub fn push_with_progress(
        &mut self,
        bag: &BagRef,
        source_dir: &Path,
        registry: Option<&str>,
        progress: &mut ProgressReporter<'_>,
    ) -> Result<()> {
        self.push_with_progress_and_options(
            bag,
            source_dir,
            registry,
            PushOptions::default(),
            progress,
        )
    }

    /// Pushes a bag to a registry with explicit compression options.
    pub fn push_with_progress_and_options(
        &mut self,
        bag: &BagRef,
        source_dir: &Path,
        registry: Option<&str>,
        options: PushOptions,
        progress: &mut ProgressReporter<'_>,
    ) -> Result<()> {
        let source = bag::discover_bag(source_dir)?;
        let (cfg, driver) = self.choose_registry(registry)?;
        Self::ensure_auth(cfg, true)?;
        progress.emit(
            "push",
            format!(
                "preparing '{}' for registry '{}'",
                bag.without_attachment(),
                cfg.name
            ),
        );

        let cache_dir = cache::bag_cache_dir(&bag.without_attachment())?;
        let packed_file = cache_dir.join("bundle.marina.tar.gz");
        let packed_meta = pack::pack_bag_with_progress_and_options(
            &source,
            &packed_file,
            pack::PackOptions {
                transform: crate::io::mcap_transform::PushTransformOptions {
                    pointcloud_mode: options.pointcloud_mode,
                    pointcloud_precision_m: options.pointcloud_precision_m,
                    output_mcap_compression: options.packed_mcap_compression,
                },
                archive_compression: options.packed_archive_compression,
            },
            progress,
        )?;

        progress.emit(
            "push",
            format!(
                "uploading packed bundle to registry '{}' ({})",
                cfg.name, cfg.kind
            ),
        );
        let bundle_hash = compute_bundle_hash(&packed_file)?;
        let push_meta = PushMeta {
            original_bytes: packed_meta.original_bytes,
            packed_bytes: packed_meta.packed_bytes,
            bundle_hash,
            pointcloud: pointcloud_mode_label(options.pointcloud_mode).to_string(),
            mcap_compression: mcap_compression_label(options.packed_mcap_compression).to_string(),
        };
        driver.push(
            &cfg.name,
            &bag.without_attachment(),
            &packed_file,
            &push_meta,
        )?;

        if options.write_http_index {
            progress.emit(
                "push",
                format!("writing http index.json for registry '{}'", cfg.name),
            );
            driver.write_http_index()?;
        }

        let ready_dir = cache_dir.join("ready");
        progress.emit("push", "refreshing local ready-to-use cache");
        copy_source(source_dir, &ready_dir)?;

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
        progress.emit("push", "push complete");
        Ok(())
    }

    /// Pulls all remote bags matching `pattern`.
    pub fn pull_pattern(&mut self, pattern: &str, registry: Option<&str>) -> Result<Vec<BagRef>> {
        let mut progress = ProgressReporter::silent();
        self.pull_pattern_with_progress_and_options(
            pattern,
            registry,
            PullOptions::default(),
            &mut progress,
        )
    }

    /// Pulls all remote bags matching `pattern` and emits progress events.
    pub fn pull_pattern_with_progress(
        &mut self,
        pattern: &str,
        registry: Option<&str>,
        progress: &mut ProgressReporter<'_>,
    ) -> Result<Vec<BagRef>> {
        self.pull_pattern_with_progress_and_options(
            pattern,
            registry,
            PullOptions::default(),
            progress,
        )
    }

    /// Pulls all remote bags matching `pattern` and applies explicit unpack options.
    pub fn pull_pattern_with_progress_and_options(
        &mut self,
        pattern: &str,
        registry: Option<&str>,
        options: PullOptions,
        progress: &mut ProgressReporter<'_>,
    ) -> Result<Vec<BagRef>> {
        let (_cfg, driver) = self.choose_registry(registry)?;
        let refs = driver.list(pattern)?;
        let mut pulled = Vec::new();

        progress.emit(
            "pull",
            format!("found {} matching bag(s) for '{}'", refs.len(), pattern),
        );
        for bag in refs {
            self.pull_exact_with_progress_and_options(&bag, registry, options, progress)?;
            pulled.push(bag);
        }

        Ok(pulled)
    }

    pub fn pull_exact(&mut self, bag: &BagRef, registry: Option<&str>) -> Result<PathBuf> {
        let mut progress = ProgressReporter::silent();
        self.pull_exact_with_progress_and_options(
            bag,
            registry,
            PullOptions::default(),
            &mut progress,
        )
    }

    /// Pulls one exact bag and emits phase progress events.
    pub fn pull_exact_with_progress(
        &mut self,
        bag: &BagRef,
        registry: Option<&str>,
        progress: &mut ProgressReporter<'_>,
    ) -> Result<PathBuf> {
        self.pull_exact_with_progress_and_options(bag, registry, PullOptions::default(), progress)
    }

    /// Pulls one exact bag and applies explicit unpack options.
    pub fn pull_exact_with_progress_and_options(
        &mut self,
        bag: &BagRef,
        registry: Option<&str>,
        options: PullOptions,
        progress: &mut ProgressReporter<'_>,
    ) -> Result<PathBuf> {
        let (cfg, driver) = self.choose_registry(registry)?;
        Self::ensure_auth(cfg, false)?;
        progress.emit(
            "pull",
            format!(
                "downloading '{}' from registry '{}' ({})",
                bag.without_attachment(),
                cfg.name,
                cfg.kind
            ),
        );

        let cache_dir = cache::bag_cache_dir(&bag.without_attachment())?;
        let packed_file = cache_dir.join("bundle.remote.tar.gz");
        let descriptor = driver.pull(&bag.without_attachment(), &packed_file)?;
        let ready_dir = cache_dir.join("ready");
        if ready_dir.exists() {
            fs::remove_dir_all(&ready_dir)?;
        }
        fs::create_dir_all(&ready_dir)?;
        pack::unpack_bag_with_progress_and_options(
            &packed_file,
            &ready_dir,
            pack::UnpackOptions {
                transform: crate::io::mcap_transform::PullTransformOptions {
                    output_mcap_compression: options.unpacked_mcap_compression,
                },
            },
            progress,
        )?;

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
        progress.emit("pull", "pull complete");

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

        let mut names: Vec<_> = self.registries.keys().cloned().collect();
        names.sort();
        let mut matches: Vec<(String, BagRef)> = Vec::new();
        for name in names {
            if let Some((_cfg, drv)) = self.registries.get(&name)
                && let Ok(list) = drv.list(&bag_ref.without_attachment().to_string())
                && list.iter().any(|b| b == &bag_ref.without_attachment())
            {
                matches.push((name, bag_ref.without_attachment()));
            }
        }

        match matches.len() {
            0 => Err(anyhow!(
                "target '{}' is neither a local mcap bag directory nor known in cache/registries",
                target
            )),
            1 => Ok(ResolveResult::RemoteAvailable {
                registry: matches.remove(0).0,
                bag: bag_ref.without_attachment(),
                needs_pull: true,
            }),
            _ => Ok(ResolveResult::Ambiguous {
                candidates: matches,
            }),
        }
    }

    /// Exports a cached bag (or one attachment) to `out`.
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

    /// Removes one bag from local cache and catalog.
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

    /// Removes one bag from a remote registry.
    pub fn remove_remote(&self, bag: &BagRef, registry: Option<&str>) -> Result<()> {
        let (cfg, driver) = self.choose_registry(registry)?;
        Self::ensure_auth(cfg, true)?;
        driver.remove(&bag.without_attachment())
    }

    /// Searches one registry using glob-like `pattern`.
    pub fn search_remote(&self, pattern: &str, registry: Option<&str>) -> Result<Vec<BagRef>> {
        let (_cfg, driver) = self.choose_registry(registry)?;
        driver.list(pattern)
    }

    /// Deletes local cache resources.
    ///
    /// With `all = true`, also removes local registry configuration files.
    pub fn clean(&mut self, all: bool) -> Result<()> {
        self.catalog.entries.clear();
        cache::save_catalog(&self.catalog)?;
        config::remove_local_state(all)?;
        Ok(())
    }

    /// Returns a preformatted cached-size line for one bag.
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

    /// Returns cached size stats for one bag.
    pub fn cached_size_stats(&self, bag: &BagRef) -> Option<CachedSizeStats> {
        self.catalog
            .entries
            .get(&bag.without_attachment().to_string())
            .map(|entry| CachedSizeStats {
                original_bytes: entry.original_bytes,
                packed_bytes: entry.packed_bytes,
            })
    }

    /// Lists all locally cached bags sorted by bag reference.
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

    /// Searches all registries and returns tagged hits with registry names.
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

    /// Fetch lightweight metadata for a bag in a specific registry.
    pub fn bag_info(&self, registry: &str, bag: &BagRef) -> Option<BagInfo> {
        self.registries
            .get(registry)
            .and_then(|(_, drv)| drv.bag_info(bag).ok().flatten())
    }

    /// List all bags across all registries with their stored metadata.
    pub fn list_all_remotes_with_info(&self) -> Vec<(RemoteBagHit, Option<BagInfo>)> {
        self.search_all_remotes("*")
            .into_iter()
            .map(|hit| {
                let info = self.bag_info(&hit.registry, &hit.bag);
                (hit, info)
            })
            .collect()
    }
}

fn compute_bundle_hash(path: &Path) -> Result<String> {
    use std::io::Read as _;
    let mut file = fs::File::open(path)
        .with_context(|| format!("failed to open bundle for hashing: {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let hash: String = hasher
        .finalize()
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect();
    Ok(hash[..12].to_string())
}

fn pointcloud_mode_label(mode: PointCloudCompressionMode) -> &'static str {
    match mode {
        PointCloudCompressionMode::Lossless => "lossless",
        PointCloudCompressionMode::Lossy => "lossy",
        PointCloudCompressionMode::Disabled => "disabled",
    }
}

fn mcap_compression_label(c: McapChunkCompression) -> &'static str {
    match c {
        McapChunkCompression::None => "none",
        McapChunkCompression::Zstd => "zstd",
        McapChunkCompression::Lz4 => "lz4",
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

fn copy_source(src: &Path, dst: &Path) -> Result<()> {
    if src.is_file() {
        fs::create_dir_all(dst)?;
        let name = src
            .file_name()
            .ok_or_else(|| anyhow!("source file has no file name: {}", src.display()))?;
        fs::copy(src, dst.join(name))
            .with_context(|| format!("failed copying {} into {}", src.display(), dst.display()))?;
        return Ok(());
    }

    copy_dir(src, dst)
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
