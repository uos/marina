use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use sha2::{Digest, Sha256};

use crate::io::mcap_transform::{McapChunkCompression, PointCloudCompressionMode};
use crate::io::{bag, pack};
use crate::model::bag_ref::BagRef;
use crate::progress::ProgressReporter;
use crate::registry::driver::{BagInfo, PushMeta, RegistryDriver};
use crate::registry::folder::FolderRegistry;
#[cfg(feature = "gdrive")]
use crate::registry::gdrive::GDriveRegistry;
use crate::registry::http::HttpRegistry;
use crate::registry::ssh::SshRegistry;
use crate::registry::stub::StubRegistry;
use crate::storage::cache::{self, CacheEntry, Catalog};
use crate::storage::config::{self, RegistryConfig};

/// Statistics returned by [`Marina::mirror_registry`].
#[derive(Debug, Default, Clone)]
pub struct MirrorStats {
    /// Bags pushed to target for the first time.
    pub pushed: u32,
    /// Bags replaced in target because the source hash changed.
    pub updated: u32,
    /// Bags skipped because they were already up to date (or not comparable).
    pub skipped: u32,
}

pub async fn connection_warning(
    name: &str,
    uri: &str,
    driver: &dyn RegistryDriver,
) -> Option<String> {
    if let Err(e) = driver.check_connection().await {
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
}

/// A single remote search hit across configured registries.
#[derive(Debug, Clone)]
pub struct RemoteBagHit {
    pub registry: String,
    pub bag: BagRef,
}

/// One file found inside an inspected bag.
#[derive(Debug, Clone)]
pub struct InspectFile {
    /// Path relative to the bag root.
    pub relative_path: String,
    pub size_bytes: u64,
}

/// Remote registry hit returned by [`Marina::inspect_bag`].
#[derive(Debug, Clone)]
pub struct InspectRemoteHit {
    pub registry: String,
    pub info: Option<BagInfo>,
    pub timed_out: bool,
}

/// Full inspection result for one dataset.
#[derive(Debug, Clone)]
pub struct InspectResult {
    pub bag: BagRef,
    /// Present when the bag is in the local cache.
    pub local_dir: Option<PathBuf>,
    /// File listing from the local cache; empty when not cached.
    pub local_files: Vec<InspectFile>,
    /// Metadata from each queried remote registry.
    pub remote_hits: Vec<InspectRemoteHit>,
}

/// Compression options used while pushing a bag into a packed archive.
#[derive(Debug, Clone, Copy)]
pub struct PushOptions {
    pub pointcloud_mode: PointCloudCompressionMode,
    pub pointcloud_precision_m: f64,
    pub packed_mcap_compression: McapChunkCompression,
    pub packed_archive_compression: pack::ArchiveCompression,
    pub write_http_index: bool,
    pub dry_run: bool,
    /// Move the source into the cache instead of copying it.
    pub move_source_to_cache: bool,
}

impl Default for PushOptions {
    fn default() -> Self {
        Self {
            pointcloud_mode: PointCloudCompressionMode::Lossless,
            pointcloud_precision_m: 0.001,
            packed_mcap_compression: McapChunkCompression::Zstd,
            packed_archive_compression: pack::ArchiveCompression::Gzip,
            write_http_index: false,
            dry_run: false,
            move_source_to_cache: false,
        }
    }
}

/// Compression options used while unpacking a pulled archive into ready cache.
#[derive(Debug, Clone, Copy)]
pub struct PullOptions {
    pub unpacked_mcap_compression: McapChunkCompression,
    /// Skip the local hash check and always re-download from the remote.
    pub force: bool,
}

impl Default for PullOptions {
    fn default() -> Self {
        Self {
            unpacked_mcap_compression: McapChunkCompression::Lz4,
            force: false,
        }
    }
}

/// High-level marina runtime that owns registry drivers and local catalog state.
pub struct Marina {
    registries: HashMap<String, (RegistryConfig, Arc<dyn RegistryDriver>)>,
    catalog: Catalog,
    default_registry: Option<String>,
}

impl Marina {
    /// Loads marina configuration, registry drivers, and local cache catalog.
    pub fn load() -> Result<Self> {
        let registry_file = config::load_registries()?;
        let default_registry = registry_file.settings.default_registry.clone();

        let mut registries = HashMap::new();

        for reg in registry_file.registry {
            let driver = make_registry_driver(&reg)?;
            registries.insert(reg.name.clone(), (reg, driver));
        }

        let catalog = cache::load_catalog()?;
        Ok(Self {
            registries,
            catalog,
            default_registry,
        })
    }

    /// Returns the configured default registry name, if any.
    pub fn default_registry(&self) -> Option<&str> {
        self.default_registry.as_deref()
    }

    /// Adds a new registry and persists it to `registries.toml`.
    pub fn add_registry(&mut self, registry: RegistryConfig) -> Result<()> {
        validate_registry_name(&registry.name)?;
        let mut existing = config::load_registries()?;
        if existing.registry.iter().any(|r| r.name == registry.name) {
            return Err(anyhow!("registry '{}' already exists", registry.name));
        }
        existing.registry.push(registry.clone());
        config::save_registries(&existing)?;

        let driver = make_registry_driver(&registry)?;

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
    ) -> Result<(RegistryConfig, Arc<dyn RegistryDriver>)> {
        if self.registries.is_empty() {
            return Err(anyhow!(
                "no registries configured. Add one with: marina registry add <name> <uri>"
            ));
        }

        match name {
            Some(n) => {
                let (cfg, drv) = self
                    .registries
                    .get(n)
                    .ok_or_else(|| anyhow!("registry '{}' not found", n))?;
                Ok((cfg.clone(), Arc::clone(drv)))
            }
            None => {
                let (_, (cfg, drv)) = self
                    .registries
                    .iter()
                    .next()
                    .ok_or_else(|| anyhow!("no registries available"))?;
                Ok((cfg.clone(), Arc::clone(drv)))
            }
        }
    }

    fn ensure_auth(cfg: &RegistryConfig, required: bool) -> Result<()> {
        if !required {
            return Ok(());
        }
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

    pub async fn push(
        &mut self,
        bag: &BagRef,
        source_dir: &Path,
        registry: Option<&str>,
    ) -> Result<()> {
        let mut progress = ProgressReporter::silent();
        self.push_with_progress_and_options(
            bag,
            source_dir,
            registry,
            PushOptions::default(),
            &mut progress,
        )
        .await
    }

    /// Pushes a bag to a registry and emits phase progress events.
    pub async fn push_with_progress(
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
        .await
    }

    /// Pushes a bag to a registry with explicit compression options.
    pub async fn push_with_progress_and_options(
        &mut self,
        bag: &BagRef,
        source_dir: &Path,
        registry: Option<&str>,
        options: PushOptions,
        progress: &mut ProgressReporter<'_>,
    ) -> Result<()> {
        let (cfg, driver) = self.choose_registry(registry)?;
        Self::ensure_auth(&cfg, true)?;
        progress.emit(
            "push",
            format!("checking write access for registry '{}'", cfg.name),
        );
        driver
            .check_write_access()
            .await
            .with_context(|| format!("write preflight failed for registry '{}'", cfg.name))?;

        let source = bag::discover_bag(source_dir)?;
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

        if options.dry_run {
            progress.emit(
                "push",
                format!(
                    "dry-run: packed bundle prepared for registry '{}' ({}); upload/index/cache update skipped",
                    cfg.name, cfg.kind
                ),
            );
            progress.emit("push", "dry-run complete");
            return Ok(());
        }

        progress.emit(
            "push",
            format!(
                "uploading packed bundle to registry '{}' ({})",
                cfg.name, cfg.kind
            ),
        );
        let bundle_hash = compute_bundle_hash(&packed_file)?;
        let ros_pipeline_applies = source.mcap.is_some() || source.has_db3;
        let push_meta = PushMeta {
            original_bytes: packed_meta.original_bytes,
            packed_bytes: packed_meta.packed_bytes,
            bundle_hash,
            pointcloud: if ros_pipeline_applies {
                let label = pointcloud_mode_label(options.pointcloud_mode);
                if options.pointcloud_mode == PointCloudCompressionMode::Lossy {
                    format!(
                        "{} {}",
                        label,
                        format_precision(options.pointcloud_precision_m)
                    )
                } else {
                    label.to_string()
                }
            } else {
                "n/a".to_string()
            },
            mcap_compression: if ros_pipeline_applies {
                mcap_compression_label(options.packed_mcap_compression).to_string()
            } else {
                "n/a".to_string()
            },
            pushed_at: now_unix_secs(),
        };
        driver
            .push(
                &cfg.name,
                &bag.without_attachment(),
                &packed_file,
                &push_meta,
            )
            .await?;
        fs::remove_file(&packed_file)?;

        if options.write_http_index || cfg.kind == "ssh" {
            progress.emit(
                "push",
                format!("writing http index.json for registry '{}'", cfg.name),
            );
            driver.write_http_index().await?;
        }

        // Ensure the bag lives in our cache so local_dir is always under our
        // control.  If the caller already pushed from the cache path we skip
        // the copy/move.
        let ready_dir = cache_dir.join("ready");
        let canonical_source = source_dir.canonicalize()?;
        if canonical_source != ready_dir.canonicalize().unwrap_or_default() {
            if ready_dir.exists() {
                fs::remove_dir_all(&ready_dir)?;
            }
            if options.move_source_to_cache {
                progress.emit("push", "moving source to local cache");
                move_or_copy(&canonical_source, &ready_dir)?;
            } else {
                progress.emit("push", "copying to local cache");
                copy_dir(&canonical_source, &ready_dir)?;
            }
        }

        self.catalog.entries.insert(
            bag.without_attachment().to_string(),
            CacheEntry {
                bag: bag.without_attachment(),
                local_dir: ready_dir,
                packed_bytes: packed_meta.packed_bytes,
                bundle_hash: Some(push_meta.bundle_hash.clone()),
            },
        );
        cache::save_catalog(&self.catalog)?;
        progress.emit("push", "push complete");
        Ok(())
    }

    /// Pulls all remote bags matching `pattern`.
    pub async fn pull_pattern(
        &mut self,
        pattern: &str,
        registry: Option<&str>,
    ) -> Result<Vec<BagRef>> {
        let mut progress = ProgressReporter::silent();
        self.pull_pattern_with_progress_and_options(
            pattern,
            registry,
            PullOptions::default(),
            &mut progress,
        )
        .await
    }

    /// Pulls all remote bags matching `pattern` and emits progress events.
    pub async fn pull_pattern_with_progress(
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
        .await
    }

    /// Pulls all remote bags matching `pattern` and applies explicit unpack options.
    pub async fn pull_pattern_with_progress_and_options(
        &mut self,
        pattern: &str,
        registry: Option<&str>,
        options: PullOptions,
        progress: &mut ProgressReporter<'_>,
    ) -> Result<Vec<BagRef>> {
        let (_cfg, driver) = self.choose_registry(registry)?;
        let refs = driver.list(pattern).await?;
        let mut pulled = Vec::new();

        progress.emit(
            "pull",
            format!("found {} matching bag(s) for '{}'", refs.len(), pattern),
        );
        for bag in refs {
            self.pull_exact_with_progress_and_options(&bag, registry, options, progress)
                .await?;
            pulled.push(bag);
        }

        Ok(pulled)
    }

    pub async fn pull_exact(&mut self, bag: &BagRef, registry: Option<&str>) -> Result<PathBuf> {
        let mut progress = ProgressReporter::silent();
        self.pull_exact_with_progress_and_options(
            bag,
            registry,
            PullOptions::default(),
            &mut progress,
        )
        .await
    }

    /// Pulls one exact bag and emits phase progress events.
    pub async fn pull_exact_with_progress(
        &mut self,
        bag: &BagRef,
        registry: Option<&str>,
        progress: &mut ProgressReporter<'_>,
    ) -> Result<PathBuf> {
        self.pull_exact_with_progress_and_options(bag, registry, PullOptions::default(), progress)
            .await
    }

    /// Pulls one exact bag and applies explicit unpack options.
    pub async fn pull_exact_with_progress_and_options(
        &mut self,
        bag: &BagRef,
        registry: Option<&str>,
        options: PullOptions,
        progress: &mut ProgressReporter<'_>,
    ) -> Result<PathBuf> {
        let (cfg, driver) = self.choose_registry(registry)?;
        Self::ensure_auth(&cfg, false)?;

        let key = bag.without_attachment().to_string();

        if !options.force {
            // Fast path: stored hash + local dir present → skip without any network call.
            if let Some(entry) = self.catalog.entries.get(&key) {
                if entry.local_dir.exists() && entry.bundle_hash.is_some() {
                    let cached_path = entry.local_dir.clone();
                    progress.emit("pull", "already up to date");
                    return Ok(cached_path);
                }
            }

            // No stored hash (old entry) — fetch remote metadata to verify and migrate.
            progress.emit("pull", "checking remote...");
            let remote_info = driver
                .bag_info(&bag.without_attachment())
                .await
                .ok()
                .flatten();

            if let Some(ref info) = remote_info {
                if let Some(entry) = self.catalog.entries.get(&key) {
                    if entry.local_dir.exists() && entry.packed_bytes == info.packed_bytes {
                        let cached_path = entry.local_dir.clone();
                        if let Some(rh) = &info.bundle_hash {
                            if let Some(e) = self.catalog.entries.get_mut(&key) {
                                e.bundle_hash = Some(rh.clone());
                            }
                            cache::save_catalog(&self.catalog)?;
                        }
                        progress.emit("pull", "already up to date");
                        return Ok(cached_path);
                    }
                }
            }
        }
        progress.emit(
            "pull",
            format!("downloading from registry '{}' ({})", cfg.name, cfg.kind),
        );

        let cache_dir = cache::bag_cache_dir(&bag.without_attachment())?;
        let packed_file = cache_dir.join("bundle.remote.tar.gz");
        let ready_dir = cache_dir.join("ready");

        crate::cleanup::register(packed_file.clone());
        crate::cleanup::register(ready_dir.clone());

        let descriptor = driver.pull(&bag.without_attachment(), &packed_file).await?;

        // Compute hash from the downloaded bundle so the catalog is always up to date.
        let remote_hash = compute_bundle_hash(&packed_file).ok();

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
        fs::remove_file(&packed_file)?;

        self.catalog.entries.insert(
            bag.without_attachment().to_string(),
            CacheEntry {
                bag: bag.without_attachment(),
                local_dir: ready_dir.clone(),
                packed_bytes: descriptor.packed_bytes,
                bundle_hash: remote_hash,
            },
        );
        cache::save_catalog(&self.catalog)?;
        crate::cleanup::commit();
        progress.emit("pull", "pull complete");

        Ok(ready_dir)
    }

    /// Returns true if `s` could be a hash prefix (all hex digits, at least 4 chars).
    fn looks_like_hash_prefix(s: &str) -> bool {
        s.len() >= 4 && s.chars().all(|c| c.is_ascii_hexdigit())
    }

    /// Searches registries for bags whose bundle hash starts with `prefix`.
    /// Respects the `registry` scope if provided.
    async fn find_by_hash_prefix(
        &self,
        prefix: &str,
        registry: Option<&str>,
    ) -> Vec<(String, BagRef)> {
        let prefix = prefix.to_ascii_lowercase();
        let mut matches: Vec<(String, BagRef)> = Vec::new();

        if let Some(reg_name) = registry {
            if let Some((_, drv)) = self.registries.get(reg_name) {
                if let Ok(bags) = drv.list_with_info("*").await {
                    for (bag, info) in bags {
                        if info
                            .and_then(|i| i.bundle_hash)
                            .is_some_and(|h| h.to_ascii_lowercase().starts_with(&prefix))
                        {
                            matches.push((reg_name.to_string(), bag));
                        }
                    }
                }
            }
        } else {
            let mut names = self.registries.keys().cloned().collect::<Vec<_>>();
            names.sort();
            let mut join_set = tokio::task::JoinSet::new();
            for name in names {
                if let Some((_, drv)) = self.registries.get(&name) {
                    let drv = Arc::clone(drv);
                    let prefix = prefix.clone();
                    let name = name.clone();
                    join_set.spawn(async move {
                        drv.list_with_info("*")
                            .await
                            .ok()
                            .unwrap_or_default()
                            .into_iter()
                            .filter_map(|(bag, info)| {
                                let hash = info?.bundle_hash?;
                                if hash.to_ascii_lowercase().starts_with(&prefix) {
                                    Some((name.clone(), bag))
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<_>>()
                    });
                }
            }
            while let Some(Ok(chunk)) = join_set.join_next().await {
                matches.extend(chunk);
            }
        }

        matches.sort_by_key(|(name, _)| name.clone());
        matches
    }

    pub async fn resolve_target(
        &self,
        target: &str,
        registry: Option<&str>,
    ) -> Result<ResolveResult> {
        let path = Path::new(target);
        if bag::has_direct_mcap(path)? {
            return Ok(ResolveResult::LocalPath(path.to_path_buf()));
        }

        // Hash-prefix lookup: if the target looks like a hex prefix, search by hash first.
        if Self::looks_like_hash_prefix(target) {
            let hash_matches = self.find_by_hash_prefix(target, registry).await;
            match hash_matches.len() {
                0 => {}
                1 => {
                    let (reg, bag) = hash_matches.into_iter().next().unwrap();
                    return Ok(ResolveResult::RemoteAvailable {
                        registry: reg,
                        bag,
                        needs_pull: true,
                    });
                }
                _ => {
                    return Ok(ResolveResult::Ambiguous {
                        candidates: hash_matches,
                    });
                }
            }
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

        let mut matches: Vec<(String, BagRef)> = Vec::new();
        let exact = bag_ref.without_attachment().to_string();

        if let Some(registry_name) = registry {
            let (cfg, drv) = self.choose_registry(Some(registry_name))?;
            if drv
                .list(&exact)
                .await?
                .iter()
                .any(|b| b == &bag_ref.without_attachment())
            {
                matches.push((cfg.name.clone(), bag_ref.without_attachment()));
            }
        } else {
            let mut names: Vec<_> = self.registries.keys().cloned().collect();
            names.sort();

            let mut join_set = tokio::task::JoinSet::new();
            for name in names {
                if let Some((_, drv)) = self.registries.get(&name) {
                    let drv = Arc::clone(drv);
                    let exact = exact.clone();
                    let bag_ref_clone = bag_ref.without_attachment();
                    join_set.spawn(async move {
                        if drv
                            .list(&exact)
                            .await
                            .ok()
                            .is_some_and(|list| list.iter().any(|b| b == &bag_ref_clone))
                        {
                            Some((name, bag_ref_clone))
                        } else {
                            None
                        }
                    });
                }
            }

            while let Some(Ok(result)) = join_set.join_next().await {
                if let Some(hit) = result {
                    matches.push(hit);
                }
            }
        }
        matches.sort_by_key(|(name, _)| name.clone());

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
            let root = cache::bag_cache_dir(&bag.without_attachment())?;
            // Only delete local_dir if it lives inside the marina cache; pushed bags
            // point at the original source directory and must not be deleted.
            if entry.local_dir.starts_with(&root) && entry.local_dir.exists() {
                fs::remove_dir_all(&entry.local_dir)?;
            }
            if root.exists() {
                fs::remove_dir_all(root)?;
            }
            cache::save_catalog(&self.catalog)?;
        }
        Ok(())
    }

    /// Removes one bag from a remote registry.
    pub async fn remove_remote(
        &self,
        bag: &BagRef,
        registry: Option<&str>,
        write_http_index: bool,
    ) -> Result<()> {
        let (cfg, driver) = self.choose_registry(registry)?;
        Self::ensure_auth(&cfg, true)?;
        driver.remove(&bag.without_attachment()).await?;
        if write_http_index || cfg.kind == "ssh" {
            driver.write_http_index().await?;
        }
        Ok(())
    }

    /// Checks whether the caller has write (delete) access to a registry.
    /// Returns the resolved registry name on success.
    pub async fn check_write_access(&self, registry: Option<&str>) -> Result<String> {
        let (cfg, driver) = self.choose_registry(registry)?;
        Self::ensure_auth(&cfg, true)?;
        driver.check_write_access().await?;
        Ok(cfg.name.clone())
    }

    /// Searches one registry using glob-like `pattern`.
    pub async fn search_remote(
        &self,
        pattern: &str,
        registry: Option<&str>,
    ) -> Result<Vec<BagRef>> {
        let (_cfg, driver) = self.choose_registry(registry)?;
        driver.list(pattern).await
    }

    /// Registers a local bag in the catalog without pushing to a registry.
    ///
    /// If `path` is `Some`, the bag is copied into the marina cache so that
    /// `local_dir` is always under our control.
    /// If `path` is `None`, a new cache directory is prepared and returned so
    /// the caller (e.g. `ros2 bag record`) can record directly into it.
    pub fn import_local(
        &mut self,
        bag: &BagRef,
        path: Option<&Path>,
        move_to_cache: bool,
    ) -> Result<PathBuf> {
        let bag = bag.without_attachment();
        let key = bag.to_string();

        let cache_dir = cache::bag_cache_dir(&bag)?;
        let ready_dir = cache_dir.join("ready");

        if let Some(p) = path {
            if !p.exists() {
                return Err(anyhow!("path does not exist: {}", p.display()));
            }
            let canonical = p.canonicalize()?;
            if canonical != ready_dir.canonicalize().unwrap_or_default() {
                if ready_dir.exists() {
                    fs::remove_dir_all(&ready_dir)?;
                }
                if move_to_cache {
                    move_or_copy(&canonical, &ready_dir)?;
                } else {
                    copy_dir(&canonical, &ready_dir)?;
                }
            }
        }

        self.catalog.entries.insert(
            key,
            cache::CacheEntry {
                bag,
                local_dir: ready_dir.clone(),
                packed_bytes: 0,
                bundle_hash: None,
            },
        );
        cache::save_catalog(&self.catalog)?;
        Ok(ready_dir)
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
                let original_bytes = disk_size(&entry.local_dir);
                format!(
                    "{}: original {} bytes, packed {} bytes",
                    bag.without_attachment(),
                    original_bytes,
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
                original_bytes: disk_size(&entry.local_dir),
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
                original_bytes: disk_size(&entry.local_dir),
            })
            .collect::<Vec<_>>();
        out.sort_by_key(|a| a.bag.to_string());
        out
    }

    /// Searches all registries and returns tagged hits with registry names.
    /// Queries all registries concurrently.
    pub async fn search_all_remotes(&self, pattern: &str) -> Vec<RemoteBagHit> {
        let mut names = self.registries.keys().cloned().collect::<Vec<_>>();
        names.sort();

        let mut join_set = tokio::task::JoinSet::new();
        for name in names {
            if let Some((_, driver)) = self.registries.get(&name) {
                let driver = Arc::clone(driver);
                let pattern = pattern.to_string();
                let name = name.clone();
                join_set.spawn(async move {
                    driver
                        .list(&pattern)
                        .await
                        .ok()
                        .unwrap_or_default()
                        .into_iter()
                        .map(|bag| RemoteBagHit {
                            registry: name.clone(),
                            bag,
                        })
                        .collect::<Vec<_>>()
                });
            }
        }

        let mut hits: Vec<RemoteBagHit> = Vec::new();
        while let Some(Ok(chunk)) = join_set.join_next().await {
            hits.extend(chunk);
        }

        hits.sort_by(|a, b| {
            a.registry
                .cmp(&b.registry)
                .then_with(|| a.bag.to_string().cmp(&b.bag.to_string()))
        });
        hits
    }

    /// Fetch lightweight metadata for a bag in a specific registry.
    pub async fn bag_info(&self, registry: &str, bag: &BagRef) -> Option<BagInfo> {
        if let Some((_, drv)) = self.registries.get(registry) {
            drv.bag_info(bag).await.ok().flatten()
        } else {
            None
        }
    }

    /// List all bags across all registries with their stored metadata.
    /// Queries all registries concurrently.
    pub async fn list_all_remotes_with_info(&self) -> Vec<(RemoteBagHit, Option<BagInfo>)> {
        let mut names = self.registries.keys().cloned().collect::<Vec<_>>();
        names.sort();

        let mut join_set = tokio::task::JoinSet::new();
        for name in names {
            if let Some((_, driver)) = self.registries.get(&name) {
                let driver = Arc::clone(driver);
                let name = name.clone();
                join_set.spawn(async move {
                    driver
                        .list_with_info("*")
                        .await
                        .ok()
                        .unwrap_or_default()
                        .into_iter()
                        .map(|(bag, info)| {
                            (
                                RemoteBagHit {
                                    registry: name.clone(),
                                    bag,
                                },
                                info,
                            )
                        })
                        .collect::<Vec<_>>()
                });
            }
        }

        let mut result: Vec<(RemoteBagHit, Option<BagInfo>)> = Vec::new();
        while let Some(Ok(chunk)) = join_set.join_next().await {
            result.extend(chunk);
        }

        result.sort_by(|(a, _), (b, _)| {
            a.registry
                .cmp(&b.registry)
                .then_with(|| a.bag.to_string().cmp(&b.bag.to_string()))
        });
        result
    }

    /// Mirror all bags from `source` registry into `target` registry.
    pub async fn mirror_registry(
        &self,
        source_name: &str,
        target_name: &str,
        progress: &mut ProgressReporter<'_>,
    ) -> Result<MirrorStats> {
        let source_drv = self
            .registries
            .get(source_name)
            .ok_or_else(|| anyhow!("source registry '{}' not found", source_name))?
            .1
            .clone();
        let target_drv = self
            .registries
            .get(target_name)
            .ok_or_else(|| anyhow!("target registry '{}' not found", target_name))?
            .1
            .clone();

        progress.emit(
            "mirror",
            format!("listing source registry '{}'", source_name),
        );
        let source_items = source_drv.list_with_info("*").await?;

        progress.emit(
            "mirror",
            format!("listing target registry '{}'", target_name),
        );
        let target_map: HashMap<String, String> = target_drv
            .list_with_info("*")
            .await?
            .into_iter()
            .map(|(bag, info)| {
                let hash = info
                    .and_then(|i| i.bundle_hash)
                    .ok_or_else(|| anyhow!("target bag '{}' has no hash", bag))?;
                Ok((bag.to_string(), hash))
            })
            .collect::<Result<_>>()?;

        let mut stats = MirrorStats::default();

        for (bag, source_info) in source_items {
            let source_info =
                source_info.ok_or_else(|| anyhow!("source bag '{}' has no metadata", bag))?;
            let source_hash = source_info
                .bundle_hash
                .ok_or_else(|| anyhow!("source bag '{}' has no hash", bag))?;
            let bag_key = bag.to_string();

            if let Some(target_hash) = target_map.get(&bag_key) {
                if *target_hash == source_hash {
                    progress.emit("mirror", format!("skip {} (up to date)", bag));
                    stats.skipped += 1;
                    continue;
                }
                progress.emit("mirror", format!("update {} (hash changed)", bag));
            } else {
                progress.emit("mirror", format!("push {} (not in target)", bag));
            }

            let tmp_dir = tempfile::tempdir().context("failed to create temp dir for mirror")?;
            let tmp_bundle = tmp_dir.path().join("bundle.marina");

            progress.emit("mirror", format!("downloading from '{}'", source_name));
            source_drv
                .pull(&bag, &tmp_bundle)
                .await
                .with_context(|| format!("failed to pull '{}' from '{}'", bag, source_name))?;

            let bundle_hash = compute_bundle_hash(&tmp_bundle)?;
            let packed_bytes = fs::metadata(&tmp_bundle)?.len();

            let push_meta = PushMeta {
                original_bytes: source_info.original_bytes,
                packed_bytes,
                bundle_hash,
                pointcloud: source_info.pointcloud.unwrap_or_default(),
                mcap_compression: source_info.mcap_compression.unwrap_or_default(),
                pushed_at: now_unix_secs(),
            };

            progress.emit("mirror", format!("uploading {} to '{}'", bag, target_name));
            target_drv
                .push(target_name, &bag, &tmp_bundle, &push_meta)
                .await
                .with_context(|| format!("failed to push '{}' to '{}'", bag, target_name))?;

            if target_map.contains_key(&bag_key) {
                stats.updated += 1;
            } else {
                stats.pushed += 1;
            }
        }

        Ok(stats)
    }

    /// List bags in a specific registry with their stored metadata.
    pub async fn search_remote_with_info(
        &self,
        registry: &str,
        pattern: &str,
    ) -> Vec<(BagRef, Option<BagInfo>)> {
        if let Some((_, drv)) = self.registries.get(registry) {
            drv.list_with_info(pattern).await.ok().unwrap_or_default()
        } else {
            Vec::new()
        }
    }

    /// Inspect a dataset: collect local file listing and remote metadata.
    ///
    /// `registry` scopes the remote lookup to a single registry when provided.
    pub async fn inspect_bag(
        &self,
        target: &str,
        registry: Option<&str>,
        timeout_secs: u64,
    ) -> Result<InspectResult> {
        let bag_ref: BagRef = target.parse()?;
        let key = bag_ref.without_attachment().to_string();

        // Local files.
        let (local_dir, local_files) = if let Some(entry) = self.catalog.entries.get(&key) {
            if entry.local_dir.exists() {
                let source = crate::io::bag::discover_bag(&entry.local_dir).ok();
                let files = if let Some(src) = source {
                    let root = &src.root;
                    let mut files: Vec<InspectFile> = Vec::new();

                    // recording file
                    if let Some(ref mcap) = src.mcap {
                        let size = fs::metadata(mcap).map(|m| m.len()).unwrap_or(0);
                        let rel = mcap
                            .strip_prefix(root)
                            .ok()
                            .map(|p| p.to_string_lossy().into_owned())
                            .unwrap_or_else(|| mcap.to_string_lossy().into_owned());
                        files.push(InspectFile {
                            relative_path: rel,
                            size_bytes: size,
                        });
                    }

                    // attachments
                    for att in &src.attachments {
                        let size = fs::metadata(att).map(|m| m.len()).unwrap_or(0);
                        let rel = att
                            .strip_prefix(root)
                            .ok()
                            .map(|p| p.to_string_lossy().into_owned())
                            .unwrap_or_else(|| att.to_string_lossy().into_owned());
                        files.push(InspectFile {
                            relative_path: rel,
                            size_bytes: size,
                        });
                    }

                    files.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
                    files
                } else {
                    Vec::new()
                };
                (Some(entry.local_dir.clone()), files)
            } else {
                (None, Vec::new())
            }
        } else {
            (None, Vec::new())
        };

        // Remote metadata.
        let bag_no_att = bag_ref.without_attachment();
        let names: Vec<String> = match registry {
            Some(r) => vec![r.to_string()],
            None => {
                let mut n = self.registries.keys().cloned().collect::<Vec<_>>();
                n.sort();
                n
            }
        };

        let timeout = std::time::Duration::from_secs(timeout_secs);
        let mut join_set = tokio::task::JoinSet::new();
        for name in names {
            if let Some((_, drv)) = self.registries.get(&name) {
                let drv = Arc::clone(drv);
                let bag = bag_no_att.clone();
                let name = name.clone();
                join_set.spawn(async move {
                    match tokio::time::timeout(timeout, drv.bag_info(&bag)).await {
                        Ok(r) => InspectRemoteHit {
                            registry: name,
                            info: r.ok().flatten(),
                            timed_out: false,
                        },
                        Err(_) => InspectRemoteHit {
                            registry: name,
                            info: None,
                            timed_out: true,
                        },
                    }
                });
            }
        }

        let mut remote_hits: Vec<InspectRemoteHit> = Vec::new();
        while let Some(Ok(hit)) = join_set.join_next().await {
            remote_hits.push(hit);
        }
        remote_hits.sort_by(|a, b| a.registry.cmp(&b.registry));

        Ok(InspectResult {
            bag: bag_no_att,
            local_dir,
            local_files,
            remote_hits,
        })
    }
}

fn validate_registry_name(name: &str) -> Result<()> {
    let valid = !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !valid {
        let suggestion = name.replace('-', "_");
        return Err(anyhow!(
            "registry name '{}' is invalid: the config file format only supports letters, digits, \
            and underscores in identifiers{}",
            name,
            if suggestion != name {
                format!(" — try '{}'", suggestion)
            } else {
                String::new()
            }
        ));
    }
    Ok(())
}

fn make_registry_driver(registry: &RegistryConfig) -> Result<Arc<dyn RegistryDriver>> {
    let driver: Arc<dyn RegistryDriver> = match registry.kind.as_str() {
        "folder" | "directory" => {
            Arc::new(FolderRegistry::from_uri(&registry.name, &registry.uri)?)
        }
        "ssh" => Arc::new(SshRegistry::from_uri(
            &registry.name,
            &registry.uri,
            registry.auth_env.clone(),
        )?),
        "http" => Arc::new(HttpRegistry::from_uri(&registry.name, &registry.uri)?),
        "gdrive" => {
            #[cfg(feature = "gdrive")]
            {
                Arc::new(GDriveRegistry::from_uri(
                    &registry.name,
                    &registry.uri,
                    registry.auth_env.clone(),
                )?)
            }
            #[cfg(not(feature = "gdrive"))]
            {
                Arc::new(StubRegistry::new(
                    "gdrive",
                    &registry.uri,
                    registry.auth_env.clone(),
                ))
            }
        }
        other => Arc::new(StubRegistry::new(
            other,
            &registry.uri,
            registry.auth_env.clone(),
        )),
    };
    Ok(driver)
}

/// Scans `path` on disk and returns the total byte count of all files found.
/// Returns 0 if the path does not exist or cannot be read.
fn disk_size(path: &Path) -> u64 {
    crate::io::bag::discover_bag(path)
        .map(|s| s.original_bytes)
        .unwrap_or(0)
}

fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
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

fn format_precision(precision_m: f64) -> String {
    let fmt = |v: f64, unit: &str| -> String {
        if v.fract() == 0.0 {
            format!("{}{}", v as u64, unit)
        } else {
            format!("{:.3}{}", v, unit)
                .trim_end_matches('0')
                .trim_end_matches('.')
                .to_string()
                + unit
        }
    };
    if precision_m >= 1.0 {
        fmt(precision_m, "m")
    } else if precision_m >= 0.001 {
        fmt(precision_m * 1_000.0, "mm")
    } else {
        fmt(precision_m * 1_000_000.0, "µm")
    }
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

/// Tries a cheap `rename` first; falls back to copy+delete on cross-device errors.
fn move_or_copy(src: &Path, dst: &Path) -> Result<()> {
    if fs::rename(src, dst).is_ok() {
        return Ok(());
    }
    copy_dir(src, dst)?;
    fs::remove_dir_all(src)?;
    Ok(())
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
