//! `marina serve` — expose a local registry over a Minot network.
//!
//! The server is a thin adapter, deliberately. It holds an ordinary
//! [`RegistryDriver`] — a folder, an SSH registry, anything already configured —
//! and answers requests by delegating to it. That is what makes a `minot://`
//! registry behave exactly like the registry behind it rather than being a
//! second implementation that drifts.
//!
//! # Binding
//!
//! Minot is given no authentication of its own, so this binds **loopback only**
//! and expects to be reached through an SSH tunnel. See the streaming plan's
//! auth section: the client forwards a local port to the server's loopback port
//! using the credentials the SSH registry already manages, so users configure
//! them once.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use mt_flow::{BytesSource, FlowConfig, FlowSender};
use mt_pubsub::{CoordMode, Node, NodeConfig, Qos};
use mt_service::ServiceServer;

use crate::registry::driver::RegistryDriver;
use crate::registry::minot::protocol::{
    PROTOCOL_VERSION, Request, Response, WireBagInfo, WireBagRef, WireFile, WireManifestFile,
    service_topic, version_mismatch,
};

/// How a served registry is reached and what it is allowed to do.
pub struct ServeOptions {
    /// Name clients use to address this registry. Also namespaces the topics.
    pub registry: String,
    /// Restrict Minot to this machine. True unless you have arranged transport
    /// security yourself — see the module docs.
    pub local_only: bool,
    /// Chunking and window sizing for bundle transfers.
    pub flow: FlowConfig,
    /// Where datasets are unpacked so their bytes can be served by range.
    ///
    /// Defaults to a `serve/` directory under the marina cache. Materialising
    /// here rather than in the ordinary cache keeps a server's working set
    /// separate from whatever the same machine pulled for its own use.
    pub materialize_root: Option<std::path::PathBuf>,
    /// Largest range a client may ask for in one request, as a guard against a
    /// misbehaving or hostile client asking for a gigabyte.
    pub max_range_bytes: u32,
    /// Accept staged dataset uploads. Off by default because Minot itself does
    /// not authenticate clients; SSH is the intended authorization boundary.
    pub allow_write: bool,
}

impl ServeOptions {
    pub fn new(registry: impl Into<String>) -> Self {
        Self {
            registry: registry.into(),
            local_only: true,
            flow: FlowConfig::default(),
            materialize_root: None,
            max_range_bytes: 8 * 1024 * 1024,
            allow_write: false,
        }
    }
}

/// Serve `driver` until the process is stopped.
pub async fn serve(driver: Arc<dyn RegistryDriver>, options: ServeOptions) -> Result<()> {
    if options.local_only {
        mt_sea::network::set_local_only(true);
    } else {
        // Direct clients address this server explicitly. Multicast discovery
        // would merge it with unrelated Minot runs on the same LAN.
        mt_sea::network::set_unicast_only(true);
        log::warn!(
            "marina serve is not restricted to this machine. Minot provides no \
             authentication, so anything that can reach this port can {} the \
             '{}' registry. Prefer an SSH tunnel to a loopback-bound server.",
            if options.allow_write {
                "read and write"
            } else {
                "read"
            },
            options.registry
        );
    }

    driver
        .check_connection()
        .await
        .context("the registry being served is not reachable")?;
    if options.allow_write {
        driver
            .check_write_access()
            .await
            .context("the registry being served is not writable")?;
    }

    // TryReliable, never Reliable: a client that dies must not torpedo the
    // server, and the server must survive its own coordinator restarting.
    let node = Arc::new(
        Node::create(
            NodeConfig::new(format!("marina_serve_{}", options.registry))
                .mode(Qos::TryReliable)
                .coord_mode(CoordMode::Start)
                .wan(),
        )
        .await
        .context("could not join the Minot network")?,
    );

    let topic = service_topic(&options.registry);
    let server = ServiceServer::<Request, Response>::new(Arc::clone(&node), topic.clone())
        .await
        .with_context(|| format!("could not listen on '{topic}'"))?;

    log::info!(
        "marina serve: exposing registry '{}' on '{}' (protocol {}.{})",
        options.registry,
        topic,
        PROTOCOL_VERSION.0,
        PROTOCOL_VERSION.1
    );

    let materialize_root = match options.materialize_root {
        Some(root) => root,
        None => crate::storage::config::cache_dir()
            .context("could not locate the cache directory")?
            .join("serve"),
    };
    std::fs::create_dir_all(&materialize_root).with_context(|| {
        format!(
            "could not create the serving directory {}",
            materialize_root.display()
        )
    })?;

    let handler = Arc::new(RequestHandler {
        driver,
        node: Arc::clone(&node),
        registry: options.registry.clone(),
        flow: options.flow,
        materialize_root,
        max_range_bytes: options.max_range_bytes,
        allow_write: options.allow_write,
        materialize_lock: tokio::sync::Mutex::new(()),
        materializing: tokio::sync::Mutex::new(HashSet::new()),
        materialize_errors: tokio::sync::Mutex::new(HashMap::new()),
        write_lock: tokio::sync::Mutex::new(()),
    });

    ServiceServer::start(
        server,
        Arc::new(move |request| {
            let handler = Arc::clone(&handler);
            async move { handler.handle(request).await }
        }),
    )
    .await;

    Ok(())
}

struct RequestHandler {
    driver: Arc<dyn RegistryDriver>,
    node: Arc<Node>,
    registry: String,
    flow: FlowConfig,
    materialize_root: std::path::PathBuf,
    max_range_bytes: u32,
    allow_write: bool,
    /// Serialises the disk-heavy restore itself.
    materialize_lock: tokio::sync::Mutex<()>,
    /// Dataset keys currently restoring, so every poll returns immediately and
    /// only the first one starts work.
    materializing: tokio::sync::Mutex<HashSet<String>>,
    /// A background failure is returned by the next poll instead of leaving a
    /// client waiting forever.
    materialize_errors: tokio::sync::Mutex<HashMap<String, String>>,
    /// Serialises staging mutations and commits. Upload traffic is already
    /// sequential per client; this also prevents two clients racing one tag.
    write_lock: tokio::sync::Mutex<()>,
}

impl RequestHandler {
    async fn handle(self: &Arc<Self>, request: Request) -> Result<Response, String> {
        match request {
            Request::Hello { major, minor } => {
                if let Some(reason) = version_mismatch(major, minor) {
                    log::warn!("marina serve: refusing a client — {reason}");
                    return Err(reason);
                }
                Ok(Response::Hello {
                    major: PROTOCOL_VERSION.0,
                    minor: PROTOCOL_VERSION.1,
                    registry: self.registry.clone(),
                })
            }

            Request::List { pattern } => {
                let bags = self
                    .driver
                    .list(&pattern)
                    .await
                    .map_err(|error| format!("list failed: {error}"))?;
                Ok(Response::List(
                    bags.iter().map(WireBagRef::from).collect::<Vec<_>>(),
                ))
            }

            Request::BagInfo { bag } => {
                let bag = bag.into();
                let info = self
                    .driver
                    .bag_info(&bag)
                    .await
                    .map_err(|error| format!("bag_info failed: {error}"))?;
                Ok(Response::BagInfo(info.as_ref().map(WireBagInfo::from)))
            }

            Request::PullBegin { bag } => self.begin_pull(bag.into()).await,

            Request::Stat { bag } => self.stat(bag.into()).await,

            Request::ReadRange {
                bag,
                path,
                offset,
                len,
            } => self.read_range(bag.into(), &path, offset, len).await,

            Request::BeginWrite { bag } => self.begin_write(bag.into()).await,

            Request::WriteRange {
                bag,
                path,
                offset,
                data,
            } => self.write_range(bag.into(), &path, offset, data).await,

            Request::CommitWrite { bag, files } => self.commit_write(bag.into(), files).await,
        }
    }

    /// Where a dataset is unpacked on this server.
    fn dataset_dir(&self, bag: &crate::model::bag_ref::BagRef) -> std::path::PathBuf {
        self.materialize_root
            .join(bag.without_attachment().cache_key())
    }

    /// Ensure the dataset is unpacked here, pulling it from the backing
    /// registry the first time.
    async fn materialize(
        &self,
        bag: &crate::model::bag_ref::BagRef,
    ) -> Result<std::path::PathBuf, String> {
        let ready = self.dataset_dir(bag).join("ready");
        if ready.is_dir() {
            return Ok(ready);
        }

        let _guard = self.materialize_lock.lock().await;
        // Checked again under the lock: another request may have done it while
        // this one waited.
        if ready.is_dir() {
            return Ok(ready);
        }

        log::info!("marina serve: materialising '{bag}' for range reads");
        let staging = tempfile::Builder::new()
            .prefix("marina-materialize-")
            .suffix(".tar.gz")
            .tempfile()
            .map_err(|error| format!("could not stage '{bag}': {error}"))?;
        self.driver
            .pull(bag, staging.path())
            .await
            .map_err(|error| format!("could not read '{bag}' from the served registry: {error}"))?;

        // Unpacked beside the destination and renamed, so an interrupted
        // materialisation never leaves a half-written tree that looks ready.
        let parent = self.dataset_dir(bag);
        let incoming = parent.join(".incoming");
        if incoming.exists() {
            let _ = std::fs::remove_dir_all(&incoming);
        }
        std::fs::create_dir_all(&incoming)
            .map_err(|error| format!("could not prepare '{bag}': {error}"))?;

        let staging_path = staging.path().to_path_buf();
        let incoming_for_task = incoming.clone();
        // Unpacking is CPU- and disk-bound and entirely synchronous; keeping it
        // off the async worker leaves the server able to answer other clients.
        tokio::task::spawn_blocking(move || {
            crate::io::pack::unpack_bag(&staging_path, &incoming_for_task)
        })
        .await
        .map_err(|error| format!("unpacking '{bag}' panicked: {error}"))?
        .map_err(|error| format!("could not unpack '{bag}': {error}"))?;

        std::fs::rename(&incoming, &ready)
            .map_err(|error| format!("could not install '{bag}': {error}"))?;
        Ok(ready)
    }

    async fn stat(
        self: &Arc<Self>,
        bag: crate::model::bag_ref::BagRef,
    ) -> Result<Response, String> {
        let ready = self.dataset_dir(&bag).join("ready");
        if !ready.is_dir() {
            let key = bag.without_attachment().to_string();
            if let Some(error) = self.materialize_errors.lock().await.get(&key).cloned() {
                return Err(format!("materialising '{bag}' failed: {error}"));
            }

            let should_start = self.materializing.lock().await.insert(key.clone());
            if should_start {
                let handler = Arc::clone(self);
                let bag_for_task = bag.clone();
                tokio::spawn(async move {
                    if let Err(error) = handler.materialize(&bag_for_task).await {
                        log::error!("marina serve: materialising '{bag_for_task}' failed: {error}");
                        handler
                            .materialize_errors
                            .lock()
                            .await
                            .insert(key.clone(), error);
                    }
                    handler.materializing.lock().await.remove(&key);
                });
            }

            return Ok(Response::Materializing {
                message: if should_start {
                    format!("restoring '{bag}' on the server")
                } else {
                    format!("still restoring '{bag}' on the server")
                },
            });
        }

        let mut files = Vec::new();
        for entry in walkdir::WalkDir::new(&ready).follow_links(false) {
            let entry = entry.map_err(|error| format!("could not list '{bag}': {error}"))?;
            if !entry.file_type().is_file() {
                continue;
            }
            let relative = entry
                .path()
                .strip_prefix(&ready)
                .map_err(|error| format!("could not list '{bag}': {error}"))?
                .to_string_lossy()
                .replace('\\', "/");
            let size = entry
                .metadata()
                .map_err(|error| format!("could not stat '{relative}': {error}"))?
                .len();
            files.push(WireFile {
                path: relative,
                size,
            });
        }
        files.sort_by(|a, b| a.path.cmp(&b.path));

        // A sqlite3 bag needs a real file for rusqlite to open, so it cannot be
        // read by range. Say so plainly and let the client fall back to a pull
        // rather than failing halfway through a read.
        let has_db3 = files.iter().any(|file| file.path.ends_with(".db3"));
        let (streamable, reason) = if has_db3 {
            (
                false,
                Some(
                    "this dataset is a sqlite3 bag, which cannot be read by byte range;                      pull it instead"
                        .to_string(),
                ),
            )
        } else {
            (true, None)
        };

        Ok(Response::Stat {
            files,
            streamable,
            reason,
        })
    }

    async fn read_range(
        &self,
        bag: crate::model::bag_ref::BagRef,
        path: &str,
        offset: u64,
        len: u32,
    ) -> Result<Response, String> {
        if len > self.max_range_bytes {
            return Err(format!(
                "requested range of {len} bytes exceeds this server's limit of {}",
                self.max_range_bytes
            ));
        }
        let ready = self.materialize(&bag).await?;
        let target = safe_join(&ready, path)?;

        let data = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<u8>> {
            use std::io::{Read, Seek, SeekFrom};
            let mut file = std::fs::File::open(&target)?;
            let size = file.metadata()?.len();
            if offset >= size {
                return Ok(Vec::new());
            }
            let readable = (size - offset).min(len as u64) as usize;
            let mut buffer = vec![0u8; readable];
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(&mut buffer)?;
            Ok(buffer)
        })
        .await
        .map_err(|error| format!("reading '{path}' panicked: {error}"))?
        .map_err(|error| format!("could not read '{path}': {error}"))?;

        Ok(Response::ReadRange { data })
    }

    fn ensure_writes_enabled(&self) -> Result<(), String> {
        if self.allow_write {
            Ok(())
        } else {
            Err("this marina server is read-only; restart it with --allow-write".to_string())
        }
    }

    fn write_staging_dir(&self, bag: &crate::model::bag_ref::BagRef) -> std::path::PathBuf {
        self.dataset_dir(bag).join(".write-incoming")
    }

    async fn begin_write(&self, bag: crate::model::bag_ref::BagRef) -> Result<Response, String> {
        self.ensure_writes_enabled()?;
        let _guard = self.write_lock.lock().await;
        let staging = self.write_staging_dir(&bag);
        std::fs::create_dir_all(&staging)
            .map_err(|error| format!("could not stage '{bag}': {error}"))?;
        let files = list_file_sizes(&staging)
            .map_err(|error| format!("could not inspect staged upload '{bag}': {error}"))?;
        Ok(Response::WriteStatus { files })
    }

    async fn write_range(
        &self,
        bag: crate::model::bag_ref::BagRef,
        path: &str,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<Response, String> {
        use std::io::{Read, Seek, SeekFrom, Write};

        self.ensure_writes_enabled()?;
        if data.len() > self.max_range_bytes as usize {
            return Err(format!(
                "upload range of {} bytes exceeds this server's limit of {}",
                data.len(),
                self.max_range_bytes
            ));
        }
        let _guard = self.write_lock.lock().await;
        let staging = self.write_staging_dir(&bag);
        std::fs::create_dir_all(&staging)
            .map_err(|error| format!("could not stage '{bag}': {error}"))?;
        let target = safe_join(&staging, path)?;
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| format!("could not create upload directory: {error}"))?;
        }
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&target)
            .map_err(|error| format!("could not open staged file '{path}': {error}"))?;
        let current = file
            .metadata()
            .map_err(|error| format!("could not inspect staged file '{path}': {error}"))?
            .len();

        // A client may retry after losing the acknowledgement. Verify the
        // overlap, then append only bytes the server does not already have.
        if offset < current {
            let overlap = (current - offset).min(data.len() as u64) as usize;
            let mut existing = vec![0u8; overlap];
            file.seek(SeekFrom::Start(offset))
                .and_then(|_| file.read_exact(&mut existing))
                .map_err(|error| {
                    format!("could not verify repeated range for '{path}': {error}")
                })?;
            if existing != data[..overlap] {
                return Err(format!(
                    "staged file '{path}' differs at offset {offset}; use a new Marina tag or clear the interrupted upload"
                ));
            }
            if overlap == data.len() {
                return Ok(Response::WriteAck {
                    next_offset: current,
                });
            }
            file.seek(SeekFrom::End(0))
                .and_then(|_| file.write_all(&data[overlap..]))
                .and_then(|_| file.flush())
                .map_err(|error| format!("could not append staged file '{path}': {error}"))?;
            return Ok(Response::WriteAck {
                next_offset: current + (data.len() - overlap) as u64,
            });
        }

        if offset > current {
            // The watermark is authoritative. Returning it lets a resumed
            // client repair the missing prefix without creating a sparse hole.
            return Ok(Response::WriteAck {
                next_offset: current,
            });
        }
        file.seek(SeekFrom::End(0))
            .and_then(|_| file.write_all(&data))
            .and_then(|_| file.flush())
            .map_err(|error| format!("could not append staged file '{path}': {error}"))?;
        Ok(Response::WriteAck {
            next_offset: current + data.len() as u64,
        })
    }

    async fn commit_write(
        &self,
        bag: crate::model::bag_ref::BagRef,
        files: Vec<WireManifestFile>,
    ) -> Result<Response, String> {
        use crate::io::mcap_transform::{McapChunkCompression, PointCloudCompressionMode};
        use crate::io::pack::{ArchiveCompression, PackOptions};
        use crate::registry::driver::PushMeta;
        use crate::storage::cache::MirrorFile;
        use sha2::{Digest, Sha256};

        self.ensure_writes_enabled()?;
        let _guard = self.write_lock.lock().await;
        let parent = self.dataset_dir(&bag);
        let staging = self.write_staging_dir(&bag);
        let ready = parent.join("ready");
        let backup = parent.join(".write-previous");
        let mut expected = files
            .into_iter()
            .map(|file| MirrorFile {
                path: file.path,
                size: file.size,
                sha256: file.sha256,
            })
            .collect::<Vec<_>>();
        expected.sort_by(|left, right| left.path.cmp(&right.path));

        let source_dir = if staging.is_dir() {
            staging.clone()
        } else if ready.is_dir() {
            ready.clone()
        } else {
            return Err(format!("no staged upload exists for '{bag}'"));
        };
        let actual = crate::storage::cache::mirror_manifest(&source_dir)
            .map_err(|error| format!("could not verify staged upload '{bag}': {error}"))?;
        if actual != expected {
            return Err(format!(
                "cannot commit '{bag}': staged files do not match the completed local bag"
            ));
        }
        crate::io::bag::discover_bag(&source_dir)
            .map_err(|error| format!("cannot commit invalid ROS bag '{bag}': {error}"))?;

        if source_dir == staging {
            if backup.exists() {
                std::fs::remove_dir_all(&backup)
                    .map_err(|error| format!("could not clear old '{bag}' backup: {error}"))?;
            }
            if ready.exists() {
                std::fs::rename(&ready, &backup)
                    .map_err(|error| format!("could not preserve old '{bag}': {error}"))?;
            }
            if let Err(error) = std::fs::rename(&staging, &ready) {
                if backup.exists() {
                    let _ = std::fs::rename(&backup, &ready);
                }
                return Err(format!("could not publish staged '{bag}': {error}"));
            }
        }

        // The served registry still stores its ordinary packed object. Build
        // it losslessly from the atomically installed native bag, then delegate
        // the final write to the backing registry driver.
        let packed = parent.join(".write-bundle.marina.tar.gz");
        let ready_for_pack = ready.clone();
        let packed_for_task = packed.clone();
        let packed_meta = tokio::task::spawn_blocking(move || {
            let source = crate::io::bag::discover_bag(&ready_for_pack)?;
            let mut progress = crate::ProgressReporter::silent();
            crate::io::pack::pack_bag_with_progress_and_options(
                &source,
                &packed_for_task,
                PackOptions {
                    transform: crate::io::mcap_transform::PushTransformOptions {
                        pointcloud_mode: PointCloudCompressionMode::Disabled,
                        pointcloud_precision_m: 0.001,
                        output_mcap_compression: McapChunkCompression::None,
                    },
                    archive_compression: ArchiveCompression::Gzip,
                    db3_vacuum: false,
                },
                &mut progress,
            )
        })
        .await
        .map_err(|error| format!("packing '{bag}' panicked: {error}"))?
        .map_err(|error| format!("could not pack '{bag}': {error}"))?;
        let packed_bytes = std::fs::read(&packed)
            .map_err(|error| format!("could not hash packed '{bag}': {error}"))?;
        let bundle_hash = Sha256::digest(&packed_bytes)
            .iter()
            .take(6)
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let pushed_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.driver
            .push(
                &self.registry,
                &bag,
                &packed,
                &PushMeta {
                    original_bytes: packed_meta.original_bytes,
                    packed_bytes: packed_meta.packed_bytes,
                    bundle_hash,
                    pointcloud: "disabled".to_string(),
                    mcap_compression: "none".to_string(),
                    pushed_at,
                },
            )
            .await
            .map_err(|error| format!("could not publish '{bag}' to backing registry: {error}"))?;
        let _ = std::fs::remove_file(&packed);
        if backup.exists() {
            let _ = std::fs::remove_dir_all(&backup);
        }
        Ok(Response::WriteCommitted)
    }

    /// Fetch the bundle from the backing registry and start streaming it.
    ///
    /// The bundle is materialised into a temporary file first, because the
    /// backing driver's contract is "download to this path" and because the
    /// client is told the exact size up front so it can verify what it got.
    async fn begin_pull(&self, bag: crate::model::bag_ref::BagRef) -> Result<Response, String> {
        let staging = tempfile::Builder::new()
            .prefix("marina-serve-")
            .suffix(".tar.gz")
            .tempfile()
            .map_err(|error| format!("could not stage the bundle: {error}"))?;
        let staging_path = staging.path().to_path_buf();

        let descriptor = self
            .driver
            .pull(&bag, &staging_path)
            .await
            .map_err(|error| format!("could not read '{bag}' from the served registry: {error}"))?;

        let bytes = std::fs::read(&staging_path)
            .map_err(|error| format!("could not read the staged bundle: {error}"))?;
        let packed_bytes = bytes.len() as u64;

        // A flow name unique to this request, so concurrent pulls of the same
        // dataset do not land on the same topic.
        let flow = format!(
            "marina_{}_{}",
            self.registry,
            uuid_like(&bag.to_string(), packed_bytes)
        );

        let node = Arc::clone(&self.node);
        let config = self.flow;
        let flow_name = flow.clone();
        let label = bag.to_string();
        tokio::spawn(async move {
            // Held until the send finishes so the file outlives the transfer.
            let _staging = staging;
            let mut sender = match FlowSender::open(&node, &flow_name, config).await {
                Ok(sender) => sender,
                Err(error) => {
                    log::error!("marina serve: could not open flow for '{label}': {error}");
                    return;
                }
            };
            let mut source = BytesSource::new(bytes);
            match sender.send_all(&mut source).await {
                Ok(sent) => log::info!("marina serve: sent {sent} bytes of '{label}'"),
                Err(error) => log::error!("marina serve: sending '{label}' failed: {error}"),
            }
        });

        Ok(Response::PullBegin {
            flow,
            packed_bytes,
            original_bytes: descriptor.original_bytes,
        })
    }
}

/// Resolve a client-supplied relative path inside `root`, refusing anything
/// that escapes it.
///
/// The path comes off the network, so `../../etc/passwd` has to be impossible
/// rather than merely unlikely. Rejecting the components outright is clearer
/// than canonicalising and comparing prefixes, and does not depend on the file
/// existing.
fn safe_join(root: &std::path::Path, relative: &str) -> Result<std::path::PathBuf, String> {
    use std::path::Component;

    if relative.is_empty() {
        return Err("an empty path is not a file in this dataset".to_string());
    }
    let candidate = std::path::Path::new(relative);
    for component in candidate.components() {
        match component {
            Component::Normal(_) => {}
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(format!("'{relative}' is not a path inside the dataset"));
            }
        }
    }
    Ok(root.join(candidate))
}

fn list_file_sizes(root: &std::path::Path) -> anyhow::Result<Vec<WireFile>> {
    let mut files = Vec::new();
    for entry in walkdir::WalkDir::new(root).follow_links(false) {
        let entry = entry?;
        if entry.path() == root || entry.file_type().is_dir() {
            continue;
        }
        anyhow::ensure!(
            !entry.file_type().is_symlink(),
            "symbolic links are not supported in streamed uploads"
        );
        files.push(WireFile {
            path: entry
                .path()
                .strip_prefix(root)?
                .to_string_lossy()
                .replace('\\', "/"),
            size: entry.metadata()?.len(),
        });
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

/// A short, collision-resistant-enough token for a flow name.
///
/// Not a real UUID: this only has to distinguish concurrent transfers within one
/// server, so the dataset name, its size, and the clock are plenty. Avoids
/// pulling in a uuid dependency for a naming detail.
fn uuid_like(seed: &str, size: u64) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    seed.hash(&mut hasher);
    size.hash(&mut hasher);
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flow_names_differ_between_concurrent_transfers() {
        let first = uuid_like("run:v1", 1024);
        let second = uuid_like("run:v1", 1024);
        assert_ne!(
            first, second,
            "two pulls of the same dataset must not share a flow topic"
        );
    }

    #[test]
    fn a_path_cannot_escape_the_dataset() {
        let root = std::path::Path::new("/srv/data");
        assert!(safe_join(root, "../../etc/passwd").is_err());
        assert!(safe_join(root, "nested/../../../etc/passwd").is_err());
        assert!(safe_join(root, "/etc/passwd").is_err());
        assert!(safe_join(root, "").is_err());
    }

    #[test]
    fn an_ordinary_path_resolves_inside_the_dataset() {
        let root = std::path::Path::new("/srv/data");
        assert_eq!(
            safe_join(root, "nested/run_0.mcap").unwrap(),
            root.join("nested/run_0.mcap")
        );
    }

    #[test]
    fn serve_options_default_to_this_machine_only() {
        let options = ServeOptions::new("team");
        assert!(
            options.local_only,
            "Minot has no authentication, so the safe default is loopback"
        );
        assert!(
            !options.allow_write,
            "remote writes must be explicitly enabled"
        );
    }
}
