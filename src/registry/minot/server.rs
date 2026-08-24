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
    PROTOCOL_VERSION, Request, Response, WireBagInfo, WireBagRef, WireFile, service_topic,
    version_mismatch,
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
}

impl ServeOptions {
    pub fn new(registry: impl Into<String>) -> Self {
        Self {
            registry: registry.into(),
            local_only: true,
            flow: FlowConfig::default(),
            materialize_root: None,
            max_range_bytes: 8 * 1024 * 1024,
        }
    }
}

/// Serve `driver` until the process is stopped.
pub async fn serve(driver: Arc<dyn RegistryDriver>, options: ServeOptions) -> Result<()> {
    if options.local_only {
        mt_sea::network::set_local_only(true);
    } else {
        log::warn!(
            "marina serve is not restricted to this machine. Minot provides no \
             authentication, so anything that can reach this port can read the \
             '{}' registry. Prefer an SSH tunnel to a loopback-bound server.",
            options.registry
        );
    }

    driver
        .check_connection()
        .await
        .context("the registry being served is not reachable")?;

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
        materialize_lock: tokio::sync::Mutex::new(()),
        materializing: tokio::sync::Mutex::new(HashSet::new()),
        materialize_errors: tokio::sync::Mutex::new(HashMap::new()),
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
    /// Serialises the disk-heavy restore itself.
    materialize_lock: tokio::sync::Mutex<()>,
    /// Dataset keys currently restoring, so every poll returns immediately and
    /// only the first one starts work.
    materializing: tokio::sync::Mutex<HashSet<String>>,
    /// A background failure is returned by the next poll instead of leaving a
    /// client waiting forever.
    materialize_errors: tokio::sync::Mutex<HashMap<String, String>>,
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
    }
}
