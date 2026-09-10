//! Background jobs for the TUI.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context as _, Result};
use tokio::sync::mpsc::UnboundedSender;

use mt_dataset::core::{CacheMirrorOptions, Marina, PullOptions, PushOptions, ResolveResult};
use mt_dataset::model::bag_ref::BagRef;
use mt_dataset::progress::{ProgressEvent, ProgressReporter, ProgressSink};
use mt_dataset::registry::driver::BagInfo;
use mt_dataset::storage::config::{self, RegistryConfig};

pub type JobId = u64;

/// What a job touches, so a row can show a spinner while its own work runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobTarget {
    Registry(String),
    Dataset(String),
    Cache,
}

#[derive(Debug, Clone)]
pub enum JobKind {
    ListRemote {
        registry: String,
    },
    Search {
        registry: String,
        pattern: String,
    },
    Pull {
        target: String,
        registry: Option<String>,
        options: PullOptions,
    },
    Push {
        bag: BagRef,
        source: PathBuf,
        registry: Option<String>,
        options: Box<PushOptions>,
    },
    Import {
        bag: BagRef,
        path: Option<PathBuf>,
        move_to_cache: bool,
    },
    Export {
        bag: BagRef,
        output: PathBuf,
    },
    RemoveLocal {
        bag: BagRef,
    },
    RemoveRemote {
        bag: BagRef,
        registry: String,
        write_http_index: bool,
    },
    Clean {
        all: bool,
    },
    MirrorRegistry {
        source: String,
        target: String,
        patterns: Vec<String>,
    },
    MirrorCacheSsh {
        target: String,
        patterns: Vec<String>,
        options: Box<CacheMirrorOptions>,
    },
    RegistryAdd(Box<RegistryConfig>),
    RegistryRemove {
        name: String,
        delete_data: bool,
    },
    RegistryAuth {
        name: String,
    },
    Resolve {
        target: String,
        registry: Option<String>,
    },
}

impl JobKind {
    /// Short label shown in the jobs pane.
    pub fn label(&self) -> String {
        match self {
            Self::ListRemote { registry } => format!("list {registry}"),
            Self::Search { registry, pattern } => format!("search {pattern} in {registry}"),
            Self::Pull { target, .. } => format!("pull {target}"),
            Self::Push { bag, .. } => format!("push {bag}"),
            Self::Import { bag, .. } => format!("import {bag}"),
            Self::Export { bag, output } => format!("export {bag} -> {}", output.display()),
            Self::RemoveLocal { bag } => format!("rm local {bag}"),
            Self::RemoveRemote { bag, registry, .. } => format!("rm {bag} from {registry}"),
            Self::Clean { all } => {
                if *all {
                    "clean --all".to_string()
                } else {
                    "clean".to_string()
                }
            }
            Self::MirrorRegistry { source, target, .. } => format!("mirror {source} -> {target}"),
            Self::MirrorCacheSsh { target, .. } => format!("mirror cache -> {target}"),
            Self::RegistryAdd(cfg) => format!("registry add {}", cfg.name),
            Self::RegistryRemove { name, .. } => format!("registry rm {name}"),
            Self::RegistryAuth { name } => format!("registry auth {name}"),
            Self::Resolve { target, .. } => format!("resolve {target}"),
        }
    }

    pub fn target(&self) -> JobTarget {
        match self {
            Self::ListRemote { registry }
            | Self::Search { registry, .. }
            | Self::MirrorRegistry {
                source: registry, ..
            }
            | Self::RegistryAuth { name: registry }
            | Self::RegistryRemove { name: registry, .. } => JobTarget::Registry(registry.clone()),
            Self::RegistryAdd(cfg) => JobTarget::Registry(cfg.name.clone()),
            Self::Pull { target, .. } | Self::Resolve { target, .. } => {
                JobTarget::Dataset(target.clone())
            }
            Self::Push { bag, .. }
            | Self::Import { bag, .. }
            | Self::Export { bag, .. }
            | Self::RemoveLocal { bag }
            | Self::RemoveRemote { bag, .. } => JobTarget::Dataset(bag.to_string()),
            Self::Clean { .. } | Self::MirrorCacheSsh { .. } => JobTarget::Cache,
        }
    }

    /// Transfers are throttled; single round-trip queries are not.
    pub fn is_transfer(&self) -> bool {
        matches!(
            self,
            Self::Pull { .. }
                | Self::Push { .. }
                | Self::Export { .. }
                | Self::MirrorRegistry { .. }
                | Self::MirrorCacheSsh { .. }
        )
    }
}

/// What changed once a job finished, so the UI reloads only what it must.
#[derive(Debug, Clone, Copy, Default)]
pub struct Refresh {
    pub local: bool,
    pub registries: bool,
    pub remotes: bool,
}

impl Refresh {
    pub fn local() -> Self {
        Self {
            local: true,
            ..Self::default()
        }
    }

    pub fn remotes() -> Self {
        Self {
            remotes: true,
            ..Self::default()
        }
    }

    pub fn everything() -> Self {
        Self {
            local: true,
            registries: true,
            remotes: true,
        }
    }
}

#[derive(Debug)]
pub enum JobOutcome {
    RemoteListing {
        registry: String,
        rows: Vec<(BagRef, Option<BagInfo>)>,
    },
    Resolved {
        target: String,
        path: PathBuf,
    },
    /// A message for the jobs pane plus the state it invalidated.
    Message {
        text: String,
        refresh: Refresh,
    },
}

#[derive(Debug)]
pub enum JobEvent {
    Progress {
        id: JobId,
        phase: &'static str,
        message: String,
    },
    Done {
        id: JobId,
        outcome: Box<JobOutcome>,
    },
    Failed {
        id: JobId,
        error: String,
    },
}

/// Progress sink that forwards `mt_dataset` progress lines to the UI loop.
///
/// The sink itself never leaves its job thread; only the channel sender does.
struct ChannelProgress {
    id: JobId,
    tx: UnboundedSender<JobEvent>,
}

impl ProgressSink for ChannelProgress {
    fn emit(&mut self, event: ProgressEvent) {
        let _ = self.tx.send(JobEvent::Progress {
            id: self.id,
            phase: event.phase,
            message: event.message,
        });
    }
}

fn registry_timeout() -> Duration {
    let secs = config::load_registries()
        .map(|f| f.settings.registry_timeout_secs)
        .unwrap_or(10);
    Duration::from_secs(secs)
}

/// Runs `kind` on a dedicated thread and reports back over `tx`.
pub fn spawn(id: JobId, kind: JobKind, tx: UnboundedSender<JobEvent>) {
    let thread = std::thread::Builder::new().name(format!("marina-job-{id}"));
    let spawned = thread.spawn(move || {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                let _ = tx.send(JobEvent::Failed {
                    id,
                    error: format!("could not start a job runtime: {error}"),
                });
                return;
            }
        };
        let result = runtime.block_on(execute(id, kind, &tx));
        let event = match result {
            Ok(outcome) => JobEvent::Done {
                id,
                outcome: Box::new(outcome),
            },
            Err(error) => JobEvent::Failed {
                id,
                error: format!("{error:#}"),
            },
        };
        let _ = tx.send(event);
    });

    if let Err(error) = spawned {
        let _ = JobEvent::Failed {
            id,
            error: format!("could not start a job thread: {error}"),
        };
        log::error!("could not start a job thread: {error}");
    }
}

async fn execute(id: JobId, kind: JobKind, tx: &UnboundedSender<JobEvent>) -> Result<JobOutcome> {
    let mut marina = Marina::load().context("could not load the Marina configuration")?;

    match kind {
        JobKind::ListRemote { registry } => {
            let rows =
                with_timeout(&registry, marina.search_remote_with_info(&registry, "*")).await?;
            Ok(JobOutcome::RemoteListing { registry, rows })
        }
        JobKind::Search { registry, pattern } => {
            let rows = with_timeout(
                &registry,
                marina.search_remote_with_info(&registry, &pattern),
            )
            .await?;
            Ok(JobOutcome::RemoteListing { registry, rows })
        }
        JobKind::Pull {
            target,
            registry,
            options,
        } => {
            let mut sink = ChannelProgress { id, tx: tx.clone() };
            let mut progress = ProgressReporter::new(&mut sink);
            let text = if target.contains('*') {
                let pulled = marina
                    .pull_pattern_with_progress_and_options(
                        &target,
                        registry.as_deref(),
                        options,
                        &mut progress,
                    )
                    .await?;
                format!("pulled {} dataset(s)", pulled.len())
            } else {
                let bag: BagRef = target.parse()?;
                let path = marina
                    .pull_exact_with_progress_and_options(
                        &bag,
                        registry.as_deref(),
                        options,
                        &mut progress,
                    )
                    .await?;
                format!("pulled {} -> {}", bag.without_attachment(), path.display())
            };
            Ok(JobOutcome::Message {
                text,
                refresh: Refresh::local(),
            })
        }
        JobKind::Push {
            bag,
            source,
            registry,
            options,
        } => {
            let dry_run = options.dry_run;
            let mut sink = ChannelProgress { id, tx: tx.clone() };
            let mut progress = ProgressReporter::new(&mut sink);
            marina
                .push_with_progress_and_options(
                    &bag,
                    &source,
                    registry.as_deref(),
                    *options,
                    &mut progress,
                )
                .await?;
            let text = if dry_run {
                format!("dry-run complete for {}", bag.without_attachment())
            } else {
                format!("pushed {}", bag.without_attachment())
            };
            Ok(JobOutcome::Message {
                text,
                refresh: if dry_run {
                    Refresh::default()
                } else {
                    Refresh::everything()
                },
            })
        }
        JobKind::Import {
            bag,
            path,
            move_to_cache,
        } => {
            let imported = marina.import_local(&bag, path.as_deref(), move_to_cache)?;
            Ok(JobOutcome::Message {
                text: format!("imported {bag} -> {}", imported.display()),
                refresh: Refresh::local(),
            })
        }
        JobKind::Export { bag, output } => {
            marina.export(&bag, &output)?;
            Ok(JobOutcome::Message {
                text: format!("exported {bag} -> {}", output.display()),
                refresh: Refresh::default(),
            })
        }
        JobKind::RemoveLocal { bag } => {
            marina.remove_local(&bag)?;
            Ok(JobOutcome::Message {
                text: format!("removed local {bag}"),
                refresh: Refresh::local(),
            })
        }
        JobKind::RemoveRemote {
            bag,
            registry,
            write_http_index,
        } => {
            marina
                .check_write_access(Some(&registry))
                .await
                .with_context(|| format!("registry '{registry}' is not writable"))?;
            marina
                .remove_remote(&bag, Some(&registry), write_http_index)
                .await?;
            Ok(JobOutcome::Message {
                text: format!("removed {bag} from {registry}"),
                refresh: Refresh::remotes(),
            })
        }
        JobKind::Clean { all } => {
            marina.clean(all)?;
            Ok(JobOutcome::Message {
                text: if all {
                    "removed cache and registries".to_string()
                } else {
                    "removed cached resources (registries kept)".to_string()
                },
                refresh: Refresh::everything(),
            })
        }
        JobKind::MirrorRegistry {
            source,
            target,
            patterns,
        } => {
            let mut sink = ChannelProgress { id, tx: tx.clone() };
            let mut progress = ProgressReporter::new(&mut sink);
            let patterns = if patterns.is_empty() {
                vec!["*".to_string()]
            } else {
                patterns
            };
            let stats = marina
                .mirror_registry_filtered(&source, &target, patterns.as_slice(), &mut progress)
                .await?;
            Ok(JobOutcome::Message {
                text: format!(
                    "mirror complete: {} pushed, {} updated, {} skipped",
                    stats.pushed, stats.updated, stats.skipped
                ),
                refresh: Refresh::remotes(),
            })
        }
        JobKind::MirrorCacheSsh {
            target,
            patterns,
            options,
        } => {
            let mut sink = ChannelProgress { id, tx: tx.clone() };
            let mut progress = ProgressReporter::new(&mut sink);
            let stats = marina
                .mirror_cache_to_ssh(&target, &patterns, *options, &mut progress)
                .await?;
            Ok(JobOutcome::Message {
                text: format!(
                    "mirror complete: {} pushed, {} updated, {} skipped ({} rsync, {} native)",
                    stats.pushed,
                    stats.updated,
                    stats.skipped,
                    stats.rsync_datasets,
                    stats.native_datasets
                ),
                refresh: Refresh::default(),
            })
        }
        JobKind::RegistryAdd(config) => {
            let name = config.name.clone();
            let kind = config.kind.clone();
            marina.add_registry(*config)?;
            Ok(JobOutcome::Message {
                text: format!("registry added: {name} ({kind})"),
                refresh: Refresh::everything(),
            })
        }
        JobKind::RegistryRemove { name, delete_data } => {
            let removed = marina.remove_registry(&name, delete_data)?;
            Ok(JobOutcome::Message {
                text: format!("removed registry '{}' ({})", removed.name, removed.kind),
                refresh: Refresh::everything(),
            })
        }
        JobKind::RegistryAuth { name } => registry_auth(&marina, &name, id, tx).await,
        JobKind::Resolve { target, registry } => {
            let resolved = marina.resolve_target(&target, registry.as_deref()).await?;
            match resolved {
                ResolveResult::LocalPath(path) | ResolveResult::Cached(path) => {
                    Ok(JobOutcome::Resolved { target, path })
                }
                ResolveResult::RemoteAvailable { registry, bag, .. } => Ok(JobOutcome::Message {
                    text: format!(
                        "{} is only in registry '{registry}'. Pull it first",
                        bag.without_attachment()
                    ),
                    refresh: Refresh::default(),
                }),
                ResolveResult::Ambiguous { candidates } => Ok(JobOutcome::Message {
                    text: format!(
                        "{target} is in {}. Pick a registry and pull it",
                        candidates
                            .iter()
                            .map(|(registry, _)| registry.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                    refresh: Refresh::default(),
                }),
            }
        }
    }
}

#[cfg(feature = "gdrive")]
async fn registry_auth(
    marina: &Marina,
    name: &str,
    id: JobId,
    tx: &UnboundedSender<JobEvent>,
) -> Result<JobOutcome> {
    use mt_dataset::registry::gdrive_auth;

    let config = marina
        .list_registry_configs()
        .into_iter()
        .find(|candidate| candidate.name == name)
        .ok_or_else(|| anyhow::anyhow!("registry '{name}' not found"))?
        .clone();
    if config.kind != "gdrive" {
        anyhow::bail!(
            "registry '{name}' is kind '{}', only gdrive registries support OAuth auth",
            config.kind
        );
    }

    let (client_id, client_secret) = gdrive_auth::resolve_client_credentials(None, None)?;
    // The TUI owns the terminal, so the browser flow prints its URL into the
    // job log instead of taking over stdout.
    let _ = tx.send(JobEvent::Progress {
        id,
        phase: "auth",
        message: format!("starting the OAuth flow for '{name}'"),
    });
    gdrive_auth::run_oauth_flow_with_options(name, &client_id, &client_secret, false, None).await?;

    let status = gdrive_auth::oauth_status(name).await?;
    Ok(JobOutcome::Message {
        text: format!(
            "authenticated '{name}' (token {})",
            status.token_path.display()
        ),
        refresh: Refresh::remotes(),
    })
}

#[cfg(not(feature = "gdrive"))]
async fn registry_auth(
    _marina: &Marina,
    _name: &str,
    _id: JobId,
    _tx: &UnboundedSender<JobEvent>,
) -> Result<JobOutcome> {
    anyhow::bail!("Google Drive support requires a build with the `gdrive` feature")
}

/// Read-only registry queries fail soft: a registry that does not answer within
/// the configured timeout is reported as an error on its own row rather than
/// stalling the whole listing.
async fn with_timeout<T>(
    registry: &str,
    future: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    let timeout = registry_timeout();
    match tokio::time::timeout(timeout, future).await {
        Ok(result) => result,
        Err(_) => Err(anyhow::anyhow!(
            "registry '{registry}' did not respond within {}s",
            timeout.as_secs()
        )),
    }
}
