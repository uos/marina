use std::path::PathBuf;
use std::{io::IsTerminal, io::Write};

use anyhow::{Context as _, Result};
use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::engine::{ArgValueCompleter, CompletionCandidate};
use log::{error, warn};

use crate::core::{Marina, PullOptions, PushOptions, ResolveResult};
use crate::io::mcap_transform::{McapChunkCompression, PointCloudCompressionMode};
use crate::io::pack::ArchiveCompression;
use crate::model::bag_ref::BagRef;
use crate::progress::{ProgressReporter, WriterProgress};
use crate::registry::driver::BagInfo;
#[cfg(feature = "gdrive")]
use crate::registry::gdrive_auth;
use crate::storage::config::{
    self, ConfigArchiveCompression, ConfigMcapCompression, ConfigPointcloudMode, RegistryConfig,
    TimeDisplay,
};

#[derive(Parser)]
#[command(name = "marina")]
#[command(
    about = "A dataset manager for robotics built to organize, share, and discover ROS 2 bagfiles or datasets across teams and storage backends."
)]
#[command(version)]
struct Cli {
    #[arg(short = 'y', long = "yes", global = true)]
    yes: bool,
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Registry(RegistryCmd),
    #[command(alias = "ls")]
    List(LocalListArgs),
    Search(SearchArgs),
    Push(PushArgs),
    Pull(PullArgs),
    Resolve(ResolveArgs),
    Export(ExportArgs),
    Rm(RemoveArgs),
    Clean(CleanArgs),
    Import(ImportArgs),
    Inspect(InspectArgs),
    #[command(hide = true)]
    CompleteRefresh,
    Completions(CompletionsArgs),
    Version,
}

#[derive(Args)]
struct LocalListArgs {
    /// List datasets available in all remote registries instead of the local cache
    #[arg(long)]
    remote: bool,
    /// Filter to a specific registry (only with --remote)
    #[arg(long)]
    registry: Option<String>,
}

#[derive(Args)]
struct SearchArgs {
    pattern: String,
    #[arg(long)]
    registry: Option<String>,
}

#[derive(Subcommand)]
enum RegistrySub {
    Add(AddRegistryArgs),
    #[command(alias = "ls")]
    List,
    Rm(RemoveRegistryArgs),
    /// Authenticate a gdrive registry via browser OAuth flow
    Auth(AuthRegistryArgs),
    /// Mirror all datasets from one registry into another
    Mirror(MirrorRegistryArgs),
}

#[derive(Args)]
struct MirrorRegistryArgs {
    /// Source registry name
    source: String,
    /// Target registry name
    target: String,
}

#[derive(Args)]
struct RegistryCmd {
    #[command(subcommand)]
    cmd: RegistrySub,
}

#[derive(Args)]
struct AddRegistryArgs {
    name: String,
    uri: String,
    #[arg(long)]
    kind: Option<String>,
    #[arg(long)]
    auth_env: Option<String>,
}

#[derive(Args)]
struct RemoveRegistryArgs {
    name: String,
    #[arg(long)]
    delete_data: bool,
}

#[derive(Args)]
struct AuthRegistryArgs {
    /// Name of the registry to authenticate
    name: String,
    /// Show persisted OAuth status for this registry
    #[arg(long)]
    status: bool,
    /// OAuth client ID (or set MARINA_GDRIVE_CLIENT_ID env var)
    #[arg(long)]
    client_id: Option<String>,
    /// OAuth client secret (or set MARINA_GDRIVE_CLIENT_SECRET env var)
    #[arg(long)]
    client_secret: Option<String>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliPointcloudMode {
    Off,
    Lossy,
    Lossless,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliMcapCompression {
    None,
    Zstd,
    Lz4,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliArchiveCompression {
    Gzip,
    None,
}

#[derive(Args)]
struct PushArgs {
    #[arg(value_name = "DATASET", add = ArgValueCompleter::new(complete_datasets))]
    bag: BagRef,
    source: PathBuf,
    #[arg(long)]
    registry: Option<String>,
    #[arg(long, value_enum)]
    pointcloud_mode: Option<CliPointcloudMode>,
    #[arg(long)]
    pointcloud_accuracy_mm: Option<f64>,
    #[arg(long, value_enum)]
    packed_mcap_compression: Option<CliMcapCompression>,
    #[arg(long, value_enum)]
    packed_archive_compression: Option<CliArchiveCompression>,
    #[arg(long)]
    write_http_index: bool,
    #[arg(long)]
    dry_run: bool,
    #[arg(long)]
    move_to_cache: bool,
    #[arg(long)]
    no_progress: bool,
}

#[derive(Args)]
struct PullArgs {
    #[arg(value_name = "DATASET", add = ArgValueCompleter::new(complete_remote_datasets))]
    target: String,
    #[arg(long)]
    registry: Option<String>,
    #[arg(long, value_enum)]
    unpacked_mcap_compression: Option<CliMcapCompression>,
    #[arg(long)]
    no_progress: bool,
    #[arg(long)]
    force: bool,
}

#[derive(Args)]
struct ResolveArgs {
    #[arg(value_name = "DATASET", add = ArgValueCompleter::new(complete_datasets))]
    target: String,
    #[arg(long)]
    registry: Option<String>,
}

#[derive(Args)]
struct ExportArgs {
    #[arg(value_name = "DATASET", add = ArgValueCompleter::new(complete_datasets))]
    target: BagRef,
    output: PathBuf,
}

#[derive(Args)]
struct RemoveArgs {
    #[arg(value_name = "PATTERN", add = ArgValueCompleter::new(complete_datasets))]
    pattern: String,
    #[arg(long)]
    remote: bool,
    #[arg(long)]
    registry: Option<String>,
    #[arg(long)]
    write_http_index: bool,
}

#[derive(Args)]
struct CleanArgs {
    #[arg(short = 'a', long = "all")]
    all: bool,
}

#[derive(Args)]
struct ImportArgs {
    target: BagRef,
    path: Option<PathBuf>,
    #[arg(long)]
    move_to_cache: bool,
}

#[derive(Args)]
struct InspectArgs {
    #[arg(value_name = "DATASET", add = ArgValueCompleter::new(complete_datasets))]
    target: String,
    #[arg(long)]
    registry: Option<String>,
}

#[derive(Args)]
struct CompletionsArgs {
    shell: clap_complete::Shell,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct CompletionIndex {
    timestamp: u64,
    registries: std::collections::HashMap<String, Vec<String>>,
}

fn completion_cache_path() -> Option<std::path::PathBuf> {
    dirs::cache_dir().map(|d| d.join("marina").join("completions.json"))
}

fn spawn_complete_refresh() {
    if let Ok(exe) = std::env::current_exe() {
        let _ = std::process::Command::new(exe)
            .arg("complete-refresh")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();
    }
}

/// Load the remote completion index, trigger a background refresh if stale,
/// and return `(index, needs_refresh)`.
fn load_completion_index() -> (Option<CompletionIndex>, bool) {
    let Some(cache_path) = completion_cache_path() else {
        return (None, true);
    };
    let ttl = config::load_registries()
        .map(|f| f.settings.completion_cache_ttl_secs)
        .unwrap_or(600);

    match std::fs::read_to_string(&cache_path) {
        Ok(contents) => match serde_json::from_str::<CompletionIndex>(&contents) {
            Ok(idx) => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let stale = now.saturating_sub(idx.timestamp) > ttl;
                (Some(idx), stale)
            }
            Err(_) => (None, true),
        },
        Err(_) => (None, true),
    }
}

fn complete_datasets(current: &std::ffi::OsStr) -> Vec<CompletionCandidate> {
    let prefix = current.to_string_lossy();
    let Ok(marina) = crate::core::Marina::load() else {
        return vec![];
    };

    // Local cached bags — disk only, no network.
    let mut candidates: Vec<CompletionCandidate> = marina
        .list_cached_bags()
        .into_iter()
        .filter(|item| item.bag.to_string().starts_with(prefix.as_ref()))
        .map(|item| {
            let name = item.bag.to_string();
            CompletionCandidate::new(name.clone())
                .tag(Some("cached".to_owned().into()))
                .id(Some(name))
        })
        .collect();

    let (index, needs_refresh) = load_completion_index();

    if let Some(ref idx) = index {
        for (reg_name, bags) in &idx.registries {
            for bag_name in bags {
                if bag_name.starts_with(prefix.as_ref()) {
                    candidates.push(
                        CompletionCandidate::new(bag_name.clone())
                            .tag(Some(reg_name.clone().into()))
                            .id(Some(bag_name.clone())),
                    );
                }
            }
        }
    }

    if needs_refresh {
        spawn_complete_refresh();
    }

    candidates
}

fn complete_remote_datasets(current: &std::ffi::OsStr) -> Vec<CompletionCandidate> {
    let prefix = current.to_string_lossy();
    let (index, needs_refresh) = load_completion_index();

    let mut candidates = Vec::new();
    if let Some(ref idx) = index {
        for (reg_name, bags) in &idx.registries {
            for bag_name in bags {
                if bag_name.starts_with(prefix.as_ref()) {
                    candidates.push(
                        CompletionCandidate::new(bag_name.clone())
                            .tag(Some(reg_name.clone().into()))
                            .id(Some(bag_name.clone())),
                    );
                }
            }
        }
    }

    if needs_refresh {
        spawn_complete_refresh();
    }

    candidates
}

pub async fn run() -> Result<()> {
    let prog_name: &'static str = match std::env::var("MARINA_PROG_NAME") {
        Ok(s) => Box::leak(s.into_boxed_str()),
        Err(_) => "marina",
    };
    clap_complete::CompleteEnv::with_factory(|| Cli::command().name(prog_name)).complete();
    let mut args = std::env::args().collect::<Vec<_>>();
    if let Some(first) = args.first_mut() {
        *first = prog_name.to_string();
    }
    run_with_args(args).await
}

pub async fn run_with_args(args: Vec<String>) -> Result<()> {
    let raw_yes = args.iter().any(|arg| arg == "-y" || arg == "--yes");
    let cli = Cli::parse_from(args);
    run_parsed(cli, raw_yes).await
}

/// Prompt the user to pick one registry from a list, or auto-accept the first if `yes` is set.
/// `items` is a list of `(registry_name, display_label)` pairs.
fn pick_registry(prompt: &str, items: &[(String, String)], yes: bool) -> Result<String> {
    if yes || items.len() == 1 {
        return Ok(items[0].0.clone());
    }
    eprintln!("{}:", prompt);
    for (i, (name, label)) in items.iter().enumerate() {
        eprintln!("  [{}] {}  ({})", i + 1, label, name);
    }
    eprint!("Select [1]: ");
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    let input = input.trim();
    let idx: usize = if input.is_empty() {
        1
    } else {
        input
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid selection"))?
    };
    if idx == 0 || idx > items.len() {
        return Err(anyhow::anyhow!("selection out of range"));
    }
    Ok(items[idx - 1].0.clone())
}

fn is_interactive_shell() -> bool {
    std::io::stdin().is_terminal() && std::io::stdout().is_terminal()
}

fn confirm_yes_default(prompt: &str, yes: bool) -> Result<bool> {
    if yes {
        return Ok(true);
    }
    eprint!("{} [Y/n]: ", prompt);
    std::io::stderr().flush()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    let input = input.trim().to_ascii_lowercase();
    Ok(input.is_empty() || input == "y" || input == "yes")
}

fn pick_pull_candidate(
    prompt: &str,
    items: &[(String, BagRef, Option<BagInfo>)],
    time_display: TimeDisplay,
) -> Result<Option<usize>> {
    if items.is_empty() {
        return Ok(None);
    }
    eprintln!("{}:", prompt);
    let rows: Vec<[String; 9]> = items
        .iter()
        .enumerate()
        .map(|(i, (registry, bag, info))| {
            let (hash, orig, packed, clouds, mcap, pushed) =
                format_bag_info(info.as_ref(), time_display);
            [
                (i + 1).to_string(),
                bag.to_string(),
                registry.clone(),
                hash,
                orig,
                packed,
                clouds,
                mcap,
                pushed,
            ]
        })
        .collect();

    let headers = [
        "IDX", "DATASET", "REGISTRY", "HASH", "ORIGINAL", "PACKED", "CLOUDS", "ARCHIVE", "PUSHED",
    ];
    let mut widths = headers.map(|h| h.len());
    for row in &rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.len());
        }
    }
    let fmt_row = |cols: &[&str; 9]| {
        let mut s = String::new();
        for (i, col) in cols.iter().enumerate() {
            if i > 0 {
                s.push_str("  ");
            }
            if i + 1 < cols.len() {
                s.push_str(&format!("{:<width$}", col, width = widths[i]));
            } else {
                s.push_str(col);
            }
        }
        s
    };

    eprintln!("{}", fmt_row(&headers));
    for row in &rows {
        eprintln!("{}", fmt_row(&row.each_ref().map(|s| s.as_str())));
    }
    eprint!("Select [1]: ");
    std::io::stderr().flush()?;

    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    let input = input.trim();
    let idx: usize = if input.is_empty() {
        1
    } else {
        input
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid selection"))?
    };
    if idx == 0 || idx > items.len() {
        return Err(anyhow::anyhow!("selection out of range"));
    }
    Ok(Some(idx - 1))
}

fn print_remote_detail_table(
    mut all: Vec<(String, BagRef, Option<BagInfo>)>,
    time_display: TimeDisplay,
) {
    if all.is_empty() {
        println!("no remote datasets found");
        return;
    }

    let is_tty = std::io::stdout().is_terminal();

    // Sort: no-namespace first, then by namespace, then by name, then full string.
    all.sort_by(|(_, a, _), (_, b, _)| {
        a.namespace
            .as_deref()
            .unwrap_or("")
            .cmp(b.namespace.as_deref().unwrap_or(""))
            .then_with(|| a.name.cmp(&b.name))
            .then_with(|| a.to_string().cmp(&b.to_string()))
    });

    struct Row {
        namespace: Option<String>,
        base_name: String,
        display_name: String,
        rest_cols: [String; 7],
    }

    let rows: Vec<Row> = all
        .into_iter()
        .map(|(registry, bag, info)| {
            let (hash, orig, packed, clouds, mcap, pushed) =
                format_bag_info(info.as_ref(), time_display);
            let full = bag.to_string();
            let display_name = match &bag.namespace {
                Some(ns) => full
                    .strip_prefix(&format!("{ns}/"))
                    .unwrap_or(&full)
                    .to_string(),
                None => full,
            };
            Row {
                namespace: bag.namespace.clone(),
                base_name: bag.name.clone(),
                display_name,
                rest_cols: [registry, hash, orig, packed, clouds, mcap, pushed],
            }
        })
        .collect();

    let headers = [
        "DATASET", "REGISTRY", "HASH", "ORIGINAL", "PACKED", "CLOUDS", "ARCHIVE", "PUSHED",
    ];
    let mut widths = headers.map(|h| h.len());
    for row in &rows {
        widths[0] = widths[0].max(row.display_name.len());
        for (i, cell) in row.rest_cols.iter().enumerate() {
            widths[i + 1] = widths[i + 1].max(cell.len());
        }
    }

    // Print header row.
    let mut header_line = format!("{:<width$}", headers[0], width = widths[0]);
    for (i, h) in headers.iter().enumerate().skip(1) {
        if i + 1 < headers.len() {
            header_line.push_str(&format!("  {:<width$}", h, width = widths[i]));
        } else {
            header_line.push_str(&format!("  {}", h));
        }
    }
    println!("{}", header_line);

    let mut group_counts: std::collections::HashMap<(Option<String>, &str), usize> =
        std::collections::HashMap::new();
    for row in &rows {
        *group_counts
            .entry((row.namespace.clone(), row.base_name.as_str()))
            .or_insert(0) += 1;
    }

    let mut started = false;
    let mut prev_ns: Option<String> = None; // namespace of the last printed row
    let mut prev_base: Option<String> = None;

    for row in &rows {
        let group_key = (row.namespace.clone(), row.base_name.as_str());
        let is_grouped = group_counts.get(&group_key).copied().unwrap_or(0) > 1;
        let ns_changed = !started || prev_ns != row.namespace;
        let base_changed = prev_base.as_deref() != Some(row.base_name.as_str());

        // Namespace section header.
        if ns_changed {
            if started {
                println!(); // blank line between namespace sections
            }
            if let Some(ref ns) = row.namespace {
                if is_tty {
                    println!("\x1b[1;36m{}/\x1b[0m", ns);
                } else {
                    println!("{}/", ns);
                }
            }
            prev_base = None;
        }

        if base_changed && !ns_changed && prev_base.is_some() {
            let prev_key = (row.namespace.clone(), prev_base.as_deref().unwrap_or(""));
            let prev_was_grouped = group_counts.get(&prev_key).copied().unwrap_or(0) > 1;
            if is_grouped || prev_was_grouped {
                println!();
            }
        }

        if base_changed && is_grouped {
            if is_tty {
                println!("\x1b[1;4m{}\x1b[0m", row.base_name);
            } else {
                println!("{}", row.base_name);
            }
        }

        started = true;
        prev_ns = row.namespace.clone();
        prev_base = Some(row.base_name.clone());

        // DATASET column: dim the base-name prefix when name-grouped.
        let dataset_col = if is_tty && is_grouped {
            let after_base = row
                .display_name
                .strip_prefix(row.base_name.as_str())
                .unwrap_or(&row.display_name);
            let padding = widths[0].saturating_sub(row.display_name.len());
            format!(
                "\x1b[2m{}\x1b[0m{}{}",
                row.base_name,
                after_base,
                " ".repeat(padding)
            )
        } else {
            format!("{:<width$}", row.display_name, width = widths[0])
        };

        let mut line = dataset_col;
        for (i, cell) in row.rest_cols.iter().enumerate() {
            if i + 1 < row.rest_cols.len() {
                line.push_str(&format!("  {:<width$}", cell, width = widths[i + 1]));
            } else {
                line.push_str(&format!("  {}", cell));
            }
        }
        println!("{}", line);
    }
}

async fn pull_and_print(
    marina: &mut Marina,
    bag: &BagRef,
    registry: Option<&str>,
    pull_options: PullOptions,
) -> Result<()> {
    let path = {
        let mut stdout = std::io::stdout();
        let mut sink = WriterProgress::new(&mut stdout);
        let mut progress = ProgressReporter::new(&mut sink);
        marina
            .pull_exact_with_progress_and_options(bag, registry, pull_options, &mut progress)
            .await?
    };
    println!("pulled {} -> {}", bag.without_attachment(), path.display());
    if let Some(stats) = marina.cached_size_stats(bag) {
        print_size_summary("cached size", stats.original_bytes, stats.packed_bytes);
    }
    Ok(())
}

async fn run_parsed(cli: Cli, raw_yes: bool) -> Result<()> {
    crate::cleanup::init();
    let prog = std::env::var("MARINA_PROG_NAME").unwrap_or_else(|_| "marina".to_string());
    let yes = cli.yes || raw_yes;
    let compression = config::load_compression_config()?;
    let mut marina = Marina::load()?;

    match cli.cmd {
        Commands::Registry(cmd) => match cmd.cmd {
            RegistrySub::Add(args) => {
                let kind = args
                    .kind
                    .unwrap_or_else(|| config::infer_kind_from_uri(&args.uri).to_string());
                let uri = if kind == "folder" {
                    let scheme_end = args.uri.find("://").map(|i| i + 3).unwrap_or(0);
                    let path_part = &args.uri[scheme_end..];
                    let path = std::path::Path::new(path_part);
                    if path.is_relative() {
                        let abs = std::env::current_dir()?.join(path);
                        format!("folder://{}", abs.display())
                    } else {
                        args.uri
                    }
                } else {
                    args.uri
                };
                marina.add_registry(RegistryConfig {
                    name: args.name.clone(),
                    kind: kind.clone(),
                    uri,
                    auth_env: args.auth_env,
                })?;
                println!("registry added: {} ({})", args.name, kind);
                spawn_complete_refresh();
            }
            RegistrySub::List => {
                let registry_path = config::registry_file_path()?;
                println!("config: {}", registry_path.display());
                let entries = marina.list_registry_configs();
                if entries.is_empty() {
                    println!("no registries configured");
                } else {
                    let name_w = entries
                        .iter()
                        .map(|c| c.name.len())
                        .max()
                        .unwrap_or(0)
                        .max(4);
                    println!("{:<name_w$}  URI", "NAME");
                    for cfg in &entries {
                        println!("{:<name_w$}  {}", cfg.name, cfg.uri);
                    }
                }
            }
            RegistrySub::Auth(args) => {
                let cfg = marina
                    .list_registry_configs()
                    .into_iter()
                    .find(|c| c.name == args.name)
                    .ok_or_else(|| anyhow::anyhow!("registry '{}' not found", args.name))?
                    .clone();
                if cfg.kind != "gdrive" {
                    return Err(anyhow::anyhow!(
                        "registry '{}' is kind '{}', only gdrive registries support OAuth auth",
                        args.name,
                        cfg.kind
                    ));
                }
                #[cfg(not(feature = "gdrive"))]
                {
                    let _ = args;
                    return Err(anyhow::anyhow!(
                        "gdrive support is disabled in this build; rebuild with feature `gdrive`"
                    ));
                }
                #[cfg(feature = "gdrive")]
                {
                    if args.status {
                        let status = gdrive_auth::oauth_status(&args.name).await?;
                        println!("registry: {}", args.name);
                        println!("token file: {}", status.token_path.display());
                        if !status.token_present {
                            println!("status: missing (run `{prog} registry auth {}`)", args.name);
                        } else if status.token_valid {
                            println!("status: valid");
                            if let Some(expires_at) = status.expires_at {
                                let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|d| d.as_secs())
                                    .unwrap_or(0);
                                let remaining = expires_at.saturating_sub(now);
                                println!("expires in: {}s", remaining);
                            }
                        } else {
                            println!("status: invalid (refresh failed)");
                            if let Some(err) = status.refresh_error {
                                println!("refresh error: {}", err);
                            }
                        }
                        return Ok(());
                    }
                    let (client_id, client_secret) = gdrive_auth::resolve_client_credentials(
                        args.client_id,
                        args.client_secret,
                    )?;
                    gdrive_auth::run_oauth_flow(&args.name, &client_id, &client_secret).await?;
                }
            }
            RegistrySub::Mirror(args) => {
                let mut out = std::io::stdout();
                let mut sink = WriterProgress::new(&mut out);
                let mut progress = ProgressReporter::new(&mut sink);
                let stats = marina
                    .mirror_registry(&args.source, &args.target, &mut progress)
                    .await?;
                println!(
                    "mirror complete: {} pushed, {} updated, {} skipped",
                    stats.pushed, stats.updated, stats.skipped
                );
            }
            RegistrySub::Rm(args) => {
                let removed = marina.remove_registry(&args.name, args.delete_data)?;
                println!("removed registry '{}' ({})", removed.name, removed.kind);
                if matches!(removed.kind.as_str(), "folder" | "directory") {
                    let location = local_registry_data_path(&removed.uri);
                    if removed.data_deleted {
                        println!("deleted registry data at {}", location.display());
                    } else {
                        println!(
                            "data kept at {} (use --delete-data to remove it)",
                            location.display()
                        );
                    }
                } else {
                    println!("remote location was {}", removed.uri);
                }
                spawn_complete_refresh();
            }
        },
        Commands::List(args) => {
            if args.remote {
                let settings = config::load_registries()
                    .map(|f| f.settings)
                    .unwrap_or_default();
                let timeout_secs = settings.registry_timeout_secs;

                let eff_registry = args.registry.as_deref().or(marina.default_registry());
                let registry_names: Vec<String> = if let Some(reg) = eff_registry {
                    vec![reg.to_string()]
                } else {
                    marina
                        .list_registry_configs()
                        .into_iter()
                        .map(|c| c.name.clone())
                        .collect()
                };

                let futs = registry_names.iter().map(|name| {
                    let fut = marina.search_remote_with_info(name, "*");
                    let name = name.clone();
                    async move {
                        match tokio::time::timeout(
                            std::time::Duration::from_secs(timeout_secs),
                            fut,
                        )
                        .await
                        {
                            Ok(results) => Ok((name, results)),
                            Err(_) => Err(name),
                        }
                    }
                });
                let outcomes = futures::future::join_all(futs).await;

                let mut all: Vec<(String, BagRef, Option<BagInfo>)> = Vec::new();
                for outcome in outcomes {
                    match outcome {
                        Ok((name, results)) => {
                            for (bag, info) in results {
                                all.push((name.clone(), bag, info));
                            }
                        }
                        Err(name) => {
                            error!(
                                "registry '{}' did not respond within {}s — check your network or remove it with `{prog} registry rm {}`",
                                name, timeout_secs, name
                            );
                        }
                    }
                }

                print_remote_detail_table(all, settings.time_display);
            } else {
                let mut items = marina.list_cached_bags();
                if items.is_empty() {
                    println!("no local datasets cached");
                } else {
                    let is_tty = std::io::stdout().is_terminal();
                    items.sort_by(|a, b| {
                        a.bag
                            .namespace
                            .as_deref()
                            .unwrap_or("")
                            .cmp(b.bag.namespace.as_deref().unwrap_or(""))
                            .then_with(|| a.bag.name.cmp(&b.bag.name))
                            .then_with(|| a.bag.to_string().cmp(&b.bag.to_string()))
                    });
                    let mut prev_ns: Option<Option<String>> = None; // None = not started
                    for item in &items {
                        let ns_changed = prev_ns
                            .as_ref()
                            .map(|p| *p != item.bag.namespace)
                            .unwrap_or(true);
                        if ns_changed {
                            if prev_ns.is_some() {
                                println!();
                            }
                            if let Some(ref ns) = item.bag.namespace {
                                if is_tty {
                                    println!("\x1b[1;36m{}/\x1b[0m", ns);
                                } else {
                                    println!("{}/", ns);
                                }
                            }
                            prev_ns = Some(item.bag.namespace.clone());
                        }
                        let display = match &item.bag.namespace {
                            Some(ns) => {
                                let full = item.bag.to_string();
                                full.strip_prefix(&format!("{ns}/"))
                                    .unwrap_or(&full)
                                    .to_string()
                            }
                            None => item.bag.to_string(),
                        };
                        println!("{}", display);
                        println!("  path: {}", item.local_dir.display());
                        println!("  size: {}", human_bytes(item.original_bytes));
                    }
                }
            }
        }
        Commands::Search(args) => {
            let eff_registry = args.registry.as_deref().or(marina.default_registry());
            let rows = if let Some(registry) = eff_registry {
                marina
                    .search_remote_with_info(registry, &args.pattern)
                    .await
                    .into_iter()
                    .map(|(bag, info)| (registry.to_string(), bag, info))
                    .collect::<Vec<_>>()
            } else {
                let mut rows = Vec::new();
                let cfg_names: Vec<String> = marina
                    .list_registry_configs()
                    .into_iter()
                    .map(|c| c.name.clone())
                    .collect();
                for name in cfg_names {
                    match marina.search_remote(&args.pattern, Some(&name)).await {
                        Ok(bags) => {
                            for bag in bags {
                                let info = marina.bag_info(&name, &bag).await;
                                rows.push((name.clone(), bag, info));
                            }
                        }
                        Err(err) => {
                            warn!("failed searching registry '{}': {}", name, err);
                        }
                    }
                }
                rows
            };
            let time_display = config::load_registries()
                .map(|f| f.settings.time_display)
                .unwrap_or_default();
            print_remote_detail_table(rows, time_display);
        }
        Commands::Push(args) => {
            let registry = match args.registry.as_deref().or(marina.default_registry()) {
                Some(r) => Some(r.to_string()),
                None => {
                    let cfgs = marina.list_registry_configs();
                    if cfgs.len() > 1 {
                        let items: Vec<(String, String)> = cfgs
                            .iter()
                            .map(|c| (c.name.clone(), format!("{} {}", c.kind, c.uri)))
                            .collect();
                        Some(pick_registry(
                            "Multiple registries configured, pick one to push to",
                            &items,
                            yes,
                        )?)
                    } else {
                        None
                    }
                }
            };
            let push_options = PushOptions {
                pointcloud_mode: args
                    .pointcloud_mode
                    .map(cli_pointcloud_mode_to_core)
                    .unwrap_or_else(|| config_pointcloud_mode_to_core(compression.pointcloud_mode)),
                pointcloud_precision_m: args
                    .pointcloud_accuracy_mm
                    .unwrap_or(compression.pointcloud_accuracy_mm)
                    / 1000.0,
                packed_mcap_compression: args
                    .packed_mcap_compression
                    .map(cli_mcap_compression_to_core)
                    .unwrap_or_else(|| {
                        config_mcap_compression_to_core(compression.packed_mcap_compression)
                    }),
                packed_archive_compression: args
                    .packed_archive_compression
                    .map(cli_archive_compression_to_core)
                    .unwrap_or_else(|| {
                        config_archive_compression_to_core(compression.packed_archive_compression)
                    }),
                write_http_index: args.write_http_index,
                dry_run: args.dry_run,
                move_source_to_cache: args.move_to_cache,
            };
            if !args.no_progress {
                let mut stdout = std::io::stdout();
                let mut sink = WriterProgress::new(&mut stdout);
                let mut progress = ProgressReporter::new(&mut sink);
                marina
                    .push_with_progress_and_options(
                        &args.bag,
                        &args.source,
                        registry.as_deref(),
                        push_options,
                        &mut progress,
                    )
                    .await?;
            } else {
                let mut progress = ProgressReporter::silent();
                marina
                    .push_with_progress_and_options(
                        &args.bag,
                        &args.source,
                        registry.as_deref(),
                        push_options,
                        &mut progress,
                    )
                    .await?;
            }
            if args.dry_run {
                println!(
                    "dry-run complete for {} (no upload performed)",
                    args.bag.without_attachment()
                );
            } else if let Some(stats) = marina.cached_size_stats(&args.bag) {
                spawn_complete_refresh();
                print_size_summary(
                    &format!("pushed {}", args.bag.without_attachment()),
                    stats.original_bytes,
                    stats.packed_bytes,
                );
            } else {
                spawn_complete_refresh();
                println!("pushed {}", args.bag.without_attachment());
            }
        }
        Commands::Pull(args) => {
            let pull_options = PullOptions {
                unpacked_mcap_compression: args
                    .unpacked_mcap_compression
                    .map(cli_mcap_compression_to_core)
                    .unwrap_or_else(|| {
                        config_mcap_compression_to_core(compression.unpacked_mcap_compression)
                    }),
                force: args.force,
            };
            // Resolve which registry to use, disambiguating if needed
            let registry: Option<String> = match args
                .registry
                .as_deref()
                .or(marina.default_registry())
            {
                Some(r) => Some(r.to_string()),
                None => {
                    let mut unique: Vec<(String, String)> = Vec::new();
                    let cfg_names: Vec<String> = marina
                        .list_registry_configs()
                        .into_iter()
                        .map(|c| c.name.clone())
                        .collect();
                    for name in cfg_names {
                        match marina.search_remote(&args.target, Some(&name)).await {
                            Ok(bags) => {
                                if let Some(bag) = bags.first() {
                                    let hash = marina
                                        .bag_info(&name, bag)
                                        .await
                                        .and_then(|i| i.bundle_hash)
                                        .map(|hx| format!("hash:{hx}"))
                                        .unwrap_or_default();
                                    unique.push((name.clone(), format!("{} {}", name, hash)));
                                }
                            }
                            Err(err) => {
                                warn!("failed searching registry '{}': {}", name, err);
                            }
                        }
                    }
                    unique.sort_by(|a, b| a.0.cmp(&b.0));
                    if unique.len() > 1 {
                        Some(pick_registry(
                            &format!("'{}' found in multiple registries, pick one", args.target),
                            &unique,
                            yes,
                        )?)
                    } else if unique.len() == 1 {
                        Some(unique[0].0.clone())
                    } else {
                        None
                    }
                }
            };
            if args.target.contains('*') {
                let pulled: Vec<BagRef> = if !args.no_progress {
                    let mut stdout = std::io::stdout();
                    let mut sink = WriterProgress::new(&mut stdout);
                    let mut progress = ProgressReporter::new(&mut sink);
                    marina
                        .pull_pattern_with_progress_and_options(
                            &args.target,
                            registry.as_deref(),
                            pull_options,
                            &mut progress,
                        )
                        .await?
                } else {
                    let mut progress = ProgressReporter::silent();
                    marina
                        .pull_pattern_with_progress_and_options(
                            &args.target,
                            registry.as_deref(),
                            pull_options,
                            &mut progress,
                        )
                        .await?
                };
                for bag in &pulled {
                    println!("pulled {}", bag);
                }
                println!("pulled {} dataset(s)", pulled.len());
            } else {
                let bag: BagRef = args.target.parse()?;
                if args.no_progress {
                    let mut progress = ProgressReporter::silent();
                    let path = marina
                        .pull_exact_with_progress_and_options(
                            &bag,
                            registry.as_deref(),
                            pull_options,
                            &mut progress,
                        )
                        .await?;
                    println!("pulled {} -> {}", bag.without_attachment(), path.display());
                    if let Some(stats) = marina.cached_size_stats(&bag) {
                        print_size_summary("cached size", stats.original_bytes, stats.packed_bytes);
                    }
                } else {
                    pull_and_print(&mut marina, &bag, registry.as_deref(), pull_options).await?;
                }
            }
        }
        Commands::Resolve(args) => {
            let interactive = is_interactive_shell();
            let quiet_non_interactive_yes = yes && !interactive;
            let eff_registry: Option<String> = args
                .registry
                .clone()
                .or_else(|| marina.default_registry().map(|s| s.to_string()));

            match marina
                .resolve_target(&args.target, eff_registry.as_deref())
                .await?
            {
                ResolveResult::LocalPath(p) => println!("{}", p.display()),
                ResolveResult::Cached(p) => println!("{}", p.display()),
                ResolveResult::RemoteAvailable {
                    registry,
                    bag,
                    needs_pull,
                } => {
                    if needs_pull {
                        let should_pull = if interactive {
                            if !quiet_non_interactive_yes {
                                println!("{} available in registry '{}'", bag, registry);
                            }
                            confirm_yes_default(
                                &format!(
                                    "pull {} from '{}' now?",
                                    bag.without_attachment(),
                                    registry
                                ),
                                yes,
                            )?
                        } else {
                            if !quiet_non_interactive_yes {
                                println!(
                                    "{} available in registry '{}'; run: {prog} pull {} --registry {}",
                                    bag, registry, bag, registry
                                );
                            }
                            yes
                        };

                        if should_pull {
                            let pull_options = PullOptions {
                                unpacked_mcap_compression: config_mcap_compression_to_core(
                                    compression.unpacked_mcap_compression,
                                ),
                                force: false,
                            };
                            if quiet_non_interactive_yes {
                                let mut progress = ProgressReporter::silent();
                                marina
                                    .pull_exact_with_progress_and_options(
                                        &bag,
                                        Some(registry.as_str()),
                                        pull_options,
                                        &mut progress,
                                    )
                                    .await?;
                            } else {
                                let path: std::path::PathBuf = {
                                    let mut stdout = std::io::stdout();
                                    let mut sink = WriterProgress::new(&mut stdout);
                                    let mut progress = ProgressReporter::new(&mut sink);
                                    marina
                                        .pull_exact_with_progress_and_options(
                                            &bag,
                                            Some(registry.as_str()),
                                            pull_options,
                                            &mut progress,
                                        )
                                        .await?
                                };
                                println!(
                                    "pulled {} -> {}",
                                    bag.without_attachment(),
                                    path.display()
                                );
                                if let Some(stats) = marina.cached_size_stats(&bag) {
                                    print_size_summary(
                                        "cached size",
                                        stats.original_bytes,
                                        stats.packed_bytes,
                                    );
                                }
                            }
                            match marina
                                .resolve_target(&args.target, eff_registry.as_deref())
                                .await?
                            {
                                ResolveResult::LocalPath(resolved)
                                | ResolveResult::Cached(resolved) => {
                                    println!("{}", resolved.display())
                                }
                                _ => {
                                    return Err(anyhow::anyhow!(
                                        "failed to resolve '{}' after pull",
                                        args.target
                                    ));
                                }
                            }
                        }
                    }
                }
                ResolveResult::Ambiguous { candidates } => {
                    if interactive {
                        let mut items: Vec<(String, BagRef, Option<BagInfo>)> = Vec::new();
                        for (registry, bag) in &candidates {
                            let info = marina.bag_info(registry, bag).await;
                            items.push((registry.clone(), bag.clone(), info));
                        }

                        let time_display = config::load_registries()
                            .map(|f| f.settings.time_display)
                            .unwrap_or_default();

                        let choice = if yes {
                            Some(0)
                        } else {
                            pick_pull_candidate(
                                &format!(
                                    "'{}' found in multiple registries, pull now?",
                                    args.target
                                ),
                                &items,
                                time_display,
                            )?
                        };

                        if let Some(choice) = choice {
                            let (registry, bag, _) = &items[choice];
                            let pull_options = PullOptions {
                                unpacked_mcap_compression: config_mcap_compression_to_core(
                                    compression.unpacked_mcap_compression,
                                ),
                                force: false,
                            };
                            pull_and_print(&mut marina, bag, Some(registry.as_str()), pull_options)
                                .await?;
                            match marina
                                .resolve_target(&args.target, eff_registry.as_deref())
                                .await?
                            {
                                ResolveResult::LocalPath(resolved)
                                | ResolveResult::Cached(resolved) => {
                                    println!("{}", resolved.display())
                                }
                                _ => {
                                    return Err(anyhow::anyhow!(
                                        "failed to resolve '{}' after pull",
                                        args.target
                                    ));
                                }
                            }
                        }
                    } else if quiet_non_interactive_yes {
                        let (registry, bag) = candidates.first().ok_or_else(|| {
                            anyhow::anyhow!("no candidates found for '{}'", args.target)
                        })?;
                        let pull_options = PullOptions {
                            unpacked_mcap_compression: config_mcap_compression_to_core(
                                compression.unpacked_mcap_compression,
                            ),
                            force: false,
                        };
                        let mut progress = ProgressReporter::silent();
                        marina
                            .pull_exact_with_progress_and_options(
                                bag,
                                Some(registry.as_str()),
                                pull_options,
                                &mut progress,
                            )
                            .await?;
                        match marina
                            .resolve_target(&args.target, args.registry.as_deref())
                            .await?
                        {
                            ResolveResult::LocalPath(resolved)
                            | ResolveResult::Cached(resolved) => {
                                println!("{}", resolved.display())
                            }
                            _ => {
                                return Err(anyhow::anyhow!(
                                    "failed to resolve '{}' after pull",
                                    args.target
                                ));
                            }
                        }
                    } else {
                        println!("'{}' found in multiple registries:", args.target);
                        for (registry, bag) in &candidates {
                            let hash = marina
                                .bag_info(registry, bag)
                                .await
                                .and_then(|i| i.bundle_hash)
                                .map(|h| format!("  [hash:{h}]"))
                                .unwrap_or_default();
                            println!("  {prog} pull {} --registry {}{}", bag, registry, hash);
                        }
                    }
                }
            }
        }
        Commands::Export(args) => {
            marina.export(&args.target, &args.output)?;
            println!("exported {} -> {}", args.target, args.output.display());
        }
        Commands::Rm(args) => {
            let pat = glob::Pattern::new(&args.pattern)
                .with_context(|| format!("invalid pattern '{}'", args.pattern))?;

            // Collect local matches.
            let local: Vec<BagRef> = marina
                .list_cached_bags()
                .into_iter()
                .filter(|b| pat.matches(&b.bag.to_string()))
                .map(|b| b.bag)
                .collect();

            // Collect remote matches: search all relevant registries, filter by pattern,
            // then verify write access for each registry that has at least one match.
            // Registries without write access are silently excluded from the candidate list.
            let remote: Vec<(BagRef, String)> = if args.remote {
                let registries: Vec<String> = if let Some(ref r) = args.registry {
                    vec![r.clone()]
                } else {
                    marina
                        .list_registry_configs()
                        .into_iter()
                        .map(|c| c.name.clone())
                        .collect()
                };
                let mut hits: Vec<(BagRef, String)> = Vec::new();
                for reg_name in registries {
                    let matches: Vec<BagRef> = marina
                        .search_remote("*", Some(&reg_name))
                        .await
                        .unwrap_or_default()
                        .into_iter()
                        .filter(|b| pat.matches(&b.to_string()))
                        .collect();
                    if matches.is_empty() {
                        continue;
                    }
                    // Only include this registry if we can actually delete from it.
                    if marina.check_write_access(Some(&reg_name)).await.is_ok() {
                        for b in matches {
                            hits.push((b, reg_name.clone()));
                        }
                    }
                }
                hits
            } else {
                vec![]
            };

            // Build one entry per bag, grouping all remote registries together.
            let local_keys: std::collections::HashSet<String> =
                local.iter().map(|b| b.to_string()).collect();

            struct RmEntry {
                bag: BagRef,
                is_local: bool,
                remote_registries: Vec<String>,
            }

            let mut all: Vec<RmEntry> = Vec::new();
            // Local bags (with any matching remote registries collected).
            for bag in &local {
                let key = bag.to_string();
                let remote_regs: Vec<String> = remote
                    .iter()
                    .filter(|(b, _)| b.to_string() == key)
                    .map(|(_, r)| r.clone())
                    .collect();
                all.push(RmEntry {
                    bag: bag.clone(),
                    is_local: true,
                    remote_registries: remote_regs,
                });
            }
            // Remote-only bags (not cached locally).
            for (bag, reg) in &remote {
                let key = bag.to_string();
                if local_keys.contains(&key) {
                    continue;
                }
                if let Some(entry) = all.iter_mut().find(|e| e.bag.to_string() == key) {
                    entry.remote_registries.push(reg.clone());
                } else {
                    all.push(RmEntry {
                        bag: bag.clone(),
                        is_local: false,
                        remote_registries: vec![reg.clone()],
                    });
                }
            }
            all.sort_by(|a, b| a.bag.to_string().cmp(&b.bag.to_string()));

            if all.is_empty() {
                println!("no datasets matching '{}'", args.pattern);
            } else {
                if !yes && !is_interactive_shell() {
                    anyhow::bail!(
                        "non-interactive shell: use -y to confirm removal of {} dataset(s)",
                        all.len()
                    );
                }
                for entry in all {
                    if entry.is_local
                        && confirm_yes_default(&format!("Remove {} (local)?", entry.bag), yes)?
                    {
                        marina.remove_local(&entry.bag)?;
                        println!("removed local {}", entry.bag);
                    }
                    for reg in &entry.remote_registries {
                        if confirm_yes_default(&format!("Remove {} from {}?", entry.bag, reg), yes)?
                        {
                            marina
                                .remove_remote(&entry.bag, Some(reg), args.write_http_index)
                                .await?;
                            println!("removed remote {} from {}", entry.bag, reg);
                        }
                    }
                }
                spawn_complete_refresh();
            }
        }
        Commands::Clean(args) => {
            marina.clean(args.all)?;
            if args.all {
                println!("removed cache and registries");
            } else {
                println!("removed cached resources (registries kept)");
            }
        }
        Commands::Import(args) => {
            let path =
                marina.import_local(&args.target, args.path.as_deref(), args.move_to_cache)?;
            if args.path.is_some() {
                println!("imported {} -> {}", args.target, path.display());
            } else {
                // Print just the path so the command can be used in $() substitution.
                println!("{}", path.display());
            }
            spawn_complete_refresh();
        }
        Commands::Inspect(args) => {
            let timeout_secs = config::load_registries()
                .map(|f| f.settings.registry_timeout_secs)
                .unwrap_or(10);
            let result = marina
                .inspect_bag(&args.target, args.registry.as_deref(), timeout_secs)
                .await?;
            print_inspect_result(&result);
        }
        Commands::CompleteRefresh => {
            let timeout_secs = config::load_registries()
                .map(|f| f.settings.registry_timeout_secs)
                .unwrap_or(10);

            let registry_names: Vec<String> = marina
                .list_registry_configs()
                .into_iter()
                .map(|c| c.name.clone())
                .collect();

            if registry_names.is_empty() {
                return Ok(());
            }

            let futs = registry_names
                .iter()
                .map(|name| marina.search_remote("*", Some(name.as_str())));

            let per_registry = match tokio::time::timeout(
                std::time::Duration::from_secs(timeout_secs),
                futures::future::join_all(futs),
            )
            .await
            {
                Ok(r) => r,
                Err(_) => return Ok(()),
            };

            let mut registries = std::collections::HashMap::new();
            for (result, reg_name) in per_registry.into_iter().zip(registry_names.iter()) {
                if let Ok(bags) = result {
                    registries.insert(
                        reg_name.clone(),
                        bags.into_iter().map(|b| b.to_string()).collect(),
                    );
                }
            }

            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            let index = CompletionIndex {
                timestamp,
                registries,
            };
            if let Some(cache_path) = completion_cache_path() {
                if let Some(parent) = cache_path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                if let Ok(json) = serde_json::to_string(&index) {
                    let _ = std::fs::write(cache_path, json);
                }
            }
        }
        Commands::Completions(args) => {
            // Safety: single-threaded at this point, no other env readers active
            unsafe { std::env::set_var("COMPLETE", args.shell.to_string()) };
            let prog_name: &'static str = match std::env::var("MARINA_PROG_NAME") {
                Ok(s) => Box::leak(s.into_boxed_str()),
                Err(_) => "marina",
            };
            clap_complete::CompleteEnv::with_factory(|| Cli::command().name(prog_name)).complete();
        }
        Commands::Version => {
            println!("{}", env!("CARGO_PKG_VERSION"));
        }
    }

    Ok(())
}

fn cli_pointcloud_mode_to_core(mode: CliPointcloudMode) -> PointCloudCompressionMode {
    match mode {
        CliPointcloudMode::Off => PointCloudCompressionMode::Disabled,
        CliPointcloudMode::Lossy => PointCloudCompressionMode::Lossy,
        CliPointcloudMode::Lossless => PointCloudCompressionMode::Lossless,
    }
}

fn cli_mcap_compression_to_core(comp: CliMcapCompression) -> McapChunkCompression {
    match comp {
        CliMcapCompression::None => McapChunkCompression::None,
        CliMcapCompression::Zstd => McapChunkCompression::Zstd,
        CliMcapCompression::Lz4 => McapChunkCompression::Lz4,
    }
}

fn cli_archive_compression_to_core(comp: CliArchiveCompression) -> ArchiveCompression {
    match comp {
        CliArchiveCompression::Gzip => ArchiveCompression::Gzip,
        CliArchiveCompression::None => ArchiveCompression::None,
    }
}

fn config_pointcloud_mode_to_core(mode: ConfigPointcloudMode) -> PointCloudCompressionMode {
    match mode {
        ConfigPointcloudMode::Off => PointCloudCompressionMode::Disabled,
        ConfigPointcloudMode::Lossy => PointCloudCompressionMode::Lossy,
        ConfigPointcloudMode::Lossless => PointCloudCompressionMode::Lossless,
    }
}

fn config_mcap_compression_to_core(comp: ConfigMcapCompression) -> McapChunkCompression {
    match comp {
        ConfigMcapCompression::None => McapChunkCompression::None,
        ConfigMcapCompression::Zstd => McapChunkCompression::Zstd,
        ConfigMcapCompression::Lz4 => McapChunkCompression::Lz4,
    }
}

fn config_archive_compression_to_core(comp: ConfigArchiveCompression) -> ArchiveCompression {
    match comp {
        ConfigArchiveCompression::Gzip => ArchiveCompression::Gzip,
        ConfigArchiveCompression::None => ArchiveCompression::None,
    }
}

fn print_inspect_result(result: &crate::core::InspectResult) {
    let is_tty = std::io::stdout().is_terminal();
    let time_display = config::load_registries()
        .map(|f| f.settings.time_display)
        .unwrap_or_default();

    println!("{}", result.bag);

    // Local section.
    if let Some(dir) = &result.local_dir {
        println!("  local: {}", dir.display());
        if result.local_files.is_empty() {
            println!("  files: (none)");
        } else {
            println!("  files:");
            for f in &result.local_files {
                println!("    {}  {}", human_bytes(f.size_bytes), f.relative_path);
            }
        }
    } else {
        println!("  local: not cached");
        println!("  files: pull to see attachment listing");
    }

    // Remote section.
    if !result.remote_hits.is_empty() {
        println!("  remote:");
        for hit in &result.remote_hits {
            if hit.timed_out {
                warn!(
                    "registry '{}' did not respond in time — skipped",
                    hit.registry
                );
            } else if let Some(info) = &hit.info {
                let hash = info
                    .bundle_hash
                    .as_deref()
                    .map(|h| {
                        if is_tty {
                            format!("  hash:{}", &h[..h.len().min(12)])
                        } else {
                            format!("  hash:{}", h)
                        }
                    })
                    .unwrap_or_default();
                let pushed = format_pushed_at(info.pushed_at, time_display);
                println!(
                    "    {}  original {}  packed {}{}  pushed {}",
                    hit.registry,
                    human_bytes(info.original_bytes),
                    human_bytes(info.packed_bytes),
                    hash,
                    pushed,
                );
            }
        }
    }
}

fn print_size_summary(title: &str, original_bytes: u64, packed_bytes: u64) {
    let ratio = if original_bytes > 0 {
        packed_bytes as f64 / original_bytes as f64
    } else {
        0.0
    };
    let saved = original_bytes.saturating_sub(packed_bytes);
    println!("{}", title);
    println!("  original: {}", human_bytes(original_bytes));
    println!(
        "  packed:   {} ({:.1}% of original, saved {})",
        human_bytes(packed_bytes),
        ratio * 100.0,
        human_bytes(saved)
    );
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", bytes, UNITS[unit])
    } else {
        format!("{:.2} {}", value, UNITS[unit])
    }
}

fn format_pushed_at(pushed_at: Option<u64>, display: TimeDisplay) -> String {
    let Some(ts) = pushed_at else {
        return "-".into();
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    if display == TimeDisplay::Absolute {
        // Format as YYYY-MM-DD using only the timestamp
        let secs_per_day = 86400u64;
        let days_since_epoch = ts / secs_per_day;
        // Compute Gregorian date from days since 1970-01-01
        let mut y = 1970u32;
        let mut d = days_since_epoch as u32;
        loop {
            let days_in_year = if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
                366
            } else {
                365
            };
            if d < days_in_year {
                break;
            }
            d -= days_in_year;
            y += 1;
        }
        let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
        let month_days = [
            31u32,
            if leap { 29 } else { 28 },
            31,
            30,
            31,
            30,
            31,
            31,
            30,
            31,
            30,
            31,
        ];
        let mut m = 1u32;
        for md in &month_days {
            if d < *md {
                break;
            }
            d -= md;
            m += 1;
        }
        return format!("{:04}-{:02}-{:02}", y, m, d + 1);
    }

    let elapsed = now.saturating_sub(ts);
    match elapsed {
        0..=59 => format!("{}s ago", elapsed),
        60..=3599 => format!("{}m ago", elapsed / 60),
        3600..=86399 => format!("{}h ago", elapsed / 3600),
        86400..=604799 => format!("{}d ago", elapsed / 86400),
        604800..=2591999 => format!("{}w ago", elapsed / 604800),
        2592000..=31535999 => format!("{}mo ago", elapsed / 2592000),
        _ => format!("{}y ago", elapsed / 31536000),
    }
}

fn format_bag_info(
    info: Option<&BagInfo>,
    time_display: TimeDisplay,
) -> (String, String, String, String, String, String) {
    match info {
        None => (
            "-".into(),
            "-".into(),
            "-".into(),
            "-".into(),
            "-".into(),
            "-".into(),
        ),
        Some(i) => (
            i.bundle_hash.clone().unwrap_or_else(|| "-".into()),
            human_bytes(i.original_bytes),
            human_bytes(i.packed_bytes),
            i.pointcloud.clone().unwrap_or_else(|| "-".into()),
            i.mcap_compression.clone().unwrap_or_else(|| "-".into()),
            format_pushed_at(i.pushed_at, time_display),
        ),
    }
}

fn local_registry_data_path(uri: &str) -> PathBuf {
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
